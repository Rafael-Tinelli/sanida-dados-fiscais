from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import re
import unicodedata
from typing import Any, Mapping

from bs4 import BeautifulSoup, Comment


HTML_REVIEW_FINGERPRINT_VERSION = "html-visible-text-links-v1"
PARSED_REVIEW_FINGERPRINT_VERSION = "parsed-normalized-payload-v1"
RAW_REVIEW_FINGERPRINT_VERSION = "raw-snapshot-sha256-v1"


class ReviewEvidenceIdentityError(ValueError):
    pass


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFC", value)
    return re.sub(r"\s+", " ", value).strip()


def canonical_html_review_fingerprint(body: bytes) -> str:
    """Hash material human-readable HTML content, not transport/markup noise.

    This is deliberately *not* a fiscal parser. It preserves the ordered visible
    text plus anchor labels/targets while discarding script/style/comment markup,
    tag attributes and whitespace/layout differences. A legal/operational text or
    link change therefore rotates the fingerprint; presentation-only HTML churn does
    not.
    """
    soup = BeautifulSoup(body, "html.parser")
    for node in soup(["script", "style", "noscript", "template"]):
        node.decompose()
    for comment in soup.find_all(string=lambda item: isinstance(item, Comment)):
        comment.extract()

    visible_text = _normalize_text(" ".join(soup.stripped_strings))
    links: list[dict[str, str]] = []
    for anchor in soup.find_all("a"):
        href = anchor.get("href")
        if not isinstance(href, str):
            continue
        label = _normalize_text(anchor.get_text(" ", strip=True))
        target = _normalize_text(href)
        if label or target:
            links.append({"label": label, "href": target})

    material = {
        "fingerprint_version": HTML_REVIEW_FINGERPRINT_VERSION,
        "visible_text": visible_text,
        "links": links,
    }
    return sha256(_canonical_bytes(material)).hexdigest()


def canonical_parsed_review_fingerprint(normalized_candidate: Any) -> str:
    """Hash the parser contract and normalized semantic payload, not HTML bytes.

    Raw snapshots remain content-addressed and auditable in the canonical contract.
    This fingerprint is used only for human-review identity. A parser-backed source
    may change presentation bytes without creating a different human decision when
    the same parser/version still validates the source and emits the same normalized
    payload. Parser incompatibility, parser/version changes, source changes or
    normalized payload changes all rotate or block the review identity.
    """
    status = _enum_value(getattr(normalized_candidate, "status", ""))
    payload = getattr(normalized_candidate, "payload", None)
    parser_id = getattr(normalized_candidate, "parser_id", None)
    parser_version = getattr(normalized_candidate, "parser_version", None)
    source_id = getattr(normalized_candidate, "source_id", None)
    source_url = getattr(normalized_candidate, "source_url", None)

    if status != "PARSED" or not isinstance(payload, dict):
        raise ReviewEvidenceIdentityError("parser-backed review evidence requires PARSED normalized payload")
    if not all(isinstance(value, str) and value for value in (source_id, source_url, parser_id, parser_version)):
        raise ReviewEvidenceIdentityError("parser-backed normalized candidate identity is incomplete")

    material = {
        "fingerprint_version": PARSED_REVIEW_FINGERPRINT_VERSION,
        "source_id": source_id,
        "source_url": source_url,
        "parser_id": parser_id,
        "parser_version": parser_version,
        "payload": payload,
    }
    return sha256(_canonical_bytes(material)).hexdigest()


def _safe_snapshot_path(root: Path, relative_path: str) -> Path:
    rel = Path(relative_path)
    if rel.is_absolute() or ".." in rel.parts:
        raise ReviewEvidenceIdentityError(f"unsafe evidence snapshot path: {relative_path}")
    root_resolved = root.resolve()
    candidate = (root_resolved / rel).resolve()
    try:
        candidate.relative_to(root_resolved)
    except ValueError as exc:
        raise ReviewEvidenceIdentityError(
            f"evidence snapshot escapes configured root: {relative_path}"
        ) from exc
    if not candidate.is_file():
        raise ReviewEvidenceIdentityError(f"evidence snapshot missing: {relative_path}")
    return candidate


def _enum_value(value: Any) -> str:
    return str(getattr(value, "value", value))


def _parsed_review_fingerprint_for_observation(
    observation: Any,
    normalized_candidates: Mapping[str, Any],
) -> tuple[str, str]:
    source_id = getattr(observation, "source_id", None)
    raw_sha = getattr(observation, "snapshot_sha256", None)
    parser_id = getattr(observation, "parser_id", None)
    parser_version = getattr(observation, "parser_version", None)

    if not isinstance(source_id, str):
        raise ReviewEvidenceIdentityError("parser-backed observation has no source_id")
    normalized = normalized_candidates.get(source_id)
    if normalized is None:
        raise ReviewEvidenceIdentityError(
            f"parser-backed review evidence has no normalized candidate: {source_id}"
        )
    if getattr(normalized, "snapshot_sha256", None) != raw_sha:
        raise ReviewEvidenceIdentityError(
            f"parser-backed raw snapshot mismatch for {source_id}"
        )
    if getattr(normalized, "parser_id", None) != parser_id:
        raise ReviewEvidenceIdentityError(
            f"parser identity mismatch for {source_id}"
        )
    if getattr(normalized, "parser_version", None) != parser_version:
        raise ReviewEvidenceIdentityError(
            f"parser version mismatch for {source_id}"
        )
    return PARSED_REVIEW_FINGERPRINT_VERSION, canonical_parsed_review_fingerprint(normalized)


def _review_fingerprint_for_observation(
    observation: Any,
    snapshot_root: Path,
    normalized_candidates: Mapping[str, Any],
) -> tuple[str, str]:
    raw_sha = getattr(observation, "snapshot_sha256", None)
    snapshot_path = getattr(observation, "snapshot_path", None)
    parser_id = getattr(observation, "parser_id", None)
    retrieval_method = _enum_value(getattr(observation, "retrieval_method", ""))

    if not isinstance(raw_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", raw_sha):
        raise ReviewEvidenceIdentityError("review evidence requires a valid raw snapshot SHA-256")

    if parser_id:
        return _parsed_review_fingerprint_for_observation(observation, normalized_candidates)

    eligible_structural_html = (
        retrieval_method == "http_html"
        and isinstance(snapshot_path, str)
        and snapshot_path.lower().endswith((".html", ".htm"))
    )
    if not eligible_structural_html:
        return RAW_REVIEW_FINGERPRINT_VERSION, raw_sha

    path = _safe_snapshot_path(snapshot_root, snapshot_path)
    body = path.read_bytes()
    actual_raw_sha = sha256(body).hexdigest()
    if actual_raw_sha != raw_sha:
        raise ReviewEvidenceIdentityError(
            f"snapshot integrity mismatch for {snapshot_path}: provenance={raw_sha} actual={actual_raw_sha}"
        )
    return HTML_REVIEW_FINGERPRINT_VERSION, canonical_html_review_fingerprint(body)


def build_review_identity_candidate(
    candidate: Any,
    *,
    authority_snapshot_root: Path,
    normalized_candidates: Mapping[str, Any] | None = None,
) -> tuple[Any, dict[str, dict[str, str]]]:
    """Return a review-only clone with material evidence identities.

    The canonical candidate/release is never mutated and retains every exact raw
    snapshot SHA-256. Only the clone used to compute `review_key` substitutes:

    - parser-backed HTTP evidence -> parser id/version + normalized semantic payload;
    - unparsed HTTP HTML -> visible text + link targets;
    - all other evidence -> exact raw snapshot SHA-256.

    This keeps human approval sensitive to material fiscal/evidence changes while
    preventing volatile server markup from creating an impossible stale-approval
    loop. Missing/inconsistent parser output fails closed rather than falling back.
    """
    if hasattr(candidate, "model_copy"):
        review_candidate = candidate.model_copy(deep=True)
    else:
        review_candidate = deepcopy(candidate)

    normalized_candidates = normalized_candidates or {}
    identities: dict[str, dict[str, str]] = {}
    cache: dict[tuple[str, str, str | None, str | None, str | None], tuple[str, str]] = {}

    rules = getattr(review_candidate, "rules", None)
    if not isinstance(rules, list):
        raise ReviewEvidenceIdentityError("candidate has no mutable rules list")

    for rule in rules:
        provenance = getattr(rule, "provenance", None)
        if not isinstance(provenance, list):
            continue
        for observation in provenance:
            source_id = getattr(observation, "source_id", None)
            raw_sha = getattr(observation, "snapshot_sha256", None)
            snapshot_path = getattr(observation, "snapshot_path", None)
            parser_id = getattr(observation, "parser_id", None)
            parser_version = getattr(observation, "parser_version", None)
            if not isinstance(source_id, str) or not isinstance(raw_sha, str):
                continue
            key = (source_id, raw_sha, snapshot_path, parser_id, parser_version)
            if key not in cache:
                cache[key] = _review_fingerprint_for_observation(
                    observation,
                    authority_snapshot_root,
                    normalized_candidates,
                )
            mode, fingerprint = cache[key]
            identities[source_id] = {
                "mode": mode,
                "review_fingerprint_sha256": fingerprint,
                "raw_snapshot_sha256": raw_sha,
            }
            observation.snapshot_sha256 = fingerprint

    return review_candidate, identities
