"""Fail-closed no-publication gate for verified presentation-only source refreshes.

Keep collected raw snapshots in evidence/; do not create a new fiscal release merely
because a government website rotated a transport script or site navigation.
Material, structural, legal-text, parser-identity or parameter changes continue
through the existing review/publication assessment unchanged.
"""
from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from .review_evidence_identity_v1 import (
    ReviewEvidenceIdentityError,
    _safe_snapshot_path,
    canonical_html_review_fingerprint,
)
from .semantic_diff_v1 import PromotionOutcome


_NOISE_ONLY_PATHS = frozenset({
    "provenance[0].observed_at_utc",
    "provenance[0].snapshot_path",
    "provenance[0].snapshot_sha256",
    "quality.last_validated_at_utc",
    "rule_version",
})


def _material_html_fingerprint(body: bytes, source_id: str) -> str | None:
    if source_id == "MTE_TERMINATION_FAQ":
        # The Plone portal duplicates its global navigation outside the article.
        # Require this exact official article container: never fall back to the
        # whole page when its identity/structure is no longer recognizable.
        soup = BeautifulSoup(body, "html.parser")
        matches = soup.select("#content-core")
        if len(matches) != 1:
            return None
        body = str(matches[0]).encode("utf-8")
    return canonical_html_review_fingerprint(body)


def _verified_snapshot(root: Path, observation: Any) -> bytes:
    path = getattr(observation, "snapshot_path", None)
    digest = getattr(observation, "snapshot_sha256", None)
    if not isinstance(path, str) or not path.lower().endswith((".html", ".htm")):
        raise ReviewEvidenceIdentityError("non-HTML evidence is not eligible for noise suppression")
    if not isinstance(digest, str) or len(digest) != 64:
        raise ReviewEvidenceIdentityError("invalid snapshot digest")
    raw = _safe_snapshot_path(root, path).read_bytes()
    if sha256(raw).hexdigest() != digest:
        raise ReviewEvidenceIdentityError("snapshot bytes do not match recorded digest")
    return raw


def is_presentation_only_refresh(
    *,
    previous: Any,
    candidate: Any,
    assessment: Any,
    authority_snapshot_root: Path,
) -> bool:
    """True only if *every* delta is source transport/presentation noise.

    Never permit an automatic parameter, legal-text, version-of-parser, effective
    date, rule inventory or contract-level change through this branch.
    Raw evidence is independently hash-verified for old and new observations.
    """
    if previous is None or assessment.outcome != PromotionOutcome.REVIEW_REQUIRED:
        return False
    if assessment.diff.contract_changed_paths:
        return False

    old_rules = {rule.rule_id: rule for rule in previous.rules}
    new_rules = {rule.rule_id: rule for rule in candidate.rules}
    if (
        len(old_rules) != len(previous.rules)
        or len(new_rules) != len(candidate.rules)
        or len(old_rules) != 32
        or set(old_rules) != set(new_rules)
    ):
        return False

    changed = [item for item in assessment.diff.rule_diffs if item.changed]
    if not changed:
        return False

    for item in changed:
        if (
            item.change_class.value != "SOURCE_REFRESH_NO_CHANGE"
            or item.occurrence != 0
            or not set(item.changed_paths).issubset(_NOISE_ONLY_PATHS)
            or "provenance[0].snapshot_sha256" not in item.changed_paths
        ):
            return False
        old_prov = old_rules[item.rule_id].provenance
        new_prov = new_rules[item.rule_id].provenance
        if len(old_prov) != 1 or len(new_prov) != 1:
            return False
        old, new = old_prov[0], new_prov[0]
        for field in ("source_id", "parser_id", "parser_version", "role",
                      "retrieval_method", "status"):
            if getattr(old, field, None) != getattr(new, field, None):
                return False
        if getattr(old, "status", None) != getattr(new, "status", None):
            return False
        if getattr(old, "snapshot_sha256", None) == getattr(new, "snapshot_sha256", None):
            return False
        try:
            before = _verified_snapshot(authority_snapshot_root, old)
            after = _verified_snapshot(authority_snapshot_root, new)
            old_id = _material_html_fingerprint(before, old.source_id)
            new_id = _material_html_fingerprint(after, new.source_id)
        except (OSError, ValueError, ReviewEvidenceIdentityError):
            return False
        if old_id is None or new_id is None or old_id != new_id:
            return False

    return True
