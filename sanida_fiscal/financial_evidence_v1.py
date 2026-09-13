from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from .source_runtime_v1 import CandidateStore, SourceStateStore
from .sources_v1 import ParseStatus


FINANCIAL_PROVENANCE = {
    "selic": "BCB_SELIC_META_SGS_432",
    "cdi": "BCB_CDI_DAILY_SGS_12",
}


class FinancialEvidenceError(RuntimeError):
    pass


def _parse_utc(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise FinancialEvidenceError(f"{label} must be a non-empty UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FinancialEvidenceError(f"{label} is not ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise FinancialEvidenceError(f"{label} must be UTC")
    return parsed


def _resolve_inside(root: Path, relative_path: str, label: str) -> Path:
    if not isinstance(relative_path, str) or not relative_path:
        raise FinancialEvidenceError(f"{label} path is missing")
    relative = Path(relative_path)
    if relative.is_absolute():
        raise FinancialEvidenceError(f"{label} path must be relative")
    root_resolved = root.resolve()
    target = (root / relative).resolve()
    if not target.is_relative_to(root_resolved):
        raise FinancialEvidenceError(f"{label} path escapes evidence root")
    return target


def _verify_file_sha256(root: Path, relative_path: str, expected_sha256: str, label: str) -> Path:
    if not isinstance(expected_sha256, str) or len(expected_sha256) != 64:
        raise FinancialEvidenceError(f"{label} sha256 is invalid")
    target = _resolve_inside(root, relative_path, label)
    if not target.is_file():
        raise FinancialEvidenceError(f"{label} evidence file does not exist: {relative_path}")
    actual = sha256(target.read_bytes()).hexdigest()
    if actual != expected_sha256:
        raise FinancialEvidenceError(
            f"{label} sha256 mismatch: expected={expected_sha256} actual={actual}"
        )
    return target


def verify_financial_source_provenance(
    *,
    sources: Mapping[str, Any],
    runtime_root: Path,
) -> dict[str, dict[str, str]]:
    """Verify Selic/CDI provenance against durable snapshot/candidate/state evidence.

    This function intentionally accepts an already-extracted provenance mapping so
    both `taxas_bacen.json` and the nested financial provenance copied into
    `dados_fiscais.json` can be verified by the same code path.
    """
    runtime_root = Path(runtime_root)
    if not isinstance(sources, Mapping):
        raise FinancialEvidenceError("financial source provenance must be an object")

    state_store = SourceStateStore(runtime_root / "state")
    snapshot_root = runtime_root / "snapshots"
    candidate_root = runtime_root / "candidates"
    verified: dict[str, dict[str, str]] = {}

    for provenance_key, source_id in FINANCIAL_PROVENANCE.items():
        provenance = sources.get(provenance_key)
        if not isinstance(provenance, Mapping):
            raise FinancialEvidenceError(f"missing provenance for {provenance_key}")
        if provenance.get("source_id") != source_id:
            raise FinancialEvidenceError(
                f"{provenance_key} source_id mismatch: expected={source_id} got={provenance.get('source_id')}"
            )

        state = state_store.load(source_id)
        if state is None:
            raise FinancialEvidenceError(f"missing operational state for {source_id}")
        if state.last_parse_status != ParseStatus.PARSED:
            raise FinancialEvidenceError(f"{source_id} has no last-good PARSED state")

        snapshot_sha = provenance.get("snapshot_sha256")
        candidate_sha = provenance.get("candidate_sha256")
        if state.last_parsed_snapshot_sha256 != snapshot_sha:
            raise FinancialEvidenceError(f"{source_id} snapshot provenance differs from persisted state")
        if state.last_candidate_sha256 != candidate_sha:
            raise FinancialEvidenceError(f"{source_id} candidate provenance differs from persisted state")
        if state.last_successful_parser_id != provenance.get("parser_id"):
            raise FinancialEvidenceError(f"{source_id} parser_id provenance differs from last-good state")
        if state.last_successful_parser_version != provenance.get("parser_version"):
            raise FinancialEvidenceError(f"{source_id} parser_version provenance differs from last-good state")
        if state.source_url != provenance.get("url"):
            raise FinancialEvidenceError(f"{source_id} source URL provenance differs from persisted state")

        observed = _parse_utc(provenance.get("observed_at_utc"), f"{source_id}.observed_at_utc")
        if state.last_parsed_at_utc != observed:
            raise FinancialEvidenceError(f"{source_id} observed_at provenance differs from last-good state")

        snapshot_path = state.last_parsed_snapshot_path
        if snapshot_path is None:
            raise FinancialEvidenceError(f"{source_id} state has no last_parsed_snapshot_path")
        _verify_file_sha256(snapshot_root, snapshot_path, snapshot_sha, f"{source_id}.snapshot")

        candidate_path = state.last_candidate_path
        if candidate_path is None:
            raise FinancialEvidenceError(f"{source_id} state has no last_candidate_path")
        _verify_file_sha256(candidate_root, candidate_path, candidate_sha, f"{source_id}.candidate")
        CandidateStore(candidate_root).read(
            relative_path=candidate_path,
            expected_sha256=candidate_sha,
        )

        verified[source_id] = {
            "snapshot_sha256": snapshot_sha,
            "snapshot_path": snapshot_path,
            "candidate_sha256": candidate_sha,
            "candidate_path": candidate_path,
        }

    return verified


def verify_financial_artifact_evidence(
    *,
    artifact_path: Path,
    runtime_root: Path,
) -> dict[str, dict[str, str]]:
    artifact_path = Path(artifact_path)
    runtime_root = Path(runtime_root)
    if not artifact_path.is_file():
        raise FinancialEvidenceError(f"financial artifact does not exist: {artifact_path}")

    try:
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FinancialEvidenceError("financial artifact is not readable JSON") from exc
    if not isinstance(artifact, dict):
        raise FinancialEvidenceError("financial artifact root must be an object")

    meta = artifact.get("meta")
    sources = meta.get("sources") if isinstance(meta, dict) else None
    if not isinstance(sources, dict):
        raise FinancialEvidenceError("financial artifact has no source provenance")

    return verify_financial_source_provenance(
        sources=sources,
        runtime_root=runtime_root,
    )
