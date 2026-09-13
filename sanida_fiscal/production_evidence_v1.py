from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from .source_runtime_v1 import CandidateStore, SourceStateStore
from .sources_v1 import ParseStatus


PAYROLL_PROVENANCE = {
    "irrf": "RFB_IRRF_TABLE_2026",
    "inss": "INSS_TABLE_2026",
}


class ProductionEvidenceError(RuntimeError):
    pass


def _parse_utc(value: Any, label: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ProductionEvidenceError(f"{label} must be a non-empty UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProductionEvidenceError(f"{label} is not ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ProductionEvidenceError(f"{label} must be UTC")
    return parsed


def _resolve_inside(root: Path, relative_path: str, label: str) -> Path:
    if not isinstance(relative_path, str) or not relative_path:
        raise ProductionEvidenceError(f"{label} path is missing")
    relative = Path(relative_path)
    if relative.is_absolute():
        raise ProductionEvidenceError(f"{label} path must be relative")
    root_resolved = root.resolve()
    target = (root / relative).resolve()
    if not target.is_relative_to(root_resolved):
        raise ProductionEvidenceError(f"{label} path escapes evidence root")
    return target


def _verify_file_sha256(root: Path, relative_path: str, expected_sha256: str, label: str) -> Path:
    if not isinstance(expected_sha256, str) or len(expected_sha256) != 64:
        raise ProductionEvidenceError(f"{label} sha256 is invalid")
    target = _resolve_inside(root, relative_path, label)
    if not target.is_file():
        raise ProductionEvidenceError(f"{label} evidence file does not exist: {relative_path}")
    actual = sha256(target.read_bytes()).hexdigest()
    if actual != expected_sha256:
        raise ProductionEvidenceError(
            f"{label} sha256 mismatch: expected={expected_sha256} actual={actual}"
        )
    return target


def verify_legacy_artifact_evidence(
    *,
    artifact_path: Path,
    runtime_root: Path,
) -> dict[str, dict[str, str]]:
    artifact_path = Path(artifact_path)
    runtime_root = Path(runtime_root)
    if not artifact_path.is_file():
        raise ProductionEvidenceError(f"legacy artifact does not exist: {artifact_path}")

    try:
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProductionEvidenceError("legacy artifact is not readable JSON") from exc
    if not isinstance(artifact, dict):
        raise ProductionEvidenceError("legacy artifact root must be an object")

    meta = artifact.get("meta")
    sources = meta.get("sources") if isinstance(meta, dict) else None
    if not isinstance(sources, dict):
        raise ProductionEvidenceError("legacy artifact has no source provenance")

    state_store = SourceStateStore(runtime_root / "state")
    snapshot_root = runtime_root / "snapshots"
    candidate_root = runtime_root / "candidates"
    verified: dict[str, dict[str, str]] = {}

    for provenance_key, source_id in PAYROLL_PROVENANCE.items():
        provenance = sources.get(provenance_key)
        if not isinstance(provenance, dict):
            raise ProductionEvidenceError(f"missing provenance for {provenance_key}")
        if provenance.get("source_id") != source_id:
            raise ProductionEvidenceError(
                f"{provenance_key} source_id mismatch: expected={source_id} got={provenance.get('source_id')}"
            )

        state = state_store.load(source_id)
        if state is None:
            raise ProductionEvidenceError(f"missing operational state for {source_id}")
        if state.last_parse_status != ParseStatus.PARSED:
            raise ProductionEvidenceError(f"{source_id} has no last-good PARSED state")

        snapshot_sha = provenance.get("snapshot_sha256")
        candidate_sha = provenance.get("candidate_sha256")
        if state.last_parsed_snapshot_sha256 != snapshot_sha:
            raise ProductionEvidenceError(f"{source_id} snapshot provenance differs from persisted state")
        if state.last_candidate_sha256 != candidate_sha:
            raise ProductionEvidenceError(f"{source_id} candidate provenance differs from persisted state")
        if state.last_successful_parser_id != provenance.get("parser_id"):
            raise ProductionEvidenceError(f"{source_id} parser_id provenance differs from last-good state")
        if state.last_successful_parser_version != provenance.get("parser_version"):
            raise ProductionEvidenceError(f"{source_id} parser_version provenance differs from last-good state")
        if state.source_url != provenance.get("url"):
            raise ProductionEvidenceError(f"{source_id} source URL provenance differs from persisted state")

        observed = _parse_utc(provenance.get("observed_at_utc"), f"{source_id}.observed_at_utc")
        if state.last_parsed_at_utc != observed:
            raise ProductionEvidenceError(f"{source_id} observed_at provenance differs from last-good state")

        snapshot_path = state.last_parsed_snapshot_path
        if snapshot_path is None:
            raise ProductionEvidenceError(f"{source_id} state has no last_parsed_snapshot_path")
        _verify_file_sha256(snapshot_root, snapshot_path, snapshot_sha, f"{source_id}.snapshot")

        candidate_path = state.last_candidate_path
        if candidate_path is None:
            raise ProductionEvidenceError(f"{source_id} state has no last_candidate_path")
        candidate_file = _verify_file_sha256(
            candidate_root,
            candidate_path,
            candidate_sha,
            f"{source_id}.candidate",
        )
        try:
            candidate_payload = json.loads(candidate_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ProductionEvidenceError(f"{source_id} candidate evidence is not JSON") from exc
        if not isinstance(candidate_payload, dict):
            raise ProductionEvidenceError(f"{source_id} candidate evidence root must be an object")

        # CandidateStore.read performs the same digest contract used by the producer.
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
