from __future__ import annotations

from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from .contract_v1_2 import GovernanceEvidenceObservation


class GovernanceEvidenceError(RuntimeError):
    pass


def _canonical_bytes(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def load_governance_registry(path: Path) -> dict[str, dict[str, Any]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if raw.get("schema_version") not in {"1.0.0", "1.1.0"}:
        raise GovernanceEvidenceError("unsupported governance source registry schema")
    sources = raw.get("sources")
    if not isinstance(sources, list) or not sources:
        raise GovernanceEvidenceError("governance source registry is empty")

    indexed: dict[str, dict[str, Any]] = {}
    covered_rules: set[str] = set()
    for item in sources:
        if not isinstance(item, dict):
            raise GovernanceEvidenceError("governance source registry entry must be an object")
        source_id = item.get("source_id")
        covers = item.get("covers")
        paths = item.get("repository_paths")
        if not isinstance(source_id, str) or not source_id:
            raise GovernanceEvidenceError("governance source_id is missing")
        if source_id in indexed:
            raise GovernanceEvidenceError(f"duplicate governance source_id: {source_id}")
        if item.get("role") != "internal_governance":
            raise GovernanceEvidenceError(f"{source_id}: invalid governance role")
        if not isinstance(covers, list) or not covers or any(not isinstance(rule, str) for rule in covers):
            raise GovernanceEvidenceError(f"{source_id}: invalid covers")
        if not isinstance(paths, list) or not paths or any(not isinstance(value, str) for value in paths):
            raise GovernanceEvidenceError(f"{source_id}: invalid repository_paths")
        overlap = covered_rules & set(covers)
        if overlap:
            raise GovernanceEvidenceError(f"governance rule covered twice: {sorted(overlap)}")
        covered_rules.update(covers)
        indexed[source_id] = item

    expected = {
        "technical.money_decimal_and_rounding",
        "technical.contract_vigency_and_quality",
    }
    if covered_rules != expected:
        raise GovernanceEvidenceError(
            f"governance registry must cover technical rules exactly; got={sorted(covered_rules)}"
        )
    return indexed


def build_governance_evidence(
    *,
    repository_root: Path,
    registry_path: Path,
    evidence_root: Path,
    observed_at_utc: datetime,
) -> dict[str, GovernanceEvidenceObservation]:
    """Create hash-addressed manifests for internal technical-contract rules.

    The manifest hashes the exact bytes of versioned repository paths. It is a
    governance/audit artifact only and is intentionally separate from official
    legal/fiscal source evidence.
    """
    root = Path(repository_root).resolve()
    registry = load_governance_registry(registry_path)
    output: dict[str, GovernanceEvidenceObservation] = {}

    for source_id, spec in sorted(registry.items()):
        file_hashes: dict[str, str] = {}
        byte_lengths: dict[str, int] = {}
        for relative in spec["repository_paths"]:
            path = (root / relative).resolve()
            if not path.is_relative_to(root):
                raise GovernanceEvidenceError(f"{source_id}: repository path escapes root")
            if not path.is_file():
                raise GovernanceEvidenceError(f"{source_id}: repository path missing: {relative}")
            body = path.read_bytes()
            file_hashes[relative] = sha256(body).hexdigest()
            byte_lengths[relative] = len(body)

        manifest = {
            "schema_version": "1.0.0",
            "source_id": source_id,
            "role": "internal_governance",
            "covers": spec["covers"],
            "repository_paths": spec["repository_paths"],
            "file_sha256": file_hashes,
            "file_byte_length": byte_lengths,
        }
        body = _canonical_bytes(manifest)
        digest = sha256(body).hexdigest()
        relative_snapshot = f"{source_id}/{digest[:2]}/{digest}.json"
        target = Path(evidence_root) / relative_snapshot
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if target.read_bytes() != body:
                raise GovernanceEvidenceError("governance evidence hash collision or corruption")
        else:
            with target.open("xb") as fh:
                fh.write(body)

        observation = GovernanceEvidenceObservation(
            source_id=source_id,
            observed_at_utc=observed_at_utc,
            snapshot_sha256=digest,
            snapshot_path=relative_snapshot,
            repository_paths=list(spec["repository_paths"]),
            locator=",".join(spec["covers"]),
        )
        for rule_id in spec["covers"]:
            output[rule_id] = observation

    return output
