from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from hashlib import sha256
import json
from pathlib import Path
from typing import Any

from .financial_evidence_v1 import (
    FinancialEvidenceError,
    verify_preserved_financial_last_good_provenance,
)
from .source_ids_v1 import (
    INSS_SOURCE_ID,
    RFB_SOURCE_ID,
    canonical_source_id,
)
from .source_runtime_v1 import CandidateStore, SourceStateStore
from .source_catalog_v1 import (
    parser_binding, parser_for_reference_year, resolve_source_for_reference_year,
)
from .sources_v1 import load_source_registry, ParserIncompatibleError
from .legacy_artifact_v1 import build_legacy_payroll_fields
from .financial_reference_v1 import annualize_cdi_daily_rate_pct


PAYROLL_PROVENANCE = {
    "irrf": RFB_SOURCE_ID,
    "inss": INSS_SOURCE_ID,
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


def _archived_payroll_source(
    *,
    provenance_key: str,
    canonical_id: str,
    provenance: dict[str, Any],
    reference_year: int,
    generated_at: datetime,
    runtime_root: Path,
) -> tuple[dict[str, str], dict[str, Any]]:
    """Reparse the immutable original RFB/INSS bytes and bind to original data.

    This path audits the already-stored artifact only. It never certifies an old
    observation as a current collection or authorizes a new fiscal publication.
    """
    original_id = str(provenance["source_id"])
    observed = _parse_utc(provenance.get("observed_at_utc"), f"{canonical_id}.observed_at_utc")
    if observed.year != reference_year or observed > generated_at:
        raise ProductionEvidenceError(f"{canonical_id} archived observation time is outside artifact competence")
    if provenance.get("collection_status") != "COLLECTED" or provenance.get("http_code") != 200:
        raise ProductionEvidenceError(f"{canonical_id} archived source has no verified HTTP 200 collection")
    binding = parser_binding(canonical_id)
    if provenance.get("parser_id") != binding.parser_id or provenance.get("parser_version") != binding.parser_version:
        raise ProductionEvidenceError(f"{canonical_id} archived parser identity is incompatible")
    registry = load_source_registry(Path("docs/source-registry-v1.json"))
    official = resolve_source_for_reference_year(
        registry[canonical_id], source_id=canonical_id, reference_year=reference_year
    )
    if provenance.get("url") != official.url:
        raise ProductionEvidenceError(f"{canonical_id} archived source URL differs from registered authority")
    snapshot_hash = provenance.get("snapshot_sha256")
    candidate_hash = provenance.get("candidate_sha256")
    if (not isinstance(snapshot_hash, str) or len(snapshot_hash) != 64
        or not isinstance(candidate_hash, str) or len(candidate_hash) != 64):
        raise ProductionEvidenceError(f"{canonical_id} archived source hashes are missing")
    candidate_root, snapshot_root = runtime_root / "candidates", runtime_root / "snapshots"
    namespaces = list(dict.fromkeys((original_id, canonical_id)))
    paths = []
    for namespace in namespaces:
        path = CandidateStore.relative_path(namespace, candidate_hash)
        if (candidate_root / path).is_file():
            paths.append((namespace, path))
    if len(paths) != 1:
        raise ProductionEvidenceError(f"{canonical_id} archived candidate missing or ambiguous")
    namespace, candidate_path = paths[0]
    candidate_file = _verify_file_sha256(
        candidate_root, candidate_path, candidate_hash, f"{canonical_id}.candidate"
    )
    try:
        payload = json.loads(candidate_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProductionEvidenceError(f"{canonical_id} archived candidate not JSON") from exc
    if not isinstance(payload, dict) or payload.get("reference_year") != reference_year:
        raise ProductionEvidenceError(f"{canonical_id} archived candidate competence mismatch")
    snapshot_dir = snapshot_root / namespace / snapshot_hash[:2]
    matches = sorted(snapshot_dir.glob(snapshot_hash + ".*"))
    if len(matches) != 1:
        raise ProductionEvidenceError(f"{canonical_id} archived snapshot missing or ambiguous")
    snapshot_path = str(matches[0].relative_to(snapshot_root))
    source_file = _verify_file_sha256(
        snapshot_root, snapshot_path, snapshot_hash, f"{canonical_id}.snapshot"
    )
    try:
        expected = parser_for_reference_year(binding, reference_year)(source_file.read_bytes())
    except (ParserIncompatibleError, OSError) as exc:
        raise ProductionEvidenceError(f"{canonical_id} immutable historical snapshot cannot be reparsed") from exc
    if expected != payload:
        raise ProductionEvidenceError(f"{canonical_id} candidate differs from its immutable raw snapshot")
    return (
        {
            "snapshot_sha256": snapshot_hash,
            "snapshot_path": snapshot_path,
            "candidate_sha256": candidate_hash,
            "candidate_path": candidate_path,
        },
        payload,
    )


def verify_legacy_artifact_evidence(
    *,
    artifact_path: Path,
    runtime_root: Path,
    allow_archived_payroll: bool = False,
) -> dict[str, dict[str, str]]:
    """Verify the full four-source evidence chain of `dados_fiscais.json`.

    Phase 4 requires the compatibility artifact to be backed not only by the
    current RFB/INSS candidates but also by the exact Selic/CDI provenance that
    entered through `taxas_bacen.json`.
    """
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

    generated_at = _parse_utc(meta.get("generated_at_utc"), "artifact.generated_at_utc")
    reference_year = artifact.get("ano")
    if isinstance(reference_year, bool) or not isinstance(reference_year, int):
        raise ProductionEvidenceError("artifact year is missing")
    archived_payroll_payloads: dict[str, dict[str, Any]] = {}
    state_store = SourceStateStore(runtime_root / "state")
    snapshot_root = runtime_root / "snapshots"
    candidate_root = runtime_root / "candidates"
    verified: dict[str, dict[str, str]] = {}

    for provenance_key, source_id in PAYROLL_PROVENANCE.items():
        provenance = sources.get(provenance_key)
        if not isinstance(provenance, dict):
            raise ProductionEvidenceError(f"missing provenance for {provenance_key}")
        provenance_source_id = provenance.get("source_id")
        if not isinstance(provenance_source_id, str) or canonical_source_id(provenance_source_id) != source_id:
            raise ProductionEvidenceError(
                f"{provenance_key} source_id mismatch: expected canonical={source_id} "
                f"got={provenance_source_id}"
            )

        if allow_archived_payroll:
            record, payload = _archived_payroll_source(
                provenance_key=provenance_key, canonical_id=source_id,
                provenance=provenance, reference_year=reference_year,
                generated_at=generated_at, runtime_root=runtime_root,
            )
            verified[provenance_source_id] = record
            archived_payroll_payloads[provenance_key] = payload
            continue

        # Existing compatibility artifacts may legitimately point to the legacy
        # year-qualified state until the first post-migration producer run.
        state = state_store.load(provenance_source_id)
        if state is None and provenance_source_id != source_id:
            state = state_store.load(source_id)
        if state is None:
            raise ProductionEvidenceError(
                f"missing operational state for {provenance_source_id}"
            )

        snapshot_sha = provenance.get("snapshot_sha256")
        candidate_sha = provenance.get("candidate_sha256")
        if state.last_parsed_snapshot_sha256 != snapshot_sha:
            raise ProductionEvidenceError(f"{source_id} snapshot provenance differs from persisted last-good state")
        if state.last_candidate_sha256 != candidate_sha:
            raise ProductionEvidenceError(f"{source_id} candidate provenance differs from persisted last-good state")
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

        CandidateStore(candidate_root).read(
            relative_path=candidate_path,
            expected_sha256=candidate_sha,
        )

        verified[provenance_source_id] = {
            "snapshot_sha256": snapshot_sha,
            "snapshot_path": snapshot_path,
            "candidate_sha256": candidate_sha,
            "candidate_path": candidate_path,
        }

    taxas_meta = sources.get("taxas")
    if not isinstance(taxas_meta, dict):
        raise ProductionEvidenceError("missing financial provenance envelope in dados_fiscais.json")
    if taxas_meta.get("origin") != "local_file" or taxas_meta.get("origin_ref") != "taxas_bacen.json":
        raise ProductionEvidenceError("dados_fiscais.json must consume the local evidence-gated taxas_bacen.json")
    financial_sources = taxas_meta.get("source_meta")
    if not isinstance(financial_sources, dict):
        raise ProductionEvidenceError("dados_fiscais.json has no nested Selic/CDI provenance")

    # The local reference can have moved forward since payroll publication.
    # Compare exact metadata when it is still the same file; otherwise replay
    # archived immutable source evidence and validate the embedded amounts.
    original_taxas_path = artifact_path.parent / "taxas_bacen.json"
    if not original_taxas_path.is_file():
        raise ProductionEvidenceError("local taxas_bacen.json missing for embedded provenance check")
    try:
        original_taxas = json.loads(original_taxas_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ProductionEvidenceError("local taxas_bacen.json unreadable") from exc
    if (not isinstance(original_taxas, dict)
        or original_taxas.get("schema_version") != taxas_meta.get("schema_version")
        or taxas_meta.get("schema_version") != "1.4.0"):
        raise ProductionEvidenceError("financial provenance verification failed: local reference schema mismatch")
    local_meta = original_taxas.get("meta")
    if not isinstance(local_meta, dict):
        raise ProductionEvidenceError("financial provenance verification failed: local rate metadata missing")
    if (local_meta.get("sources") == financial_sources
        and local_meta.get("generated_at_utc") == taxas_meta.get("generated_at_utc")
        and original_taxas.get("taxas") != artifact.get("taxas")):
        raise ProductionEvidenceError("financial provenance verification failed: exact local financial amounts mismatch")

    taxa_generated = _parse_utc(taxas_meta.get("generated_at_utc"), "taxas.generated_at_utc")
    if taxa_generated > generated_at:
        raise ProductionEvidenceError("financial provenance verification failed: financial evidence is later than payroll artifact")
    for key in ("selic", "cdi"):
        source_meta = financial_sources.get(key)
        if not isinstance(source_meta, dict):
            raise ProductionEvidenceError(f"financial provenance verification failed: {key} metadata missing")
        if _parse_utc(source_meta.get("observed_at_utc"), f"{key}.observed_at_utc") > taxa_generated:
            raise ProductionEvidenceError(f"financial provenance verification failed: {key} observation after generated_at")
    try:
        financial_verified = verify_preserved_financial_last_good_provenance(
            sources=financial_sources, runtime_root=runtime_root,
        )
    except (FinancialEvidenceError, OSError, ValueError, RuntimeError) as exc:
        raise ProductionEvidenceError(f"financial provenance verification failed: {exc}") from exc
    try:
        selic = Decimal(str(financial_sources["selic"]["source_value_pct"]))
        daily = Decimal(str(financial_sources["cdi"]["source_value_pct"]))
        annual = annualize_cdi_daily_rate_pct(str(daily))
    except (InvalidOperation, KeyError, ValueError) as exc:
        raise ProductionEvidenceError("financial provenance verification failed: invalid source-rate arithmetic") from exc
    embedded = artifact.get("taxas")
    if (not isinstance(embedded, dict)
        or embedded.get("cdi_basis") != "bcb_sgs_12_daily_compounded_252"
        or Decimal(str(embedded.get("selic"))) != selic
        or Decimal(str(embedded.get("cdi"))) != annual):
        raise ProductionEvidenceError("financial provenance verification failed: embedded rates differ from archived candidates")
    verified.update(financial_verified)

    if allow_archived_payroll:
        expected_fields = build_legacy_payroll_fields(
            rfb_payload=archived_payroll_payloads["irrf"],
            inss_payload=archived_payroll_payloads["inss"],
            expected_year=reference_year,
        )
        for field in ("ano", "dep", "irrf", "inss"):
            if artifact.get(field) != expected_fields.get(field):
                raise ProductionEvidenceError(f"archived payroll field differs from immutable source: {field}")

    return verified



def verify_preserved_legacy_artifact_evidence(
    *, artifact_path: Path, runtime_root: Path
) -> dict[str, dict[str, str]]:
    """Audit an already-published payroll file without authorizing freshness."""
    return verify_legacy_artifact_evidence(
        artifact_path=artifact_path,
        runtime_root=runtime_root,
        allow_archived_payroll=True,
    )
