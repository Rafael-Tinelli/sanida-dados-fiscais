from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _load_json(path: str):
    return json.loads(_read(path))


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"Phase 4 pre-closure: {message}")


def main() -> None:
    audit = _load_json("docs/phase4-final-boundary-audit-v1.json")
    findings = audit.get("findings", {})
    for finding_id in ("A01", "A02", "A03", "A04"):
        _require(findings.get(finding_id, {}).get("status") == "CORRECTED", f"{finding_id} is not corrected")
    _require(findings.get("A05", {}).get("status") == "OPEN", "A05 must remain explicitly OPEN at this checkpoint")
    _require(audit.get("closure_authorized") is False, "pre-closure checkpoint must not authorize Phase 4 closure")

    scraper = _read("scraper.py")
    for forbidden in (
        "SFA_TAXAS_JSON_URL",
        "TAXAS_JSON_URL_DEFAULT",
        "raw.githubusercontent.com",
        '"remote_url"',
    ):
        _require(forbidden not in scraper, f"A01 remote financial fallback leaked into scraper: {forbidden}")
    for required in (
        "FINANCIAL_ARTIFACT_SCHEMA_VERSION",
        "verify_financial_artifact_evidence",
        "bcb_sgs_12_daily_compounded_252",
        'return local, "local_file", TAXAS_FILE_LOCAL',
    ):
        _require(required in scraper, f"A01 strict financial consumer marker missing: {required}")

    boundary = _load_json("docs/phase4-legacy-artifact-boundary-v1.json")
    financial_input = boundary.get("financial_reference_input", {})
    _require(financial_input.get("required_schema_version") == "1.4.0", "A01 required financial schema is not 1.4.0")
    _require(financial_input.get("required_origin") == "local_file", "A01 financial origin is not local_file")
    _require(financial_input.get("remote_fallback_allowed") is False, "A01 remote financial fallback is allowed")
    composed = boundary.get("composed_evidence_gate", {})
    _require(
        set(composed.get("required_sources", []))
        == {
            "RFB_IRRF_TABLE_2026",
            "INSS_TABLE_2026",
            "BCB_SELIC_META_SGS_432",
            "BCB_CDI_DAILY_SGS_12",
        },
        "A01 composed evidence source set drift",
    )

    production_evidence = _read("sanida_fiscal/production_evidence_v1.py")
    _require("verify_financial_source_provenance" in production_evidence, "A01 production gate does not verify nested financial provenance")
    _require('taxas_meta.get("origin") != "local_file"' in production_evidence, "A01 production gate does not require local financial origin")

    financial_policy = _load_json("docs/phase4-financial-reference-policy-v1.json")
    freshness = financial_policy.get("freshness_policy", {})
    _require(freshness.get("selic_meta", {}).get("max_observation_age_calendar_days") is None, "A02 Selic persistence policy drift")
    _require(freshness.get("selic_meta", {}).get("future_observation_allowed") is False, "A02 future Selic observation allowed")
    _require(freshness.get("cdi_daily", {}).get("max_observation_age_calendar_days") == 7, "A02 CDI freshness window drift")
    _require(freshness.get("cdi_daily", {}).get("future_observation_allowed") is False, "A02 future CDI observation allowed")

    financial_artifact = _read("sanida_fiscal/financial_artifact_v1.py")
    for marker in (
        "CDI_MAX_OBSERVATION_AGE_DAYS = 7",
        "CDI observation is stale",
        "Selic observation_date cannot be in the future",
        "CDI observation_date cannot be in the future",
    ):
        _require(marker in financial_artifact, f"A02 executable freshness marker missing: {marker}")

    main_workflow = _read(".github/workflows/main.yml")
    taxas_workflow = _read(".github/workflows/taxas.yml")
    for workflow_name, workflow in (("main.yml", main_workflow), ("taxas.yml", taxas_workflow)):
        _require("group: sanida-dados-fiscais-writes-main" in workflow, f"A03 {workflow_name} concurrency group drift")
        _require("cancel-in-progress: false" in workflow, f"A03 {workflow_name} must not cancel in-progress writer")
        _require("queue: max" in workflow, f"A03 {workflow_name} must queue pending writers")

    persistence = _load_json("docs/phase4-production-persistence-v1.json")
    env = persistence.get("production_environment", {})
    _require(env.get("queue_policy") == "max", "A03 persistence policy queue drift")
    _require(env.get("cancel_in_progress") is False, "A03 persistence policy cancel drift")
    integrity = persistence.get("integrity_gates", {})
    _require(integrity.get("dados_fiscais_must_verify_all_four_sources") is True, "A04 four-source integrity gate not documented")
    _require(integrity.get("dados_fiscais_must_reject_remote_financial_origin") is True, "A04 local-only financial integrity gate not documented")
    _require(persistence.get("scope", {}).get("phase4_closure") == "not_yet_authorized_until_A05_is_resolved", "A04 closure status overclaims readiness")

    remake = _read(".github/workflows/remake-ci.yml")
    _require("scripts/validate_phase4_foundation.py" in remake, "A04 foundation gate missing from CI")
    _require("scripts/validate_phase4_preclosure_gate.py" in remake, "A04 pre-closure gate missing from CI")

    print("Phase 4 pre-closure: PASS (A01-A04 corrected; A05 OPEN; formal closure NOT authorized)")


if __name__ == "__main__":
    main()
