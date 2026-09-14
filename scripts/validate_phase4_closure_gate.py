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
        raise SystemExit(f"Phase 4 closure gate: {message}")


def main() -> None:
    audit = _load_json("docs/phase4-final-boundary-audit-v1.json")
    findings = audit.get("findings", {})
    _require(audit.get("status") == "formal_closure_authorized", "boundary audit does not authorize formal closure")
    _require(audit.get("closure_authorized") is True, "closure_authorized is not true")
    for finding_id in ("A01", "A02", "A03", "A04", "A05"):
        _require(findings.get(finding_id, {}).get("status") == "CORRECTED", f"{finding_id} is not CORRECTED")
    _require(
        findings.get("A05", {}).get("policy") == "preserve_auditable_block_new_consumption",
        "A05 quarantine policy drift",
    )

    financial_policy = _load_json("docs/phase4-financial-reference-policy-v1.json")
    a05 = financial_policy.get("failure_policy", {}).get("parser_incompatible_last_good", {})
    expected_a05 = {
        "policy_id": "preserve_auditable_block_new_consumption",
        "preserve_previous_snapshot_candidate_and_artifact": True,
        "allow_audit_of_preserved_last_good": True,
        "allow_new_taxas_artifact_generation": False,
        "allow_new_dados_fiscais_composition": False,
        "allow_generated_at_refresh": False,
        "allow_value_relabel_as_current": False,
    }
    for key, expected in expected_a05.items():
        _require(a05.get(key) == expected, f"A05 policy field drift: {key}")
    _require(
        a05.get("resume_condition") == "a current collection must again produce PARSED with valid evidence",
        "A05 recovery condition drift",
    )

    evidence = _read("sanida_fiscal/financial_evidence_v1.py")
    for marker in (
        'PARSER_INCOMPATIBLE_LAST_GOOD_POLICY = "preserve_auditable_block_new_consumption"',
        "verify_preserved_financial_last_good_provenance",
        "verify_preserved_financial_last_good_artifact_evidence",
        "require_current_parsed=True",
        "require_current_parsed=False",
        "current parser state is PARSER_INCOMPATIBLE",
    ):
        _require(marker in evidence, f"A05 executable evidence marker missing: {marker}")

    tests = _read("tests/test_financial_evidence_v1.py")
    for marker in (
        "test_A05_parser_incompatible_preserves_last_good_but_blocks_new_consumption",
        "ParseStatus.PARSER_INCOMPATIBLE",
        "verify_preserved_financial_last_good_artifact_evidence",
        'match="PARSER_INCOMPATIBLE"',
    ):
        _require(marker in tests, f"A05 regression-test marker missing: {marker}")

    persistence = _load_json("docs/phase4-production-persistence-v1.json")
    _require(persistence.get("status") == "phase4_closure_ready", "production persistence is not closure-ready")
    state_contract = persistence.get("state_contract", {})
    _require(
        state_contract.get("parser_incompatible_last_good_consumption_policy")
        == "preserve_auditable_block_new_consumption",
        "persistence A05 policy drift",
    )
    integrity = persistence.get("integrity_gates", {})
    _require(
        integrity.get("current_financial_parser_incompatible_must_block_new_consumption") is True,
        "persistence gate does not block incompatible current parser",
    )
    _require(
        integrity.get("preserved_financial_last_good_must_remain_auditable") is True,
        "persistence gate does not preserve last-good auditability",
    )
    _require(
        persistence.get("scope", {}).get("phase4_closure")
        == "authorized_subject_to_green_formal_closure_gate_on_final_head",
        "scope does not authorize Phase 4 closure review",
    )

    workflow = _read(".github/workflows/remake-ci.yml")
    _require("scripts/validate_phase4_foundation.py" in workflow, "Phase 4 foundation gate missing from CI")
    _require("scripts/validate_phase4_preclosure_gate.py" in workflow, "Phase 4 boundary gate missing from CI")
    _require("scripts/validate_phase4_closure_gate.py" in workflow, "Phase 4 formal closure gate missing from CI")

    phase4_doc = _read("docs/phase4-sources-sensors-v1.md")
    _require("Fase 5" in phase4_doc, "Phase 4 handoff does not preserve Phase 5 boundary")
    _require("Fase 6" in phase4_doc, "Phase 4 handoff does not preserve Phase 6 boundary")

    print("Phase 4 closure gate: PASS (A01-A05 corrected; formal closure authorized)")


if __name__ == "__main__":
    main()
