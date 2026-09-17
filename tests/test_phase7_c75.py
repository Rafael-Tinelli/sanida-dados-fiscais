from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.simulate_phase7_c75_postdeploy_validation import run_simulation
from scripts.validate_phase7_c75_external_delivery_evidence import EXPECTED_ASSETS, validate as validate_external

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "state/phase7-c75-closure.json"
RUNNER = ROOT / "scripts/run_phase7_c75_postdeploy_validation.py"
HOST_VALIDATOR = ROOT / "scripts/validate_phase7_c75_remote_evidence.py"
EXTERNAL_PROBE = ROOT / "scripts/run_phase7_c75_external_delivery_probe.ps1"
EXTERNAL_VALIDATOR = ROOT / "scripts/validate_phase7_c75_external_delivery_evidence.py"
INITIAL_RECORD = ROOT / "docs/phase7-c75-initial-host-observation.json"
FINAL_RECORD = ROOT / "docs/phase7-c75-final-production-validation-record.json"
V2_FINAL_RECORD = ROOT / "docs/phase7-v2-c75-final-production-validation-505144918d05fb4932064f6f9da9b9358c448ac0.json"
DOC = ROOT / "docs/phase7-c75-postdeploy-closure.md"

HOST_SHA = "996561d9acaee9473d4bbd2035ce32c8d1bff85c8e9e6eb38b10e2f26a8ff447"
EXTERNAL_BLOCKED_SHA = "908551d36f340d97fbaa1277768fd1330651c1a555f7312c286003300843fd70"
EXTERNAL_PASS_SHA = "e162b1f7d60427bc9fd2679bdce683adac566ecdfcd349637dfef883cb981284"
HISTORICAL_AUTHORIZED_COMMIT = "ccc5a31c3da7c1c93570df0337e553e5a06404ac"
V2_AUTHORIZED_COMMIT = "505144918d05fb4932064f6f9da9b9358c448ac0"


def test_c75_state_is_formally_concluded_without_production_file_mutation() -> None:
    state = json.loads(STATE.read_text(encoding="utf-8"))
    assert state["checkpoint"] == "C7.5"
    assert state["schema_version"] == "1.2.0"
    assert state["status"] == "CONCLUÍDO"
    assert state["phase7_status"] == "CONCLUÍDA"
    assert state["remake_status"] == "CONCLUÍDO"
    assert state["production_deployed"] is True
    assert state["production_mutation_allowed"] is False
    assert state["production_files_mutated_by_c75"] is False
    assert state["edge_cache_remediation_performed"] is True
    assert state["closure_condition_met"] is True
    assert state["host_origin_evidence"]["raw_evidence_sha256"] == HOST_SHA
    assert state["host_origin_evidence"]["formal_revalidation_status"] == "PASS"
    assert state["external_delivery_evidence"]["initial_blocked_sha256"] == EXTERNAL_BLOCKED_SHA
    assert state["external_delivery_evidence"]["final_pass_sha256"] == EXTERNAL_PASS_SHA
    assert state["external_delivery_evidence"]["final_assets_matching"] == 8
    assert state["external_delivery_evidence"]["authorized_commit"] == HISTORICAL_AUTHORIZED_COMMIT
    assert state["cache_remediation"]["c74_redeployment_performed"] is False
    assert state["cache_remediation"]["production_files_rewritten"] is False


def test_c75_final_record_preserves_blocked_and_pass_evidence() -> None:
    record = json.loads(FINAL_RECORD.read_text(encoding="utf-8"))
    assert record["record_type"] == "phase7_final_postdeploy_closure"
    assert record["status"] == "PASS"
    assert record["phase7_status"] == "CONCLUÍDA"
    assert record["production_files_mutated_by_c75"] is False
    assert record["host_origin"]["raw_evidence_sha256"] == HOST_SHA
    assert record["host_origin"]["formal_revalidation_status"] == "PASS"
    assert record["external_delivery_initial"]["status"] == "BLOCKED"
    assert record["external_delivery_initial"]["evidence_sha256"] == EXTERNAL_BLOCKED_SHA
    assert record["external_delivery_initial"]["assets_matching"] == 5
    assert record["external_delivery_final"]["status"] == "PASS"
    assert record["external_delivery_final"]["evidence_sha256"] == EXTERNAL_PASS_SHA
    assert record["external_delivery_final"]["assets_matching"] == 8
    assert len(record["external_delivery_final"]["assets"]) == 8
    assert record["cache_diagnosis"]["cache_control"] == "max-age=31536000"
    assert record["cache_diagnosis"]["cache_busted_origin_matches"] == 3
    assert record["closure_conditions"]["c74_redeployment_performed"] is False
    assert record["closure_conditions"]["phase7_close_recommended"] is True


def test_c75_v2_final_record_preserves_current_deployment_binding() -> None:
    record = json.loads(V2_FINAL_RECORD.read_text(encoding="utf-8"))
    assert record["record_type"] == "phase7_v2_final_postdeploy_closure"
    assert record["status"] == "PASS"
    assert record["phase7_v2_status"] == "CONCLUDED"
    assert record["authorization"]["authorized_source_commit"] == V2_AUTHORIZED_COMMIT
    assert record["origin_integrity"]["managed_js_assets_matching"] == 8
    assert record["external_delivery"]["authorized_commit"] if "authorized_commit" in record["external_delivery"] else V2_AUTHORIZED_COMMIT
    assert record["external_delivery"]["assets_matching"] == 8
    assert record["external_delivery"]["evidence_sha256"] == "b70c0948e95821d85edf30baa05ef38a05e03c49a2db565d9eafd842ebb6c05d"
    assert record["journal_finalization"]["status"] == "APPLIED_EXTERNALLY_HEALTHY"
    assert record["journal_finalization"]["final_health_status"] == "HEALTHY"
    assert record["rollback_performed"] is False
    assert record["closure_conditions"]["temporary_c75_workflows_removed_before_merge"] is True


def test_c75_simulation_proves_stability_and_fail_closed_drift() -> None:
    result = run_simulation()
    for key in (
        "positive_pass",
        "positive_managed_32",
        "positive_dependencies_11",
        "positive_public_js_8",
        "positive_two_http_probes",
        "postdeploy_window_verified",
        "managed_drift_blocked",
        "stale_public_js_blocked",
        "read_only_positive",
        "read_only_all_scenarios",
    ):
        assert result[key] is True
    assert result["production_mutated"] is False


def _external_evidence() -> dict:
    sha = "a" * 64
    assets = []
    for name in sorted(EXPECTED_ASSETS):
        assets.append(
            {
                "asset": name,
                "expected_source_url": f"https://raw.githubusercontent.com/Rafael-Tinelli/sanida-dados-fiscais/{HISTORICAL_AUTHORIZED_COMMIT}/consumers/frontend/{name}",
                "expected_http_status": 200,
                "expected_sha256": sha,
                "public_url": f"https://sanida.com.br/financas/calculadoras/assets/{name}",
                "public_http_status": 200,
                "public_sha256": sha,
                "match": True,
            }
        )
    return {
        "schema_version": "1.0.0",
        "checkpoint": "C7.5",
        "mode": "external_client_public_delivery_validation",
        "status": "PASS",
        "production_mutated": False,
        "authorized_commit": HISTORICAL_AUTHORIZED_COMMIT,
        "assets_expected": 8,
        "assets_matching": 8,
        "assets": assets,
        "block_reasons": [],
    }


def test_c75_external_delivery_validator_accepts_exact_8_of_8() -> None:
    result = validate_external(_external_evidence())
    assert result["status"] == "PASS"
    assert result["public_assets"] == 8
    assert result["external_delivery_verified"] is True
    assert result["production_mutated"] is False


def test_c75_external_delivery_validator_rejects_public_sha_drift() -> None:
    evidence = _external_evidence()
    evidence["assets"][0]["public_sha256"] = "b" * 64
    evidence["assets"][0]["match"] = False
    evidence["assets_matching"] = 7
    evidence["status"] = "BLOCKED"
    evidence["block_reasons"] = ["public_asset_mismatch:" + evidence["assets"][0]["asset"]]
    with pytest.raises(SystemExit):
        validate_external(evidence)


def test_c75_origin_external_and_cache_boundaries_are_explicit() -> None:
    runner = RUNNER.read_text(encoding="utf-8")
    host_validator = HOST_VALIDATOR.read_text(encoding="utf-8")
    external_probe = EXTERNAL_PROBE.read_text(encoding="utf-8")
    external_validator = EXTERNAL_VALIDATOR.read_text(encoding="utf-8")
    initial_record = json.loads(INITIAL_RECORD.read_text(encoding="utf-8"))
    final_record = json.loads(FINAL_RECORD.read_text(encoding="utf-8"))
    doc = DOC.read_text(encoding="utf-8")

    assert "run_phase7_c74_controlled_deploy" not in runner
    assert "os.replace" not in runner
    assert ".unlink(" not in runner
    assert ".write_bytes(" not in runner
    assert 'startswith("public_js_delivery_drift:")' in host_validator
    assert 'external_delivery_required' in host_validator
    assert 'external_client_public_delivery_validation' in external_probe
    assert V2_AUTHORIZED_COMMIT in external_probe
    assert 'public_http_status' in external_validator
    assert initial_record["status"] == "BLOCKED"
    assert initial_record["origin_validation"]["managed_files_matching"] == 32
    assert initial_record["followup_diagnosis"]["production_files_changed"] is False
    assert final_record["edge_cache_remediation_performed"] is True
    assert "max-age=31536000" in doc
    assert "purga seletiva" in doc
    assert "não reimplantou" in doc
