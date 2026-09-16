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
DOC = ROOT / "docs/phase7-c75-postdeploy-closure.md"


def test_c75_state_is_read_only_and_waits_for_external_client_evidence() -> None:
    state = json.loads(STATE.read_text(encoding="utf-8"))
    assert state["checkpoint"] == "C7.5"
    assert state["schema_version"] == "1.1.0"
    assert state["status"] == "AWAITING_EXTERNAL_CLIENT_DELIVERY_VALIDATION"
    assert state["phase7_status"] == "EM ANDAMENTO"
    assert state["production_deployed"] is True
    assert state["production_mutation_allowed"] is False
    assert state["minimum_postdeploy_window_seconds"] == 600
    assert state["host_origin_observation"]["raw_evidence_sha256"] == "996561d9acaee9473d4bbd2035ce32c8d1bff85c8e9e6eb38b10e2f26a8ff447"
    assert state["delivery_boundary"]["external_client_evidence_required"] is True
    required = state["required_validation"]
    assert required["managed_files_match_c74_bundle"] == 32
    assert required["preexisting_dependencies_match_c73_baseline"] == 11
    assert required["planned_directories_still_safe"] == 4
    assert required["external_client_public_js_matches_authorized_commit"] == 8
    assert required["independent_http_probes_minimum"] == 2
    assert required["production_mutated_by_c75"] is False


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
                "expected_source_url": f"https://raw.githubusercontent.com/Rafael-Tinelli/sanida-dados-fiscais/ccc5a31c3da7c1c93570df0337e553e5a06404ac/consumers/frontend/{name}",
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
        "authorized_commit": "ccc5a31c3da7c1c93570df0337e553e5a06404ac",
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


def test_c75_origin_and_external_delivery_boundaries_are_explicit() -> None:
    runner = RUNNER.read_text(encoding="utf-8")
    host_validator = HOST_VALIDATOR.read_text(encoding="utf-8")
    external_probe = EXTERNAL_PROBE.read_text(encoding="utf-8")
    external_validator = EXTERNAL_VALIDATOR.read_text(encoding="utf-8")
    initial_record = json.loads(INITIAL_RECORD.read_text(encoding="utf-8"))
    doc = DOC.read_text(encoding="utf-8")

    for marker in (
        "post_deploy_read_only_validation",
        "postdeploy_window_too_short",
        "managed_file_drift:",
        "dependency_drift:",
        "c74_temporary_files_left_behind",
        "canonical_artifact_sha_mismatch",
        "fiscal_health_not_healthy",
        "legacy_folha_not_410",
        "public_js_delivery_drift:",
        '"production_mutated": False',
    ):
        assert marker in runner
    assert "run_phase7_c74_controlled_deploy" not in runner
    assert "os.replace" not in runner
    assert ".unlink(" not in runner
    assert ".write_bytes(" not in runner

    assert 'startswith("public_js_delivery_drift:")' in host_validator
    assert 'external_delivery_required' in host_validator
    assert 'external_client_public_delivery_validation' in external_probe
    assert 'ccc5a31c3da7c1c93570df0337e553e5a06404ac' in external_probe
    assert 'raw.githubusercontent.com' in external_probe
    assert 'public_http_status' in external_validator
    assert initial_record["status"] == "BLOCKED"
    assert initial_record["origin_validation"]["managed_files_matching"] == 32
    assert initial_record["followup_diagnosis"]["production_files_changed"] is False
    assert "cliente externo" in doc
    assert "não deve ser reimplantada" in doc
