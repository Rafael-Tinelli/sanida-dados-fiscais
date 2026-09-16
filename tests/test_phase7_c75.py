from __future__ import annotations

import json
from pathlib import Path

from scripts.simulate_phase7_c75_postdeploy_validation import run_simulation

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "state/phase7-c75-closure.json"
RUNNER = ROOT / "scripts/run_phase7_c75_postdeploy_validation.py"
VALIDATOR = ROOT / "scripts/validate_phase7_c75_remote_evidence.py"
DOC = ROOT / "docs/phase7-c75-postdeploy-closure.md"


def test_c75_state_is_read_only_and_waits_for_remote_evidence() -> None:
    state = json.loads(STATE.read_text(encoding="utf-8"))
    assert state["checkpoint"] == "C7.5"
    assert state["status"] == "AWAITING_REMOTE_POSTDEPLOY_VALIDATION"
    assert state["phase7_status"] == "EM ANDAMENTO"
    assert state["production_deployed"] is True
    assert state["production_mutation_allowed"] is False
    assert state["minimum_postdeploy_window_seconds"] == 600
    required = state["required_validation"]
    assert required["managed_files_match_c74_bundle"] == 32
    assert required["preexisting_dependencies_match_c73_baseline"] == 11
    assert required["planned_directories_still_safe"] == 4
    assert required["public_js_matches_bundle"] == 8
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


def test_c75_runner_has_read_only_and_public_delivery_boundaries() -> None:
    runner = RUNNER.read_text(encoding="utf-8")
    validator = VALIDATOR.read_text(encoding="utf-8")
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
        "production_mutated\": False",
    ):
        assert marker in runner
    assert "run_phase7_c74_controlled_deploy" not in runner
    assert "os.replace" not in runner
    assert ".unlink(" not in runner
    assert ".write_bytes(" not in runner
    assert "phase7_close_recommended" in validator
    assert "public JS" in doc or "JavaScript" in doc
    assert "não deve ser reimplantada" in doc
