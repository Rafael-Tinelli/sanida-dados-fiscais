from __future__ import annotations

import json
from pathlib import Path

from scripts.simulate_phase7_c74_controlled_deployment import run_simulation

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "state/phase7-c74-deployment.json"
AUTH_RECORD = ROOT / "docs/phase7-c74-authorization-record.json"
PROD_RECORD = ROOT / "docs/phase7-c74-production-deployment-record.json"
DOC = ROOT / "docs/phase7-c74-controlled-deployment.md"
DEPLOY = ROOT / "scripts/run_phase7_c74_controlled_deploy.py"
ROLLBACK = ROOT / "scripts/run_phase7_c74_rollback.py"
CI = ROOT / ".github/workflows/remake-ci.yml"


def test_c74_authorization_history_remains_exactly_bound() -> None:
    auth = json.loads(AUTH_RECORD.read_text(encoding="utf-8"))
    assert auth["schema_version"] == "1.0.0"
    assert auth["checkpoint"] == "C7.4"
    assert auth["record_type"] == "single_use_deployment_authorization"
    assert auth["status"] == "AUTHORIZED_READY_TO_DEPLOY"
    assert auth["authorization_id"] == "c74-20260916-a741aa78-843dca3e"
    assert auth["single_use_authorization"] is True
    assert auth["deployment_authorized"] is True
    assert auth["production_deployed"] is False
    candidate = auth["candidate"]
    assert candidate["bundle_manifest_sha256"] == "843dca3e843bfe066cee5f1a39754741a9adb47d49fee9749876d4e17f4dedf1"
    assert candidate["c73_remote_evidence_sha256"] == "e4f69319ad2cb1e712c8807138a7aea86a8a2c08c5ecc9bebcd81399b045fec7"
    assert candidate["release_id"] == "fiscal-v1-sha256-a741aa7873950d029a5c6b1c929727267125424013f09c69137b7e80b294153e"
    assert candidate["managed_files"] == 32
    assert candidate["preexisting_dependencies"] == 11
    assert candidate["planned_directory_creations"] == 4


def test_c74_state_is_concluded_after_single_authorized_deploy() -> None:
    state = json.loads(STATE.read_text(encoding="utf-8"))
    assert state["schema_version"] == "1.1.0"
    assert state["checkpoint"] == "C7.4"
    assert state["status"] == "CONCLUÍDO"
    assert state["authorization_id"] == "c74-20260916-a741aa78-843dca3e"
    assert state["single_use_authorization"] is True
    assert state["authorization_consumed"] is True
    assert state["deployment_authorized"] is True
    assert state["production_deployed"] is True
    assert state["post_deploy_validated"] is True
    assert state["rollback_performed"] is False
    assert state["closure_condition_met"] is True
    result = state["deployment_result"]
    assert result["status"] == "APPLIED_HEALTHY"
    assert result["managed_files_applied"] == 32
    assert result["created_directories"] == 4
    assert result["deployment_state_sha256"] == "9fc57e626dd8ab3a7666da18c7f1397f35729987a3be62ac83ed6ed7ca0bcc4c"
    health = state["health"]
    assert health["fiscal_health"] == "healthy"
    assert health["fiscal_health_http_status"] == 200
    assert health["fiscal_release_http_status"] == 200
    assert health["fiscal_release_matches_candidate"] is True
    assert health["legacy_folha_endpoint_http_status"] == 410
    assert health["calculator_http_status"] == {"H26": 200, "H27": 200, "H28": 200, "H29": 200}


def test_c74_production_record_matches_state_and_authorization() -> None:
    state = json.loads(STATE.read_text(encoding="utf-8"))
    auth = json.loads(AUTH_RECORD.read_text(encoding="utf-8"))
    record = json.loads(PROD_RECORD.read_text(encoding="utf-8"))
    assert record["schema_version"] == "1.0.0"
    assert record["checkpoint"] == "C7.4"
    assert record["record_type"] == "production_deployment_validation_record"
    assert record["recorded_from"] == "operator_terminal_output"
    assert record["authorization_id"] == auth["authorization_id"] == state["authorization_id"]
    assert record["status"] == "APPLIED_HEALTHY"
    assert record["production_deployed"] is True
    assert record["post_deploy_validated"] is True
    assert record["rollback_performed"] is False
    assert record["release_id"] == auth["candidate"]["release_id"]
    assert record["bundle_manifest_sha256"] == auth["candidate"]["bundle_manifest_sha256"]
    assert record["c73_remote_evidence_sha256"] == auth["candidate"]["c73_remote_evidence_sha256"]
    assert record["deployment_state"]["sha256"] == state["deployment_result"]["deployment_state_sha256"]
    assert record["apply"]["managed_files_applied"] == 32
    assert record["apply"]["planned_directories_created"] == 4
    assert record["health"]["legacy_folha"]["http_status"] == 410
    assert {k: v["http_status"] for k, v in record["health"]["calculators"].items()} == {
        "H26": 200,
        "H27": 200,
        "H28": 200,
        "H29": 200,
    }


def test_c74_simulation_proves_apply_and_two_rollback_paths() -> None:
    result = run_simulation()
    assert result["checkpoint"] == "C7.4"
    assert result["success_apply_verified"] is True
    assert result["success_post_deploy_health_verified"] is True
    assert result["single_use_journal_verified"] is True
    assert result["partial_apply_rollback_exact"] is True
    assert result["post_deploy_health_failure_rollback_exact"] is True
    assert result["managed_files"] == 32
    assert result["preexisting_dependencies"] == 11
    assert result["production_mutated"] is False


def test_c74_deploy_has_fail_closed_and_health_boundaries() -> None:
    deploy = DEPLOY.read_text(encoding="utf-8")
    rollback = ROLLBACK.read_text(encoding="utf-8")
    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "single-use deployment journal already exists",
        "managed target state drift since C7.3",
        "dependency state drift since C7.3",
        "bundle manifest SHA is not the authorized candidate",
        "directory creation was not authorized by preflight",
        "APPLIED_AWAITING_HEALTH",
        "APPLIED_HEALTHY",
        "ROLLED_BACK",
        "/blog/wp-json/sfa/v1/fiscal-health",
        "/blog/wp-json/sfa/v1/fiscal-release",
        "/blog/wp-json/sfa/v1/folha",
        "expected 410",
        "Fatal error",
        "Parse error",
        "os.replace",
    ):
        assert marker in deploy
    assert "rmtree(" not in deploy + rollback
    assert "rm -rf" not in deploy + rollback
    assert "MANUAL_ROLLBACK_COMPLETE" in rollback
    assert "**Status:** CONCLUÍDO" in doc
    assert "single-use" in doc
    assert "APPLIED_HEALTHY" in doc
    assert "9fc57e626dd8ab3a7666da18c7f1397f35729987a3be62ac83ed6ed7ca0bcc4c" in doc


def test_c74_ci_cannot_mask_evidence_pipeline_failures() -> None:
    ci = CI.read_text(encoding="utf-8")
    assert ci.count("set -o pipefail") >= 3
    assert "PYTHONPATH=. python scripts/simulate_phase7_c73_preflight.py | tee" in ci
    assert "PYTHONPATH=. python scripts/simulate_phase7_c74_controlled_deployment.py | tee" in ci
    assert "PYTHONPATH=. python scripts/validate_phase7_c74_gate.py" in ci
