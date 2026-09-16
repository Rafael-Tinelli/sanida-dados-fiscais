from __future__ import annotations

import json
from pathlib import Path

from scripts.simulate_phase7_c74_controlled_deployment import run_simulation

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / "state/phase7-c74-deployment.json"
DOC = ROOT / "docs/phase7-c74-controlled-deployment.md"
DEPLOY = ROOT / "scripts/run_phase7_c74_controlled_deploy.py"
ROLLBACK = ROOT / "scripts/run_phase7_c74_rollback.py"


def test_c74_authorization_is_single_use_and_exactly_bound() -> None:
    state = json.loads(STATE.read_text(encoding="utf-8"))
    assert state["schema_version"] == "1.0.0"
    assert state["checkpoint"] == "C7.4"
    assert state["status"] == "AUTHORIZED_READY_TO_DEPLOY"
    assert state["authorization_id"] == "c74-20260916-a741aa78-843dca3e"
    assert state["single_use_authorization"] is True
    assert state["deployment_authorized"] is True
    assert state["production_deployed"] is False
    assert state["post_deploy_validated"] is False
    candidate = state["candidate"]
    assert candidate["bundle_manifest_sha256"] == "843dca3e843bfe066cee5f1a39754741a9adb47d49fee9749876d4e17f4dedf1"
    assert candidate["c73_remote_evidence_sha256"] == "e4f69319ad2cb1e712c8807138a7aea86a8a2c08c5ecc9bebcd81399b045fec7"
    assert candidate["release_id"] == "fiscal-v1-sha256-a741aa7873950d029a5c6b1c929727267125424013f09c69137b7e80b294153e"
    assert candidate["managed_files"] == 32
    assert candidate["preexisting_dependencies"] == 11
    assert candidate["planned_directory_creations"] == 4


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
    assert "**Status:** EM ANDAMENTO" in doc
    assert "single-use" in doc
    assert "APPLIED_HEALTHY" in doc
