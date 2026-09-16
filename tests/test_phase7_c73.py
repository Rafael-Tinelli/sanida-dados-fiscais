from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

from scripts.simulate_phase7_c73_preflight import run_simulation

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/phase7-c73-preflight-contract-v1.json"
STORE = ROOT / "releases/fiscal-v1"
PLUGIN = ROOT / "consumers/wordpress/sanida-fiscais-auto.php"
ADMIN = ROOT / "consumers/wordpress/includes/trait-sanida-admin-debug.php"
NETWORK = ROOT / "consumers/wordpress/includes/trait-sanida-fiscal-network.php"
CONTRACT = ROOT / "consumers/wordpress/includes/trait-sanida-fiscal-contract.php"
HEALTH_HARNESS = ROOT / "tests/php/phase7_c73_health.php"


def current_release_fixture() -> tuple[dict, Path]:
    pointer = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / pointer["artifact"]
    return pointer, artifact


def test_c73_preflight_contract_is_fail_closed_and_predeploy() -> None:
    spec = json.loads(SPEC.read_text(encoding="utf-8"))
    assert spec["schema_version"] == "1.0.0"
    assert spec["checkpoint"] == "C7.3"
    assert spec["status"] == "READY_FOR_REMOTE_READ_ONLY_PREFLIGHT"
    assert spec["production_deployed"] is False
    assert spec["required_counts"] == {"managed_files": 32, "preexisting_dependencies": 11}
    assert spec["target_roots"] == ["site_root", "wordpress_plugin_dir"]
    assert spec["technical_go_no_go"]["NO_GO_on_any_failed_requirement"] is True
    assert spec["technical_go_no_go"]["GO_does_not_execute_deploy"] is True
    assert spec["technical_go_no_go"]["GO_does_not_replace_explicit_deployment_authorization"] is True
    assert spec["rollback_preconditions"]["new_directories_may_not_be_created_by_deploy"] is True


def test_c73_simulated_preflight_proves_positive_negative_and_non_mutation() -> None:
    result = run_simulation()
    assert result["checkpoint"] == "C7.3"
    assert result["production_deployed"] is False
    assert result["production_mutated"] is False
    assert result["pass_case"]["status"] == "PASS"
    assert result["pass_case"]["technical_go_no_go"] == "GO"
    assert result["pass_case"]["dependencies"] == 11
    assert result["pass_case"]["managed_targets"] == 32
    assert result["missing_dependency_blocked"] is True
    assert result["missing_parent_blocked"] is True
    assert result["non_mutation_verified"] is True


def test_c73_observability_route_is_registered_without_new_fiscal_authority() -> None:
    plugin = PLUGIN.read_text(encoding="utf-8")
    admin = ADMIN.read_text(encoding="utf-8")
    network = NETWORK.read_text(encoding="utf-8")
    assert "register_fiscal_health_route" in plugin
    assert "'/fiscal-health'" in admin
    assert "fiscal_health_snapshot" in admin
    assert "'healthy'" in admin
    assert "'degraded_last_good'" in admin
    assert "'blocked_known_successor'" in admin
    assert "'unavailable'" in admin
    assert "Cache-Control', 'no-store'" in admin
    assert "X-Sanida-Fiscal-Status" in admin
    assert "X-Sanida-Fiscal-Release" in admin
    assert "body_sample" not in admin
    assert "fiscais_health" in admin
    assert "dados_fiscais.json" not in plugin + admin + network


def test_c73_health_endpoint_states_against_real_current_release() -> None:
    php = shutil.which("php")
    if php is None:
        return

    pointer, artifact = current_release_fixture()
    completed = subprocess.run(
        [
            php,
            str(HEALTH_HARNESS),
            str(CONTRACT),
            str(NETWORK),
            str(ADMIN),
            str(STORE / "current.json"),
            str(artifact),
            pointer["release_id"],
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    payload = json.loads(completed.stdout)

    healthy = payload["healthy"]
    assert healthy["kind"] == "response"
    assert healthy["data"]["status"] == "healthy"
    assert healthy["data"]["ready"] is True
    assert healthy["data"]["release_id"] == pointer["release_id"]
    assert healthy["headers"]["Cache-Control"] == "no-store"
    assert healthy["headers"]["X-Sanida-Fiscal-Status"] == "healthy"
    assert healthy["headers"]["X-Sanida-Fiscal-Release"] == pointer["release_id"]

    degraded = payload["degraded"]
    assert degraded["kind"] == "response"
    assert degraded["data"]["status"] == "degraded_last_good"
    assert degraded["data"]["ready"] is True
    assert degraded["data"]["release_id"] == pointer["release_id"]
    assert degraded["headers"]["X-Sanida-Fiscal-Status"] == "degraded_last_good"

    blocked = payload["blocked"]
    assert blocked["kind"] == "error"
    assert blocked["code"] == "sfa_fiscal_health_unavailable"
    assert blocked["status"] == 503
    assert blocked["health"]["status"] == "blocked_known_successor"
    assert blocked["health"]["ready"] is False
    assert blocked["health"]["release_id"] is None

    unavailable = payload["unavailable"]
    assert unavailable["kind"] == "error"
    assert unavailable["code"] == "sfa_fiscal_health_unavailable"
    assert unavailable["status"] == 503
    assert unavailable["health"]["status"] == "unavailable"
    assert unavailable["health"]["ready"] is False
