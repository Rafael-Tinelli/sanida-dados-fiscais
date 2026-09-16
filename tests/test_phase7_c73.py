from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import tempfile

import pytest

from scripts.build_phase7_c71_bundle import build_bundle
from scripts.run_phase7_c73_host_preflight import run_preflight
from scripts.simulate_phase7_c73_directory_rollback import run_simulation as run_directory_rollback_simulation
from scripts.simulate_phase7_c73_preflight import run_simulation, seed_host
from scripts.validate_phase7_c73_remote_evidence import validate_remote_evidence

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/phase7-c73-preflight-contract-v1.json"
STORE = ROOT / "releases/fiscal-v1"
DEPLOYMENT_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"
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
    assert spec["schema_version"] == "1.1.0"
    assert spec["checkpoint"] == "C7.3"
    assert spec["status"] == "READY_FOR_REMOTE_READ_ONLY_PREFLIGHT"
    assert spec["production_deployed"] is False
    assert spec["required_counts"] == {"managed_files": 32, "preexisting_dependencies": 11}
    assert spec["target_roots"] == ["site_root", "wordpress_plugin_dir"]
    assert spec["technical_go_no_go"]["NO_GO_on_any_failed_requirement"] is True
    assert spec["technical_go_no_go"]["GO_does_not_execute_deploy"] is True
    assert spec["technical_go_no_go"]["GO_does_not_replace_explicit_deployment_authorization"] is True
    rollback = spec["rollback_preconditions"]
    assert rollback["new_directories_may_be_created_by_deploy"] is True
    assert rollback["new_directory_creation_requires_read_only_preflight_plan"] is True
    assert rollback["created_directory_journal_required"] is True
    assert rollback["rollback_removes_only_directories_created_by_this_deploy_when_empty"] is True
    assert rollback["recursive_directory_delete_forbidden"] is True
    assert rollback["nonempty_created_directory_must_be_preserved_and_escalated"] is True


def test_c73_simulated_preflight_allows_safe_missing_parents_but_blocks_unsafe_parent() -> None:
    result = run_simulation()
    assert result["checkpoint"] == "C7.3"
    assert result["production_deployed"] is False
    assert result["production_mutated"] is False
    assert result["pass_case"]["status"] == "PASS"
    assert result["pass_case"]["technical_go_no_go"] == "GO"
    assert result["pass_case"]["dependencies"] == 11
    assert result["pass_case"]["managed_targets"] == 32
    assert result["pass_case"]["planned_directory_creations"] > 0
    assert result["missing_dependency_blocked"] is True
    assert result["creatable_missing_parent_allowed"] is True
    assert result["uncreatable_parent_blocked"] is True
    assert result["non_mutation_verified"] is True


def test_c73_directory_rollback_is_exact_and_never_recursively_deletes_unmanaged_content() -> None:
    result = run_directory_rollback_simulation()
    assert result["checkpoint"] == "C7.3"
    assert result["production_deployed"] is False
    assert result["production_mutated"] is False
    assert result["created_directories_exercised"] > 0
    assert result["exact_tree_rollback_verified"] is True
    assert result["created_empty_directories_removed"] is True
    assert result["recursive_directory_delete_forbidden_by_implementation"] is True
    assert result["nonempty_created_directory_preserved"] is True
    assert result["unmanaged_content_preserved"] is True


def test_c73_remote_evidence_validator_accepts_safe_directory_creation_plan() -> None:
    with tempfile.TemporaryDirectory(prefix="c73-remote-validator-") as raw:
        tmp = Path(raw)
        bundle_dir = tmp / "bundle"
        bundle = build_bundle(DEPLOYMENT_MANIFEST, bundle_dir)
        roots = seed_host(bundle, tmp / "host", leave_creatable_parents_missing=True)
        evidence = run_preflight(bundle_dir / "bundle-manifest.json", roots["site_root"], roots["wordpress_plugin_dir"])
        evidence_path = tmp / "remote-evidence.json"
        evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        validated = validate_remote_evidence(evidence_path, bundle_dir / "bundle-manifest.json")
        assert validated["status"] == "PASS"
        assert validated["technical_go_no_go"] == "GO"
        assert validated["managed_targets"] == 32
        assert validated["preexisting_dependencies"] == 11
        assert validated["planned_directory_creations"] > 0
        assert validated["production_mutated"] is False
        assert validated["deployment_authorized"] is False

        evidence["status"] = "BLOCKED"
        evidence["technical_go_no_go"] = "NO_GO"
        evidence_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        with pytest.raises(SystemExit):
            validate_remote_evidence(evidence_path, bundle_dir / "bundle-manifest.json")


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
