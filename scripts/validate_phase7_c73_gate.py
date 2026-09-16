#!/usr/bin/env python3
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.publication_v1 import FiscalReleaseStore
from scripts.simulate_phase7_c73_preflight import run_simulation

STORE = ROOT / "releases/fiscal-v1"
SPEC = ROOT / "docs/phase7-c73-preflight-contract-v1.json"
RUNBOOK = ROOT / "docs/phase7-c73-production-runbook.md"
README = ROOT / "README.md"
CI = ROOT / ".github/workflows/remake-ci.yml"
DEPLOYMENT_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"
PLUGIN = ROOT / "consumers/wordpress/sanida-fiscais-auto.php"
ADMIN = ROOT / "consumers/wordpress/includes/trait-sanida-admin-debug.php"
NETWORK = ROOT / "consumers/wordpress/includes/trait-sanida-fiscal-network.php"
CONTRACT = ROOT / "consumers/wordpress/includes/trait-sanida-fiscal-contract.php"
HEALTH_HARNESS = ROOT / "tests/php/phase7_c73_health.php"
HOST_PREFLIGHT = ROOT / "scripts/run_phase7_c73_host_preflight.py"
SIMULATION = ROOT / "scripts/simulate_phase7_c73_preflight.py"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C7.3 gate failed: {message}")


def main() -> int:
    for path in (
        SPEC,
        RUNBOOK,
        README,
        CI,
        DEPLOYMENT_MANIFEST,
        PLUGIN,
        ADMIN,
        NETWORK,
        CONTRACT,
        HEALTH_HARNESS,
        HOST_PREFLIGHT,
        SIMULATION,
    ):
        require(path.is_file(), f"required C7.3 file missing: {path.relative_to(ROOT)}")

    release = FiscalReleaseStore(STORE).load_current()
    require(release is not None, "current fiscal release unavailable")
    assert release is not None
    require(release.status.value == "PUBLISHED", "current release is not PUBLISHED")
    require(release.schema_version == "1.2.0", "current schema is not 1.2.0")
    require(release.consumer_compatibility.contract_api_version == "1.2.0", "current contract API is not 1.2.0")
    require(len(release.rules) == 32, "current release is not 32/32")
    require(release.release_id == release.expected_release_id(), "current release_id is not canonical")

    pointer_path = STORE / "current.json"
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    require(pointer["release_id"] == release.release_id, "current.json release drift")
    artifact = STORE / pointer["artifact"]
    require(artifact.is_file(), "current immutable artifact missing")
    require(sha256(artifact.read_bytes()).hexdigest() == pointer["artifact_sha256"], "current immutable artifact SHA mismatch")

    deployment = json.loads(DEPLOYMENT_MANIFEST.read_text(encoding="utf-8"))
    require(deployment.get("checkpoint") == "C7.1", "deployment manifest checkpoint drift")
    require(deployment.get("production_deployed") is False, "C7.3 must remain pre-deploy")
    require(deployment.get("status") == "ready_not_deployed", "deployment manifest status drift")
    require(len(deployment.get("managed_files") or []) == 32, "managed deployment count drift")
    require(len(deployment.get("preexisting_dependencies") or []) == 11, "preexisting dependency count drift")

    spec = json.loads(SPEC.read_text(encoding="utf-8"))
    require(spec.get("schema_version") == "1.0.0", "preflight spec schema drift")
    require(spec.get("checkpoint") == "C7.3", "preflight spec checkpoint drift")
    require(spec.get("status") == "READY_FOR_REMOTE_READ_ONLY_PREFLIGHT", "preflight spec readiness drift")
    require(spec.get("production_deployed") is False, "preflight spec claims deployment")
    require(spec.get("required_counts") == {"managed_files": 32, "preexisting_dependencies": 11}, "preflight count contract drift")
    require(spec.get("technical_go_no_go", {}).get("NO_GO_on_any_failed_requirement") is True, "preflight is not fail closed")
    require(spec.get("technical_go_no_go", {}).get("GO_does_not_execute_deploy") is True, "technical GO crosses deployment boundary")
    require(spec.get("technical_go_no_go", {}).get("GO_does_not_replace_explicit_deployment_authorization") is True, "technical GO replaces explicit authorization")
    require(spec.get("rollback_preconditions", {}).get("new_directories_may_not_be_created_by_deploy") is True, "new directory rollback boundary missing")

    host_preflight_text = HOST_PREFLIGHT.read_text(encoding="utf-8")
    for marker in (
        '"mode": "read_only_host_preflight"',
        '"technical_go_no_go": "GO" if status == "PASS" else "NO_GO"',
        '"production_mutated": False',
        '"deployment_authorized": False',
        'managed_parent_missing:',
        'dependency_missing:',
        'managed_target_symlink_not_allowed:',
        'insufficient_or_unknown_free_space:',
        'php_cli_unavailable',
    ):
        require(marker in host_preflight_text, f"host preflight missing fail-closed marker: {marker}")
    for forbidden in ("unlink(", "mkdir(", "chmod(", "write_bytes("):
        require(forbidden not in host_preflight_text, f"host preflight contains target mutation primitive: {forbidden}")

    simulation = run_simulation()
    require(simulation.get("production_deployed") is False, "simulation claims production deployment")
    require(simulation.get("production_mutated") is False, "simulation claims production mutation")
    require(simulation.get("non_mutation_verified") is True, "preflight non-mutation not proven")
    require(simulation.get("missing_dependency_blocked") is True, "missing dependency did not block")
    require(simulation.get("missing_parent_blocked") is True, "missing managed parent did not block")
    require(simulation.get("pass_case", {}).get("status") == "PASS", "positive preflight case failed")
    require(simulation.get("pass_case", {}).get("technical_go_no_go") == "GO", "positive preflight case is not GO")
    require(simulation.get("pass_case", {}).get("dependencies") == 11, "positive preflight dependency count drift")
    require(simulation.get("pass_case", {}).get("managed_targets") == 32, "positive preflight managed count drift")

    plugin = PLUGIN.read_text(encoding="utf-8")
    admin = ADMIN.read_text(encoding="utf-8")
    for marker in (
        "register_fiscal_health_route",
        "add_action('rest_api_init',  [$this, 'register_fiscal_health_route'])",
    ):
        require(marker in plugin, f"plugin missing C7.3 observability marker: {marker}")
    for marker in (
        "register_rest_route('sfa/v1', '/fiscal-health'",
        "fiscal_health_snapshot",
        "'healthy'",
        "'degraded_last_good'",
        "'blocked_known_successor'",
        "'unavailable'",
        "'status' => 503",
        "Cache-Control', 'no-store'",
        "X-Sanida-Fiscal-Status",
        "X-Sanida-Fiscal-Release",
        "'fiscais_health' => $this->fiscal_health_snapshot($package)",
    ):
        require(marker in admin, f"operational health marker missing: {marker}")
    require("body_sample" not in admin, "public health implementation references raw body sample")

    php = shutil.which("php")
    require(php is not None, "php is required for C7.3 health E2E")
    completed = subprocess.run(
        [
            php,
            str(HEALTH_HARNESS),
            str(CONTRACT),
            str(NETWORK),
            str(ADMIN),
            str(pointer_path),
            str(artifact),
            release.release_id,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    require(completed.returncode == 0, f"health harness failed: {completed.stderr or completed.stdout}")
    health = json.loads(completed.stdout)
    require(health["healthy"]["data"]["status"] == "healthy", "healthy state drift")
    require(health["healthy"]["data"]["release_id"] == release.release_id, "healthy release_id drift")
    require(health["healthy"]["headers"]["Cache-Control"] == "no-store", "health response is cacheable")
    require(health["degraded"]["data"]["status"] == "degraded_last_good", "degraded last-good state drift")
    require(health["blocked"]["status"] == 503, "known-successor health did not return 503")
    require(health["blocked"]["health"]["status"] == "blocked_known_successor", "known-successor health status drift")
    require(health["unavailable"]["status"] == 503, "unavailable health did not return 503")
    require(health["unavailable"]["health"]["status"] == "unavailable", "unavailable health status drift")

    runbook = RUNBOOK.read_text(encoding="utf-8")
    for marker in (
        "**Status:** EM REVISÃO",
        "production_deployed=false",
        "11 dependências",
        "32 destinos",
        "technical_go_no_go",
        "NO_GO",
        "backup exato",
        "/wp-json/sfa/v1/fiscal-health",
        "blocked_known_successor",
        "C7.3 só muda para `CONCLUÍDO`",
        "evidência remota read-only",
    ):
        require(marker in runbook, f"runbook missing C7.3 marker: {marker}")

    readme = README.read_text(encoding="utf-8")
    for marker in (
        "C7.3 — runbook, observabilidade e pré-flight de produção",
        "EM REVISÃO",
        "remote read-only",
        "production_deployed=false",
    ):
        require(marker in readme, f"README missing C7.3 state marker: {marker}")

    ci = CI.read_text(encoding="utf-8")
    require("python scripts/simulate_phase7_c73_preflight.py" in ci, "Remake CI does not run C7.3 preflight simulation")
    require("python scripts/validate_phase7_c73_gate.py" in ci, "Remake CI does not execute C7.3 readiness gate")
    require("c73-preflight-simulation-${{ github.sha }}" in ci, "Remake CI does not preserve C7.3 simulation evidence")

    print(
        "Phase 7 C7.3 readiness gate: PASS "
        f"(release={release.release_id}, managed=32, dependencies=11, health=verified, preflight=read-only, remote_preflight=REQUIRED, deployment_authorized=false, production_deployed=false)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
