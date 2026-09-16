#!/usr/bin/env python3
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.publication_v1 import FiscalReleaseStore
from scripts.simulate_phase7_c73_directory_rollback import run_simulation as run_directory_rollback_simulation
from scripts.simulate_phase7_c73_preflight import run_simulation

STORE = ROOT / "releases/fiscal-v1"
SPEC = ROOT / "docs/phase7-c73-preflight-contract-v1.json"
RUNBOOK = ROOT / "docs/phase7-c73-production-runbook.md"
READINESS = ROOT / "state/phase7-c73-readiness.json"
REMOTE_EVIDENCE_RECORD = ROOT / "docs/phase7-c73-remote-preflight-validation-record.json"
REMOTE_VALIDATOR = ROOT / "scripts/validate_phase7_c73_remote_evidence.py"
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
DIRECTORY_ROLLBACK_SIMULATION = ROOT / "scripts/simulate_phase7_c73_directory_rollback.py"

EXPECTED_REMOTE_EVIDENCE_SHA256 = "e4f69319ad2cb1e712c8807138a7aea86a8a2c08c5ecc9bebcd81399b045fec7"
EXPECTED_REMOTE_BUNDLE_MANIFEST_SHA256 = "843dca3e843bfe066cee5f1a39754741a9adb47d49fee9749876d4e17f4dedf1"
EXPECTED_REMOTE_SOURCE_COMMIT = "8cdbba12da8205381fe942a53e1be45a82596917"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C7.3 gate failed: {message}")


def main() -> int:
    for path in (
        SPEC,
        RUNBOOK,
        READINESS,
        REMOTE_EVIDENCE_RECORD,
        REMOTE_VALIDATOR,
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
        DIRECTORY_ROLLBACK_SIMULATION,
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
    require(spec.get("schema_version") == "1.1.0", "preflight spec schema drift")
    require(spec.get("checkpoint") == "C7.3", "preflight spec checkpoint drift")
    require(spec.get("status") == "READY_FOR_REMOTE_READ_ONLY_PREFLIGHT", "preflight spec readiness drift")
    require(spec.get("production_deployed") is False, "preflight spec claims deployment")
    require(spec.get("required_counts") == {"managed_files": 32, "preexisting_dependencies": 11}, "preflight count contract drift")
    require(spec.get("technical_go_no_go", {}).get("NO_GO_on_any_failed_requirement") is True, "preflight is not fail closed")
    require(spec.get("technical_go_no_go", {}).get("GO_does_not_execute_deploy") is True, "technical GO crosses deployment boundary")
    require(spec.get("technical_go_no_go", {}).get("GO_does_not_replace_explicit_deployment_authorization") is True, "technical GO replaces explicit authorization")
    rollback = spec.get("rollback_preconditions", {})
    require(rollback.get("new_directories_may_be_created_by_deploy") is True, "managed directory creation not explicitly permitted")
    require(rollback.get("new_directory_creation_requires_read_only_preflight_plan") is True, "directory creation lacks preflight plan requirement")
    require(rollback.get("created_directory_journal_required") is True, "created-directory journal not required")
    require(rollback.get("rollback_removes_only_directories_created_by_this_deploy_when_empty") is True, "empty-only directory rollback boundary missing")
    require(rollback.get("recursive_directory_delete_forbidden") is True, "recursive directory delete is not forbidden")
    require(rollback.get("nonempty_created_directory_must_be_preserved_and_escalated") is True, "nonempty directory preservation boundary missing")

    readiness = json.loads(READINESS.read_text(encoding="utf-8"))
    require(readiness.get("schema_version") == "1.1.0", "readiness schema drift")
    require(readiness.get("checkpoint") == "C7.3", "readiness checkpoint drift")
    require(readiness.get("status") == "CONCLUÍDO", "C7.3 is not formally closed")
    require(readiness.get("repository_readiness") == "READY", "repository-side C7.3 readiness not READY")
    require(readiness.get("remote_preflight") == "PASS_VALIDATED", "remote preflight not recorded as validated PASS")
    require(readiness.get("technical_go_no_go") == "GO", "C7.3 technical go/no-go is not GO")
    require(readiness.get("deployment_authorized") is False, "C7.3 incorrectly authorizes deployment")
    require(readiness.get("production_deployed") is False, "C7.3 incorrectly claims deployment")
    require(readiness.get("production_mutated_by_c73") is False, "C7.3 readiness claims production mutation")
    require(readiness.get("remote_evidence_record") == REMOTE_EVIDENCE_RECORD.relative_to(ROOT).as_posix(), "remote evidence record path drift")
    require(readiness.get("next_checkpoint") == "C7.4_CONTROLLED_DEPLOYMENT_AUTHORIZATION", "next checkpoint drift")

    evidence_record = json.loads(REMOTE_EVIDENCE_RECORD.read_text(encoding="utf-8"))
    require(evidence_record.get("schema_version") == "1.0.0", "remote validation record schema drift")
    require(evidence_record.get("checkpoint") == "C7.3", "remote validation record checkpoint drift")
    require(evidence_record.get("record_type") == "remote_preflight_validation_record", "remote validation record type drift")
    require(evidence_record.get("source_commit") == EXPECTED_REMOTE_SOURCE_COMMIT, "remote source commit drift")
    host_evidence = evidence_record.get("host_evidence") or {}
    bundle_record = evidence_record.get("bundle_manifest") or {}
    require(host_evidence.get("sha256") == EXPECTED_REMOTE_EVIDENCE_SHA256, "remote evidence SHA drift")
    require(bundle_record.get("sha256") == EXPECTED_REMOTE_BUNDLE_MANIFEST_SHA256, "remote bundle manifest SHA drift")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", str(host_evidence.get("sha256") or ""))), "remote evidence SHA malformed")
    require(bool(re.fullmatch(r"[0-9a-f]{64}", str(bundle_record.get("sha256") or ""))), "remote bundle SHA malformed")
    require(evidence_record.get("release_id") == release.release_id, "remote preflight release binding drift")
    preflight_record = evidence_record.get("preflight") or {}
    require(preflight_record.get("status") == "PASS", "recorded remote preflight not PASS")
    require(preflight_record.get("technical_go_no_go") == "GO", "recorded remote preflight not GO")
    require(preflight_record.get("production_deployed") is False, "recorded preflight claims deployment")
    require(preflight_record.get("production_mutated") is False, "recorded preflight claims mutation")
    require(preflight_record.get("deployment_authorized") is False, "recorded preflight claims authorization")
    require(preflight_record.get("managed_targets") == 32, "recorded managed target count drift")
    require(preflight_record.get("preexisting_dependencies") == 11, "recorded dependency count drift")
    require(preflight_record.get("existing_managed_targets") == 10, "recorded existing target count drift")
    require(preflight_record.get("absent_managed_targets") == 22, "recorded absent target count drift")
    require(preflight_record.get("all_managed_parent_directories_ready") is True, "recorded parent readiness failed")
    require(preflight_record.get("planned_directory_creations") == 4, "recorded planned directory count drift")
    require(preflight_record.get("block_reasons") == [], "recorded remote preflight has block reasons")
    formal_validator = evidence_record.get("formal_validator") or {}
    require(formal_validator.get("schema_version") == "1.1.0", "formal validator schema drift")
    require(formal_validator.get("status") == "PASS", "formal remote validation not PASS")
    require(formal_validator.get("technical_go_no_go") == "GO", "formal remote validation not GO")
    require(formal_validator.get("managed_targets") == 32, "formal validator managed count drift")
    require(formal_validator.get("preexisting_dependencies") == 11, "formal validator dependency count drift")
    require(formal_validator.get("planned_directory_creations") == 4, "formal validator directory plan drift")
    require(formal_validator.get("production_mutated") is False, "formal validator claims mutation")
    require(formal_validator.get("deployment_authorized") is False, "formal validator claims authorization")
    require(formal_validator.get("bundle_manifest_sha256") == EXPECTED_REMOTE_BUNDLE_MANIFEST_SHA256, "formal validator bundle SHA drift")

    host_preflight_text = HOST_PREFLIGHT.read_text(encoding="utf-8")
    for marker in (
        '"mode": "read_only_host_preflight"',
        '"technical_go_no_go": "GO" if status == "PASS" else "NO_GO"',
        '"production_mutated": False',
        '"deployment_authorized": False',
        'managed_parent_uncreatable',
        'managed_parent_not_directory',
        'planned_directory_creations',
        'dependency_missing:',
        'managed_target_symlink_not_allowed:',
        'insufficient_or_unknown_free_space:',
        'php_cli_unavailable',
    ):
        require(marker in host_preflight_text, f"host preflight missing fail-closed marker: {marker}")
    for forbidden in ("unlink(", "mkdir(", "chmod(", "write_bytes("):
        require(forbidden not in host_preflight_text, f"host preflight contains target mutation primitive: {forbidden}")

    remote_validator_text = REMOTE_VALIDATOR.read_text(encoding="utf-8")
    for marker in (
        'evidence.get("status") == "PASS"',
        'evidence.get("technical_go_no_go") == "GO"',
        'evidence.get("production_mutated") is False',
        'len(dependencies) == 11',
        'len(managed) == 32',
        'planned directory creation journal drift',
        'all_managed_parent_directories_ready',
        'bundle manifest SHA mismatch',
    ):
        require(marker in remote_validator_text, f"remote evidence validator missing marker: {marker}")

    simulation = run_simulation()
    require(simulation.get("production_deployed") is False, "simulation claims production deployment")
    require(simulation.get("production_mutated") is False, "simulation claims production mutation")
    require(simulation.get("non_mutation_verified") is True, "preflight non-mutation not proven")
    require(simulation.get("missing_dependency_blocked") is True, "missing dependency did not block")
    require(simulation.get("creatable_missing_parent_allowed") is True, "safe missing parent was not allowed")
    require(simulation.get("uncreatable_parent_blocked") is True, "unsafe missing parent did not block")
    require(simulation.get("pass_case", {}).get("status") == "PASS", "positive preflight case failed")
    require(simulation.get("pass_case", {}).get("technical_go_no_go") == "GO", "positive preflight case is not GO")
    require(simulation.get("pass_case", {}).get("dependencies") == 11, "positive preflight dependency count drift")
    require(simulation.get("pass_case", {}).get("managed_targets") == 32, "positive preflight managed count drift")
    require(simulation.get("pass_case", {}).get("planned_directory_creations", 0) > 0, "positive preflight did not exercise directory planning")

    directory_rollback = run_directory_rollback_simulation()
    for key in (
        "exact_tree_rollback_verified",
        "created_empty_directories_removed",
        "recursive_directory_delete_forbidden_by_implementation",
        "nonempty_created_directory_preserved",
        "unmanaged_content_preserved",
    ):
        require(directory_rollback.get(key) is True, f"directory rollback simulation did not prove {key}")
    require(directory_rollback.get("created_directories_exercised", 0) > 0, "directory rollback did not exercise new directories")
    require(directory_rollback.get("production_mutated") is False, "directory rollback simulation claims production mutation")

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
        "**Status:** CONCLUÍDO",
        "production_deployed=false",
        "deployment_authorized=false",
        "11/11 dependências",
        "32/32 destinos",
        "planned_directory_creations=4",
        EXPECTED_REMOTE_EVIDENCE_SHA256,
        EXPECTED_REMOTE_BUNDLE_MANIFEST_SHA256,
        "`rmdir`",
        "deleção recursiva",
        "/wp-json/sfa/v1/fiscal-health",
        "blocked_known_successor",
        "C7.4 — autorização e implantação controlada",
    ):
        require(marker in runbook, f"runbook missing C7.3 closure marker: {marker}")

    readme = README.read_text(encoding="utf-8")
    for marker in (
        "C7.3 — runbook, observabilidade e pré-flight de produção",
        "C7.1–C7.3 estão concluídos",
        "C7.4 — autorização e implantação controlada",
        "production_deployed=false",
        "### Fase 7 — Fechamento e operação evergreen",
        "**Status: EM ANDAMENTO**",
    ):
        require(marker in readme, f"README lost C7.3 closure boundary: {marker}")

    ci = CI.read_text(encoding="utf-8")
    require("python scripts/simulate_phase7_c73_preflight.py" in ci, "Remake CI does not run C7.3 preflight simulation")
    require("python scripts/validate_phase7_c73_gate.py" in ci, "Remake CI does not execute C7.3 closure gate")
    require("c73-preflight-simulation-${{ github.sha }}" in ci, "Remake CI does not preserve C7.3 simulation evidence")

    print(
        "Phase 7 C7.3 closure gate: PASS "
        f"(release={release.release_id}, managed=32, dependencies=11, remote_preflight=PASS_VALIDATED, planned_directories=4, health=verified, directory_rollback=verified, technical_go_no_go=GO, deployment_authorized=false, production_deployed=false)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
