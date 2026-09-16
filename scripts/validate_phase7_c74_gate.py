#!/usr/bin/env python3
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import tempfile

from scripts.build_phase7_c71_bundle import build_bundle
from scripts.simulate_phase7_c74_controlled_deployment import run_simulation

ROOT = Path(__file__).resolve().parents[1]
C73 = ROOT / "state/phase7-c73-readiness.json"
C73_RECORD = ROOT / "docs/phase7-c73-remote-preflight-validation-record.json"
C74 = ROOT / "state/phase7-c74-deployment.json"
DOC = ROOT / "docs/phase7-c74-controlled-deployment.md"
DEPLOY = ROOT / "scripts/run_phase7_c74_controlled_deploy.py"
ROLLBACK = ROOT / "scripts/run_phase7_c74_rollback.py"
SIM = ROOT / "scripts/simulate_phase7_c74_controlled_deployment.py"
TEST = ROOT / "tests/test_phase7_c74.py"
README = ROOT / "README.md"
CI = ROOT / ".github/workflows/remake-ci.yml"
SOURCE_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C7.4 gate failed: {message}")


def sha256_file(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def main() -> int:
    for path in (C73, C73_RECORD, C74, DOC, DEPLOY, ROLLBACK, SIM, TEST, README, CI, SOURCE_MANIFEST):
        require(path.is_file(), f"required file missing: {path.relative_to(ROOT)}")

    c73 = json.loads(C73.read_text(encoding="utf-8"))
    require(c73.get("status") == "CONCLUÍDO", "C7.3 is not concluded")
    require(c73.get("remote_preflight") == "PASS_VALIDATED", "C7.3 remote preflight is not validated")
    require(c73.get("technical_go_no_go") == "GO", "C7.3 technical GO missing")
    require(c73.get("production_deployed") is False, "C7.3 unexpectedly claims deployment")

    record = json.loads(C73_RECORD.read_text(encoding="utf-8"))
    require(record.get("preflight", {}).get("status") == "PASS", "C7.3 evidence record is not PASS")
    require(record.get("preflight", {}).get("technical_go_no_go") == "GO", "C7.3 evidence record is not GO")
    require(record.get("preflight", {}).get("managed_targets") == 32, "C7.3 managed count drift")
    require(record.get("preflight", {}).get("preexisting_dependencies") == 11, "C7.3 dependency count drift")
    require(record.get("preflight", {}).get("planned_directory_creations") == 4, "C7.3 planned directory count drift")

    c74 = json.loads(C74.read_text(encoding="utf-8"))
    require(c74.get("checkpoint") == "C7.4", "C7.4 state checkpoint drift")
    require(c74.get("status") == "AUTHORIZED_READY_TO_DEPLOY", "C7.4 is not authorized-ready")
    require(c74.get("authorization_id") == "c74-20260916-a741aa78-843dca3e", "authorization id drift")
    require(c74.get("single_use_authorization") is True, "authorization is not single-use")
    require(c74.get("deployment_authorized") is True, "deployment authorization not recorded")
    require(c74.get("production_deployed") is False, "repository must remain pre-deploy before host execution")
    require(c74.get("post_deploy_validated") is False, "post-deploy cannot be validated before host execution")
    candidate = c74.get("candidate") or {}
    require(candidate.get("managed_files") == 32, "authorized managed count drift")
    require(candidate.get("preexisting_dependencies") == 11, "authorized dependency count drift")
    require(candidate.get("planned_directory_creations") == 4, "authorized planned directory count drift")
    require(candidate.get("c73_remote_evidence_sha256") == record.get("host_evidence", {}).get("sha256"), "authorized C7.3 evidence SHA drift")
    require(candidate.get("release_id") == record.get("release_id"), "authorized release drift")

    with tempfile.TemporaryDirectory(prefix="c74-gate-") as raw:
        bundle_dir = Path(raw) / "bundle"
        bundle = build_bundle(SOURCE_MANIFEST, bundle_dir)
        manifest = bundle_dir / "bundle-manifest.json"
        require(sha256_file(manifest) == candidate.get("bundle_manifest_sha256"), "authorized bundle manifest SHA no longer reconstructs exactly")
        require(bundle.get("release", {}).get("release_id") == candidate.get("release_id"), "rebuilt bundle release drift")
        require(bundle.get("managed_file_count") == 32, "rebuilt bundle managed count drift")
        require(len(bundle.get("preexisting_dependencies") or []) == 11, "rebuilt bundle dependency count drift")

    deploy = DEPLOY.read_text(encoding="utf-8")
    for marker in (
        "single-use deployment journal already exists",
        "compare_c73_state",
        "snapshot_managed",
        "AUTHORIZED_SNAPSHOT_VERIFIED",
        "APPLYING",
        "APPLIED_AWAITING_HEALTH",
        "APPLIED_HEALTHY",
        "ROLLED_BACK",
        "deployment_priority",
        "atomic_write_managed",
        "verify_deployed",
        "verify_dependencies",
        "rollback(snapshot",
        "SIGINT",
        "SIGTERM",
        "/blog/wp-json/sfa/v1/fiscal-health",
        "/blog/wp-json/sfa/v1/fiscal-release",
        "/blog/wp-json/sfa/v1/folha",
        "expected 410",
    ):
        require(marker in deploy, f"controlled deploy marker missing: {marker}")
    require("rmtree(" not in deploy, "recursive directory deletion present in deploy")
    require("rm -rf" not in deploy, "shell recursive deletion present in deploy")

    rb = ROLLBACK.read_text(encoding="utf-8")
    require("MANUAL_ROLLBACK_COMPLETE" in rb, "manual rollback completion marker missing")
    require("rollback(snapshot" in rb, "manual rollback does not use canonical rollback")
    require("rmtree(" not in rb and "rm -rf" not in rb, "recursive deletion present in manual rollback")

    simulation = run_simulation()
    for key in (
        "success_apply_verified",
        "success_post_deploy_health_verified",
        "single_use_journal_verified",
        "partial_apply_rollback_exact",
        "post_deploy_health_failure_rollback_exact",
    ):
        require(simulation.get(key) is True, f"C7.4 simulation did not prove {key}")
    require(simulation.get("managed_files") == 32, "simulation managed count drift")
    require(simulation.get("preexisting_dependencies") == 11, "simulation dependency count drift")
    require(simulation.get("production_mutated") is False, "simulation claims production mutation")

    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "**Status:** EM ANDAMENTO",
        "c74-20260916-a741aa78-843dca3e",
        "single-use",
        "APPLIED_HEALTHY",
        "rollback automático",
        "run_phase7_c74_rollback.py",
        "C7.4 só muda para `CONCLUÍDO`",
    ):
        require(marker in doc, f"C7.4 runbook marker missing: {marker}")

    readme = README.read_text(encoding="utf-8")
    for marker in (
        "C7.3 — runbook, observabilidade e pré-flight de produção",
        "C7.4 — autorização e implantação controlada",
        "AUTHORIZED_READY_TO_DEPLOY",
        "production_deployed=false",
    ):
        require(marker in readme, f"README missing C7.4 state marker: {marker}")

    ci = CI.read_text(encoding="utf-8")
    require("python scripts/simulate_phase7_c74_controlled_deployment.py" in ci, "Remake CI does not run C7.4 simulation")
    require("python scripts/validate_phase7_c74_gate.py" in ci, "Remake CI does not run C7.4 gate")
    require("c74-controlled-deployment-simulation-${{ github.sha }}" in ci, "Remake CI does not preserve C7.4 simulation evidence")

    print(
        "Phase 7 C7.4 authorization gate: PASS "
        f"(authorization={c74['authorization_id']}, bundle={candidate['bundle_manifest_sha256'][:12]}, release={candidate['release_id']}, managed=32, dependencies=11, simulation=apply+rollback+health, deployment_authorized=true, production_deployed=false)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
