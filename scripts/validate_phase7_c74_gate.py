#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

from scripts.simulate_phase7_c74_controlled_deployment import run_simulation

ROOT = Path(__file__).resolve().parents[1]
C73 = ROOT / "state/phase7-c73-readiness.json"
C73_RECORD = ROOT / "docs/phase7-c73-remote-preflight-validation-record.json"
C74 = ROOT / "state/phase7-c74-deployment.json"
AUTH_RECORD = ROOT / "docs/phase7-c74-authorization-record.json"
PROD_RECORD = ROOT / "docs/phase7-c74-production-deployment-record.json"
DOC = ROOT / "docs/phase7-c74-controlled-deployment.md"
DEPLOY = ROOT / "scripts/run_phase7_c74_controlled_deploy.py"
ROLLBACK = ROOT / "scripts/run_phase7_c74_rollback.py"
SIM = ROOT / "scripts/simulate_phase7_c74_controlled_deployment.py"
TEST = ROOT / "tests/test_phase7_c74.py"
README = ROOT / "README.md"
CI = ROOT / ".github/workflows/remake-ci.yml"
SOURCE_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"

AUTHORIZED_COMMIT = "ccc5a31c3da7c1c93570df0337e553e5a06404ac"
AUTHORIZATION_ID = "c74-20260916-a741aa78-843dca3e"
DEPLOYMENT_STATE_SHA = "9fc57e626dd8ab3a7666da18c7f1397f35729987a3be62ac83ed6ed7ca0bcc4c"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C7.4 gate failed: {message}")


def load(path: Path) -> dict:
    require(path.is_file(), f"required file missing: {path.relative_to(ROOT)}")
    value = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(value, dict), f"invalid JSON object: {path.relative_to(ROOT)}")
    return value


def main() -> int:
    for path in (
        C73,
        C73_RECORD,
        C74,
        AUTH_RECORD,
        PROD_RECORD,
        DOC,
        DEPLOY,
        ROLLBACK,
        SIM,
        TEST,
        README,
        CI,
        SOURCE_MANIFEST,
    ):
        require(path.is_file(), f"required file missing: {path.relative_to(ROOT)}")

    # C7.3 remains immutable historical evidence for the deployment that actually happened.
    c73 = load(C73)
    require(c73.get("status") == "CONCLUÍDO", "C7.3 is not concluded")
    require(c73.get("remote_preflight") == "PASS_VALIDATED", "C7.3 remote preflight is not validated")
    require(c73.get("technical_go_no_go") == "GO", "C7.3 technical GO missing")
    require(c73.get("production_deployed") is False, "historical C7.3 state must remain pre-deploy")

    c73_record = load(C73_RECORD)
    require(c73_record.get("preflight", {}).get("status") == "PASS", "C7.3 evidence record is not PASS")
    require(c73_record.get("preflight", {}).get("technical_go_no_go") == "GO", "C7.3 evidence record is not GO")
    require(c73_record.get("preflight", {}).get("managed_targets") == 32, "C7.3 managed count drift")
    require(c73_record.get("preflight", {}).get("preexisting_dependencies") == 11, "C7.3 dependency count drift")
    require(c73_record.get("preflight", {}).get("planned_directory_creations") == 4, "C7.3 planned directory count drift")

    # Authorization is a historical single-use record. Its candidate hash is NOT expected to
    # reconstruct from today's source tree after legitimate maintenance. History is instead
    # proven by immutable cross-bindings to the production record and concluded C7.4 state.
    auth = load(AUTH_RECORD)
    require(auth.get("checkpoint") == "C7.4", "authorization checkpoint drift")
    require(auth.get("record_type") == "single_use_deployment_authorization", "authorization record type drift")
    require(auth.get("status") == "AUTHORIZED_READY_TO_DEPLOY", "authorization history is not pre-deploy ready")
    require(auth.get("authorization_id") == AUTHORIZATION_ID, "authorization id drift")
    require(auth.get("single_use_authorization") is True, "authorization is not single-use")
    require(auth.get("deployment_authorized") is True, "authorization not recorded")
    require(auth.get("production_deployed") is False, "historical authorization must remain pre-deploy")
    candidate = auth.get("candidate") or {}
    require(candidate.get("managed_files") == 32, "authorized managed count drift")
    require(candidate.get("preexisting_dependencies") == 11, "authorized dependency count drift")
    require(candidate.get("planned_directory_creations") == 4, "authorized directory count drift")
    require(candidate.get("c73_remote_evidence_sha256") == c73_record.get("host_evidence", {}).get("sha256"), "authorized C7.3 evidence SHA drift")
    require(candidate.get("release_id") == c73_record.get("release_id"), "authorized release drift")
    require(isinstance(candidate.get("bundle_manifest_sha256"), str) and len(candidate["bundle_manifest_sha256"]) == 64, "authorized bundle manifest SHA invalid")

    prod = load(PROD_RECORD)
    require(prod.get("checkpoint") == "C7.4", "production record checkpoint drift")
    require(prod.get("record_type") == "production_deployment_validation_record", "production record type drift")
    require(prod.get("recorded_from") == "operator_terminal_output", "production record provenance drift")
    require(prod.get("authorization_id") == auth.get("authorization_id"), "production authorization id drift")
    require(prod.get("authorized_commit") == AUTHORIZED_COMMIT, "authorized production commit drift")
    require(prod.get("status") == "APPLIED_HEALTHY", "historical production deployment was not APPLIED_HEALTHY")
    require(prod.get("deployment_authorized") is True, "production record lost deployment authorization")
    require(prod.get("production_deployed") is True, "production record does not confirm deployment")
    require(prod.get("post_deploy_validated") is True, "production record does not confirm post-deploy validation")
    require(prod.get("rollback_performed") is False, "production record unexpectedly reports rollback")
    require(prod.get("release_id") == candidate.get("release_id"), "production release differs from authorization")
    require(prod.get("bundle_manifest_sha256") == candidate.get("bundle_manifest_sha256"), "production bundle SHA differs from authorization")
    require(prod.get("c73_remote_evidence_sha256") == candidate.get("c73_remote_evidence_sha256"), "production C7.3 evidence SHA differs from authorization")
    require(prod.get("deployment_state", {}).get("sha256") == DEPLOYMENT_STATE_SHA, "deployment-state SHA drift")
    require(prod.get("apply", {}).get("managed_files_applied") == 32, "production managed apply count drift")
    require(prod.get("apply", {}).get("planned_directories_created") == 4, "production created directory count drift")
    require(prod.get("apply", {}).get("error") is None, "production record contains an apply error")
    health = prod.get("health") or {}
    require(health.get("fiscal_health", {}).get("http_status") == 200, "fiscal health HTTP drift")
    require(health.get("fiscal_health", {}).get("status") == "healthy", "fiscal health not healthy")
    require(health.get("fiscal_health", {}).get("release_id") == candidate.get("release_id"), "fiscal health release drift")
    require(health.get("fiscal_release", {}).get("http_status") == 200, "fiscal release HTTP drift")
    require(health.get("fiscal_release", {}).get("release_id") == candidate.get("release_id"), "fiscal release id drift")
    require(health.get("legacy_folha", {}).get("http_status") == 410, "legacy folha is not 410")
    calculators = health.get("calculators") or {}
    require(set(calculators) == {"H26", "H27", "H28", "H29"}, "calculator production health inventory drift")
    require(all(row.get("http_status") == 200 for row in calculators.values()), "one or more historical calculator pages were not HTTP 200")

    c74 = load(C74)
    require(c74.get("schema_version") == "1.1.0", "C7.4 concluded state schema drift")
    require(c74.get("checkpoint") == "C7.4", "C7.4 state checkpoint drift")
    require(c74.get("status") == "CONCLUÍDO", "C7.4 state is not concluded")
    require(c74.get("authorization_id") == auth.get("authorization_id"), "C7.4 state authorization id drift")
    require(c74.get("single_use_authorization") is True, "C7.4 state lost single-use boundary")
    require(c74.get("authorization_consumed") is True, "single-use authorization was not consumed")
    require(c74.get("deployment_authorized") is True, "C7.4 concluded state lost authorization")
    require(c74.get("production_deployed") is True, "C7.4 concluded state does not claim production deployment")
    require(c74.get("post_deploy_validated") is True, "C7.4 concluded state does not claim post-deploy validation")
    require(c74.get("rollback_performed") is False, "C7.4 concluded state unexpectedly reports rollback")
    require(c74.get("closure_condition_met") is True, "C7.4 closure condition is not met")
    require(c74.get("candidate") == candidate, "C7.4 state candidate differs from original authorization")
    result = c74.get("deployment_result") or {}
    require(result.get("status") == "APPLIED_HEALTHY", "historical C7.4 result is not APPLIED_HEALTHY")
    require(result.get("managed_files_applied") == 32, "C7.4 state managed count drift")
    require(result.get("created_directories") == 4, "C7.4 state directory count drift")
    require(result.get("deployment_state_sha256") == prod.get("deployment_state", {}).get("sha256"), "C7.4 state deployment-state SHA differs from production record")

    # Current code is validated by C7.1 and by this isolated deployment simulation. We must
    # not compare today's bundle hash with the single-use bundle deployed in the past.
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

    deploy = DEPLOY.read_text(encoding="utf-8")
    for marker in (
        "single-use deployment journal already exists",
        "compare_c73_state",
        "snapshot_managed",
        "AUTHORIZED_SNAPSHOT_VERIFIED",
        "APPLYING",
        "APPLIED_AWAITING_HEALTH",
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

    rollback = ROLLBACK.read_text(encoding="utf-8")
    require("MANUAL_ROLLBACK_COMPLETE" in rollback, "manual rollback completion marker missing")
    require("rollback(snapshot" in rollback, "manual rollback does not use canonical rollback")
    require("rmtree(" not in rollback and "rm -rf" not in rollback, "recursive deletion present in manual rollback")

    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "**Status:** CONCLUÍDO",
        AUTHORIZATION_ID,
        "single-use",
        "APPLIED_HEALTHY",
        "production_deployed=true",
        DEPLOYMENT_STATE_SHA,
        "C7.4 está CONCLUÍDO",
    ):
        require(marker in doc, f"C7.4 closure document marker missing: {marker}")

    readme = README.read_text(encoding="utf-8")
    for marker in (
        "C7.3 — runbook, observabilidade e pré-flight de produção",
        "C7.4 — autorização e implantação controlada",
        "APPLIED_HEALTHY",
        "production_deployed=true",
    ):
        require(marker in readme, f"README missing C7.4 closure marker: {marker}")

    ci = CI.read_text(encoding="utf-8")
    require("PYTHONPATH=. python scripts/simulate_phase7_c74_controlled_deployment.py" in ci, "Remake CI does not run C7.4 simulation")
    require("PYTHONPATH=. python scripts/validate_phase7_c74_gate.py" in ci, "Remake CI does not run C7.4 gate")
    require("c74-controlled-deployment-simulation-${{ github.sha }}" in ci, "Remake CI does not preserve C7.4 simulation evidence")
    require(ci.count("set -o pipefail") >= 3, "Remake CI evidence pipelines are not fail-closed")

    print(
        "Phase 7 C7.4 historical closure gate: PASS "
        f"(authorization={AUTHORIZATION_ID}, historical_deployment=APPLIED_HEALTHY, release={candidate['release_id']}, managed=32, rollback=false; current bundle validated separately by C7.1/simulation)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
