#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

from scripts.simulate_phase7_c75_postdeploy_validation import run_simulation

ROOT = Path(__file__).resolve().parents[1]
C74 = ROOT / "state/phase7-c74-deployment.json"
C74_RECORD = ROOT / "docs/phase7-c74-production-deployment-record.json"
C75 = ROOT / "state/phase7-c75-closure.json"
DOC = ROOT / "docs/phase7-c75-postdeploy-closure.md"
RUNNER = ROOT / "scripts/run_phase7_c75_postdeploy_validation.py"
REMOTE_VALIDATOR = ROOT / "scripts/validate_phase7_c75_remote_evidence.py"
SIM = ROOT / "scripts/simulate_phase7_c75_postdeploy_validation.py"
TEST = ROOT / "tests/test_phase7_c75.py"
README = ROOT / "README.md"
CI = ROOT / ".github/workflows/remake-ci.yml"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C7.5 gate failed: {message}")


def load(path: Path) -> dict:
    require(path.is_file(), f"missing required file: {path.relative_to(ROOT)}")
    value = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(value, dict), f"invalid JSON object: {path.relative_to(ROOT)}")
    return value


def main() -> int:
    for path in (C74, C74_RECORD, C75, DOC, RUNNER, REMOTE_VALIDATOR, SIM, TEST, README, CI):
        require(path.is_file(), f"required file missing: {path.relative_to(ROOT)}")

    c74 = load(C74)
    record = load(C74_RECORD)
    require(c74.get("status") == "CONCLUÍDO", "C7.4 is not concluded")
    require(c74.get("production_deployed") is True, "C7.4 production deployment missing")
    require(c74.get("post_deploy_validated") is True, "C7.4 post-deploy validation missing")
    require(c74.get("rollback_performed") is False, "C7.4 reports rollback")
    require(record.get("status") == "APPLIED_HEALTHY", "C7.4 production record is not APPLIED_HEALTHY")
    require(record.get("deployment_state", {}).get("sha256") == "9fc57e626dd8ab3a7666da18c7f1397f35729987a3be62ac83ed6ed7ca0bcc4c", "C7.4 journal SHA drift")

    c75 = load(C75)
    require(c75.get("checkpoint") == "C7.5", "C7.5 checkpoint drift")
    require(c75.get("status") == "AWAITING_REMOTE_POSTDEPLOY_VALIDATION", "C7.5 readiness state drift")
    require(c75.get("phase7_status") == "EM ANDAMENTO", "Phase 7 closed before remote C7.5 evidence")
    require(c75.get("production_deployed") is True, "C7.5 lost production deployment state")
    require(c75.get("production_mutation_allowed") is False, "C7.5 incorrectly allows production mutation")
    require(c75.get("minimum_postdeploy_window_seconds") == 600, "C7.5 stability window drift")
    required = c75.get("required_validation") or {}
    require(required.get("managed_files_match_c74_bundle") == 32, "managed-file closure count drift")
    require(required.get("preexisting_dependencies_match_c73_baseline") == 11, "dependency closure count drift")
    require(required.get("planned_directories_still_safe") == 4, "created-directory closure count drift")
    require(required.get("public_js_matches_bundle") == 8, "public-JS closure count drift")
    require(required.get("independent_http_probes_minimum") == 2, "HTTP probe minimum drift")
    require(required.get("production_mutated_by_c75") is False, "C7.5 mutation boundary drift")

    runner = RUNNER.read_text(encoding="utf-8")
    for marker in (
        "post_deploy_read_only_validation",
        "postdeploy_window_too_short",
        "managed_file_drift:",
        "dependency_drift:",
        "canonical_artifact_sha_mismatch",
        "public_js_delivery_drift:",
        "fiscal_health_not_healthy",
        "legacy_folha_not_410",
        "--evidence-out must remain outside production roots",
    ):
        require(marker in runner, f"runner marker missing: {marker}")
    for forbidden in ("run_phase7_c74_controlled_deploy", "os.replace", ".unlink(", ".write_bytes("):
        require(forbidden not in runner, f"C7.5 runner contains production mutation/deploy primitive: {forbidden}")

    simulation = run_simulation()
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
        require(simulation.get(key) is True, f"simulation did not prove {key}")
    require(simulation.get("production_mutated") is False, "simulation claims production mutation")

    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "**Status:** EM ANDAMENTO",
        "600 segundos",
        "32/32 arquivos",
        "11/11 dependências",
        "oito JavaScript",
        "duas rodadas independentes",
        "production_mutated=false",
        "Fase 7 só muda para `CONCLUÍDA`",
    ):
        require(marker in doc, f"C7.5 runbook marker missing: {marker}")

    readme = README.read_text(encoding="utf-8")
    for marker in (
        "C7.4 — autorização e implantação controlada",
        "C7.5 — validação pós-deploy e fechamento formal da Fase 7",
        "production_deployed=true",
        "Fase 7 — Fechamento e operação evergreen",
        "Status: EM ANDAMENTO",
    ):
        require(marker in readme, f"README missing C7.5 readiness marker: {marker}")

    ci = CI.read_text(encoding="utf-8")
    require("python scripts/simulate_phase7_c75_postdeploy_validation.py" in ci, "Remake CI does not run C7.5 simulation")
    require("python scripts/validate_phase7_c75_gate.py" in ci, "Remake CI does not run C7.5 gate")
    require("c75-postdeploy-simulation-${{ github.sha }}" in ci, "Remake CI does not preserve C7.5 simulation evidence")

    print(
        "Phase 7 C7.5 readiness gate: PASS "
        "(C7.4=APPLIED_HEALTHY, production_deployed=true, stability_window=600s, managed=32, dependencies=11, public_js=8, probes=2, drift_fail_closed=true, production_mutation_allowed=false, remote_validation=pending)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
