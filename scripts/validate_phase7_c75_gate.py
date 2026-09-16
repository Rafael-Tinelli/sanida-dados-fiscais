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
INITIAL_RECORD = ROOT / "docs/phase7-c75-initial-host-observation.json"
RUNNER = ROOT / "scripts/run_phase7_c75_postdeploy_validation.py"
HOST_VALIDATOR = ROOT / "scripts/validate_phase7_c75_remote_evidence.py"
EXTERNAL_PROBE = ROOT / "scripts/run_phase7_c75_external_delivery_probe.ps1"
EXTERNAL_VALIDATOR = ROOT / "scripts/validate_phase7_c75_external_delivery_evidence.py"
SIM = ROOT / "scripts/simulate_phase7_c75_postdeploy_validation.py"
TEST = ROOT / "tests/test_phase7_c75.py"
README = ROOT / "README.md"
CI = ROOT / ".github/workflows/remake-ci.yml"

EXPECTED_HOST_EVIDENCE_SHA = "996561d9acaee9473d4bbd2035ce32c8d1bff85c8e9e6eb38b10e2f26a8ff447"
AUTHORIZED_COMMIT = "ccc5a31c3da7c1c93570df0337e553e5a06404ac"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C7.5 gate failed: {message}")


def load(path: Path) -> dict:
    require(path.is_file(), f"missing required file: {path.relative_to(ROOT)}")
    value = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(value, dict), f"invalid JSON object: {path.relative_to(ROOT)}")
    return value


def main() -> int:
    for path in (
        C74,
        C74_RECORD,
        C75,
        DOC,
        INITIAL_RECORD,
        RUNNER,
        HOST_VALIDATOR,
        EXTERNAL_PROBE,
        EXTERNAL_VALIDATOR,
        SIM,
        TEST,
        README,
        CI,
    ):
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
    require(c75.get("schema_version") == "1.1.0", "C7.5 schema drift")
    require(c75.get("checkpoint") == "C7.5", "C7.5 checkpoint drift")
    require(c75.get("status") == "AWAITING_EXTERNAL_CLIENT_DELIVERY_VALIDATION", "C7.5 split-boundary readiness state drift")
    require(c75.get("phase7_status") == "EM ANDAMENTO", "Phase 7 closed before external delivery evidence")
    require(c75.get("production_deployed") is True, "C7.5 lost production deployment state")
    require(c75.get("production_mutation_allowed") is False, "C7.5 incorrectly allows production mutation")
    require(c75.get("minimum_postdeploy_window_seconds") == 600, "C7.5 stability window drift")

    host = c75.get("host_origin_observation") or {}
    require(host.get("raw_evidence_sha256") == EXPECTED_HOST_EVIDENCE_SHA, "initial HostGator evidence SHA drift")
    require(host.get("initial_status") == "BLOCKED", "initial HostGator result history drift")
    require(host.get("non_delivery_blocks") == 0, "unexpected non-delivery host blocks recorded")
    require(host.get("managed_files_matching") == 32, "host managed-file count drift")
    require(host.get("preexisting_dependencies_matching") == 11, "host dependency count drift")
    require(host.get("created_directories") == 4, "host directory count drift")
    require(host.get("http_probes") == 2, "host HTTP probe count drift")
    require(host.get("production_mutated") is False, "host observation claims mutation")

    boundary = c75.get("delivery_boundary") or {}
    require(boundary.get("host_public_delivery_observation_is_diagnostic_only") is True, "host delivery diagnostic boundary missing")
    require(boundary.get("external_client_evidence_required") is True, "external-client evidence is not required")
    require(boundary.get("external_probe") == EXTERNAL_PROBE.relative_to(ROOT).as_posix(), "external probe path drift")
    require(boundary.get("external_validator") == EXTERNAL_VALIDATOR.relative_to(ROOT).as_posix(), "external validator path drift")
    require(boundary.get("authorized_commit") == AUTHORIZED_COMMIT, "external delivery authorized commit drift")

    required = c75.get("required_validation") or {}
    require(required.get("managed_files_match_c74_bundle") == 32, "managed-file closure count drift")
    require(required.get("preexisting_dependencies_match_c73_baseline") == 11, "dependency closure count drift")
    require(required.get("planned_directories_still_safe") == 4, "created-directory closure count drift")
    require(required.get("external_client_public_js_matches_authorized_commit") == 8, "external public-JS closure count drift")
    require(required.get("independent_http_probes_minimum") == 2, "HTTP probe minimum drift")
    require(required.get("production_mutated_by_c75") is False, "C7.5 mutation boundary drift")

    initial = load(INITIAL_RECORD)
    require(initial.get("record_type") == "initial_host_postdeploy_observation", "initial host record type drift")
    require(initial.get("host_evidence", {}).get("sha256") == EXPECTED_HOST_EVIDENCE_SHA, "initial host record SHA drift")
    require(initial.get("status") == "BLOCKED", "initial host record status drift")
    require(initial.get("production_mutated") is False, "initial host record claims mutation")
    require(initial.get("origin_validation", {}).get("managed_files_matching") == 32, "initial host origin managed count drift")
    require(initial.get("origin_validation", {}).get("preexisting_dependencies_matching") == 11, "initial host origin dependency count drift")
    require(initial.get("host_public_js_observation", {}).get("matching") == 5, "initial host public-JS history drift")
    require(len(initial.get("host_public_js_observation", {}).get("block_reasons") or []) == 3, "initial host delivery block history drift")
    require(initial.get("followup_diagnosis", {}).get("production_files_changed") is False, "follow-up diagnosis claims production mutation")
    require(initial.get("followup_diagnosis", {}).get("c74_redeployment_performed") is False, "follow-up diagnosis claims C7.4 redeploy")

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

    host_validator = HOST_VALIDATOR.read_text(encoding="utf-8")
    for marker in (
        'startswith("public_js_delivery_drift:")',
        'external_delivery_required',
        'host_origin_validation',
        'non-delivery blocks',
    ):
        require(marker in host_validator, f"host validator split-boundary marker missing: {marker}")

    external_probe = EXTERNAL_PROBE.read_text(encoding="utf-8")
    for marker in (
        "external_client_public_delivery_validation",
        AUTHORIZED_COMMIT,
        "raw.githubusercontent.com",
        "https://sanida.com.br/financas/calculadoras/assets",
        "assets_matching",
        "production_mutated = $false",
    ):
        require(marker in external_probe, f"external probe marker missing: {marker}")

    external_validator = EXTERNAL_VALIDATOR.read_text(encoding="utf-8")
    for marker in (
        "external_client_public_delivery_validation",
        "assets_matching\") == 8",
        "public_http_status\") == 200",
        "expected_sha == public_sha",
        AUTHORIZED_COMMIT,
    ):
        require(marker in external_validator, f"external validator marker missing: {marker}")

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
        "cliente externo",
        "oito JavaScript",
        EXPECTED_HOST_EVIDENCE_SHA,
        AUTHORIZED_COMMIT,
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
        "Phase 7 C7.5 split-boundary readiness gate: PASS "
        "(C7.4=APPLIED_HEALTHY, host_origin=observed, host_evidence_sha=pinned, managed=32, dependencies=11, probes=2, host_edge_delivery=diagnostic_only, external_client_js=8_required, production_mutation_allowed=false, phase7=EM_ANDAMENTO)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
