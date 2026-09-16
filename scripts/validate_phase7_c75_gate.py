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
FINAL_RECORD = ROOT / "docs/phase7-c75-final-production-validation-record.json"
RUNNER = ROOT / "scripts/run_phase7_c75_postdeploy_validation.py"
HOST_VALIDATOR = ROOT / "scripts/validate_phase7_c75_remote_evidence.py"
EXTERNAL_PROBE = ROOT / "scripts/run_phase7_c75_external_delivery_probe.ps1"
EXTERNAL_VALIDATOR = ROOT / "scripts/validate_phase7_c75_external_delivery_evidence.py"
SIM = ROOT / "scripts/simulate_phase7_c75_postdeploy_validation.py"
TEST = ROOT / "tests/test_phase7_c75.py"
README = ROOT / "README.md"
CI = ROOT / ".github/workflows/remake-ci.yml"

EXPECTED_HOST_EVIDENCE_SHA = "996561d9acaee9473d4bbd2035ce32c8d1bff85c8e9e6eb38b10e2f26a8ff447"
EXPECTED_EXTERNAL_BLOCKED_SHA = "908551d36f340d97fbaa1277768fd1330651c1a555f7312c286003300843fd70"
EXPECTED_EXTERNAL_PASS_SHA = "e162b1f7d60427bc9fd2679bdce683adac566ecdfcd349637dfef883cb981284"
AUTHORIZED_COMMIT = "ccc5a31c3da7c1c93570df0337e553e5a06404ac"
EXPECTED_RELEASE = "fiscal-v1-sha256-a741aa7873950d029a5c6b1c929727267125424013f09c69137b7e80b294153e"


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
        FINAL_RECORD,
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
    c74_record = load(C74_RECORD)
    require(c74.get("status") == "CONCLUÍDO", "C7.4 is not concluded")
    require(c74.get("production_deployed") is True, "C7.4 production deployment missing")
    require(c74.get("post_deploy_validated") is True, "C7.4 post-deploy validation missing")
    require(c74.get("rollback_performed") is False, "C7.4 reports rollback")
    require(c74_record.get("status") == "APPLIED_HEALTHY", "C7.4 production record is not APPLIED_HEALTHY")
    require(c74_record.get("deployment_state", {}).get("sha256") == "9fc57e626dd8ab3a7666da18c7f1397f35729987a3be62ac83ed6ed7ca0bcc4c", "C7.4 journal SHA drift")

    c75 = load(C75)
    require(c75.get("schema_version") == "1.2.0", "C7.5 final schema drift")
    require(c75.get("checkpoint") == "C7.5", "C7.5 checkpoint drift")
    require(c75.get("status") == "CONCLUÍDO", "C7.5 is not concluded")
    require(c75.get("phase7_status") == "CONCLUÍDA", "Phase 7 is not concluded")
    require(c75.get("remake_status") == "CONCLUÍDO", "remake is not concluded")
    require(c75.get("production_deployed") is True, "production deployment state lost")
    require(c75.get("production_mutation_allowed") is False, "C7.5 unexpectedly allows production mutation")
    require(c75.get("production_files_mutated_by_c75") is False, "C7.5 reports production file mutation")
    require(c75.get("edge_cache_remediation_performed") is True, "selective edge-cache remediation not recorded")
    require(c75.get("release_id") == EXPECTED_RELEASE, "final release id drift")
    require(c75.get("closure_condition_met") is True, "C7.5 closure condition not met")

    host = c75.get("host_origin_evidence") or {}
    require(host.get("raw_evidence_sha256") == EXPECTED_HOST_EVIDENCE_SHA, "HostGator evidence SHA drift")
    require(host.get("formal_revalidation_status") == "PASS", "host-origin formal validation missing")
    require(host.get("managed_files_matching") == 32, "host managed-file count drift")
    require(host.get("preexisting_dependencies_matching") == 11, "host dependency count drift")
    require(host.get("created_directories") == 4, "host directory count drift")
    require(host.get("http_probes") == 2, "host HTTP probe count drift")
    require(host.get("production_mutated") is False, "host evidence claims mutation")

    external = c75.get("external_delivery_evidence") or {}
    require(external.get("initial_blocked_sha256") == EXPECTED_EXTERNAL_BLOCKED_SHA, "initial external BLOCKED SHA drift")
    require(external.get("initial_assets_matching") == 5, "initial external asset count drift")
    require(external.get("final_pass_sha256") == EXPECTED_EXTERNAL_PASS_SHA, "final external PASS SHA drift")
    require(external.get("final_status") == "PASS", "external delivery is not PASS")
    require(external.get("final_assets_matching") == 8, "external delivery did not prove 8/8")
    require(external.get("assets_expected") == 8, "external delivery expectation drift")
    require(external.get("authorized_commit") == AUTHORIZED_COMMIT, "external delivery authorized commit drift")
    require(external.get("production_mutated") is False, "external evidence claims production mutation")

    cache = c75.get("cache_remediation") or {}
    require(cache.get("affected_assets") == ["folha-core.js", "ferias-clt.js", "rescisao-clt.js"], "cache remediation asset set drift")
    require(cache.get("origin_cache_bust_matches") == 3, "cache-bust origin proof drift")
    require(cache.get("c74_redeployment_performed") is False, "cache remediation incorrectly records redeploy")
    require(cache.get("production_files_rewritten") is False, "cache remediation incorrectly records production rewrite")

    initial = load(INITIAL_RECORD)
    require(initial.get("status") == "BLOCKED", "initial HostGator BLOCKED history drift")
    require(initial.get("host_evidence", {}).get("sha256") == EXPECTED_HOST_EVIDENCE_SHA, "initial host record SHA drift")
    require(initial.get("origin_validation", {}).get("managed_files_matching") == 32, "initial origin managed count drift")
    require(initial.get("origin_validation", {}).get("preexisting_dependencies_matching") == 11, "initial origin dependency count drift")
    require(initial.get("production_mutated") is False, "initial host record claims mutation")

    final_record = load(FINAL_RECORD)
    require(final_record.get("record_type") == "phase7_final_postdeploy_closure", "final record type drift")
    require(final_record.get("status") == "PASS", "final record is not PASS")
    require(final_record.get("phase7_status") == "CONCLUÍDA", "final record Phase 7 status drift")
    require(final_record.get("production_files_mutated_by_c75") is False, "final record claims production mutation")
    require(final_record.get("host_origin", {}).get("raw_evidence_sha256") == EXPECTED_HOST_EVIDENCE_SHA, "final record host SHA drift")
    require(final_record.get("host_origin", {}).get("formal_revalidation_status") == "PASS", "final record host validation missing")
    require(final_record.get("external_delivery_initial", {}).get("evidence_sha256") == EXPECTED_EXTERNAL_BLOCKED_SHA, "final record initial external SHA drift")
    require(final_record.get("external_delivery_final", {}).get("evidence_sha256") == EXPECTED_EXTERNAL_PASS_SHA, "final record external PASS SHA drift")
    require(final_record.get("external_delivery_final", {}).get("assets_matching") == 8, "final record external assets drift")
    require(len(final_record.get("external_delivery_final", {}).get("assets") or {}) == 8, "final record asset hash set incomplete")
    require(final_record.get("closure_conditions", {}).get("phase7_close_recommended") is True, "final record does not recommend closure")
    require(final_record.get("closure_conditions", {}).get("c74_redeployment_performed") is False, "final record reports C7.4 redeploy")

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
        "**Status:** CONCLUÍDO",
        "**Fase 7:** CONCLUÍDA",
        EXPECTED_HOST_EVIDENCE_SHA,
        EXPECTED_EXTERNAL_BLOCKED_SHA,
        EXPECTED_EXTERNAL_PASS_SHA,
        "max-age=31536000",
        "purga seletiva",
        "production_files_mutated_by_c75=false",
        "Remake:** CONCLUÍDO",
    ):
        require(marker in doc, f"C7.5 final runbook marker missing: {marker}")

    readme = README.read_text(encoding="utf-8")
    for marker in (
        "### Fase 7 — Fechamento e operação evergreen",
        "**Status: CONCLUÍDA**",
        "C7.5 — validação pós-deploy e fechamento formal",
        EXPECTED_EXTERNAL_PASS_SHA,
        "max-age=31536000",
        "remake está formalmente concluído",
    ):
        require(marker in readme, f"README missing final C7.5/Phase 7 marker: {marker}")

    ci = CI.read_text(encoding="utf-8")
    require("python scripts/simulate_phase7_c75_postdeploy_validation.py" in ci, "Remake CI does not run C7.5 simulation")
    require("python scripts/validate_phase7_c75_gate.py" in ci, "Remake CI does not run C7.5 gate")

    print(
        "Phase 7 C7.5 final closure gate: PASS "
        "(C7.4=APPLIED_HEALTHY, host_origin=PASS, managed=32, dependencies=11, probes=2, external_client_js=8/8, cache_remediation=selective, production_files_mutated=false, C7.5=CONCLUÍDO, Phase7=CONCLUÍDA, remake=CONCLUÍDO)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
