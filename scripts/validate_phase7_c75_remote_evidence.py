#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
C74_STATE = ROOT / "state/phase7-c74-deployment.json"
C74_RECORD = ROOT / "docs/phase7-c74-production-deployment-record.json"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C7.5 remote evidence validation failed: {message}")


def load(path: Path) -> dict:
    require(path.is_file(), f"missing JSON file: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(value, dict), f"JSON root must be object: {path}")
    return value


def validate(evidence: dict, c74: dict, record: dict) -> dict:
    require(evidence.get("schema_version") == "1.0.0", "schema version drift")
    require(evidence.get("checkpoint") == "C7.5", "checkpoint drift")
    require(evidence.get("mode") == "post_deploy_read_only_validation", "mode drift")
    require(evidence.get("status") == "PASS", "post-deploy evidence is not PASS")
    require(evidence.get("phase7_close_recommended") is True, "Phase 7 close recommendation missing")
    require(evidence.get("production_deployed") is True, "production deployment not observed")
    require(evidence.get("production_mutated") is False, "C7.5 must be read-only")
    require((evidence.get("block_reasons") or []) == [], "post-deploy evidence contains block reasons")
    require(float(evidence.get("postdeploy_window_seconds", 0)) >= float(evidence.get("minimum_postdeploy_window_seconds", 600)), "post-deploy stability window too short")

    require(c74.get("status") == "CONCLUÍDO", "C7.4 repository state not concluded")
    require(c74.get("production_deployed") is True, "C7.4 repository state not deployed")
    require(c74.get("post_deploy_validated") is True, "C7.4 repository state not validated")
    require(c74.get("rollback_performed") is False, "C7.4 repository state reports rollback")
    require(record.get("status") == "APPLIED_HEALTHY", "C7.4 production record not healthy")
    require(evidence.get("authorization_id") == record.get("authorization_id") == c74.get("authorization_id"), "authorization identity drift")

    journal = evidence.get("journal") or {}
    expected_journal_sha = (record.get("deployment_state") or {}).get("sha256")
    require(journal.get("expected_sha256") == expected_journal_sha, "journal expected SHA drift")
    require(journal.get("observed_sha256") == expected_journal_sha, "journal observed SHA drift")
    require(journal.get("sha256_match") is True, "journal SHA not verified")
    require(journal.get("status") == "APPLIED_HEALTHY", "journal state not APPLIED_HEALTHY")
    require(journal.get("rollback_performed") is False, "journal reports rollback")

    bundle = evidence.get("bundle") or {}
    require(bundle.get("expected_manifest_sha256") == record.get("bundle_manifest_sha256"), "bundle expected SHA drift")
    require(bundle.get("observed_manifest_sha256") == record.get("bundle_manifest_sha256"), "bundle observed SHA drift")
    require(bundle.get("manifest_sha256_match") is True, "bundle manifest SHA not verified")
    require(bundle.get("managed_files_expected") == 32, "managed file expectation drift")
    require(bundle.get("managed_files_matching") == 32, "not all managed files match deployed bundle")

    dependencies = evidence.get("dependencies") or {}
    require(dependencies.get("c73_evidence_expected_sha256") == record.get("c73_remote_evidence_sha256"), "C7.3 expected evidence SHA drift")
    require(dependencies.get("c73_evidence_observed_sha256") == record.get("c73_remote_evidence_sha256"), "C7.3 observed evidence SHA drift")
    require(dependencies.get("c73_evidence_sha256_match") is True, "C7.3 evidence SHA not verified")
    require(dependencies.get("expected") == 11, "dependency expectation drift")
    require(dependencies.get("matching") == 11, "not all preexisting dependencies match baseline")

    filesystem = evidence.get("filesystem") or {}
    managed = filesystem.get("managed_files") or []
    deps = filesystem.get("preexisting_dependencies") or []
    created = filesystem.get("created_directories") or []
    require(len(managed) == 32 and all(item.get("match") is True for item in managed), "managed filesystem evidence incomplete")
    require(len(deps) == 11 and all(item.get("match") is True for item in deps), "dependency filesystem evidence incomplete")
    require(len(created) == 4 and all(item.get("present_directory_not_symlink") is True for item in created), "created-directory persistence drift")
    require((filesystem.get("c74_temporary_files") or []) == [], "C7.4 temporary files remain in managed parents")

    canonical = evidence.get("canonical_source") or {}
    release_id = evidence.get("release_id")
    require(isinstance(release_id, str) and release_id.startswith("fiscal-v1-sha256-"), "canonical release id malformed")
    require(canonical.get("current_http_status") == 200, "canonical current.json unavailable")
    require(canonical.get("release_id") == release_id, "canonical pointer release drift")
    require(canonical.get("artifact_http_status") == 200, "canonical immutable artifact unavailable")
    require(canonical.get("artifact_sha256_match") is True, "canonical immutable artifact SHA mismatch")
    require(canonical.get("contract_api_version") == "1.2.0", "canonical contract API version drift")
    require(canonical.get("schema_version_contract") == "1.2.0", "canonical contract schema version drift")

    probes = evidence.get("http_probes") or []
    require(len(probes) >= 2, "fewer than two independent HTTP probes")
    for probe in probes:
        health = probe.get("fiscal_health") or {}
        fiscal_release = probe.get("fiscal_release") or {}
        legacy = probe.get("legacy_folha") or {}
        calculators = probe.get("calculators") or {}
        require(health.get("http_status") == 200, "fiscal-health HTTP drift")
        require(health.get("status") == "healthy", "fiscal-health not healthy")
        require(health.get("release_id") == release_id, "fiscal-health release drift")
        require("no-store" in str(health.get("cache_control") or "").lower(), "fiscal-health became cacheable")
        require(health.get("header_status") == "healthy", "fiscal-health status header drift")
        require(health.get("header_release_id") == release_id, "fiscal-health release header drift")
        require(fiscal_release.get("http_status") == 200, "fiscal-release HTTP drift")
        require(fiscal_release.get("release_id") == release_id, "fiscal-release body drift")
        require(fiscal_release.get("header_release_id") == release_id, "fiscal-release header drift")
        require(legacy.get("http_status") == 410, "legacy folha endpoint not 410")
        require(set(calculators) == {"H26", "H27", "H28", "H29"}, "calculator probe set drift")
        for key, item in calculators.items():
            require(item.get("http_status") == 200, f"{key} not HTTP 200")
            require(item.get("fatal_or_parse_error") is False, f"{key} exposes PHP fatal/parse error")
            require(item.get("expected_assets_referenced") is True, f"{key} no longer references expected assets")

    public_js = evidence.get("public_js") or []
    require(len(public_js) == 8, "public JS delivery count drift")
    require(all(item.get("http_status") == 200 and item.get("match") is True for item in public_js), "one or more public JS assets differ from deployed bundle")

    return {
        "schema_version": "1.0.0",
        "checkpoint": "C7.5",
        "status": "PASS",
        "phase7_close_recommended": True,
        "production_deployed": True,
        "production_mutated": False,
        "authorization_id": evidence.get("authorization_id"),
        "release_id": release_id,
        "postdeploy_window_seconds": evidence.get("postdeploy_window_seconds"),
        "managed_files": 32,
        "preexisting_dependencies": 11,
        "public_js_assets": 8,
        "http_probes": len(probes),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate C7.5 HostGator post-deploy evidence")
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--c74-state", type=Path, default=C74_STATE)
    parser.add_argument("--c74-record", type=Path, default=C74_RECORD)
    args = parser.parse_args()

    summary = validate(load(args.evidence), load(args.c74_state), load(args.c74_record))
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
