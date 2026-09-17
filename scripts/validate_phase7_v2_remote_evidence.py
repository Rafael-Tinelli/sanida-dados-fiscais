#!/usr/bin/env python3
from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path

EXPECTED_MANAGED_FILES = 33
EXPECTED_DEPENDENCIES = 11


def fail(message: str) -> None:
    raise SystemExit(f"Phase 7 v2 remote evidence failed: {message}")


def sha256_file(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--bundle-manifest", type=Path, required=True)
    args = parser.parse_args()

    evidence = json.loads(args.evidence.read_text(encoding="utf-8"))
    bundle = json.loads(args.bundle_manifest.read_text(encoding="utf-8"))

    if bundle.get("inventory_version") != 2:
        fail("bundle inventory_version is not 2")
    if bundle.get("managed_file_count") != EXPECTED_MANAGED_FILES:
        fail("bundle managed file count drift")
    if len(bundle.get("preexisting_dependencies") or []) != EXPECTED_DEPENDENCIES:
        fail("bundle dependency count drift")
    if evidence.get("inventory_version") != 2:
        fail("evidence inventory_version is not 2")
    if evidence.get("status") != "PASS" or evidence.get("technical_go_no_go") != "GO":
        fail(f"preflight is not PASS/GO: {evidence.get('block_reasons')}")
    if evidence.get("production_mutated") is not False:
        fail("preflight claims production mutation")
    if evidence.get("production_deployed") is not False:
        fail("preflight claims production deployment")
    if evidence.get("deployment_authorized") is not False:
        fail("preflight claims deployment authorization")
    if evidence.get("bundle_manifest_sha256") != sha256_file(args.bundle_manifest):
        fail("bundle manifest SHA mismatch")
    if (evidence.get("release") or {}).get("release_id") != (bundle.get("release") or {}).get("release_id"):
        fail("release binding mismatch")

    summary = evidence.get("summary") or {}
    if summary.get("managed_files_expected") != EXPECTED_MANAGED_FILES:
        fail("summary managed expected drift")
    if summary.get("managed_files_observed") != EXPECTED_MANAGED_FILES:
        fail("summary managed observed drift")
    if summary.get("preexisting_dependencies_expected") != EXPECTED_DEPENDENCIES:
        fail("summary dependency expected drift")
    if summary.get("preexisting_dependencies_observed") != EXPECTED_DEPENDENCIES:
        fail("summary dependency observed drift")
    if summary.get("all_managed_parent_directories_ready") is not True:
        fail("managed parent directories are not ready")
    if summary.get("php_cli_available") is not True:
        fail("PHP CLI unavailable")
    if len(evidence.get("managed_targets") or []) != EXPECTED_MANAGED_FILES:
        fail("managed target evidence count drift")
    if len(evidence.get("dependencies") or []) != EXPECTED_DEPENDENCIES:
        fail("dependency evidence count drift")
    if evidence.get("block_reasons") != []:
        fail("preflight contains block reasons")

    print(
        "Phase 7 v2 remote evidence: PASS "
        f"(managed={EXPECTED_MANAGED_FILES}, dependencies={EXPECTED_DEPENDENCIES}, "
        f"bundle_sha={sha256_file(args.bundle_manifest)})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
