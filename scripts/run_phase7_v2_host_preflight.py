#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import json

from scripts import run_phase7_c73_host_preflight as base

EXPECTED_MANAGED_FILES = 33
EXPECTED_DEPENDENCIES = 11


def load_bundle_manifest_v2(path: Path) -> dict:
    if not path.is_file():
        base.fail(f"bundle manifest missing: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != "1.0.0" or data.get("checkpoint") != "C7.1":
        base.fail("unsupported bundle manifest")
    if data.get("inventory_version") != 2:
        base.fail("expected inventory_version=2")
    if data.get("production_deployed") is not False:
        base.fail("bundle manifest must remain pre-deploy")
    files = data.get("files") or []
    dependencies = data.get("preexisting_dependencies") or []
    if len(files) != EXPECTED_MANAGED_FILES:
        base.fail(f"expected {EXPECTED_MANAGED_FILES} managed files, got {len(files)}")
    if int(data.get("managed_file_count", -1)) != EXPECTED_MANAGED_FILES:
        base.fail("managed_file_count drift")
    if len(dependencies) != EXPECTED_DEPENDENCIES:
        base.fail(f"expected {EXPECTED_DEPENDENCIES} preexisting dependencies, got {len(dependencies)}")
    if set(data.get("bundle_roots") or {}) != {"site_root", "wordpress_plugin_dir"}:
        base.fail("bundle roots drift")
    return data


def run_preflight(bundle_manifest_path: Path, site_root: Path, wordpress_plugin_dir: Path) -> dict:
    original_loader = base.load_bundle_manifest
    try:
        base.load_bundle_manifest = load_bundle_manifest_v2
        result = base.run_preflight(bundle_manifest_path, site_root, wordpress_plugin_dir)
    finally:
        base.load_bundle_manifest = original_loader
    result["inventory_version"] = 2
    result["summary"]["managed_files_expected"] = EXPECTED_MANAGED_FILES
    result["summary"]["preexisting_dependencies_expected"] = EXPECTED_DEPENDENCIES
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 7 v2 read-only HostGator preflight")
    parser.add_argument("--bundle-manifest", type=Path, required=True)
    parser.add_argument("--site-root", type=Path, required=True)
    parser.add_argument("--wordpress-plugin-dir", type=Path, required=True)
    parser.add_argument("--evidence-out", type=Path, required=True)
    args = parser.parse_args()

    site_root = args.site_root.resolve()
    wordpress_plugin_dir = args.wordpress_plugin_dir.resolve()
    evidence_out = args.evidence_out.resolve()
    if base.path_is_within(evidence_out, site_root) or base.path_is_within(evidence_out, wordpress_plugin_dir):
        base.fail("--evidence-out must be outside production target roots")

    result = run_preflight(args.bundle_manifest, site_root, wordpress_plugin_dir)
    payload = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    evidence_out.parent.mkdir(parents=True, exist_ok=True)
    evidence_out.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0 if result["status"] == "PASS" else 3


if __name__ == "__main__":
    raise SystemExit(main())
