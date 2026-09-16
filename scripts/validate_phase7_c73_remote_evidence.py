#!/usr/bin/env python3
from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
import re


def fail(message: str) -> None:
    raise SystemExit(f"C7.3 remote evidence validation failed: {message}")


def require(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def load_json(path: Path) -> dict:
    require(path.is_file(), f"JSON file missing: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(data, dict), f"JSON object required: {path}")
    return data


def validate_remote_evidence(evidence_path: Path, bundle_manifest_path: Path) -> dict:
    evidence_path = evidence_path.resolve()
    bundle_manifest_path = bundle_manifest_path.resolve()
    evidence = load_json(evidence_path)
    bundle = load_json(bundle_manifest_path)

    require(bundle.get("schema_version") == "1.0.0", "unsupported bundle schema")
    require(bundle.get("checkpoint") == "C7.1", "bundle is not C7.1")
    require(bundle.get("production_deployed") is False, "bundle claims production deployment")
    require(len(bundle.get("files") or []) == 32, "bundle managed-file count drift")
    require(len(bundle.get("preexisting_dependencies") or []) == 11, "bundle dependency count drift")

    require(evidence.get("schema_version") == "1.0.0", "unsupported evidence schema")
    require(evidence.get("checkpoint") == "C7.3", "evidence checkpoint drift")
    require(evidence.get("mode") == "read_only_host_preflight", "evidence mode drift")
    require(evidence.get("status") == "PASS", "remote preflight is not PASS")
    require(evidence.get("technical_go_no_go") == "GO", "remote preflight is not GO")
    require(evidence.get("production_deployed") is False, "preflight claims production deployment")
    require(evidence.get("production_mutated") is False, "preflight claims production mutation")
    require(evidence.get("deployment_authorized") is False, "preflight incorrectly authorizes deployment")
    require(evidence.get("block_reasons") == [], "remote preflight contains block reasons")

    expected_manifest_sha = sha256(bundle_manifest_path.read_bytes()).hexdigest()
    require(evidence.get("bundle_manifest_sha256") == expected_manifest_sha, "bundle manifest SHA mismatch")

    bundle_release = bundle.get("release") or {}
    evidence_release = evidence.get("release") or {}
    for key in ("release_id", "artifact", "artifact_sha256", "contract_schema_version", "contract_api_version"):
        require(evidence_release.get(key) == bundle_release.get(key), f"release binding drift: {key}")

    roots = evidence.get("roots") or {}
    require(set(roots) == {"site_root", "wordpress_plugin_dir"}, "remote root set drift")
    for name, state in roots.items():
        require(state.get("exists") is True, f"root missing: {name}")
        require(state.get("is_dir") is True, f"root is not directory: {name}")
        require(state.get("readable") is True, f"root not readable: {name}")
        require(state.get("traversable") is True, f"root not traversable: {name}")
        require(isinstance(state.get("absolute_path"), str) and state["absolute_path"].startswith("/"), f"root path is not absolute: {name}")

    dependencies = evidence.get("dependencies") or []
    require(len(dependencies) == 11, "remote dependency evidence count drift")
    expected_dependencies = {
        (item["target_root"], item["target_path"])
        for item in bundle["preexisting_dependencies"]
    }
    observed_dependencies = {
        (item.get("target_root"), item.get("relative_path"))
        for item in dependencies
    }
    require(observed_dependencies == expected_dependencies, "remote dependency identities drift")
    for item in dependencies:
        key = f"{item.get('target_root')}:{item.get('relative_path')}"
        require(item.get("exists") is True, f"dependency missing: {key}")
        require(item.get("is_file") is True, f"dependency not regular file: {key}")
        require(item.get("is_symlink") is False, f"dependency symlink not allowed: {key}")
        require(item.get("readable") is True, f"dependency not readable: {key}")
        require(bool(re.fullmatch(r"[0-9a-f]{64}", str(item.get("sha256") or ""))), f"dependency SHA missing: {key}")

    managed = evidence.get("managed_targets") or []
    require(len(managed) == 32, "remote managed-target evidence count drift")
    expected_managed = {
        (item["target_root"], item["target_path"], item["bundle_sha256"] if "bundle_sha256" in item else item["sha256"])
        for item in bundle["files"]
    }
    observed_managed = {
        (item.get("target_root"), item.get("relative_path"), item.get("bundle_sha256"))
        for item in managed
    }
    require(observed_managed == expected_managed, "remote managed-target identities/hash binding drift")
    for item in managed:
        key = f"{item.get('target_root')}:{item.get('relative_path')}"
        parent = item.get("parent") or {}
        require(parent.get("exists") is True and parent.get("is_dir") is True, f"managed parent missing: {key}")
        require(parent.get("readable") is True and parent.get("traversable") is True, f"managed parent inaccessible: {key}")
        require(parent.get("writable") is True, f"managed parent not writable: {key}")
        if item.get("exists"):
            require(item.get("is_file") is True, f"managed target not regular file: {key}")
            require(item.get("is_symlink") is False, f"managed target symlink not allowed: {key}")
            require(item.get("readable") is True, f"managed target not readable: {key}")
            require(item.get("writable") is True, f"managed target not writable: {key}")
            require(bool(re.fullmatch(r"[0-9a-f]{64}", str(item.get("sha256") or ""))), f"managed target SHA missing: {key}")

    disk = evidence.get("disk_requirements") or {}
    require(set(disk) == {"site_root", "wordpress_plugin_dir"}, "disk requirement root set drift")
    for name, state in disk.items():
        require(state.get("sufficient") is True, f"insufficient/unknown free space: {name}")
        require(isinstance(state.get("minimum_free_bytes"), int), f"minimum free bytes missing: {name}")
        require(isinstance(state.get("observed_free_bytes"), int), f"observed free bytes missing: {name}")
        require(state["observed_free_bytes"] >= state["minimum_free_bytes"], f"free-space arithmetic drift: {name}")

    php = evidence.get("php_cli") or {}
    require(php.get("available") is True, "PHP CLI unavailable in remote evidence")
    require(isinstance(php.get("version"), str) and bool(php["version"]), "PHP version missing")

    summary = evidence.get("summary") or {}
    require(summary.get("managed_files_expected") == 32, "summary managed expected drift")
    require(summary.get("managed_files_observed") == 32, "summary managed observed drift")
    require(summary.get("preexisting_dependencies_expected") == 11, "summary dependency expected drift")
    require(summary.get("preexisting_dependencies_observed") == 11, "summary dependency observed drift")
    require(summary.get("all_managed_parent_directories_preexisting") is True, "summary parent precondition failed")
    require(summary.get("php_cli_available") is True, "summary PHP precondition failed")

    return {
        "schema_version": "1.0.0",
        "checkpoint": "C7.3",
        "status": "PASS",
        "technical_go_no_go": "GO",
        "release_id": evidence_release.get("release_id"),
        "managed_targets": len(managed),
        "preexisting_dependencies": len(dependencies),
        "production_mutated": False,
        "deployment_authorized": False,
        "bundle_manifest_sha256": expected_manifest_sha,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate real C7.3 HostGator preflight evidence")
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--bundle-manifest", type=Path, required=True)
    args = parser.parse_args()
    result = validate_remote_evidence(args.evidence, args.bundle_manifest)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
