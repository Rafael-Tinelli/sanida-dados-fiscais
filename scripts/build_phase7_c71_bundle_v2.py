#!/usr/bin/env python3
from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.publication_v1 import FiscalReleaseStore

DEFAULT_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v2.json"
STORE = ROOT / "releases/fiscal-v1"
EXPECTED_MANAGED_FILES = 33


def fail(message: str) -> None:
    raise SystemExit(f"C7.1 v2 bundle build failed: {message}")


def safe_relative(value: str, field: str) -> Path:
    path = Path(value)
    if path.is_absolute() or not path.parts or any(part in ("", ".", "..") for part in path.parts):
        fail(f"unsafe {field}: {value!r}")
    return path


def validate_release(binding: dict) -> tuple[dict, Path]:
    release = FiscalReleaseStore(STORE).load_current()
    if release is None:
        fail("current fiscal release unavailable")
    if release.status.value != binding.get("required_release_status"):
        fail("current fiscal release status mismatch")
    if release.schema_version != binding.get("required_contract_schema_version"):
        fail("contract schema version mismatch")
    if release.consumer_compatibility.contract_api_version != binding.get("required_contract_api_version"):
        fail("contract API version mismatch")
    if len(release.rules) != int(binding.get("required_rule_count", -1)):
        fail("fiscal rule count mismatch")
    if {item.value for item in release.consumer_compatibility.consumers} != set(binding.get("required_consumers", [])):
        fail("consumer compatibility set mismatch")
    if release.release_id != release.expected_release_id():
        fail("release_id is not canonical")

    pointer = ROOT / safe_relative(str(binding.get("pointer", "")), "release pointer")
    pointer_data = json.loads(pointer.read_text(encoding="utf-8"))
    if pointer_data.get("release_id") != release.release_id:
        fail("current.json does not point to loaded release")
    artifact = STORE / pointer_data["artifact"]
    if not artifact.is_file():
        fail("immutable fiscal artifact missing")
    if sha256(artifact.read_bytes()).hexdigest() != pointer_data.get("artifact_sha256"):
        fail("immutable fiscal artifact hash mismatch")
    return pointer_data, artifact


def build_bundle(manifest_path: Path, output_dir: Path) -> dict:
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != "1.0.0" or manifest.get("checkpoint") != "C7.1":
        fail("unsupported deployment manifest")
    if manifest.get("inventory_version") != 2:
        fail("expected inventory_version=2")
    if manifest.get("production_deployed") is not False:
        fail("v2 manifest must remain pre-deploy")

    roots = manifest.get("bundle_roots") or {}
    if set(roots) != {"site_root", "wordpress_plugin_dir"}:
        fail("bundle roots drift")

    pointer, artifact = validate_release(manifest["release_binding"])
    managed = manifest.get("managed_files") or []
    dependencies = manifest.get("preexisting_dependencies") or []
    if len(managed) != EXPECTED_MANAGED_FILES:
        fail(f"expected {EXPECTED_MANAGED_FILES} managed files, got {len(managed)}")
    if len(dependencies) != 11:
        fail(f"expected 11 preexisting dependencies, got {len(dependencies)}")

    seen_sources: set[str] = set()
    seen_destinations: set[str] = set()
    records: list[dict] = []
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    for item in managed:
        source_text = str(item.get("source", ""))
        root_name = str(item.get("target_root", ""))
        target_text = str(item.get("target_path", ""))
        if root_name not in roots:
            fail(f"unknown target_root: {root_name}")
        if source_text in seen_sources:
            fail(f"duplicate managed source: {source_text}")
        seen_sources.add(source_text)

        source_rel = safe_relative(source_text, "source")
        target_rel = safe_relative(target_text, "target_path")
        bundle_rel = safe_relative(str(roots[root_name]), "bundle root") / target_rel
        key = bundle_rel.as_posix()
        if key in seen_destinations:
            fail(f"duplicate bundle destination: {key}")
        seen_destinations.add(key)

        source = ROOT / source_rel
        if not source.is_file():
            fail(f"managed source missing: {source_text}")
        body = source.read_bytes()
        destination = output_dir / bundle_rel
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(body)
        records.append({
            "source": source_text,
            "target_root": root_name,
            "target_path": target_rel.as_posix(),
            "bundle_path": key,
            "sha256": sha256(body).hexdigest(),
            "bytes": len(body),
        })

    output_manifest = {
        "schema_version": "1.0.0",
        "inventory_version": 2,
        "checkpoint": "C7.1",
        "production_deployed": False,
        "source_manifest": manifest_path.relative_to(ROOT).as_posix(),
        "source_manifest_sha256": sha256(manifest_path.read_bytes()).hexdigest(),
        "release": {
            "release_id": pointer["release_id"],
            "artifact": pointer["artifact"],
            "artifact_sha256": pointer["artifact_sha256"],
            "contract_schema_version": pointer["schema_version_contract"],
            "contract_api_version": pointer["contract_api_version"],
            "immutable_artifact_repository_sha256": sha256(artifact.read_bytes()).hexdigest(),
        },
        "bundle_roots": roots,
        "managed_file_count": len(records),
        "files": sorted(records, key=lambda item: item["bundle_path"]),
        "preexisting_dependencies": dependencies,
        "rollback_policy": manifest.get("rollback_policy") or {},
    }
    (output_dir / "bundle-manifest.json").write_text(
        json.dumps(output_manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    result = build_bundle(args.manifest.resolve(), args.output_dir.resolve())
    print(f"Phase 7 C7.1 v2 bundle: BUILT (files={result['managed_file_count']}, release={result['release']['release_id']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
