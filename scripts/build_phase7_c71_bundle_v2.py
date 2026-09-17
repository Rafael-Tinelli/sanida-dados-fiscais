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

from scripts.build_phase7_c71_bundle import build_bundle, safe_relative

DEFAULT_EXTENSION = ROOT / "docs/phase7-c71-deployment-manifest-v2.json"


def fail(message: str) -> None:
    raise SystemExit(f"C7.1 H25 bundle extension failed: {message}")


def load_extension(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != "1.0.0" or data.get("checkpoint") != "C7.1-H25-extension":
        fail("unsupported H25 extension manifest")
    if data.get("status") != "ready_not_deployed" or data.get("production_deployed") is not False:
        fail("H25 extension manifest must remain pre-deploy")
    if data.get("inherit_preexisting_dependencies") is not True:
        fail("H25 extension must inherit preexisting dependencies")
    if data.get("inherit_rollback_policy") is not True:
        fail("H25 extension must inherit rollback policy")
    additions = data.get("managed_files_add") or []
    if not additions:
        fail("H25 extension has no managed files")
    return data


def build_bundle_v2(extension_path: Path, output_dir: Path) -> dict:
    extension_path = extension_path.resolve()
    extension = load_extension(extension_path)

    base_rel = safe_relative(str(extension.get("base_manifest", "")), "base_manifest")
    base_manifest = ROOT / base_rel
    if not base_manifest.is_file():
        fail(f"base manifest missing: {base_rel.as_posix()}")

    bundle = build_bundle(base_manifest, output_dir)
    base_expected = int(extension.get("base_managed_file_count", -1))
    if bundle.get("managed_file_count") != base_expected:
        fail(
            "base managed-file count drift: "
            f"expected {base_expected}, got {bundle.get('managed_file_count')}"
        )

    roots = bundle.get("bundle_roots") or {}
    records = list(bundle.get("files") or [])
    seen_sources = {str(item["source"]) for item in records}
    seen_destinations = {str(item["bundle_path"]) for item in records}

    for item in extension["managed_files_add"]:
        source_text = str(item.get("source", ""))
        root_name = str(item.get("target_root", ""))
        target_text = str(item.get("target_path", ""))
        if root_name not in roots:
            fail(f"unknown target_root for {source_text}: {root_name}")

        source_rel = safe_relative(source_text, "source")
        target_rel = safe_relative(target_text, "target_path")
        root_rel = safe_relative(str(roots[root_name]), "bundle root")
        bundle_rel = root_rel / target_rel
        bundle_key = bundle_rel.as_posix()

        if source_text in seen_sources:
            fail(f"duplicate managed source: {source_text}")
        if bundle_key in seen_destinations:
            fail(f"duplicate bundle destination: {bundle_key}")

        source = ROOT / source_rel
        if not source.is_file():
            fail(f"managed source missing: {source_text}")
        body = source.read_bytes()
        destination = output_dir / bundle_rel
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(body)

        records.append(
            {
                "source": source_text,
                "target_root": root_name,
                "target_path": target_rel.as_posix(),
                "bundle_path": bundle_key,
                "sha256": sha256(body).hexdigest(),
                "bytes": len(body),
            }
        )
        seen_sources.add(source_text)
        seen_destinations.add(bundle_key)

    expected = int(extension.get("resulting_managed_file_count", -1))
    if len(records) != expected:
        fail(f"resulting managed-file count drift: expected {expected}, got {len(records)}")

    extension_bytes = extension_path.read_bytes()
    bundle["managed_file_count"] = len(records)
    bundle["files"] = sorted(records, key=lambda row: row["bundle_path"])
    bundle["extension_manifest"] = extension_path.relative_to(ROOT).as_posix()
    bundle["extension_manifest_sha256"] = sha256(extension_bytes).hexdigest()
    bundle["extension_checkpoint"] = extension["checkpoint"]
    bundle["source_manifest_chain"] = [
        base_manifest.relative_to(ROOT).as_posix(),
        extension_path.relative_to(ROOT).as_posix(),
    ]
    bundle["production_deployed"] = False

    manifest_out = output_dir / "bundle-manifest.json"
    manifest_out.write_text(
        json.dumps(bundle, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return bundle


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--extension", type=Path, default=DEFAULT_EXTENSION)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    output_dir = args.output_dir.resolve()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    result = build_bundle_v2(args.extension.resolve(), output_dir)
    print(
        "Phase 7 C7.1 + H25 deployment bundle: BUILT "
        f"(files={result['managed_file_count']}, output={output_dir})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
