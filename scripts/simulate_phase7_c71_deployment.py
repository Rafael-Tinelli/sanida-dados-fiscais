#!/usr/bin/env python3
from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
import shutil
import tempfile


def fail(message: str) -> None:
    raise SystemExit(f"C7.1 deployment simulation failed: {message}")


def safe_relative(value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or not path.parts or any(part in ("", ".", "..") for part in path.parts):
        fail(f"unsafe path: {value!r}")
    return path


def load_bundle(bundle_dir: Path) -> dict:
    path = bundle_dir / "bundle-manifest.json"
    if not path.is_file():
        fail("bundle-manifest.json missing")
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("checkpoint") != "C7.1" or data.get("production_deployed") is not False:
        fail("bundle manifest is not a C7.1 pre-production bundle")
    return data


def target_for(record: dict, roots: dict[str, Path]) -> Path:
    root_name = record["target_root"]
    if root_name not in roots:
        fail(f"unknown target root: {root_name}")
    return roots[root_name] / safe_relative(record["target_path"])


def snapshot_managed(records: list[dict], roots: dict[str, Path], backup_dir: Path) -> dict:
    states: dict[str, dict] = {}
    backup_dir.mkdir(parents=True, exist_ok=True)
    for record in records:
        key = record["bundle_path"]
        destination = target_for(record, roots)
        existed = destination.is_file()
        state = {"existed": existed, "sha256": None, "backup_path": None}
        if existed:
            body = destination.read_bytes()
            state["sha256"] = sha256(body).hexdigest()
            backup_path = backup_dir / safe_relative(key)
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            backup_path.write_bytes(body)
            state["backup_path"] = backup_path.relative_to(backup_dir).as_posix()
        states[key] = state
    (backup_dir / "snapshot.json").write_text(
        json.dumps(states, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return states


def apply_bundle(bundle_dir: Path, manifest: dict, roots: dict[str, Path]) -> None:
    for record in manifest["files"]:
        source = bundle_dir / safe_relative(record["bundle_path"])
        if not source.is_file():
            fail(f"bundle file missing: {record['bundle_path']}")
        body = source.read_bytes()
        if sha256(body).hexdigest() != record["sha256"]:
            fail(f"bundle file hash mismatch: {record['bundle_path']}")
        destination = target_for(record, roots)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(body)


def rollback_managed(manifest: dict, roots: dict[str, Path], backup_dir: Path, states: dict) -> None:
    for record in manifest["files"]:
        key = record["bundle_path"]
        destination = target_for(record, roots)
        state = states[key]
        if state["existed"]:
            backup_path = backup_dir / safe_relative(state["backup_path"])
            body = backup_path.read_bytes()
            if sha256(body).hexdigest() != state["sha256"]:
                fail(f"backup corruption: {key}")
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(body)
        elif destination.exists():
            destination.unlink()


def run_simulation(bundle_dir: Path) -> dict:
    manifest = load_bundle(bundle_dir)
    records = manifest.get("files") or []
    if not records:
        fail("bundle has no managed files")

    with tempfile.TemporaryDirectory(prefix="c71-deploy-sim-") as raw:
        root = Path(raw)
        roots = {
            "site_root": root / "target-site-root",
            "wordpress_plugin_dir": root / "target-wordpress-plugin",
        }
        for path in roots.values():
            path.mkdir(parents=True)

        # Seed both pre-existing and absent managed destinations so rollback proves
        # exact restoration and deletion of files introduced by the new bundle.
        original: dict[str, dict] = {}
        for index, record in enumerate(records):
            destination = target_for(record, roots)
            existed = index % 5 != 0
            if existed:
                destination.parent.mkdir(parents=True, exist_ok=True)
                body = f"pre-c71::{record['bundle_path']}\n".encode("utf-8")
                destination.write_bytes(body)
                original[record["bundle_path"]] = {"existed": True, "sha256": sha256(body).hexdigest()}
            else:
                original[record["bundle_path"]] = {"existed": False, "sha256": None}

        dependency_hashes: dict[str, str] = {}
        for dependency in manifest.get("preexisting_dependencies") or []:
            if dependency["target_root"] != "site_root":
                fail("C7.1 simulation only supports site-root preexisting dependencies")
            path = roots["site_root"] / safe_relative(dependency["target_path"])
            path.parent.mkdir(parents=True, exist_ok=True)
            body = f"preexisting::{dependency['target_path']}\n".encode("utf-8")
            path.write_bytes(body)
            dependency_hashes[dependency["target_path"]] = sha256(body).hexdigest()

        sentinels = {
            "site": roots["site_root"] / "unmanaged-c71-sentinel.txt",
            "plugin": roots["wordpress_plugin_dir"] / "unmanaged-c71-sentinel.txt",
        }
        for name, path in sentinels.items():
            path.write_text(f"keep::{name}\n", encoding="utf-8")
        sentinel_hashes = {name: sha256(path.read_bytes()).hexdigest() for name, path in sentinels.items()}

        backup_dir = root / "backup"
        states = snapshot_managed(records, roots, backup_dir)
        for key, expected in original.items():
            if states[key]["existed"] != expected["existed"] or states[key]["sha256"] != expected["sha256"]:
                fail(f"snapshot state mismatch: {key}")

        apply_bundle(bundle_dir, manifest, roots)
        for record in records:
            deployed = target_for(record, roots)
            if not deployed.is_file() or sha256(deployed.read_bytes()).hexdigest() != record["sha256"]:
                fail(f"apply verification failed: {record['bundle_path']}")

        rollback_managed(manifest, roots, backup_dir, states)
        for record in records:
            key = record["bundle_path"]
            destination = target_for(record, roots)
            expected = original[key]
            if expected["existed"]:
                if not destination.is_file() or sha256(destination.read_bytes()).hexdigest() != expected["sha256"]:
                    fail(f"rollback did not restore exact bytes: {key}")
            elif destination.exists():
                fail(f"rollback did not remove newly introduced file: {key}")

        for dependency in manifest.get("preexisting_dependencies") or []:
            path = roots["site_root"] / safe_relative(dependency["target_path"])
            if sha256(path.read_bytes()).hexdigest() != dependency_hashes[dependency["target_path"]]:
                fail(f"preexisting dependency mutated: {dependency['target_path']}")
        for name, path in sentinels.items():
            if sha256(path.read_bytes()).hexdigest() != sentinel_hashes[name]:
                fail(f"unmanaged sentinel mutated: {name}")

        return {
            "checkpoint": "C7.1",
            "managed_files": len(records),
            "apply_verified": True,
            "rollback_exact_bytes_verified": True,
            "new_file_cleanup_verified": True,
            "preexisting_dependencies_preserved": True,
            "unmanaged_files_preserved": True,
            "production_mutated": False,
        }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle-dir", type=Path, required=True)
    args = parser.parse_args()
    result = run_simulation(args.bundle_dir.resolve())
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
