#!/usr/bin/env python3
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import tempfile

from scripts.build_phase7_c71_bundle import build_bundle
from scripts.run_phase7_c73_host_preflight import safe_relative

ROOT = Path(__file__).resolve().parents[1]
SOURCE_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"


def fail(message: str) -> None:
    raise SystemExit(f"C7.3 directory rollback simulation failed: {message}")


def target_for(record: dict, roots: dict[str, Path]) -> Path:
    return roots[record["target_root"]] / safe_relative(record["target_path"])


def tree_snapshot(root: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for path in sorted(root.rglob("*")):
        rel = path.relative_to(root).as_posix()
        if path.is_dir():
            out[rel] = {"type": "dir"}
        elif path.is_file():
            body = path.read_bytes()
            out[rel] = {"type": "file", "bytes": len(body), "sha256": sha256(body).hexdigest()}
        else:
            out[rel] = {"type": "other"}
    return out


def ensure_parent(parent: Path, root: Path, created: set[Path]) -> None:
    try:
        parent.relative_to(root)
    except ValueError:
        fail(f"parent escapes root: {parent}")

    missing: list[Path] = []
    cursor = parent
    while cursor != root and not cursor.exists():
        missing.append(cursor)
        cursor = cursor.parent
    if not cursor.exists() or not cursor.is_dir() or cursor.is_symlink():
        fail(f"nearest existing ancestor is unsafe: {cursor}")
    for directory in reversed(missing):
        directory.mkdir()
        created.add(directory)


def seed_host(bundle: dict, base: Path) -> tuple[dict[str, Path], dict[str, dict]]:
    roots = {
        "site_root": base / "public_html",
        "wordpress_plugin_dir": base / "public_html" / "blog" / "wp-content" / "plugins" / "sanida-fiscais-auto",
    }
    for path in roots.values():
        path.mkdir(parents=True, exist_ok=True)

    for dependency in bundle.get("preexisting_dependencies") or []:
        path = target_for(dependency, roots)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"dependency::{dependency['target_path']}\n", encoding="utf-8")

    deliberately_missing_prefixes = (
        ("wordpress_plugin_dir", "includes/"),
        ("site_root", "financas/calculadoras/ferias-clt/parts/"),
    )
    original: dict[str, dict] = {}
    for index, record in enumerate(bundle.get("files") or []):
        key = record["bundle_path"]
        path = target_for(record, roots)
        missing_parent_case = any(
            record["target_root"] == root_name and record["target_path"].startswith(prefix)
            for root_name, prefix in deliberately_missing_prefixes
        )
        existed = (index % 4 != 0) and not missing_parent_case
        if existed:
            path.parent.mkdir(parents=True, exist_ok=True)
            body = f"production-before-c73::{key}\n".encode("utf-8")
            path.write_bytes(body)
            original[key] = {"existed": True, "sha256": sha256(body).hexdigest(), "body": body}
        else:
            original[key] = {"existed": False, "sha256": None, "body": None}
    return roots, original


def apply_bundle(bundle_dir: Path, bundle: dict, roots: dict[str, Path]) -> set[Path]:
    created_dirs: set[Path] = set()
    for record in bundle["files"]:
        source = bundle_dir / safe_relative(record["bundle_path"])
        body = source.read_bytes()
        if sha256(body).hexdigest() != record["sha256"]:
            fail(f"bundle hash mismatch: {record['bundle_path']}")
        destination = target_for(record, roots)
        ensure_parent(destination.parent, roots[record["target_root"]], created_dirs)
        destination.write_bytes(body)
    return created_dirs


def rollback_bundle(bundle: dict, roots: dict[str, Path], original: dict[str, dict], created_dirs: set[Path]) -> list[str]:
    for record in bundle["files"]:
        key = record["bundle_path"]
        destination = target_for(record, roots)
        state = original[key]
        if state["existed"]:
            destination.write_bytes(state["body"])
        elif destination.exists():
            destination.unlink()

    nonempty: list[str] = []
    for directory in sorted(created_dirs, key=lambda p: len(p.parts), reverse=True):
        try:
            directory.rmdir()
        except OSError:
            nonempty.append(str(directory))
    return nonempty


def run_simulation() -> dict:
    with tempfile.TemporaryDirectory(prefix="c73-dir-rollback-") as raw:
        tmp = Path(raw)
        bundle_dir = tmp / "bundle"
        bundle = build_bundle(SOURCE_MANIFEST, bundle_dir)

        host = tmp / "host-exact"
        roots, original = seed_host(bundle, host)
        before = tree_snapshot(host)
        created_dirs = apply_bundle(bundle_dir, bundle, roots)
        if not created_dirs:
            fail("simulation did not create managed directories")
        for record in bundle["files"]:
            path = target_for(record, roots)
            if not path.is_file() or sha256(path.read_bytes()).hexdigest() != record["sha256"]:
                fail(f"apply verification failed: {record['bundle_path']}")
        nonempty = rollback_bundle(bundle, roots, original, created_dirs)
        if nonempty:
            fail(f"exact rollback left created directories nonempty: {nonempty}")
        after = tree_snapshot(host)
        if before != after:
            fail("exact rollback did not restore original tree")

        host_safe = tmp / "host-nonempty"
        roots_safe, original_safe = seed_host(bundle, host_safe)
        created_safe = apply_bundle(bundle_dir, bundle, roots_safe)
        if not created_safe:
            fail("safety scenario did not create directories")
        deepest = max(created_safe, key=lambda p: len(p.parts))
        sentinel = deepest / "unmanaged-after-apply.txt"
        sentinel.write_text("must-survive\n", encoding="utf-8")
        nonempty_safe = rollback_bundle(bundle, roots_safe, original_safe, created_safe)
        if not sentinel.is_file() or sentinel.read_text(encoding="utf-8") != "must-survive\n":
            fail("rollback deleted unmanaged content from created directory")
        if not nonempty_safe:
            fail("nonempty created directory was not reported")

        return {
            "schema_version": "1.0.0",
            "checkpoint": "C7.3",
            "production_deployed": False,
            "production_mutated": False,
            "created_directories_exercised": len(created_dirs),
            "exact_tree_rollback_verified": True,
            "created_empty_directories_removed": True,
            "recursive_directory_delete_forbidden_by_implementation": True,
            "nonempty_created_directory_preserved": True,
            "unmanaged_content_preserved": True,
        }


def main() -> int:
    print(json.dumps(run_simulation(), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
