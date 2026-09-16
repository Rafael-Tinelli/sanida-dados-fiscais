#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess


def fail(message: str) -> None:
    raise SystemExit(f"C7.3 host preflight failed: {message}")


def safe_relative(value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or not path.parts or any(part in ("", ".", "..") for part in path.parts):
        fail(f"unsafe relative path: {value!r}")
    return path


def path_is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def sha256_file(path: Path) -> str:
    h = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def bool_access(path: Path, mode: int) -> bool:
    try:
        return os.access(path, mode)
    except OSError:
        return False


def file_record(path: Path, *, relative_path: str, kind: str) -> dict:
    exists = path.exists()
    is_symlink = path.is_symlink()
    is_file = path.is_file() if exists else False
    record = {
        "kind": kind,
        "relative_path": relative_path,
        "absolute_path": str(path),
        "exists": exists,
        "is_file": is_file,
        "is_symlink": is_symlink,
        "readable": bool_access(path, os.R_OK) if exists else False,
        "writable": bool_access(path, os.W_OK) if exists else False,
        "bytes": None,
        "sha256": None,
        "mode": None,
    }
    if exists or is_symlink:
        try:
            st = path.lstat()
            record["mode"] = format(stat.S_IMODE(st.st_mode), "04o")
        except OSError:
            pass
    if exists and is_file and not is_symlink:
        try:
            record["bytes"] = path.stat().st_size
            record["sha256"] = sha256_file(path)
        except OSError:
            pass
    return record


def directory_state(path: Path) -> dict:
    exists = path.exists()
    is_symlink = path.is_symlink()
    is_dir = path.is_dir() if exists else False
    return {
        "absolute_path": str(path),
        "exists": exists,
        "is_dir": is_dir,
        "is_symlink": is_symlink,
        "readable": bool_access(path, os.R_OK) if exists else False,
        "traversable": bool_access(path, os.X_OK) if exists else False,
        "writable": bool_access(path, os.W_OK) if exists else False,
    }


def parent_plan(parent: Path, root: Path) -> dict:
    try:
        parent.relative_to(root)
    except ValueError:
        fail(f"managed parent escapes target root: {parent}")

    if parent.exists() or parent.is_symlink():
        state = directory_state(parent)
        ready = (
            state["exists"]
            and state["is_dir"]
            and not state["is_symlink"]
            and state["readable"]
            and state["traversable"]
            and state["writable"]
        )
        if state["is_symlink"]:
            block_code = "managed_parent_symlink_not_allowed"
        elif not state["exists"] or not state["is_dir"]:
            block_code = "managed_parent_not_directory"
        elif not state["readable"] or not state["traversable"]:
            block_code = "managed_parent_not_accessible"
        elif not state["writable"]:
            block_code = "managed_parent_not_writable"
        else:
            block_code = None
        return {
            **state,
            "creation_required": False,
            "creatable": ready,
            "ready": ready,
            "block_code": block_code,
            "missing_directories": [],
            "nearest_existing_ancestor": state,
        }

    missing: list[Path] = []
    cursor = parent
    while cursor != root and not cursor.exists() and not cursor.is_symlink():
        missing.append(cursor)
        cursor = cursor.parent

    ancestor = directory_state(cursor)
    creatable = (
        ancestor["exists"]
        and ancestor["is_dir"]
        and not ancestor["is_symlink"]
        and ancestor["readable"]
        and ancestor["traversable"]
        and ancestor["writable"]
    )
    missing_rel = [path.relative_to(root).as_posix() for path in reversed(missing)]
    return {
        "absolute_path": str(parent),
        "exists": False,
        "is_dir": False,
        "is_symlink": False,
        "readable": False,
        "traversable": False,
        "writable": False,
        "creation_required": True,
        "creatable": creatable,
        "ready": creatable,
        "block_code": None if creatable else "managed_parent_uncreatable",
        "missing_directories": missing_rel,
        "nearest_existing_ancestor": ancestor,
    }


def php_cli_info() -> dict:
    executable = shutil.which("php")
    if not executable:
        return {"available": False, "executable": None, "version": None}
    completed = subprocess.run(
        [executable, "-r", "echo PHP_VERSION;"],
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "available": completed.returncode == 0,
        "executable": executable,
        "version": completed.stdout.strip() if completed.returncode == 0 else None,
    }


def load_bundle_manifest(path: Path) -> dict:
    if not path.is_file():
        fail(f"bundle manifest missing: {path}")
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != "1.0.0" or data.get("checkpoint") != "C7.1":
        fail("unsupported bundle manifest")
    if data.get("production_deployed") is not False:
        fail("bundle manifest must remain pre-deploy")
    files = data.get("files") or []
    dependencies = data.get("preexisting_dependencies") or []
    if len(files) != 32:
        fail(f"expected 32 managed files, got {len(files)}")
    if len(dependencies) != 11:
        fail(f"expected 11 preexisting dependencies, got {len(dependencies)}")
    if set(data.get("bundle_roots") or {}) != {"site_root", "wordpress_plugin_dir"}:
        fail("bundle roots drift")
    return data


def target_for(record: dict, roots: dict[str, Path]) -> Path:
    root_name = str(record.get("target_root", ""))
    if root_name not in roots:
        fail(f"unknown target root: {root_name}")
    return roots[root_name] / safe_relative(str(record.get("target_path", "")))


def root_record(path: Path) -> dict:
    exists = path.exists()
    is_dir = path.is_dir() if exists else False
    is_symlink = path.is_symlink()
    free = None
    if exists and is_dir:
        try:
            free = shutil.disk_usage(path).free
        except OSError:
            free = None
    return {
        "absolute_path": str(path),
        "exists": exists,
        "is_dir": is_dir,
        "is_symlink": is_symlink,
        "readable": bool_access(path, os.R_OK) if exists else False,
        "traversable": bool_access(path, os.X_OK) if exists else False,
        "writable": bool_access(path, os.W_OK) if exists else False,
        "free_bytes": free,
    }


def run_preflight(bundle_manifest_path: Path, site_root: Path, wordpress_plugin_dir: Path) -> dict:
    bundle_manifest_path = bundle_manifest_path.resolve()
    roots = {
        "site_root": site_root.resolve(),
        "wordpress_plugin_dir": wordpress_plugin_dir.resolve(),
    }
    manifest = load_bundle_manifest(bundle_manifest_path)

    block_reasons: list[str] = []
    root_state = {name: root_record(path) for name, path in roots.items()}
    for name, state in root_state.items():
        if not state["exists"] or not state["is_dir"] or state["is_symlink"]:
            block_reasons.append(f"root_missing_not_directory_or_symlink:{name}")
        if not state["readable"] or not state["traversable"]:
            block_reasons.append(f"root_not_readable_or_traversable:{name}")

    dependencies: list[dict] = []
    for item in manifest.get("preexisting_dependencies") or []:
        target = target_for(item, roots)
        record = file_record(
            target,
            relative_path=str(item["target_path"]),
            kind=str(item.get("kind", "dependency")),
        )
        record["target_root"] = item["target_root"]
        dependencies.append(record)
        if not record["exists"]:
            block_reasons.append(f"dependency_missing:{item['target_root']}:{item['target_path']}")
        elif record["is_symlink"]:
            block_reasons.append(f"dependency_symlink_not_allowed:{item['target_root']}:{item['target_path']}")
        elif not record["is_file"]:
            block_reasons.append(f"dependency_not_regular_file:{item['target_root']}:{item['target_path']}")
        elif not record["readable"]:
            block_reasons.append(f"dependency_not_readable:{item['target_root']}:{item['target_path']}")

    managed_targets: list[dict] = []
    planned_directories: set[tuple[str, str]] = set()
    bundle_bytes_by_root = {name: 0 for name in roots}
    existing_backup_bytes_by_root = {name: 0 for name in roots}
    existing_count = 0
    absent_count = 0

    for item in manifest.get("files") or []:
        target = target_for(item, roots)
        root_name = item["target_root"]
        file_state = file_record(
            target,
            relative_path=str(item["target_path"]),
            kind="managed_target",
        )
        plan = parent_plan(target.parent, roots[root_name])
        file_state.update(
            {
                "target_root": root_name,
                "bundle_path": item["bundle_path"],
                "bundle_sha256": item["sha256"],
                "bundle_bytes": int(item["bytes"]),
                "parent": plan,
            }
        )
        managed_targets.append(file_state)
        bundle_bytes_by_root[root_name] += int(item["bytes"])

        if not plan["ready"]:
            block_reasons.append(
                f"{plan['block_code'] or 'managed_parent_uncreatable'}:{root_name}:{item['target_path']}"
            )
        elif plan["creation_required"]:
            for rel in plan["missing_directories"]:
                planned_directories.add((root_name, rel))

        if file_state["exists"]:
            existing_count += 1
            if file_state["is_symlink"]:
                block_reasons.append(f"managed_target_symlink_not_allowed:{root_name}:{item['target_path']}")
            elif not file_state["is_file"]:
                block_reasons.append(f"managed_target_not_regular_file:{root_name}:{item['target_path']}")
            else:
                if not file_state["readable"]:
                    block_reasons.append(f"managed_target_not_readable:{root_name}:{item['target_path']}")
                if not file_state["writable"]:
                    block_reasons.append(f"managed_target_not_writable:{root_name}:{item['target_path']}")
                if isinstance(file_state["bytes"], int):
                    existing_backup_bytes_by_root[root_name] += int(file_state["bytes"])
        else:
            absent_count += 1

    disk_requirements = {}
    for root_name in roots:
        minimum = bundle_bytes_by_root[root_name] + existing_backup_bytes_by_root[root_name]
        free = root_state[root_name]["free_bytes"]
        sufficient = isinstance(free, int) and free >= minimum
        disk_requirements[root_name] = {
            "bundle_bytes": bundle_bytes_by_root[root_name],
            "existing_backup_bytes": existing_backup_bytes_by_root[root_name],
            "minimum_free_bytes": minimum,
            "observed_free_bytes": free,
            "sufficient": sufficient,
        }
        if not sufficient:
            block_reasons.append(f"insufficient_or_unknown_free_space:{root_name}")

    php = php_cli_info()
    if not php["available"]:
        block_reasons.append("php_cli_unavailable")

    planned_directory_creations = [
        {"target_root": root_name, "relative_path": rel}
        for root_name, rel in sorted(planned_directories)
    ]
    block_reasons = sorted(set(block_reasons))
    status = "PASS" if not block_reasons else "BLOCKED"
    return {
        "schema_version": "1.1.0",
        "checkpoint": "C7.3",
        "mode": "read_only_host_preflight",
        "status": status,
        "technical_go_no_go": "GO" if status == "PASS" else "NO_GO",
        "production_deployed": False,
        "production_mutated": False,
        "deployment_authorized": False,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "bundle_manifest": str(bundle_manifest_path),
        "bundle_manifest_sha256": sha256_file(bundle_manifest_path),
        "release": manifest.get("release") or {},
        "roots": root_state,
        "summary": {
            "managed_files_expected": 32,
            "managed_files_observed": len(managed_targets),
            "existing_managed_targets": existing_count,
            "absent_managed_targets": absent_count,
            "preexisting_dependencies_expected": 11,
            "preexisting_dependencies_observed": len(dependencies),
            "all_managed_parent_directories_preexisting": len(planned_directory_creations) == 0,
            "all_managed_parent_directories_ready": not any(reason.startswith("managed_parent_") for reason in block_reasons),
            "planned_directory_creations": len(planned_directory_creations),
            "php_cli_available": bool(php["available"]),
        },
        "php_cli": php,
        "disk_requirements": disk_requirements,
        "planned_directory_creations": planned_directory_creations,
        "dependencies": dependencies,
        "managed_targets": managed_targets,
        "block_reasons": block_reasons,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="C7.3 read-only HostGator preflight")
    parser.add_argument("--bundle-manifest", type=Path, required=True)
    parser.add_argument("--site-root", type=Path, required=True)
    parser.add_argument("--wordpress-plugin-dir", type=Path, required=True)
    parser.add_argument("--evidence-out", type=Path)
    args = parser.parse_args()

    site_root = args.site_root.resolve()
    wordpress_plugin_dir = args.wordpress_plugin_dir.resolve()
    evidence_out = args.evidence_out.resolve() if args.evidence_out else None
    if evidence_out and (
        path_is_within(evidence_out, site_root)
        or path_is_within(evidence_out, wordpress_plugin_dir)
    ):
        fail("--evidence-out must be outside production target roots")

    result = run_preflight(
        args.bundle_manifest,
        site_root,
        wordpress_plugin_dir,
    )
    payload = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if evidence_out:
        evidence_out.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0 if result["status"] == "PASS" else 3


if __name__ == "__main__":
    raise SystemExit(main())
