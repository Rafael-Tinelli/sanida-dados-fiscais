#!/usr/bin/env python3
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import tempfile

from scripts.build_phase7_c71_bundle import build_bundle
from scripts.run_phase7_c73_host_preflight import run_preflight, safe_relative

ROOT = Path(__file__).resolve().parents[1]
SOURCE_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"


def fail(message: str) -> None:
    raise SystemExit(f"C7.3 preflight simulation failed: {message}")


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


def seed_host(bundle: dict, base: Path, *, leave_creatable_parents_missing: bool = True) -> dict[str, Path]:
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

    missing_prefixes = (
        ("wordpress_plugin_dir", "includes/"),
        ("site_root", "financas/calculadoras/ferias-clt/parts/"),
    )
    for index, record in enumerate(bundle.get("files") or []):
        path = target_for(record, roots)
        if leave_creatable_parents_missing and any(
            record["target_root"] == root_name and record["target_path"].startswith(prefix)
            for root_name, prefix in missing_prefixes
        ):
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        if index % 4 != 0:
            path.write_text(f"production-before-c73::{record['bundle_path']}\n", encoding="utf-8")
    return roots


def run_simulation() -> dict:
    with tempfile.TemporaryDirectory(prefix="c73-preflight-sim-") as raw:
        tmp = Path(raw)
        bundle_dir = tmp / "bundle"
        bundle = build_bundle(SOURCE_MANIFEST, bundle_dir)
        bundle_manifest = bundle_dir / "bundle-manifest.json"

        host = tmp / "host-pass"
        roots = seed_host(bundle, host, leave_creatable_parents_missing=True)
        before = tree_snapshot(host)
        passed = run_preflight(bundle_manifest, roots["site_root"], roots["wordpress_plugin_dir"])
        after = tree_snapshot(host)
        if before != after:
            fail("PASS preflight mutated target tree")
        if passed["status"] != "PASS" or passed["technical_go_no_go"] != "GO":
            fail(f"expected PASS/GO, got {passed['status']}/{passed['technical_go_no_go']}: {passed['block_reasons']}")
        if len(passed["dependencies"]) != 11 or len(passed["managed_targets"]) != 32:
            fail("PASS evidence count drift")
        if passed["summary"].get("planned_directory_creations", 0) <= 0:
            fail("positive case did not exercise creatable missing parents")
        if passed["summary"].get("all_managed_parent_directories_ready") is not True:
            fail("positive case did not mark parent directory plan ready")
        if passed["production_mutated"] is not False or passed["deployment_authorized"] is not False:
            fail("preflight crossed deployment boundary")

        missing_dep_host = tmp / "host-missing-dependency"
        roots_dep = seed_host(bundle, missing_dep_host)
        dependency = bundle["preexisting_dependencies"][0]
        dependency_path = target_for(dependency, roots_dep)
        dependency_path.unlink()
        before_dep = tree_snapshot(missing_dep_host)
        blocked_dep = run_preflight(bundle_manifest, roots_dep["site_root"], roots_dep["wordpress_plugin_dir"])
        after_dep = tree_snapshot(missing_dep_host)
        if before_dep != after_dep:
            fail("blocked dependency preflight mutated target tree")
        expected_prefix = f"dependency_missing:{dependency['target_root']}:{dependency['target_path']}"
        if blocked_dep["status"] != "BLOCKED" or expected_prefix not in blocked_dep["block_reasons"]:
            fail("missing dependency did not fail closed")

        uncreatable_host = tmp / "host-uncreatable-parent"
        roots_parent = seed_host(bundle, uncreatable_host, leave_creatable_parents_missing=False)
        candidate = next(
            (
                record
                for record in bundle["files"]
                if record["target_path"] == "financas/calculadoras/salario-liquido-clt/index.php"
            ),
            None,
        )
        if candidate is None:
            fail("isolated H26 managed target missing from bundle")
        candidate_path = target_for(candidate, roots_parent)
        if candidate_path.exists():
            candidate_path.unlink()
        parent = candidate_path.parent
        if any(parent.iterdir()):
            fail(f"H26 parent unexpectedly contains seeded files: {parent}")
        parent.rmdir()
        parent.write_text("blocks-directory-creation\n", encoding="utf-8")

        before_parent = tree_snapshot(uncreatable_host)
        blocked_parent = run_preflight(bundle_manifest, roots_parent["site_root"], roots_parent["wordpress_plugin_dir"])
        after_parent = tree_snapshot(uncreatable_host)
        if before_parent != after_parent:
            fail("blocked parent preflight mutated target tree")
        prefix = f"managed_parent_not_directory:{candidate['target_root']}:{candidate['target_path']}"
        if blocked_parent["status"] != "BLOCKED" or prefix not in blocked_parent["block_reasons"]:
            fail("uncreatable managed parent did not fail closed")

        return {
            "schema_version": "1.1.0",
            "checkpoint": "C7.3",
            "production_deployed": False,
            "production_mutated": False,
            "pass_case": {
                "status": passed["status"],
                "technical_go_no_go": passed["technical_go_no_go"],
                "dependencies": len(passed["dependencies"]),
                "managed_targets": len(passed["managed_targets"]),
                "existing_managed_targets": passed["summary"]["existing_managed_targets"],
                "absent_managed_targets": passed["summary"]["absent_managed_targets"],
                "planned_directory_creations": passed["summary"]["planned_directory_creations"],
            },
            "missing_dependency_blocked": True,
            "creatable_missing_parent_allowed": True,
            "uncreatable_parent_blocked": True,
            "non_mutation_verified": True,
        }


def main() -> int:
    result = run_simulation()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
