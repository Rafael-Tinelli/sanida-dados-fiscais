#!/usr/bin/env python3
from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import tempfile

from scripts.build_phase7_c71_bundle import build_bundle
from scripts.run_phase7_c73_host_preflight import run_preflight, safe_relative
from scripts.run_phase7_c74_controlled_deploy import run_controlled_deployment

ROOT = Path(__file__).resolve().parents[1]
SOURCE_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"


def sha256_file(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


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


def seed_host(bundle: dict, base: Path) -> dict[str, Path]:
    roots = {
        "site_root": base / "public_html",
        "wordpress_plugin_dir": base / "public_html" / "blog" / "wp-content" / "plugins" / "sanida-fiscais-auto",
    }
    for root in roots.values():
        root.mkdir(parents=True, exist_ok=True)

    for dep in bundle["preexisting_dependencies"]:
        path = target_for(dep, roots)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"dependency::{dep['target_path']}\n", encoding="utf-8")

    missing_prefixes = (
        ("wordpress_plugin_dir", "includes/"),
        ("site_root", "financas/calculadoras/decimo-terceiro/parts/"),
        ("site_root", "financas/calculadoras/ferias-clt/parts/"),
        ("site_root", "financas/calculadoras/rescisao-clt/parts/"),
    )
    for index, record in enumerate(bundle["files"]):
        missing_parent = any(
            record["target_root"] == root_name and record["target_path"].startswith(prefix)
            for root_name, prefix in missing_prefixes
        )
        existed = (index % 3 == 0) and not missing_parent
        if existed:
            path = target_for(record, roots)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"before-c74::{record['bundle_path']}\n", encoding="utf-8")
    return roots


def write_authorization(path: Path, *, bundle_manifest: Path, c73_path: Path, bundle: dict, authorization_id: str) -> None:
    auth = {
        "schema_version": "1.0.0",
        "checkpoint": "C7.4",
        "status": "AUTHORIZED_READY_TO_DEPLOY",
        "authorization_id": authorization_id,
        "single_use_authorization": True,
        "deployment_authorized": True,
        "production_deployed": False,
        "candidate": {
            "bundle_manifest_sha256": sha256_file(bundle_manifest),
            "c73_remote_evidence_sha256": sha256_file(c73_path),
            "release_id": bundle["release"]["release_id"],
            "managed_files": 32,
            "preexisting_dependencies": 11,
            "planned_directory_creations": 4,
        },
    }
    path.write_text(json.dumps(auth, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def fake_health(_base_url: str, release_id: str) -> dict:
    return {
        "fiscal_health": {"http_status": 200, "status": "healthy", "release_id": release_id},
        "fiscal_release": {"http_status": 200, "release_id": release_id},
        "legacy_folha": {"http_status": 410},
        "calculators": {name: {"http_status": 200, "bytes": 1000} for name in ("H26", "H27", "H28", "H29")},
    }


def run_simulation() -> dict:
    with tempfile.TemporaryDirectory(prefix="c74-deploy-sim-") as raw:
        tmp = Path(raw)
        bundle_dir = tmp / "bundle"
        bundle = build_bundle(SOURCE_MANIFEST, bundle_dir)
        manifest = bundle_dir / "bundle-manifest.json"

        # Success path.
        host_ok = tmp / "host-ok"
        roots_ok = seed_host(bundle, host_ok)
        c73_ok = run_preflight(manifest, roots_ok["site_root"], roots_ok["wordpress_plugin_dir"])
        if c73_ok["status"] != "PASS":
            raise RuntimeError(f"success seed preflight failed: {c73_ok['block_reasons']}")
        c73_ok_path = tmp / "c73-ok.json"
        c73_ok_path.write_text(json.dumps(c73_ok, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        auth_ok = tmp / "auth-ok.json"
        auth_id_ok = "sim-c74-success"
        write_authorization(auth_ok, bundle_manifest=manifest, c73_path=c73_ok_path, bundle=bundle, authorization_id=auth_id_ok)
        journal_ok = tmp / "journals" / auth_id_ok
        result_ok = run_controlled_deployment(
            bundle_dir=bundle_dir,
            site_root=roots_ok["site_root"],
            wordpress_plugin_dir=roots_ok["wordpress_plugin_dir"],
            c73_evidence_path=c73_ok_path,
            authorization_path=auth_ok,
            authorization_id=auth_id_ok,
            journal_dir=journal_ok,
            health_base_url="https://example.invalid",
            health_checker=fake_health,
        )
        if result_ok["status"] != "APPLIED_HEALTHY" or result_ok["production_deployed"] is not True:
            raise RuntimeError(f"success deployment failed: {result_ok}")
        for record in bundle["files"]:
            path = target_for(record, roots_ok)
            if not path.is_file() or sha256_file(path) != record["sha256"]:
                raise RuntimeError(f"success deployment hash mismatch: {record['bundle_path']}")

        # Failure after partial writes must roll back exactly.
        host_rb = tmp / "host-rollback"
        roots_rb = seed_host(bundle, host_rb)
        before_rb = tree_snapshot(host_rb)
        c73_rb = run_preflight(manifest, roots_rb["site_root"], roots_rb["wordpress_plugin_dir"])
        c73_rb_path = tmp / "c73-rb.json"
        c73_rb_path.write_text(json.dumps(c73_rb, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        auth_rb = tmp / "auth-rb.json"
        auth_id_rb = "sim-c74-rollback"
        write_authorization(auth_rb, bundle_manifest=manifest, c73_path=c73_rb_path, bundle=bundle, authorization_id=auth_id_rb)
        journal_rb = tmp / "journals" / auth_id_rb
        result_rb = run_controlled_deployment(
            bundle_dir=bundle_dir,
            site_root=roots_rb["site_root"],
            wordpress_plugin_dir=roots_rb["wordpress_plugin_dir"],
            c73_evidence_path=c73_rb_path,
            authorization_path=auth_rb,
            authorization_id=auth_id_rb,
            journal_dir=journal_rb,
            health_base_url="https://example.invalid",
            health_checker=fake_health,
            fail_after_writes=9,
        )
        after_rb = tree_snapshot(host_rb)
        if result_rb["status"] != "ROLLED_BACK" or result_rb["rollback_performed"] is not True:
            raise RuntimeError(f"partial failure did not roll back: {result_rb}")
        if before_rb != after_rb:
            raise RuntimeError("partial failure rollback did not restore exact production tree")

        # Health failure after complete apply must also roll back exactly.
        host_health = tmp / "host-health-fail"
        roots_health = seed_host(bundle, host_health)
        before_health = tree_snapshot(host_health)
        c73_health = run_preflight(manifest, roots_health["site_root"], roots_health["wordpress_plugin_dir"])
        c73_health_path = tmp / "c73-health.json"
        c73_health_path.write_text(json.dumps(c73_health, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        auth_health = tmp / "auth-health.json"
        auth_id_health = "sim-c74-health-rollback"
        write_authorization(auth_health, bundle_manifest=manifest, c73_path=c73_health_path, bundle=bundle, authorization_id=auth_id_health)
        journal_health = tmp / "journals" / auth_id_health

        def broken_health(_base_url: str, _release_id: str) -> dict:
            raise RuntimeError("injected post-deploy health failure")

        result_health = run_controlled_deployment(
            bundle_dir=bundle_dir,
            site_root=roots_health["site_root"],
            wordpress_plugin_dir=roots_health["wordpress_plugin_dir"],
            c73_evidence_path=c73_health_path,
            authorization_path=auth_health,
            authorization_id=auth_id_health,
            journal_dir=journal_health,
            health_base_url="https://example.invalid",
            health_checker=broken_health,
        )
        after_health = tree_snapshot(host_health)
        if result_health["status"] != "ROLLED_BACK":
            raise RuntimeError(f"health failure did not roll back: {result_health}")
        if before_health != after_health:
            raise RuntimeError("health failure rollback did not restore exact production tree")

        return {
            "schema_version": "1.0.0",
            "checkpoint": "C7.4",
            "success_apply_verified": True,
            "success_post_deploy_health_verified": True,
            "single_use_journal_verified": journal_ok.is_dir(),
            "partial_apply_rollback_exact": True,
            "post_deploy_health_failure_rollback_exact": True,
            "managed_files": 32,
            "preexisting_dependencies": 11,
            "production_mutated": False,
        }


def main() -> int:
    print(json.dumps(run_simulation(), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
