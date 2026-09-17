#!/usr/bin/env python3
from __future__ import annotations

import argparse
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator

from scripts import run_phase7_c74_controlled_deploy as base
from scripts.run_phase7_v2_host_preflight import run_preflight as run_preflight_v2

EXPECTED_MANAGED_FILES = 33
EXPECTED_DEPENDENCIES = 11


class V2DeploymentError(base.DeploymentError):
    pass


def load_bundle_v2(bundle_dir: Path) -> tuple[dict, Path]:
    manifest_path = bundle_dir / "bundle-manifest.json"
    manifest = base.load_json(manifest_path)
    if manifest.get("schema_version") != "1.0.0" or manifest.get("checkpoint") != "C7.1":
        raise V2DeploymentError("unsupported deployment bundle")
    if manifest.get("inventory_version") != 2:
        raise V2DeploymentError("bundle inventory_version must be 2")
    if manifest.get("production_deployed") is not False:
        raise V2DeploymentError("bundle is not a pre-production C7.1 v2 bundle")
    if len(manifest.get("files") or []) != EXPECTED_MANAGED_FILES:
        raise V2DeploymentError(f"bundle must contain exactly {EXPECTED_MANAGED_FILES} managed files")
    if int(manifest.get("managed_file_count", -1)) != EXPECTED_MANAGED_FILES:
        raise V2DeploymentError("managed_file_count drift")
    if len(manifest.get("preexisting_dependencies") or []) != EXPECTED_DEPENDENCIES:
        raise V2DeploymentError(f"bundle must declare exactly {EXPECTED_DEPENDENCIES} preexisting dependencies")
    return manifest, manifest_path


def verify_bindings_v2(
    bundle: dict,
    manifest_path: Path,
    c73_path: Path,
    c73: dict,
    auth: dict,
) -> None:
    candidate = auth.get("candidate") or {}
    manifest_sha = base.sha256_file(manifest_path)
    c73_sha = base.sha256_file(c73_path)
    release_id = (bundle.get("release") or {}).get("release_id")

    if candidate.get("bundle_manifest_sha256") != manifest_sha:
        raise V2DeploymentError("bundle manifest SHA is not the authorized candidate")
    if candidate.get("c73_remote_evidence_sha256") != c73_sha:
        raise V2DeploymentError("C7.3 v2 evidence SHA is not the authorized evidence")
    if candidate.get("release_id") != release_id:
        raise V2DeploymentError("bundle release_id is not the authorized release")
    if c73.get("bundle_manifest_sha256") != manifest_sha:
        raise V2DeploymentError("C7.3 v2 evidence is bound to another bundle manifest")
    if (c73.get("release") or {}).get("release_id") != release_id:
        raise V2DeploymentError("C7.3 v2 evidence is bound to another release")
    if c73.get("inventory_version") != 2:
        raise V2DeploymentError("C7.3 evidence is not inventory_version=2")
    if candidate.get("managed_files") != EXPECTED_MANAGED_FILES:
        raise V2DeploymentError("authorization managed-file count drift")
    if candidate.get("preexisting_dependencies") != EXPECTED_DEPENDENCIES:
        raise V2DeploymentError("authorization dependency count drift")


def network_health_check_v2(base_url: str, release_id: str) -> dict:
    results = _ORIGINAL_NETWORK_HEALTH_CHECK(base_url, release_id)
    base_url = base_url.rstrip("/")
    status, body, _headers = base.request_url(base_url + "/financas/calculadoras/")
    if status != 200:
        raise V2DeploymentError(f"H25 central returned HTTP {status}")
    text = body.decode("utf-8", errors="replace")
    required = (
        "/financas/calculadoras/salario-liquido-clt/",
        "/financas/calculadoras/decimo-terceiro/",
        "/financas/calculadoras/ferias-clt/",
        "/financas/calculadoras/rescisao-clt/",
        "Não calcula FGTS",
        "aviso prévio",
    )
    missing = [marker for marker in required if marker not in text]
    if missing:
        raise V2DeploymentError(f"H25 central missing required markers: {missing}")
    if "Fatal error" in text or "Parse error" in text:
        raise V2DeploymentError("H25 central contains PHP fatal/parse error")
    results["H25"] = {"http_status": status, "bytes": len(body), "required_markers": "PASS"}
    return results


_ORIGINAL_NETWORK_HEALTH_CHECK = base.network_health_check


@contextmanager
def configured_v2_runtime() -> Iterator[None]:
    old_load_bundle = base.load_bundle
    old_verify_bindings = base.verify_bindings
    old_run_preflight = base.run_preflight
    old_network_health_check = base.network_health_check
    try:
        base.load_bundle = load_bundle_v2
        base.verify_bindings = verify_bindings_v2
        base.run_preflight = run_preflight_v2
        base.network_health_check = network_health_check_v2
        yield
    finally:
        base.load_bundle = old_load_bundle
        base.verify_bindings = old_verify_bindings
        base.run_preflight = old_run_preflight
        base.network_health_check = old_network_health_check


def run_controlled_deployment_v2(
    *,
    bundle_dir: Path,
    site_root: Path,
    wordpress_plugin_dir: Path,
    c73_evidence_path: Path,
    authorization_path: Path,
    authorization_id: str,
    journal_dir: Path,
    health_base_url: str,
    health_checker: Callable[[str, str], dict] | None = None,
    fail_after_writes: int | None = None,
) -> dict:
    kwargs = {
        "bundle_dir": bundle_dir,
        "site_root": site_root,
        "wordpress_plugin_dir": wordpress_plugin_dir,
        "c73_evidence_path": c73_evidence_path,
        "authorization_path": authorization_path,
        "authorization_id": authorization_id,
        "journal_dir": journal_dir,
        "health_base_url": health_base_url,
        "fail_after_writes": fail_after_writes,
    }
    if health_checker is not None:
        kwargs["health_checker"] = health_checker
    with configured_v2_runtime():
        return base.run_controlled_deployment(**kwargs)


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 7 v2 single-use controlled production deployment")
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--site-root", type=Path, required=True)
    parser.add_argument("--wordpress-plugin-dir", type=Path, required=True)
    parser.add_argument("--c73-evidence", type=Path, required=True)
    parser.add_argument("--authorization-file", type=Path, required=True)
    parser.add_argument("--authorization-id", required=True)
    parser.add_argument("--journal-dir", type=Path, required=True)
    parser.add_argument("--health-base-url", required=True)
    args = parser.parse_args()

    try:
        result = run_controlled_deployment_v2(
            bundle_dir=args.bundle_dir,
            site_root=args.site_root,
            wordpress_plugin_dir=args.wordpress_plugin_dir,
            c73_evidence_path=args.c73_evidence,
            authorization_path=args.authorization_file,
            authorization_id=args.authorization_id,
            journal_dir=args.journal_dir,
            health_base_url=args.health_base_url,
        )
    except Exception as exc:
        print(f"Phase 7 v2 controlled deployment failed before/without a completed journal state: {type(exc).__name__}: {exc}")
        return 3

    import json
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if result.get("status") == "APPLIED_HEALTHY" and result.get("production_deployed") is True:
        return 0
    if result.get("status") == "ROLLED_BACK":
        return 4
    return 5


if __name__ == "__main__":
    raise SystemExit(main())
