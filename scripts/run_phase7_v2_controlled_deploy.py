#!/usr/bin/env python3
from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
from typing import Callable

from scripts import run_phase7_c74_controlled_deploy as base
from scripts.deployment_health_v2 import (
    DeliveryHealthError,
    ORIGIN_PENDING_STATUS,
    atomic_json,
    mark_origin_healthy_pending_external,
)
from scripts.run_phase7_v2_host_preflight import run_preflight as run_preflight_v2

EXPECTED_MANAGED_FILES = 33
EXPECTED_DEPENDENCIES = 11
LEGACY_NETWORK_HEALTH_CHECK = base.network_health_check


class V2DeploymentError(base.DeploymentError):
    pass


def sha256_file(path: Path) -> str:
    h = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_json(path: Path) -> dict:
    if not path.is_file():
        raise V2DeploymentError(f"required JSON missing: {path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise V2DeploymentError(f"invalid JSON {path}: {exc}") from exc


def load_bundle_v2(bundle_dir: Path) -> tuple[dict, Path]:
    manifest_path = bundle_dir / "bundle-manifest.json"
    manifest = load_json(manifest_path)
    if manifest.get("schema_version") != "1.0.0" or manifest.get("checkpoint") != "C7.1":
        raise V2DeploymentError("unsupported deployment bundle")
    if manifest.get("inventory_version") != 2:
        raise V2DeploymentError("deployment bundle must use inventory_version=2")
    if manifest.get("production_deployed") is not False:
        raise V2DeploymentError("bundle is not a pre-production C7.1 bundle")
    files = manifest.get("files") or []
    deps = manifest.get("preexisting_dependencies") or []
    if len(files) != EXPECTED_MANAGED_FILES or manifest.get("managed_file_count") != EXPECTED_MANAGED_FILES:
        raise V2DeploymentError(f"v2 bundle must contain exactly {EXPECTED_MANAGED_FILES} managed files")
    if len(deps) != EXPECTED_DEPENDENCIES:
        raise V2DeploymentError(f"v2 bundle must declare exactly {EXPECTED_DEPENDENCIES} preexisting dependencies")
    return manifest, manifest_path


def verify_bindings_v2(bundle: dict, manifest_path: Path, c73_path: Path, c73: dict, auth: dict) -> None:
    candidate = auth.get("candidate") or {}
    manifest_sha = sha256_file(manifest_path)
    c73_sha = sha256_file(c73_path)
    release_id = (bundle.get("release") or {}).get("release_id")

    if candidate.get("inventory_version") != 2:
        raise V2DeploymentError("authorization is not for inventory_version=2")
    if candidate.get("bundle_manifest_sha256") != manifest_sha:
        raise V2DeploymentError("bundle manifest SHA is not the authorized candidate")
    if candidate.get("c73_remote_evidence_sha256") != c73_sha:
        raise V2DeploymentError("C7.3 remote evidence SHA is not the authorized evidence")
    if candidate.get("release_id") != release_id:
        raise V2DeploymentError("bundle release_id is not the authorized release")
    if candidate.get("managed_files") != EXPECTED_MANAGED_FILES:
        raise V2DeploymentError("authorization managed file count drift")
    if candidate.get("preexisting_dependencies") != EXPECTED_DEPENDENCIES:
        raise V2DeploymentError("authorization dependency count drift")

    if c73.get("inventory_version") != 2:
        raise V2DeploymentError("C7.3 evidence is not inventory_version=2")
    if c73.get("bundle_manifest_sha256") != manifest_sha:
        raise V2DeploymentError("C7.3 evidence is bound to another bundle manifest")
    if (c73.get("release") or {}).get("release_id") != release_id:
        raise V2DeploymentError("C7.3 evidence is bound to another release")
    summary = c73.get("summary") or {}
    if summary.get("managed_files_expected") != EXPECTED_MANAGED_FILES or summary.get("managed_files_observed") != EXPECTED_MANAGED_FILES:
        raise V2DeploymentError("C7.3 managed inventory count drift")
    if summary.get("preexisting_dependencies_expected") != EXPECTED_DEPENDENCIES or summary.get("preexisting_dependencies_observed") != EXPECTED_DEPENDENCIES:
        raise V2DeploymentError("C7.3 dependency inventory count drift")


def validate_authorized_candidate(
    *,
    authorization_path: Path,
    authorization_id: str,
    authorized_source_commit: str,
    site_root: Path,
    wordpress_plugin_dir: Path,
) -> dict:
    auth = load_json(authorization_path)
    if auth.get("schema_version") != "1.0.0" or auth.get("checkpoint") != "C7.4":
        raise V2DeploymentError("unsupported C7.4 v2 authorization record")
    if auth.get("record_type") != "single_use_deployment_authorization_v2":
        raise V2DeploymentError("authorization record_type is not v2")
    if auth.get("authorization_id") != authorization_id:
        raise V2DeploymentError("authorization id mismatch")
    if auth.get("single_use_authorization") is not True or auth.get("deployment_authorized") is not True:
        raise V2DeploymentError("deployment is not explicitly authorized for single use")
    if auth.get("production_deployed") is not False:
        raise V2DeploymentError("authorization already claims production deployment")

    commit = str(authorized_source_commit or "").strip().lower()
    if len(commit) != 40 or any(ch not in "0123456789abcdef" for ch in commit):
        raise V2DeploymentError("authorized source commit must be a 40-character hexadecimal SHA")
    candidate = auth.get("candidate") or {}
    if str(candidate.get("source_commit") or "").lower() != commit:
        raise V2DeploymentError("authorized source commit mismatch")

    roots = auth.get("target_roots") or {}
    if Path(str(roots.get("site_root") or "")).resolve() != site_root.resolve():
        raise V2DeploymentError("authorized site_root mismatch")
    if Path(str(roots.get("wordpress_plugin_dir") or "")).resolve() != wordpress_plugin_dir.resolve():
        raise V2DeploymentError("authorized wordpress_plugin_dir mismatch")
    return auth


def network_health_check_v2(base_url: str, release_id: str) -> dict:
    results = LEGACY_NETWORK_HEALTH_CHECK(base_url, release_id)
    base_url = base_url.rstrip("/")
    status, body, _ = base.request_url(base_url + "/financas/calculadoras/")
    text = body.decode("utf-8", errors="ignore")
    if status != 200:
        raise V2DeploymentError(f"H25 page returned HTTP {status}")
    if "Fatal error" in text or "Parse error" in text:
        raise V2DeploymentError("H25 page contains PHP fatal/parse error")
    required_paths = (
        "/financas/calculadoras/salario-liquido-clt/",
        "/financas/calculadoras/decimo-terceiro/",
        "/financas/calculadoras/ferias-clt/",
        "/financas/calculadoras/rescisao-clt/",
    )
    missing = [path for path in required_paths if path not in text]
    if missing:
        raise V2DeploymentError(f"H25 is missing calculator links: {missing}")
    if "Não calcula FGTS" not in text or "aviso prévio" not in text:
        raise V2DeploymentError("H25 rescisão boundary copy is missing")
    results.setdefault("calculators", {})["H25"] = {
        "http_status": status,
        "bytes": len(body),
        "required_links": len(required_paths),
        "rescisao_boundary_copy": True,
    }
    return results


def run_controlled_deployment_v2(
    *,
    authorized_source_commit: str,
    health_checker: Callable[[str, str], dict] | None = None,
    **kwargs,
) -> dict:
    authorization_path = Path(kwargs["authorization_path"]).resolve()
    site_root = Path(kwargs["site_root"]).resolve()
    wordpress_plugin_dir = Path(kwargs["wordpress_plugin_dir"]).resolve()
    authorization_id = str(kwargs["authorization_id"])
    validate_authorized_candidate(
        authorization_path=authorization_path,
        authorization_id=authorization_id,
        authorized_source_commit=authorized_source_commit,
        site_root=site_root,
        wordpress_plugin_dir=wordpress_plugin_dir,
    )

    original_load_bundle = base.load_bundle
    original_preflight = base.run_preflight
    original_verify_bindings = base.verify_bindings
    original_network_health = base.network_health_check
    try:
        base.load_bundle = load_bundle_v2
        base.run_preflight = run_preflight_v2
        base.verify_bindings = verify_bindings_v2
        base.network_health_check = network_health_check_v2
        if health_checker is not None:
            kwargs["health_checker"] = health_checker
        state = base.run_controlled_deployment(**kwargs)
    finally:
        base.load_bundle = original_load_bundle
        base.run_preflight = original_preflight
        base.verify_bindings = original_verify_bindings
        base.network_health_check = original_network_health

    if state.get("status") == "APPLIED_HEALTHY" and state.get("production_deployed") is True:
        state = mark_origin_healthy_pending_external(state, authorized_commit=authorized_source_commit)
        journal_dir = Path(kwargs["journal_dir"]).resolve()
        atomic_json(journal_dir / "deployment-state.json", state)
    return state


def main() -> int:
    parser = argparse.ArgumentParser(description="C7.4 inventory-v2 single-use controlled production deployment")
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--site-root", type=Path, required=True)
    parser.add_argument("--wordpress-plugin-dir", type=Path, required=True)
    parser.add_argument("--c73-evidence", type=Path, required=True)
    parser.add_argument("--authorization-file", type=Path, required=True)
    parser.add_argument("--authorization-id", required=True)
    parser.add_argument("--authorized-source-commit", required=True)
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
            authorized_source_commit=args.authorized_source_commit,
            journal_dir=args.journal_dir,
            health_base_url=args.health_base_url,
        )
    except (base.DeploymentError, V2DeploymentError, DeliveryHealthError, SystemExit, ValueError) as exc:
        print(json.dumps({"checkpoint": "C7.4", "inventory_version": 2, "status": "BLOCKED_BEFORE_WRITE", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 3

    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if result.get("status") == ORIGIN_PENDING_STATUS and result.get("production_deployed") is True:
        return 0
    if result.get("status") == "ROLLED_BACK":
        return 4
    return 5


if __name__ == "__main__":
    raise SystemExit(main())
