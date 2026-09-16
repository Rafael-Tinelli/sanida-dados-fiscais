#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import time
from typing import Callable, Dict, List, Optional, Tuple
from urllib.error import HTTPError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_C74_STATE = ROOT / "state/phase7-c74-deployment.json"
DEFAULT_C74_RECORD = ROOT / "docs/phase7-c74-production-deployment-record.json"
DEFAULT_POINTER_URL = "https://raw.githubusercontent.com/Rafael-Tinelli/sanida-dados-fiscais/main/releases/fiscal-v1/current.json"
DEFAULT_BASE_URL = "https://sanida.com.br"

CALCULATORS = {
    "H26": ("/financas/calculadoras/salario-liquido-clt/", ["folha-core.js", "salario-liquido.js"]),
    "H27": ("/financas/calculadoras/decimo-terceiro/", ["folha-core.js", "folha-thirteenth.js", "decimo-terceiro.js"]),
    "H28": ("/financas/calculadoras/ferias-clt/", ["folha-core.js", "folha-vacation.js", "ferias-clt.js"]),
    "H29": ("/financas/calculadoras/rescisao-clt/", ["folha-core.js", "folha-termination.js", "rescisao-clt.js"]),
}


def fail(message: str) -> None:
    raise SystemExit(f"C7.5 post-deploy validation failed: {message}")


def sha256_bytes(body: bytes) -> str:
    return sha256(body).hexdigest()


def sha256_file(path: Path) -> str:
    h = sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def load_json(path: Path, label: str) -> dict:
    if not path.is_file():
        fail(f"{label} missing: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        fail(f"{label} is not valid JSON: {exc}")
    if not isinstance(value, dict):
        fail(f"{label} must be an object")
    return value


def default_fetch(url: str, timeout: int = 30) -> dict:
    request = Request(
        url,
        headers={
            "Accept": "application/json, text/plain, text/html, */*",
            "User-Agent": "Sanida-C7.5-PostDeploy/1.0 (+https://sanida.com.br)",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read()
            return {
                "url": url,
                "http_status": int(response.status),
                "headers": {str(k).lower(): str(v) for k, v in response.headers.items()},
                "body": body,
                "error": None,
            }
    except HTTPError as exc:
        return {
            "url": url,
            "http_status": int(exc.code),
            "headers": {str(k).lower(): str(v) for k, v in exc.headers.items()},
            "body": exc.read(),
            "error": None,
        }
    except Exception as exc:
        return {
            "url": url,
            "http_status": None,
            "headers": {},
            "body": b"",
            "error": f"{type(exc).__name__}: {exc}",
        }


def decode_json_response(response: dict) -> Optional[dict]:
    try:
        value = json.loads(response.get("body", b"").decode("utf-8"))
    except Exception:
        return None
    return value if isinstance(value, dict) else None


def target_for(record: dict, roots: Dict[str, Path]) -> Path:
    root_name = str(record.get("target_root", ""))
    if root_name not in roots:
        fail(f"unknown target root: {root_name}")
    return roots[root_name] / safe_relative(str(record.get("target_path", "")))


def run_validation(
    *,
    bundle_manifest_path: Path,
    c73_evidence_path: Path,
    deployment_state_path: Path,
    c74_state_path: Path,
    c74_record_path: Path,
    site_root: Path,
    wordpress_plugin_dir: Path,
    base_url: str = DEFAULT_BASE_URL,
    pointer_url: str = DEFAULT_POINTER_URL,
    min_window_seconds: int = 600,
    probe_count: int = 2,
    probe_interval_seconds: float = 2.0,
    fetcher: Callable[[str], dict] = default_fetch,
    now: Optional[datetime] = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict:
    started = now or datetime.now(timezone.utc)
    roots = {
        "site_root": site_root.resolve(),
        "wordpress_plugin_dir": wordpress_plugin_dir.resolve(),
    }
    block_reasons: List[str] = []

    c74 = load_json(c74_state_path, "C7.4 state")
    record = load_json(c74_record_path, "C7.4 production record")
    manifest = load_json(bundle_manifest_path, "C7.4 bundle manifest")
    c73 = load_json(c73_evidence_path, "C7.3 host evidence")
    deployment_state = load_json(deployment_state_path, "C7.4 deployment state")

    if c74.get("status") != "CONCLUÍDO" or c74.get("production_deployed") is not True or c74.get("post_deploy_validated") is not True:
        block_reasons.append("c74_not_formally_concluded")
    if c74.get("rollback_performed") is not False:
        block_reasons.append("c74_repository_state_reports_rollback")
    if record.get("status") != "APPLIED_HEALTHY" or record.get("production_deployed") is not True or record.get("rollback_performed") is not False:
        block_reasons.append("c74_production_record_not_applied_healthy")

    expected_bundle_sha = str(record.get("bundle_manifest_sha256") or "")
    observed_bundle_sha = sha256_file(bundle_manifest_path)
    if observed_bundle_sha != expected_bundle_sha:
        block_reasons.append("bundle_manifest_sha_drift")

    expected_c73_sha = str(record.get("c73_remote_evidence_sha256") or "")
    observed_c73_sha = sha256_file(c73_evidence_path)
    if observed_c73_sha != expected_c73_sha:
        block_reasons.append("c73_evidence_sha_drift")

    expected_state_sha = str((record.get("deployment_state") or {}).get("sha256") or "")
    observed_state_sha = sha256_file(deployment_state_path)
    if observed_state_sha != expected_state_sha:
        block_reasons.append("deployment_state_sha_drift")

    if deployment_state.get("status") != "APPLIED_HEALTHY":
        block_reasons.append("deployment_state_not_applied_healthy")
    if deployment_state.get("production_deployed") is not True:
        block_reasons.append("deployment_state_not_deployed")
    if deployment_state.get("rollback_performed") is not False:
        block_reasons.append("deployment_state_reports_rollback")
    if deployment_state.get("error") is not None:
        block_reasons.append("deployment_state_has_error")

    completed_raw = str(deployment_state.get("completed_at_utc") or record.get("completed_at_utc") or "")
    try:
        completed = parse_utc(completed_raw)
        validation_age_seconds = max(0.0, (started - completed).total_seconds())
    except Exception:
        completed = None
        validation_age_seconds = 0.0
        block_reasons.append("deployment_completion_time_invalid")
    if validation_age_seconds < float(min_window_seconds):
        block_reasons.append("postdeploy_window_too_short")

    files = manifest.get("files") or []
    dependencies_manifest = manifest.get("preexisting_dependencies") or []
    if len(files) != 32:
        block_reasons.append("bundle_managed_count_not_32")
    if len(dependencies_manifest) != 11:
        block_reasons.append("bundle_dependency_count_not_11")

    managed_results = []
    managed_match_count = 0
    managed_parents = set()
    for item in files:
        target = target_for(item, roots)
        managed_parents.add(target.parent)
        exists = target.is_file() and not target.is_symlink()
        observed_sha = sha256_file(target) if exists else None
        expected_sha = str(item.get("sha256") or "")
        match = exists and observed_sha == expected_sha
        if match:
            managed_match_count += 1
        else:
            block_reasons.append(f"managed_file_drift:{item.get('target_root')}:{item.get('target_path')}")
        managed_results.append(
            {
                "target_root": item.get("target_root"),
                "target_path": item.get("target_path"),
                "exists_regular_not_symlink": exists,
                "expected_sha256": expected_sha,
                "observed_sha256": observed_sha,
                "match": match,
            }
        )

    c73_dependencies = c73.get("dependencies") or []
    c73_dependency_map = {
        (str(item.get("target_root")), str(item.get("relative_path"))): item
        for item in c73_dependencies
    }
    dependency_results = []
    dependency_match_count = 0
    for item in dependencies_manifest:
        key = (str(item.get("target_root")), str(item.get("target_path")))
        baseline = c73_dependency_map.get(key) or {}
        target = target_for(item, roots)
        exists = target.is_file() and not target.is_symlink()
        observed_sha = sha256_file(target) if exists else None
        expected_sha = baseline.get("sha256")
        match = bool(expected_sha) and exists and observed_sha == expected_sha
        if match:
            dependency_match_count += 1
        else:
            block_reasons.append(f"dependency_drift:{key[0]}:{key[1]}")
        dependency_results.append(
            {
                "target_root": key[0],
                "target_path": key[1],
                "exists_regular_not_symlink": exists,
                "expected_sha256": expected_sha,
                "observed_sha256": observed_sha,
                "match": match,
            }
        )

    created_directories = deployment_state.get("created_directories") or []
    created_directory_results = []
    for item in created_directories:
        root_name = str(item.get("target_root", ""))
        rel = str(item.get("relative_path", ""))
        target = roots[root_name] / safe_relative(rel) if root_name in roots else None
        present = bool(target and target.is_dir() and not target.is_symlink())
        if not present:
            block_reasons.append(f"created_directory_missing_or_changed:{root_name}:{rel}")
        created_directory_results.append({"target_root": root_name, "relative_path": rel, "present_directory_not_symlink": present})
    if len(created_directory_results) != 4:
        block_reasons.append("created_directory_count_not_4")

    leftover_temp_files = []
    for parent in sorted(managed_parents, key=str):
        if not parent.is_dir():
            continue
        for candidate in parent.glob(".*.c74-*"):
            leftover_temp_files.append(str(candidate))
    if leftover_temp_files:
        block_reasons.append("c74_temporary_files_left_behind")

    pointer_response = fetcher(pointer_url)
    pointer = decode_json_response(pointer_response)
    if pointer_response.get("http_status") != 200 or pointer is None:
        block_reasons.append("canonical_current_pointer_unavailable_or_invalid")
        pointer = {}
    canonical_release_id = str(pointer.get("release_id") or "")
    artifact_rel = str(pointer.get("artifact") or "")
    artifact_url = urljoin(pointer_url, artifact_rel) if artifact_rel else None
    artifact_response = fetcher(artifact_url) if artifact_url else {"http_status": None, "headers": {}, "body": b"", "error": "missing artifact"}
    artifact_sha = sha256_bytes(artifact_response.get("body", b"")) if artifact_response.get("http_status") == 200 else None
    artifact = decode_json_response(artifact_response)
    if artifact_response.get("http_status") != 200:
        block_reasons.append("canonical_artifact_unavailable")
    if artifact_sha != pointer.get("artifact_sha256"):
        block_reasons.append("canonical_artifact_sha_mismatch")
    if not isinstance(artifact, dict) or artifact.get("release_id") != canonical_release_id:
        block_reasons.append("canonical_artifact_release_mismatch")

    expected_contract = "1.2.0"
    if pointer.get("contract_api_version") != expected_contract or pointer.get("schema_version_contract") != expected_contract:
        block_reasons.append("canonical_pointer_contract_version_drift")

    probes = []
    for probe_index in range(max(1, probe_count)):
        health_response = fetcher(base_url.rstrip("/") + "/blog/wp-json/sfa/v1/fiscal-health")
        health = decode_json_response(health_response) or {}
        release_response = fetcher(base_url.rstrip("/") + "/blog/wp-json/sfa/v1/fiscal-release")
        release = decode_json_response(release_response) or {}
        folha_response = fetcher(base_url.rstrip("/") + "/blog/wp-json/sfa/v1/folha")

        health_headers = health_response.get("headers") or {}
        release_headers = release_response.get("headers") or {}
        if health_response.get("http_status") != 200:
            block_reasons.append(f"probe_{probe_index + 1}:fiscal_health_http_not_200")
        if health.get("status") != "healthy":
            block_reasons.append(f"probe_{probe_index + 1}:fiscal_health_not_healthy")
        if health.get("release_id") != canonical_release_id:
            block_reasons.append(f"probe_{probe_index + 1}:fiscal_health_release_mismatch")
        if "no-store" not in str(health_headers.get("cache-control", "")).lower():
            block_reasons.append(f"probe_{probe_index + 1}:fiscal_health_cache_control_drift")
        if str(health_headers.get("x-sanida-fiscal-status", "")) != "healthy":
            block_reasons.append(f"probe_{probe_index + 1}:fiscal_health_header_status_drift")
        if str(health_headers.get("x-sanida-fiscal-release", "")) != canonical_release_id:
            block_reasons.append(f"probe_{probe_index + 1}:fiscal_health_header_release_drift")

        if release_response.get("http_status") != 200:
            block_reasons.append(f"probe_{probe_index + 1}:fiscal_release_http_not_200")
        if release.get("release_id") != canonical_release_id:
            block_reasons.append(f"probe_{probe_index + 1}:fiscal_release_body_mismatch")
        if str(release_headers.get("x-sanida-fiscal-release", "")) != canonical_release_id:
            block_reasons.append(f"probe_{probe_index + 1}:fiscal_release_header_mismatch")
        if folha_response.get("http_status") != 410:
            block_reasons.append(f"probe_{probe_index + 1}:legacy_folha_not_410")

        calculators = {}
        for key, (path, required_tokens) in CALCULATORS.items():
            response = fetcher(base_url.rstrip("/") + path)
            body_text = response.get("body", b"").decode("utf-8", errors="replace")
            fatal = "Fatal error" in body_text or "Parse error" in body_text
            tokens_present = all(token in body_text for token in required_tokens)
            if response.get("http_status") != 200:
                block_reasons.append(f"probe_{probe_index + 1}:{key}_http_not_200")
            if fatal:
                block_reasons.append(f"probe_{probe_index + 1}:{key}_php_error_exposed")
            if not tokens_present:
                block_reasons.append(f"probe_{probe_index + 1}:{key}_expected_assets_not_referenced")
            calculators[key] = {
                "http_status": response.get("http_status"),
                "bytes": len(response.get("body", b"")),
                "fatal_or_parse_error": fatal,
                "expected_assets_referenced": tokens_present,
            }

        probes.append(
            {
                "probe": probe_index + 1,
                "observed_at_utc": datetime.now(timezone.utc).isoformat(),
                "fiscal_health": {
                    "http_status": health_response.get("http_status"),
                    "status": health.get("status"),
                    "release_id": health.get("release_id"),
                    "cache_control": health_headers.get("cache-control"),
                    "header_status": health_headers.get("x-sanida-fiscal-status"),
                    "header_release_id": health_headers.get("x-sanida-fiscal-release"),
                },
                "fiscal_release": {
                    "http_status": release_response.get("http_status"),
                    "release_id": release.get("release_id"),
                    "header_release_id": release_headers.get("x-sanida-fiscal-release"),
                },
                "legacy_folha": {"http_status": folha_response.get("http_status")},
                "calculators": calculators,
            }
        )
        if probe_index + 1 < max(1, probe_count) and probe_interval_seconds > 0:
            sleeper(probe_interval_seconds)

    public_js = []
    for item in files:
        if item.get("target_root") != "site_root" or not str(item.get("target_path", "")).endswith(".js"):
            continue
        url = base_url.rstrip("/") + "/" + str(item["target_path"]).lstrip("/")
        response = fetcher(url)
        observed_sha = sha256_bytes(response.get("body", b"")) if response.get("http_status") == 200 else None
        expected_sha = str(item.get("sha256") or "")
        match = response.get("http_status") == 200 and observed_sha == expected_sha
        if not match:
            block_reasons.append(f"public_js_delivery_drift:{item.get('target_path')}")
        public_js.append(
            {
                "target_path": item.get("target_path"),
                "http_status": response.get("http_status"),
                "expected_sha256": expected_sha,
                "observed_sha256": observed_sha,
                "match": match,
            }
        )
    if len(public_js) != 8:
        block_reasons.append("public_js_count_not_8")

    block_reasons = sorted(set(block_reasons))
    status = "PASS" if not block_reasons else "BLOCKED"
    finished = datetime.now(timezone.utc)
    return {
        "schema_version": "1.0.0",
        "checkpoint": "C7.5",
        "mode": "post_deploy_read_only_validation",
        "status": status,
        "phase7_close_recommended": status == "PASS",
        "production_deployed": True,
        "production_mutated": False,
        "validation_started_at_utc": started.isoformat(),
        "validation_completed_at_utc": finished.isoformat(),
        "postdeploy_window_seconds": validation_age_seconds,
        "minimum_postdeploy_window_seconds": min_window_seconds,
        "authorization_id": record.get("authorization_id"),
        "release_id": canonical_release_id,
        "journal": {
            "expected_sha256": expected_state_sha,
            "observed_sha256": observed_state_sha,
            "sha256_match": observed_state_sha == expected_state_sha,
            "status": deployment_state.get("status"),
            "rollback_performed": deployment_state.get("rollback_performed"),
        },
        "bundle": {
            "expected_manifest_sha256": expected_bundle_sha,
            "observed_manifest_sha256": observed_bundle_sha,
            "manifest_sha256_match": observed_bundle_sha == expected_bundle_sha,
            "managed_files_expected": 32,
            "managed_files_matching": managed_match_count,
        },
        "dependencies": {
            "c73_evidence_expected_sha256": expected_c73_sha,
            "c73_evidence_observed_sha256": observed_c73_sha,
            "c73_evidence_sha256_match": observed_c73_sha == expected_c73_sha,
            "expected": 11,
            "matching": dependency_match_count,
        },
        "filesystem": {
            "managed_files": managed_results,
            "preexisting_dependencies": dependency_results,
            "created_directories": created_directory_results,
            "c74_temporary_files": leftover_temp_files,
        },
        "canonical_source": {
            "current_url": pointer_url,
            "current_http_status": pointer_response.get("http_status"),
            "release_id": canonical_release_id,
            "artifact_url": artifact_url,
            "artifact_http_status": artifact_response.get("http_status"),
            "artifact_expected_sha256": pointer.get("artifact_sha256"),
            "artifact_observed_sha256": artifact_sha,
            "artifact_sha256_match": artifact_sha == pointer.get("artifact_sha256"),
            "contract_api_version": pointer.get("contract_api_version"),
            "schema_version_contract": pointer.get("schema_version_contract"),
        },
        "http_probes": probes,
        "public_js": public_js,
        "block_reasons": block_reasons,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="C7.5 read-only post-deploy validation")
    parser.add_argument("--bundle-manifest", type=Path, required=True)
    parser.add_argument("--c73-evidence", type=Path, required=True)
    parser.add_argument("--deployment-state", type=Path, required=True)
    parser.add_argument("--site-root", type=Path, required=True)
    parser.add_argument("--wordpress-plugin-dir", type=Path, required=True)
    parser.add_argument("--c74-state", type=Path, default=DEFAULT_C74_STATE)
    parser.add_argument("--c74-record", type=Path, default=DEFAULT_C74_RECORD)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--pointer-url", default=DEFAULT_POINTER_URL)
    parser.add_argument("--min-window-seconds", type=int, default=600)
    parser.add_argument("--probe-count", type=int, default=2)
    parser.add_argument("--probe-interval-seconds", type=float, default=2.0)
    parser.add_argument("--evidence-out", type=Path)
    args = parser.parse_args()

    site_root = args.site_root.resolve()
    plugin_root = args.wordpress_plugin_dir.resolve()
    evidence_out = args.evidence_out.resolve() if args.evidence_out else None
    if evidence_out and (path_is_within(evidence_out, site_root) or path_is_within(evidence_out, plugin_root)):
        fail("--evidence-out must remain outside production roots")

    result = run_validation(
        bundle_manifest_path=args.bundle_manifest.resolve(),
        c73_evidence_path=args.c73_evidence.resolve(),
        deployment_state_path=args.deployment_state.resolve(),
        c74_state_path=args.c74_state.resolve(),
        c74_record_path=args.c74_record.resolve(),
        site_root=site_root,
        wordpress_plugin_dir=plugin_root,
        base_url=args.base_url,
        pointer_url=args.pointer_url,
        min_window_seconds=args.min_window_seconds,
        probe_count=args.probe_count,
        probe_interval_seconds=args.probe_interval_seconds,
    )
    payload = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if evidence_out:
        evidence_out.parent.mkdir(parents=True, exist_ok=True)
        evidence_out.write_text(payload, encoding="utf-8")
    print(payload, end="")
    return 0 if result["status"] == "PASS" else 3


if __name__ == "__main__":
    raise SystemExit(main())
