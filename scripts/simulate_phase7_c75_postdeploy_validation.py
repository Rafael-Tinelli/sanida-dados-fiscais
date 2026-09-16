#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
from pathlib import Path
import tempfile

from scripts.run_phase7_c75_postdeploy_validation import CALCULATORS, run_validation

ROOT = Path(__file__).resolve().parents[1]
SOURCE_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"
RELEASE_ID = "fiscal-v1-sha256-a741aa7873950d029a5c6b1c929727267125424013f09c69137b7e80b294153e"
AUTH_ID = "c74-20260916-a741aa78-843dca3e"
POINTER_URL = "https://raw.githubusercontent.com/Rafael-Tinelli/sanida-dados-fiscais/main/releases/fiscal-v1/current.json"
BASE_URL = "https://sanida.com.br"


def digest(body: bytes) -> str:
    return sha256(body).hexdigest()


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def tree_signature(roots: list[Path]) -> dict:
    result = {}
    for root in roots:
        for path in sorted(root.rglob("*")):
            rel = f"{root.name}/{path.relative_to(root).as_posix()}"
            if path.is_file():
                result[rel] = ("file", digest(path.read_bytes()))
            elif path.is_dir():
                result[rel] = ("dir", None)
    return result


def build_fixture(base: Path) -> dict:
    source = json.loads(SOURCE_MANIFEST.read_text(encoding="utf-8"))
    site = base / "site"
    plugin = base / "plugin"
    site.mkdir()
    plugin.mkdir()
    roots = {"site_root": site, "wordpress_plugin_dir": plugin}

    records = []
    bodies = {}
    for item in source["managed_files"]:
        target_path = item["target_path"]
        body = (f"managed::{item['target_root']}::{target_path}\n").encode()
        target = roots[item["target_root"]] / target_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        bodies[(item["target_root"], target_path)] = body
        records.append(
            {
                "source": item["source"],
                "target_root": item["target_root"],
                "target_path": target_path,
                "bundle_path": f"{source['bundle_roots'][item['target_root']]}/{target_path}",
                "sha256": digest(body),
                "bytes": len(body),
            }
        )

    dependency_records = []
    for item in source["preexisting_dependencies"]:
        body = (f"dependency::{item['target_root']}::{item['target_path']}\n").encode()
        target = roots[item["target_root"]] / item["target_path"]
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
        dependency_records.append(
            {
                "target_root": item["target_root"],
                "relative_path": item["target_path"],
                "sha256": digest(body),
                "exists": True,
                "is_file": True,
                "is_symlink": False,
            }
        )

    bundle_manifest = {
        "schema_version": "1.0.0",
        "checkpoint": "C7.1",
        "production_deployed": False,
        "bundle_roots": source["bundle_roots"],
        "managed_file_count": len(records),
        "files": sorted(records, key=lambda item: item["bundle_path"]),
        "preexisting_dependencies": source["preexisting_dependencies"],
        "release": {"release_id": RELEASE_ID},
    }
    bundle_manifest_path = base / "bundle-manifest.json"
    write_json(bundle_manifest_path, bundle_manifest)

    c73 = {"schema_version": "1.1.0", "checkpoint": "C7.3", "dependencies": dependency_records}
    c73_path = base / "c73.json"
    write_json(c73_path, c73)

    created_directories = [
        {"target_root": "wordpress_plugin_dir", "relative_path": "includes"},
        {"target_root": "site_root", "relative_path": "financas/calculadoras/decimo-terceiro/parts"},
        {"target_root": "site_root", "relative_path": "financas/calculadoras/ferias-clt/parts"},
        {"target_root": "site_root", "relative_path": "financas/calculadoras/rescisao-clt/parts"},
    ]
    for item in created_directories:
        (roots[item["target_root"]] / item["relative_path"]).mkdir(parents=True, exist_ok=True)

    now = datetime(2026, 9, 16, 18, 0, tzinfo=timezone.utc)
    deployment_state = {
        "status": "APPLIED_HEALTHY",
        "production_deployed": True,
        "rollback_performed": False,
        "error": None,
        "release_id": RELEASE_ID,
        "completed_at_utc": (now - timedelta(minutes=20)).isoformat(),
        "created_directories": created_directories,
    }
    deployment_state_path = base / "deployment-state.json"
    write_json(deployment_state_path, deployment_state)

    c74_record = {
        "status": "APPLIED_HEALTHY",
        "production_deployed": True,
        "rollback_performed": False,
        "authorization_id": AUTH_ID,
        "bundle_manifest_sha256": digest(bundle_manifest_path.read_bytes()),
        "c73_remote_evidence_sha256": digest(c73_path.read_bytes()),
        "deployment_state": {"sha256": digest(deployment_state_path.read_bytes())},
    }
    c74_record_path = base / "c74-record.json"
    write_json(c74_record_path, c74_record)

    c74_state = {
        "checkpoint": "C7.4",
        "status": "CONCLUÍDO",
        "authorization_id": AUTH_ID,
        "production_deployed": True,
        "post_deploy_validated": True,
        "rollback_performed": False,
    }
    c74_state_path = base / "c74-state.json"
    write_json(c74_state_path, c74_state)

    artifact_rel = "releases/fiscal-v1-sha256-a741aa7873950d029a5c6b1c929727267125424013f09c69137b7e80b294153e.json"
    artifact_body = json.dumps({"release_id": RELEASE_ID, "rules": [1] * 32}, separators=(",", ":")).encode()
    pointer_body = json.dumps(
        {
            "release_id": RELEASE_ID,
            "artifact": artifact_rel,
            "artifact_sha256": digest(artifact_body),
            "contract_api_version": "1.2.0",
            "schema_version_contract": "1.2.0",
        },
        separators=(",", ":"),
    ).encode()
    artifact_url = POINTER_URL.rsplit("/", 1)[0] + "/" + artifact_rel

    responses = {
        POINTER_URL: (200, {}, pointer_body),
        artifact_url: (200, {}, artifact_body),
        BASE_URL + "/blog/wp-json/sfa/v1/fiscal-health": (
            200,
            {
                "cache-control": "no-store",
                "x-sanida-fiscal-status": "healthy",
                "x-sanida-fiscal-release": RELEASE_ID,
            },
            json.dumps({"status": "healthy", "release_id": RELEASE_ID}).encode(),
        ),
        BASE_URL + "/blog/wp-json/sfa/v1/fiscal-release": (
            200,
            {"x-sanida-fiscal-release": RELEASE_ID},
            json.dumps({"release_id": RELEASE_ID}).encode(),
        ),
        BASE_URL + "/blog/wp-json/sfa/v1/folha": (410, {}, b"gone"),
    }
    for key, (path, tokens) in CALCULATORS.items():
        responses[BASE_URL + path] = (200, {}, ("<html>" + " ".join(tokens) + "</html>").encode())
    for item in records:
        if item["target_root"] == "site_root" and item["target_path"].endswith(".js"):
            responses[BASE_URL + "/" + item["target_path"]] = (200, {}, bodies[("site_root", item["target_path"])])

    def fetcher(url: str) -> dict:
        status, headers, body = responses.get(url, (404, {}, b"missing"))
        return {"url": url, "http_status": status, "headers": headers, "body": body, "error": None}

    return {
        "site": site,
        "plugin": plugin,
        "bundle_manifest": bundle_manifest_path,
        "c73": c73_path,
        "deployment_state": deployment_state_path,
        "c74_state": c74_state_path,
        "c74_record": c74_record_path,
        "now": now,
        "fetcher": fetcher,
        "responses": responses,
        "records": records,
    }


def validate_fixture(fixture: dict, fetcher=None) -> dict:
    return run_validation(
        bundle_manifest_path=fixture["bundle_manifest"],
        c73_evidence_path=fixture["c73"],
        deployment_state_path=fixture["deployment_state"],
        c74_state_path=fixture["c74_state"],
        c74_record_path=fixture["c74_record"],
        site_root=fixture["site"],
        wordpress_plugin_dir=fixture["plugin"],
        base_url=BASE_URL,
        pointer_url=POINTER_URL,
        min_window_seconds=600,
        probe_count=2,
        probe_interval_seconds=0,
        fetcher=fetcher or fixture["fetcher"],
        now=fixture["now"],
        sleeper=lambda _: None,
    )


def run_simulation() -> dict:
    with tempfile.TemporaryDirectory(prefix="c75-sim-") as raw:
        base = Path(raw)
        fixture = build_fixture(base)
        before = tree_signature([fixture["site"], fixture["plugin"]])
        positive = validate_fixture(fixture)
        after_positive = tree_signature([fixture["site"], fixture["plugin"]])

        first = fixture["records"][0]
        drift_path = (fixture["site"] if first["target_root"] == "site_root" else fixture["plugin"]) / first["target_path"]
        original = drift_path.read_bytes()
        drift_path.write_bytes(original + b"drift")
        drift_result = validate_fixture(fixture)
        drift_path.write_bytes(original)

        stale_target = next(item for item in fixture["records"] if item["target_root"] == "site_root" and item["target_path"].endswith(".js"))
        stale_url = BASE_URL + "/" + stale_target["target_path"]

        def stale_fetcher(url: str) -> dict:
            result = fixture["fetcher"](url)
            if url == stale_url:
                result = dict(result)
                result["body"] = b"stale-public-cache"
            return result

        stale_result = validate_fixture(fixture, fetcher=stale_fetcher)
        after_all = tree_signature([fixture["site"], fixture["plugin"]])

        return {
            "schema_version": "1.0.0",
            "checkpoint": "C7.5",
            "positive_pass": positive.get("status") == "PASS" and positive.get("phase7_close_recommended") is True,
            "positive_managed_32": (positive.get("bundle") or {}).get("managed_files_matching") == 32,
            "positive_dependencies_11": (positive.get("dependencies") or {}).get("matching") == 11,
            "positive_public_js_8": len(positive.get("public_js") or []) == 8 and all(item.get("match") for item in positive.get("public_js") or []),
            "positive_two_http_probes": len(positive.get("http_probes") or []) == 2,
            "postdeploy_window_verified": float(positive.get("postdeploy_window_seconds", 0)) >= 600,
            "managed_drift_blocked": drift_result.get("status") == "BLOCKED" and any(str(reason).startswith("managed_file_drift:") for reason in drift_result.get("block_reasons") or []),
            "stale_public_js_blocked": stale_result.get("status") == "BLOCKED" and any(str(reason).startswith("public_js_delivery_drift:") for reason in stale_result.get("block_reasons") or []),
            "read_only_positive": before == after_positive,
            "read_only_all_scenarios": before == after_all,
            "production_mutated": False,
        }


def main() -> int:
    result = run_simulation()
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    required = [
        "positive_pass",
        "positive_managed_32",
        "positive_dependencies_11",
        "positive_public_js_8",
        "positive_two_http_probes",
        "postdeploy_window_verified",
        "managed_drift_blocked",
        "stale_public_js_blocked",
        "read_only_positive",
        "read_only_all_scenarios",
    ]
    return 0 if all(result.get(key) is True for key in required) else 3


if __name__ == "__main__":
    raise SystemExit(main())
