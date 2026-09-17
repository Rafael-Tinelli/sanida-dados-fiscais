from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import tempfile

from scripts.build_phase7_c71_bundle_v2 import build_bundle
from scripts.deployment_health_v2 import ORIGIN_PENDING_STATUS
from scripts.run_phase7_v2_controlled_deploy import run_controlled_deployment_v2
from scripts.run_phase7_v2_host_preflight import run_preflight
from scripts.simulate_phase7_c73_preflight import seed_host, tree_snapshot

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v2.json"
SOURCE_COMMIT = "a" * 40


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def prepare(tmp: Path, authorization_id: str):
    bundle_dir = tmp / "bundle"
    bundle = build_bundle(MANIFEST, bundle_dir)
    roots = seed_host(bundle, tmp / "host", leave_creatable_parents_missing=False)
    evidence = run_preflight(bundle_dir / "bundle-manifest.json", roots["site_root"], roots["wordpress_plugin_dir"])
    assert evidence["status"] == "PASS"
    assert evidence["technical_go_no_go"] == "GO"
    evidence_path = tmp / "preflight.json"
    write_json(evidence_path, evidence)

    auth = {
        "schema_version": "1.0.0",
        "checkpoint": "C7.4",
        "record_type": "single_use_deployment_authorization_v2",
        "status": "AUTHORIZED_READY_TO_DEPLOY",
        "authorization_id": authorization_id,
        "single_use_authorization": True,
        "deployment_authorized": True,
        "production_deployed": False,
        "candidate": {
            "source_commit": SOURCE_COMMIT,
            "inventory_version": 2,
            "bundle_manifest_sha256": sha(bundle_dir / "bundle-manifest.json"),
            "c73_remote_evidence_sha256": sha(evidence_path),
            "release_id": bundle["release"]["release_id"],
            "managed_files": 33,
            "preexisting_dependencies": 11,
            "planned_directory_creations": evidence["summary"]["planned_directory_creations"],
        },
        "target_roots": {
            "site_root": str(roots["site_root"].resolve()),
            "wordpress_plugin_dir": str(roots["wordpress_plugin_dir"].resolve()),
        },
    }
    auth_path = tmp / "authorization.json"
    write_json(auth_path, auth)
    return bundle_dir, roots, evidence_path, auth_path


def healthy(_base_url: str, _release_id: str) -> dict:
    return {"simulation": "PASS"}


def test_v2_deploy_applies_33_files_and_enters_external_pending_state() -> None:
    with tempfile.TemporaryDirectory(prefix="phase7-v2-deploy-") as raw:
        tmp = Path(raw)
        authorization_id = "c74-v2-sim-success"
        bundle_dir, roots, evidence_path, auth_path = prepare(tmp, authorization_id)
        journal = tmp / "journal" / authorization_id

        state = run_controlled_deployment_v2(
            bundle_dir=bundle_dir,
            site_root=roots["site_root"],
            wordpress_plugin_dir=roots["wordpress_plugin_dir"],
            c73_evidence_path=evidence_path,
            authorization_path=auth_path,
            authorization_id=authorization_id,
            authorized_source_commit=SOURCE_COMMIT,
            journal_dir=journal,
            health_base_url="https://example.invalid",
            health_checker=healthy,
        )

        assert state["status"] == ORIGIN_PENDING_STATUS
        assert state["production_deployed"] is True
        assert state["origin_health_verified"] is True
        assert state["external_delivery_verified"] is False
        assert len(state["applied_files"]) == 33
        manifest = json.loads((bundle_dir / "bundle-manifest.json").read_text(encoding="utf-8"))
        for record in manifest["files"]:
            target = roots[record["target_root"]] / record["target_path"]
            assert target.is_file()
            assert sha(target) == record["sha256"]


def test_v2_deploy_rolls_back_exactly_after_injected_failure() -> None:
    with tempfile.TemporaryDirectory(prefix="phase7-v2-rollback-") as raw:
        tmp = Path(raw)
        authorization_id = "c74-v2-sim-rollback"
        bundle_dir, roots, evidence_path, auth_path = prepare(tmp, authorization_id)
        host_root = tmp / "host"
        before = tree_snapshot(host_root)
        journal = tmp / "journal" / authorization_id

        state = run_controlled_deployment_v2(
            bundle_dir=bundle_dir,
            site_root=roots["site_root"],
            wordpress_plugin_dir=roots["wordpress_plugin_dir"],
            c73_evidence_path=evidence_path,
            authorization_path=auth_path,
            authorization_id=authorization_id,
            authorized_source_commit=SOURCE_COMMIT,
            journal_dir=journal,
            health_base_url="https://example.invalid",
            health_checker=healthy,
            fail_after_writes=5,
        )

        after = tree_snapshot(host_root)
        assert state["status"] == "ROLLED_BACK"
        assert state["production_deployed"] is False
        assert state["rollback_performed"] is True
        assert state["rollback"]["rollback_verified"] is True
        assert before == after
