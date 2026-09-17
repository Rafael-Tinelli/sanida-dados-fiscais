from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import tempfile

from scripts.build_phase7_c71_bundle_v2 import build_bundle
from scripts.run_phase7_v2_controlled_deploy import run_controlled_deployment_v2
from scripts.run_phase7_v2_host_preflight import run_preflight
from scripts.simulate_phase7_c73_preflight import seed_host, tree_snapshot

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v2.json"


def sha256_file(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def make_auth(bundle_manifest: Path, evidence: Path, release_id: str, auth_id: str) -> dict:
    return {
        "schema_version": "1.0.0",
        "checkpoint": "C7.4",
        "status": "AUTHORIZED_READY_TO_DEPLOY",
        "authorization_id": auth_id,
        "single_use_authorization": True,
        "deployment_authorized": True,
        "production_deployed": False,
        "candidate": {
            "bundle_manifest_sha256": sha256_file(bundle_manifest),
            "c73_remote_evidence_sha256": sha256_file(evidence),
            "release_id": release_id,
            "managed_files": 33,
            "preexisting_dependencies": 11,
            "planned_directory_creations": 0,
        },
    }


def setup_case(tmp: Path) -> tuple[Path, dict[str, Path], Path, Path, str]:
    bundle_dir = tmp / "bundle"
    bundle = build_bundle(MANIFEST, bundle_dir)
    roots = seed_host(bundle, tmp / "host", leave_creatable_parents_missing=False)
    evidence = run_preflight(bundle_dir / "bundle-manifest.json", roots["site_root"], roots["wordpress_plugin_dir"])
    assert evidence["status"] == "PASS"
    assert evidence["summary"]["managed_files_observed"] == 33
    evidence_path = tmp / "preflight.json"
    evidence_path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    auth_id = "v2-test-auth"
    auth = make_auth(bundle_dir / "bundle-manifest.json", evidence_path, bundle["release"]["release_id"], auth_id)
    auth_path = tmp / "authorization.json"
    auth_path.write_text(json.dumps(auth, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return bundle_dir, roots, evidence_path, auth_path, auth_id


def test_v2_controlled_deployment_applies_33_files_and_stays_bound() -> None:
    with tempfile.TemporaryDirectory(prefix="phase7-v2-deploy-") as raw:
        tmp = Path(raw)
        bundle_dir, roots, evidence_path, auth_path, auth_id = setup_case(tmp)

        result = run_controlled_deployment_v2(
            bundle_dir=bundle_dir,
            site_root=roots["site_root"],
            wordpress_plugin_dir=roots["wordpress_plugin_dir"],
            c73_evidence_path=evidence_path,
            authorization_path=auth_path,
            authorization_id=auth_id,
            journal_dir=tmp / "journal" / auth_id,
            health_base_url="https://example.invalid",
            health_checker=lambda _base, _release: {"simulation": "PASS"},
        )

        assert result["status"] == "APPLIED_HEALTHY"
        assert result["production_deployed"] is True
        assert result["rollback_performed"] is False

        manifest = json.loads((bundle_dir / "bundle-manifest.json").read_text(encoding="utf-8"))
        assert len(manifest["files"]) == 33
        for record in manifest["files"]:
            root = roots[record["target_root"]]
            target = root / record["target_path"]
            assert target.is_file()
            assert sha256_file(target) == record["sha256"]


def test_v2_controlled_deployment_rolls_back_exactly_after_injected_failure() -> None:
    with tempfile.TemporaryDirectory(prefix="phase7-v2-rollback-") as raw:
        tmp = Path(raw)
        bundle_dir, roots, evidence_path, auth_path, auth_id = setup_case(tmp)
        before = tree_snapshot(tmp / "host")

        result = run_controlled_deployment_v2(
            bundle_dir=bundle_dir,
            site_root=roots["site_root"],
            wordpress_plugin_dir=roots["wordpress_plugin_dir"],
            c73_evidence_path=evidence_path,
            authorization_path=auth_path,
            authorization_id=auth_id,
            journal_dir=tmp / "journal" / auth_id,
            health_base_url="https://example.invalid",
            health_checker=lambda _base, _release: {"simulation": "PASS"},
            fail_after_writes=5,
        )
        after = tree_snapshot(tmp / "host")

        assert result["status"] == "ROLLED_BACK"
        assert result["production_deployed"] is False
        assert result["rollback_performed"] is True
        assert result["rollback"]["rollback_verified"] is True
        assert before == after
