from __future__ import annotations

from pathlib import Path
import subprocess
import sys
import tempfile

from scripts.build_phase7_c71_bundle_v2 import build_bundle
from scripts.run_phase7_v2_host_preflight import run_preflight
from scripts.simulate_phase7_c73_preflight import seed_host, tree_snapshot

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v2.json"
VALIDATOR = ROOT / "scripts/validate_phase7_v2_remote_evidence.py"


def test_v2_preflight_is_read_only_and_validates_33_targets() -> None:
    with tempfile.TemporaryDirectory(prefix="phase7-v2-preflight-") as raw:
        tmp = Path(raw)
        bundle_dir = tmp / "bundle"
        bundle = build_bundle(MANIFEST, bundle_dir)
        roots = seed_host(bundle, tmp / "host", leave_creatable_parents_missing=True)
        before = tree_snapshot(tmp / "host")

        evidence = run_preflight(
            bundle_dir / "bundle-manifest.json",
            roots["site_root"],
            roots["wordpress_plugin_dir"],
        )
        after = tree_snapshot(tmp / "host")

        assert before == after
        assert evidence["status"] == "PASS"
        assert evidence["technical_go_no_go"] == "GO"
        assert evidence["inventory_version"] == 2
        assert evidence["summary"]["managed_files_expected"] == 33
        assert evidence["summary"]["managed_files_observed"] == 33
        assert evidence["summary"]["preexisting_dependencies_observed"] == 11
        assert evidence["production_mutated"] is False
        assert evidence["deployment_authorized"] is False

        evidence_path = tmp / "evidence.json"
        import json
        evidence_path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        completed = subprocess.run(
            [
                sys.executable,
                str(VALIDATOR),
                "--evidence",
                str(evidence_path),
                "--bundle-manifest",
                str(bundle_dir / "bundle-manifest.json"),
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr or completed.stdout
