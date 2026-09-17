#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_phase7_c71_bundle_v2 import build_bundle
from scripts.simulate_phase7_c71_deployment import run_simulation

V1 = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"
V2 = ROOT / "docs/phase7-c71-deployment-manifest-v2.json"
H25 = ROOT / "consumers/frontend/calculadoras/index.php"
EXPECTED_MANAGED = 33
EXPECTED_DEPENDENCIES = 11


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C7.1 v2 gate failed: {message}")


def key(item: dict) -> str:
    return f"{item['target_root']}:{str(item['target_path']).lstrip('/')}"


def main() -> int:
    for path in (V1, V2, H25):
        require(path.is_file(), f"required file missing: {path.relative_to(ROOT)}")

    historical = json.loads(V1.read_text(encoding="utf-8"))
    current = json.loads(V2.read_text(encoding="utf-8"))

    # Historical v1 remains byte-semantically the 32-file inventory already used by C7.3-C7.5.
    require(len(historical.get("managed_files") or []) == 32, "historical v1 inventory was rewritten")
    require(
        all(item.get("target_path") != "financas/calculadoras/index.php" for item in historical.get("managed_files") or []),
        "historical v1 incorrectly claims H25",
    )

    require(current.get("inventory_version") == 2, "v2 inventory marker missing")
    require(current.get("supersedes_manifest") == "docs/phase7-c71-deployment-manifest-v1.json", "v2 supersession marker drift")
    require(current.get("status") == "ready_not_deployed", "v2 status drift")
    require(current.get("production_deployed") is False, "v2 incorrectly claims deployment")
    require(current.get("release_binding", {}).get("required_rule_count") == 32, "fiscal rule count was reopened")
    require(current.get("release_binding", {}).get("required_consumers") == ["H26", "H27", "H28", "H29"], "fiscal consumer set was reopened")

    managed = current.get("managed_files") or []
    dependencies = current.get("preexisting_dependencies") or []
    require(len(managed) == EXPECTED_MANAGED, "v2 managed count drift")
    require(len(dependencies) == EXPECTED_DEPENDENCIES, "v2 dependency count drift")
    require(len({item["source"] for item in managed}) == EXPECTED_MANAGED, "duplicate v2 source")
    require(len({key(item) for item in managed}) == EXPECTED_MANAGED, "duplicate v2 target")

    h25_rows = [item for item in managed if item.get("target_path") == "financas/calculadoras/index.php"]
    require(len(h25_rows) == 1, "H25 target missing or duplicated")
    require(h25_rows[0].get("source") == "consumers/frontend/calculadoras/index.php", "H25 source mapping drift")

    dep_targets = {key(item) for item in dependencies}
    referenced: set[str] = set()
    text = H25.read_text(encoding="utf-8")
    for match in re.findall(r"\$_SERVER\['DOCUMENT_ROOT'\]\s*\.\s*'(/PHP/[^']+)'", text):
        referenced.add(f"site_root:{match.lstrip('/')}")
    for match in re.findall(r"(?:href|src)=\"(/financas/calculadoras/assets/[^\"?]+)", text):
        referenced.add(f"site_root:{match.lstrip('/')}")
    require(not (referenced - dep_targets), f"undeclared H25 dependencies: {sorted(referenced - dep_targets)}")

    with tempfile.TemporaryDirectory(prefix="c71-v2-gate-") as raw:
        bundle_dir = Path(raw) / "bundle"
        bundle = build_bundle(V2, bundle_dir)
        require(bundle.get("managed_file_count") == EXPECTED_MANAGED, "built v2 bundle count drift")
        require(bundle.get("inventory_version") == 2, "built v2 inventory marker missing")
        require(bundle.get("production_deployed") is False, "built v2 bundle claims deployment")
        require((bundle_dir / "site-root/financas/calculadoras/index.php").is_file(), "H25 absent from built v2 bundle")

        php = shutil.which("php")
        if php:
            completed = subprocess.run(
                [php, "-l", str(bundle_dir / "site-root/financas/calculadoras/index.php")],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            require(completed.returncode == 0, completed.stderr or completed.stdout)

        simulation = run_simulation(bundle_dir)
        for marker in (
            "apply_verified",
            "rollback_exact_bytes_verified",
            "new_file_cleanup_verified",
            "preexisting_dependencies_preserved",
            "unmanaged_files_preserved",
        ):
            require(simulation.get(marker) is True, f"v2 rollback simulation failed: {marker}")
        require(simulation.get("production_mutated") is False, "v2 simulation claims production mutation")

    print(
        "Phase 7 C7.1 v2 gate: PASS "
        "(historical_v1=32 immutable, current_v2=33 including H25, dependencies=11, fiscal_rules=32, fresh_preflight_required=true)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
