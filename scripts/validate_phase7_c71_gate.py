#!/usr/bin/env python3
from __future__ import annotations

from hashlib import sha256
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

from sanida_fiscal.publication_v1 import FiscalReleaseStore
from scripts.build_phase7_c71_bundle import build_bundle
from scripts.simulate_phase7_c71_deployment import run_simulation

STORE = ROOT / "releases/fiscal-v1"
DEPLOYMENT_MANIFEST = ROOT / "docs/phase7-c71-deployment-manifest-v1.json"
DOC = ROOT / "docs/phase7-c71-deployment-bundle.md"
README = ROOT / "README.md"
CI = ROOT / ".github/workflows/remake-ci.yml"
PLUGIN = ROOT / "consumers/wordpress/sanida-fiscais-auto.php"
PLUGIN_DIR = ROOT / "consumers/wordpress"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C7.1 gate failed: {message}")


def normalized_target(root: str, path: str) -> str:
    return f"{root}:{path.lstrip('/')}"


def main() -> int:
    for path in (DEPLOYMENT_MANIFEST, DOC, README, CI, PLUGIN):
        require(path.is_file(), f"required C7.1 file missing: {path.relative_to(ROOT)}")

    release = FiscalReleaseStore(STORE).load_current()
    require(release is not None, "current fiscal release unavailable")
    assert release is not None
    require(release.status.value == "PUBLISHED", "current release is not PUBLISHED")
    require(release.schema_version == "1.2.0", "current contract schema is not 1.2.0")
    require(release.consumer_compatibility.contract_api_version == "1.2.0", "current contract API is not 1.2.0")
    require(len(release.rules) == 32, "current release is not 32/32")
    require({item.value for item in release.consumer_compatibility.consumers} == {"H26", "H27", "H28", "H29"}, "consumer set drift")
    require(release.release_id == release.expected_release_id(), "release_id is not canonical")

    pointer = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    require(pointer["release_id"] == release.release_id, "current.json points to another release")
    artifact = STORE / pointer["artifact"]
    require(artifact.is_file(), "current immutable release artifact missing")
    require(sha256(artifact.read_bytes()).hexdigest() == pointer["artifact_sha256"], "current artifact SHA mismatch")

    source_manifest = json.loads(DEPLOYMENT_MANIFEST.read_text(encoding="utf-8"))
    require(source_manifest.get("status") == "ready_not_deployed", "deployment manifest status drift")
    require(source_manifest.get("production_deployed") is False, "C7.1 must not claim production deployment")
    managed = source_manifest.get("managed_files") or []
    dependencies = source_manifest.get("preexisting_dependencies") or []
    require(len(managed) == 32, "C7.1 managed file set must contain exactly 32 files")
    require(len(dependencies) == 11, "C7.1 preexisting dependency set must contain exactly 11 files")

    managed_targets = {
        normalized_target(item["target_root"], item["target_path"])
        for item in managed
    }
    require(len(managed_targets) == len(managed), "duplicate managed deployment target")
    dependency_targets = {
        normalized_target(item["target_root"], item["target_path"])
        for item in dependencies
    }
    require(len(dependency_targets) == len(dependencies), "duplicate preexisting dependency")
    require(not (managed_targets & dependency_targets), "managed target also declared as preexisting dependency")

    # Every local runtime/include dependency referenced by the managed PHP pages must
    # be either part of the bundle or explicitly declared as pre-existing.
    php_sources = [ROOT / item["source"] for item in managed if item["source"].endswith(".php")]
    referenced: set[str] = set()
    for source in php_sources:
        text = source.read_text(encoding="utf-8")
        for match in re.findall(r"\$_SERVER\['DOCUMENT_ROOT'\]\s*\.\s*'(/PHP/[^']+)'", text):
            referenced.add(normalized_target("site_root", match))
        for match in re.findall(r"(?:href|src)=\"(/financas/calculadoras/assets/[^\"?]+)", text):
            referenced.add(normalized_target("site_root", match))
    missing_refs = sorted(referenced - managed_targets - dependency_targets)
    require(not missing_refs, f"undeclared page dependencies: {missing_refs}")

    plugin_paths = [PLUGIN, *sorted((PLUGIN_DIR / "includes").glob("*.php"))]
    require(len(plugin_paths) == 8, "unexpected WordPress plugin source set")
    plugin_text = "\n".join(path.read_text(encoding="utf-8") for path in plugin_paths)
    for forbidden in (
        "SHORTCODE_DISPLAY_ADAPTER_RETIRE_BY",
        "build_shortcode_display_adapter",
        "get_shortcode_display_data",
        "wordpress_table_shortcodes_v1",
        "compatibility_adapter",
        "adapter_consumers",
        "shortcode-display-v1",
    ):
        require(forbidden not in plugin_text, f"expired presentation adapter remains: {forbidden}")
    for marker in (
        "Version:     2.7.1",
        "canonical_rules_for_shortcodes",
        "inss.employee.progressive_table",
        "irrf.monthly.progressive_table",
        "irrf.simplified_monthly_discount",
        "presentation_shortcodes_mode' => 'direct_canonical_release'",
    ):
        require(marker in plugin_text, f"direct canonical presentation marker missing: {marker}")

    with tempfile.TemporaryDirectory(prefix="c71-gate-") as raw:
        bundle_dir = Path(raw) / "bundle"
        bundle = build_bundle(DEPLOYMENT_MANIFEST, bundle_dir)
        require(bundle["managed_file_count"] == 32, "built bundle file count drift")
        require(bundle["release"]["release_id"] == release.release_id, "bundle bound to wrong release")
        require(bundle["production_deployed"] is False, "built bundle claims production deployment")

        for record in bundle["files"]:
            path = bundle_dir / record["bundle_path"]
            require(path.is_file(), f"built bundle file missing: {record['bundle_path']}")
            require(sha256(path.read_bytes()).hexdigest() == record["sha256"], f"built bundle hash mismatch: {record['bundle_path']}")

        bundle_text = "\n".join(
            (bundle_dir / item["bundle_path"]).read_text(encoding="utf-8", errors="ignore")
            for item in bundle["files"]
        )
        for forbidden in (
            "wordpress_table_shortcodes_v1",
            "build_shortcode_display_adapter",
            "shortcode-display-v1",
            "dados_fiscais.json",
            "SFA_FISCAIS_JSON_URL",
        ):
            require(forbidden not in bundle_text, f"forbidden legacy authority/adapter leaked into bundle: {forbidden}")

        node = shutil.which("node")
        require(node is not None, "node is required for C7.1 bundle E2E")
        asset = bundle_dir / "site-root/financas/calculadoras/assets"
        runtimes = (
            ("H26", ROOT / "tests/js/phase6_c63_h26_runtime.cjs", [asset / "folha-core.js", asset / "salario-liquido.js", artifact]),
            ("H27", ROOT / "tests/js/phase6_c64_h27_runtime.cjs", [asset / "folha-core.js", asset / "folha-thirteenth.js", asset / "decimo-terceiro.js", artifact]),
            ("H28", ROOT / "tests/js/phase6_c65_h28_runtime.cjs", [asset / "folha-core.js", asset / "folha-vacation.js", asset / "ferias-clt.js", artifact]),
            ("H29", ROOT / "tests/js/phase6_c66_h29_runtime.cjs", [asset / "folha-core.js", asset / "folha-termination.js", asset / "rescisao-clt.js", artifact]),
        )
        for consumer, runtime, args in runtimes:
            completed = subprocess.run(
                [node, str(runtime), *(str(arg) for arg in args)],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            require(completed.returncode == 0, f"{consumer} bundled runtime failed: {completed.stderr or completed.stdout}")
            payload = json.loads(completed.stdout)
            require(payload.get("release_id") == release.release_id, f"{consumer} bundled runtime used wrong release")

        php = shutil.which("php")
        if php:
            for record in bundle["files"]:
                if not record["bundle_path"].endswith(".php"):
                    continue
                path = bundle_dir / record["bundle_path"]
                completed = subprocess.run([php, "-l", str(path)], cwd=ROOT, capture_output=True, text=True, check=False)
                require(completed.returncode == 0, f"bundled PHP lint failed for {record['bundle_path']}: {completed.stderr or completed.stdout}")

        simulation = run_simulation(bundle_dir)
        for key in (
            "apply_verified",
            "rollback_exact_bytes_verified",
            "new_file_cleanup_verified",
            "preexisting_dependencies_preserved",
            "unmanaged_files_preserved",
        ):
            require(simulation.get(key) is True, f"deployment simulation did not prove {key}")
        require(simulation.get("production_mutated") is False, "deployment simulation claims production mutation")

    ci = CI.read_text(encoding="utf-8")
    require("python scripts/build_phase7_c71_bundle.py --output-dir /tmp/c71-deployment-bundle" in ci, "Remake CI does not build C7.1 bundle")
    require("python scripts/validate_phase7_c71_gate.py" in ci, "Remake CI does not execute C7.1 gate")

    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "**Status:** CONCLUÍDO",
        "plugin 2.7.0",
        "32 arquivos gerenciados",
        "11 dependências pré-existentes",
        "rollback",
        "não realiza implantação no HostGator",
        "C7.2",
    ):
        require(marker in doc, f"C7.1 documentation missing marker: {marker}")

    readme = README.read_text(encoding="utf-8")
    for marker in (
        "### Fase 7 — Fechamento e operação evergreen\n\n**Status: EM ANDAMENTO**",
        "C7.1 — bundle pré-deploy e rollback",
        "plugin 2.7.0",
        "C7.2",
    ):
        require(marker in readme, f"README missing C7.1 state marker: {marker}")

    print(
        "Phase 7 C7.1 gate: PASS "
        f"(release={release.release_id}, managed=32, dependencies=11, e2e=H26-H29, rollback=exact, production_deployed=false)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
