from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.publication_v1 import FiscalReleaseStore

STORE = ROOT / "releases/fiscal-v1"
PLUGIN = ROOT / "consumers/wordpress/sanida-fiscais-auto.php"
PLUGIN_INCLUDES = ROOT / "consumers/wordpress/includes"
DOC = ROOT / "docs/phase6-c67-closure.md"
README = ROOT / "README.md"
CI = ROOT / ".github/workflows/remake-ci.yml"

FRONTEND_FILES = (
    ROOT / "consumers/frontend/folha-core.js",
    ROOT / "consumers/frontend/salario-liquido.js",
    ROOT / "consumers/frontend/folha-thirteenth.js",
    ROOT / "consumers/frontend/decimo-terceiro.js",
    ROOT / "consumers/frontend/folha-vacation.js",
    ROOT / "consumers/frontend/ferias-clt.js",
    ROOT / "consumers/frontend/folha-termination.js",
    ROOT / "consumers/frontend/rescisao-clt.js",
)
CONSUMERS = {
    "H26": ROOT / "consumers/frontend/salario-liquido.js",
    "H27": ROOT / "consumers/frontend/decimo-terceiro.js",
    "H28": ROOT / "consumers/frontend/ferias-clt.js",
    "H29": ROOT / "consumers/frontend/rescisao-clt.js",
}
PHASE6_GATES = tuple(ROOT / f"scripts/validate_phase6_c6{i}_gate.py" for i in range(1, 8))


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C6.7 closure gate failed: {message}")


def main() -> int:
    for path in (*FRONTEND_FILES, PLUGIN, DOC, README, CI, *PHASE6_GATES):
        require(path.is_file(), f"required closure file missing: {path.relative_to(ROOT)}")

    release = FiscalReleaseStore(STORE).load_current()
    require(release is not None, "current fiscal release unavailable")
    assert release is not None
    require(release.status.value == "PUBLISHED", "current release is not PUBLISHED")
    require(release.schema_version == "1.2.0", "current release schema is not 1.2.0")
    require(
        release.consumer_compatibility.contract_api_version == "1.2.0",
        "current release API is not 1.2.0",
    )
    require(
        {item.value for item in release.consumer_compatibility.consumers} == {"H26", "H27", "H28", "H29"},
        "current release consumers are not exactly H26-H29",
    )
    require(len(release.rules) == 32, "current release is not 32/32")
    require(release.release_id == release.expected_release_id(), "release_id is not content-addressed canonical")

    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    require(manifest.get("release_id") == release.release_id, "current.json release_id diverges from loaded release")
    artifact = STORE / manifest["artifact"]
    require(artifact.is_file(), "current immutable release artifact is missing")
    require(
        sha256(artifact.read_bytes()).hexdigest() == manifest.get("artifact_sha256"),
        "current immutable release artifact SHA-256 mismatch",
    )

    frontend_text = "\n".join(path.read_text(encoding="utf-8") for path in FRONTEND_FILES)
    for forbidden in (
        "dados_fiscais.json",
        "/sfa/v1/folha",
        "raw.githubusercontent.com",
        "SFA_FISCAIS_JSON_URL",
        "607.20",
        "189.59",
        "1621.00",
        "2902.84",
        "4354.27",
        "8475.55",
        "total13 * 0.5",
        "Math.round(dias / 3)",
        "Math.round(dias/3)",
        "salario / 30",
        "salario/30",
    ):
        require(forbidden not in frontend_text, f"legacy consumer authority/formula remains: {forbidden}")

    core = FRONTEND_FILES[0].read_text(encoding="utf-8")
    require("/blog/wp-json/sfa/v1/fiscal-release" in core, "folha-core does not target canonical fiscal-release REST")
    require("SFA_FOLHA" in core, "folha-core runtime namespace missing")

    for consumer, path in CONSUMERS.items():
        source = path.read_text(encoding="utf-8")
        require(
            "SFA.fetchRelease({ consumer: CONSUMER })" in source,
            f"{consumer} does not fetch the canonical release through folha-core",
        )
        require("release_id" in source, f"{consumer} does not preserve release_id")

    require("SFA.THIRTEENTH.calculate" in CONSUMERS["H27"].read_text(encoding="utf-8"), "H27 shared runtime missing")
    require("SFA.VACATION.calculate" in CONSUMERS["H28"].read_text(encoding="utf-8"), "H28 shared runtime missing")
    require("SFA.TERMINATION.calculate" in CONSUMERS["H29"].read_text(encoding="utf-8"), "H29 shared runtime missing")

    plugin_paths = [PLUGIN, *sorted(PLUGIN_INCLUDES.glob("*.php"))]
    require(len(plugin_paths) == 8, "unexpected WordPress plugin source set")
    plugin_text = "\n".join(path.read_text(encoding="utf-8") for path in plugin_paths)

    for forbidden in (
        "dados_fiscais.json",
        "SFA_FISCAIS_JSON_URL",
        "private function build_folha_payload(",
        "private function build_legacy_adapter(",
        "legacy-folha-adapter-v1",
        "private function get_data(",
        "SFA_V2_Calc",
        "calcINSS(",
        "calcIRRF13(",
        "aplicarReducao(",
        "total13 / 2",
        "data-sfa=\"folha\"",
    ):
        require(forbidden not in plugin_text, f"retired fiscal runtime remains in WordPress plugin: {forbidden}")

    for marker in (
        "Version:     2.6.0",
        "register_rest_route('sfa/v1', '/fiscal-release'",
        "register_rest_route('sfa/v1', '/folha'",
        "sfa_legacy_folha_retired",
        "['status' => 410",
        "'/wp-json/sfa/v1/fiscal-release'",
        "SHORTCODE_DISPLAY_ADAPTER_RETIRE_BY",
        "C7.1-before-production-deployment",
        "build_shortcode_display_adapter",
        "wordpress_table_shortcodes_v1",
        "'adapter_consumers' => ['ano_ref','inss_tabela','irrf_tabela']",
        "data-sfa-retired=\"folha\"",
        "data-sfa-retired=\"calc_salario_liquido\"",
        "data-sfa-retired=\"calc13_assets\"",
        "/financas/calculadoras/salario-liquido-clt/",
        "/financas/calculadoras/decimo-terceiro/",
    ):
        require(marker in plugin_text, f"C6.7 WordPress marker missing: {marker}")

    node = shutil.which("node")
    require(node is not None, "node is required for Phase 6 closure validation")
    for path in FRONTEND_FILES:
        completed = subprocess.run(
            [node, "--check", str(path)], cwd=ROOT, check=False, capture_output=True, text=True
        )
        require(completed.returncode == 0, f"node --check failed for {path.name}: {completed.stderr or completed.stdout}")

    php = shutil.which("php")
    if php:
        for path in plugin_paths:
            completed = subprocess.run(
                [php, "-l", str(path)], cwd=ROOT, check=False, capture_output=True, text=True
            )
            require(completed.returncode == 0, f"PHP lint failed for {path.name}: {completed.stderr or completed.stdout}")

    ci = CI.read_text(encoding="utf-8")
    for index in range(1, 8):
        marker = f"python scripts/validate_phase6_c6{index}_gate.py"
        require(marker in ci, f"Remake CI missing permanent Phase 6 gate C6.{index}")

    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "**Status:** CONCLUÍDO",
        "410 Gone",
        "wordpress_table_shortcodes_v1",
        "C7.1-before-production-deployment",
        "dados_fiscais.json",
        "não afirma implantação no HostGator",
        "Fase 7 — fechamento E2E e operação evergreen",
    ):
        require(marker in doc, f"C6.7 closure document missing marker: {marker}")

    readme = README.read_text(encoding="utf-8")
    for marker in (
        "### Fase 6 — Migração dos consumidores",
        "**Status: CONCLUÍDA**",
        "C6.7 — remoção controlada do legado",
        "C7.1-before-production-deployment",
        "Fase 7 — Fechamento e operação evergreen",
        "Fases 0–6 estão formalmente concluídas",
    ):
        require(marker in readme, f"README not synchronized for Phase 6 closure: {marker}")

    print(
        "Phase 6 C6.7 formal closure gate: PASS "
        f"(release={release.release_id}, rules={len(release.rules)}, consumers=H26-H29, legacy_fiscal_authority=retired)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
