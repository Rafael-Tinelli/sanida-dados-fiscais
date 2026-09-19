from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sanida_fiscal.publication_v1 import FiscalReleaseStore, required_inventory_rule_ids


PLUGIN = ROOT / "consumers/wordpress/sanida-fiscais-auto.php"
DOC = ROOT / "docs/phase6-c61-wordpress-consumer.md"
CI = ROOT / ".github/workflows/remake-ci.yml"
STORE_ROOT = ROOT / "releases/fiscal-v1"
COVERAGE = ROOT / "docs/contract-coverage-v1.json"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C6.1 gate failed: {message}")


def main() -> int:
    require(PLUGIN.is_file(), "canonical WordPress plugin source is missing")
    require(DOC.is_file(), "C6.1 closure document is missing")

    release = FiscalReleaseStore(STORE_ROOT).load_current()
    require(release is not None, "no current fiscal release")
    assert release is not None
    require(release.schema_version == "1.2.0", "current release schema is not 1.2.0")
    require(
        release.consumer_compatibility.contract_api_version == "1.2.0",
        "current release API is not 1.2.0",
    )
    require(release.status.value == "PUBLISHED", "current release is not PUBLISHED")
    require(len(release.rules) == 32, "current release is not 32/32")
    require(release.release_id == release.expected_release_id(), "current release_id is not canonical")

    manifest = json.loads((STORE_ROOT / "current.json").read_text(encoding="utf-8"))
    artifact = STORE_ROOT / manifest["artifact"]
    require(artifact.is_file(), "current immutable artifact is missing")
    require(
        sha256(artifact.read_bytes()).hexdigest() == manifest["artifact_sha256"],
        "current immutable artifact SHA-256 mismatch",
    )

    expected_plugin_paths = [
        PLUGIN,
        PLUGIN.parent / "includes/trait-sanida-fiscal-network.php",
        PLUGIN.parent / "includes/trait-sanida-fiscal-contract.php",
        PLUGIN.parent / "includes/trait-sanida-taxas.php",
        PLUGIN.parent / "includes/trait-sanida-shortcodes-core.php",
        PLUGIN.parent / "includes/trait-sanida-calc13.php",
        PLUGIN.parent / "includes/trait-sanida-salary-calc.php",
        PLUGIN.parent / "includes/trait-sanida-admin-debug.php",
    ]
    plugin_paths = [PLUGIN, *sorted((PLUGIN.parent / "includes").glob("*.php"))]
    require(
        set(plugin_paths) == set(expected_plugin_paths) and len(plugin_paths) == 8,
        "C6.1 WordPress source set is incomplete or contains obsolete PHP modules",
    )
    text = "\n".join(path.read_text(encoding="utf-8") for path in plugin_paths)

    forbidden = (
        "dados_fiscais.json",
        "SFA_FISCAIS_JSON_URL",
        "private function minimal_fallback(",
        "private function build_legacy_adapter(",
        "private function build_folha_payload(",
        "legacy-folha-adapter-v1",
        "private function get_data(",
        "SFA_V2_Calc",
        "calcINSS(",
        "calcIRRF13(",
        "aplicarReducao(",
        "total13 / 2",
        "SHORTCODE_DISPLAY_ADAPTER_RETIRE_BY",
        "C7.1-before-production-deployment",
        "build_shortcode_display_adapter",
        "wordpress_table_shortcodes_v1",
        "compatibility_adapter",
        "adapter_consumers",
        "shortcode-display-v1",
        "607.20",
        "189.59",
        "1621.00",
        "2902.84",
        "4354.27",
        "8475.55",
        "'selic' => 15.00",
        "'cdi'   => 14.90",
        "'origin' => 'minimal_fallback'",
        "isset($d['taxas']['selic']) ? (float)$d['taxas']['selic'] : 0",
        "isset($d['taxas']['cdi']) ? (float)$d['taxas']['cdi'] : 0",
        "(float)($t['selic'] ?? 0)",
        "(float)($t['cdi'] ?? 0)",
    )
    for token in forbidden:
        require(token not in text, f"legacy fiscal authority/adapter/formula remains in plugin: {token}")

    required_markers = (
        "Version:     2.8.0",
        "/releases/fiscal-v1/current.json",
        "RELEASE_BASE_URL_DEFAULT",
        "hash('sha256', $artifact['body'])",
        "hash_equals($manifest['artifact_sha256'], $actual_sha)",
        "release_artifact_sha256",
        "expected_release_id_from_raw_body",
        "artifact_body",
        "json_decode($raw_body)",
        "new stdClass()",
        "SUPPORTED_SCHEMA_VERSION       = '1.2.0'",
        "SUPPORTED_CONTRACT_API_VERSION = '1.2.0'",
        "$r['status'] !== 'PUBLISHED'",
        "count($r['rules']) !== 32",
        "if ($known_successor) return false;",
        "historical_release_relabelled_as_current' => false",
        "register_rest_route('sfa/v1', '/fiscal-release'",
        "register_rest_route('sfa/v1', '/folha'",
        "sfa_legacy_folha_retired",
        "'status' => 410",
        "'/wp-json/sfa/v1/fiscal-release'",
        "X-Sanida-Fiscal-Release",
        "['status' => 503]",
        "canonical_rules_for_shortcodes",
        "inss.employee.progressive_table",
        "irrf.monthly.progressive_table",
        "irrf.simplified_monthly_discount",
        "presentation_shortcodes_mode' => 'direct_canonical_release'",
        "data-sfa-retired=\"folha\"",
        "/financas/calculadoras/salario-liquido-clt/",
        "/financas/calculadoras/decimo-terceiro/",
        "taxas_unavailable_fail_closed",
        "'origin' => 'unavailable_fail_closed'",
        "'unavailable' => true",
        "'cdi_basis' => 'unavailable'",
        "fiscais_unavailable_text('taxa')",
        "release_within_effective_window",
        "rule_within_effective_window",
        "CDI_MAX_OBSERVATION_AGE_DAYS = 7",
        "cdi_observation_is_fresh",
        "BCB_CDI_DAILY_SGS_12",
        "max_observation_age_calendar_days",
    )
    for marker in required_markers:
        require(marker in text, f"plugin missing required C6.1/C7.1 marker: {marker}")

    coverage = json.loads(COVERAGE.read_text(encoding="utf-8"))
    required_ids = required_inventory_rule_ids(coverage)
    require(len(required_ids) == 32, "coverage inventory is not 32")
    for rule_id in required_ids:
        require(f"'{rule_id}'" in text, f"plugin does not require rule_id {rule_id}")

    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "**Status:** CONCLUÍDO",
        "releases/fiscal-v1/current.json",
        "`HUMAN_REVIEWED`",
        "adaptador de compatibilidade",
        "C6.2",
        "não afirma implantação no HostGator",
    ):
        require(marker in doc, f"C6.1 document missing marker: {marker}")

    ci = CI.read_text(encoding="utf-8")
    require(
        "python scripts/validate_phase6_c61_gate.py" in ci,
        "Remake CI does not execute the permanent C6.1 gate",
    )

    php = shutil.which("php")
    if php:
        for php_path in plugin_paths:
            completed = subprocess.run(
                [php, "-l", str(php_path)],
                cwd=ROOT,
                check=False,
                capture_output=True,
                text=True,
            )
            require(
                completed.returncode == 0,
                f"PHP lint failed for {php_path}: {completed.stderr or completed.stdout}",
            )

    print(
        "Phase 6 C6.1 WordPress consumer gate: PASS "
        f"(release={release.release_id}, rules={len(release.rules)}, legacy_fiscal_runtime=retired, presentation=direct_release)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
