from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from sanida_fiscal.publication_v1 import FiscalReleaseStore, required_inventory_rule_ids


ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "consumers/wordpress/sanida-fiscais-auto.php"
STORE_ROOT = ROOT / "releases/fiscal-v1"
COVERAGE = ROOT / "docs/contract-coverage-v1.json"


def _plugin_text() -> str:
    parts = [PLUGIN]
    parts.extend(sorted((PLUGIN.parent / "includes").glob("*.php")))
    return "\n".join(path.read_text(encoding="utf-8") for path in parts)


def _current_manifest() -> dict:
    return json.loads((STORE_ROOT / "current.json").read_text(encoding="utf-8"))


def test_c61_has_a_real_published_v12_baseline() -> None:
    release = FiscalReleaseStore(STORE_ROOT).load_current()
    assert release is not None
    assert release.schema_version == "1.2.0"
    assert release.consumer_compatibility.contract_api_version == "1.2.0"
    assert release.status.value == "PUBLISHED"
    assert len(release.rules) == 32
    assert release.release_id == release.expected_release_id()
    assert [item.value for item in release.consumer_compatibility.consumers] == [
        "H26",
        "H27",
        "H28",
        "H29",
    ]


def test_c61_current_manifest_points_to_exact_immutable_artifact() -> None:
    manifest = _current_manifest()
    release_id = manifest["release_id"]
    assert manifest["artifact"] == f"releases/{release_id}.json"
    artifact = STORE_ROOT / manifest["artifact"]
    body = artifact.read_bytes()
    assert sha256(body).hexdigest() == manifest["artifact_sha256"]


def test_c61_plugin_uses_manifest_and_immutable_release_not_legacy_json() -> None:
    text = _plugin_text()
    assert "Version:     2.6.0" in text
    assert "/releases/fiscal-v1/current.json" in text
    assert "RELEASE_BASE_URL_DEFAULT" in text
    assert "dados_fiscais.json" not in text
    assert "SFA_FISCAIS_JSON_URL" not in text
    assert "private function minimal_fallback(" not in text
    assert "private function build_legacy_adapter(" not in text
    assert "private function get_data(" not in text


def test_c61_plugin_verifies_bytes_release_identity_and_exact_compatibility() -> None:
    text = _plugin_text()
    for marker in (
        "hash('sha256', $artifact['body'])",
        "hash_equals($manifest['artifact_sha256'], $actual_sha)",
        "release_artifact_sha256",
        "expected_release_id_from_raw_body",
        "artifact_body",
        "json_decode($raw_body)",
        "new stdClass()",
        "SUPPORTED_SCHEMA_VERSION       = '1.2.0'",
        "SUPPORTED_CONTRACT_API_VERSION = '1.2.0'",
        "SUPPORTED_MANIFEST_VERSION     = '1.0.0'",
        "$r['status'] !== 'PUBLISHED'",
        "unknown_fields' => 'reject'",
        "unknown_payload_types' => 'reject'",
        "unsupported_behavior' => 'hard_fail'",
    ):
        assert marker in text


def test_c61_plugin_requires_exact_closed_inventory() -> None:
    text = _plugin_text()
    coverage = json.loads(COVERAGE.read_text(encoding="utf-8"))
    required = required_inventory_rule_ids(coverage)
    assert len(required) == 32
    for rule_id in required:
        assert f"'{rule_id}'" in text
    assert "count($r['rules']) !== 32" in text
    assert "quality']['status'] ?? null) !== 'VALIDATED'" in text


def test_c61_last_good_is_fail_closed_and_never_relabels_known_successor() -> None:
    text = _plugin_text()
    for marker in (
        "allow_when_source_unavailable",
        "allow_relabel_historical_as_current",
        "require_no_known_successor",
        "if ($known_successor) return false;",
        "historical_release_relabelled_as_current' => false",
        "'known_successor' => (bool)$known_successor",
    ):
        assert marker in text
    for forbidden in ("607.20", "189.59", "1621.00", "2902.84", "4354.27", "8475.55"):
        assert forbidden not in text


def test_c61_rest_exposes_canonical_release_and_retires_legacy_folha_endpoint() -> None:
    text = _plugin_text()
    assert "register_rest_route('sfa/v1', '/fiscal-release'" in text
    assert "register_rest_route('sfa/v1', '/folha'" in text
    assert "X-Sanida-Fiscal-Release" in text
    assert "sfa_legacy_folha_retired" in text
    assert "'status' => 410" in text
    assert "'/wp-json/sfa/v1/fiscal-release'" in text
    assert "legacy-folha-adapter-v1" not in text
    assert "private function build_folha_payload(" not in text
    assert "['status' => 503]" in text


def test_c61_only_named_presentation_adapter_survives_with_retirement_deadline() -> None:
    text = _plugin_text()
    for marker in (
        "SHORTCODE_DISPLAY_ADAPTER_RETIRE_BY",
        "C7.1-before-production-deployment",
        "build_shortcode_display_adapter",
        "wordpress_table_shortcodes_v1",
        "'adapter_consumers' => ['ano_ref','inss_tabela','irrf_tabela']",
    ):
        assert marker in text
    assert "legacy-folha-adapter-v1" not in text
    assert "data-sfa=\"folha\"" not in text


def test_c61_legacy_calculator_shortcodes_are_bridges_not_fiscal_engines() -> None:
    text = _plugin_text()
    for marker in (
        "data-sfa-retired=\"folha\"",
        "data-sfa-retired=\"calc_salario_liquido\"",
        "data-sfa-retired=\"calc13_assets\"",
        "/financas/calculadoras/salario-liquido-clt/",
        "/financas/calculadoras/decimo-terceiro/",
    ):
        assert marker in text
    for forbidden in (
        "SFA_V2_Calc",
        "calcINSS(",
        "calcIRRF13(",
        "aplicarReducao(",
        "total13 / 2",
    ):
        assert forbidden not in text


def test_c61_plugin_keeps_financial_reference_separate() -> None:
    text = _plugin_text()
    assert "TAXAS_JSON_URL_DEFAULT" in text
    assert "minimal_taxas_fallback" in text
    assert "OPT_TAXAS_LAST_GOOD" in text


def test_c61_plugin_has_no_stale_generated_at_relabeling_for_fiscal_release() -> None:
    text = _plugin_text()
    block = re.search(
        r"private function with_package_runtime\(.*?private function get_release_package\(",
        text,
        flags=re.S,
    )
    assert block is not None
    assert "generated_at_utc" not in block.group(0)


def test_c61_wordpress_source_set_is_explicit_and_complete() -> None:
    expected = {
        PLUGIN,
        PLUGIN.parent / "includes/trait-sanida-fiscal-network.php",
        PLUGIN.parent / "includes/trait-sanida-fiscal-contract.php",
        PLUGIN.parent / "includes/trait-sanida-taxas.php",
        PLUGIN.parent / "includes/trait-sanida-shortcodes-core.php",
        PLUGIN.parent / "includes/trait-sanida-calc13.php",
        PLUGIN.parent / "includes/trait-sanida-salary-calc.php",
        PLUGIN.parent / "includes/trait-sanida-admin-debug.php",
    }
    actual = {PLUGIN, *sorted((PLUGIN.parent / "includes").glob("*.php"))}
    assert actual == expected


def test_c61_php_identity_matches_real_release_and_preserves_empty_json_objects() -> None:
    php = shutil.which("php")
    if php is None:
        return

    manifest = _current_manifest()
    artifact = STORE_ROOT / manifest["artifact"]
    contract_trait = PLUGIN.parent / "includes/trait-sanida-fiscal-contract.php"
    harness = r'''<?php
    define('ABSPATH', __DIR__);
    function wp_json_encode($value, $flags = 0, $depth = 512) {
      return json_encode($value, $flags, $depth);
    }
    require $argv[1];
    final class C61_Identity_Harness {
      const CONTRACT_ID = 'br.sanida.fiscal';
      const SUPPORTED_SCHEMA_VERSION = '1.2.0';
      const SUPPORTED_CONTRACT_API_VERSION = '1.2.0';
      const SUPPORTED_MANIFEST_VERSION = '1.0.0';
      use Sanida_Fiscais_Fiscal_Contract_Trait;
      public function expected_id($body) { return $this->expected_release_id_from_raw_body($body); }
      public function valid_package($package) { return $this->validate_release_package($package); }
    }
    $manifest = json_decode(file_get_contents($argv[2]), true);
    $body = file_get_contents($argv[3]);
    $release = json_decode($body, true);
    $h = new C61_Identity_Harness();
    echo json_encode([
      'expected' => $h->expected_id($body),
      'valid' => $h->valid_package([
        'manifest' => $manifest,
        'release' => $release,
        'artifact_body' => $body,
      ]),
    ]);
    ?>'''
    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "harness.php"
        manifest_path = Path(tmp) / "current.json"
        script.write_text(harness, encoding="utf-8")
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        completed = subprocess.run(
            [php, str(script), str(contract_trait), str(manifest_path), str(artifact)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result == {"expected": manifest["release_id"], "valid": True}
