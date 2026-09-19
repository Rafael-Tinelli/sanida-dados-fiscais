from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "consumers/wordpress/sanida-fiscais-auto.php"
TAXAS_TRAIT = PLUGIN.parent / "includes/trait-sanida-taxas.php"
SHORTCODES_TRAIT = PLUGIN.parent / "includes/trait-sanida-shortcodes-core.php"
SERIES = ROOT / "taxas_bacen_series.json"


def _php() -> str | None:
    return shutil.which("php")


def test_v280_contract_markers_and_real_artifact_shape() -> None:
    plugin = PLUGIN.read_text(encoding="utf-8")
    taxas = TAXAS_TRAIT.read_text(encoding="utf-8")
    shortcodes = SHORTCODES_TRAIT.read_text(encoding="utf-8")
    artifact = json.loads(SERIES.read_text(encoding="utf-8"))

    assert "Version:     2.8.0" in plugin
    assert "TAXAS_SERIES_JSON_URL_DEFAULT" in plugin
    assert "taxas_bacen_series.json" in plugin
    assert "TAXAS_SERIES_SCHEMA_VERSION = '1.0.0'" in plugin
    assert "TAXAS_SERIES_MONTHS = 120" in plugin

    assert "validate_taxas_series_payload" in taxas
    assert "taxas_series_cdi_is_fresh" in taxas
    assert "register_financial_series_route" in taxas
    assert "register_rest_route('sfa/v1', '/taxas-historicas'" in taxas
    assert "'BCB_SELIC_META_SGS_432'" in taxas
    assert "'BCB_CDI_DAILY_SGS_12'" in taxas

    assert "'taxas_historicas_json' => 'sc_taxas_historicas_json'" in shortcodes
    assert "public function sc_taxas_historicas_json" in shortcodes

    assert artifact["schema_version"] == "1.0.0"
    assert artifact["meta"]["window"]["months"] == 120
    assert len(artifact["points"]) == 120
    assert artifact["points"][-1]["month_complete"] is False


def test_v280_php_accepts_real_series_and_serves_compact_views_fail_closed() -> None:
    php = _php()
    if php is None:
        return

    harness = r"""<?php
    define('ABSPATH', __DIR__);
    define('DAY_IN_SECONDS', 86400);

    $GLOBALS['sfa_transients'] = [];
    $GLOBALS['sfa_options'] = [];

    function current_time($type) {
      if ($type === 'Y-m-d') return '2026-09-19';
      return time();
    }
    function get_transient($key) {
      return array_key_exists($key, $GLOBALS['sfa_transients'])
        ? $GLOBALS['sfa_transients'][$key]
        : false;
    }
    function set_transient($key, $value, $ttl) {
      $GLOBALS['sfa_transients'][$key] = $value;
      return true;
    }
    function delete_transient($key) {
      unset($GLOBALS['sfa_transients'][$key]);
      return true;
    }
    function get_option($key) {
      return $GLOBALS['sfa_options'][$key] ?? false;
    }
    function update_option($key, $value, $autoload = null) {
      $GLOBALS['sfa_options'][$key] = $value;
      return true;
    }
    function wp_json_encode($value, $flags = 0, $depth = 512) {
      return json_encode($value, $flags, $depth);
    }
    function shortcode_atts($pairs, $atts, $shortcode = '') {
      return array_merge($pairs, is_array($atts) ? $atts : []);
    }

    require $argv[1];
    require $argv[2];

    final class V280_Harness {
      const TAXAS_SERIES_SCHEMA_VERSION = '1.0.0';
      const TAXAS_SERIES_MONTHS = 120;
      const CDI_MAX_OBSERVATION_AGE_DAYS = 7;
      const T_TAXAS_SERIES_CACHE = 'series_cache';
      const OPT_TAXAS_SERIES_LAST_GOOD = 'series_lg';
      const OPT_TAXAS_SERIES_ETAG = 'series_etag';
      const TTL_TAXAS_SERIES_SUCCESS = 21600;
      const TTL_TAXAS_SERIES_FAIL = 900;
      const TAXAS_SERIES_JSON_URL_DEFAULT = 'https://fixture.invalid/taxas_bacen_series.json';
      const VERSION = '2.8.0';

      use Sanida_Fiscais_Taxas_Trait;
      use Sanida_Fiscais_Shortcodes_Core_Trait;

      public function valid_series($payload, $date) {
        return $this->validate_taxas_series_payload($payload, $date);
      }
      public function view_series($months, $include) {
        return $this->taxas_series_view($months, $include);
      }
      public function shortcode_series($atts) {
        return $this->sc_taxas_historicas_json($atts);
      }
      protected function sslverify() { return true; }
    }

    $artifact = json_decode(file_get_contents($argv[3]), true);
    $h = new V280_Harness();

    $valid = $h->valid_series($artifact, '2026-09-19');

    $GLOBALS['sfa_transients'][V280_Harness::T_TAXAS_SERIES_CACHE] = $artifact;
    $view12 = $h->view_series(12, true);

    $GLOBALS['sfa_transients'][V280_Harness::T_TAXAS_SERIES_CACHE] = $artifact;
    $view12Complete = $h->view_series(12, false);

    $GLOBALS['sfa_transients'][V280_Harness::T_TAXAS_SERIES_CACHE] = $artifact;
    $shortcode = json_decode($h->shortcode_series([
      'meses' => '12',
      'incluir_mes_corrente' => '0',
    ]), true);

    $stale = $artifact;
    $last = count($stale['points']) - 1;
    $stale['points'][$last]['cdi_observation_date'] = '2026-09-10';

    $missing = $artifact;
    array_splice($missing['points'], 40, 1);

    $wrongSource = $artifact;
    $wrongSource['meta']['sources']['cdi']['source_id'] = 'WRONG';

    $wrongCompleteness = $artifact;
    $wrongCompleteness['points'][$last]['month_complete'] = true;

    echo json_encode([
      'valid_real' => $valid,
      'view12_count' => count($view12['points']),
      'view12_available' => $view12['available'],
      'view12_last' => $view12['window']['last_month'],
      'complete12_count' => count($view12Complete['points']),
      'complete12_last' => $view12Complete['window']['last_month'],
      'complete12_current' => $view12Complete['window']['include_current_month'],
      'shortcode_available' => $shortcode['available'] ?? null,
      'shortcode_count' => isset($shortcode['points']) ? count($shortcode['points']) : null,
      'shortcode_current' => $shortcode['window']['include_current_month'] ?? null,
      'stale_rejected' => !$h->valid_series($stale, '2026-09-19'),
      'missing_rejected' => !$h->valid_series($missing, '2026-09-19'),
      'source_rejected' => !$h->valid_series($wrongSource, '2026-09-19'),
      'completeness_rejected' => !$h->valid_series($wrongCompleteness, '2026-09-19'),
    ]);
    ?>"""

    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "v280-series.php"
        script.write_text(harness, encoding="utf-8")
        completed = subprocess.run(
            [php, str(script), str(TAXAS_TRAIT), str(SHORTCODES_TRAIT), str(SERIES)],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    assert completed.returncode == 0, completed.stderr or completed.stdout
    result = json.loads(completed.stdout)

    assert result["valid_real"] is True
    assert result["view12_count"] == 12
    assert result["view12_available"] is True
    assert result["view12_last"] == "2026-09"
    assert result["complete12_count"] == 12
    assert result["complete12_last"] == "2026-08"
    assert result["complete12_current"] is False
    assert result["shortcode_available"] is True
    assert result["shortcode_count"] == 12
    assert result["shortcode_current"] is False
    assert result["stale_rejected"] is True
    assert result["missing_rejected"] is True
    assert result["source_rejected"] is True
    assert result["completeness_rejected"] is True


def test_v280_series_failure_policy_contains_no_synthetic_points() -> None:
    text = TAXAS_TRAIT.read_text(encoding="utf-8")
    assert "'origin' => 'unavailable_fail_closed'" in text
    assert "'available' => false" in text
    assert "'points' => []" in text
    for forbidden in (
        "FALLBACK_SELIC_SERIES",
        "FALLBACK_CDI_SERIES",
        "'selic_annual_rate_pct' => 13.75",
        "'cdi_annualized_rate_pct' => 13.65",
    ):
        assert forbidden not in text


def test_v280_series_remains_separate_from_fiscal_contract_and_current_rates() -> None:
    plugin = PLUGIN.read_text(encoding="utf-8")
    taxas = TAXAS_TRAIT.read_text(encoding="utf-8")
    assert "CURRENT_JSON_URL_DEFAULT" in plugin
    assert "TAXAS_JSON_URL_DEFAULT" in plugin
    assert "TAXAS_SERIES_JSON_URL_DEFAULT" in plugin
    assert "validate_taxas_payload" in taxas
    assert "validate_taxas_series_payload" in taxas
    assert "minimal_taxas_fallback" in taxas
    assert "minimal_taxas_series_fallback" in taxas
