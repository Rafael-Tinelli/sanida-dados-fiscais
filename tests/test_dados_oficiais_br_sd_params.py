from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "integrations/wordpress/dados-oficiais-br/dados-oficiais-br.php"


def source() -> str:
    return PLUGIN.read_text(encoding="utf-8")


def test_sd_params_contract_is_evergreen_and_fail_closed() -> None:
    text = source()

    for marker in (
        "sd_parametros_json",
        "SD_PARAMS_CACHE_KEY",
        "SD_PARAMS_LAST_GOOD_OPTION",
        "CRON_HOOK_SD_PARAMS_REFRESH",
        "sd_reference_year_candidates",
        "current_reference_year_marker_missing",
        "band_continuity_mismatch",
        "second_band_reference_mismatch",
        "cap_threshold_mismatch",
        "second_band_base_mismatch",
        "minimum_wage_crosscheck_mismatch",
        "current_year_evidence_unparseable",
        "status' => 'unavailable",
        "dobr_sd_params_auto_urls",
        "portalfat.mte.gov.br/category/noticias/",
    ):
        assert marker in text

    assert "monthDay >= 111" not in text
    assert "400 * DAY_IN_SECONDS" not in text
    assert "60 * DAY_IN_SECONDS" in text

    # Annual monetary parameters must come from the official source, not source code.
    for forbidden in (
        "2222.17",
        "3703.99",
        "1777.74",
        "2518.65",
        "2138.76",
        "3564.96",
        "2424.11",
    ):
        assert forbidden not in text


def test_sd_params_parser_accepts_new_year_without_code_change() -> None:
    php = shutil.which("php")
    if php is None:
        return

    harness = r"""<?php
    const ABSPATH = __DIR__;
    const HOUR_IN_SECONDS = 3600;
    const MINUTE_IN_SECONDS = 60;
    const DAY_IN_SECONDS = 86400;

    function add_shortcode($a,$b){}
    function is_admin(){return false;}
    function add_action($a,$b,$c=null,$d=null){}
    function add_filter($a,$b,$c=null,$d=null){}
    function is_admin_bar_showing(){return false;}
    function register_activation_hook($a,$b){}
    function register_deactivation_hook($a,$b){}
    function apply_filters($tag,$value){return $value;}
    function current_time($type){
      if ($type === 'Y') return '2027';
      if ($type === 'timestamp') return strtotime('2027-02-15 12:00:00');
      return time();
    }
    function wp_strip_all_tags($s,$remove_breaks=false){return strip_tags($s);}
    function wp_json_encode($v,$flags=0){return json_encode($v,$flags);}

    require $argv[1];

    $rc = new ReflectionClass('DOBR_Plugin');
    $obj = $rc->newInstanceWithoutConstructor();

    $parser = $rc->getMethod('sd_extract_from_official_html');
    $parser->setAccessible(true);

    $html = '<article>'
      . 'Tabela Anual do Seguro-Desemprego - 2027. '
      . 'Tabela com vigência a partir de 3 de fevereiro de 2027. '
      . 'Até R$ 2.100,00 - Multiplica-se o salário médio por 75%. '
      . 'De R$ 2.100,01 até R$ 3.500,00 - O que exceder a R$ 2.100,00 multiplica-se por 55% e soma-se com R$ 1.575,00. '
      . 'Acima de R$ 3.500,00 - O valor será invariável de R$ 2.500,00. '
      . 'O valor do benefício não será inferior ao valor do salário mínimo de R$ 1.700,00 vigente para o ano de 2027.'
      . '</article>';

    $ok = $parser->invoke($obj, $html, 2027);
    if (empty($ok['ok'])) {
      fwrite(STDERR, json_encode($ok));
      exit(31);
    }

    if (($ok['reference_year'] ?? null) !== 2027) exit(32);
    if (($ok['effective_from'] ?? '') !== '2027-02-03') exit(33);
    if (abs(($ok['first_band_limit'] ?? 0) - 2100.00) > 0.001) exit(34);
    if (abs(($ok['first_band_rate'] ?? 0) - 0.75) > 0.0001) exit(35);
    if (abs(($ok['second_band_limit'] ?? 0) - 3500.00) > 0.001) exit(36);
    if (abs(($ok['second_band_excess_rate'] ?? 0) - 0.55) > 0.0001) exit(37);
    if (abs(($ok['second_band_base'] ?? 0) - 1575.00) > 0.001) exit(38);
    if (abs(($ok['cap'] ?? 0) - 2500.00) > 0.001) exit(39);
    if (abs(($ok['floor'] ?? 0) - 1700.00) > 0.001) exit(40);

    $bad = str_replace('R$ 1.575,00', 'R$ 1.580,00', $html);
    $r2 = $parser->invoke($obj, $bad, 2027);
    if (!empty($r2['ok']) || ($r2['error'] ?? '') !== 'second_band_base_mismatch') {
      fwrite(STDERR, json_encode($r2));
      exit(41);
    }

    $old = str_replace(
      ['2027', 'R$ 2.100,00', 'R$ 2.100,01', 'R$ 3.500,00', 'R$ 1.575,00', 'R$ 2.500,00', 'R$ 1.700,00'],
      ['2026', 'R$ 2.000,00', 'R$ 2.000,01', 'R$ 3.300,00', 'R$ 1.500,00', 'R$ 2.400,00', 'R$ 1.600,00'],
      $html
    );
    $r3 = $parser->invoke($obj, $old, 2027);
    if (!empty($r3['ok']) || ($r3['error'] ?? '') !== 'current_reference_year_marker_missing') exit(42);

    $activeMethod = $rc->getMethod('sd_active_candidate');
    $activeMethod->setAccessible(true);

    $previousStillActive = $activeMethod->invoke($obj, [
      2027 => ['ok'=>true, 'reference_year'=>2027, 'effective_from'=>'2027-03-01'],
      2026 => ['ok'=>true, 'reference_year'=>2026, 'effective_from'=>'2026-02-01'],
    ]);
    if (($previousStillActive['reference_year'] ?? null) !== 2026) exit(43);

    $currentNowActive = $activeMethod->invoke($obj, [
      2027 => ['ok'=>true, 'reference_year'=>2027, 'effective_from'=>'2027-02-03'],
      2026 => ['ok'=>true, 'reference_year'=>2026, 'effective_from'=>'2026-02-01'],
    ]);
    if (($currentNowActive['reference_year'] ?? null) !== 2027) exit(44);

    $yearsMethod = $rc->getMethod('sd_reference_year_candidates');
    $yearsMethod->setAccessible(true);
    $years = $yearsMethod->invoke($obj);
    if ($years !== [2027, 2026]) exit(45);

    echo "DOBR_SD_PARAMS_HARNESS_PASS\n";
    ?>"""

    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "dobr-sd-harness.php"
        script.write_text(harness, encoding="utf-8")
        run = subprocess.run(
            [php, str(script), str(PLUGIN)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )

    assert run.returncode == 0, run.stderr or run.stdout
    assert "DOBR_SD_PARAMS_HARNESS_PASS" in run.stdout
