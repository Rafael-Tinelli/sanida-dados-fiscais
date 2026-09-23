from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
PLUGIN = ROOT / "integrations/wordpress/dados-oficiais-br/dados-oficiais-br.php"


def source() -> str:
    return PLUGIN.read_text(encoding="utf-8")


def test_dobr_v148_identity_and_shortcodes() -> None:
    text = source()
    assert "Plugin Name: Dados Oficiais BR" in text
    assert "Version: 1.4.9" in text
    for shortcode in (
        "sm_valor",
        "sm_valor_raw",
        "sm_vigencia",
        "sm_dia",
        "sm_hora",
        "pis_valor",
        "sd_min_parcela",
        "sd_parametros_json",
        "sm_serie_json",
        "desemprego_serie_json",
        "desemprego_valor",
    ):
        assert f"add_shortcode('{shortcode}'" in text


def test_dobr_salary_is_year_aware_and_fail_closed() -> None:
    text = source()
    for marker in (
        "current_year_marker_missing",
        "employee_first_bracket_missing",
        "contributor_floor_confirmation_missing",
        "contributor_floor_confirmation_mismatch",
        "inss_current_year_employee_bracket_plus_contributor_floor",
        "cacheYear === $currentYear",
        "sm_effective_is_current",
        "return '—';",
    ):
        assert marker in text

    assert "DOBR_SM_SEED_VALOR', 1621.00" not in text
    assert "DOBR_SM_SEED_VIGENCIA', '2026-01-01'" not in text
    assert "Default atualizado para 2026" not in text
    assert "$defaults = ['valor'=>0.0, 'vigencia'=>''];" in text


def test_dobr_salary_rounding_contract_is_locked() -> None:
    text = source()
    assert "sm_ratio_round_up_cent" in text
    assert "ceil((($valor * 100.0) / $divisor) - 1e-12)" in text
    assert "ceil(((float)$sm['valor'] / 12.0) * $m - 1e-12)" in text


def test_dobr_unemployment_last_good_has_freshness_limit() -> None:
    text = source()
    for marker in (
        "DOBR_DESEMPREGO_LAST_GOOD_MAX_AGE",
        "60 * DAY_IN_SECONDS",
        "desemprego_last_good_is_fresh",
        "last_good_age_seconds",
        "last_good_max_age_seconds",
        "last_good_stale",
        "stale desemprego fallback cache invalidated",
    ):
        assert marker in text


def test_dobr_has_no_stale_admin_version_copy() -> None:
    text = source()
    assert "plugin 1.4.4" not in text
    assert "Status do Salário Mínimo (v1.4.4)" not in text
    assert "Status do Salário Mínimo (v1.4.9)" in text
    assert text.count("'plugin_version_esperada' => '1.4.9'") == 2


def test_dobr_php_lint_and_evergreen_harness() -> None:
    php = shutil.which("php")
    if php is None:
        return

    lint = subprocess.run(
        [php, "-l", str(PLUGIN)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert lint.returncode == 0, lint.stderr or lint.stdout

    harness = r"""<?php
    const ABSPATH = __DIR__;
    const HOUR_IN_SECONDS = 3600;
    const MINUTE_IN_SECONDS = 60;
    const DAY_IN_SECONDS = 86400;
    function add_shortcode($a,$b){}
    function is_admin(){return false;}
    function add_action($a,$b,$c=null,$d=null){}
    function is_admin_bar_showing(){return false;}
    function add_filter($a,$b,$c=null,$d=null){}
    function register_activation_hook($a,$b){}
    function register_deactivation_hook($a,$b){}
    function wp_next_scheduled($a){return false;}
    function wp_strip_all_tags($s,$remove_breaks=false){return strip_tags($s);}
    function current_time($type){return $type==='Y' ? '2026' : time();}
    function wp_json_encode($v,$flags=0){return json_encode($v,$flags);}
    require $argv[1];

    $rc = new ReflectionClass('DOBR_Plugin');
    $obj = $rc->newInstanceWithoutConstructor();

    $parser = $rc->getMethod('sm_extract_from_gov_html');
    $parser->setAccessible(true);
    $ok = "TABELAS VÁLIDAS A PARTIR DA COMPETÊNCIA JANEIRO DE 2026 "
        . "1. Para Empregado, Empregado Doméstico e Trabalhador Avulso: "
        . "Salário de Contribuição (R$) Até R$ 1.621,00 7,5% "
        . "2. Para Contribuinte Individual, Facultativo e Microempreendedor Individual - MEI: "
        . "Salário de Contribuição (R$) Alíquota Valor "
        . "R$ 1.621,00 5% (*) R$ 81,05 R$ 1.621,00 11% (**) R$ 178,31";
    $bad = str_replace('R$ 1.621,00 11%', 'R$ 1.700,00 11%', $ok);

    $r1 = $parser->invoke($obj, $ok, 2026);
    $r2 = $parser->invoke($obj, $bad, 2026);

    if (empty($r1['ok']) || abs($r1['valor'] - 1621) > 0.001) exit(21);
    if (!empty($r2['ok']) || ($r2['error'] ?? '') !== 'contributor_floor_confirmation_mismatch') exit(22);

    $fresh = $rc->getMethod('desemprego_last_good_is_fresh');
    $fresh->setAccessible(true);
    if (!$fresh->invoke($obj, ['ts' => time() - 3600])) exit(23);
    if ($fresh->invoke($obj, ['ts' => time() - (61 * DAY_IN_SECONDS)])) exit(24);

    $ratio = $rc->getMethod('sm_ratio_round_up_cent');
    $ratio->setAccessible(true);
    if (abs($ratio->invoke($obj, 1621.0, 30) - 54.04) > 0.001) exit(25);
    if (abs($ratio->invoke($obj, 1621.0, 220) - 7.37) > 0.001) exit(26);

    echo "DOBR_HARNESS_PASS\n";
    ?>"""

    with tempfile.TemporaryDirectory() as tmp:
        script = Path(tmp) / "dobr-harness.php"
        script.write_text(harness, encoding="utf-8")
        run = subprocess.run(
            [php, str(script), str(PLUGIN)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )

    assert run.returncode == 0, run.stderr or run.stdout
    assert "DOBR_HARNESS_PASS" in run.stdout
