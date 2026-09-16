from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import shutil
import subprocess

import pytest

from sanida_fiscal.engine_v1 import FiscalEngineError
from sanida_fiscal.publication_v1 import FiscalReleaseStore
from sanida_fiscal.termination_v1 import calculate_h29_limited_estimate
from sanida_fiscal.types_v1 import AssessmentContext, TerminationScopeItem

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "consumers/frontend/folha-core.js"
TERMINATION = ROOT / "consumers/frontend/folha-termination.js"
H29 = ROOT / "consumers/frontend/rescisao-clt.js"
H29_PAGE = ROOT / "consumers/frontend/rescisao-clt/index.php"
H29_PARTS = ROOT / "consumers/frontend/rescisao-clt/parts"
NODE_RUNTIME = ROOT / "tests/js/phase6_c66_h29_runtime.cjs"
STORE = ROOT / "releases/fiscal-v1"
TARGET_DATE = date(2026, 3, 31)


def _node_result() -> dict:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for C6.6 H29 parity")
    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    completed = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(TERMINATION), str(H29), str(artifact)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def _python_case(reason: str = "02", **overrides):
    release = FiscalReleaseStore(STORE).load_current()
    assert release is not None
    rounding_rule = release.select_rule(
        "technical.money_decimal_and_rounding", TARGET_DATE, AssessmentContext.TECHNICAL
    )
    assert rounding_rule.rounding_policy is not None
    params = {
        "contract": release,
        "esocial_reason": reason,
        "employment_regime": "monthly",
        "contract_term": "indefinite",
        "employment_start": date(2025, 9, 1),
        "termination_date": date(2026, 3, 31),
        "monthly_base_salary": "3100.00",
        "days_counted_through_termination": 10,
        "termination_month_remuneration": "3600.00",
        "thirteenth_rounding": rounding_rule.rounding_policy,
    }
    params.update(overrides)
    return release, calculate_h29_limited_estimate(**params)


def _python_standard(reason: str = "02"):
    return _python_case(reason)


def test_c66_h29_standard_case_matches_python_engine() -> None:
    release, py = _python_standard()
    node = _node_result()
    h29 = node["standard"]

    assert node["release_id"] == release.release_id
    assert h29["fiscal_metadata"]["release_id"] == release.release_id
    assert h29["consumer"] == "H29"
    assert h29["result_promise"] == py.result_promise == "partial_estimate"
    assert h29["user_disclosure_required"] is py.user_disclosure_required is True
    assert h29["reason"]["code"] == py.reason.code == "02"

    assert py.salary_balance is not None
    assert Decimal(h29["salary_balance"]["amount"]) == py.salary_balance.amount == Decimal("1000.00")
    assert h29["salary_balance"]["calendar_days_in_month"] == py.salary_balance.calendar_days_in_month == 31

    assert py.thirteenth_proportional is not None
    assert h29["thirteenth_proportional"]["twelfths"] == py.thirteenth_proportional.accrual.twelfths == 3
    assert Decimal(h29["thirteenth_proportional"]["reference_remuneration"]) == py.thirteenth_proportional.reference.total_reference == Decimal("3600.00")
    assert Decimal(h29["thirteenth_proportional"]["gross_thirteenth"]) == py.thirteenth_proportional.gross.gross_thirteenth == Decimal("900.00")

    assert py.vacation_proportional is not None
    assert h29["vacation_proportional"]["twelfths"] == py.vacation_proportional.twelfths == 7
    assert h29["vacation_proportional"]["period"]["start"] == py.vacation_proportional.period.start.isoformat() == "2025-09-01"
    assert h29["vacation_proportional"]["period"]["end_inclusive"] == py.vacation_proportional.period.end_inclusive.isoformat() == "2026-08-31"


def test_c66_reason_matrix_is_exact_and_reason_01_blocks_proportionals() -> None:
    result = _node_result()
    by_code = {row["code"]: row for row in result["reasons"]}
    assert set(by_code) == {"01", "02", "07", "33"}
    assert by_code["01"]["has_salary_balance"] is True
    assert by_code["01"]["has_thirteenth"] is False
    assert by_code["01"]["has_vacation"] is False
    for code in ("02", "07", "33"):
        assert by_code[code]["has_salary_balance"] is True
        assert by_code[code]["has_thirteenth"] is True
        assert by_code[code]["has_vacation"] is True

    _, py01 = _python_standard("01")
    assert py01.salary_balance is not None
    assert py01.thirteenth_proportional is None
    assert py01.vacation_proportional is None


def test_c66_salary_balance_uses_real_calendar_days_not_universal_30() -> None:
    result = _node_result()
    march = result["march_salary_balance"]
    april = result["april_salary_balance"]
    assert march["calendar_days_in_month"] == 31
    assert Decimal(march["amount"]) == Decimal("1000.00")
    assert april["calendar_days_in_month"] == 30
    assert Decimal(april["amount"]) == Decimal("1033.33")


def test_c66_thirteenth_and_vacation_keep_distinct_calendars_and_15_day_boundary() -> None:
    result = _node_result()
    standard = result["standard"]
    assert standard["thirteenth_proportional"]["twelfths"] == 3
    assert standard["vacation_proportional"]["twelfths"] == 7
    assert result["boundary14"] == {"thirteenth_twelfths": 0, "vacation_twelfths": 0}
    assert result["boundary15"] == {"thirteenth_twelfths": 1, "vacation_twelfths": 1}


def test_c66_scope_is_explicitly_partial_and_excludes_unmodeled_termination_items() -> None:
    release, py = _python_standard()
    node = _node_result()["standard"]
    assert node["result_promise"] == "partial_estimate"
    assert node["user_disclosure_required"] is True
    assert set(node["included_items"]) == {item.value for item in py.included_items}
    assert set(node["excluded_items"]) == {item.value for item in py.excluded_items}
    for item in (
        TerminationScopeItem.NOTICE_PAY_OR_NOTICE_DISCOUNT,
        TerminationScopeItem.FGTS_TERMINATION_FINE,
        TerminationScopeItem.FGTS_WITHDRAWAL,
        TerminationScopeItem.UNEMPLOYMENT_INSURANCE,
        TerminationScopeItem.STABILITY_INDEMNITIES,
        TerminationScopeItem.FIXED_TERM_CONTRACT_TERMINATION_RULES,
        TerminationScopeItem.VARIABLE_TERMINATION_ITEMS_NOT_EXPLICITLY_MODELED,
    ):
        assert item.value in node["excluded_items"]
    assert node["acquired_or_overdue_vacation"]["monetary_calculation"] == "not_automated_in_h29_v1"
    assert node["acquired_or_overdue_vacation"]["overdue_double"] == "requires_explicit_rule"
    assert node["fiscal_metadata"]["release_id"] == release.release_id


def test_c66_h29_fails_closed_outside_supported_scope() -> None:
    result = _node_result()
    assert result["unsupported_reason_rejected"] is True
    assert result["hourly_rejected"] is True
    assert result["fixed_term_rejected"] is True
    assert result["excess_days_rejected"] is True
    assert result["negative_money_rejected"] is True
    assert result["required_rules_present"] is True


def test_c66_h29_rejects_missing_or_zero_required_monetary_bases() -> None:
    result = _node_result()
    assert result["empty_salary_rejected"] is True
    assert result["whitespace_salary_rejected"] is True
    assert result["zero_salary_rejected"] is True
    assert result["empty_thirteenth_reference_rejected"] is True
    assert result["zero_thirteenth_reference_rejected"] is True
    assert result["reason01_allows_empty_thirteenth_reference"] is True

    with pytest.raises(FiscalEngineError, match="monthly_base_salary must be greater than zero"):
        _python_case(monthly_base_salary="0")
    with pytest.raises(FiscalEngineError, match="termination_month_remuneration must be greater than zero"):
        _python_case(termination_month_remuneration="0")

    _, reason01 = _python_case(reason="01", termination_month_remuneration="0")
    assert reason01.thirteenth_proportional is None


def test_c66_h29_same_month_days_suggestion_uses_inclusive_employment_period() -> None:
    suggestions = _node_result()["suggested_days"]
    assert suggestions["same_month_midmonth"] == 11
    assert suggestions["same_month_from_first"] == 20
    assert suggestions["leap_february"] == 2
    assert suggestions["prior_month_admission"] == 20
    assert suggestions["no_admission_yet"] == 20
    assert suggestions["admission_after_termination"] is None


def test_c66_source_has_no_legacy_rescission_shortcuts_or_complete_total_promise() -> None:
    shared = TERMINATION.read_text(encoding="utf-8")
    h29 = H29.read_text(encoding="utf-8")
    for forbidden in (
        "dados_fiscais.json",
        "/sfa/v1/folha",
        "avosFeriasProporcionais",
        "salario / 30",
        "salario/30",
        "Math.round",
        "total da rescisão =",
        "terminationMonthRemuneration: inputValue('remuneracao_mes_desligamento') || '0'",
    ):
        assert forbidden not in shared
        assert forbidden not in h29
    for required in (
        "termination.reason_scope",
        "termination.partial_output_scope",
        "termination.salary_balance",
        "termination.thirteenth_proportional",
        "termination.vacation_proportional",
        "termination.acquired_and_overdue_vacation",
        "thirteenth.accrual.twelfths",
        "thirteenth.reference_remuneration",
        "vacation.acquisition_period",
        "technical.money_decimal_and_rounding",
        "universal_fixed_denominator !== false",
        "partial_estimate",
        "positiveMoney(req.monthlyBaseSalary",
        "positiveMoney(req.terminationMonthRemuneration",
    ):
        assert required in shared
    for required in (
        "SFA.fetchRelease({ consumer: CONSUMER })",
        "SFA.H29.calculate(",
        "suggestedDaysCounted",
        "employmentStartField.addEventListener('change', syncDaysWithDates)",
        "terminationMonthRemuneration: inputValue('remuneracao_mes_desligamento')",
    ):
        assert required in h29


def test_c66_page_requires_reason_and_exposes_partial_scope_and_audit_memory() -> None:
    page = H29_PAGE.read_text(encoding="utf-8")
    source = page + "\n" + "\n".join(
        p.read_text(encoding="utf-8") for p in sorted(H29_PARTS.glob("*.php"))
    )
    assert source.index("/financas/calculadoras/assets/folha-core.js") < source.index(
        "/financas/calculadoras/assets/folha-termination.js"
    ) < source.index("/financas/calculadoras/assets/rescisao-clt.js")
    for marker in (
        'name="motivo_esocial"',
        'value="01"',
        'value="02"',
        'value="07"',
        'value="33"',
        'name="data_admissao"',
        'name="data_desligamento"',
        'name="salario_base_mensal"',
        'name="dias_computados"',
        'name="remuneracao_mes_desligamento"',
        'data-row="saldo-formula"',
        'data-row="13-avos"',
        'data-row="ferias-periodo"',
        'data-row="ferias-avos"',
        'data-list="incluidos"',
        'data-list="excluidos"',
        'data-row="release-id"',
        "Esta não é uma calculadora de “total da rescisão”",
        "Fora do cálculo automático",
        "se a admissão ocorreu no mesmo mês",
        "Ajuste manualmente se férias, afastamentos",
    ):
        assert marker in source
