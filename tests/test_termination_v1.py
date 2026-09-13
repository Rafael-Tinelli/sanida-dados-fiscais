from __future__ import annotations

from copy import deepcopy
from datetime import date
from decimal import Decimal
import json
from pathlib import Path

from hypothesis import given, strategies as st
import pytest

from sanida_fiscal.contract_v1 import FiscalContractV1
from sanida_fiscal.engine_v1 import FiscalEngineError
from sanida_fiscal.termination_v1 import (
    calculate_h29_limited_estimate,
    calculate_salary_balance,
    resolve_termination_reason,
    select_termination_rule_bundle,
)
from sanida_fiscal.types_v1 import (
    RoundingPolicy,
    RoundingStage,
    TerminationScopeItem,
)

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"


def load_contract() -> FiscalContractV1:
    return FiscalContractV1.model_validate(json.loads(EXAMPLE.read_text(encoding="utf-8")))


def thirteenth_rounding() -> RoundingPolicy:
    return RoundingPolicy(
        decimal_places=2,
        mode="ROUND_HALF_UP",
        stage=RoundingStage.PER_ASSESSMENT_RESULT,
    )


def test_h29_bundle_connects_exactly_the_four_supported_reasons() -> None:
    contract = load_contract()
    bundle = select_termination_rule_bundle(contract, date(2026, 3, 31))
    matrix = bundle.reason_rule.payload
    assert {row.code for row in matrix.rows} == {"01", "02", "07", "33"}
    assert set(bundle.thirteenth_eligibility.eligible_codes) == {"02", "07", "33"}
    assert set(bundle.thirteenth_eligibility.ineligible_codes) == {"01"}
    assert set(bundle.vacation_eligibility.eligible_codes) == {"02", "07", "33"}
    assert set(bundle.vacation_eligibility.ineligible_codes) == {"01"}


def test_reason_01_keeps_salary_balance_but_blocks_proportional_items() -> None:
    contract = load_contract()
    result = calculate_h29_limited_estimate(
        contract=contract,
        termination_date=date(2026, 3, 31),
        esocial_reason="01",
        employment_regime="monthly",
        contract_term="indefinite",
        employment_start=date(2025, 9, 1),
        monthly_base_salary="3100.00",
        days_counted_through_termination=10,
        termination_month_remuneration="3600.00",
        thirteenth_rounding=thirteenth_rounding(),
    )
    assert result.reason.salary_balance is True
    assert result.salary_balance is not None
    assert result.salary_balance.amount == Decimal("1000.00")
    assert result.thirteenth_proportional is None
    assert result.vacation_proportional is None
    assert result.acquired_vacation_if_due is True


@pytest.mark.parametrize("reason", ["02", "07", "33"])
def test_reasons_02_07_33_enable_both_proportional_items(reason: str) -> None:
    contract = load_contract()
    result = calculate_h29_limited_estimate(
        contract=contract,
        termination_date=date(2026, 3, 31),
        esocial_reason=reason,
        employment_regime="monthly",
        contract_term="indefinite",
        employment_start=date(2025, 9, 1),
        monthly_base_salary="3100.00",
        days_counted_through_termination=10,
        termination_month_remuneration="3600.00",
        thirteenth_rounding=thirteenth_rounding(),
    )
    assert result.salary_balance is not None
    assert result.salary_balance.amount == Decimal("1000.00")
    assert result.thirteenth_proportional is not None
    assert result.thirteenth_proportional.accrual.twelfths == 3
    assert result.thirteenth_proportional.gross.gross_thirteenth == Decimal("900.00")
    assert result.vacation_proportional is not None
    assert result.vacation_proportional.twelfths == 7


def test_h29_preserves_calendar_year_thirteenth_vs_acquisition_period_vacation() -> None:
    contract = load_contract()
    result = calculate_h29_limited_estimate(
        contract=contract,
        termination_date=date(2026, 3, 31),
        esocial_reason="02",
        employment_regime="monthly",
        contract_term="indefinite",
        employment_start=date(2025, 9, 1),
        monthly_base_salary="3100.00",
        days_counted_through_termination=31,
        termination_month_remuneration="3600.00",
        thirteenth_rounding=thirteenth_rounding(),
    )
    assert result.thirteenth_proportional is not None
    assert result.vacation_proportional is not None
    assert result.thirteenth_proportional.accrual.twelfths == 3
    assert result.vacation_proportional.twelfths == 7
    assert result.vacation_proportional.period.start == date(2025, 9, 1)
    assert result.vacation_proportional.period.end_inclusive == date(2026, 8, 31)


def test_salary_balance_uses_calendar_days_not_universal_30() -> None:
    contract = load_contract()
    bundle_march = select_termination_rule_bundle(contract, date(2026, 3, 10))
    march = calculate_salary_balance(
        monthly_base_salary="3100.00",
        termination_date=date(2026, 3, 10),
        days_counted_through_termination=10,
        payload=bundle_march.salary_proration,
        rounding_policy=bundle_march.salary_rounding,
    )
    assert march.calendar_days_in_month == 31
    assert march.amount == Decimal("1000.00")

    bundle_april = select_termination_rule_bundle(contract, date(2026, 4, 10))
    april = calculate_salary_balance(
        monthly_base_salary="3100.00",
        termination_date=date(2026, 4, 10),
        days_counted_through_termination=10,
        payload=bundle_april.salary_proration,
        rounding_policy=bundle_april.salary_rounding,
    )
    assert april.calendar_days_in_month == 30
    assert april.amount == Decimal("1033.33")
    assert april.amount != march.amount


def test_salary_balance_reference_case_31_days_is_reproduced() -> None:
    contract = load_contract()
    bundle = select_termination_rule_bundle(contract, date(2026, 3, 10))
    result = calculate_salary_balance(
        monthly_base_salary="3100.00",
        termination_date=date(2026, 3, 10),
        days_counted_through_termination=10,
        payload=bundle.salary_proration,
        rounding_policy=bundle.salary_rounding,
    )
    assert result.amount == Decimal("1000.00")


def test_unsupported_reason_fails_closed() -> None:
    contract = load_contract()
    bundle = select_termination_rule_bundle(contract, date(2026, 3, 31))
    with pytest.raises(FiscalEngineError, match="UNSUPPORTED"):
        resolve_termination_reason(
            bundle=bundle,
            esocial_reason="04",
            employment_regime="monthly",
            contract_term="indefinite",
        )


@pytest.mark.parametrize(
    ("employment_regime", "contract_term"),
    [
        ("hourly", "indefinite"),
        ("monthly", "fixed"),
    ],
)
def test_outside_h29_employment_scope_fails_closed(
    employment_regime: str,
    contract_term: str,
) -> None:
    contract = load_contract()
    bundle = select_termination_rule_bundle(contract, date(2026, 3, 31))
    with pytest.raises(FiscalEngineError, match="outside H29 v1 scope"):
        resolve_termination_reason(
            bundle=bundle,
            esocial_reason="02",
            employment_regime=employment_regime,
            contract_term=contract_term,
        )


def test_h29_result_declares_partial_estimate_and_exclusions() -> None:
    contract = load_contract()
    result = calculate_h29_limited_estimate(
        contract=contract,
        termination_date=date(2026, 3, 31),
        esocial_reason="02",
        employment_regime="monthly",
        contract_term="indefinite",
        employment_start=date(2025, 9, 1),
        monthly_base_salary="3100.00",
        days_counted_through_termination=10,
        termination_month_remuneration="3600.00",
        thirteenth_rounding=thirteenth_rounding(),
    )
    assert result.result_promise == "partial_estimate"
    assert result.user_disclosure_required is True
    assert set(result.included_items) == {
        TerminationScopeItem.SALARY_BALANCE,
        TerminationScopeItem.THIRTEENTH_PROPORTIONAL,
        TerminationScopeItem.VACATION_PROPORTIONAL,
        TerminationScopeItem.ACQUIRED_OR_OVERDUE_VACATION,
    }
    assert TerminationScopeItem.NOTICE_PAY_OR_NOTICE_DISCOUNT in result.excluded_items
    assert TerminationScopeItem.FGTS_TERMINATION_FINE in result.excluded_items
    assert TerminationScopeItem.FGTS_WITHDRAWAL in result.excluded_items
    assert TerminationScopeItem.UNEMPLOYMENT_INSURANCE in result.excluded_items
    assert TerminationScopeItem.FIXED_TERM_CONTRACT_TERMINATION_RULES in result.excluded_items


def test_h29_matrix_and_specific_item_rules_cannot_silently_diverge() -> None:
    data = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    rule = next(
        item
        for item in data["rules"]
        if item["rule_id"] == "termination.thirteenth_proportional"
    )
    rule["payload"]["eligible_codes"] = ["01", "02", "07", "33"]
    rule["payload"]["ineligible_codes"] = []
    contract = FiscalContractV1.model_validate(data)
    with pytest.raises(FiscalEngineError, match="diverges from termination matrix"):
        select_termination_rule_bundle(contract, date(2026, 3, 31))


def test_h29_partial_scope_cannot_silently_drop_a_core_item() -> None:
    data = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    rule = next(
        item
        for item in data["rules"]
        if item["rule_id"] == "termination.partial_output_scope"
    )
    rule["payload"]["included_items"].remove("vacation_proportional")
    contract = FiscalContractV1.model_validate(data)
    with pytest.raises(FiscalEngineError, match="included scope diverges"):
        select_termination_rule_bundle(contract, date(2026, 3, 31))


def test_h29_15_day_frontier_is_shared_without_conflating_accrual_calendars() -> None:
    contract = load_contract()
    before = calculate_h29_limited_estimate(
        contract=contract,
        termination_date=date(2026, 3, 30),
        esocial_reason="02",
        employment_regime="monthly",
        contract_term="indefinite",
        employment_start=date(2026, 3, 17),
        monthly_base_salary="3100.00",
        days_counted_through_termination=14,
        termination_month_remuneration="3600.00",
        thirteenth_rounding=thirteenth_rounding(),
    )
    after = calculate_h29_limited_estimate(
        contract=contract,
        termination_date=date(2026, 3, 31),
        esocial_reason="02",
        employment_regime="monthly",
        contract_term="indefinite",
        employment_start=date(2026, 3, 17),
        monthly_base_salary="3100.00",
        days_counted_through_termination=15,
        termination_month_remuneration="3600.00",
        thirteenth_rounding=thirteenth_rounding(),
    )
    assert before.thirteenth_proportional is not None
    assert before.vacation_proportional is not None
    assert after.thirteenth_proportional is not None
    assert after.vacation_proportional is not None
    assert before.thirteenth_proportional.accrual.twelfths == 0
    assert before.vacation_proportional.twelfths == 0
    assert after.thirteenth_proportional.accrual.twelfths == 1
    assert after.vacation_proportional.twelfths == 1


@given(
    first=st.integers(min_value=0, max_value=28),
    second=st.integers(min_value=0, max_value=28),
)
def test_salary_balance_is_monotonic_with_days_counted(first: int, second: int) -> None:
    contract = load_contract()
    bundle = select_termination_rule_bundle(contract, date(2026, 2, 28))
    low, high = sorted((first, second))
    low_result = calculate_salary_balance(
        monthly_base_salary="2800.00",
        termination_date=date(2026, 2, 28),
        days_counted_through_termination=low,
        payload=bundle.salary_proration,
        rounding_policy=bundle.salary_rounding,
    )
    high_result = calculate_salary_balance(
        monthly_base_salary="2800.00",
        termination_date=date(2026, 2, 28),
        days_counted_through_termination=high,
        payload=bundle.salary_proration,
        rounding_policy=bundle.salary_rounding,
    )
    assert Decimal("0.00") <= low_result.amount <= high_result.amount <= Decimal("2800.00")
