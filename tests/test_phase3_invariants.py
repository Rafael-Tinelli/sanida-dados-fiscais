from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta
from decimal import Decimal
import json
from pathlib import Path

from hypothesis import given, strategies as st
import pytest

from sanida_fiscal.contract_v1 import FiscalContractV1
from sanida_fiscal.engine_v1 import (
    IrrfAssessmentIdentity,
    IrrfIncomeType,
    calculate_affine_reduction,
    select_irrf_rule_bundle,
)
from sanida_fiscal.money import quantize
from sanida_fiscal.termination_v1 import (
    calculate_salary_balance,
    resolve_termination_reason,
    select_termination_rule_bundle,
)
from sanida_fiscal.thirteenth_v1 import (
    calculate_thirteenth_accrual,
    select_thirteenth_rule_bundle,
)
from sanida_fiscal.types_v1 import AssessmentContext
from sanida_fiscal.vacation_v1 import (
    calculate_proportional_vacation_accrual,
    cash_allowance_tax_bases,
    resolve_cash_allowance_tax_treatment,
    select_vacation_acquisition_rule,
    select_vacation_rule_bundle,
)

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"


def D(value: str | int) -> Decimal:
    return Decimal(str(value))


def load_contract() -> FiscalContractV1:
    return FiscalContractV1.model_validate(json.loads(EXAMPLE.read_text(encoding="utf-8")))


def monthly_irrf_rules():
    assessment = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.MONTHLY,
        origin_context=AssessmentContext.MONTHLY,
    )
    return select_irrf_rule_bundle(load_contract(), date(2026, 9, 13), assessment)


@pytest.mark.parametrize(
    ("income", "expected_statutory"),
    [
        ("4999.99", "312.89"),
        ("5000.00", "312.89"),
        ("5000.01", "312.89"),
        ("7349.99", "0.01"),
        ("7350.00", "0.00"),
        ("7350.01", "0.00"),
    ],
)
def test_reduction_boundaries_are_explicit_and_never_use_post_deduction_base(
    income: str, expected_statutory: str
) -> None:
    rules = monthly_irrf_rules()
    result = calculate_affine_reduction(
        input_income=income,
        pre_reduction_tax="9999.99",
        payload=rules.reduction,
        rounding_policy=rules.reduction_rounding,
    )
    assert result.input_income == D(income)
    assert result.statutory_reduction == D(expected_statutory)
    assert D("0") <= result.applied_reduction <= D("9999.99")
    assert D("0") <= result.final_tax <= D("9999.99")


@given(
    month=st.integers(min_value=1, max_value=12),
    salary_cents=st.integers(min_value=0, max_value=2_000_000),
    days=st.integers(min_value=0, max_value=28),
)
def test_salary_balance_matches_calendar_denominator_across_months(
    month: int, salary_cents: int, days: int
) -> None:
    contract = load_contract()
    calendar_days = monthrange(2026, month)[1]
    termination_date = date(2026, month, calendar_days)
    bundle = select_termination_rule_bundle(contract, termination_date)
    salary = Decimal(salary_cents) / Decimal(100)
    result = calculate_salary_balance(
        monthly_base_salary=salary,
        termination_date=termination_date,
        days_counted_through_termination=days,
        payload=bundle.salary_proration,
        rounding_policy=bundle.salary_rounding,
    )
    expected = quantize(
        salary * Decimal(days) / Decimal(calendar_days),
        bundle.salary_rounding,
    )
    assert result.calendar_days_in_month == calendar_days
    assert result.amount == expected
    assert D("0") <= result.amount <= salary


@given(
    first=st.integers(min_value=0, max_value=364),
    second=st.integers(min_value=0, max_value=364),
)
def test_vacation_accrual_is_monotonic_inside_one_acquisition_period(
    first: int, second: int
) -> None:
    payload = select_vacation_acquisition_rule(
        load_contract(), date(2026, 8, 31), AssessmentContext.TERMINATION
    )
    start = date(2025, 9, 1)
    low, high = sorted((first, second))
    low_result = calculate_proportional_vacation_accrual(
        employment_start=start,
        through_date=start + timedelta(days=low),
        payload=payload,
    )
    high_result = calculate_proportional_vacation_accrual(
        employment_start=start,
        through_date=start + timedelta(days=high),
        payload=payload,
    )
    assert 0 <= low_result.twelfths <= high_result.twelfths <= 12
    assert low_result.period.start == high_result.period.start == start


@given(
    first=st.integers(min_value=0, max_value=364),
    second=st.integers(min_value=0, max_value=364),
)
def test_thirteenth_accrual_is_monotonic_within_reference_year(
    first: int, second: int
) -> None:
    payload = select_thirteenth_rule_bundle(
        load_contract(), date(2026, 12, 31), AssessmentContext.THIRTEENTH
    ).accrual
    year_start = date(2026, 1, 1)
    low, high = sorted((first, second))
    low_result = calculate_thirteenth_accrual(
        employment_start=year_start,
        accrual_end=year_start + timedelta(days=low),
        reference_year=2026,
        payload=payload,
    )
    high_result = calculate_thirteenth_accrual(
        employment_start=year_start,
        accrual_end=year_start + timedelta(days=high),
        reference_year=2026,
        payload=payload,
    )
    assert 0 <= low_result.twelfths <= high_result.twelfths <= 12


@pytest.mark.parametrize(
    ("code", "thirteenth", "vacation"),
    [
        ("01", False, False),
        ("02", True, True),
        ("07", True, True),
        ("33", True, True),
    ],
)
def test_h29_reason_matrix_is_total_over_the_closed_supported_set(
    code: str, thirteenth: bool, vacation: bool
) -> None:
    bundle = select_termination_rule_bundle(load_contract(), date(2026, 9, 13))
    decision = resolve_termination_reason(
        bundle=bundle,
        esocial_reason=code,
        employment_regime="monthly",
        contract_term="indefinite",
    )
    assert decision.salary_balance is True
    assert decision.thirteenth_proportional is thirteenth
    assert decision.vacation_proportional is vacation


@given(
    principal_cents=st.integers(min_value=0, max_value=1_000_000),
    third_cents=st.integers(min_value=0, max_value=500_000),
)
def test_cash_allowance_tax_bases_never_merge_principal_into_irrf_or_cp(
    principal_cents: int, third_cents: int
) -> None:
    bundle = select_vacation_rule_bundle(load_contract(), date(2026, 9, 13))
    treatment = resolve_cash_allowance_tax_treatment(
        principal_payload=bundle.principal_incidence,
        constitutional_third_payload=bundle.constitutional_third_incidence,
    )
    principal = Decimal(principal_cents) / Decimal(100)
    third = Decimal(third_cents) / Decimal(100)
    bases = cash_allowance_tax_bases(
        principal_amount=principal,
        constitutional_third_amount=third,
        treatment=treatment,
    )
    assert bases.irrf_taxable_amount == third
    assert bases.social_security_base == D("0")
    assert bases.principal_amount + bases.constitutional_third_amount == principal + third
