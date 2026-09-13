from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest
from hypothesis import given, strategies as st

from sanida_fiscal.contract_v1 import FiscalContractV1
from sanida_fiscal.engine_v1 import (
    FiscalEngineError,
    IrrfAssessmentIdentity,
    IrrfIncomeType,
    select_irrf_rule_bundle,
)
from sanida_fiscal.thirteenth_v1 import (
    VariableComponentMode,
    assess_thirteenth_fiscal_2026,
    assess_thirteenth_social_security,
    calculate_thirteenth_accrual,
    calculate_thirteenth_advance,
    calculate_thirteenth_gross,
    qualifies_thirteenth_month,
    select_thirteenth_reference_remuneration,
    select_thirteenth_rule_bundle,
)
from sanida_fiscal.types_v1 import (
    AssessmentContext,
    ProgressiveCalculationMethod,
    ProgressiveTablePayload,
    RoundingPolicy,
    ScalarPayload,
    ScalarUnit,
)

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_PATH = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"
REFERENCE_CASES_PATH = ROOT / "tests" / "reference_cases" / "phase1_reference_cases.json"


def D(value: str | int) -> Decimal:
    return Decimal(str(value))


def load_candidate() -> FiscalContractV1:
    return FiscalContractV1.model_validate(
        json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))
    )


def reference_cases() -> dict[str, dict]:
    payload = json.loads(REFERENCE_CASES_PATH.read_text(encoding="utf-8"))
    return {case["case_id"]: case for case in payload["cases"]}


def money_rounding(stage: str = "per_assessment_result") -> RoundingPolicy:
    return RoundingPolicy(decimal_places=2, mode="ROUND_HALF_UP", stage=stage)


def synthetic_social_security_table() -> ProgressiveTablePayload:
    return ProgressiveTablePayload.model_validate(
        {
            "type": "progressive_table",
            "calculation_method": ProgressiveCalculationMethod.MARGINAL_BY_BRACKET,
            "cap_base": "3000.00",
            "brackets": [
                {"upper_bound": "1000.00", "rate": "0.10", "deduction": "0"},
                {"upper_bound": "2000.00", "rate": "0.20", "deduction": "0"},
                {"upper_bound": None, "rate": "0.30", "deduction": "0"},
            ],
        }
    )


def thirteenth_bundle(origin: AssessmentContext = AssessmentContext.THIRTEENTH):
    return select_thirteenth_rule_bundle(load_candidate(), date(2026, 9, 13), origin)


def test_official_14_and_15_day_reference_cases() -> None:
    cases = reference_cases()
    payload = thirteenth_bundle().accrual

    case_14 = cases["thirteenth_accrual_14_days_no_avo"]
    case_15 = cases["thirteenth_accrual_15_days_one_avo"]

    assert qualifies_thirteenth_month(
        case_14["inputs"]["qualifying_days_in_month"], payload
    ) is bool(case_14["expected"]["twelfths_for_month"])
    assert qualifies_thirteenth_month(
        case_15["inputs"]["qualifying_days_in_month"], payload
    ) is bool(case_15["expected"]["twelfths_for_month"])


def test_accrual_counts_calendar_month_service_days_inclusively() -> None:
    payload = thirteenth_bundle().accrual

    fourteen_days = calculate_thirteenth_accrual(
        employment_start=date(2026, 1, 18),
        accrual_end=date(2026, 1, 31),
        reference_year=2026,
        payload=payload,
    )
    fifteen_days = calculate_thirteenth_accrual(
        employment_start=date(2026, 1, 17),
        accrual_end=date(2026, 1, 31),
        reference_year=2026,
        payload=payload,
    )

    assert fourteen_days.months[0].service_days == 14
    assert fourteen_days.twelfths == 0
    assert fifteen_days.months[0].service_days == 15
    assert fifteen_days.twelfths == 1


def test_full_year_never_exceeds_twelve_twelfths() -> None:
    payload = thirteenth_bundle().accrual
    result = calculate_thirteenth_accrual(
        employment_start=date(2020, 1, 1),
        accrual_end=date(2026, 12, 31),
        reference_year=2026,
        payload=payload,
    )
    assert result.twelfths == 12
    assert len(result.months) == 12


def test_annual_reference_uses_december_due_remuneration() -> None:
    bundle = thirteenth_bundle(AssessmentContext.THIRTEENTH)
    result = select_thirteenth_reference_remuneration(
        origin_context=AssessmentContext.THIRTEENTH,
        payload=bundle.reference,
        december_due_remuneration="4000.00",
        variable_payload=bundle.variable,
    )
    assert result.fixed_reference == D("4000.00")
    assert result.total_reference == D("4000.00")


def test_termination_reference_uses_termination_month_remuneration() -> None:
    bundle = thirteenth_bundle(AssessmentContext.TERMINATION)
    result = select_thirteenth_reference_remuneration(
        origin_context=AssessmentContext.TERMINATION,
        payload=bundle.reference,
        termination_month_remuneration="4500.00",
        variable_payload=bundle.variable,
    )
    assert result.fixed_reference == D("4500.00")
    assert result.total_reference == D("4500.00")
    assert bundle.advance is None


def test_variable_component_must_be_explicit_external_input() -> None:
    bundle = thirteenth_bundle()
    with pytest.raises(FiscalEngineError, match="explicitly marked"):
        select_thirteenth_reference_remuneration(
            origin_context=AssessmentContext.THIRTEENTH,
            payload=bundle.reference,
            december_due_remuneration="4000.00",
            variable_payload=bundle.variable,
            variable_component="500.00",
        )

    result = select_thirteenth_reference_remuneration(
        origin_context=AssessmentContext.THIRTEENTH,
        payload=bundle.reference,
        december_due_remuneration="4000.00",
        variable_payload=bundle.variable,
        variable_component_mode=VariableComponentMode.EXTERNAL_PRECOMPUTED,
        variable_component="500.00",
    )
    assert result.total_reference == D("4500.00")


def test_seven_twelfths_gross_uses_reference_and_explicit_rounding() -> None:
    bundle = thirteenth_bundle()
    accrual = calculate_thirteenth_accrual(
        employment_start=date(2026, 1, 1),
        accrual_end=date(2026, 7, 31),
        reference_year=2026,
        payload=bundle.accrual,
    )
    reference = select_thirteenth_reference_remuneration(
        origin_context=AssessmentContext.THIRTEENTH,
        payload=bundle.reference,
        december_due_remuneration="3600.00",
        variable_payload=bundle.variable,
    )
    gross = calculate_thirteenth_gross(
        reference=reference,
        accrual=accrual,
        payload=bundle.accrual,
        rounding_policy=money_rounding(),
    )
    assert accrual.twelfths == 7
    assert gross.gross_thirteenth == D("2100.00")


def test_official_fixed_advance_reference_case() -> None:
    case = reference_cases()["thirteenth_fixed_advance_previous_month_4000"]
    payload = thirteenth_bundle().advance
    assert payload is not None

    result = calculate_thirteenth_advance(
        payment_date=date(2026, 11, 30),
        previous_month_salary=case["inputs"]["previous_month_salary"],
        payload=payload,
        rounding_policy=money_rounding(),
    )
    assert result.reference_remuneration == D(
        case["expected"]["advance_reference_remuneration"]
    )
    assert result.amount == D(case["expected"]["advance_amount"])


def test_advance_does_not_fall_back_to_total13_half_for_special_cases() -> None:
    payload = thirteenth_bundle().advance
    assert payload is not None

    with pytest.raises(FiscalEngineError, match="admission_in_year"):
        calculate_thirteenth_advance(
            payment_date=date(2026, 11, 30),
            previous_month_salary="4000.00",
            payload=payload,
            rounding_policy=money_rounding(),
            admission_in_year=True,
        )
    with pytest.raises(FiscalEngineError, match="variable_remuneration"):
        calculate_thirteenth_advance(
            payment_date=date(2026, 11, 30),
            previous_month_salary="4000.00",
            payload=payload,
            rounding_policy=money_rounding(),
            has_variable_remuneration=True,
        )


def test_advance_rejects_month_outside_contract_window() -> None:
    payload = thirteenth_bundle().advance
    assert payload is not None
    with pytest.raises(FiscalEngineError, match="outside contract window"):
        calculate_thirteenth_advance(
            payment_date=date(2026, 1, 31),
            previous_month_salary="4000.00",
            payload=payload,
            rounding_policy=money_rounding(),
        )


def test_thirteenth_social_security_is_its_own_assessment() -> None:
    assessment = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.THIRTEENTH,
        origin_context=AssessmentContext.THIRTEENTH,
    )
    result = assess_thirteenth_social_security(
        assessment=assessment,
        gross_thirteenth="2500.00",
        progressive_table=synthetic_social_security_table(),
        rounding_policy=money_rounding(),
    )
    assert result.assessment == assessment
    assert result.contribution_base == D("2500.00")
    assert result.amount == D("450.00")

    monthly = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.MONTHLY,
        origin_context=AssessmentContext.MONTHLY,
    )
    with pytest.raises(FiscalEngineError, match="requires thirteenth assessment"):
        assess_thirteenth_social_security(
            assessment=monthly,
            gross_thirteenth="2500.00",
            progressive_table=synthetic_social_security_table(),
            rounding_policy=money_rounding(),
        )


def test_thirteenth_irrf_selects_its_own_reduction_rule() -> None:
    contract = load_candidate()
    assessment = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.THIRTEENTH,
        origin_context=AssessmentContext.THIRTEENTH,
    )
    bundle = select_irrf_rule_bundle(contract, date(2026, 9, 13), assessment)
    assert bundle.reduction_rule_id == "thirteenth.irrf.reduction.2026"
    assert bundle.assessment.rule_context == AssessmentContext.THIRTEENTH


def test_complete_thirteenth_fiscal_assessment_keeps_inss_and_irrf_isolated() -> None:
    contract = load_candidate()
    dependent = ScalarPayload(value=D("189.59"), unit=ScalarUnit.BRL_PER_DEPENDENT)
    simplified = ScalarPayload(value=D("607.20"), unit=ScalarUnit.BRL)

    result = assess_thirteenth_fiscal_2026(
        contract=contract,
        target_date=date(2026, 9, 13),
        origin_context=AssessmentContext.THIRTEENTH,
        gross_thirteenth="6000.00",
        social_security_table=synthetic_social_security_table(),
        social_security_rounding=money_rounding(),
        dependent_count=1,
        dependent_deduction=dependent,
        pension="100.00",
        simplified_discount=simplified,
    )

    assert result.assessment.income_type == IrrfIncomeType.THIRTEENTH
    assert result.social_security.amount == D("600.00")
    assert result.irrf.deduction_components.social_security == D("600.00")
    assert result.irrf.deduction_components.dependents == D("189.59")
    assert result.irrf.deduction_components.pension == D("100.00")
    assert result.irrf.legal_deductions == D("889.59")
    assert result.irrf.deduction_mode == "legal"
    assert result.irrf.reduction_input_income == D("6000.00")
    assert result.irrf.assessment.income_type == IrrfIncomeType.THIRTEENTH


def test_termination_thirteenth_uses_thirteenth_fiscal_context_not_monthly() -> None:
    result = assess_thirteenth_fiscal_2026(
        contract=load_candidate(),
        target_date=date(2026, 9, 13),
        origin_context=AssessmentContext.TERMINATION,
        gross_thirteenth="3000.00",
        social_security_table=synthetic_social_security_table(),
        social_security_rounding=money_rounding(),
        dependent_count=0,
        dependent_deduction=ScalarPayload(
            value=D("189.59"), unit=ScalarUnit.BRL_PER_DEPENDENT
        ),
        pension="0.00",
        simplified_discount=ScalarPayload(value=D("607.20"), unit=ScalarUnit.BRL),
    )
    assert result.assessment.origin_context == AssessmentContext.TERMINATION
    assert result.assessment.income_type == IrrfIncomeType.THIRTEENTH
    assert result.assessment.rule_context == AssessmentContext.THIRTEENTH


@given(st.integers(min_value=0, max_value=31))
def test_qualifying_day_rule_has_single_boundary(service_days: int) -> None:
    payload = thirteenth_bundle().accrual
    assert qualifies_thirteenth_month(service_days, payload) is (
        service_days >= payload.qualifying_days
    )


@given(st.dates(min_value=date(2026, 1, 1), max_value=date(2026, 12, 31)))
def test_accrual_is_always_between_zero_and_twelve(start: date) -> None:
    payload = thirteenth_bundle().accrual
    result = calculate_thirteenth_accrual(
        employment_start=start,
        accrual_end=date(2026, 12, 31),
        reference_year=2026,
        payload=payload,
    )
    assert 0 <= result.twelfths <= 12
