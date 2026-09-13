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
    assess_irrf_2026,
    build_irrf_legal_deductions,
    calculate_affine_reduction,
    calculate_progressive,
    select_irrf_rule_bundle,
)
from sanida_fiscal.money import DecimalInputError, as_decimal
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
TARGET_DATE = date(2026, 9, 13)


def D(value: str | int) -> Decimal:
    return Decimal(str(value))


def load_candidate() -> FiscalContractV1:
    return FiscalContractV1.model_validate(
        json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))
    )


def reference_cases() -> dict[str, dict]:
    payload = json.loads(REFERENCE_CASES_PATH.read_text(encoding="utf-8"))
    return {case["case_id"]: case for case in payload["cases"]}


def monthly_assessment() -> IrrfAssessmentIdentity:
    return IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.MONTHLY,
        origin_context=AssessmentContext.MONTHLY,
    )


def dependent_payload_for(
    contract: FiscalContractV1, assessment: IrrfAssessmentIdentity
) -> ScalarPayload:
    rule = contract.select_rule(
        "irrf.dependent_deduction", TARGET_DATE, assessment.rule_context
    )
    assert isinstance(rule.payload, ScalarPayload)
    return rule.payload


def simplified_discount() -> ScalarPayload:
    # The scalar family/value is frozen in Phase 1. It is intentionally passed
    # to each assessment instead of kept as global mutable engine state.
    return ScalarPayload(value=D("607.20"), unit=ScalarUnit.BRL)


def assessment_deductions(
    contract: FiscalContractV1,
    assessment: IrrfAssessmentIdentity,
    *,
    social_security: Decimal,
    dependent_count: int = 0,
    pension: Decimal = D("0"),
):
    return build_irrf_legal_deductions(
        assessment=assessment,
        social_security=social_security,
        dependent_count=dependent_count,
        dependent_deduction=dependent_payload_for(contract, assessment),
        pension=pension,
    )


@pytest.mark.parametrize(
    "case_id",
    [
        "rfd_irrf_2026_salary_3036",
        "rfd_irrf_2026_salary_4000",
        "rfd_irrf_2026_salary_5000",
        "rfd_irrf_2026_salary_6000_A01_regression",
        "rfd_irrf_2026_salary_7607_20_boundary_semantics",
    ],
)
def test_official_rfb_2026_irrf_reference_cases(case_id: str) -> None:
    cases = reference_cases()
    case = cases[case_id]
    contract = load_candidate()
    assessment = monthly_assessment()
    rules = select_irrf_rule_bundle(contract, TARGET_DATE, assessment)

    social_security = D(case["inputs"].get("official_social_security_deduction", "0"))
    deductions = assessment_deductions(
        contract, assessment, social_security=social_security
    )

    result = assess_irrf_2026(
        assessment=assessment,
        gross_taxable_income=D(case["inputs"]["taxable_income"]),
        legal_deductions=deductions,
        simplified_discount=simplified_discount(),
        rules=rules,
    )

    expected = case["expected"]
    assert result.assessment == assessment
    assert result.deduction_mode == expected["deduction_mode"]
    assert result.irrf_tax_base == D(expected["irrf_tax_base"])
    assert result.pre_reduction_irrf == D(expected["pre_reduction_irrf"])
    assert result.final_irrf == D(expected["final_irrf"])

    if "reduction_input_income" in expected:
        assert result.reduction_input_income == D(expected["reduction_input_income"])
    if "reduction_applied" in expected:
        assert result.reduction_amount == D(expected["reduction_applied"])


def test_A01_reduction_input_never_collapses_to_irrf_tax_base() -> None:
    case = reference_cases()["rfd_irrf_2026_salary_6000_A01_regression"]
    contract = load_candidate()
    assessment = monthly_assessment()
    rules = select_irrf_rule_bundle(contract, TARGET_DATE, assessment)
    deductions = assessment_deductions(
        contract, assessment, social_security=D("649.60")
    )

    result = assess_irrf_2026(
        assessment=assessment,
        gross_taxable_income=D("6000.00"),
        legal_deductions=deductions,
        simplified_discount=simplified_discount(),
        rules=rules,
    )

    assert case["invariant"] == "reduction_input_income != irrf_tax_base"
    assert result.reduction_input_income == D("6000.00")
    assert result.irrf_tax_base == D("5350.40")
    assert result.reduction_input_income != result.irrf_tax_base
    assert result.final_irrf == D("382.88")


def test_legal_deduction_components_are_explicit_and_context_bound() -> None:
    contract = load_candidate()
    assessment = monthly_assessment()
    deductions = assessment_deductions(
        contract,
        assessment,
        social_security=D("500.00"),
        dependent_count=2,
        pension=D("100.00"),
    )

    assert deductions.social_security == D("500.00")
    assert deductions.dependent_unit == D("189.59")
    assert deductions.dependents == D("379.18")
    assert deductions.pension == D("100.00")
    assert deductions.total == D("979.18")
    assert deductions.assessment == assessment


def test_dependent_scalar_must_be_per_dependent() -> None:
    assessment = monthly_assessment()
    with pytest.raises(FiscalEngineError, match="BRL_per_dependent"):
        build_irrf_legal_deductions(
            assessment=assessment,
            social_security=D("0"),
            dependent_count=1,
            dependent_deduction=ScalarPayload(value=D("189.59"), unit=ScalarUnit.BRL),
            pension=D("0"),
        )


def test_wrong_income_type_for_origin_context_is_rejected() -> None:
    with pytest.raises(FiscalEngineError, match="incompatible with origin context"):
        IrrfAssessmentIdentity(
            income_type=IrrfIncomeType.THIRTEENTH,
            origin_context=AssessmentContext.MONTHLY,
        )


def test_termination_is_origin_not_a_fourth_irrf_income_type() -> None:
    salary_balance = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.MONTHLY,
        origin_context=AssessmentContext.TERMINATION,
    )
    thirteenth = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.THIRTEENTH,
        origin_context=AssessmentContext.TERMINATION,
    )

    assert salary_balance.rule_context == AssessmentContext.MONTHLY
    assert thirteenth.rule_context == AssessmentContext.THIRTEENTH

    with pytest.raises(FiscalEngineError, match="incompatible with origin context"):
        IrrfAssessmentIdentity(
            income_type=IrrfIncomeType.VACATION,
            origin_context=AssessmentContext.TERMINATION,
        )


def test_rule_selection_is_income_type_specific_even_inside_termination() -> None:
    contract = load_candidate()
    salary_balance = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.MONTHLY,
        origin_context=AssessmentContext.TERMINATION,
    )
    thirteenth = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.THIRTEENTH,
        origin_context=AssessmentContext.TERMINATION,
    )

    salary_rules = select_irrf_rule_bundle(contract, TARGET_DATE, salary_balance)
    thirteenth_rules = select_irrf_rule_bundle(contract, TARGET_DATE, thirteenth)

    assert salary_rules.reduction_rule_id == "irrf.reduction.2026"
    assert thirteenth_rules.reduction_rule_id == "thirteenth.irrf.reduction.2026"
    assert salary_rules.assessment.rule_context == AssessmentContext.MONTHLY
    assert thirteenth_rules.assessment.rule_context == AssessmentContext.THIRTEENTH


def test_monthly_deductions_cannot_leak_into_thirteenth_assessment() -> None:
    contract = load_candidate()
    monthly = monthly_assessment()
    thirteenth = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.THIRTEENTH,
        origin_context=AssessmentContext.THIRTEENTH,
    )
    monthly_deductions = assessment_deductions(
        contract, monthly, social_security=D("700.00"), pension=D("50.00")
    )
    thirteenth_rules = select_irrf_rule_bundle(contract, TARGET_DATE, thirteenth)

    with pytest.raises(FiscalEngineError, match="different IRRF assessment context"):
        assess_irrf_2026(
            assessment=thirteenth,
            gross_taxable_income=D("6000.00"),
            legal_deductions=monthly_deductions,
            simplified_discount=simplified_discount(),
            rules=thirteenth_rules,
        )


def test_rule_bundle_cannot_be_reused_across_assessments() -> None:
    contract = load_candidate()
    monthly = monthly_assessment()
    vacation = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.VACATION,
        origin_context=AssessmentContext.VACATION_ENJOYED,
    )
    deductions = assessment_deductions(
        contract, monthly, social_security=D("700.00")
    )
    vacation_rules = select_irrf_rule_bundle(contract, TARGET_DATE, vacation)

    with pytest.raises(FiscalEngineError, match="rule bundle belongs to a different assessment"):
        assess_irrf_2026(
            assessment=monthly,
            gross_taxable_income=D("6000.00"),
            legal_deductions=deductions,
            simplified_discount=simplified_discount(),
            rules=vacation_rules,
        )


def test_termination_monthly_and_thirteenth_keep_distinct_deduction_memories() -> None:
    contract = load_candidate()
    salary_balance = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.MONTHLY,
        origin_context=AssessmentContext.TERMINATION,
    )
    thirteenth = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.THIRTEENTH,
        origin_context=AssessmentContext.TERMINATION,
    )

    salary_deductions = assessment_deductions(
        contract,
        salary_balance,
        social_security=D("700.00"),
        pension=D("0"),
    )
    thirteenth_deductions = assessment_deductions(
        contract,
        thirteenth,
        social_security=D("800.00"),
        pension=D("100.00"),
    )

    salary_result = assess_irrf_2026(
        assessment=salary_balance,
        gross_taxable_income=D("6000.00"),
        legal_deductions=salary_deductions,
        simplified_discount=simplified_discount(),
        rules=select_irrf_rule_bundle(contract, TARGET_DATE, salary_balance),
    )
    thirteenth_result = assess_irrf_2026(
        assessment=thirteenth,
        gross_taxable_income=D("6000.00"),
        legal_deductions=thirteenth_deductions,
        simplified_discount=simplified_discount(),
        rules=select_irrf_rule_bundle(contract, TARGET_DATE, thirteenth),
    )

    assert salary_result.assessment.origin_context == AssessmentContext.TERMINATION
    assert thirteenth_result.assessment.origin_context == AssessmentContext.TERMINATION
    assert salary_result.assessment.income_type == IrrfIncomeType.MONTHLY
    assert thirteenth_result.assessment.income_type == IrrfIncomeType.THIRTEENTH
    assert salary_result.legal_deductions == D("700.00")
    assert thirteenth_result.legal_deductions == D("900.00")
    assert salary_result.deduction_components != thirteenth_result.deduction_components


def test_thirteenth_assessment_uses_own_reduction_rule() -> None:
    contract = load_candidate()
    assessment = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.THIRTEENTH,
        origin_context=AssessmentContext.THIRTEENTH,
    )
    deductions = assessment_deductions(
        contract, assessment, social_security=D("649.60")
    )
    rules = select_irrf_rule_bundle(contract, TARGET_DATE, assessment)

    result = assess_irrf_2026(
        assessment=assessment,
        gross_taxable_income=D("6000.00"),
        legal_deductions=deductions,
        simplified_discount=simplified_discount(),
        rules=rules,
    )

    assert rules.reduction_rule_id == "thirteenth.irrf.reduction.2026"
    assert result.reduction_input_income == D("6000.00")
    assert result.irrf_tax_base == D("5350.40")
    assert result.final_irrf == D("382.88")


def test_vacation_assessment_is_separate_from_monthly_payroll() -> None:
    contract = load_candidate()
    vacation = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.VACATION,
        origin_context=AssessmentContext.VACATION_ENJOYED,
    )
    deductions = assessment_deductions(
        contract,
        vacation,
        social_security=D("650.00"),
        dependent_count=1,
        pension=D("0"),
    )
    rules = select_irrf_rule_bundle(contract, TARGET_DATE, vacation)

    result = assess_irrf_2026(
        assessment=vacation,
        gross_taxable_income=D("6000.00"),
        legal_deductions=deductions,
        simplified_discount=simplified_discount(),
        rules=rules,
    )

    assert result.assessment.rule_context == AssessmentContext.VACATION_ENJOYED
    assert result.deduction_components.dependents == D("189.59")
    assert rules.reduction_rule_id == "irrf.reduction.2026"


def marginal_payload() -> ProgressiveTablePayload:
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


def money_rounding() -> RoundingPolicy:
    return RoundingPolicy(
        decimal_places=2,
        mode="ROUND_HALF_UP",
        stage="per_assessment_result",
    )


def test_marginal_progressive_engine_and_cap() -> None:
    payload = marginal_payload()
    policy = money_rounding()

    result = calculate_progressive(D("2500"), payload, policy)
    assert result.amount == D("450.00")
    assert [component.taxable_amount for component in result.components] == [
        D("1000.00"),
        D("1000.00"),
        D("500.00"),
    ]

    capped = calculate_progressive(D("4000"), payload, policy)
    assert capped.assessed_base == D("3000.00")
    assert capped.amount == D("600.00")


def test_float_never_enters_fiscal_decimal_path() -> None:
    with pytest.raises(DecimalInputError):
        as_decimal(0.1)  # type: ignore[arg-type]


def test_negative_fiscal_base_is_rejected() -> None:
    with pytest.raises(FiscalEngineError):
        calculate_progressive(D("-0.01"), marginal_payload(), money_rounding())


@given(st.integers(min_value=0, max_value=500_000), st.integers(min_value=0, max_value=500_000))
def test_marginal_progressive_assessment_is_monotonic(a_cents: int, b_cents: int) -> None:
    low, high = sorted((a_cents, b_cents))
    payload = marginal_payload()
    policy = money_rounding()

    low_tax = calculate_progressive(D(low) / 100, payload, policy).amount
    high_tax = calculate_progressive(D(high) / 100, payload, policy).amount
    assert low_tax <= high_tax


@given(st.integers(min_value=0, max_value=1_000_000))
def test_affine_reduction_never_makes_tax_negative(income_cents: int) -> None:
    contract = load_candidate()
    assessment = monthly_assessment()
    rules = select_irrf_rule_bundle(contract, TARGET_DATE, assessment)
    pre_tax = D("1000.00")
    result = calculate_affine_reduction(
        D(income_cents) / 100,
        pre_tax,
        rules.reduction,
        rules.reduction_rounding,
    )
    assert D("0") <= result.final_tax <= pre_tax
    assert D("0") <= result.applied_reduction <= pre_tax
