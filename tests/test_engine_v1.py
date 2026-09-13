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
    assess_irrf_2026,
    calculate_affine_reduction,
    calculate_progressive,
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


def D(value: str | int) -> Decimal:
    return Decimal(str(value))


def load_candidate() -> FiscalContractV1:
    return FiscalContractV1.model_validate(
        json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))
    )


def reference_cases() -> dict[str, dict]:
    payload = json.loads(REFERENCE_CASES_PATH.read_text(encoding="utf-8"))
    return {case["case_id"]: case for case in payload["cases"]}


def irrf_rules():
    contract = load_candidate()
    target = date(2026, 9, 13)
    table_rule = contract.select_rule(
        "irrf.monthly.progressive_table", target, AssessmentContext.MONTHLY
    )
    reduction_rule = contract.select_rule(
        "irrf.reduction.2026", target, AssessmentContext.MONTHLY
    )
    assert table_rule.rounding_policy is not None
    assert reduction_rule.rounding_policy is not None
    return table_rule, reduction_rule


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
    table_rule, reduction_rule = irrf_rules()

    legal = []
    if "official_social_security_deduction" in case["inputs"]:
        legal.append(D(case["inputs"]["official_social_security_deduction"]))

    result = assess_irrf_2026(
        gross_taxable_income=D(case["inputs"]["taxable_income"]),
        legal_deductions=legal,
        simplified_discount=ScalarPayload(
            value=D("607.20"), unit=ScalarUnit.BRL
        ),
        progressive_table=table_rule.payload,
        progressive_rounding=table_rule.rounding_policy,
        reduction=reduction_rule.payload,
        reduction_rounding=reduction_rule.rounding_policy,
    )

    expected = case["expected"]
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
    table_rule, reduction_rule = irrf_rules()

    result = assess_irrf_2026(
        gross_taxable_income=D("6000.00"),
        legal_deductions=[D("649.60")],
        simplified_discount=ScalarPayload(value=D("607.20"), unit=ScalarUnit.BRL),
        progressive_table=table_rule.payload,
        progressive_rounding=table_rule.rounding_policy,
        reduction=reduction_rule.payload,
        reduction_rounding=reduction_rule.rounding_policy,
    )

    assert case["invariant"] == "reduction_input_income != irrf_tax_base"
    assert result.reduction_input_income == D("6000.00")
    assert result.irrf_tax_base == D("5350.40")
    assert result.reduction_input_income != result.irrf_tax_base
    assert result.final_irrf == D("382.88")


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
    _, reduction_rule = irrf_rules()
    pre_tax = D("1000.00")
    result = calculate_affine_reduction(
        D(income_cents) / 100,
        pre_tax,
        reduction_rule.payload,
        reduction_rule.rounding_policy,
    )
    assert D("0") <= result.final_tax <= pre_tax
    assert D("0") <= result.applied_reduction <= pre_tax
