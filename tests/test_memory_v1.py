from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from sanida_fiscal.contract_v1 import FiscalContractV1
from sanida_fiscal.engine_v1 import (
    FiscalEngineError,
    IrrfAssessmentIdentity,
    IrrfIncomeType,
    assess_irrf_2026,
    build_irrf_legal_deductions,
    select_irrf_rule_bundle,
)
from sanida_fiscal.memory_v1 import (
    CalculationFact,
    CalculationMemory,
    MemoryRole,
    MemoryUnit,
    memory_from_h29,
    memory_from_irrf,
    memory_from_vacation_cash_allowance_bases,
)
from sanida_fiscal.termination_v1 import calculate_h29_limited_estimate
from sanida_fiscal.types_v1 import (
    AssessmentContext,
    RoundingPolicy,
    RoundingStage,
    ScalarPayload,
    ScalarUnit,
)
from sanida_fiscal.vacation_v1 import (
    cash_allowance_tax_bases,
    resolve_cash_allowance_tax_treatment,
    select_vacation_rule_bundle,
)

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"


def D(value: str | int) -> Decimal:
    return Decimal(str(value))


def load_contract() -> FiscalContractV1:
    return FiscalContractV1.model_validate(json.loads(EXAMPLE.read_text(encoding="utf-8")))


def money_rounding() -> RoundingPolicy:
    return RoundingPolicy(
        decimal_places=2,
        mode="ROUND_HALF_UP",
        stage=RoundingStage.PER_ASSESSMENT_RESULT,
    )


def dependent_payload(contract: FiscalContractV1, context: AssessmentContext) -> ScalarPayload:
    rule = contract.select_rule("irrf.dependent_deduction", date(2026, 9, 13), context)
    assert isinstance(rule.payload, ScalarPayload)
    return rule.payload


def simplified_discount() -> ScalarPayload:
    return ScalarPayload(value=D("607.20"), unit=ScalarUnit.BRL)


def test_irrf_common_memory_reconciles_a01_without_losing_semantics() -> None:
    contract = load_contract()
    assessment = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.MONTHLY,
        origin_context=AssessmentContext.MONTHLY,
    )
    rules = select_irrf_rule_bundle(contract, date(2026, 9, 13), assessment)
    deductions = build_irrf_legal_deductions(
        assessment=assessment,
        social_security="649.60",
        dependent_count=0,
        dependent_deduction=dependent_payload(contract, AssessmentContext.MONTHLY),
        pension="0.00",
    )
    result = assess_irrf_2026(
        assessment=assessment,
        gross_taxable_income="6000.00",
        legal_deductions=deductions,
        simplified_discount=simplified_discount(),
        rules=rules,
    )

    memory = memory_from_irrf(result, rules)

    assert memory.calculation_type == "irrf_assessment"
    assert memory.origin_context == AssessmentContext.MONTHLY
    assert memory.assessment_income_type == "monthly"
    assert memory.fact("gross_taxable_income").value == "6000.00"
    assert memory.fact("irrf_tax_base").value == "5350.40"
    assert memory.fact("reduction_input_income").value == "6000.00"
    assert memory.fact("final_irrf").value == "382.88"
    assert memory.fact("reduction_input_income").value != memory.fact("irrf_tax_base").value
    assert rules.reduction_rule_id in memory.fact("final_irrf").rule_ids


def test_common_memory_serialization_is_deterministic_and_json_safe() -> None:
    fact = CalculationFact(
        key="amount",
        value="10.00",
        unit=MemoryUnit.BRL,
        role=MemoryRole.OUTPUT,
        rule_ids=("rule.one",),
    )
    memory = CalculationMemory(
        calculation_type="example",
        origin_context=AssessmentContext.MONTHLY,
        assessment_income_type="monthly",
        facts=(fact,),
    )

    first = json.dumps(memory.to_dict(), ensure_ascii=False, separators=(",", ":"))
    second = json.dumps(memory.to_dict(), ensure_ascii=False, separators=(",", ":"))
    assert first == second
    assert '"value":"10.00"' in first
    assert "10.0" not in first


def test_common_memory_rejects_duplicate_fact_keys() -> None:
    facts = (
        CalculationFact("same", "1", MemoryUnit.COUNT, MemoryRole.INPUT),
        CalculationFact("same", "2", MemoryUnit.COUNT, MemoryRole.OUTPUT),
    )
    with pytest.raises(FiscalEngineError, match="fact keys must be unique"):
        CalculationMemory(
            calculation_type="invalid",
            origin_context=AssessmentContext.MONTHLY,
            assessment_income_type=None,
            facts=facts,
        )


def test_h29_memory_preserves_partial_scope_and_separate_children() -> None:
    estimate = calculate_h29_limited_estimate(
        contract=load_contract(),
        termination_date=date(2026, 3, 31),
        esocial_reason="02",
        employment_regime="monthly",
        contract_term="indefinite",
        employment_start=date(2025, 9, 1),
        monthly_base_salary="3100.00",
        days_counted_through_termination=10,
        termination_month_remuneration="3600.00",
        thirteenth_rounding=money_rounding(),
    )

    memory = memory_from_h29(estimate)
    children = {child.calculation_type: child for child in memory.children}

    assert memory.fact("result_promise").value == "partial_estimate"
    assert memory.fact("user_disclosure_required").value == "true"
    assert memory.fact("esocial_reason").value == "02"
    assert set(children) == {
        "salary_balance",
        "thirteenth_proportional",
        "vacation_proportional_accrual",
    }
    assert children["salary_balance"].fact("salary_balance_amount").value == "1000.00"
    assert children["thirteenth_proportional"].fact("twelfths").value == "3"
    assert children["thirteenth_proportional"].fact("gross_thirteenth").value == "900.00"
    assert children["vacation_proportional_accrual"].fact("twelfths").value == "7"


def test_h29_reason_01_memory_does_not_fabricate_proportional_children() -> None:
    estimate = calculate_h29_limited_estimate(
        contract=load_contract(),
        termination_date=date(2026, 3, 31),
        esocial_reason="01",
        employment_regime="monthly",
        contract_term="indefinite",
        employment_start=date(2025, 9, 1),
        monthly_base_salary="3100.00",
        days_counted_through_termination=10,
        termination_month_remuneration="3600.00",
        thirteenth_rounding=money_rounding(),
    )
    memory = memory_from_h29(estimate)
    assert [child.calculation_type for child in memory.children] == ["salary_balance"]


def test_cash_allowance_common_memory_keeps_principal_and_third_separate() -> None:
    contract = load_contract()
    bundle = select_vacation_rule_bundle(contract, date(2026, 9, 13))
    treatment = resolve_cash_allowance_tax_treatment(
        principal_payload=bundle.principal_incidence,
        constitutional_third_payload=bundle.constitutional_third_incidence,
    )
    bases = cash_allowance_tax_bases(
        principal_amount="1000.00",
        constitutional_third_amount="333.33",
        treatment=treatment,
    )
    memory = memory_from_vacation_cash_allowance_bases(bases)

    assert memory.fact("cash_allowance_principal").value == "1000.00"
    assert memory.fact("constitutional_third_on_cash_allowance").value == "333.33"
    assert memory.fact("irrf_taxable_amount").value == "333.33"
    assert memory.fact("social_security_base").value == "0"
