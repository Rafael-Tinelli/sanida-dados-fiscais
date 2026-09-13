from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.contract_v1 import FiscalContractV1
from sanida_fiscal.engine_v1 import (
    IrrfAssessmentIdentity,
    IrrfIncomeType,
    assess_irrf_2026,
    build_irrf_legal_deductions,
    select_irrf_rule_bundle,
)
from sanida_fiscal.memory_v1 import (
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

EXAMPLE = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"

REQUIRED_PHASE3_FILES = {
    "sanida_fiscal/money.py",
    "sanida_fiscal/engine_v1.py",
    "sanida_fiscal/thirteenth_v1.py",
    "sanida_fiscal/vacation_v1.py",
    "sanida_fiscal/termination_v1.py",
    "sanida_fiscal/memory_v1.py",
    "tests/test_engine_v1.py",
    "tests/test_thirteenth_v1.py",
    "tests/test_vacation_v1.py",
    "tests/test_termination_v1.py",
    "tests/test_memory_v1.py",
    "tests/test_phase3_invariants.py",
    "docs/phase3-library-v1.md",
    "docs/phase3-closure-gate.md",
}

ALLOWED_WORKFLOWS = {"main.yml", "taxas.yml", "remake-ci.yml"}


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


def validate_required_files() -> None:
    missing = sorted(path for path in REQUIRED_PHASE3_FILES if not (ROOT / path).is_file())
    if missing:
        raise SystemExit(f"Phase 3 gate: missing required files: {missing}")


def validate_workflow_hygiene() -> None:
    workflow_dir = ROOT / ".github" / "workflows"
    actual = {path.name for path in workflow_dir.glob("*.yml")}
    unexpected = sorted(actual - ALLOWED_WORKFLOWS)
    if unexpected:
        raise SystemExit(f"Phase 3 gate: unexpected temporary workflows remain: {unexpected}")


def validate_a01_and_common_irrf_memory(contract: FiscalContractV1) -> None:
    assessment = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.MONTHLY,
        origin_context=AssessmentContext.MONTHLY,
    )
    dependent_rule = contract.select_rule(
        "irrf.dependent_deduction", date(2026, 9, 13), AssessmentContext.MONTHLY
    )
    if not isinstance(dependent_rule.payload, ScalarPayload):
        raise SystemExit("Phase 3 gate: dependent deduction payload is not scalar")

    rules = select_irrf_rule_bundle(contract, date(2026, 9, 13), assessment)
    deductions = build_irrf_legal_deductions(
        assessment=assessment,
        social_security="649.60",
        dependent_count=0,
        dependent_deduction=dependent_rule.payload,
        pension="0.00",
    )
    result = assess_irrf_2026(
        assessment=assessment,
        gross_taxable_income="6000.00",
        legal_deductions=deductions,
        simplified_discount=ScalarPayload(value=D("607.20"), unit=ScalarUnit.BRL),
        rules=rules,
    )
    memory = memory_from_irrf(result, rules)

    if result.final_irrf != D("382.88"):
        raise SystemExit("Phase 3 gate: A01 final IRRF regression failed")
    if result.reduction_input_income != D("6000.00") or result.irrf_tax_base != D("5350.40"):
        raise SystemExit("Phase 3 gate: A01 base semantics regressed")
    if memory.fact("final_irrf").value != "382.88":
        raise SystemExit("Phase 3 gate: common IRRF memory does not reconcile final result")


def validate_h29_common_memory(contract: FiscalContractV1) -> None:
    supported = calculate_h29_limited_estimate(
        contract=contract,
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
    supported_memory = memory_from_h29(supported)
    children = {child.calculation_type: child for child in supported_memory.children}

    if supported.result_promise != "partial_estimate" or not supported.user_disclosure_required:
        raise SystemExit("Phase 3 gate: H29 no longer declares a disclosed partial estimate")
    if children.get("salary_balance") is None:
        raise SystemExit("Phase 3 gate: H29 memory lacks salary balance")
    if children["salary_balance"].fact("salary_balance_amount").value != "1000.00":
        raise SystemExit("Phase 3 gate: H29 salary balance regression failed")
    if children.get("thirteenth_proportional") is None:
        raise SystemExit("Phase 3 gate: H29 memory lacks proportional 13th")
    if children["thirteenth_proportional"].fact("twelfths").value != "3":
        raise SystemExit("Phase 3 gate: H29 13th calendar regression failed")
    if children.get("vacation_proportional_accrual") is None:
        raise SystemExit("Phase 3 gate: H29 memory lacks proportional vacation")
    if children["vacation_proportional_accrual"].fact("twelfths").value != "7":
        raise SystemExit("Phase 3 gate: H29 vacation acquisition-period regression failed")

    just_cause = calculate_h29_limited_estimate(
        contract=contract,
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
    if [child.calculation_type for child in memory_from_h29(just_cause).children] != [
        "salary_balance"
    ]:
        raise SystemExit("Phase 3 gate: reason 01 fabricated proportional H29 items")


def validate_vacation_component_separation(contract: FiscalContractV1) -> None:
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
    if memory.fact("irrf_taxable_amount").value != "333.33":
        raise SystemExit("Phase 3 gate: vacation cash-allowance IRRF basis regressed")
    if memory.fact("social_security_base").value not in {"0", "0.00"}:
        raise SystemExit("Phase 3 gate: vacation cash-allowance CP basis regressed")


def main() -> None:
    validate_required_files()
    validate_workflow_hygiene()
    contract = load_contract()
    validate_a01_and_common_irrf_memory(contract)
    validate_h29_common_memory(contract)
    validate_vacation_component_separation(contract)
    print("Phase 3 closure gate: PASS")


if __name__ == "__main__":
    main()
