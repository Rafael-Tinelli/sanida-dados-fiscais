from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from .engine_v1 import (
    FiscalEngineError,
    IrrfAssessmentMemory,
    IrrfRuleBundle,
)
from .termination_v1 import (
    H29LimitedEstimate,
    H29ThirteenthProportionalMemory,
    SalaryBalanceMemory,
)
from .thirteenth_v1 import ThirteenthFiscalAssessmentMemory
from .types_v1 import AssessmentContext
from .vacation_v1 import (
    VacationCashAllowanceTaxBases,
    VacationProportionalAccrual,
)


class MemoryRole(str, Enum):
    INPUT = "input"
    INTERMEDIATE = "intermediate"
    OUTPUT = "output"
    METADATA = "metadata"


class MemoryUnit(str, Enum):
    BRL = "BRL"
    COUNT = "count"
    DATE = "date"
    CODE = "code"
    TEXT = "text"
    BOOLEAN = "boolean"
    RATIO = "ratio"


@dataclass(frozen=True)
class CalculationFact:
    """One deterministic, serialization-ready fact in a calculation memory.

    Values are stored as canonical text on purpose. Monetary values enter through
    ``Decimal`` instances and are never converted to binary float. This keeps the
    audit representation stable across Python/JSON consumers while preserving the
    typed engine memories as the source objects.
    """

    key: str
    value: str
    unit: MemoryUnit
    role: MemoryRole
    rule_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.key or self.key.strip() != self.key:
            raise FiscalEngineError("calculation-memory fact key must be non-empty and trimmed")
        if self.value == "":
            raise FiscalEngineError("calculation-memory fact value cannot be empty")
        if len(set(self.rule_ids)) != len(self.rule_ids):
            raise FiscalEngineError("calculation-memory fact rule_ids must be unique")
        if any(not item or item.strip() != item for item in self.rule_ids):
            raise FiscalEngineError("calculation-memory rule_ids must be non-empty and trimmed")


@dataclass(frozen=True)
class CalculationMemory:
    """Common envelope for auditable memories produced by distinct fiscal nuclei.

    The envelope normalizes *representation*, not legal semantics. Monthly IRRF,
    13th salary, vacation and termination remain separate calculations and can be
    nested as children without sharing tax bases or deduction state.
    """

    calculation_type: str
    origin_context: AssessmentContext
    assessment_income_type: str | None
    facts: tuple[CalculationFact, ...]
    children: tuple["CalculationMemory", ...] = ()

    def __post_init__(self) -> None:
        if not self.calculation_type or self.calculation_type.strip() != self.calculation_type:
            raise FiscalEngineError("calculation_type must be non-empty and trimmed")
        keys = [fact.key for fact in self.facts]
        if len(keys) != len(set(keys)):
            raise FiscalEngineError("calculation-memory fact keys must be unique within a node")

    def fact(self, key: str) -> CalculationFact:
        matches = [fact for fact in self.facts if fact.key == key]
        if len(matches) != 1:
            raise KeyError(key)
        return matches[0]

    def to_dict(self) -> dict:
        return {
            "calculation_type": self.calculation_type,
            "origin_context": self.origin_context.value,
            "assessment_income_type": self.assessment_income_type,
            "facts": [
                {
                    "key": fact.key,
                    "value": fact.value,
                    "unit": fact.unit.value,
                    "role": fact.role.value,
                    "rule_ids": list(fact.rule_ids),
                }
                for fact in self.facts
            ],
            "children": [child.to_dict() for child in self.children],
        }


def _decimal_text(value: Decimal) -> str:
    if not isinstance(value, Decimal):
        raise FiscalEngineError("calculation-memory monetary value must be Decimal")
    if not value.is_finite():
        raise FiscalEngineError("calculation-memory monetary value must be finite")
    return format(value, "f")


def _money(
    key: str,
    value: Decimal,
    role: MemoryRole,
    *rule_ids: str,
) -> CalculationFact:
    return CalculationFact(
        key=key,
        value=_decimal_text(value),
        unit=MemoryUnit.BRL,
        role=role,
        rule_ids=tuple(rule_ids),
    )


def _count(key: str, value: int, role: MemoryRole, *rule_ids: str) -> CalculationFact:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FiscalEngineError("calculation-memory count must be an integer")
    return CalculationFact(
        key=key,
        value=str(value),
        unit=MemoryUnit.COUNT,
        role=role,
        rule_ids=tuple(rule_ids),
    )


def _text(
    key: str,
    value: str,
    role: MemoryRole,
    unit: MemoryUnit = MemoryUnit.TEXT,
    *rule_ids: str,
) -> CalculationFact:
    if not isinstance(value, str) or value == "":
        raise FiscalEngineError("calculation-memory text value must be non-empty")
    return CalculationFact(
        key=key,
        value=value,
        unit=unit,
        role=role,
        rule_ids=tuple(rule_ids),
    )


def _boolean(key: str, value: bool, role: MemoryRole) -> CalculationFact:
    if not isinstance(value, bool):
        raise FiscalEngineError("calculation-memory boolean must be bool")
    return CalculationFact(
        key=key,
        value="true" if value else "false",
        unit=MemoryUnit.BOOLEAN,
        role=role,
    )


def memory_from_irrf(
    memory: IrrfAssessmentMemory,
    rules: IrrfRuleBundle,
) -> CalculationMemory:
    if memory.assessment != rules.assessment:
        raise FiscalEngineError("IRRF memory and rule bundle belong to different assessments")

    deductions = memory.deduction_components
    return CalculationMemory(
        calculation_type="irrf_assessment",
        origin_context=memory.assessment.origin_context,
        assessment_income_type=memory.assessment.income_type.value,
        facts=(
            _money("gross_taxable_income", memory.gross_taxable_income, MemoryRole.INPUT),
            _money("social_security_deduction", deductions.social_security, MemoryRole.INPUT),
            _count("dependent_count", deductions.dependent_count, MemoryRole.INPUT),
            _money(
                "dependent_unit_deduction",
                deductions.dependent_unit,
                MemoryRole.METADATA,
                "irrf.dependent_deduction",
            ),
            _money("dependents_deduction", deductions.dependents, MemoryRole.INTERMEDIATE),
            _money("pension_deduction", deductions.pension, MemoryRole.INPUT),
            _money("legal_deductions_total", memory.legal_deductions, MemoryRole.INTERMEDIATE),
            _money("simplified_discount", memory.simplified_discount, MemoryRole.INPUT),
            _text("deduction_mode", memory.deduction_mode, MemoryRole.INTERMEDIATE),
            _money(
                "irrf_tax_base",
                memory.irrf_tax_base,
                MemoryRole.INTERMEDIATE,
                rules.progressive_rule_id,
            ),
            _money(
                "pre_reduction_irrf",
                memory.pre_reduction_irrf,
                MemoryRole.INTERMEDIATE,
                rules.progressive_rule_id,
            ),
            _money(
                "reduction_input_income",
                memory.reduction_input_income,
                MemoryRole.INTERMEDIATE,
                rules.reduction_rule_id,
            ),
            _money(
                "reduction_amount",
                memory.reduction_amount,
                MemoryRole.INTERMEDIATE,
                rules.reduction_rule_id,
            ),
            _money(
                "final_irrf",
                memory.final_irrf,
                MemoryRole.OUTPUT,
                rules.progressive_rule_id,
                rules.reduction_rule_id,
            ),
        ),
    )


def memory_from_thirteenth_fiscal(
    memory: ThirteenthFiscalAssessmentMemory,
    irrf_rules: IrrfRuleBundle,
) -> CalculationMemory:
    if memory.assessment != irrf_rules.assessment or memory.irrf.assessment != memory.assessment:
        raise FiscalEngineError("thirteenth fiscal memories do not share one assessment identity")

    irrf_child = memory_from_irrf(memory.irrf, irrf_rules)
    return CalculationMemory(
        calculation_type="thirteenth_fiscal_assessment",
        origin_context=memory.assessment.origin_context,
        assessment_income_type=memory.assessment.income_type.value,
        facts=(
            _money("gross_thirteenth", memory.gross_thirteenth, MemoryRole.INPUT),
            _money(
                "social_security_contribution_base",
                memory.social_security.contribution_base,
                MemoryRole.INTERMEDIATE,
                "thirteenth.inss.separate_assessment",
            ),
            _money(
                "social_security_amount",
                memory.social_security.amount,
                MemoryRole.OUTPUT,
                "thirteenth.inss.separate_assessment",
            ),
            _money("final_irrf", memory.irrf.final_irrf, MemoryRole.OUTPUT, irrf_rules.reduction_rule_id),
        ),
        children=(irrf_child,),
    )


def memory_from_salary_balance(memory: SalaryBalanceMemory) -> CalculationMemory:
    return CalculationMemory(
        calculation_type="salary_balance",
        origin_context=AssessmentContext.TERMINATION,
        assessment_income_type=None,
        facts=(
            _money(
                "monthly_base_salary",
                memory.monthly_base_salary,
                MemoryRole.INPUT,
                "termination.salary_balance",
            ),
            _text(
                "termination_date",
                memory.termination_date.isoformat(),
                MemoryRole.INPUT,
                MemoryUnit.DATE,
                "termination.salary_balance",
            ),
            _count(
                "days_counted_through_termination",
                memory.days_counted_through_termination,
                MemoryRole.INPUT,
                "termination.salary_balance",
            ),
            _count(
                "calendar_days_in_month",
                memory.calendar_days_in_month,
                MemoryRole.INTERMEDIATE,
                "termination.salary_balance",
            ),
            _money(
                "salary_balance_amount",
                memory.amount,
                MemoryRole.OUTPUT,
                "termination.salary_balance",
            ),
        ),
    )


def memory_from_h29_thirteenth(
    memory: H29ThirteenthProportionalMemory,
) -> CalculationMemory:
    return CalculationMemory(
        calculation_type="thirteenth_proportional",
        origin_context=AssessmentContext.TERMINATION,
        assessment_income_type="thirteenth",
        facts=(
            _count(
                "twelfths",
                memory.accrual.twelfths,
                MemoryRole.INTERMEDIATE,
                "thirteenth.accrual.twelfths",
            ),
            _money(
                "reference_remuneration",
                memory.reference.total_reference,
                MemoryRole.INPUT,
                "thirteenth.reference_remuneration",
            ),
            _text(
                "fraction",
                f"{memory.gross.fraction_numerator}/{memory.gross.fraction_denominator}",
                MemoryRole.METADATA,
                MemoryUnit.RATIO,
                "thirteenth.accrual.twelfths",
            ),
            _money(
                "gross_thirteenth",
                memory.gross.gross_thirteenth,
                MemoryRole.OUTPUT,
                "termination.thirteenth_proportional",
                "thirteenth.accrual.twelfths",
                "thirteenth.reference_remuneration",
            ),
        ),
    )


def memory_from_vacation_proportional(
    memory: VacationProportionalAccrual,
) -> CalculationMemory:
    return CalculationMemory(
        calculation_type="vacation_proportional_accrual",
        origin_context=AssessmentContext.TERMINATION,
        assessment_income_type=None,
        facts=(
            _text(
                "employment_start",
                memory.period.employment_start.isoformat(),
                MemoryRole.INPUT,
                MemoryUnit.DATE,
                "vacation.acquisition_period",
            ),
            _text(
                "acquisition_period_start",
                memory.period.start.isoformat(),
                MemoryRole.INTERMEDIATE,
                MemoryUnit.DATE,
                "vacation.acquisition_period",
            ),
            _text(
                "acquisition_period_end",
                memory.period.end_inclusive.isoformat(),
                MemoryRole.INTERMEDIATE,
                MemoryUnit.DATE,
                "vacation.acquisition_period",
            ),
            _text(
                "through_date",
                memory.through_date.isoformat(),
                MemoryRole.INPUT,
                MemoryUnit.DATE,
                "vacation.acquisition_period",
            ),
            _count(
                "twelfths",
                memory.twelfths,
                MemoryRole.OUTPUT,
                "vacation.acquisition_period",
                "termination.vacation_proportional",
            ),
        ),
    )


def memory_from_vacation_cash_allowance_bases(
    memory: VacationCashAllowanceTaxBases,
) -> CalculationMemory:
    return CalculationMemory(
        calculation_type="vacation_cash_allowance_tax_bases",
        origin_context=AssessmentContext.VACATION_CASH_ALLOWANCE,
        assessment_income_type=None,
        facts=(
            _money(
                "cash_allowance_principal",
                memory.principal_amount,
                MemoryRole.INPUT,
                "vacation.abono.ir_exemption",
            ),
            _money(
                "constitutional_third_on_cash_allowance",
                memory.constitutional_third_amount,
                MemoryRole.INPUT,
                "vacation.abono_constitutional_third.ir_incidence",
            ),
            _money(
                "irrf_taxable_amount",
                memory.irrf_taxable_amount,
                MemoryRole.OUTPUT,
                "vacation.abono.ir_exemption",
                "vacation.abono_constitutional_third.ir_incidence",
            ),
            _money(
                "social_security_base",
                memory.social_security_base,
                MemoryRole.OUTPUT,
                "vacation.abono.ir_exemption",
                "vacation.abono_constitutional_third.ir_incidence",
            ),
        ),
    )


def memory_from_h29(estimate: H29LimitedEstimate) -> CalculationMemory:
    children: list[CalculationMemory] = []
    if estimate.salary_balance is not None:
        children.append(memory_from_salary_balance(estimate.salary_balance))
    if estimate.thirteenth_proportional is not None:
        children.append(memory_from_h29_thirteenth(estimate.thirteenth_proportional))
    if estimate.vacation_proportional is not None:
        children.append(memory_from_vacation_proportional(estimate.vacation_proportional))

    return CalculationMemory(
        calculation_type="h29_limited_estimate",
        origin_context=AssessmentContext.TERMINATION,
        assessment_income_type=None,
        facts=(
            _text("result_promise", estimate.result_promise, MemoryRole.METADATA),
            _boolean("user_disclosure_required", estimate.user_disclosure_required, MemoryRole.METADATA),
            _text("esocial_reason", estimate.reason.code, MemoryRole.INPUT, MemoryUnit.CODE, "termination.reason_scope"),
            _text("reason_label", estimate.reason.label, MemoryRole.METADATA, MemoryUnit.TEXT, "termination.reason_scope"),
            _boolean("acquired_vacation_if_due", estimate.acquired_vacation_if_due, MemoryRole.METADATA),
            _text(
                "included_items",
                ",".join(item.value for item in estimate.included_items),
                MemoryRole.METADATA,
                MemoryUnit.TEXT,
                "termination.partial_output_scope",
            ),
            _text(
                "excluded_items",
                ",".join(item.value for item in estimate.excluded_items),
                MemoryRole.METADATA,
                MemoryUnit.TEXT,
                "termination.partial_output_scope",
            ),
        ),
        children=tuple(children),
    )
