from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Literal

from .contract_v1 import FiscalContractV1
from .money import DecimalInput, as_decimal, quantize
from .types_v1 import (
    AffineReductionPayload,
    AssessmentContext,
    ProgressiveCalculationMethod,
    ProgressiveTablePayload,
    RoundingPolicy,
    RoundingStage,
    ScalarPayload,
    ScalarUnit,
)

ZERO = Decimal("0")


class FiscalEngineError(ValueError):
    """Raised when a valid contract payload cannot be executed safely."""


class IrrfIncomeType(str, Enum):
    MONTHLY = "monthly"
    THIRTEENTH = "thirteenth"
    VACATION = "vacation"


_ALLOWED_ORIGIN_CONTEXTS: dict[AssessmentContext, frozenset[IrrfIncomeType]] = {
    AssessmentContext.MONTHLY: frozenset({IrrfIncomeType.MONTHLY}),
    AssessmentContext.THIRTEENTH: frozenset({IrrfIncomeType.THIRTEENTH}),
    AssessmentContext.VACATION_ENJOYED: frozenset({IrrfIncomeType.VACATION}),
    # Termination is an origin/event context, not a fourth IRRF income type.
    # H29 can contain a monthly salary-balance assessment and a separate
    # thirteenth assessment. Indemnified vacation remains outside IRRF here.
    AssessmentContext.TERMINATION: frozenset(
        {IrrfIncomeType.MONTHLY, IrrfIncomeType.THIRTEENTH}
    ),
}

_RULE_CONTEXT_BY_INCOME_TYPE: dict[IrrfIncomeType, AssessmentContext] = {
    IrrfIncomeType.MONTHLY: AssessmentContext.MONTHLY,
    IrrfIncomeType.THIRTEENTH: AssessmentContext.THIRTEENTH,
    IrrfIncomeType.VACATION: AssessmentContext.VACATION_ENJOYED,
}

_REDUCTION_RULE_BY_INCOME_TYPE: dict[IrrfIncomeType, str] = {
    IrrfIncomeType.MONTHLY: "irrf.reduction.2026",
    IrrfIncomeType.THIRTEENTH: "thirteenth.irrf.reduction.2026",
    IrrfIncomeType.VACATION: "vacation.irrf.reduction.2026",
}

_REDUCTION_INPUT_SEMANTIC_BY_INCOME_TYPE: dict[IrrfIncomeType, str] = {
    IrrfIncomeType.MONTHLY: (
        "taxable_income_subject_to_monthly_incidence_before_irrf_deductions"
    ),
    IrrfIncomeType.THIRTEENTH: "thirteenth_taxable_income_before_irrf_deductions",
    IrrfIncomeType.VACATION: (
        "taxable_vacation_income_subject_to_separate_monthly_irrf_assessment_before_deductions"
    ),
}


@dataclass(frozen=True)
class IrrfAssessmentIdentity:
    """Identity of one isolated IRRF assessment.

    ``origin_context`` explains where the income arose (ordinary payroll,
    thirteenth, vacation or termination). ``income_type`` determines the
    legally separate IRRF assessment and therefore the rules/deductions that
    may be used. A termination event never becomes a fourth income type.
    """

    income_type: IrrfIncomeType
    origin_context: AssessmentContext

    def __post_init__(self) -> None:
        allowed = _ALLOWED_ORIGIN_CONTEXTS.get(self.origin_context)
        if allowed is None or self.income_type not in allowed:
            raise FiscalEngineError(
                "IRRF income type is incompatible with origin context: "
                f"{self.income_type.value}/{self.origin_context.value}"
            )

    @property
    def rule_context(self) -> AssessmentContext:
        return _RULE_CONTEXT_BY_INCOME_TYPE[self.income_type]


@dataclass(frozen=True)
class ProgressiveComponent:
    lower_bound: Decimal
    upper_bound: Decimal | None
    taxable_amount: Decimal
    rate: Decimal
    amount: Decimal


@dataclass(frozen=True)
class ProgressiveAssessmentResult:
    input_base: Decimal
    assessed_base: Decimal
    amount: Decimal
    components: tuple[ProgressiveComponent, ...] = ()
    selected_rate: Decimal | None = None
    selected_deduction: Decimal | None = None


@dataclass(frozen=True)
class ReductionResult:
    input_income: Decimal
    statutory_reduction: Decimal
    applied_reduction: Decimal
    final_tax: Decimal


@dataclass(frozen=True)
class IrrfLegalDeductions:
    assessment: IrrfAssessmentIdentity
    social_security: Decimal
    dependent_count: int
    dependent_unit: Decimal
    dependents: Decimal
    pension: Decimal
    total: Decimal


@dataclass(frozen=True)
class IrrfRuleBundle:
    assessment: IrrfAssessmentIdentity
    progressive_rule_id: str
    reduction_rule_id: str
    progressive_table: ProgressiveTablePayload
    progressive_rounding: RoundingPolicy
    reduction: AffineReductionPayload
    reduction_rounding: RoundingPolicy


@dataclass(frozen=True)
class IrrfAssessmentMemory:
    assessment: IrrfAssessmentIdentity
    deduction_components: IrrfLegalDeductions
    gross_taxable_income: Decimal
    legal_deductions: Decimal
    simplified_discount: Decimal
    deduction_mode: Literal["legal", "simplified"]
    irrf_tax_base: Decimal
    pre_reduction_irrf: Decimal
    reduction_input_income: Decimal
    reduction_amount: Decimal
    final_irrf: Decimal


def _non_negative(value: DecimalInput, *, name: str) -> Decimal:
    amount = as_decimal(value, name=name)
    if amount < ZERO:
        raise FiscalEngineError(f"{name} cannot be negative")
    return amount


def build_irrf_legal_deductions(
    *,
    assessment: IrrfAssessmentIdentity,
    social_security: DecimalInput,
    dependent_count: int,
    dependent_deduction: ScalarPayload,
    pension: DecimalInput,
) -> IrrfLegalDeductions:
    """Build legal deductions for exactly one isolated IRRF assessment.

    All components are explicit. In particular, ``pension`` has no default so
    callers cannot silently convert an omitted pension deduction into zero.
    """
    if isinstance(dependent_count, bool) or not isinstance(dependent_count, int):
        raise FiscalEngineError("dependent_count must be an integer")
    if dependent_count < 0:
        raise FiscalEngineError("dependent_count cannot be negative")
    if dependent_deduction.unit != ScalarUnit.BRL_PER_DEPENDENT:
        raise FiscalEngineError("dependent deduction must be BRL_per_dependent")

    social = _non_negative(social_security, name="social_security")
    pension_amount = _non_negative(pension, name="pension")
    dependent_unit = _non_negative(
        dependent_deduction.value, name="dependent_deduction"
    )
    dependents = dependent_unit * dependent_count
    total = social + dependents + pension_amount

    return IrrfLegalDeductions(
        assessment=assessment,
        social_security=social,
        dependent_count=dependent_count,
        dependent_unit=dependent_unit,
        dependents=dependents,
        pension=pension_amount,
        total=total,
    )


def select_irrf_rule_bundle(
    contract: FiscalContractV1,
    target_date: date,
    assessment: IrrfAssessmentIdentity,
) -> IrrfRuleBundle:
    """Select only rules compatible with the assessment income type.

    For a termination-origin assessment, rule selection deliberately uses the
    underlying income-type context (monthly or thirteenth), preventing H29 from
    blending those assessments under the generic ``termination`` context.
    """
    rule_context = assessment.rule_context
    progressive_rule_id = "irrf.monthly.progressive_table"
    reduction_rule_id = _REDUCTION_RULE_BY_INCOME_TYPE[assessment.income_type]

    progressive_rule = contract.select_rule(
        progressive_rule_id, target_date, rule_context
    )
    reduction_rule = contract.select_rule(reduction_rule_id, target_date, rule_context)

    if not isinstance(progressive_rule.payload, ProgressiveTablePayload):
        raise FiscalEngineError("selected IRRF table is not a progressive_table payload")
    if not isinstance(reduction_rule.payload, AffineReductionPayload):
        raise FiscalEngineError("selected IRRF reduction is not an affine_reduction payload")
    if progressive_rule.rounding_policy is None:
        raise FiscalEngineError("selected IRRF table lacks rounding policy")
    if reduction_rule.rounding_policy is None:
        raise FiscalEngineError("selected IRRF reduction lacks rounding policy")

    expected_semantic = _REDUCTION_INPUT_SEMANTIC_BY_INCOME_TYPE[
        assessment.income_type
    ]
    if reduction_rule.payload.input_semantic != expected_semantic:
        raise FiscalEngineError(
            "reduction input semantic does not match IRRF income type: "
            f"expected {expected_semantic}, got {reduction_rule.payload.input_semantic}"
        )

    return IrrfRuleBundle(
        assessment=assessment,
        progressive_rule_id=progressive_rule_id,
        reduction_rule_id=reduction_rule_id,
        progressive_table=progressive_rule.payload,
        progressive_rounding=progressive_rule.rounding_policy,
        reduction=reduction_rule.payload,
        reduction_rounding=reduction_rule.rounding_policy,
    )


def calculate_progressive(
    base: DecimalInput,
    payload: ProgressiveTablePayload,
    rounding_policy: RoundingPolicy,
) -> ProgressiveAssessmentResult:
    """Execute a typed progressive table without inferring its calculation method."""
    input_base = _non_negative(base, name="base")
    assessed_base = input_base
    if payload.cap_base is not None:
        assessed_base = min(assessed_base, payload.cap_base)

    if payload.calculation_method == ProgressiveCalculationMethod.RATE_TIMES_BASE_MINUS_DEDUCTION:
        selected = next(
            (
                bracket
                for bracket in payload.brackets
                if bracket.upper_bound is None or assessed_base <= bracket.upper_bound
            ),
            None,
        )
        if selected is None:
            raise FiscalEngineError("progressive table has no bracket for assessed base")
        raw = max(ZERO, assessed_base * selected.rate - selected.deduction)
        return ProgressiveAssessmentResult(
            input_base=input_base,
            assessed_base=assessed_base,
            amount=quantize(raw, rounding_policy),
            selected_rate=selected.rate,
            selected_deduction=selected.deduction,
        )

    if payload.calculation_method != ProgressiveCalculationMethod.MARGINAL_BY_BRACKET:
        raise FiscalEngineError(
            f"unsupported progressive calculation method: {payload.calculation_method}"
        )

    lower = ZERO
    components: list[ProgressiveComponent] = []
    total = ZERO

    for bracket in payload.brackets:
        upper = bracket.upper_bound
        segment_upper = assessed_base if upper is None else min(assessed_base, upper)
        taxable = max(ZERO, segment_upper - lower)

        if taxable > ZERO:
            raw_component = taxable * bracket.rate
            component_amount = (
                quantize(raw_component, rounding_policy)
                if rounding_policy.stage == RoundingStage.PER_COMPONENT
                else raw_component
            )
            components.append(
                ProgressiveComponent(
                    lower_bound=lower,
                    upper_bound=upper,
                    taxable_amount=taxable,
                    rate=bracket.rate,
                    amount=component_amount,
                )
            )
            total += component_amount

        if upper is None or assessed_base <= upper:
            break
        lower = upper

    return ProgressiveAssessmentResult(
        input_base=input_base,
        assessed_base=assessed_base,
        amount=quantize(total, rounding_policy),
        components=tuple(components),
    )


def calculate_affine_reduction(
    input_income: DecimalInput,
    pre_reduction_tax: DecimalInput,
    payload: AffineReductionPayload,
    rounding_policy: RoundingPolicy,
) -> ReductionResult:
    """Apply the typed 2026-style affine reduction to a pre-reduction tax result."""
    income = _non_negative(input_income, name="input_income")
    pre_tax = _non_negative(pre_reduction_tax, name="pre_reduction_tax")

    if income <= payload.full_relief_income_limit:
        statutory = payload.max_reduction
    elif income <= payload.phaseout_income_limit:
        statutory = payload.intercept - payload.slope * income
        if payload.floor_at_zero:
            statutory = max(ZERO, statutory)
    else:
        statutory = ZERO

    statutory = quantize(statutory, rounding_policy)
    applied = quantize(min(pre_tax, statutory), rounding_policy)
    final_tax = quantize(max(ZERO, pre_tax - applied), rounding_policy)

    return ReductionResult(
        input_income=income,
        statutory_reduction=statutory,
        applied_reduction=applied,
        final_tax=final_tax,
    )


def assess_irrf_2026(
    *,
    assessment: IrrfAssessmentIdentity,
    gross_taxable_income: DecimalInput,
    legal_deductions: IrrfLegalDeductions,
    simplified_discount: ScalarPayload,
    rules: IrrfRuleBundle,
) -> IrrfAssessmentMemory:
    """Build one auditable, context-isolated IRRF assessment memory."""
    if legal_deductions.assessment != assessment:
        raise FiscalEngineError(
            "legal deductions belong to a different IRRF assessment context"
        )
    if rules.assessment != assessment:
        raise FiscalEngineError("IRRF rule bundle belongs to a different assessment")

    gross = _non_negative(gross_taxable_income, name="gross_taxable_income")

    if simplified_discount.unit != ScalarUnit.BRL:
        raise FiscalEngineError("simplified discount must be a BRL scalar")
    simplified = _non_negative(simplified_discount.value, name="simplified_discount")
    legal_total = legal_deductions.total

    if simplified > legal_total:
        deduction_mode: Literal["legal", "simplified"] = "simplified"
        selected_deduction = simplified
    else:
        deduction_mode = "legal"
        selected_deduction = legal_total

    tax_base = max(ZERO, gross - selected_deduction)
    pre = calculate_progressive(
        tax_base, rules.progressive_table, rules.progressive_rounding
    )

    # A01 invariant: the reduction input is the taxable income before IRRF deductions,
    # never the post-deduction tax base. Each assessment invokes this independently.
    reduction_result = calculate_affine_reduction(
        gross,
        pre.amount,
        rules.reduction,
        rules.reduction_rounding,
    )

    return IrrfAssessmentMemory(
        assessment=assessment,
        deduction_components=legal_deductions,
        gross_taxable_income=gross,
        legal_deductions=legal_total,
        simplified_discount=simplified,
        deduction_mode=deduction_mode,
        irrf_tax_base=tax_base,
        pre_reduction_irrf=pre.amount,
        reduction_input_income=gross,
        reduction_amount=reduction_result.applied_reduction,
        final_irrf=reduction_result.final_tax,
    )