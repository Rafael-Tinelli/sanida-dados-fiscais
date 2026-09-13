from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Iterable, Literal

from .money import DecimalInput, as_decimal, quantize
from .types_v1 import (
    AffineReductionPayload,
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
class IrrfAssessmentMemory:
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
    gross_taxable_income: DecimalInput,
    legal_deductions: Iterable[DecimalInput],
    simplified_discount: ScalarPayload,
    progressive_table: ProgressiveTablePayload,
    progressive_rounding: RoundingPolicy,
    reduction: AffineReductionPayload,
    reduction_rounding: RoundingPolicy,
) -> IrrfAssessmentMemory:
    """Build the auditable IRRF memory required by the Phase 2 handoff."""
    gross = _non_negative(gross_taxable_income, name="gross_taxable_income")

    if simplified_discount.unit != ScalarUnit.BRL:
        raise FiscalEngineError("simplified discount must be a BRL scalar")
    simplified = _non_negative(simplified_discount.value, name="simplified_discount")

    legal_values = [
        _non_negative(value, name="legal_deduction") for value in legal_deductions
    ]
    legal_total = sum(legal_values, ZERO)

    if simplified > legal_total:
        deduction_mode: Literal["legal", "simplified"] = "simplified"
        selected_deduction = simplified
    else:
        deduction_mode = "legal"
        selected_deduction = legal_total

    tax_base = max(ZERO, gross - selected_deduction)
    pre = calculate_progressive(tax_base, progressive_table, progressive_rounding)

    # A01 invariant: the reduction input is the taxable income before IRRF deductions,
    # never the post-deduction tax base.
    reduction_result = calculate_affine_reduction(
        gross,
        pre.amount,
        reduction,
        reduction_rounding,
    )

    return IrrfAssessmentMemory(
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
