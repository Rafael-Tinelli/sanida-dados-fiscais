from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum

from .contract_v1 import FiscalContractV1
from .engine_v1 import (
    FiscalEngineError,
    IrrfAssessmentIdentity,
    IrrfAssessmentMemory,
    IrrfIncomeType,
    ProgressiveAssessmentResult,
    assess_irrf_2026,
    build_irrf_legal_deductions,
    calculate_progressive,
    select_irrf_rule_bundle,
)
from .money import DecimalInput, as_decimal, quantize
from .types_v1 import (
    AssessmentContext,
    ProgressiveTablePayload,
    RemunerationReferencePayload,
    RoundingPolicy,
    ScalarPayload,
    ThirteenthAdvancePayload,
    ThresholdAccrualPayload,
    VariableRemunerationPayload,
)

ZERO = Decimal("0")


class VariableComponentMode(str, Enum):
    NONE = "none"
    EXTERNAL_PRECOMPUTED = "external_precomputed"


@dataclass(frozen=True)
class ThirteenthMonthAccrual:
    month: int
    service_days: int
    qualifies: bool


@dataclass(frozen=True)
class ThirteenthAccrualResult:
    reference_year: int
    employment_start: date
    accrual_end: date
    months: tuple[ThirteenthMonthAccrual, ...]
    twelfths: int


@dataclass(frozen=True)
class ThirteenthReferenceRemuneration:
    origin_context: AssessmentContext
    fixed_reference: Decimal
    variable_component_mode: VariableComponentMode
    variable_component: Decimal
    total_reference: Decimal


@dataclass(frozen=True)
class ThirteenthGrossMemory:
    reference_remuneration: Decimal
    twelfths: int
    fraction_numerator: int
    fraction_denominator: int
    gross_thirteenth: Decimal


@dataclass(frozen=True)
class ThirteenthAdvanceResult:
    payment_date: date
    reference_remuneration: Decimal
    fraction_numerator: int
    fraction_denominator: int
    amount: Decimal


@dataclass(frozen=True)
class ThirteenthSocialSecurityMemory:
    assessment: IrrfAssessmentIdentity
    contribution_base: Decimal
    assessment_result: ProgressiveAssessmentResult

    @property
    def amount(self) -> Decimal:
        return self.assessment_result.amount


@dataclass(frozen=True)
class ThirteenthFiscalAssessmentMemory:
    assessment: IrrfAssessmentIdentity
    gross_thirteenth: Decimal
    social_security: ThirteenthSocialSecurityMemory
    irrf: IrrfAssessmentMemory


@dataclass(frozen=True)
class ThirteenthRuleBundle:
    accrual: ThresholdAccrualPayload
    reference: RemunerationReferencePayload
    variable: VariableRemunerationPayload
    advance: ThirteenthAdvancePayload | None


def _non_negative(value: DecimalInput, *, name: str) -> Decimal:
    amount = as_decimal(value, name=name)
    if amount < ZERO:
        raise FiscalEngineError(f"{name} cannot be negative")
    return amount


def select_thirteenth_rule_bundle(
    contract: FiscalContractV1,
    target_date: date,
    origin_context: AssessmentContext,
) -> ThirteenthRuleBundle:
    if origin_context not in {AssessmentContext.THIRTEENTH, AssessmentContext.TERMINATION}:
        raise FiscalEngineError("thirteenth rules require thirteenth or termination origin")

    accrual_rule = contract.select_rule(
        "thirteenth.accrual.twelfths", target_date, origin_context
    )
    reference_rule = contract.select_rule(
        "thirteenth.reference_remuneration", target_date, origin_context
    )
    variable_rule = contract.select_rule(
        "thirteenth.variable_remuneration", target_date, origin_context
    )

    if not isinstance(accrual_rule.payload, ThresholdAccrualPayload):
        raise FiscalEngineError("thirteenth accrual rule has incompatible payload")
    if not isinstance(reference_rule.payload, RemunerationReferencePayload):
        raise FiscalEngineError("thirteenth remuneration rule has incompatible payload")
    if not isinstance(variable_rule.payload, VariableRemunerationPayload):
        raise FiscalEngineError("thirteenth variable remuneration rule has incompatible payload")

    advance = None
    if origin_context == AssessmentContext.THIRTEENTH:
        advance_rule = contract.select_rule(
            "thirteenth.advance", target_date, AssessmentContext.THIRTEENTH
        )
        if not isinstance(advance_rule.payload, ThirteenthAdvancePayload):
            raise FiscalEngineError("thirteenth advance rule has incompatible payload")
        advance = advance_rule.payload

    return ThirteenthRuleBundle(
        accrual=accrual_rule.payload,
        reference=reference_rule.payload,
        variable=variable_rule.payload,
        advance=advance,
    )


def qualifies_thirteenth_month(
    service_days: int,
    payload: ThresholdAccrualPayload,
) -> bool:
    if isinstance(service_days, bool) or not isinstance(service_days, int):
        raise FiscalEngineError("service_days must be an integer")
    if service_days < 0:
        raise FiscalEngineError("service_days cannot be negative")
    return service_days >= payload.qualifying_days


def calculate_thirteenth_accrual(
    *,
    employment_start: date,
    accrual_end: date,
    reference_year: int,
    payload: ThresholdAccrualPayload,
) -> ThirteenthAccrualResult:
    """Count calendar-month twelfths using the contract's qualifying-day threshold.

    The caller supplies the legally relevant ``accrual_end``. Notice projection,
    suspensions or other service-day adjustments are not inferred here.
    """
    if employment_start > accrual_end:
        raise FiscalEngineError("employment_start cannot be after accrual_end")
    if reference_year < 1900 or reference_year > 9999:
        raise FiscalEngineError("reference_year out of supported range")

    year_start = date(reference_year, 1, 1)
    year_end = date(reference_year, 12, 31)
    start = max(employment_start, year_start)
    end = min(accrual_end, year_end)

    if start > end:
        return ThirteenthAccrualResult(
            reference_year=reference_year,
            employment_start=employment_start,
            accrual_end=accrual_end,
            months=(),
            twelfths=0,
        )

    months: list[ThirteenthMonthAccrual] = []
    units = 0
    for month in range(start.month, end.month + 1):
        month_start = date(reference_year, month, 1)
        month_end = date(reference_year, month, monthrange(reference_year, month)[1])
        service_start = max(start, month_start)
        service_end = min(end, month_end)
        service_days = max(0, (service_end - service_start).days + 1)
        qualifies = qualifies_thirteenth_month(service_days, payload)
        if qualifies and units < payload.max_units:
            units += 1
        months.append(
            ThirteenthMonthAccrual(
                month=month,
                service_days=service_days,
                qualifies=qualifies,
            )
        )

    return ThirteenthAccrualResult(
        reference_year=reference_year,
        employment_start=employment_start,
        accrual_end=accrual_end,
        months=tuple(months),
        twelfths=units,
    )


def select_thirteenth_reference_remuneration(
    *,
    origin_context: AssessmentContext,
    payload: RemunerationReferencePayload,
    december_due_remuneration: DecimalInput | None = None,
    termination_month_remuneration: DecimalInput | None = None,
    variable_payload: VariableRemunerationPayload | None = None,
    variable_component_mode: VariableComponentMode = VariableComponentMode.NONE,
    variable_component: DecimalInput = "0",
) -> ThirteenthReferenceRemuneration:
    """Select the fixed reference required by the contract and optionally add a
    precomputed variable component that is explicitly marked as external input.
    """
    if origin_context == AssessmentContext.THIRTEENTH:
        if payload.annual_reference != "december_due_remuneration":
            raise FiscalEngineError("unsupported annual thirteenth reference semantic")
        if december_due_remuneration is None:
            raise FiscalEngineError("december_due_remuneration is required")
        fixed = _non_negative(december_due_remuneration, name="december_due_remuneration")
    elif origin_context == AssessmentContext.TERMINATION:
        if payload.termination_reference != "termination_month_remuneration":
            raise FiscalEngineError("unsupported termination thirteenth reference semantic")
        if termination_month_remuneration is None:
            raise FiscalEngineError("termination_month_remuneration is required")
        fixed = _non_negative(
            termination_month_remuneration, name="termination_month_remuneration"
        )
    else:
        raise FiscalEngineError("thirteenth reference requires thirteenth or termination origin")

    variable = _non_negative(variable_component, name="variable_component")
    if variable_component_mode == VariableComponentMode.NONE:
        if variable != ZERO:
            raise FiscalEngineError(
                "non-zero variable component must be explicitly marked external_precomputed"
            )
    elif variable_component_mode == VariableComponentMode.EXTERNAL_PRECOMPUTED:
        if variable_payload is None:
            raise FiscalEngineError("variable remuneration payload is required")
        if not variable_payload.precomputed_average_allowed_only_as_external_input:
            raise FiscalEngineError("contract does not allow precomputed variable input")
    else:  # defensive for future enum additions
        raise FiscalEngineError("unsupported variable component mode")

    return ThirteenthReferenceRemuneration(
        origin_context=origin_context,
        fixed_reference=fixed,
        variable_component_mode=variable_component_mode,
        variable_component=variable,
        total_reference=fixed + variable,
    )


def calculate_thirteenth_gross(
    *,
    reference: ThirteenthReferenceRemuneration,
    accrual: ThirteenthAccrualResult,
    payload: ThresholdAccrualPayload,
    rounding_policy: RoundingPolicy,
) -> ThirteenthGrossMemory:
    if accrual.twelfths > payload.max_units:
        raise FiscalEngineError("thirteenth twelfths exceed contract maximum")
    raw = (
        reference.total_reference
        * Decimal(accrual.twelfths)
        * Decimal(payload.fraction_numerator)
        / Decimal(payload.fraction_denominator)
    )
    gross = quantize(raw, rounding_policy)
    return ThirteenthGrossMemory(
        reference_remuneration=reference.total_reference,
        twelfths=accrual.twelfths,
        fraction_numerator=payload.fraction_numerator,
        fraction_denominator=payload.fraction_denominator,
        gross_thirteenth=gross,
    )


def calculate_thirteenth_advance(
    *,
    payment_date: date,
    previous_month_salary: DecimalInput,
    payload: ThirteenthAdvancePayload,
    rounding_policy: RoundingPolicy,
    admission_in_year: bool = False,
    has_variable_remuneration: bool = False,
) -> ThirteenthAdvanceResult:
    """Execute only the fixed-remuneration branch represented by the closed
    official reference case. Special branches are rejected instead of guessed.
    """
    if payload.fixed_reference != "previous_month_salary":
        raise FiscalEngineError("unsupported thirteenth advance reference")
    if not payload.payment_window_start_month <= payment_date.month <= payload.payment_window_end_month:
        raise FiscalEngineError("thirteenth advance payment month is outside contract window")
    if admission_in_year:
        raise FiscalEngineError(
            "admission_in_year requires dedicated thirteenth advance handling"
        )
    if has_variable_remuneration:
        raise FiscalEngineError(
            "variable_remuneration requires dedicated thirteenth advance handling"
        )

    reference = _non_negative(previous_month_salary, name="previous_month_salary")
    raw = (
        reference
        * Decimal(payload.fraction_numerator)
        / Decimal(payload.fraction_denominator)
    )
    amount = quantize(raw, rounding_policy)
    return ThirteenthAdvanceResult(
        payment_date=payment_date,
        reference_remuneration=reference,
        fraction_numerator=payload.fraction_numerator,
        fraction_denominator=payload.fraction_denominator,
        amount=amount,
    )


def assess_thirteenth_social_security(
    *,
    assessment: IrrfAssessmentIdentity,
    gross_thirteenth: DecimalInput,
    progressive_table: ProgressiveTablePayload,
    rounding_policy: RoundingPolicy,
) -> ThirteenthSocialSecurityMemory:
    if assessment.income_type != IrrfIncomeType.THIRTEENTH:
        raise FiscalEngineError("thirteenth social security requires thirteenth assessment")
    gross = _non_negative(gross_thirteenth, name="gross_thirteenth")
    result = calculate_progressive(gross, progressive_table, rounding_policy)
    return ThirteenthSocialSecurityMemory(
        assessment=assessment,
        contribution_base=gross,
        assessment_result=result,
    )


def assess_thirteenth_fiscal_2026(
    *,
    contract: FiscalContractV1,
    target_date: date,
    origin_context: AssessmentContext,
    gross_thirteenth: DecimalInput,
    social_security_table: ProgressiveTablePayload,
    social_security_rounding: RoundingPolicy,
    dependent_count: int,
    dependent_deduction: ScalarPayload,
    pension: DecimalInput,
    simplified_discount: ScalarPayload,
) -> ThirteenthFiscalAssessmentMemory:
    """Assess INSS and IRRF for the 13th as isolated bases.

    The social-security table is an explicit typed input until the executable
    CANDIDATE materializes ``inss.employee.progressive_table``. No monthly
    contribution or monthly IRRF memory is accepted by this function.
    """
    assessment = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.THIRTEENTH,
        origin_context=origin_context,
    )
    gross = _non_negative(gross_thirteenth, name="gross_thirteenth")

    social_security = assess_thirteenth_social_security(
        assessment=assessment,
        gross_thirteenth=gross,
        progressive_table=social_security_table,
        rounding_policy=social_security_rounding,
    )
    deductions = build_irrf_legal_deductions(
        assessment=assessment,
        social_security=social_security.amount,
        dependent_count=dependent_count,
        dependent_deduction=dependent_deduction,
        pension=pension,
    )
    irrf_rules = select_irrf_rule_bundle(contract, target_date, assessment)
    irrf = assess_irrf_2026(
        assessment=assessment,
        gross_taxable_income=gross,
        legal_deductions=deductions,
        simplified_discount=simplified_discount,
        rules=irrf_rules,
    )

    return ThirteenthFiscalAssessmentMemory(
        assessment=assessment,
        gross_thirteenth=gross,
        social_security=social_security,
        irrf=irrf,
    )
