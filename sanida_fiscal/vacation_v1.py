from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from .contract_v1 import FiscalContractV1
from .engine_v1 import FiscalEngineError
from .money import DecimalInput, as_decimal
from .types_v1 import (
    AssessmentContext,
    EntitlementBandsPayload,
    FractionPayload,
    Incidence,
    IncidenceComponent,
    IncidenceComponentId,
    IncidenceProfilePayload,
    PeriodRulePayload,
)

ZERO = Decimal("0")


@dataclass(frozen=True)
class VacationAcquisitionPeriod:
    employment_start: date
    period_index: int
    start: date
    end_exclusive: date

    @property
    def end_inclusive(self) -> date:
        return self.end_exclusive - timedelta(days=1)


@dataclass(frozen=True)
class VacationProportionalSlice:
    twelfth_index: int
    start: date
    end_exclusive: date
    service_days: int
    full_acquisition_month: bool
    qualifies: bool


@dataclass(frozen=True)
class VacationProportionalAccrual:
    period: VacationAcquisitionPeriod
    through_date: date
    twelfths: int
    slices: tuple[VacationProportionalSlice, ...]


@dataclass(frozen=True)
class VacationEntitlement:
    unjustified_absences: int
    entitled_days: int


@dataclass(frozen=True)
class VacationCashAllowanceDays:
    entitled_days: int
    cash_allowance_days: int
    remaining_vacation_days: int


@dataclass(frozen=True)
class VacationCashAllowanceTaxTreatment:
    principal: IncidenceComponent
    constitutional_third: IncidenceComponent


@dataclass(frozen=True)
class VacationCashAllowanceTaxBases:
    principal_amount: Decimal
    constitutional_third_amount: Decimal
    irrf_taxable_amount: Decimal
    social_security_base: Decimal


@dataclass(frozen=True)
class VacationRuleBundle:
    acquisition: PeriodRulePayload
    entitlement: EntitlementBandsPayload
    cash_allowance_fraction: FractionPayload
    principal_incidence: IncidenceProfilePayload
    constitutional_third_incidence: IncidenceProfilePayload


def _add_months(anchor: date, months: int) -> date:
    if months < 0:
        raise FiscalEngineError("months cannot be negative")
    month_index = (anchor.month - 1) + months
    year = anchor.year + month_index // 12
    month = month_index % 12 + 1
    day = min(anchor.day, monthrange(year, month)[1])
    return date(year, month, day)


def select_vacation_rule_bundle(
    contract: FiscalContractV1,
    target_date: date,
) -> VacationRuleBundle:
    acquisition_rule = contract.select_rule(
        "vacation.acquisition_period",
        target_date,
        AssessmentContext.VACATION_ENJOYED,
    )
    entitlement_rule = contract.select_rule(
        "vacation.entitlement_days_by_absences",
        target_date,
        AssessmentContext.VACATION_ENJOYED,
    )
    allowance_rule = contract.select_rule(
        "vacation.abono_pecuniario",
        target_date,
        AssessmentContext.VACATION_CASH_ALLOWANCE,
    )
    principal_rule = contract.select_rule(
        "vacation.abono.ir_exemption",
        target_date,
        AssessmentContext.VACATION_CASH_ALLOWANCE,
    )
    third_rule = contract.select_rule(
        "vacation.abono_constitutional_third.ir_incidence",
        target_date,
        AssessmentContext.VACATION_CASH_ALLOWANCE,
    )

    if not isinstance(acquisition_rule.payload, PeriodRulePayload):
        raise FiscalEngineError("vacation acquisition rule must use period_rule payload")
    if not isinstance(entitlement_rule.payload, EntitlementBandsPayload):
        raise FiscalEngineError("vacation entitlement rule must use entitlement_bands payload")
    if not isinstance(allowance_rule.payload, FractionPayload):
        raise FiscalEngineError("vacation cash allowance rule must use fraction payload")
    if not isinstance(principal_rule.payload, IncidenceProfilePayload):
        raise FiscalEngineError("cash allowance principal rule must use incidence_profile")
    if not isinstance(third_rule.payload, IncidenceProfilePayload):
        raise FiscalEngineError("cash allowance third rule must use incidence_profile")

    return VacationRuleBundle(
        acquisition=acquisition_rule.payload,
        entitlement=entitlement_rule.payload,
        cash_allowance_fraction=allowance_rule.payload,
        principal_incidence=principal_rule.payload,
        constitutional_third_incidence=third_rule.payload,
    )


def select_vacation_acquisition_rule(
    contract: FiscalContractV1,
    target_date: date,
    context: AssessmentContext,
) -> PeriodRulePayload:
    if context not in {
        AssessmentContext.VACATION_ENJOYED,
        AssessmentContext.VACATION_INDEMNIFIED,
        AssessmentContext.TERMINATION,
    }:
        raise FiscalEngineError("invalid context for vacation acquisition period")
    rule = contract.select_rule("vacation.acquisition_period", target_date, context)
    if not isinstance(rule.payload, PeriodRulePayload):
        raise FiscalEngineError("vacation acquisition rule must use period_rule payload")
    return rule.payload


def acquisition_period_for_date(
    *,
    employment_start: date,
    target_date: date,
    payload: PeriodRulePayload,
) -> VacationAcquisitionPeriod:
    if target_date < employment_start:
        raise FiscalEngineError("target_date cannot precede employment_start")
    if payload.calendar_year_reset is not False:
        raise FiscalEngineError("vacation acquisition period cannot reset on calendar year")
    if payload.anchor != "employment_start_anniversary":
        raise FiscalEngineError("unsupported vacation acquisition anchor")

    elapsed_months = (target_date.year - employment_start.year) * 12 + (
        target_date.month - employment_start.month
    )
    period_index = max(0, elapsed_months // payload.duration_months)
    start = _add_months(employment_start, period_index * payload.duration_months)
    if start > target_date:
        period_index -= 1
        start = _add_months(employment_start, period_index * payload.duration_months)
    end_exclusive = _add_months(
        employment_start, (period_index + 1) * payload.duration_months
    )

    return VacationAcquisitionPeriod(
        employment_start=employment_start,
        period_index=period_index,
        start=start,
        end_exclusive=end_exclusive,
    )


def calculate_proportional_vacation_accrual(
    *,
    employment_start: date,
    through_date: date,
    payload: PeriodRulePayload,
) -> VacationProportionalAccrual:
    if (
        payload.proportional_accrual_method
        != "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    ):
        raise FiscalEngineError("unsupported proportional vacation accrual method")

    period = acquisition_period_for_date(
        employment_start=employment_start,
        target_date=through_date,
        payload=payload,
    )

    slices: list[VacationProportionalSlice] = []
    twelfths = 0
    through_exclusive = through_date + timedelta(days=1)

    for index in range(payload.duration_months):
        slice_start = _add_months(period.start, index)
        slice_end = _add_months(period.start, index + 1)
        if slice_start >= period.end_exclusive or slice_start > through_date:
            break
        slice_end = min(slice_end, period.end_exclusive)
        service_end = min(slice_end, through_exclusive)
        service_days = max(0, (service_end - slice_start).days)
        full_month = service_end >= slice_end
        qualifies = full_month or service_days >= payload.proportional_qualifying_days
        if qualifies:
            twelfths += 1
        slices.append(
            VacationProportionalSlice(
                twelfth_index=index + 1,
                start=slice_start,
                end_exclusive=slice_end,
                service_days=service_days,
                full_acquisition_month=full_month,
                qualifies=qualifies,
            )
        )

    if twelfths > payload.duration_months:
        raise FiscalEngineError("proportional vacation exceeds acquisition period")

    return VacationProportionalAccrual(
        period=period,
        through_date=through_date,
        twelfths=twelfths,
        slices=tuple(slices),
    )


def vacation_entitlement_from_absences(
    unjustified_absences: int,
    payload: EntitlementBandsPayload,
) -> VacationEntitlement:
    if isinstance(unjustified_absences, bool) or not isinstance(unjustified_absences, int):
        raise FiscalEngineError("unjustified_absences must be an integer")
    if unjustified_absences < 0:
        raise FiscalEngineError("unjustified_absences cannot be negative")

    for band in payload.bands:
        within_upper = (
            band.max_absences is None or unjustified_absences <= band.max_absences
        )
        if unjustified_absences >= band.min_absences and within_upper:
            return VacationEntitlement(
                unjustified_absences=unjustified_absences,
                entitled_days=band.entitled_days,
            )

    raise FiscalEngineError("unjustified absences are outside supported entitlement bands")


def calculate_cash_allowance_days(
    *,
    entitled_days: int,
    payload: FractionPayload,
) -> VacationCashAllowanceDays:
    if isinstance(entitled_days, bool) or not isinstance(entitled_days, int):
        raise FiscalEngineError("entitled_days must be an integer")
    if entitled_days < 0:
        raise FiscalEngineError("entitled_days cannot be negative")

    numerator = entitled_days * payload.numerator
    if numerator % payload.denominator != 0:
        raise FiscalEngineError(
            "cash allowance fraction is non-integral; statutory rounding is not inferable"
        )
    allowance = numerator // payload.denominator
    return VacationCashAllowanceDays(
        entitled_days=entitled_days,
        cash_allowance_days=allowance,
        remaining_vacation_days=entitled_days - allowance,
    )


def _single_component(
    payload: IncidenceProfilePayload,
    expected: IncidenceComponentId,
) -> IncidenceComponent:
    if len(payload.components) != 1:
        raise FiscalEngineError("incidence profile must contain exactly one canonical component")
    component = payload.components[0]
    if component.component != expected:
        raise FiscalEngineError(
            f"incidence profile component mismatch: expected {expected.value}"
        )
    return component


def resolve_cash_allowance_tax_treatment(
    *,
    principal_payload: IncidenceProfilePayload,
    constitutional_third_payload: IncidenceProfilePayload,
) -> VacationCashAllowanceTaxTreatment:
    principal = _single_component(
        principal_payload, IncidenceComponentId.CASH_ALLOWANCE_PRINCIPAL
    )
    third = _single_component(
        constitutional_third_payload,
        IncidenceComponentId.CONSTITUTIONAL_THIRD_ON_CASH_ALLOWANCE,
    )

    if principal.irrf != Incidence.NO or principal.social_security != Incidence.NO:
        raise FiscalEngineError("cash allowance principal must be IRRF=no and CP=no")
    if third.irrf != Incidence.YES or third.social_security != Incidence.NO:
        raise FiscalEngineError(
            "constitutional third on cash allowance must be IRRF=yes and CP=no"
        )

    return VacationCashAllowanceTaxTreatment(
        principal=principal,
        constitutional_third=third,
    )


def cash_allowance_tax_bases(
    *,
    principal_amount: DecimalInput,
    constitutional_third_amount: DecimalInput,
    treatment: VacationCashAllowanceTaxTreatment,
) -> VacationCashAllowanceTaxBases:
    principal = as_decimal(principal_amount, name="cash_allowance_principal")
    third = as_decimal(
        constitutional_third_amount,
        name="constitutional_third_on_cash_allowance",
    )
    if principal < ZERO or third < ZERO:
        raise FiscalEngineError("cash allowance component amounts cannot be negative")

    irrf_taxable = ZERO
    social_security_base = ZERO

    if treatment.principal.irrf == Incidence.YES:
        irrf_taxable += principal
    elif treatment.principal.irrf != Incidence.NO:
        raise FiscalEngineError("conditional principal IRRF incidence is unsupported")

    if treatment.constitutional_third.irrf == Incidence.YES:
        irrf_taxable += third
    elif treatment.constitutional_third.irrf != Incidence.NO:
        raise FiscalEngineError("conditional third IRRF incidence is unsupported")

    if treatment.principal.social_security == Incidence.YES:
        social_security_base += principal
    elif treatment.principal.social_security != Incidence.NO:
        raise FiscalEngineError("conditional principal CP incidence is unsupported")

    if treatment.constitutional_third.social_security == Incidence.YES:
        social_security_base += third
    elif treatment.constitutional_third.social_security != Incidence.NO:
        raise FiscalEngineError("conditional third CP incidence is unsupported")

    return VacationCashAllowanceTaxBases(
        principal_amount=principal,
        constitutional_third_amount=third,
        irrf_taxable_amount=irrf_taxable,
        social_security_base=social_security_base,
    )
