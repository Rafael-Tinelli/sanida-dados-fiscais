from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from .contract_v1 import FiscalContractV1, FiscalRuleV1
from .engine_v1 import FiscalEngineError
from .money import DecimalInput, as_decimal, quantize
from .thirteenth_v1 import (
    ThirteenthAccrualResult,
    ThirteenthGrossMemory,
    ThirteenthReferenceRemuneration,
    calculate_thirteenth_accrual,
    calculate_thirteenth_gross,
    select_thirteenth_reference_remuneration,
    select_thirteenth_rule_bundle,
)
from .types_v1 import (
    ApplicabilityField,
    AssessmentContext,
    CodeEligibilityPayload,
    EligibilityMatrixPayload,
    PeriodRulePayload,
    PredicateOperator,
    ProrationPayload,
    RoundingPolicy,
    RoundingStage,
    ScopeDeclarationPayload,
    TerminationScopeItem,
)
from .vacation_v1 import (
    VacationProportionalAccrual,
    calculate_proportional_vacation_accrual,
)

ZERO = Decimal("0")


@dataclass(frozen=True)
class TerminationReasonDecision:
    code: str
    label: str
    salary_balance: bool
    thirteenth_proportional: bool
    vacation_proportional: bool
    acquired_vacation_if_due: bool


@dataclass(frozen=True)
class SalaryBalanceMemory:
    monthly_base_salary: Decimal
    termination_date: date
    days_counted_through_termination: int
    calendar_days_in_month: int
    amount: Decimal


@dataclass(frozen=True)
class H29ThirteenthProportionalMemory:
    accrual: ThirteenthAccrualResult
    reference: ThirteenthReferenceRemuneration
    gross: ThirteenthGrossMemory


@dataclass(frozen=True)
class H29LimitedEstimate:
    result_promise: str
    user_disclosure_required: bool
    reason: TerminationReasonDecision
    salary_balance: SalaryBalanceMemory | None
    thirteenth_proportional: H29ThirteenthProportionalMemory | None
    vacation_proportional: VacationProportionalAccrual | None
    acquired_vacation_if_due: bool
    included_items: tuple[TerminationScopeItem, ...]
    excluded_items: tuple[TerminationScopeItem, ...]


@dataclass(frozen=True)
class TerminationRuleBundle:
    reason_rule: FiscalRuleV1
    scope: ScopeDeclarationPayload
    salary_proration: ProrationPayload
    salary_rounding: RoundingPolicy
    thirteenth_eligibility: CodeEligibilityPayload
    vacation_eligibility: CodeEligibilityPayload
    vacation_acquisition: PeriodRulePayload


def _payload_as(rule: FiscalRuleV1, expected_type: type, *, rule_id: str):
    if not isinstance(rule.payload, expected_type):
        raise FiscalEngineError(f"{rule_id} has incompatible payload")
    return rule.payload


def _eligibility_for_code(payload: CodeEligibilityPayload, code: str) -> bool:
    if code in payload.eligible_codes:
        return True
    if code in payload.ineligible_codes:
        return False
    raise FiscalEngineError(
        f"termination reason {code} is unsupported by {payload.code_system} eligibility"
    )


def _assert_item_eligibility_matches_matrix(
    matrix: EligibilityMatrixPayload,
    *,
    thirteenth: CodeEligibilityPayload,
    vacation: CodeEligibilityPayload,
) -> None:
    matrix_codes = {row.code for row in matrix.rows}
    thirteenth_codes = set(thirteenth.eligible_codes) | set(thirteenth.ineligible_codes)
    vacation_codes = set(vacation.eligible_codes) | set(vacation.ineligible_codes)
    if thirteenth_codes != matrix_codes:
        raise FiscalEngineError("thirteenth eligibility does not cover exactly the H29 reason matrix")
    if vacation_codes != matrix_codes:
        raise FiscalEngineError("vacation eligibility does not cover exactly the H29 reason matrix")

    for row in matrix.rows:
        if _eligibility_for_code(thirteenth, row.code) != row.entitlements.thirteenth_proportional:
            raise FiscalEngineError(
                f"thirteenth eligibility diverges from termination matrix for reason {row.code}"
            )
        if _eligibility_for_code(vacation, row.code) != row.entitlements.vacation_proportional:
            raise FiscalEngineError(
                f"vacation eligibility diverges from termination matrix for reason {row.code}"
            )


def select_termination_rule_bundle(
    contract: FiscalContractV1,
    target_date: date,
) -> TerminationRuleBundle:
    reason_rule = contract.select_rule(
        "termination.reason_scope", target_date, AssessmentContext.TERMINATION
    )
    scope_rule = contract.select_rule(
        "termination.partial_output_scope", target_date, AssessmentContext.TERMINATION
    )
    salary_rule = contract.select_rule(
        "termination.salary_balance", target_date, AssessmentContext.TERMINATION
    )
    thirteenth_rule = contract.select_rule(
        "termination.thirteenth_proportional", target_date, AssessmentContext.TERMINATION
    )
    vacation_rule = contract.select_rule(
        "termination.vacation_proportional", target_date, AssessmentContext.TERMINATION
    )
    acquisition_rule = contract.select_rule(
        "vacation.acquisition_period", target_date, AssessmentContext.TERMINATION
    )

    matrix = _payload_as(
        reason_rule, EligibilityMatrixPayload, rule_id="termination.reason_scope"
    )
    scope = _payload_as(
        scope_rule, ScopeDeclarationPayload, rule_id="termination.partial_output_scope"
    )
    salary = _payload_as(
        salary_rule, ProrationPayload, rule_id="termination.salary_balance"
    )
    thirteenth = _payload_as(
        thirteenth_rule,
        CodeEligibilityPayload,
        rule_id="termination.thirteenth_proportional",
    )
    vacation = _payload_as(
        vacation_rule,
        CodeEligibilityPayload,
        rule_id="termination.vacation_proportional",
    )
    acquisition = _payload_as(
        acquisition_rule,
        PeriodRulePayload,
        rule_id="vacation.acquisition_period",
    )

    if salary_rule.rounding_policy is None:
        raise FiscalEngineError("termination.salary_balance requires explicit rounding policy")
    if salary_rule.rounding_policy.stage != RoundingStage.SALARY_BALANCE_RESULT:
        raise FiscalEngineError("salary balance rounding must occur at salary_balance_result")

    required_scope = {
        TerminationScopeItem.SALARY_BALANCE,
        TerminationScopeItem.THIRTEENTH_PROPORTIONAL,
        TerminationScopeItem.VACATION_PROPORTIONAL,
        TerminationScopeItem.ACQUIRED_OR_OVERDUE_VACATION,
    }
    if set(scope.included_items) != required_scope:
        raise FiscalEngineError("H29 v1 included scope diverges from the closed partial estimate")
    if scope.promise != "partial_estimate" or not scope.require_user_disclosure:
        raise FiscalEngineError("H29 v1 must remain a disclosed partial estimate")

    _assert_item_eligibility_matches_matrix(
        matrix,
        thirteenth=thirteenth,
        vacation=vacation,
    )

    return TerminationRuleBundle(
        reason_rule=reason_rule,
        scope=scope,
        salary_proration=salary,
        salary_rounding=salary_rule.rounding_policy,
        thirteenth_eligibility=thirteenth,
        vacation_eligibility=vacation,
        vacation_acquisition=acquisition,
    )


def _predicate_matches(actual: str, operator: PredicateOperator, expected) -> bool:
    if operator == PredicateOperator.EQ:
        return actual == expected
    if operator == PredicateOperator.NE:
        return actual != expected
    if operator == PredicateOperator.IN:
        return isinstance(expected, list) and actual in expected
    if operator == PredicateOperator.NOT_IN:
        return isinstance(expected, list) and actual not in expected
    raise FiscalEngineError(f"unsupported H29 applicability operator: {operator.value}")


def _assert_h29_scope_applicable(
    reason_rule: FiscalRuleV1,
    *,
    employment_regime: str,
    contract_term: str,
) -> None:
    values = {
        ApplicabilityField.EMPLOYMENT_REGIME: employment_regime,
        ApplicabilityField.CONTRACT_TERM: contract_term,
    }
    declared_fields = {predicate.field for predicate in reason_rule.applicability}
    if declared_fields != set(values):
        raise FiscalEngineError("H29 reason scope must declare employment regime and contract term")

    for predicate in reason_rule.applicability:
        if not _predicate_matches(values[predicate.field], predicate.operator, predicate.value):
            raise FiscalEngineError(
                "termination case is outside H29 v1 scope: "
                f"{predicate.field.value}={values[predicate.field]}"
            )


def resolve_termination_reason(
    *,
    bundle: TerminationRuleBundle,
    esocial_reason: str,
    employment_regime: str,
    contract_term: str,
) -> TerminationReasonDecision:
    _assert_h29_scope_applicable(
        bundle.reason_rule,
        employment_regime=employment_regime,
        contract_term=contract_term,
    )

    if not isinstance(esocial_reason, str) or len(esocial_reason) != 2 or not esocial_reason.isdigit():
        raise FiscalEngineError("esocial_reason must be a two-digit code")

    matrix = bundle.reason_rule.payload
    if not isinstance(matrix, EligibilityMatrixPayload):
        raise FiscalEngineError("termination reason matrix has incompatible payload")
    row = next((item for item in matrix.rows if item.code == esocial_reason), None)
    if row is None:
        raise FiscalEngineError(f"termination reason {esocial_reason} is UNSUPPORTED in H29 v1")

    thirteenth = _eligibility_for_code(bundle.thirteenth_eligibility, esocial_reason)
    vacation = _eligibility_for_code(bundle.vacation_eligibility, esocial_reason)
    if thirteenth != row.entitlements.thirteenth_proportional:
        raise FiscalEngineError("thirteenth eligibility diverges from H29 reason matrix")
    if vacation != row.entitlements.vacation_proportional:
        raise FiscalEngineError("vacation eligibility diverges from H29 reason matrix")

    return TerminationReasonDecision(
        code=row.code,
        label=row.label,
        salary_balance=row.entitlements.salary_balance,
        thirteenth_proportional=thirteenth,
        vacation_proportional=vacation,
        acquired_vacation_if_due=row.entitlements.acquired_vacation_if_due,
    )


def calculate_salary_balance(
    *,
    monthly_base_salary: DecimalInput,
    termination_date: date,
    days_counted_through_termination: int,
    payload: ProrationPayload,
    rounding_policy: RoundingPolicy,
) -> SalaryBalanceMemory:
    if payload.formula != "base_times_numerator_over_denominator":
        raise FiscalEngineError("unsupported salary balance formula")
    if payload.universal_fixed_denominator is not False:
        raise FiscalEngineError("H29 v1 forbids a universal fixed salary-balance denominator")
    if rounding_policy.stage != RoundingStage.SALARY_BALANCE_RESULT:
        raise FiscalEngineError("salary balance rounding must occur at salary_balance_result")
    if isinstance(days_counted_through_termination, bool) or not isinstance(
        days_counted_through_termination, int
    ):
        raise FiscalEngineError("days_counted_through_termination must be an integer")
    if days_counted_through_termination < 0:
        raise FiscalEngineError("days_counted_through_termination cannot be negative")

    calendar_days = monthrange(termination_date.year, termination_date.month)[1]
    if days_counted_through_termination > termination_date.day:
        raise FiscalEngineError(
            "days_counted_through_termination cannot exceed the termination day number"
        )
    if days_counted_through_termination > calendar_days:
        raise FiscalEngineError("salary balance numerator exceeds calendar month")

    base = as_decimal(monthly_base_salary, name="monthly_base_salary")
    if base < ZERO:
        raise FiscalEngineError("monthly_base_salary cannot be negative")

    raw = base * Decimal(days_counted_through_termination) / Decimal(calendar_days)
    amount = quantize(raw, rounding_policy)
    if amount < ZERO or amount > base:
        raise FiscalEngineError("salary balance result is outside safe bounds")

    return SalaryBalanceMemory(
        monthly_base_salary=base,
        termination_date=termination_date,
        days_counted_through_termination=days_counted_through_termination,
        calendar_days_in_month=calendar_days,
        amount=amount,
    )


def calculate_h29_limited_estimate(
    *,
    contract: FiscalContractV1,
    termination_date: date,
    esocial_reason: str,
    employment_regime: str,
    contract_term: str,
    employment_start: date,
    monthly_base_salary: DecimalInput,
    days_counted_through_termination: int,
    termination_month_remuneration: DecimalInput,
    thirteenth_rounding: RoundingPolicy,
) -> H29LimitedEstimate:
    """Execute only the closed H29 v1 partial scope.

    No notice projection, FGTS termination fine/withdrawal, unemployment insurance,
    stability indemnity, fixed-term termination rule, collective-bargaining item or
    unmodeled variable termination item is inferred here.
    """
    if employment_start > termination_date:
        raise FiscalEngineError("employment_start cannot be after termination_date")

    bundle = select_termination_rule_bundle(contract, termination_date)
    reason = resolve_termination_reason(
        bundle=bundle,
        esocial_reason=esocial_reason,
        employment_regime=employment_regime,
        contract_term=contract_term,
    )

    salary_balance = None
    if reason.salary_balance:
        salary_balance = calculate_salary_balance(
            monthly_base_salary=monthly_base_salary,
            termination_date=termination_date,
            days_counted_through_termination=days_counted_through_termination,
            payload=bundle.salary_proration,
            rounding_policy=bundle.salary_rounding,
        )

    thirteenth_memory = None
    if reason.thirteenth_proportional:
        thirteenth_bundle = select_thirteenth_rule_bundle(
            contract, termination_date, AssessmentContext.TERMINATION
        )
        accrual = calculate_thirteenth_accrual(
            employment_start=employment_start,
            accrual_end=termination_date,
            reference_year=termination_date.year,
            payload=thirteenth_bundle.accrual,
        )
        reference = select_thirteenth_reference_remuneration(
            origin_context=AssessmentContext.TERMINATION,
            payload=thirteenth_bundle.reference,
            termination_month_remuneration=termination_month_remuneration,
        )
        gross = calculate_thirteenth_gross(
            reference=reference,
            accrual=accrual,
            payload=thirteenth_bundle.accrual,
            rounding_policy=thirteenth_rounding,
        )
        thirteenth_memory = H29ThirteenthProportionalMemory(
            accrual=accrual,
            reference=reference,
            gross=gross,
        )

    vacation_memory = None
    if reason.vacation_proportional:
        vacation_memory = calculate_proportional_vacation_accrual(
            employment_start=employment_start,
            through_date=termination_date,
            payload=bundle.vacation_acquisition,
        )

    return H29LimitedEstimate(
        result_promise=bundle.scope.promise,
        user_disclosure_required=bundle.scope.require_user_disclosure,
        reason=reason,
        salary_balance=salary_balance,
        thirteenth_proportional=thirteenth_memory,
        vacation_proportional=vacation_memory,
        acquired_vacation_if_due=reason.acquired_vacation_if_due,
        included_items=tuple(bundle.scope.included_items),
        excluded_items=tuple(bundle.scope.excluded_items),
    )
