from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
import re
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, validate_assignment=True)


class ContractStatus(str, Enum):
    DRAFT = "DRAFT"
    CANDIDATE = "CANDIDATE"
    VALIDATED = "VALIDATED"
    PUBLISHED = "PUBLISHED"
    BLOCKED = "BLOCKED"


class QualityStatus(str, Enum):
    UNVALIDATED = "UNVALIDATED"
    VALIDATED = "VALIDATED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    INVALID = "INVALID"


class ChangeClass(str, Enum):
    SOURCE_REFRESH_NO_CHANGE = "SOURCE_REFRESH_NO_CHANGE"
    PARAMETER_CHANGE = "PARAMETER_CHANGE"
    EFFECTIVE_DATE_CHANGE = "EFFECTIVE_DATE_CHANGE"
    STRUCTURAL_CHANGE = "STRUCTURAL_CHANGE"
    RULE_ADDED = "RULE_ADDED"
    RULE_REMOVED = "RULE_REMOVED"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"
    PARSER_INCOMPATIBLE = "PARSER_INCOMPATIBLE"


class VersionBump(str, Enum):
    MAJOR = "major"
    MINOR = "minor"
    PATCH = "patch"


class SourceRole(str, Enum):
    NORMATIVE_PRIMARY = "normative_primary"
    ADMINISTRATIVE_NORM = "administrative_norm"
    OFFICIAL_OPERATIONAL = "official_operational"
    OFFICIAL_REFERENCE_CASE = "official_reference_case"


class SourceObservationStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    PARSER_INCOMPATIBLE = "PARSER_INCOMPATIBLE"


class RetrievalMethod(str, Enum):
    MANUAL = "manual"
    HTTP_HTML = "http_html"
    HTTP_PDF = "http_pdf"
    API = "api"
    DATASET = "dataset"


class Domain(str, Enum):
    INCOME_TAX = "income_tax"
    SOCIAL_SECURITY = "social_security"
    THIRTEENTH_SALARY = "thirteenth_salary"
    VACATION = "vacation"
    TERMINATION = "termination"
    TECHNICAL = "technical"


class RuleClass(str, Enum):
    PARAMETER = "parameter"
    PARAMETERIZABLE_RULE = "parameterizable_rule"
    STRUCTURAL_RULE = "structural_rule"
    PRODUCT_SCOPE_RULE = "product_scope_rule"
    TECHNICAL_CONTRACT_RULE = "technical_contract_rule"


STRUCTURAL_RULE_CLASSES = {
    RuleClass.STRUCTURAL_RULE,
    RuleClass.PRODUCT_SCOPE_RULE,
    RuleClass.TECHNICAL_CONTRACT_RULE,
}


class AssessmentContext(str, Enum):
    MONTHLY = "monthly"
    THIRTEENTH = "thirteenth"
    VACATION_ENJOYED = "vacation_enjoyed"
    VACATION_CASH_ALLOWANCE = "vacation_cash_allowance"
    VACATION_INDEMNIFIED = "vacation_indemnified"
    TERMINATION = "termination"
    TECHNICAL = "technical"


class ConsumerId(str, Enum):
    H26 = "H26"
    H27 = "H27"
    H28 = "H28"
    H29 = "H29"


class CompetenceBasis(str, Enum):
    PAYMENT_DATE = "payment_date"
    COMPETENCE_MONTH = "competence_month"
    TERMINATION_DATE = "termination_date"
    ACQUISITION_PERIOD = "acquisition_period"
    RULE_SPECIFIC = "rule_specific"


class RuleSpecificCompetenceKey(str, Enum):
    THIRTEENTH_ACCRUAL = "thirteenth_accrual_reference_year_or_termination_period"
    THIRTEENTH_REFERENCE_REMUNERATION = "thirteenth_reference_remuneration_cycle"
    THIRTEENTH_VARIABLE_REMUNERATION = "thirteenth_variable_remuneration_cycle"
    TECHNICAL_NON_TEMPORAL = "technical_non_temporal"


class PredicateOperator(str, Enum):
    EQ = "eq"
    NE = "ne"
    IN = "in"
    NOT_IN = "not_in"
    GTE = "gte"
    LTE = "lte"


class ApplicabilityField(str, Enum):
    EMPLOYMENT_REGIME = "employment_regime"
    CONTRACT_TERM = "contract_term"


class Incidence(str, Enum):
    YES = "yes"
    NO = "no"
    CONDITIONAL = "conditional"
    NOT_APPLICABLE = "not_applicable"


class IncidenceComponentId(str, Enum):
    CASH_ALLOWANCE_PRINCIPAL = "cash_allowance_principal"
    CONSTITUTIONAL_THIRD_ON_CASH_ALLOWANCE = "constitutional_third_on_cash_allowance"
    ENJOYED_VACATION_REMUNERATION = "enjoyed_vacation_remuneration"
    ENJOYED_VACATION_CONSTITUTIONAL_THIRD = "enjoyed_vacation_constitutional_third"
    INDEMNIFIED_VACATION_PRINCIPAL = "indemnified_vacation_principal"
    INDEMNIFIED_VACATION_CONSTITUTIONAL_THIRD = "indemnified_vacation_constitutional_third"


class ReleaseApprovalMode(str, Enum):
    AUTO_VALIDATED = "AUTO_VALIDATED"
    HUMAN_REVIEWED = "HUMAN_REVIEWED"


class ProgressiveCalculationMethod(str, Enum):
    MARGINAL_BY_BRACKET = "marginal_by_bracket"
    RATE_TIMES_BASE_MINUS_DEDUCTION = "rate_times_base_minus_deduction"


class ScalarUnit(str, Enum):
    BRL = "BRL"
    BRL_PER_DEPENDENT = "BRL_per_dependent"


class RoundingStage(str, Enum):
    PER_ASSESSMENT_RESULT = "per_assessment_result"
    AFTER_PRE_REDUCTION_IRRF = "after_pre_reduction_irrf"
    AFTER_PRE_REDUCTION_THIRTEENTH_IRRF = "after_pre_reduction_thirteenth_irrf"
    AFTER_PRE_REDUCTION_VACATION_IRRF = "after_pre_reduction_vacation_irrf"
    PER_COMPONENT = "per_component"
    SALARY_BALANCE_RESULT = "salary_balance_result"


class PolicyKind(str, Enum):
    DEDUCTIONS_BY_INCOME_TYPE = "deductions_by_income_type"
    INCOME_TYPE_PARTITION = "income_type_partition"
    SEPARATE_SOCIAL_SECURITY_ASSESSMENT = "separate_social_security_assessment"
    EXCLUSIVE_IRRF_ASSESSMENT = "exclusive_irrf_assessment"
    SEPARATE_VACATION_IRRF_ASSESSMENT = "separate_vacation_irrf_assessment"
    MONEY_DECIMAL_AND_ROUNDING = "money_decimal_and_rounding"
    CONTRACT_VIGENCY_AND_QUALITY = "contract_vigency_and_quality"


class PolicyAssertionId(str, Enum):
    DEDUCTIONS_MATCH_ASSESSMENT_CONTEXT = "deductions_must_match_assessment_context"
    PENSION_NOT_SILENTLY_ZEROED = "pension_must_not_be_silently_zeroed_when_applicable"
    INCOME_TYPES_ARE_SEPARATE = "monthly_thirteenth_and_vacation_are_distinct_assessments"
    THIRTEENTH_SOCIAL_SECURITY_SEPARATE = "thirteenth_social_security_is_separate_from_monthly"
    THIRTEENTH_IRRF_EXCLUSIVE = "thirteenth_irrf_is_exclusive_assessment"
    VACATION_IRRF_SEPARATE = "vacation_irrf_is_separate_assessment"
    MONEY_USES_DECIMAL = "money_uses_decimal"
    ROUNDING_EXPLICIT_PER_FORMULA = "rounding_must_be_explicit_per_formula_stage"
    GENERATED_AT_NOT_VIGENCY = "generated_at_does_not_prove_vigency"
    CONSUMER_REJECTS_INVALID_STATE = "consumer_rejects_incompatible_vigency_or_quality"


POLICY_ASSERTIONS_BY_KIND: dict[PolicyKind, frozenset[PolicyAssertionId]] = {
    PolicyKind.DEDUCTIONS_BY_INCOME_TYPE: frozenset(
        {
            PolicyAssertionId.DEDUCTIONS_MATCH_ASSESSMENT_CONTEXT,
            PolicyAssertionId.PENSION_NOT_SILENTLY_ZEROED,
        }
    ),
    PolicyKind.INCOME_TYPE_PARTITION: frozenset(
        {PolicyAssertionId.INCOME_TYPES_ARE_SEPARATE}
    ),
    PolicyKind.SEPARATE_SOCIAL_SECURITY_ASSESSMENT: frozenset(
        {PolicyAssertionId.THIRTEENTH_SOCIAL_SECURITY_SEPARATE}
    ),
    PolicyKind.EXCLUSIVE_IRRF_ASSESSMENT: frozenset(
        {PolicyAssertionId.THIRTEENTH_IRRF_EXCLUSIVE}
    ),
    PolicyKind.SEPARATE_VACATION_IRRF_ASSESSMENT: frozenset(
        {PolicyAssertionId.VACATION_IRRF_SEPARATE}
    ),
    PolicyKind.MONEY_DECIMAL_AND_ROUNDING: frozenset(
        {
            PolicyAssertionId.MONEY_USES_DECIMAL,
            PolicyAssertionId.ROUNDING_EXPLICIT_PER_FORMULA,
        }
    ),
    PolicyKind.CONTRACT_VIGENCY_AND_QUALITY: frozenset(
        {
            PolicyAssertionId.GENERATED_AT_NOT_VIGENCY,
            PolicyAssertionId.CONSUMER_REJECTS_INVALID_STATE,
        }
    ),
}


class FormulaComponentId(str, Enum):
    VACATION_REMUNERATION = "vacation_remuneration"
    CONSTITUTIONAL_THIRD = "constitutional_third"


class FormulaBasisId(str, Enum):
    VACATION_PAY_BASE = "vacation_pay_base"
    VACATION_REMUNERATION = "vacation_remuneration"


class TerminationScopeItem(str, Enum):
    SALARY_BALANCE = "salary_balance"
    THIRTEENTH_PROPORTIONAL = "thirteenth_proportional"
    VACATION_PROPORTIONAL = "vacation_proportional"
    ACQUIRED_OR_OVERDUE_VACATION = "acquired_or_overdue_vacation"
    NOTICE_PAY_OR_NOTICE_DISCOUNT = "notice_pay_or_notice_discount"
    FGTS_TERMINATION_FINE = "fgts_termination_fine"
    FGTS_WITHDRAWAL = "fgts_withdrawal"
    UNEMPLOYMENT_INSURANCE = "unemployment_insurance"
    STABILITY_INDEMNITIES = "stability_indemnities"
    COLLECTIVE_BARGAINING_SPECIFIC_ITEMS = "collective_bargaining_specific_items"
    FIXED_TERM_CONTRACT_TERMINATION_RULES = "fixed_term_contract_termination_rules"
    INDIRECT_TERMINATION_WITHOUT_JUDICIALLY_RESOLVED_CONTEXT = (
        "indirect_termination_without_judicially_resolved_context"
    )
    VARIABLE_TERMINATION_ITEMS_NOT_EXPLICITLY_MODELED = (
        "variable_termination_items_not_explicitly_modeled"
    )


class VigencyWindow(StrictModel):
    effective_from: date
    effective_until: date | None = None

    @model_validator(mode="after")
    def interval(self):
        if self.effective_until and self.effective_until < self.effective_from:
            raise ValueError("invalid vigency interval")
        return self

    def covers(self, target: date) -> bool:
        return target >= self.effective_from and (
            self.effective_until is None or target <= self.effective_until
        )


class CompetencePolicy(StrictModel):
    basis: CompetenceBasis
    rule_specific_key: RuleSpecificCompetenceKey | None = None
    description: str | None = None
    context_overrides: dict[AssessmentContext, CompetenceBasis] = Field(default_factory=dict)
    context_rule_specific_keys: dict[AssessmentContext, RuleSpecificCompetenceKey] = Field(
        default_factory=dict
    )

    @model_validator(mode="after")
    def specific(self):
        if self.basis == CompetenceBasis.RULE_SPECIFIC and self.rule_specific_key is None:
            raise ValueError("rule_specific competence requires rule_specific_key")
        if self.basis != CompetenceBasis.RULE_SPECIFIC and self.rule_specific_key is not None:
            raise ValueError("rule_specific_key requires rule_specific competence")
        for context, basis in self.context_overrides.items():
            if (
                basis == CompetenceBasis.RULE_SPECIFIC
                and context not in self.context_rule_specific_keys
            ):
                raise ValueError(
                    f"rule_specific override for {context.value} requires typed key"
                )
        extra_keys = set(self.context_rule_specific_keys) - set(self.context_overrides)
        if extra_keys:
            raise ValueError("context_rule_specific_keys require matching context_overrides")
        for context in self.context_rule_specific_keys:
            if self.context_overrides.get(context) != CompetenceBasis.RULE_SPECIFIC:
                raise ValueError("typed competence key requires rule_specific override")
        return self

    def basis_for(self, context: AssessmentContext) -> CompetenceBasis:
        return self.context_overrides.get(context, self.basis)


class ApplicabilityPredicate(StrictModel):
    field: ApplicabilityField
    operator: PredicateOperator
    value: JsonValue

    @model_validator(mode="after")
    def typed_values(self):
        if self.operator in {PredicateOperator.GTE, PredicateOperator.LTE}:
            raise ValueError("categorical applicability fields do not support range operators")
        values = self.value if isinstance(self.value, list) else [self.value]
        if self.field == ApplicabilityField.EMPLOYMENT_REGIME:
            allowed = {"monthly", "biweekly"}
        elif self.field == ApplicabilityField.CONTRACT_TERM:
            allowed = {"indefinite"}
        else:  # defensive for future enum additions
            raise ValueError("unsupported applicability field")
        if not values or any(not isinstance(item, str) or item not in allowed for item in values):
            raise ValueError(f"invalid canonical value for {self.field.value}")
        return self


class RoundingPolicy(StrictModel):
    decimal_places: int = Field(default=2, ge=0, le=12)
    mode: Literal[
        "ROUND_HALF_UP",
        "ROUND_HALF_EVEN",
        "ROUND_DOWN",
        "ROUND_FLOOR",
        "ROUND_CEILING",
    ] = "ROUND_HALF_UP"
    stage: RoundingStage


class EvidenceObservation(StrictModel):
    source_id: str = Field(min_length=1)
    role: SourceRole
    observed_at_utc: datetime
    status: SourceObservationStatus = SourceObservationStatus.AVAILABLE
    locator: str | None = None
    retrieval_method: RetrievalMethod = RetrievalMethod.MANUAL
    snapshot_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    snapshot_path: str | None = None
    parser_id: str | None = None
    parser_version: str | None = None

    @field_validator("observed_at_utc")
    @classmethod
    def utc(cls, value):
        if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
            raise ValueError("timestamp must be UTC")
        return value

    @model_validator(mode="after")
    def observation_consistency(self):
        if bool(self.parser_id) != bool(self.parser_version):
            raise ValueError("parser_id and parser_version must be provided together")
        if self.status != SourceObservationStatus.AVAILABLE:
            if self.snapshot_sha256 or self.snapshot_path:
                raise ValueError("unavailable/incompatible evidence cannot claim a snapshot")
        if self.snapshot_path and not self.snapshot_sha256:
            raise ValueError("snapshot_path requires snapshot_sha256")
        return self


class RuleQuality(StrictModel):
    status: QualityStatus
    reference_case_ids: list[str] = Field(default_factory=list)
    last_validated_at_utc: datetime | None = None
    reviewed_by_human: bool = False
    notes: str | None = None

    @field_validator("last_validated_at_utc")
    @classmethod
    def validation_utc(cls, value):
        if value is not None and (
            value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value)
        ):
            raise ValueError("last_validated_at_utc must be UTC")
        return value


class UpdatePolicy(StrictModel):
    rule_class: RuleClass
    auto_publish: bool
    structural_review_required: bool

    @model_validator(mode="after")
    def structural(self):
        if self.rule_class in STRUCTURAL_RULE_CLASSES:
            if self.auto_publish:
                raise ValueError("structural rules cannot auto-publish")
            if not self.structural_review_required:
                raise ValueError("structural rules require human review")
        return self


class LastGoodPolicy(StrictModel):
    allow_when_source_unavailable: bool = True
    require_validated_rule: Literal[True] = True
    require_within_effective_window: Literal[True] = True
    require_no_known_successor: Literal[True] = True
    allow_relabel_historical_as_current: Literal[False] = False
    on_expired: Literal["hard_fail"] = "hard_fail"
    on_unknown_vigency: Literal["hard_fail"] = "hard_fail"


class VersioningPolicy(StrictModel):
    schema_versioning: Literal["semver"] = "semver"
    contract_api_versioning: Literal["semver"] = "semver"
    rule_versioning: Literal["semver"] = "semver"
    release_id_strategy: Literal["content_addressed_sha256"] = "content_addressed_sha256"
    schema_compatibility: Literal["exact"] = "exact"
    contract_api_compatibility: Literal["exact"] = "exact"
    backward_compatibility: Literal["explicitly_tested_only"] = "explicitly_tested_only"
    forward_compatibility: Literal["not_assumed"] = "not_assumed"


class ConsumerCompatibility(StrictModel):
    schema_version: Literal["1.1.0"] = "1.1.0"
    contract_api_version: Literal["1.1.0"] = "1.1.0"
    consumers: list[ConsumerId]
    schema_match: Literal["exact"] = "exact"
    contract_api_match: Literal["exact"] = "exact"
    unknown_fields: Literal["reject"] = "reject"
    unknown_payload_types: Literal["reject"] = "reject"
    unsupported_behavior: Literal["hard_fail"] = "hard_fail"

    @field_validator("consumers")
    @classmethod
    def unique_consumers(cls, value):
        if len(value) != len(set(value)):
            raise ValueError("duplicate consumers")
        return value


class ReleaseLifecycle(StrictModel):
    validated_at_utc: datetime | None = None
    published_at_utc: datetime | None = None
    approval_mode: ReleaseApprovalMode | None = None
    approval_reference: str | None = None
    block_reasons: list[str] = Field(default_factory=list)

    @field_validator("validated_at_utc", "published_at_utc")
    @classmethod
    def lifecycle_utc(cls, value):
        if value is not None and (
            value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value)
        ):
            raise ValueError("lifecycle timestamps must be UTC")
        return value


class ProgressiveBracket(StrictModel):
    upper_bound: Decimal | None = None
    rate: Decimal = Field(ge=0, le=1)
    deduction: Decimal = Field(default=Decimal("0"), ge=0)


class ProgressiveTablePayload(StrictModel):
    type: Literal["progressive_table"] = "progressive_table"
    calculation_method: ProgressiveCalculationMethod
    brackets: list[ProgressiveBracket] = Field(min_length=1)
    cap_base: Decimal | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def ordered(self):
        previous = None
        previous_rate = Decimal("-1")
        for index, bracket in enumerate(self.brackets):
            if bracket.rate < previous_rate:
                raise ValueError("rates must be non-decreasing")
            previous_rate = bracket.rate
            if bracket.upper_bound is None:
                if index != len(self.brackets) - 1:
                    raise ValueError("open bracket must be last")
            elif previous is not None and bracket.upper_bound <= previous:
                raise ValueError("bounds must increase")
            if bracket.upper_bound is not None:
                previous = bracket.upper_bound
        if (
            self.calculation_method == ProgressiveCalculationMethod.MARGINAL_BY_BRACKET
            and any(bracket.deduction != 0 for bracket in self.brackets)
        ):
            raise ValueError("marginal_by_bracket cannot use direct table deductions")
        return self


class ScalarPayload(StrictModel):
    type: Literal["scalar"] = "scalar"
    value: Decimal
    unit: ScalarUnit


class AffineReductionPayload(StrictModel):
    type: Literal["affine_reduction"] = "affine_reduction"
    full_relief_income_limit: Decimal = Field(gt=0)
    phaseout_income_limit: Decimal = Field(gt=0)
    max_reduction: Decimal = Field(ge=0)
    full_relief_behavior: Literal["max_reduction"] = "max_reduction"
    phaseout_formula: Literal["intercept_minus_slope_times_input"] = (
        "intercept_minus_slope_times_input"
    )
    above_phaseout_behavior: Literal["zero"] = "zero"
    intercept: Decimal
    slope: Decimal = Field(gt=0)
    input_semantic: str = Field(min_length=1)
    floor_at_zero: Literal[True] = True

    @model_validator(mode="after")
    def limits(self):
        if self.phaseout_income_limit <= self.full_relief_income_limit:
            raise ValueError("invalid reduction limits")
        return self


class ThresholdAccrualPayload(StrictModel):
    type: Literal["threshold_accrual"] = "threshold_accrual"
    method: Literal["one_unit_per_month_if_days_gte_threshold"] = (
        "one_unit_per_month_if_days_gte_threshold"
    )
    fraction_numerator: int = Field(gt=0)
    fraction_denominator: int = Field(gt=0)
    qualifying_days: int = Field(gt=0)
    max_units: int = Field(gt=0)


class FractionPayload(StrictModel):
    type: Literal["fraction"] = "fraction"
    numerator: int = Field(gt=0)
    denominator: int = Field(gt=0)
    rounding: Literal["exact", "statutory"] = "exact"

    @model_validator(mode="after")
    def fraction(self):
        if self.numerator >= self.denominator:
            raise ValueError("fraction must be between 0 and 1")
        return self


class EntitlementBand(StrictModel):
    min_absences: int = Field(ge=0)
    max_absences: int | None = Field(default=None, ge=0)
    entitled_days: int = Field(ge=0)


class EntitlementBandsPayload(StrictModel):
    type: Literal["entitlement_bands"] = "entitlement_bands"
    input_semantic: Literal["unjustified_absences_in_acquisition_period"] = (
        "unjustified_absences_in_acquisition_period"
    )
    bands: list[EntitlementBand] = Field(min_length=1)
    outside_behavior: Literal["UNSUPPORTED"] = "UNSUPPORTED"

    @model_validator(mode="after")
    def ordered_bands(self):
        previous_max = None
        for band in self.bands:
            if band.max_absences is not None and band.max_absences < band.min_absences:
                raise ValueError("invalid entitlement band")
            if previous_max is not None and band.min_absences <= previous_max:
                raise ValueError("entitlement bands overlap or are unordered")
            previous_max = band.max_absences
        return self


class TerminationEntitlements(StrictModel):
    salary_balance: bool
    thirteenth_proportional: bool
    vacation_proportional: bool
    acquired_vacation_if_due: bool


class TerminationEligibilityRow(StrictModel):
    code: str = Field(pattern=r"^\d{2}$")
    label: str = Field(min_length=1)
    entitlements: TerminationEntitlements


class EligibilityMatrixPayload(StrictModel):
    type: Literal["eligibility_matrix"] = "eligibility_matrix"
    code_system: Literal["esocial_table_19_termination_reason"] = (
        "esocial_table_19_termination_reason"
    )
    rows: list[TerminationEligibilityRow] = Field(min_length=1)
    unsupported_behavior: Literal["UNSUPPORTED"] = "UNSUPPORTED"

    @model_validator(mode="after")
    def unique(self):
        if len({row.code for row in self.rows}) != len(self.rows):
            raise ValueError("duplicate termination codes")
        return self


class IncidenceComponent(StrictModel):
    component: IncidenceComponentId
    irrf: Incidence
    social_security: Incidence
    notes: str | None = None


class IncidenceProfilePayload(StrictModel):
    type: Literal["incidence_profile"] = "incidence_profile"
    components: list[IncidenceComponent] = Field(min_length=1)

    @model_validator(mode="after")
    def unique_components(self):
        if len({component.component for component in self.components}) != len(self.components):
            raise ValueError("duplicate incidence components")
        return self


class PolicyPayload(StrictModel):
    type: Literal["policy"] = "policy"
    policy_kind: PolicyKind
    assertions: list[PolicyAssertionId] = Field(min_length=1)

    @model_validator(mode="after")
    def exact_assertions(self):
        if len(self.assertions) != len(set(self.assertions)):
            raise ValueError("duplicate policy assertions")
        expected = POLICY_ASSERTIONS_BY_KIND[self.policy_kind]
        if set(self.assertions) != set(expected):
            raise ValueError(
                f"policy {self.policy_kind.value} requires canonical assertion set"
            )
        return self


class RemunerationReferencePayload(StrictModel):
    type: Literal["remuneration_reference"] = "remuneration_reference"
    annual_reference: Literal["december_due_remuneration"]
    termination_reference: Literal["termination_month_remuneration"]


class VariableRemunerationPayload(StrictModel):
    type: Literal["variable_remuneration"] = "variable_remuneration"
    accrual_through_month: Literal[11] = 11
    include_december_in_final_revision: Literal[True] = True
    revision_deadline_month: Literal[1] = 1
    revision_deadline_day: Literal[10] = 10
    precomputed_average_allowed_only_as_external_input: Literal[True] = True


class ThirteenthAdvancePayload(StrictModel):
    type: Literal["thirteenth_advance"] = "thirteenth_advance"
    fixed_reference: Literal["previous_month_salary"]
    fraction_numerator: Literal[1] = 1
    fraction_denominator: Literal[2] = 2
    payment_window_start_month: Literal[2] = 2
    payment_window_end_month: Literal[11] = 11
    special_handling: list[Literal["admission_in_year", "variable_remuneration"]] = Field(
        default_factory=lambda: ["admission_in_year", "variable_remuneration"]
    )


class PeriodRulePayload(StrictModel):
    type: Literal["period_rule"] = "period_rule"
    duration_months: int = Field(gt=0)
    anchor: Literal["employment_start_anniversary"]
    calendar_year_reset: Literal[False] = False
    proportional_accrual_method: Literal[
        "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    ] = "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    proportional_qualifying_days: Literal[15] = 15
    proportional_accrual_method: Literal[
        "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    ] = "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    proportional_qualifying_days: Literal[15] = 15
    proportional_accrual_method: Literal[
        "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    ] = "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    proportional_qualifying_days: Literal[15] = 15
    proportional_accrual_method: Literal[
        "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    ] = "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    proportional_qualifying_days: Literal[15] = 15


class FormulaComponent(StrictModel):
    component: FormulaComponentId
    basis: FormulaBasisId
    multiplier: Decimal = Field(gt=0)


class ComponentFormulaPayload(StrictModel):
    type: Literal["component_formula"] = "component_formula"
    components: list[FormulaComponent] = Field(min_length=2, max_length=2)

    @model_validator(mode="after")
    def canonical_vacation_formula(self):
        by_component = {component.component: component for component in self.components}
        if set(by_component) != {
            FormulaComponentId.VACATION_REMUNERATION,
            FormulaComponentId.CONSTITUTIONAL_THIRD,
        }:
            raise ValueError("component_formula requires canonical vacation components")
        if (
            by_component[FormulaComponentId.VACATION_REMUNERATION].basis
            != FormulaBasisId.VACATION_PAY_BASE
        ):
            raise ValueError("vacation remuneration must use vacation_pay_base")
        if (
            by_component[FormulaComponentId.CONSTITUTIONAL_THIRD].basis
            != FormulaBasisId.VACATION_REMUNERATION
        ):
            raise ValueError("constitutional third must use vacation_remuneration")
        return self


class ScopeDeclarationPayload(StrictModel):
    type: Literal["scope_declaration"] = "scope_declaration"
    promise: Literal["partial_estimate"]
    included_items: list[TerminationScopeItem] = Field(min_length=1)
    excluded_items: list[TerminationScopeItem] = Field(min_length=1)
    require_user_disclosure: Literal[True] = True

    @model_validator(mode="after")
    def disjoint_scope(self):
        if set(self.included_items) & set(self.excluded_items):
            raise ValueError("included and excluded scope items must be disjoint")
        if len(self.included_items) != len(set(self.included_items)) or len(
            self.excluded_items
        ) != len(set(self.excluded_items)):
            raise ValueError("scope items must be unique")
        return self


class ProrationPayload(StrictModel):
    type: Literal["proration"] = "proration"
    formula: Literal["base_times_numerator_over_denominator"]
    base_semantic: Literal["monthly_base_salary"]
    numerator_semantic: Literal["days_counted_through_termination"]
    denominator_semantic: Literal["calendar_days_in_month"]
    universal_fixed_denominator: Literal[False] = False


class CodeEligibilityPayload(StrictModel):
    type: Literal["code_eligibility"] = "code_eligibility"
    code_system: Literal["esocial_table_19_termination_reason"] = (
        "esocial_table_19_termination_reason"
    )
    eligible_codes: list[str] = Field(min_length=1)
    ineligible_codes: list[str] = Field(default_factory=list)
    unsupported_behavior: Literal["UNSUPPORTED"] = "UNSUPPORTED"

    @model_validator(mode="after")
    def disjoint(self):
        all_codes = self.eligible_codes + self.ineligible_codes
        if any(not re.fullmatch(r"\d{2}", code) for code in all_codes):
            raise ValueError("eligibility codes must be two digits")
        if len(all_codes) != len(set(all_codes)):
            raise ValueError("eligibility code sets must be unique and disjoint")
        return self


class PeriodStatePayload(StrictModel):
    type: Literal["period_state"] = "period_state"
    states: list[
        Literal["acquired_within_concession_period", "overdue_beyond_concession_period"]
    ] = Field(min_length=2, max_length=2)
    overdue_double_requires_explicit_rule: Literal[True] = True
    initial_consumer_may_limit_to_one_period_if_disclosed: Literal[True] = True

    @model_validator(mode="after")
    def exact_states(self):
        if set(self.states) != {
            "acquired_within_concession_period",
            "overdue_beyond_concession_period",
        }:
            raise ValueError("period_state must declare both canonical states")
        return self


RulePayload = Annotated[
    Union[
        ProgressiveTablePayload,
        ScalarPayload,
        AffineReductionPayload,
        ThresholdAccrualPayload,
        FractionPayload,
        EntitlementBandsPayload,
        EligibilityMatrixPayload,
        IncidenceProfilePayload,
        PolicyPayload,
        RemunerationReferencePayload,
        VariableRemunerationPayload,
        ThirteenthAdvancePayload,
        PeriodRulePayload,
        ComponentFormulaPayload,
        ScopeDeclarationPayload,
        ProrationPayload,
        CodeEligibilityPayload,
        PeriodStatePayload,
    ],
    Field(discriminator="type"),
]

RULE_PAYLOAD_TYPES = {
    "progressive_table",
    "scalar",
    "affine_reduction",
    "threshold_accrual",
    "fraction",
    "entitlement_bands",
    "eligibility_matrix",
    "incidence_profile",
    "policy",
    "remuneration_reference",
    "variable_remuneration",
    "thirteenth_advance",
    "period_rule",
    "component_formula",
    "scope_declaration",
    "proration",
    "code_eligibility",
    "period_state",
}
