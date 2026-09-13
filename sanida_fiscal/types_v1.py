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
    SUPERSEDED = "SUPERSEDED"
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


class PredicateOperator(str, Enum):
    EQ = "eq"
    NE = "ne"
    IN = "in"
    NOT_IN = "not_in"
    GTE = "gte"
    LTE = "lte"


class Incidence(str, Enum):
    YES = "yes"
    NO = "no"
    CONDITIONAL = "conditional"
    NOT_APPLICABLE = "not_applicable"


class ReleaseApprovalMode(str, Enum):
    AUTO_VALIDATED = "AUTO_VALIDATED"
    HUMAN_REVIEWED = "HUMAN_REVIEWED"


class PolicyKind(str, Enum):
    DEDUCTIONS_BY_INCOME_TYPE = "deductions_by_income_type"
    INCOME_TYPE_PARTITION = "income_type_partition"
    SEPARATE_SOCIAL_SECURITY_ASSESSMENT = "separate_social_security_assessment"
    EXCLUSIVE_IRRF_ASSESSMENT = "exclusive_irrf_assessment"
    SEPARATE_VACATION_IRRF_ASSESSMENT = "separate_vacation_irrf_assessment"
    MONEY_DECIMAL_AND_ROUNDING = "money_decimal_and_rounding"
    CONTRACT_VIGENCY_AND_QUALITY = "contract_vigency_and_quality"


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
    description: str | None = None
    context_overrides: dict[AssessmentContext, CompetenceBasis] = Field(default_factory=dict)
    context_descriptions: dict[AssessmentContext, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def specific(self):
        if self.basis == CompetenceBasis.RULE_SPECIFIC and not self.description:
            raise ValueError("rule_specific competence requires description")
        for context, basis in self.context_overrides.items():
            if basis == CompetenceBasis.RULE_SPECIFIC and not self.context_descriptions.get(context):
                raise ValueError(
                    f"rule_specific override for {context.value} requires context description"
                )
        extra_descriptions = set(self.context_descriptions) - set(self.context_overrides)
        if extra_descriptions:
            raise ValueError("context_descriptions require matching context_overrides")
        return self

    def basis_for(self, context: AssessmentContext) -> CompetenceBasis:
        return self.context_overrides.get(context, self.basis)


class ApplicabilityPredicate(StrictModel):
    field: str = Field(min_length=1)
    operator: PredicateOperator
    value: JsonValue


class RoundingPolicy(StrictModel):
    decimal_places: int = Field(default=2, ge=0, le=12)
    mode: Literal[
        "ROUND_HALF_UP",
        "ROUND_HALF_EVEN",
        "ROUND_DOWN",
        "ROUND_FLOOR",
        "ROUND_CEILING",
    ] = "ROUND_HALF_UP"
    stage: str = Field(min_length=1)


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


class ConsumerCompatibility(StrictModel):
    contract_api_version: str = Field(pattern=r"^\d+\.\d+\.\d+$")
    consumers: list[ConsumerId]
    unsupported_behavior: Literal["hard_fail"] = "hard_fail"


class ReleaseLifecycle(StrictModel):
    previous_release_id: str | None = None
    validated_at_utc: datetime | None = None
    published_at_utc: datetime | None = None
    superseded_at_utc: datetime | None = None
    superseded_by_release_id: str | None = None
    approval_mode: ReleaseApprovalMode | None = None
    approval_reference: str | None = None
    block_reasons: list[str] = Field(default_factory=list)

    @field_validator("validated_at_utc", "published_at_utc", "superseded_at_utc")
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
        return self


class ScalarPayload(StrictModel):
    type: Literal["scalar"] = "scalar"
    value: Decimal
    unit: str = Field(min_length=1)


class AffineReductionPayload(StrictModel):
    type: Literal["affine_reduction"] = "affine_reduction"
    full_relief_income_limit: Decimal = Field(gt=0)
    phaseout_income_limit: Decimal = Field(gt=0)
    max_reduction: Decimal = Field(ge=0)
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
    rows: list[TerminationEligibilityRow] = Field(min_length=1)
    unsupported_behavior: Literal["UNSUPPORTED"] = "UNSUPPORTED"

    @model_validator(mode="after")
    def unique(self):
        if len({row.code for row in self.rows}) != len(self.rows):
            raise ValueError("duplicate termination codes")
        return self


class IncidenceComponent(StrictModel):
    component: str = Field(min_length=1)
    irrf: Incidence
    social_security: Incidence
    notes: str | None = None


class IncidenceProfilePayload(StrictModel):
    type: Literal["incidence_profile"] = "incidence_profile"
    components: list[IncidenceComponent] = Field(min_length=1)


class PolicyPayload(StrictModel):
    type: Literal["policy"] = "policy"
    policy_kind: PolicyKind
    assertions: list[str] = Field(min_length=1)
    values: dict[str, JsonValue] = Field(default_factory=dict)


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


class FormulaComponent(StrictModel):
    component: str = Field(min_length=1)
    basis: str = Field(min_length=1)
    multiplier: Decimal = Field(gt=0)


class ComponentFormulaPayload(StrictModel):
    type: Literal["component_formula"] = "component_formula"
    components: list[FormulaComponent] = Field(min_length=1)


class ScopeDeclarationPayload(StrictModel):
    type: Literal["scope_declaration"] = "scope_declaration"
    promise: Literal["partial_estimate"]
    included_items: list[str] = Field(min_length=1)
    excluded_items: list[str] = Field(min_length=1)
    require_user_disclosure: Literal[True] = True

    @model_validator(mode="after")
    def disjoint_scope(self):
        if set(self.included_items) & set(self.excluded_items):
            raise ValueError("included and excluded scope items must be disjoint")
        return self


class ProrationPayload(StrictModel):
    type: Literal["proration"] = "proration"
    formula: Literal["base_times_numerator_over_denominator"]
    base_semantic: str = Field(min_length=1)
    numerator_semantic: str = Field(min_length=1)
    denominator_semantic: str = Field(min_length=1)
    universal_fixed_denominator: Literal[False] = False


class CodeEligibilityPayload(StrictModel):
    type: Literal["code_eligibility"] = "code_eligibility"
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
