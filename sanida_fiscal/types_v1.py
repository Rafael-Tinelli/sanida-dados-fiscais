from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, validate_assignment=True)


class ContractStatus(str, Enum):
    DRAFT="DRAFT"; CANDIDATE="CANDIDATE"; VALIDATED="VALIDATED"; PUBLISHED="PUBLISHED"; SUPERSEDED="SUPERSEDED"; BLOCKED="BLOCKED"
class QualityStatus(str, Enum):
    UNVALIDATED="UNVALIDATED"; VALIDATED="VALIDATED"; REVIEW_REQUIRED="REVIEW_REQUIRED"; INVALID="INVALID"
class ChangeClass(str, Enum):
    SOURCE_REFRESH_NO_CHANGE="SOURCE_REFRESH_NO_CHANGE"; PARAMETER_CHANGE="PARAMETER_CHANGE"; EFFECTIVE_DATE_CHANGE="EFFECTIVE_DATE_CHANGE"; STRUCTURAL_CHANGE="STRUCTURAL_CHANGE"; RULE_ADDED="RULE_ADDED"; RULE_REMOVED="RULE_REMOVED"; SOURCE_UNAVAILABLE="SOURCE_UNAVAILABLE"; PARSER_INCOMPATIBLE="PARSER_INCOMPATIBLE"
class SourceRole(str, Enum):
    NORMATIVE_PRIMARY="normative_primary"; ADMINISTRATIVE_NORM="administrative_norm"; OFFICIAL_OPERATIONAL="official_operational"; OFFICIAL_REFERENCE_CASE="official_reference_case"
class SourceObservationStatus(str, Enum):
    AVAILABLE="AVAILABLE"; UNAVAILABLE="UNAVAILABLE"; PARSER_INCOMPATIBLE="PARSER_INCOMPATIBLE"
class Domain(str, Enum):
    INCOME_TAX="income_tax"; SOCIAL_SECURITY="social_security"; THIRTEENTH_SALARY="thirteenth_salary"; VACATION="vacation"; TERMINATION="termination"; TECHNICAL="technical"
class RuleClass(str, Enum):
    PARAMETER="parameter"; PARAMETERIZABLE_RULE="parameterizable_rule"; STRUCTURAL_RULE="structural_rule"; STRUCTURAL_TECHNICAL="structural_technical"
class AssessmentContext(str, Enum):
    MONTHLY="monthly"; THIRTEENTH="thirteenth"; VACATION_ENJOYED="vacation_enjoyed"; VACATION_CASH_ALLOWANCE="vacation_cash_allowance"; VACATION_INDEMNIFIED="vacation_indemnified"; TERMINATION="termination"; TECHNICAL="technical"
class ConsumerId(str, Enum):
    H26="H26"; H27="H27"; H28="H28"; H29="H29"
class CompetenceBasis(str, Enum):
    PAYMENT_DATE="payment_date"; COMPETENCE_MONTH="competence_month"; TERMINATION_DATE="termination_date"; ACQUISITION_PERIOD="acquisition_period"; RULE_SPECIFIC="rule_specific"
class PredicateOperator(str, Enum):
    EQ="eq"; NE="ne"; IN="in"; NOT_IN="not_in"; GTE="gte"; LTE="lte"
class Incidence(str, Enum):
    YES="yes"; NO="no"; CONDITIONAL="conditional"; NOT_APPLICABLE="not_applicable"


class VigencyWindow(StrictModel):
    effective_from: date
    effective_until: date | None = None
    @model_validator(mode="after")
    def interval(self):
        if self.effective_until and self.effective_until < self.effective_from: raise ValueError("invalid vigency interval")
        return self
    def covers(self, target: date) -> bool:
        return target >= self.effective_from and (self.effective_until is None or target <= self.effective_until)


class CompetencePolicy(StrictModel):
    basis: CompetenceBasis
    description: str | None = None
    @model_validator(mode="after")
    def specific(self):
        if self.basis == CompetenceBasis.RULE_SPECIFIC and not self.description: raise ValueError("rule_specific competence requires description")
        return self


class ApplicabilityPredicate(StrictModel):
    field: str = Field(min_length=1)
    operator: PredicateOperator
    value: JsonValue


class RoundingPolicy(StrictModel):
    decimal_places: int = Field(default=2, ge=0, le=12)
    mode: Literal["ROUND_HALF_UP","ROUND_HALF_EVEN","ROUND_DOWN","ROUND_FLOOR","ROUND_CEILING"] = "ROUND_HALF_UP"
    stage: str = Field(min_length=1)


class EvidenceObservation(StrictModel):
    source_id: str = Field(min_length=1)
    role: SourceRole
    observed_at_utc: datetime
    status: SourceObservationStatus = SourceObservationStatus.AVAILABLE
    locator: str | None = None
    snapshot_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    parser_id: str | None = None
    parser_version: str | None = None
    @field_validator("observed_at_utc")
    @classmethod
    def utc(cls, v):
        if v.tzinfo is None or v.utcoffset() != timezone.utc.utcoffset(v): raise ValueError("timestamp must be UTC")
        return v


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
        if self.rule_class in {RuleClass.STRUCTURAL_RULE, RuleClass.STRUCTURAL_TECHNICAL}:
            if self.auto_publish: raise ValueError("structural rules cannot auto-publish")
            if not self.structural_review_required: raise ValueError("structural rules require human review")
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
        prev=None; rate=Decimal("-1")
        for i,b in enumerate(self.brackets):
            if b.rate < rate: raise ValueError("rates must be non-decreasing")
            rate=b.rate
            if b.upper_bound is None:
                if i != len(self.brackets)-1: raise ValueError("open bracket must be last")
            elif prev is not None and b.upper_bound <= prev: raise ValueError("bounds must increase")
            if b.upper_bound is not None: prev=b.upper_bound
        return self
class ScalarPayload(StrictModel):
    type: Literal["scalar"] = "scalar"; value: Decimal; unit: str = Field(min_length=1)
class AffineReductionPayload(StrictModel):
    type: Literal["affine_reduction"] = "affine_reduction"
    full_relief_income_limit: Decimal = Field(gt=0); phaseout_income_limit: Decimal = Field(gt=0); max_reduction: Decimal = Field(ge=0); intercept: Decimal; slope: Decimal = Field(gt=0); input_semantic: str = Field(min_length=1); floor_at_zero: Literal[True] = True
    @model_validator(mode="after")
    def limits(self):
        if self.phaseout_income_limit <= self.full_relief_income_limit: raise ValueError("invalid reduction limits")
        return self
class ThresholdAccrualPayload(StrictModel):
    type: Literal["threshold_accrual"] = "threshold_accrual"; fraction_numerator: int = Field(gt=0); fraction_denominator: int = Field(gt=0); qualifying_days: int = Field(gt=0); max_units: int = Field(gt=0)
class FractionPayload(StrictModel):
    type: Literal["fraction"] = "fraction"; numerator: int = Field(gt=0); denominator: int = Field(gt=0); rounding: Literal["exact","statutory"] = "exact"
    @model_validator(mode="after")
    def frac(self):
        if self.numerator >= self.denominator: raise ValueError("fraction must be between 0 and 1")
        return self
class EntitlementBand(StrictModel):
    min_absences: int = Field(ge=0); max_absences: int | None = Field(default=None, ge=0); entitled_days: int = Field(ge=0)
class EntitlementBandsPayload(StrictModel):
    type: Literal["entitlement_bands"] = "entitlement_bands"; bands: list[EntitlementBand] = Field(min_length=1)
class TerminationEntitlements(StrictModel):
    salary_balance: bool; thirteenth_proportional: bool; vacation_proportional: bool; acquired_vacation_if_due: bool
class TerminationEligibilityRow(StrictModel):
    code: str = Field(pattern=r"^\d{2}$"); label: str = Field(min_length=1); entitlements: TerminationEntitlements
class EligibilityMatrixPayload(StrictModel):
    type: Literal["eligibility_matrix"] = "eligibility_matrix"; rows: list[TerminationEligibilityRow] = Field(min_length=1); unsupported_behavior: Literal["UNSUPPORTED"] = "UNSUPPORTED"
    @model_validator(mode="after")
    def unique(self):
        if len({x.code for x in self.rows}) != len(self.rows): raise ValueError("duplicate termination codes")
        return self
class IncidenceComponent(StrictModel):
    component: str = Field(min_length=1); irrf: Incidence; social_security: Incidence; notes: str | None = None
class IncidenceProfilePayload(StrictModel):
    type: Literal["incidence_profile"] = "incidence_profile"; components: list[IncidenceComponent] = Field(min_length=1)
class PolicyPayload(StrictModel):
    type: Literal["policy"] = "policy"; values: dict[str, JsonValue]

RulePayload = Annotated[Union[ProgressiveTablePayload,ScalarPayload,AffineReductionPayload,ThresholdAccrualPayload,FractionPayload,EntitlementBandsPayload,EligibilityMatrixPayload,IncidenceProfilePayload,PolicyPayload], Field(discriminator="type")]
