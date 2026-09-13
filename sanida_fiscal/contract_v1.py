from __future__ import annotations

from datetime import date, datetime, timezone
from hashlib import sha256
import json, re
from typing import Literal

from pydantic import Field, field_validator, model_validator

from .types_v1 import *

RULE_ID_RE = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")
SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")

class ContractError(ValueError): pass
class RuleSelectionError(ContractError): pass
class ContractNotConsumableError(ContractError): pass


class FiscalRuleV1(StrictModel):
    rule_id: str
    rule_version: str
    domain: Domain
    calculators: list[ConsumerId] = Field(min_length=1)
    contexts: list[AssessmentContext] = Field(min_length=1)
    description: str = Field(min_length=1)
    applies_to: str = Field(min_length=1)
    applicability: list[ApplicabilityPredicate] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    calculation_order: int = Field(ge=0)
    competence: CompetencePolicy
    vigency: VigencyWindow
    rounding_policy: RoundingPolicy | None = None
    payload: RulePayload
    provenance: list[EvidenceObservation] = Field(min_length=1)
    quality: RuleQuality
    change_class: ChangeClass
    update_policy: UpdatePolicy

    @field_validator("rule_id")
    @classmethod
    def rid(cls,v):
        if not RULE_ID_RE.fullmatch(v): raise ValueError("invalid rule_id")
        return v
    @field_validator("rule_version")
    @classmethod
    def version(cls,v):
        if not SEMVER_RE.fullmatch(v): raise ValueError("rule_version must be SemVer")
        return v
    @field_validator("dependencies")
    @classmethod
    def deps(cls,v):
        if len(v) != len(set(v)): raise ValueError("duplicate dependencies")
        if any(not RULE_ID_RE.fullmatch(x) for x in v): raise ValueError("invalid dependency rule_id")
        return v
    @model_validator(mode="after")
    def structural_review(self):
        structural=self.update_policy.rule_class in {RuleClass.STRUCTURAL_RULE,RuleClass.STRUCTURAL_TECHNICAL}
        if structural and self.quality.status == QualityStatus.VALIDATED and not self.quality.reviewed_by_human:
            raise ValueError("validated structural rules require human review")
        return self


class FiscalContractV1(StrictModel):
    schema_version: Literal["1.0.0"] = "1.0.0"
    contract_id: Literal["br.sanida.fiscal"] = "br.sanida.fiscal"
    release_id: str = Field(min_length=1, pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
    status: ContractStatus
    jurisdiction: Literal["BR"] = "BR"
    generated_at_utc: datetime
    source_registry_version: str
    rule_inventory_version: str
    consumer_compatibility: ConsumerCompatibility
    last_good_policy: LastGoodPolicy = Field(default_factory=LastGoodPolicy)
    rules: list[FiscalRuleV1] = Field(min_length=1)
    notes: str | None = None

    @field_validator("generated_at_utc")
    @classmethod
    def utc(cls,v):
        if v.tzinfo is None or v.utcoffset() != timezone.utc.utcoffset(v): raise ValueError("generated_at_utc must be UTC")
        return v
    @field_validator("source_registry_version","rule_inventory_version")
    @classmethod
    def semver(cls,v):
        if not SEMVER_RE.fullmatch(v): raise ValueError("version must be SemVer")
        return v
    @model_validator(mode="after")
    def invariants(self):
        ids={r.rule_id for r in self.rules}
        for r in self.rules:
            missing=[d for d in r.dependencies if d not in ids]
            if missing: raise ValueError(f"{r.rule_id} has missing dependencies: {missing}")
        for i,left in enumerate(self.rules):
            for right in self.rules[i+1:]:
                if left.rule_id==right.rule_id and set(left.contexts)&set(right.contexts) and self._overlap(left.vigency,right.vigency):
                    raise ValueError(f"overlapping rule windows for {left.rule_id} in shared context")
        if self.status in {ContractStatus.VALIDATED,ContractStatus.PUBLISHED}:
            for r in self.rules:
                if r.quality.status != QualityStatus.VALIDATED: raise ValueError("validated/published contract requires all rules VALIDATED")
                if not any(e.status==SourceObservationStatus.AVAILABLE and e.snapshot_sha256 for e in r.provenance):
                    raise ValueError(f"{r.rule_id} requires at least one available hashed snapshot")
        return self
    @staticmethod
    def _overlap(a,b):
        return a.effective_from <= (b.effective_until or date.max) and b.effective_from <= (a.effective_until or date.max)
    def select_rule(self,rule_id:str,target_date:date,context:AssessmentContext,*,require_validated:bool=True)->FiscalRuleV1:
        matches=[r for r in self.rules if r.rule_id==rule_id and context in r.contexts and r.vigency.covers(target_date)]
        if len(matches)!=1: raise RuleSelectionError(f"expected exactly one rule for {rule_id}/{context.value}/{target_date}; found {len(matches)}")
        r=matches[0]
        if require_validated and r.quality.status != QualityStatus.VALIDATED: raise RuleSelectionError(f"rule {rule_id} is not validated")
        return r
    def assert_consumable(self)->None:
        if self.status != ContractStatus.PUBLISHED: raise ContractNotConsumableError(f"contract status {self.status.value} is not consumable; expected PUBLISHED")
    def can_use_last_good(self,rule:FiscalRuleV1,target_date:date,*,known_successor:bool)->bool:
        p=self.last_good_policy
        return bool(p.allow_when_source_unavailable and (not p.require_validated_rule or rule.quality.status==QualityStatus.VALIDATED) and (not p.require_within_effective_window or rule.vigency.covers(target_date)) and (not p.require_no_known_successor or not known_successor))
    def canonical_json(self)->str:
        return json.dumps(self.model_dump(mode="json",exclude_none=True),ensure_ascii=False,sort_keys=True,separators=(",",":"))
    def content_sha256(self)->str:
        return sha256(self.canonical_json().encode()).hexdigest()
