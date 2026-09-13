from __future__ import annotations

from datetime import date, datetime, timezone
from hashlib import sha256
import json
import re
from typing import Literal

from pydantic import Field, field_validator, model_validator

from .types_v1 import *

RULE_ID_RE = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")
SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")


class ContractError(ValueError):
    pass


class RuleSelectionError(ContractError):
    pass


class ContractNotConsumableError(ContractError):
    pass


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
    def rule_id_format(cls, value):
        if not RULE_ID_RE.fullmatch(value):
            raise ValueError("invalid rule_id")
        return value

    @field_validator("rule_version")
    @classmethod
    def version_format(cls, value):
        if not SEMVER_RE.fullmatch(value):
            raise ValueError("rule_version must be SemVer")
        return value

    @field_validator("calculators", "contexts")
    @classmethod
    def unique_enums(cls, value):
        if len(value) != len(set(value)):
            raise ValueError("duplicate enum values")
        return value

    @field_validator("dependencies")
    @classmethod
    def dependencies_format(cls, value):
        if len(value) != len(set(value)):
            raise ValueError("duplicate dependencies")
        if any(not RULE_ID_RE.fullmatch(item) for item in value):
            raise ValueError("invalid dependency rule_id")
        return value

    @model_validator(mode="after")
    def semantic_invariants(self):
        if self.rule_id in self.dependencies:
            raise ValueError("rule cannot depend on itself")

        unknown_override_contexts = set(self.competence.context_overrides) - set(self.contexts)
        if unknown_override_contexts:
            raise ValueError("competence override references context not declared by rule")

        structural = self.update_policy.rule_class in STRUCTURAL_RULE_CLASSES
        if (
            structural
            and self.quality.status == QualityStatus.VALIDATED
            and not self.quality.reviewed_by_human
        ):
            raise ValueError("validated structural rules require human review")

        if self.update_policy.rule_class == RuleClass.PARAMETER and self.change_class == ChangeClass.STRUCTURAL_CHANGE:
            raise ValueError("parameter rule cannot claim structural change class")

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
    lifecycle: ReleaseLifecycle = Field(default_factory=ReleaseLifecycle)
    last_good_policy: LastGoodPolicy = Field(default_factory=LastGoodPolicy)
    rules: list[FiscalRuleV1] = Field(min_length=1)
    notes: str | None = None

    @field_validator("generated_at_utc")
    @classmethod
    def generated_at_is_utc(cls, value):
        if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
            raise ValueError("generated_at_utc must be UTC")
        return value

    @field_validator("source_registry_version", "rule_inventory_version")
    @classmethod
    def semantic_versions(cls, value):
        if not SEMVER_RE.fullmatch(value):
            raise ValueError("version must be SemVer")
        return value

    @model_validator(mode="after")
    def invariants(self):
        ids = {rule.rule_id for rule in self.rules}
        for rule in self.rules:
            missing = [dependency for dependency in rule.dependencies if dependency not in ids]
            if missing:
                raise ValueError(f"{rule.rule_id} has missing dependencies: {missing}")

        for index, left in enumerate(self.rules):
            for right in self.rules[index + 1 :]:
                if (
                    left.rule_id == right.rule_id
                    and set(left.contexts) & set(right.contexts)
                    and self._overlap(left.vigency, right.vigency)
                ):
                    raise ValueError(
                        f"overlapping rule windows for {left.rule_id} in shared context"
                    )

        if self.status in {ContractStatus.VALIDATED, ContractStatus.PUBLISHED}:
            for rule in self.rules:
                if rule.quality.status != QualityStatus.VALIDATED:
                    raise ValueError(
                        "validated/published contract requires all rules VALIDATED"
                    )
                if not any(
                    evidence.status == SourceObservationStatus.AVAILABLE
                    and evidence.snapshot_sha256
                    for evidence in rule.provenance
                ):
                    raise ValueError(
                        f"{rule.rule_id} requires at least one available hashed snapshot"
                    )

        lifecycle = self.lifecycle
        if lifecycle.previous_release_id == self.release_id:
            raise ValueError("release cannot name itself as previous_release_id")
        if lifecycle.superseded_by_release_id == self.release_id:
            raise ValueError("release cannot supersede itself")
        if lifecycle.approval_mode is not None and not lifecycle.approval_reference:
            raise ValueError("approval_mode requires approval_reference")
        if lifecycle.validated_at_utc and lifecycle.published_at_utc:
            if lifecycle.published_at_utc < lifecycle.validated_at_utc:
                raise ValueError("published_at_utc cannot precede validated_at_utc")
        if lifecycle.published_at_utc and lifecycle.superseded_at_utc:
            if lifecycle.superseded_at_utc < lifecycle.published_at_utc:
                raise ValueError("superseded_at_utc cannot precede published_at_utc")

        if self.status == ContractStatus.DRAFT:
            if any(
                [
                    lifecycle.validated_at_utc,
                    lifecycle.published_at_utc,
                    lifecycle.superseded_at_utc,
                    lifecycle.superseded_by_release_id,
                ]
            ):
                raise ValueError("DRAFT release cannot carry validation/publication lifecycle")
        elif self.status == ContractStatus.CANDIDATE:
            if any(
                [
                    lifecycle.published_at_utc,
                    lifecycle.superseded_at_utc,
                    lifecycle.superseded_by_release_id,
                ]
            ):
                raise ValueError("CANDIDATE release cannot be published or superseded")
        elif self.status == ContractStatus.VALIDATED:
            if not lifecycle.validated_at_utc:
                raise ValueError("VALIDATED release requires validated_at_utc")
            if lifecycle.published_at_utc or lifecycle.superseded_at_utc:
                raise ValueError("VALIDATED release cannot already be published/superseded")
        elif self.status == ContractStatus.PUBLISHED:
            if not lifecycle.validated_at_utc or not lifecycle.published_at_utc:
                raise ValueError(
                    "PUBLISHED release requires validated_at_utc and published_at_utc"
                )
            if lifecycle.approval_mode is None:
                raise ValueError("PUBLISHED release requires approval_mode")
            if any(
                rule.update_policy.rule_class in STRUCTURAL_RULE_CLASSES
                and rule.change_class
                in {
                    ChangeClass.STRUCTURAL_CHANGE,
                    ChangeClass.RULE_ADDED,
                    ChangeClass.RULE_REMOVED,
                }
                for rule in self.rules
            ):
                if lifecycle.approval_mode != ReleaseApprovalMode.HUMAN_REVIEWED:
                    raise ValueError(
                        "release with structural changes requires HUMAN_REVIEWED approval"
                    )
        elif self.status == ContractStatus.SUPERSEDED:
            if (
                not lifecycle.validated_at_utc
                or not lifecycle.published_at_utc
                or not lifecycle.superseded_at_utc
                or not lifecycle.superseded_by_release_id
                or lifecycle.approval_mode is None
            ):
                raise ValueError(
                    "SUPERSEDED release requires prior validation/publication and successor metadata"
                )
        elif self.status == ContractStatus.BLOCKED:
            if not lifecycle.block_reasons:
                raise ValueError("BLOCKED release requires block_reasons")
            if lifecycle.published_at_utc or lifecycle.superseded_at_utc:
                raise ValueError("BLOCKED release cannot be published/superseded")

        return self

    @staticmethod
    def _overlap(left, right):
        return left.effective_from <= (right.effective_until or date.max) and (
            right.effective_from <= (left.effective_until or date.max)
        )

    def select_rule(
        self,
        rule_id: str,
        target_date: date,
        context: AssessmentContext,
        *,
        require_validated: bool = True,
    ) -> FiscalRuleV1:
        matches = [
            rule
            for rule in self.rules
            if rule.rule_id == rule_id
            and context in rule.contexts
            and rule.vigency.covers(target_date)
        ]
        if len(matches) != 1:
            raise RuleSelectionError(
                f"expected exactly one rule for {rule_id}/{context.value}/{target_date}; "
                f"found {len(matches)}"
            )
        rule = matches[0]
        if require_validated and rule.quality.status != QualityStatus.VALIDATED:
            raise RuleSelectionError(f"rule {rule_id} is not validated")
        return rule

    def competence_basis_for(
        self, rule_id: str, target_date: date, context: AssessmentContext
    ) -> CompetenceBasis:
        rule = self.select_rule(rule_id, target_date, context)
        return rule.competence.basis_for(context)

    def assert_consumable(self) -> None:
        if self.status != ContractStatus.PUBLISHED:
            raise ContractNotConsumableError(
                f"contract status {self.status.value} is not consumable; expected PUBLISHED"
            )

    def can_use_last_good(
        self, rule: FiscalRuleV1, target_date: date, *, known_successor: bool
    ) -> bool:
        policy = self.last_good_policy
        return bool(
            policy.allow_when_source_unavailable
            and (
                not policy.require_validated_rule
                or rule.quality.status == QualityStatus.VALIDATED
            )
            and (
                not policy.require_within_effective_window
                or rule.vigency.covers(target_date)
            )
            and (not policy.require_no_known_successor or not known_successor)
        )

    def canonical_json(self) -> str:
        return json.dumps(
            self.model_dump(mode="json", exclude_none=True),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    def content_sha256(self) -> str:
        return sha256(self.canonical_json().encode()).hexdigest()
