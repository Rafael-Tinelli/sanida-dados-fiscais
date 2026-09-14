from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal, Mapping, Union

from pydantic import Field, field_validator, model_validator

from .contract_v1 import FiscalContractV1, FiscalRuleV1, SEMVER_RE
from .types_v1 import (
    ConsumerCompatibility,
    EvidenceObservation,
    RuleClass,
    SourceObservationStatus,
    StrictModel,
)


class GovernanceEvidenceObservation(StrictModel):
    """Hash-addressed evidence for internal computational-governance rules.

    Legal/fiscal rules remain restricted to official external source evidence from
    ``docs/source-registry-v1.json``. This separate shape exists only for the two
    ``technical.*`` rules whose authority is the versioned contract/engine itself.
    It must never be used as legal or fiscal authority.
    """

    source_id: str = Field(min_length=1)
    role: Literal["internal_governance"] = "internal_governance"
    observed_at_utc: datetime
    status: Literal[SourceObservationStatus.AVAILABLE] = SourceObservationStatus.AVAILABLE
    retrieval_method: Literal["repository_snapshot"] = "repository_snapshot"
    snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    snapshot_path: str = Field(min_length=1)
    repository_paths: list[str] = Field(min_length=1)
    locator: str | None = None
    parser_id: None = None
    parser_version: None = None

    @field_validator("observed_at_utc")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
            raise ValueError("governance evidence timestamp must be UTC")
        return value

    @field_validator("repository_paths")
    @classmethod
    def unique_repository_paths(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate repository_paths in governance evidence")
        if any(not item or item.startswith("/") or ".." in item.split("/") for item in value):
            raise ValueError("governance repository_paths must be relative and confined")
        return value


RuleEvidenceV12 = Union[EvidenceObservation, GovernanceEvidenceObservation]


class ConsumerCompatibilityV12(ConsumerCompatibility):
    schema_version: Literal["1.2.0"] = "1.2.0"
    contract_api_version: Literal["1.2.0"] = "1.2.0"


class FiscalRuleV12(FiscalRuleV1):
    provenance: list[RuleEvidenceV12] = Field(min_length=1)

    @model_validator(mode="after")
    def governance_evidence_boundary(self):
        internal = [
            evidence
            for evidence in self.provenance
            if isinstance(evidence, GovernanceEvidenceObservation)
        ]
        external = [
            evidence
            for evidence in self.provenance
            if isinstance(evidence, EvidenceObservation)
        ]
        is_technical = self.update_policy.rule_class == RuleClass.TECHNICAL_CONTRACT_RULE

        if is_technical:
            if not internal:
                raise ValueError("technical contract rule requires internal governance evidence")
            if external:
                raise ValueError(
                    "technical contract rule must not misattribute external official evidence as its authority"
                )
        elif internal:
            raise ValueError(
                "legal/fiscal rules cannot use internal governance evidence as authority"
            )
        return self


class FiscalContractV12(FiscalContractV1):
    """Contract v1.2 amendment discovered by the Phase 5 publication gate.

    v1.1 remains parseable and historically immutable. v1.2 adds a distinct,
    hash-addressed internal-governance evidence lane so the two ``technical.*``
    rules can be published without fabricating government provenance.
    """

    schema_version: Literal["1.2.0"] = "1.2.0"
    governance_source_registry_version: str
    consumer_compatibility: ConsumerCompatibilityV12
    rules: list[FiscalRuleV12] = Field(min_length=1)

    @field_validator("governance_source_registry_version")
    @classmethod
    def governance_registry_semver(cls, value: str) -> str:
        if not SEMVER_RE.fullmatch(value):
            raise ValueError("governance_source_registry_version must be SemVer")
        return value

    def immutable_payload_dict(self) -> dict:
        payload = super().immutable_payload_dict()
        payload["governance_source_registry_version"] = (
            self.governance_source_registry_version
        )
        return payload


def parse_fiscal_contract(data: Mapping[str, Any]) -> FiscalContractV1:
    schema_version = data.get("schema_version")
    if schema_version == "1.1.0":
        return FiscalContractV1.model_validate(data)
    if schema_version == "1.2.0":
        return FiscalContractV12.model_validate(data)
    raise ValueError(f"unsupported fiscal contract schema_version: {schema_version}")
