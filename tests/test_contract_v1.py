from __future__ import annotations

from copy import deepcopy
from datetime import date
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from sanida_fiscal.contract_v1 import (
    AssessmentContext,
    ContractCompatibilityError,
    ContractNotConsumableError,
    FiscalContractV1,
    QualityStatus,
    ReleaseTransitionError,
    RuleSelectionError,
    semver_bump,
)
from sanida_fiscal.types_v1 import (
    CompetencePolicy,
    EntitlementBandsPayload,
    EvidenceObservation,
    PeriodStatePayload,
    POLICY_ASSERTIONS_BY_KIND,
    PolicyKind,
    PolicyPayload,
    VersionBump,
    ScopeDeclarationPayload,
    SourceObservationStatus,
    UpdatePolicy,
    CodeEligibilityPayload,
    RULE_PAYLOAD_TYPES,
)

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"
COVERAGE = ROOT / "docs" / "contract-coverage-v1.json"


def load_example() -> dict:
    return json.loads(EXAMPLE.read_text(encoding="utf-8"))


def load_coverage() -> dict:
    return json.loads(COVERAGE.read_text(encoding="utf-8"))


def add_hashes(data: dict) -> None:
    fake_hash = "a" * 64
    for rule in data["rules"]:
        available = next(
            evidence
            for evidence in rule["provenance"]
            if evidence["status"] == "AVAILABLE"
        )
        available["snapshot_sha256"] = fake_hash
        available["snapshot_path"] = (
            f"raw/{rule['rule_id'].replace('.', '_')}/snapshot.bin"
        )


def set_canonical_release_id(data: dict) -> None:
    candidate = deepcopy(data)
    candidate["status"] = "CANDIDATE"
    candidate["lifecycle"] = {}
    candidate["release_id"] = "candidate-for-hash"
    contract = FiscalContractV1.model_validate(candidate)
    data["release_id"] = contract.expected_release_id()


def test_example_validates_and_materializes_every_payload_family() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    assert contract.schema_version == "1.1.0"
    assert contract.contract_id == "br.sanida.fiscal"
    assert len(contract.rules) >= 18
    assert {rule.payload.type for rule in contract.rules} == RULE_PAYLOAD_TYPES


def test_phase1_inventory_coverage_is_declared_as_32_rules() -> None:
    coverage = load_coverage()
    assert coverage["rule_count"] == 32
    assert len(coverage["rules"]) == 32
    assert {item["representation_status"] for item in coverage["rules"]} == {"covered"}


def test_vacation_period_rule_carries_proportional_15_day_semantics() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = contract.select_rule(
        "vacation.acquisition_period",
        date(2026, 3, 31),
        AssessmentContext.TERMINATION,
    )
    assert rule.rule_version == "1.1.0"
    assert (
        rule.payload.proportional_accrual_method
        == "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    )
    assert rule.payload.proportional_qualifying_days == 15


def test_decimal_values_round_trip_as_strings_in_json_mode() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    dumped = contract.model_dump(mode="json")
    irrf = next(
        rule
        for rule in dumped["rules"]
        if rule["rule_id"] == "irrf.monthly.progressive_table"
    )
    assert irrf["payload"]["brackets"][0]["upper_bound"] == "2428.80"
    assert irrf["payload"]["brackets"][1]["rate"] == "0.075"


def test_canonical_serialization_is_deterministic() -> None:
    first = FiscalContractV1.model_validate(load_example())
    second = FiscalContractV1.model_validate(
        json.loads(json.dumps(load_example(), sort_keys=True))
    )
    assert first.canonical_json() == second.canonical_json()
    assert first.content_sha256() == second.content_sha256()


def test_a01_semantic_target_is_not_irrf_base() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = contract.select_rule(
        "irrf.reduction.2026",
        date(2026, 6, 1),
        AssessmentContext.MONTHLY,
    )
    assert (
        rule.applies_to
        == "taxable_income_subject_to_monthly_incidence_before_irrf_deductions"
    )
    assert rule.payload.input_semantic == rule.applies_to
    assert rule.applies_to != "irrf_tax_base_after_allowed_deductions"


def test_thirteenth_reduction_is_separate_context() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = contract.select_rule(
        "thirteenth.irrf.reduction.2026",
        date(2026, 12, 20),
        AssessmentContext.THIRTEENTH,
    )
    assert (
        rule.payload.input_semantic
        == "thirteenth_taxable_income_before_irrf_deductions"
    )


def test_rule_selection_fails_outside_vigency() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    with pytest.raises(RuleSelectionError):
        contract.select_rule(
            "irrf.reduction.2026",
            date(2027, 1, 1),
            AssessmentContext.MONTHLY,
        )


def test_candidate_contract_is_not_consumable() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    with pytest.raises(ContractNotConsumableError):
        contract.assert_consumable()


def test_published_contract_requires_hashed_available_evidence() -> None:
    data = load_example()
    data["status"] = "PUBLISHED"
    data["lifecycle"] = {
        "validated_at_utc": "2026-09-13T13:00:00Z",
        "published_at_utc": "2026-09-13T13:05:00Z",
        "approval_mode": "HUMAN_REVIEWED",
        "approval_reference": "review:phase2-test",
    }
    with pytest.raises(ValidationError, match="hashed snapshot"):
        FiscalContractV1.model_validate(data)


def test_published_contract_with_structural_changes_requires_human_approval() -> None:
    data = load_example()
    add_hashes(data)
    set_canonical_release_id(data)
    set_canonical_release_id(data)
    data["status"] = "PUBLISHED"
    data["lifecycle"] = {
        "validated_at_utc": "2026-09-13T13:00:00Z",
        "published_at_utc": "2026-09-13T13:05:00Z",
        "approval_mode": "AUTO_VALIDATED",
        "approval_reference": "ci:synthetic",
    }
    with pytest.raises(ValidationError, match="HUMAN_REVIEWED"):
        FiscalContractV1.model_validate(data)


def test_published_contract_with_human_approval_can_validate() -> None:
    data = load_example()
    add_hashes(data)
    set_canonical_release_id(data)
    set_canonical_release_id(data)
    data["status"] = "PUBLISHED"
    data["lifecycle"] = {
        "validated_at_utc": "2026-09-13T13:00:00Z",
        "published_at_utc": "2026-09-13T13:05:00Z",
        "approval_mode": "HUMAN_REVIEWED",
        "approval_reference": "review:synthetic",
    }
    contract = FiscalContractV1.model_validate(data)
    contract.assert_consumable()


def test_candidate_cannot_claim_published_timestamp() -> None:
    data = load_example()
    data["lifecycle"]["published_at_utc"] = "2026-09-13T13:05:00Z"
    with pytest.raises(ValidationError, match="CANDIDATE"):
        FiscalContractV1.model_validate(data)


def test_blocked_release_requires_reason() -> None:
    data = load_example()
    data["status"] = "BLOCKED"
    data["lifecycle"] = {}
    with pytest.raises(ValidationError, match="block_reasons"):
        FiscalContractV1.model_validate(data)


def test_superseded_is_not_a_mutable_release_status() -> None:
    data = load_example()
    data["status"] = "SUPERSEDED"
    with pytest.raises(ValidationError):
        FiscalContractV1.model_validate(data)


def test_structural_rule_cannot_auto_publish() -> None:
    data = load_example()
    rule = next(
        rule
        for rule in data["rules"]
        if rule["rule_id"] == "termination.reason_scope"
    )
    rule["update_policy"]["auto_publish"] = True
    with pytest.raises(ValidationError, match="structural rules cannot auto-publish"):
        FiscalContractV1.model_validate(data)


@pytest.mark.parametrize(
    "rule_class",
    ["structural_rule", "product_scope_rule", "technical_contract_rule"],
)
def test_every_structural_rule_class_requires_review(rule_class: str) -> None:
    with pytest.raises(ValidationError, match="structural rules require human review"):
        UpdatePolicy(
            rule_class=rule_class,
            auto_publish=False,
            structural_review_required=False,
        )


def test_validated_structural_rule_requires_human_review() -> None:
    data = load_example()
    rule = next(
        rule
        for rule in data["rules"]
        if rule["rule_id"] == "termination.reason_scope"
    )
    rule["quality"]["reviewed_by_human"] = False
    with pytest.raises(ValidationError, match="structural rules require human review"):
        FiscalContractV1.model_validate(data)


def test_overlapping_same_rule_and_context_is_rejected() -> None:
    data = load_example()
    original = next(
        rule
        for rule in data["rules"]
        if rule["rule_id"] == "irrf.reduction.2026"
    )
    duplicate = deepcopy(original)
    duplicate["rule_version"] = "1.0.1"
    data["rules"].append(duplicate)
    with pytest.raises(ValidationError, match="overlapping rule windows"):
        FiscalContractV1.model_validate(data)


def test_missing_dependency_is_rejected() -> None:
    data = load_example()
    rule = next(
        rule
        for rule in data["rules"]
        if rule["rule_id"] == "vacation.abono_pecuniario"
    )
    rule["dependencies"] = ["rule.that.does.not.exist"]
    with pytest.raises(ValidationError, match="missing dependencies"):
        FiscalContractV1.model_validate(data)


def test_rule_cannot_depend_on_itself() -> None:
    data = load_example()
    rule = next(
        rule
        for rule in data["rules"]
        if rule["rule_id"] == "vacation.abono_pecuniario"
    )
    rule["dependencies"] = ["vacation.abono_pecuniario"]
    with pytest.raises(ValidationError, match="depend on itself"):
        FiscalContractV1.model_validate(data)


def test_last_good_requires_current_vigency_and_validated_rule() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = contract.select_rule(
        "irrf.reduction.2026",
        date(2026, 6, 1),
        AssessmentContext.MONTHLY,
    )
    assert contract.can_use_last_good(
        rule, date(2026, 6, 1), known_successor=False
    )
    assert not contract.can_use_last_good(
        rule, date(2027, 1, 1), known_successor=False
    )
    assert not contract.can_use_last_good(
        rule, date(2026, 6, 1), known_successor=True
    )


def test_last_good_rejects_unvalidated_rule() -> None:
    data = load_example()
    rule_data = next(
        rule
        for rule in data["rules"]
        if rule["rule_id"] == "irrf.reduction.2026"
    )
    rule_data["quality"]["status"] = "UNVALIDATED"
    contract = FiscalContractV1.model_validate(data)
    rule = next(
        rule
        for rule in contract.rules
        if rule.rule_id == "irrf.reduction.2026"
    )
    assert rule.quality.status == QualityStatus.UNVALIDATED
    assert not contract.can_use_last_good(
        rule, date(2026, 6, 1), known_successor=False
    )


def test_h29_scope_is_exactly_four_supported_codes() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = next(
        rule
        for rule in contract.rules
        if rule.rule_id == "termination.reason_scope"
    )
    matrix = {row.code: row.entitlements for row in rule.payload.rows}
    assert set(matrix) == {"01", "02", "07", "33"}
    assert matrix["01"].thirteenth_proportional is False
    assert matrix["01"].vacation_proportional is False
    assert matrix["02"].thirteenth_proportional is True
    assert matrix["07"].vacation_proportional is True
    assert matrix["33"].vacation_proportional is True


def test_abono_principal_and_third_have_distinct_ir_treatment() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    principal = next(
        rule
        for rule in contract.rules
        if rule.rule_id == "vacation.abono.ir_exemption"
    )
    third = next(
        rule
        for rule in contract.rules
        if rule.rule_id == "vacation.abono_constitutional_third.ir_incidence"
    )
    assert principal.payload.components[0].irrf.value == "no"
    assert third.payload.components[0].irrf.value == "yes"
    assert principal.payload.components[0].social_security.value == "no"
    assert third.payload.components[0].social_security.value == "no"


def test_competence_override_cannot_target_undeclared_context() -> None:
    data = load_example()
    rule = next(
        rule
        for rule in data["rules"]
        if rule["rule_id"] == "vacation.abono_pecuniario"
    )
    rule["competence"]["context_overrides"] = {"termination": "termination_date"}
    with pytest.raises(ValidationError, match="context not declared"):
        FiscalContractV1.model_validate(data)


def test_rule_specific_competence_requires_description() -> None:
    with pytest.raises(ValidationError, match="requires rule_specific_key"):
        CompetencePolicy(basis="rule_specific")


def test_parser_id_and_version_must_be_paired() -> None:
    with pytest.raises(ValidationError, match="provided together"):
        EvidenceObservation(
            source_id="X",
            role="official_operational",
            observed_at_utc="2026-09-13T12:00:00Z",
            status="AVAILABLE",
            parser_id="parser-x",
        )


def test_unavailable_evidence_cannot_claim_snapshot() -> None:
    with pytest.raises(ValidationError, match="cannot claim a snapshot"):
        EvidenceObservation(
            source_id="X",
            role="official_operational",
            observed_at_utc="2026-09-13T12:00:00Z",
            status=SourceObservationStatus.UNAVAILABLE,
            snapshot_sha256="a" * 64,
        )


def test_snapshot_path_requires_hash() -> None:
    with pytest.raises(ValidationError, match="requires snapshot_sha256"):
        EvidenceObservation(
            source_id="X",
            role="official_operational",
            observed_at_utc="2026-09-13T12:00:00Z",
            status="AVAILABLE",
            snapshot_path="raw/x.html",
        )


@pytest.mark.parametrize("kind", [kind.value for kind in PolicyKind])
def test_every_policy_kind_is_schema_expressible(kind: str) -> None:
    policy_kind = PolicyKind(kind)
    payload = PolicyPayload(
        policy_kind=policy_kind,
        assertions=list(POLICY_ASSERTIONS_BY_KIND[policy_kind]),
    )
    assert payload.type == "policy"


def test_entitlement_bands_reject_overlap() -> None:
    with pytest.raises(ValidationError, match="overlap"):
        EntitlementBandsPayload(
            bands=[
                {"min_absences": 0, "max_absences": 5, "entitled_days": 30},
                {"min_absences": 5, "max_absences": 14, "entitled_days": 24},
            ]
        )


def test_code_eligibility_rejects_duplicate_or_overlap() -> None:
    with pytest.raises(ValidationError, match="unique and disjoint"):
        CodeEligibilityPayload(
            eligible_codes=["02", "07"],
            ineligible_codes=["07"],
        )


def test_scope_declaration_rejects_same_item_in_and_out() -> None:
    with pytest.raises(ValidationError, match="disjoint"):
        ScopeDeclarationPayload(
            promise="partial_estimate",
            included_items=["salary_balance"],
            excluded_items=["salary_balance"],
            require_user_disclosure=True,
        )


def test_period_state_requires_both_canonical_states() -> None:
    with pytest.raises(ValidationError, match="both canonical states"):
        PeriodStatePayload(
            states=[
                "acquired_within_concession_period",
                "acquired_within_concession_period",
            ]
        )


def test_unknown_field_is_rejected() -> None:
    data = load_example()
    data["invented_field"] = True
    with pytest.raises(ValidationError):
        FiscalContractV1.model_validate(data)



def test_versioning_policy_is_frozen_and_exact_for_v1() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    assert contract.versioning_policy.schema_versioning == "semver"
    assert contract.versioning_policy.contract_api_versioning == "semver"
    assert contract.versioning_policy.rule_versioning == "semver"
    assert contract.versioning_policy.release_id_strategy == "content_addressed_sha256"
    assert contract.versioning_policy.schema_compatibility == "exact"
    assert contract.versioning_policy.contract_api_compatibility == "exact"
    assert contract.versioning_policy.forward_compatibility == "not_assumed"


def test_semver_bump_classification() -> None:
    assert semver_bump("1.0.0", "2.0.0") == VersionBump.MAJOR
    assert semver_bump("1.0.0", "1.1.0") == VersionBump.MINOR
    assert semver_bump("1.0.0", "1.0.1") == VersionBump.PATCH
    assert semver_bump("1.0.0", "1.0.0") is None
    with pytest.raises(ValueError, match="backwards"):
        semver_bump("1.1.0", "1.0.9")


def test_reader_compatibility_is_exact_and_unknown_versions_fail() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    contract.assert_reader_compatible(
        consumer="H26", schema_version="1.1.0", contract_api_version="1.1.0"
    )
    with pytest.raises(ContractCompatibilityError, match="schema mismatch"):
        contract.assert_reader_compatible(
            consumer="H26", schema_version="1.1.0", contract_api_version="1.0.0"
        )
    with pytest.raises(ContractCompatibilityError, match="contract API mismatch"):
        contract.assert_reader_compatible(
            consumer="H26", schema_version="1.0.0", contract_api_version="1.1.0"
        )


def test_validated_release_id_is_content_addressed() -> None:
    data = load_example()
    add_hashes(data)
    data["status"] = "VALIDATED"
    data["lifecycle"] = {"validated_at_utc": "2026-09-13T13:00:00Z"}
    with pytest.raises(ValidationError, match="content-addressed"):
        FiscalContractV1.model_validate(data)
    set_canonical_release_id(data)
    contract = FiscalContractV1.model_validate(data)
    assert contract.release_id == contract.expected_release_id()
    assert contract.release_id.startswith("fiscal-v1-sha256-")


def test_published_release_artifact_is_immutable() -> None:
    data = load_example()
    add_hashes(data)
    set_canonical_release_id(data)
    data["status"] = "PUBLISHED"
    data["lifecycle"] = {
        "validated_at_utc": "2026-09-13T13:00:00Z",
        "published_at_utc": "2026-09-13T13:05:00Z",
        "approval_mode": "HUMAN_REVIEWED",
        "approval_reference": "review:immutability",
    }
    published = FiscalContractV1.model_validate(data)
    identical = FiscalContractV1.model_validate(deepcopy(data))
    identical.assert_published_immutable_against(published)

    changed = deepcopy(data)
    changed["notes"] = "mutated after publication"
    changed_contract = FiscalContractV1.model_validate(changed)
    with pytest.raises(ReleaseTransitionError, match="immutable"):
        changed_contract.assert_published_immutable_against(published)


def test_supersession_is_declared_by_successor_without_mutating_predecessor() -> None:
    data = load_example()
    add_hashes(data)
    set_canonical_release_id(data)
    data["status"] = "PUBLISHED"
    data["lifecycle"] = {
        "validated_at_utc": "2026-09-13T13:00:00Z",
        "published_at_utc": "2026-09-13T13:05:00Z",
        "approval_mode": "HUMAN_REVIEWED",
        "approval_reference": "review:predecessor",
    }
    predecessor = FiscalContractV1.model_validate(data)

    successor_data = load_example()
    successor_data["release_id"] = "candidate-successor"
    successor_data["supersedes_release_id"] = predecessor.release_id
    successor = FiscalContractV1.model_validate(successor_data)
    successor.assert_supersedes(predecessor)
    assert predecessor.status.value == "PUBLISHED"


def test_progressive_table_method_is_explicit() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = next(rule for rule in contract.rules if rule.rule_id == "irrf.monthly.progressive_table")
    assert rule.payload.calculation_method.value == "rate_times_base_minus_deduction"


def test_affine_reduction_formula_is_explicit() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = next(rule for rule in contract.rules if rule.rule_id == "irrf.reduction.2026")
    assert rule.payload.full_relief_behavior == "max_reduction"
    assert rule.payload.phaseout_formula == "intercept_minus_slope_times_input"
    assert rule.payload.above_phaseout_behavior == "zero"


def test_rule_specific_competence_uses_typed_key() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = next(rule for rule in contract.rules if rule.rule_id == "thirteenth.accrual.twelfths")
    assert (
        rule.competence.rule_specific_key.value
        == "thirteenth_accrual_reference_year_or_termination_period"
    )
