from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

from sanida_fiscal.contract_v1 import FiscalContractV1
from sanida_fiscal.semantic_diff_v1 import (
    PromotionOutcome,
    assess_promotion,
    diff_contracts,
)
from sanida_fiscal.types_v1 import ChangeClass


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"


def _example() -> dict:
    return json.loads(EXAMPLE.read_text(encoding="utf-8"))


def _hash_all_available_evidence(data: dict) -> None:
    for index, rule in enumerate(data["rules"], start=1):
        available = next(item for item in rule["provenance"] if item["status"] == "AVAILABLE")
        available["snapshot_sha256"] = f"{index:064x}"[-64:]
        available["snapshot_path"] = f"authority/{rule['rule_id'].replace('.', '_')}.bin"


def _published_baseline() -> FiscalContractV1:
    data = _example()
    _hash_all_available_evidence(data)
    data["status"] = "CANDIDATE"
    data["release_id"] = "candidate-for-phase5-test"
    data["lifecycle"] = {}
    candidate = FiscalContractV1.model_validate(data)
    release_id = candidate.expected_release_id()
    data["status"] = "PUBLISHED"
    data["release_id"] = release_id
    data["lifecycle"] = {
        "validated_at_utc": "2026-09-13T12:30:00Z",
        "published_at_utc": "2026-09-13T12:31:00Z",
        "approval_mode": "HUMAN_REVIEWED",
        "approval_reference": "phase5-test:baseline",
    }
    return FiscalContractV1.model_validate(data)


def _successor_data(previous: FiscalContractV1) -> dict:
    data = previous.model_dump(mode="json", exclude_none=True)
    data["status"] = "CANDIDATE"
    data["release_id"] = "candidate-next"
    data["generated_at_utc"] = "2026-09-14T02:30:00Z"
    data["supersedes_release_id"] = previous.release_id
    data["lifecycle"] = {}
    return data


def _rule(data: dict, rule_id: str) -> dict:
    return next(rule for rule in data["rules"] if rule["rule_id"] == rule_id)


def _parser_back(rule: dict, *, digest: str = "b") -> None:
    evidence = next(item for item in rule["provenance"] if item["status"] == "AVAILABLE")
    evidence["snapshot_sha256"] = digest * 64
    evidence["snapshot_path"] = f"runtime/{rule['rule_id'].replace('.', '_')}/{digest * 8}.bin"
    evidence["observed_at_utc"] = "2026-09-14T02:20:00Z"
    evidence["retrieval_method"] = "http_html"
    evidence["parser_id"] = "phase5_test_parser"
    evidence["parser_version"] = "1.0.0"


def test_identical_successor_requires_no_publication() -> None:
    previous = _published_baseline()
    candidate = FiscalContractV1.model_validate(_successor_data(previous))
    assessment = assess_promotion(previous, candidate)
    assert assessment.outcome == PromotionOutcome.NO_PUBLISH_REQUIRED
    assert assessment.diff.immutable_payload_changed is False
    assert not assessment.diff.change_classes


def test_numeric_parameter_change_is_classified_and_auto_publishable_when_parser_backed() -> None:
    previous = _published_baseline()
    data = _successor_data(previous)
    rule = _rule(data, "irrf.monthly.progressive_table")
    rule["payload"]["brackets"][1]["upper_bound"] = "2850.00"
    rule["rule_version"] = "1.0.1"
    rule["change_class"] = "PARAMETER_CHANGE"
    _parser_back(rule)
    candidate = FiscalContractV1.model_validate(data)
    assessment = assess_promotion(previous, candidate)
    changed = [item for item in assessment.diff.rule_diffs if item.changed]
    assert [(item.rule_id, item.change_class) for item in changed] == [
        ("irrf.monthly.progressive_table", ChangeClass.PARAMETER_CHANGE)
    ]
    assert assessment.outcome == PromotionOutcome.AUTO_PUBLISH_ALLOWED


def test_parameter_change_without_parser_backing_requires_review() -> None:
    previous = _published_baseline()
    data = _successor_data(previous)
    rule = _rule(data, "irrf.dependent_deduction")
    rule["payload"]["value"] = "200.00"
    rule["rule_version"] = "1.0.1"
    rule["change_class"] = "PARAMETER_CHANGE"
    candidate = FiscalContractV1.model_validate(data)
    assessment = assess_promotion(previous, candidate)
    assert assessment.outcome == PromotionOutcome.REVIEW_REQUIRED
    assert any("parser-backed" in reason for reason in assessment.reasons)


def test_semantic_string_change_is_structural_even_inside_parameterizable_payload() -> None:
    previous = _published_baseline()
    data = _successor_data(previous)
    rule = _rule(data, "irrf.reduction.2026")
    rule["payload"]["input_semantic"] = "some_other_income_semantic"
    rule["rule_version"] = "2.0.0"
    rule["change_class"] = "STRUCTURAL_CHANGE"
    candidate = FiscalContractV1.model_validate(data)
    assessment = assess_promotion(previous, candidate)
    changed = next(item for item in assessment.diff.rule_diffs if item.changed)
    assert changed.change_class == ChangeClass.STRUCTURAL_CHANGE
    assert assessment.outcome == PromotionOutcome.REVIEW_REQUIRED


def test_effective_date_change_is_separate_and_never_auto_published() -> None:
    previous = _published_baseline()
    data = _successor_data(previous)
    rule = _rule(data, "irrf.monthly.progressive_table")
    rule["vigency"]["effective_from"] = "2026-02-01"
    rule["rule_version"] = "1.0.1"
    rule["change_class"] = "EFFECTIVE_DATE_CHANGE"
    _parser_back(rule)
    candidate = FiscalContractV1.model_validate(data)
    assessment = assess_promotion(previous, candidate)
    changed = next(item for item in assessment.diff.rule_diffs if item.changed)
    assert changed.change_class == ChangeClass.EFFECTIVE_DATE_CHANGE
    assert assessment.outcome == PromotionOutcome.REVIEW_REQUIRED


def test_structural_rule_source_refresh_requires_human_review() -> None:
    previous = _published_baseline()
    data = _successor_data(previous)
    rule = _rule(data, "thirteenth.accrual.twelfths")
    rule["rule_version"] = "1.0.1"
    rule["change_class"] = "SOURCE_REFRESH_NO_CHANGE"
    _parser_back(rule)
    candidate = FiscalContractV1.model_validate(data)
    assessment = assess_promotion(previous, candidate)
    assert assessment.outcome == PromotionOutcome.REVIEW_REQUIRED
    assert any("structural rule" in reason for reason in assessment.reasons)


def test_declared_change_class_must_match_computed_class() -> None:
    previous = _published_baseline()
    data = _successor_data(previous)
    rule = _rule(data, "irrf.monthly.progressive_table")
    rule["payload"]["brackets"][1]["upper_bound"] = "2850.00"
    rule["rule_version"] = "1.0.1"
    rule["change_class"] = "SOURCE_REFRESH_NO_CHANGE"
    _parser_back(rule)
    candidate = FiscalContractV1.model_validate(data)
    assessment = assess_promotion(previous, candidate)
    assert assessment.outcome == PromotionOutcome.BLOCKED
    assert any("does not match computed PARAMETER_CHANGE" in reason for reason in assessment.reasons)


def test_parameter_change_requires_patch_rule_version() -> None:
    previous = _published_baseline()
    data = _successor_data(previous)
    rule = _rule(data, "irrf.monthly.progressive_table")
    rule["payload"]["brackets"][1]["upper_bound"] = "2850.00"
    rule["change_class"] = "PARAMETER_CHANGE"
    _parser_back(rule)
    candidate = FiscalContractV1.model_validate(data)
    assessment = assess_promotion(previous, candidate)
    assert assessment.outcome == PromotionOutcome.BLOCKED
    assert any("requires PATCH rule_version bump" in reason for reason in assessment.reasons)


def test_structural_change_requires_minor_or_major_bump() -> None:
    previous = _published_baseline()
    data = _successor_data(previous)
    rule = _rule(data, "irrf.reduction.2026")
    rule["applies_to"] = "different_semantic_target"
    rule["rule_version"] = "1.0.1"
    rule["change_class"] = "STRUCTURAL_CHANGE"
    candidate = FiscalContractV1.model_validate(data)
    assessment = assess_promotion(previous, candidate)
    assert assessment.outcome == PromotionOutcome.BLOCKED
    assert any("STRUCTURAL_CHANGE requires MINOR or MAJOR" in reason for reason in assessment.reasons)


def test_contract_level_governance_change_requires_review() -> None:
    previous = _published_baseline()
    data = _successor_data(previous)
    data["last_good_policy"]["allow_when_source_unavailable"] = False
    candidate = FiscalContractV1.model_validate(data)
    assessment = assess_promotion(previous, candidate)
    assert assessment.outcome == PromotionOutcome.REVIEW_REQUIRED
    assert "last_good_policy.allow_when_source_unavailable" in assessment.diff.contract_changed_paths


def test_bootstrap_release_is_human_review_only() -> None:
    data = _example()
    _hash_all_available_evidence(data)
    for rule in data["rules"]:
        rule["change_class"] = "RULE_ADDED"
    candidate = FiscalContractV1.model_validate(data)
    assessment = assess_promotion(None, candidate)
    assert assessment.outcome == PromotionOutcome.REVIEW_REQUIRED, assessment.reasons
    assert assessment.diff.change_classes == {ChangeClass.RULE_ADDED}


def test_missing_hashed_evidence_blocks_promotion() -> None:
    previous = _published_baseline()
    data = _successor_data(previous)
    rule = _rule(data, "irrf.monthly.progressive_table")
    rule["payload"]["brackets"][1]["upper_bound"] = "2850.00"
    rule["rule_version"] = "1.0.1"
    rule["change_class"] = "PARAMETER_CHANGE"
    evidence = next(item for item in rule["provenance"] if item["status"] == "AVAILABLE")
    evidence.pop("snapshot_sha256", None)
    evidence.pop("snapshot_path", None)
    evidence["parser_id"] = "phase5_test_parser"
    evidence["parser_version"] = "1.0.0"
    candidate = FiscalContractV1.model_validate(data)
    assessment = assess_promotion(previous, candidate)
    assert assessment.outcome == PromotionOutcome.BLOCKED
    assert any("no AVAILABLE hashed source evidence" in reason for reason in assessment.reasons)


def test_added_and_removed_rules_are_detected() -> None:
    previous = _published_baseline()
    data = _successor_data(previous)
    removed = _rule(data, "vacation.abono_pecuniario")
    data["rules"].remove(removed)
    for dependent_id in (
        "vacation.abono.ir_exemption",
        "vacation.abono_constitutional_third.ir_incidence",
    ):
        dependent = _rule(data, dependent_id)
        dependent["dependencies"] = []
        dependent["rule_version"] = "2.0.0"
        dependent["change_class"] = "STRUCTURAL_CHANGE"
    added = deepcopy(_rule(data, "irrf.dependent_deduction"))
    added["rule_id"] = "irrf.synthetic_added_parameter"
    added["description"] = "Synthetic Phase 5 diff fixture."
    added["dependencies"] = []
    added["rule_version"] = "1.0.0"
    added["change_class"] = "RULE_ADDED"
    data["rules"].append(added)
    candidate = FiscalContractV1.model_validate(data)
    diff = diff_contracts(previous, candidate)
    changes = {(item.rule_id, item.change_class) for item in diff.rule_diffs if item.changed}
    assert ("vacation.abono_pecuniario", ChangeClass.RULE_REMOVED) in changes
    assert ("irrf.synthetic_added_parameter", ChangeClass.RULE_ADDED) in changes
