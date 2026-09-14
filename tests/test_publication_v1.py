from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from sanida_fiscal.contract_v1 import FiscalContractV1
from sanida_fiscal.publication_v1 import (
    FiscalReleaseStore,
    HumanReviewRequiredError,
    IncompleteReleaseError,
    NoPublicationRequired,
    PublicationError,
    ReleaseStoreIntegrityError,
    assert_release_inventory_complete,
    prepare_published_release,
)
from sanida_fiscal.types_v1 import ChangeClass, ReleaseApprovalMode


ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"
REAL_COVERAGE = ROOT / "docs" / "contract-coverage-v1.json"
NOW = datetime(2026, 9, 14, 3, 0, tzinfo=timezone.utc)


def _example() -> dict:
    return json.loads(EXAMPLE.read_text(encoding="utf-8"))


def _hash_all_available(data: dict) -> None:
    for index, rule in enumerate(data["rules"], start=1):
        available = next(item for item in rule["provenance"] if item["status"] == "AVAILABLE")
        available["snapshot_sha256"] = f"{index:064x}"[-64:]
        available["snapshot_path"] = f"authority/{rule['rule_id'].replace('.', '_')}.bin"


def _rule(data: dict, rule_id: str) -> dict:
    return next(item for item in data["rules"] if item["rule_id"] == rule_id)


def _parser_back(rule: dict) -> None:
    evidence = next(item for item in rule["provenance"] if item["status"] == "AVAILABLE")
    evidence["snapshot_sha256"] = "b" * 64
    evidence["snapshot_path"] = "runtime/test/parameter.json"
    evidence["observed_at_utc"] = "2026-09-14T02:55:00Z"
    evidence["retrieval_method"] = "http_html"
    evidence["parser_id"] = "phase5_test_parser"
    evidence["parser_version"] = "1.0.0"


def _complete_candidate_data() -> tuple[dict, dict]:
    data = _example()
    _hash_all_available(data)
    for rule in data["rules"]:
        rule["change_class"] = "RULE_ADDED"

    template = deepcopy(_rule(data, "irrf.dependent_deduction"))
    missing = 32 - len(data["rules"])
    assert missing == 11
    for index in range(1, missing + 1):
        synthetic = deepcopy(template)
        synthetic["rule_id"] = f"technical.phase5.synthetic_rule_{index:02d}"
        synthetic["description"] = "Synthetic complete-inventory fixture for Phase 5 publication tests."
        synthetic["dependencies"] = []
        synthetic["rule_version"] = "1.0.0"
        synthetic["change_class"] = "RULE_ADDED"
        synthetic["provenance"][0]["snapshot_sha256"] = f"{100 + index:064x}"[-64:]
        synthetic["provenance"][0]["snapshot_path"] = f"authority/synthetic_{index:02d}.bin"
        data["rules"].append(synthetic)

    ids = [rule["rule_id"] for rule in data["rules"]]
    assert len(ids) == 32 and len(set(ids)) == 32
    coverage = {
        "schema_version": "test",
        "rule_count": 32,
        "rules": [{"rule_id": rule_id} for rule_id in ids],
    }
    data["status"] = "CANDIDATE"
    data["release_id"] = "candidate-phase5-complete"
    data["lifecycle"] = {}
    return data, coverage


def _published_baseline() -> tuple[FiscalContractV1, dict]:
    data, coverage = _complete_candidate_data()
    candidate = FiscalContractV1.model_validate(data)
    data["status"] = "PUBLISHED"
    data["release_id"] = candidate.expected_release_id()
    data["lifecycle"] = {
        "validated_at_utc": "2026-09-14T02:30:00Z",
        "published_at_utc": "2026-09-14T02:31:00Z",
        "approval_mode": "HUMAN_REVIEWED",
        "approval_reference": "phase5-test:bootstrap",
    }
    return FiscalContractV1.model_validate(data), coverage


def _successor(previous: FiscalContractV1) -> dict:
    data = previous.model_dump(mode="json", exclude_none=True)
    data["status"] = "CANDIDATE"
    data["release_id"] = "candidate-successor"
    data["generated_at_utc"] = "2026-09-14T03:00:00Z"
    data["supersedes_release_id"] = previous.release_id
    data["lifecycle"] = {}
    return data


def test_real_phase2_representative_candidate_cannot_be_published_as_complete_release() -> None:
    data = _example()
    _hash_all_available(data)
    candidate = FiscalContractV1.model_validate(data)
    coverage = json.loads(REAL_COVERAGE.read_text(encoding="utf-8"))

    with pytest.raises(IncompleteReleaseError, match="missing="):
        assert_release_inventory_complete(candidate, coverage)


def test_auto_parameter_promotion_builds_consumable_published_release() -> None:
    previous, coverage = _published_baseline()
    data = _successor(previous)
    rule = _rule(data, "irrf.monthly.progressive_table")
    rule["payload"]["brackets"][1]["upper_bound"] = "2850.00"
    rule["rule_version"] = "1.0.1"
    rule["change_class"] = "PARAMETER_CHANGE"
    _parser_back(rule)
    candidate = FiscalContractV1.model_validate(data)

    published = prepare_published_release(
        previous=previous,
        candidate=candidate,
        coverage=coverage,
        published_at_utc=NOW,
    )

    assert published.status.value == "PUBLISHED"
    assert published.lifecycle.approval_mode == ReleaseApprovalMode.AUTO_VALIDATED
    assert published.supersedes_release_id == previous.release_id
    assert published.release_id == published.expected_release_id()
    assert published.release_id != candidate.expected_release_id()
    changed = next(item for item in published.rules if item.rule_id == "irrf.monthly.progressive_table")
    unchanged_structural = next(
        item for item in published.rules if item.rule_id == "thirteenth.accrual.twelfths"
    )
    assert changed.change_class == ChangeClass.PARAMETER_CHANGE
    assert unchanged_structural.change_class == ChangeClass.SOURCE_REFRESH_NO_CHANGE
    published.assert_consumable()


def test_review_required_change_cannot_publish_without_explicit_human_reference() -> None:
    previous, coverage = _published_baseline()
    data = _successor(previous)
    rule = _rule(data, "irrf.reduction.2026")
    rule["payload"]["input_semantic"] = "different_semantic_target"
    rule["rule_version"] = "2.0.0"
    rule["change_class"] = "STRUCTURAL_CHANGE"
    candidate = FiscalContractV1.model_validate(data)

    with pytest.raises(HumanReviewRequiredError):
        prepare_published_release(
            previous=previous,
            candidate=candidate,
            coverage=coverage,
            published_at_utc=NOW,
        )

    published = prepare_published_release(
        previous=previous,
        candidate=candidate,
        coverage=coverage,
        published_at_utc=NOW,
        human_approval_reference="review:phase5-test-structural",
    )
    assert published.lifecycle.approval_mode == ReleaseApprovalMode.HUMAN_REVIEWED
    assert published.release_id == published.expected_release_id()


def test_no_semantic_release_change_does_not_publish_duplicate() -> None:
    previous, coverage = _published_baseline()
    candidate = FiscalContractV1.model_validate(_successor(previous))

    with pytest.raises(NoPublicationRequired):
        prepare_published_release(
            previous=previous,
            candidate=candidate,
            coverage=coverage,
            published_at_utc=NOW,
        )


def test_successor_must_point_to_exact_current_release() -> None:
    previous, coverage = _published_baseline()
    data = _successor(previous)
    data["supersedes_release_id"] = "fiscal-v1-sha256-" + "0" * 64
    candidate = FiscalContractV1.model_validate(data)

    with pytest.raises(PublicationError, match="exact current"):
        prepare_published_release(
            previous=previous,
            candidate=candidate,
            coverage=coverage,
            published_at_utc=NOW,
        )


def test_release_store_persists_immutable_release_and_atomic_current_pointer(tmp_path: Path) -> None:
    previous, coverage = _published_baseline()
    data = _successor(previous)
    rule = _rule(data, "irrf.monthly.progressive_table")
    rule["payload"]["brackets"][1]["upper_bound"] = "2850.00"
    rule["rule_version"] = "1.0.1"
    rule["change_class"] = "PARAMETER_CHANGE"
    _parser_back(rule)
    candidate = FiscalContractV1.model_validate(data)
    published = prepare_published_release(
        previous=previous,
        candidate=candidate,
        coverage=coverage,
        published_at_utc=NOW,
    )

    store = FiscalReleaseStore(tmp_path / "fiscal")
    manifest = store.publish(published)

    assert manifest["release_id"] == published.release_id
    assert store.release_path(published.release_id).is_file()
    assert store.current_path.is_file()
    assert store.load_current().release_id == published.release_id


def test_release_store_rejects_existing_release_path_with_different_bytes(tmp_path: Path) -> None:
    previous, coverage = _published_baseline()
    data = _successor(previous)
    rule = _rule(data, "irrf.monthly.progressive_table")
    rule["payload"]["brackets"][1]["upper_bound"] = "2850.00"
    rule["rule_version"] = "1.0.1"
    rule["change_class"] = "PARAMETER_CHANGE"
    _parser_back(rule)
    published = prepare_published_release(
        previous=previous,
        candidate=FiscalContractV1.model_validate(data),
        coverage=coverage,
        published_at_utc=NOW,
    )
    store = FiscalReleaseStore(tmp_path / "fiscal")
    path = store.release_path(published.release_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"corrupted-existing-release")

    with pytest.raises(ReleaseStoreIntegrityError, match="different bytes"):
        store.publish(published)


def test_release_store_detects_post_publication_tampering(tmp_path: Path) -> None:
    previous, coverage = _published_baseline()
    data = _successor(previous)
    rule = _rule(data, "irrf.monthly.progressive_table")
    rule["payload"]["brackets"][1]["upper_bound"] = "2850.00"
    rule["rule_version"] = "1.0.1"
    rule["change_class"] = "PARAMETER_CHANGE"
    _parser_back(rule)
    published = prepare_published_release(
        previous=previous,
        candidate=FiscalContractV1.model_validate(data),
        coverage=coverage,
        published_at_utc=NOW,
    )
    store = FiscalReleaseStore(tmp_path / "fiscal")
    store.publish(published)
    store.release_path(published.release_id).write_bytes(b"tampered")

    with pytest.raises(ReleaseStoreIntegrityError, match="sha256 mismatch"):
        store.load_current()
