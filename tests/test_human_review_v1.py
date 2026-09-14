from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from sanida_fiscal.human_review_v1 import (
    StaleReviewApprovalError,
    assert_review_approval_matches,
    build_review_packet,
    compute_review_key,
    render_review_markdown,
)


NOW = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)


class FakeCompatibility:
    def model_dump(self, **_kwargs):
        return {"schema_version": "1.2.0", "contract_api_version": "1.2.0"}


class FakeContract:
    def __init__(self, data):
        self._data = deepcopy(data)
        self.schema_version = data["schema_version"]
        self.release_id = data["release_id"]
        self.rules = list(data["rules"])
        self.consumer_compatibility = FakeCompatibility()

    def model_dump(self, **_kwargs):
        return deepcopy(self._data)


def _rule(rule_id: str, *, value: str, observed_at: str, snapshot_sha: str, calculators=None):
    return {
        "rule_id": rule_id,
        "rule_version": "1.0.0",
        "domain": "payroll_fiscal",
        "calculators": calculators or ["H26"],
        "contexts": ["monthly"],
        "description": "fixture rule",
        "applies_to": "fixture_semantic",
        "applicability": [],
        "dependencies": [],
        "calculation_order": 10,
        "competence": {"basis": "payment_date"},
        "vigency": {"effective_from": "2026-01-01"},
        "payload": {"type": "scalar", "value": value, "unit": "BRL"},
        "provenance": [
            {
                "source_id": "RFB_TEST",
                "role": "operational_official",
                "status": "AVAILABLE",
                "retrieval_method": "http_html",
                "observed_at_utc": observed_at,
                "snapshot_sha256": snapshot_sha,
                "snapshot_path": f"test/{snapshot_sha}.html",
                "parser_id": "fixture_parser",
                "parser_version": "1.0.0",
            }
        ],
        "quality": {
            "status": "VALIDATED",
            "reference_case_ids": ["A01"],
            "last_validated_at_utc": observed_at,
            "reviewed_by_human": True,
        },
        "change_class": "RULE_ADDED",
        "update_policy": {
            "rule_class": "parameter",
            "auto_publish": True,
            "structural_review_required": False,
        },
    }


def _candidate(*, observed_at: str = "2026-09-14T12:00:00Z", value: str = "100.00", snapshot_sha: str = "a" * 64):
    rule = _rule(
        "fixture.rule",
        value=value,
        observed_at=observed_at,
        snapshot_sha=snapshot_sha,
    )
    return FakeContract(
        {
            "schema_version": "1.2.0",
            "release_id": "candidate-v1-2",
            "status": "CANDIDATE",
            "generated_at_utc": observed_at,
            "source_registry_version": "1.0.0",
            "governance_source_registry_version": "1.0.0",
            "consumer_compatibility": {
                "schema_version": "1.2.0",
                "contract_api_version": "1.2.0",
            },
            "rules": [rule],
        }
    )


def _assessment(*, change_class: str = "RULE_ADDED", changed_paths=("$rule",)):
    item = SimpleNamespace(
        rule_id="fixture.rule",
        occurrence=0,
        change_class=SimpleNamespace(value=change_class),
        changed_paths=tuple(changed_paths),
        previous_rule_version=None,
        candidate_rule_version="1.0.0",
        changed=True,
    )
    return SimpleNamespace(
        outcome=SimpleNamespace(value="REVIEW_REQUIRED"),
        diff=SimpleNamespace(contract_changed_paths=(), rule_diffs=(item,)),
        reasons=("fixture.rule requires human review",),
    )


def test_review_key_ignores_observation_and_validation_clock_when_evidence_bytes_are_same():
    first = _candidate(observed_at="2026-09-14T12:00:00Z")
    later = _candidate(observed_at="2026-09-15T12:00:00Z")
    assessment = _assessment()

    assert compute_review_key(None, first, assessment) == compute_review_key(None, later, assessment)


def test_review_key_changes_when_source_snapshot_or_semantic_value_changes():
    baseline = compute_review_key(None, _candidate(), _assessment())
    changed_snapshot = compute_review_key(
        None,
        _candidate(snapshot_sha="b" * 64),
        _assessment(),
    )
    changed_value = compute_review_key(
        None,
        _candidate(value="120.00"),
        _assessment(),
    )
    assert baseline != changed_snapshot
    assert baseline != changed_value


def test_bootstrap_review_packet_distinguishes_historical_rule_from_new_materialization():
    candidate_data = _candidate().model_dump()
    second = _rule(
        "fixture.new",
        value="200.00",
        observed_at="2026-09-14T12:00:00Z",
        snapshot_sha="c" * 64,
        calculators=["H29"],
    )
    candidate_data["rules"].append(second)
    candidate = FakeContract(candidate_data)
    historical = {"rules": [deepcopy(candidate_data["rules"][0])]}
    assessment = _assessment()
    new_item = SimpleNamespace(
        rule_id="fixture.new",
        occurrence=0,
        change_class=SimpleNamespace(value="RULE_ADDED"),
        changed_paths=("$rule",),
        previous_rule_version=None,
        candidate_rule_version="1.0.0",
        changed=True,
    )
    assessment.diff.rule_diffs = assessment.diff.rule_diffs + (new_item,)

    packet = build_review_packet(
        previous=None,
        candidate=candidate,
        assessment=assessment,
        created_at_utc=NOW,
        historical_template=historical,
    )

    assert packet["review_kind"] == "BOOTSTRAP"
    assert packet["bootstrap_baseline"] == {
        "historical_rules_present": 1,
        "newly_materialized_rules": 1,
    }
    statuses = {rule["rule_id"]: rule["historical_semantic_status"] for rule in packet["rules"]}
    assert statuses["fixture.rule"] == "SEMANTICALLY_INHERITED"
    assert statuses["fixture.new"] == "NEWLY_MATERIALIZED"
    assert packet["affected_calculators"] == ["H26", "H29"]

    markdown = render_review_markdown(packet, regression_status="PASS", regression_summary="239 passed")
    assert f"/approve {packet['review_key']}" in markdown
    assert "1 regras já materializadas" in markdown
    assert "1 regras materializadas agora" in markdown
    assert "239 passed" in markdown


def test_stale_review_approval_is_rejected_fail_closed():
    current = "a" * 64
    assert_review_approval_matches(current, current)
    with pytest.raises(StaleReviewApprovalError, match="stale"):
        assert_review_approval_matches(current, "b" * 64)
    with pytest.raises(StaleReviewApprovalError, match="requires the review_key"):
        assert_review_approval_matches(current, None)
