#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _load(path: str):
    return json.loads(_read(path))


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"Phase 6 C6.0a gate: {message}")


def main() -> None:
    policy = _load("docs/phase6-c60a-human-review-policy-v1.json")
    _require(policy.get("decision_id") == "phase6.c60a.human_review_ux", "policy decision_id drift")
    _require(policy.get("status") == "C6_0a_complete", "C6.0a policy is not complete")

    packet = policy.get("review_packet", {})
    _require(packet.get("path") == "state/fiscal-release-v12-review.json", "review packet path drift")
    _require(packet.get("volatile_observation_clocks_change_review_key") is False, "volatile clocks may rotate review_key")
    _require(packet.get("source_snapshot_change_changes_review_key") is True, "source evidence no longer binds review_key")
    _require(packet.get("semantic_change_changes_review_key") is True, "semantic change no longer binds review_key")
    _require(packet.get("same_review_key_rewrites_durable_packet_or_state") is False, "same review may create durable clock churn")
    _require(packet.get("bootstrap_distinguishes_historical_inheritance_from_new_materialization") is True, "bootstrap baseline distinction disabled")
    _require(packet.get("bootstrap_expected_historical_materialized_rules") == 21, "bootstrap historical baseline count drift")
    _require(packet.get("bootstrap_expected_newly_materialized_rules") == 11, "bootstrap materialization count drift")

    issue = policy.get("github_issue", {})
    for key in (
        "automatic_on_review_required",
        "same_review_key_updates_existing_issue_when_content_changes",
        "new_review_key_closes_prior_pending_issue_as_stale",
        "assigned_to_repository_owner",
        "github_notification_expected",
        "includes_before_after",
        "includes_change_class",
        "includes_affected_calculators",
        "includes_source_snapshot_hashes",
        "includes_full_regression_status",
    ):
        _require(issue.get(key) is True, f"GitHub Issue invariant disabled: {key}")
    _require(issue.get("same_review_key_noop_patch_when_content_identical") is False, "identical Issue still receives no-op PATCH")
    _require(issue.get("email_delivery_guaranteed_by_repository") is False, "policy falsely guarantees email delivery")
    _require(issue.get("email_delivery_depends_on_github_account_notification_settings") is True, "email settings dependency missing")

    approval = policy.get("approval", {})
    _require(approval.get("preferred_surface") == "review_issue_comment", "approval surface drift")
    _require(approval.get("command") == "/approve <review_key>", "approval command drift")
    _require(approval.get("issue_comment_approver") == "repository_owner_only", "approval actor boundary weakened")
    _require(approval.get("fresh_candidate_review_key_must_equal_approved_review_key") is True, "stale approval protection disabled")
    _require(approval.get("stale_approval_blocks_publication") is True, "stale approval no longer blocks publication")

    schedule = policy.get("schedule", {})
    _require(schedule.get("may_prepare_bootstrap_review") is True, "schedule cannot prepare bootstrap review")
    _require(schedule.get("may_publish_bootstrap_without_human_approval") is False, "schedule may bootstrap without approval")

    review = _read("sanida_fiscal/human_review_v1.py")
    for marker in (
        "compute_review_key",
        "assert_review_approval_matches",
        "StaleReviewApprovalError",
        "historical_v1.1_candidate",
        "not_materialized_in_historical_v1.1_candidate",
        "SEMANTICALLY_INHERITED",
        "NEWLY_MATERIALIZED",
        "/approve {review_key}",
        "entrega por e-mail depende das configurações de notificações",
    ):
        _require(marker in review, f"human review module marker missing: {marker}")

    publisher = _read("scripts/publish_fiscal_release_v12.py")
    for marker in (
        "REVIEW_PATH = ROOT / \"state/fiscal-release-v12-review.json\"",
        "EXIT_STALE_REVIEW = 6",
        "--expected-review-key",
        "build_review_packet",
        "assert_review_approval_matches",
        'state["publication_status"] = "REVIEW_STALE"',
        'state["review_issue_action"] = "UPSERT_REQUIRED"',
        "same pending review_key; preserved durable review/state bytes without clock churn",
        "_write_pending_review_state",
    ):
        _require(marker in publisher, f"publisher review marker missing: {marker}")

    issue_script = _read("scripts/upsert_fiscal_review_issue.py")
    for marker in (
        "sanida-fiscal-review-key",
        "sanida-fiscal-review-current",
        "assignees",
        "_close_stale",
        "close_review",
        '"state": "closed"',
        "REUSED_UNCHANGED",
        "_same_issue_payload",
    ):
        _require(marker in issue_script, f"Issue automation marker missing: {marker}")

    workflow = _read(".github/workflows/fiscal-release-v12.yml")
    for marker in (
        "issue_comment:",
        "issues: write",
        "github.actor == github.repository_owner",
        "startsWith(github.event.comment.body, '/approve ')",
        "--expected-review-key",
        "Run full regression suite for pending human review",
        "python -m pytest -q tests",
        "Create or update actionable human-review Issue",
        "Close approved review Issue after publication",
        "Stale approval rejected",
    ):
        _require(marker in workflow, f"workflow C6.0a marker missing: {marker}")

    tests = _read("tests/test_human_review_v1.py")
    for marker in (
        "test_review_key_ignores_observation_and_validation_clock_when_evidence_bytes_are_same",
        "test_review_key_changes_when_source_snapshot_or_semantic_value_changes",
        "test_bootstrap_review_packet_distinguishes_historical_rule_from_new_materialization",
        "test_stale_review_approval_is_rejected_fail_closed",
    ):
        _require(marker in tests, f"C6.0a regression test missing: {marker}")

    idempotence_tests = _read("tests/test_publish_review_state_v1.py")
    for marker in (
        "test_same_review_key_preserves_review_packet_bytes",
        "test_same_pending_review_key_preserves_last_attempt_bytes",
        "test_new_review_key_replaces_pending_review_state",
    ):
        _require(marker in idempotence_tests, f"C6.0a idempotence regression missing: {marker}")

    docs = _read("docs/phase6-c60a-human-review.md")
    for marker in (
        "**Status:** CONCLUÍDO",
        "Human Review UX",
        "21 regras materializadas + 11 regras",
        "/approve <review_key>",
        "não é possível revisar A e publicar silenciosamente B",
        "O repositório não promete entrega por e-mail",
        "não cria churn Git nem nova notificação",
        "#2726",
    ):
        _require(marker in docs, f"C6.0a documentation marker missing: {marker}")

    remake = _read(".github/workflows/remake-ci.yml")
    _require("scripts/validate_phase6_c60a_gate.py" in remake, "C6.0a gate missing from Remake CI")

    print("Phase 6 C6.0a gate: PASS (actionable review Issue + deterministic review_key + stale-approval protection + no-churn pending review anchored)")


if __name__ == "__main__":
    main()
