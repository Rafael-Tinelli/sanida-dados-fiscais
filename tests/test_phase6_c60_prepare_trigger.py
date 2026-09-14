from __future__ import annotations

from pathlib import Path


WORKFLOW = Path(".github/workflows/fiscal-release-v12.yml")
POLICY = Path("docs/phase6-c60a-human-review-policy-v1.json")


def test_c60_prepare_command_is_owner_only_and_does_not_grant_approval() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "github.actor == github.repository_owner" in workflow
    assert "github.event.comment.body == '/prepare'" in workflow
    assert 'if [ "$COMMENT_BODY" = "/prepare" ]; then' in workflow
    assert 'approval_reference=""' in workflow
    assert 'review_key=""' in workflow
    assert "Only the repository owner may prepare or approve" in workflow


def test_c60_policy_records_prepare_as_non_approval_trigger() -> None:
    policy = POLICY.read_text(encoding="utf-8")

    assert '"manual_prepare_command": "/prepare"' in policy
    assert '"manual_prepare_actor": "repository_owner_only"' in policy
    assert '"manual_prepare_grants_approval": false' in policy
