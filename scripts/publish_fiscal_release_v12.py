#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sanida_fiscal.authority_evidence_v12 import (
    AuthorityEvidenceError,
    collect_authority_evidence,
)
from sanida_fiscal.governance_evidence_v12 import (
    GovernanceEvidenceError,
    build_governance_evidence,
)
from sanida_fiscal.human_review_v1 import (
    StaleReviewApprovalError,
    assert_review_approval_matches,
    build_review_packet,
)
from sanida_fiscal.publication_v1 import (
    FiscalReleaseStore,
    HumanReviewRequiredError,
    NoPublicationRequired,
    PromotionBlockedError,
    PublicationError,
    prepare_published_release,
)
from sanida_fiscal.release_assembler_v12 import ReleaseAssemblyError, assemble_candidate_v12
from sanida_fiscal.review_evidence_identity_v1 import (
    ReviewEvidenceIdentityError,
    build_review_identity_candidate,
)
from sanida_fiscal.semantic_diff_v1 import PromotionOutcome, assess_promotion


TEMPLATE = ROOT / "contracts/examples/fiscal-contract-v1.example.json"
COVERAGE = ROOT / "docs/contract-coverage-v1.json"
INVENTORY = ROOT / "docs/rule-inventory-v1.json"
SOURCE_REGISTRY = ROOT / "docs/source-registry-v1.json"
GOVERNANCE_REGISTRY = ROOT / "docs/governance-source-registry-v1.json"
AUTHORITY_EVIDENCE_ROOT = ROOT / "evidence/fiscal-authority-v1"
GOVERNANCE_EVIDENCE_ROOT = ROOT / "evidence/governance-v1"
RELEASE_ROOT = ROOT / "releases/fiscal-v1"
STATE_PATH = ROOT / "state/fiscal-release-v12-last-attempt.json"
REVIEW_PATH = ROOT / "state/fiscal-release-v12-review.json"


EXIT_OK = 0
EXIT_REVIEW_REQUIRED = 3
EXIT_BLOCKED = 4
EXIT_SOURCE_OR_ASSEMBLY_ERROR = 5
EXIT_STALE_REVIEW = 6


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_optional(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        value = _load(path)
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _utc_text(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)


def _write_state(payload: dict[str, Any]) -> None:
    _write_json_atomic(STATE_PATH, payload)


def _write_review(payload: dict[str, Any]) -> bool:
    """Persist a new decision packet only when its review identity changed.

    Repeated scheduled observations of the same semantic/evidence candidate must not
    rotate `created_at_utc`, rewrite Git history or trigger Issue churn merely because
    the wall clock advanced.
    """
    existing = _load_optional(REVIEW_PATH)
    if existing is not None and existing.get("review_key") == payload.get("review_key"):
        return False
    _write_json_atomic(REVIEW_PATH, payload)
    return True


def _write_pending_review_state(payload: dict[str, Any]) -> bool:
    existing = _load_optional(STATE_PATH)
    if (
        existing is not None
        and existing.get("publication_status") == "REVIEW_REQUIRED"
        and existing.get("review_key") == payload.get("review_key")
        and existing.get("previous_release_id") == payload.get("previous_release_id")
    ):
        return False
    _write_state(payload)
    return True


def _assessment_state(*, now: datetime, previous_release_id: str | None, candidate, assessment) -> dict[str, Any]:
    changed = [
        {
            "rule_id": item.rule_id,
            "occurrence": item.occurrence,
            "change_class": item.change_class.value,
            "changed_paths": list(item.changed_paths),
            "previous_rule_version": item.previous_rule_version,
            "candidate_rule_version": item.candidate_rule_version,
        }
        for item in assessment.diff.rule_diffs
        if item.changed
    ]
    return {
        "schema_version": "1.0.0",
        "attempted_at_utc": _utc_text(now),
        "contract_schema_version": candidate.schema_version,
        "previous_release_id": previous_release_id,
        "candidate_release_id": candidate.release_id,
        "promotion_outcome": assessment.outcome.value,
        "reasons": list(assessment.reasons),
        "contract_changed_paths": list(assessment.diff.contract_changed_paths),
        "changed_rules": changed,
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish canonical Sanida Fiscal Contract v1.2")
    parser.add_argument(
        "--human-approval-reference",
        default=None,
        help="Explicit audit reference for the human approval that authorizes this exact review packet.",
    )
    parser.add_argument(
        "--expected-review-key",
        default=None,
        help="SHA-256 review_key copied from the human-review Issue; must match the freshly assembled candidate.",
    )
    parser.add_argument(
        "--observed-at-utc",
        default=None,
        help="Optional deterministic ISO-8601 UTC timestamp for controlled runs/tests.",
    )
    parser.add_argument(
        "--scheduled",
        action="store_true",
        help="Scheduled mode may prepare a review packet but can never bootstrap without explicit human approval.",
    )
    return parser.parse_args()


def _parse_now(raw: str | None) -> datetime:
    if raw is None:
        return datetime.now(timezone.utc)
    normalized = raw.replace("Z", "+00:00")
    value = datetime.fromisoformat(normalized)
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise SystemExit("--observed-at-utc must be an offset-aware UTC timestamp")
    return value


def main() -> int:
    args = _parse_args()
    now = _parse_now(args.observed_at_utc)
    store = FiscalReleaseStore(RELEASE_ROOT)
    previous = store.load_current()

    if previous is None and args.scheduled:
        if args.human_approval_reference or args.expected_review_key:
            print("Fiscal v1.2: scheduled bootstrap cannot carry human approval.", file=sys.stderr)
            return EXIT_BLOCKED
        print(
            "Fiscal v1.2: bootstrap pending; scheduled run cannot create first release; preparing review packet only."
        )

    try:
        authority = collect_authority_evidence(
            source_registry_path=SOURCE_REGISTRY,
            rule_inventory_path=INVENTORY,
            snapshot_root=AUTHORITY_EVIDENCE_ROOT,
            observed_at_utc=now,
        )
        governance = build_governance_evidence(
            repository_root=ROOT,
            registry_path=GOVERNANCE_REGISTRY,
            evidence_root=GOVERNANCE_EVIDENCE_ROOT,
            observed_at_utc=now,
        )
        governance_registry = _load(GOVERNANCE_REGISTRY)
        historical_template = _load(TEMPLATE)
        candidate = assemble_candidate_v12(
            template_v11=historical_template,
            coverage=_load(COVERAGE),
            inventory=_load(INVENTORY),
            official_evidence=authority.evidence_by_source,
            governance_evidence=governance,
            normalized_candidates=authority.normalized_candidates,
            generated_at_utc=now,
            governance_source_registry_version=governance_registry["schema_version"],
            previous=previous,
        )
    except (AuthorityEvidenceError, GovernanceEvidenceError, ReleaseAssemblyError, OSError, ValueError) as exc:
        _write_state(
            {
                "schema_version": "1.0.0",
                "attempted_at_utc": _utc_text(now),
                "previous_release_id": previous.release_id if previous else None,
                "promotion_outcome": "BLOCKED_BEFORE_DIFF",
                "reasons": [str(exc)],
            }
        )
        print(f"Fiscal v1.2: source/assembly blocked: {exc}", file=sys.stderr)
        return EXIT_SOURCE_OR_ASSEMBLY_ERROR

    assessment = assess_promotion(previous, candidate)
    state = _assessment_state(
        now=now,
        previous_release_id=previous.release_id if previous else None,
        candidate=candidate,
        assessment=assessment,
    )

    if assessment.outcome == PromotionOutcome.NO_PUBLISH_REQUIRED:
        print("Fiscal v1.2: no semantic/evidence delta; current release preserved byte-for-byte.")
        return EXIT_OK

    if assessment.outcome == PromotionOutcome.BLOCKED:
        state["publication_status"] = "BLOCKED"
        _write_state(state)
        print("Fiscal v1.2: promotion blocked: " + "; ".join(assessment.reasons), file=sys.stderr)
        return EXIT_BLOCKED

    review_packet: dict[str, Any] | None = None
    if assessment.outcome == PromotionOutcome.REVIEW_REQUIRED:
        try:
            review_identity_candidate, review_evidence_identity = build_review_identity_candidate(
                candidate,
                authority_snapshot_root=AUTHORITY_EVIDENCE_ROOT,
                normalized_candidates=authority.normalized_candidates,
            )
        except ReviewEvidenceIdentityError as exc:
            state["publication_status"] = "BLOCKED"
            state["reasons"] = list(state.get("reasons", [])) + [
                f"review evidence identity failed closed: {exc}"
            ]
            _write_state(state)
            print(f"Fiscal v1.2: review evidence identity blocked: {exc}", file=sys.stderr)
            return EXIT_BLOCKED

        identity_packet = build_review_packet(
            previous=previous,
            candidate=review_identity_candidate,
            assessment=assessment,
            created_at_utc=now,
            historical_template=historical_template if previous is None else None,
        )
        review_packet = build_review_packet(
            previous=previous,
            candidate=candidate,
            assessment=assessment,
            created_at_utc=now,
            historical_template=historical_template if previous is None else None,
        )
        review_packet["review_key"] = identity_packet["review_key"]
        review_packet["review_evidence_identity"] = review_evidence_identity
        review_packet["approval"]["required_command"] = (
            f"/approve {review_packet['review_key']}"
        )

        review_packet_changed = _write_review(review_packet)
        state["review_key"] = review_packet["review_key"]

        if not args.human_approval_reference:
            state["publication_status"] = "REVIEW_REQUIRED"
            state["review_issue_action"] = "UPSERT_REQUIRED"
            state_changed = _write_pending_review_state(state)
            if not review_packet_changed and not state_changed:
                print(
                    "Fiscal v1.2: same pending review_key; preserved durable review/state bytes without clock churn.",
                    file=sys.stderr,
                )
            else:
                print(
                    "Fiscal v1.2: human review required; durable review packet prepared for GitHub Issue.",
                    file=sys.stderr,
                )
            return EXIT_REVIEW_REQUIRED

        try:
            assert_review_approval_matches(
                str(review_packet["review_key"]),
                args.expected_review_key,
            )
        except StaleReviewApprovalError as exc:
            state["publication_status"] = "REVIEW_STALE"
            state["review_issue_action"] = "UPSERT_REQUIRED"
            state["reasons"] = list(state.get("reasons", [])) + [str(exc)]
            _write_state(state)
            print(f"Fiscal v1.2: stale human approval blocked: {exc}", file=sys.stderr)
            return EXIT_STALE_REVIEW

    elif args.human_approval_reference is not None or args.expected_review_key is not None:
        state["publication_status"] = "BLOCKED"
        state["reasons"] = list(state.get("reasons", [])) + [
            "human approval was supplied for a candidate that does not currently require human review"
        ]
        _write_state(state)
        return EXIT_BLOCKED

    try:
        published = prepare_published_release(
            previous=previous,
            candidate=candidate,
            coverage=_load(COVERAGE),
            published_at_utc=now,
            human_approval_reference=args.human_approval_reference,
        )
        manifest = store.publish(published)
    except NoPublicationRequired:
        return EXIT_OK
    except HumanReviewRequiredError as exc:
        state["publication_status"] = "REVIEW_REQUIRED"
        state["reasons"] = [str(exc)]
        if review_packet is not None:
            state["review_key"] = review_packet["review_key"]
        _write_state(state)
        return EXIT_REVIEW_REQUIRED
    except (PromotionBlockedError, PublicationError, ValueError) as exc:
        state["publication_status"] = "BLOCKED"
        state["reasons"] = [str(exc)]
        _write_state(state)
        return EXIT_BLOCKED

    state["publication_status"] = "PUBLISHED"
    state["published_release_id"] = published.release_id
    state["approval_mode"] = published.lifecycle.approval_mode.value
    state["approval_reference"] = published.lifecycle.approval_reference
    if review_packet is not None:
        state["review_key"] = review_packet["review_key"]
    state["manifest"] = manifest
    _write_state(state)
    print(
        f"Fiscal v1.2: published {published.release_id} ({published.lifecycle.approval_mode.value})."
    )
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
