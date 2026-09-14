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
from sanida_fiscal.publication_v1 import (
    FiscalReleaseStore,
    HumanReviewRequiredError,
    NoPublicationRequired,
    PromotionBlockedError,
    PublicationError,
    prepare_published_release,
)
from sanida_fiscal.release_assembler_v12 import ReleaseAssemblyError, assemble_candidate_v12
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


EXIT_OK = 0
EXIT_REVIEW_REQUIRED = 3
EXIT_BLOCKED = 4
EXIT_SOURCE_OR_ASSEMBLY_ERROR = 5


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _utc_text(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _write_state(payload: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(STATE_PATH.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp.replace(STATE_PATH)


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
        help="Explicit review reference required for bootstrap or structural/effective changes.",
    )
    parser.add_argument(
        "--observed-at-utc",
        default=None,
        help="Optional deterministic ISO-8601 UTC timestamp for controlled runs/tests.",
    )
    parser.add_argument(
        "--scheduled",
        action="store_true",
        help="Scheduled mode: if no current release exists, exit cleanly without bootstrapping.",
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
        print("Phase 5 v1.2: bootstrap pending; scheduled run cannot create first release.")
        return EXIT_OK

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
        candidate = assemble_candidate_v12(
            template_v11=_load(TEMPLATE),
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
        print(f"Phase 5 v1.2: source/assembly blocked: {exc}", file=sys.stderr)
        return EXIT_SOURCE_OR_ASSEMBLY_ERROR

    assessment = assess_promotion(previous, candidate)
    state = _assessment_state(
        now=now,
        previous_release_id=previous.release_id if previous else None,
        candidate=candidate,
        assessment=assessment,
    )

    if assessment.outcome == PromotionOutcome.NO_PUBLISH_REQUIRED:
        state["publication_status"] = "NO_PUBLICATION_REQUIRED"
        _write_state(state)
        print("Phase 5 v1.2: no semantic/evidence delta; current release preserved.")
        return EXIT_OK

    if assessment.outcome == PromotionOutcome.BLOCKED:
        state["publication_status"] = "BLOCKED"
        _write_state(state)
        print("Phase 5 v1.2: promotion blocked: " + "; ".join(assessment.reasons), file=sys.stderr)
        return EXIT_BLOCKED

    if assessment.outcome == PromotionOutcome.REVIEW_REQUIRED and not args.human_approval_reference:
        state["publication_status"] = "REVIEW_REQUIRED"
        _write_state(state)
        print(
            "Phase 5 v1.2: human review required; rerun manually with --human-approval-reference.",
            file=sys.stderr,
        )
        return EXIT_REVIEW_REQUIRED

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
        state["publication_status"] = "NO_PUBLICATION_REQUIRED"
        _write_state(state)
        return EXIT_OK
    except HumanReviewRequiredError as exc:
        state["publication_status"] = "REVIEW_REQUIRED"
        state["reasons"] = [str(exc)]
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
    state["manifest"] = manifest
    _write_state(state)
    print(
        f"Phase 5 v1.2: published {published.release_id} ({published.lifecycle.approval_mode.value})."
    )
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
