from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Mapping

from .contract_v1 import FiscalContractV1
from .semantic_diff_v1 import ContractDiffV1, PromotionOutcome, assess_promotion
from .types_v1 import ChangeClass, ContractStatus, ReleaseApprovalMode


RELEASE_STORE_SCHEMA_VERSION = "1.0.0"


class PublicationError(RuntimeError):
    pass


class IncompleteReleaseError(PublicationError):
    pass


class PromotionBlockedError(PublicationError):
    pass


class HumanReviewRequiredError(PublicationError):
    pass


class NoPublicationRequired(PublicationError):
    pass


class ReleaseStoreIntegrityError(PublicationError):
    pass


def _require_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise PublicationError("publication timestamp must be UTC")


def required_inventory_rule_ids(coverage: Mapping[str, Any]) -> set[str]:
    rules = coverage.get("rules")
    if coverage.get("rule_count") != 32 or not isinstance(rules, list):
        raise IncompleteReleaseError("contract coverage must declare the closed 32-rule inventory")
    ids = {
        item.get("rule_id")
        for item in rules
        if isinstance(item, Mapping) and isinstance(item.get("rule_id"), str)
    }
    if len(ids) != 32:
        raise IncompleteReleaseError("contract coverage must contain exactly 32 unique rule_ids")
    return ids


def assert_release_inventory_complete(
    contract: FiscalContractV1,
    coverage: Mapping[str, Any],
) -> None:
    required = required_inventory_rule_ids(coverage)
    actual = {rule.rule_id for rule in contract.rules}
    missing = sorted(required - actual)
    unexpected = sorted(actual - required)
    if missing or unexpected:
        raise IncompleteReleaseError(
            f"canonical fiscal release must cover inventory exactly; missing={missing} unexpected={unexpected}"
        )


def _vigency_sort_key(rule: Mapping[str, Any]) -> tuple[str, str]:
    vigency = rule.get("vigency")
    if not isinstance(vigency, Mapping):
        raise PublicationError("rule vigency missing during transition reconciliation")
    effective_from = vigency.get("effective_from")
    effective_until = vigency.get("effective_until")
    if not isinstance(effective_from, str):
        raise PublicationError("rule effective_from missing during transition reconciliation")
    return effective_from, effective_until if isinstance(effective_until, str) else "9999-12-31"


def reconcile_transition_change_classes(
    candidate: FiscalContractV1,
    diff: ContractDiffV1,
) -> FiscalContractV1:
    """Make ``change_class`` describe this release transition, not history.

    A published rule can have entered an earlier release as ``RULE_ADDED`` or have
    been structurally changed in an earlier release. Carrying that historical label
    unchanged into every successor would make Phase 2 correctly demand human review
    forever. Phase 5 therefore reconciles the successor just before publication:

    * changed rules receive the computed class already checked against the candidate;
    * unchanged comparable rules become ``SOURCE_REFRESH_NO_CHANGE``;
    * newly added rules remain ``RULE_ADDED``.

    This reconciliation changes immutable release bytes, so the final release_id is
    calculated only after this step. It never weakens the Phase 2 PUBLISHED invariant.
    """
    data = candidate.model_dump(mode="json", exclude_none=True)
    raw_rules = data.get("rules")
    if not isinstance(raw_rules, list):
        raise PublicationError("candidate rules missing during transition reconciliation")

    grouped_indices: dict[str, list[int]] = {}
    for index, rule in enumerate(raw_rules):
        if not isinstance(rule, dict) or not isinstance(rule.get("rule_id"), str):
            raise PublicationError("invalid rule while reconciling transition classes")
        grouped_indices.setdefault(rule["rule_id"], []).append(index)
    for indices in grouped_indices.values():
        indices.sort(key=lambda index: _vigency_sort_key(raw_rules[index]))

    diff_map = {
        (item.rule_id, item.occurrence): item
        for item in diff.rule_diffs
        if item.change_class != ChangeClass.RULE_REMOVED
    }

    seen: set[tuple[str, int]] = set()
    for rule_id, indices in grouped_indices.items():
        for occurrence, index in enumerate(indices):
            key = (rule_id, occurrence)
            item = diff_map.get(key)
            if item is None:
                raise PublicationError(
                    f"semantic diff has no candidate rule occurrence for {rule_id}[{occurrence}]"
                )
            raw_rules[index]["change_class"] = (
                item.change_class.value
                if item.changed
                else ChangeClass.SOURCE_REFRESH_NO_CHANGE.value
            )
            seen.add(key)

    missing = sorted(set(diff_map) - seen)
    if missing:
        raise PublicationError(f"semantic diff contains unmatched candidate rules: {missing}")

    return FiscalContractV1.model_validate(data)


def prepare_published_release(
    *,
    previous: FiscalContractV1 | None,
    candidate: FiscalContractV1,
    coverage: Mapping[str, Any],
    published_at_utc: datetime,
    human_approval_reference: str | None = None,
) -> FiscalContractV1:
    """Promote one complete CANDIDATE into an immutable PUBLISHED contract.

    This function does not persist bytes. It applies Phase 5 completeness,
    semantic-diff and approval gates, reconciles release-relative change metadata,
    and then re-validates the result through the existing Phase 2 PUBLISHED
    invariants.
    """
    _require_utc(published_at_utc)
    if candidate.status != ContractStatus.CANDIDATE:
        raise PublicationError("publication input must be CANDIDATE")

    assert_release_inventory_complete(candidate, coverage)

    if previous is None:
        if candidate.supersedes_release_id is not None:
            raise PublicationError("bootstrap release cannot declare supersedes_release_id")
    else:
        previous.assert_consumable()
        if candidate.supersedes_release_id != previous.release_id:
            raise PublicationError(
                "successor candidate must supersede the exact current published release"
            )

    assessment = assess_promotion(previous, candidate)
    if assessment.outcome == PromotionOutcome.BLOCKED:
        raise PromotionBlockedError("; ".join(assessment.reasons))
    if assessment.outcome == PromotionOutcome.NO_PUBLISH_REQUIRED:
        raise NoPublicationRequired("; ".join(assessment.reasons))

    if assessment.outcome == PromotionOutcome.REVIEW_REQUIRED:
        if not isinstance(human_approval_reference, str) or not human_approval_reference.strip():
            raise HumanReviewRequiredError("; ".join(assessment.reasons))
        approval_mode = ReleaseApprovalMode.HUMAN_REVIEWED
        approval_reference = human_approval_reference.strip()
    else:
        if human_approval_reference is not None:
            raise PublicationError(
                "human_approval_reference must be omitted for AUTO_PUBLISH_ALLOWED promotion"
            )
        approval_mode = ReleaseApprovalMode.AUTO_VALIDATED
        approval_reference = "phase5:auto-promotion-v1"

    reconciled = reconcile_transition_change_classes(candidate, assessment.diff)
    data = reconciled.model_dump(mode="json", exclude_none=True)
    data["status"] = ContractStatus.PUBLISHED.value
    data["lifecycle"] = {
        "validated_at_utc": published_at_utc.isoformat().replace("+00:00", "Z"),
        "published_at_utc": published_at_utc.isoformat().replace("+00:00", "Z"),
        "approval_mode": approval_mode.value,
        "approval_reference": approval_reference,
        "block_reasons": [],
    }

    # ``change_class`` is release-relative immutable metadata. The canonical
    # identity therefore comes from the reconciled successor, never from the raw
    # CANDIDATE carrying historical labels.
    data["release_id"] = reconciled.expected_release_id()
    published = FiscalContractV1.model_validate(data)
    published.assert_consumable()
    return published


def canonical_release_bytes(contract: FiscalContractV1) -> bytes:
    contract.assert_consumable()
    return (contract.canonical_json() + "\n").encode("utf-8")


class FiscalReleaseStore:
    """Filesystem store with immutable releases and one atomic mutable pointer."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.releases_root = self.root / "releases"
        self.current_path = self.root / "current.json"

    def release_path(self, release_id: str) -> Path:
        if not release_id.startswith("fiscal-v1-sha256-"):
            raise ReleaseStoreIntegrityError("invalid fiscal release_id for store path")
        return self.releases_root / f"{release_id}.json"

    @staticmethod
    def _atomic_write(path: Path, body: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(body)
        os.replace(tmp, path)

    def publish(self, contract: FiscalContractV1) -> dict[str, Any]:
        body = canonical_release_bytes(contract)
        release_path = self.release_path(contract.release_id)
        if release_path.exists():
            if release_path.read_bytes() != body:
                raise ReleaseStoreIntegrityError(
                    "content-addressed release path already exists with different bytes"
                )
        else:
            release_path.parent.mkdir(parents=True, exist_ok=True)
            with release_path.open("xb") as fh:
                fh.write(body)

        digest = sha256(body).hexdigest()
        lifecycle = contract.lifecycle
        if lifecycle.published_at_utc is None:
            raise ReleaseStoreIntegrityError("published contract has no published_at_utc")
        manifest = {
            "schema_version": RELEASE_STORE_SCHEMA_VERSION,
            "contract_id": contract.contract_id,
            "schema_version_contract": contract.schema_version,
            "contract_api_version": contract.consumer_compatibility.contract_api_version,
            "release_id": contract.release_id,
            "supersedes_release_id": contract.supersedes_release_id,
            "artifact": f"releases/{release_path.name}",
            "artifact_sha256": digest,
            "published_at_utc": lifecycle.published_at_utc.isoformat().replace("+00:00", "Z"),
        }
        manifest_body = (
            json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\n"
        ).encode("utf-8")
        self._atomic_write(self.current_path, manifest_body)
        return manifest

    def load_current(self) -> FiscalContractV1 | None:
        if not self.current_path.exists():
            return None
        try:
            manifest = json.loads(self.current_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ReleaseStoreIntegrityError("current manifest is not readable JSON") from exc
        if not isinstance(manifest, dict) or manifest.get("schema_version") != RELEASE_STORE_SCHEMA_VERSION:
            raise ReleaseStoreIntegrityError("current manifest schema is invalid")
        release_id = manifest.get("release_id")
        artifact = manifest.get("artifact")
        expected_sha = manifest.get("artifact_sha256")
        if not isinstance(release_id, str) or not isinstance(artifact, str):
            raise ReleaseStoreIntegrityError("current manifest release identity is invalid")
        expected_artifact = f"releases/{release_id}.json"
        if artifact != expected_artifact:
            raise ReleaseStoreIntegrityError("current manifest artifact path is not canonical")
        if not isinstance(expected_sha, str) or len(expected_sha) != 64:
            raise ReleaseStoreIntegrityError("current manifest artifact_sha256 is invalid")

        root_resolved = self.root.resolve()
        path = (self.root / artifact).resolve()
        if not path.is_relative_to(root_resolved) or not path.is_file():
            raise ReleaseStoreIntegrityError("current release artifact is missing or escapes store root")
        body = path.read_bytes()
        if sha256(body).hexdigest() != expected_sha:
            raise ReleaseStoreIntegrityError("current release artifact sha256 mismatch")
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ReleaseStoreIntegrityError("current release artifact is not valid JSON") from exc
        contract = FiscalContractV1.model_validate(payload)
        contract.assert_consumable()
        if contract.release_id != release_id:
            raise ReleaseStoreIntegrityError("manifest release_id differs from artifact")
        return contract
