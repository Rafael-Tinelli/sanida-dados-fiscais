from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import Any, Iterable

from .contract_v1 import FiscalContractV1, FiscalRuleV1, semver_bump
from .types_v1 import (
    ChangeClass,
    ContractStatus,
    QualityStatus,
    RuleClass,
    SourceObservationStatus,
    STRUCTURAL_RULE_CLASSES,
    VersionBump,
)


_NUMERIC_STRING = re.compile(r"^-?\d+(?:\.\d+)?$")


class SemanticDiffError(ValueError):
    pass


class PromotionOutcome(str, Enum):
    NO_PUBLISH_REQUIRED = "NO_PUBLISH_REQUIRED"
    AUTO_PUBLISH_ALLOWED = "AUTO_PUBLISH_ALLOWED"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True)
class RuleDiffV1:
    rule_id: str
    occurrence: int
    change_class: ChangeClass
    changed_paths: tuple[str, ...]
    previous_rule_version: str | None
    candidate_rule_version: str | None
    declared_change_class: ChangeClass | None
    rule_class: RuleClass | None
    auto_publish_declared: bool

    @property
    def changed(self) -> bool:
        return bool(self.changed_paths) or self.change_class in {
            ChangeClass.RULE_ADDED,
            ChangeClass.RULE_REMOVED,
        }


@dataclass(frozen=True)
class ContractDiffV1:
    previous_release_id: str | None
    candidate_release_id: str
    contract_changed_paths: tuple[str, ...]
    rule_diffs: tuple[RuleDiffV1, ...]
    immutable_payload_changed: bool

    @property
    def change_classes(self) -> frozenset[ChangeClass]:
        return frozenset(item.change_class for item in self.rule_diffs if item.changed)


@dataclass(frozen=True)
class PromotionAssessmentV1:
    outcome: PromotionOutcome
    reasons: tuple[str, ...]
    diff: ContractDiffV1


_RULE_IGNORED_FOR_SEMANTICS = {
    "description",
    "provenance",
    "quality",
    "change_class",
    "rule_version",
}

_CONTRACT_IGNORED_FOR_SEMANTICS = {
    "release_id",
    "status",
    "generated_at_utc",
    "supersedes_release_id",
    "lifecycle",
    "rules",
    "notes",
}


def _json_value(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    return value


def _is_numeric_leaf(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    return isinstance(value, str) and bool(_NUMERIC_STRING.fullmatch(value))


def _numeric_shape(value: Any) -> Any:
    """Erase numeric values while preserving the typed semantic shape.

    This normalization is used only after both rules are known to be parameter or
    parameterizable rules. Payload type, field names, list/object shape, booleans,
    enum strings and semantic strings remain visible. Therefore a formula/target/
    shape change cannot be mislabeled as a numeric parameter refresh.
    """
    value = _json_value(value)
    if _is_numeric_leaf(value):
        return "<NUMERIC_PARAMETER>"
    if isinstance(value, dict):
        return {key: _numeric_shape(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_numeric_shape(item) for item in value]
    return value


def _diff_paths(left: Any, right: Any, prefix: str = "") -> list[str]:
    left = _json_value(left)
    right = _json_value(right)
    if left == right:
        return []
    if isinstance(left, dict) and isinstance(right, dict):
        paths: list[str] = []
        for key in sorted(set(left) | set(right)):
            child = f"{prefix}.{key}" if prefix else key
            if key not in left or key not in right:
                paths.append(child)
            else:
                paths.extend(_diff_paths(left[key], right[key], child))
        return paths
    if isinstance(left, list) and isinstance(right, list):
        paths: list[str] = []
        common = min(len(left), len(right))
        for index in range(common):
            child = f"{prefix}[{index}]"
            paths.extend(_diff_paths(left[index], right[index], child))
        for index in range(common, max(len(left), len(right))):
            paths.append(f"{prefix}[{index}]")
        return paths
    return [prefix or "$root"]


def _rule_sort_key(rule: FiscalRuleV1) -> tuple[str, str]:
    return (
        rule.vigency.effective_from.isoformat(),
        rule.vigency.effective_until.isoformat() if rule.vigency.effective_until else "9999-12-31",
    )


def _group_rules(contract: FiscalContractV1) -> dict[str, list[FiscalRuleV1]]:
    grouped: dict[str, list[FiscalRuleV1]] = {}
    for rule in contract.rules:
        grouped.setdefault(rule.rule_id, []).append(rule)
    for rules in grouped.values():
        rules.sort(key=_rule_sort_key)
    return grouped


def _structural_rule_view(rule: FiscalRuleV1) -> dict[str, Any]:
    data = rule.model_dump(mode="json", exclude_none=True)
    for key in _RULE_IGNORED_FOR_SEMANTICS | {"vigency", "payload"}:
        data.pop(key, None)
    return data


def _all_rule_changed_paths(previous: FiscalRuleV1, candidate: FiscalRuleV1) -> tuple[str, ...]:
    left = previous.model_dump(mode="json", exclude_none=True)
    right = candidate.model_dump(mode="json", exclude_none=True)
    return tuple(sorted(_diff_paths(left, right)))


def classify_rule_change(
    previous: FiscalRuleV1,
    candidate: FiscalRuleV1,
    *,
    occurrence: int = 0,
) -> RuleDiffV1:
    if previous.rule_id != candidate.rule_id:
        raise SemanticDiffError("cannot compare rules with different rule_id")

    changed_paths = _all_rule_changed_paths(previous, candidate)
    rule_class = candidate.update_policy.rule_class

    structural_paths = _diff_paths(
        _structural_rule_view(previous),
        _structural_rule_view(candidate),
    )
    vigency_paths = _diff_paths(
        previous.vigency.model_dump(mode="json", exclude_none=True),
        candidate.vigency.model_dump(mode="json", exclude_none=True),
        "vigency",
    )
    payload_left = previous.payload.model_dump(mode="json", exclude_none=True)
    payload_right = candidate.payload.model_dump(mode="json", exclude_none=True)
    payload_paths = _diff_paths(payload_left, payload_right, "payload")

    if structural_paths:
        change_class = ChangeClass.STRUCTURAL_CHANGE
    elif payload_paths:
        if (
            previous.update_policy.rule_class in STRUCTURAL_RULE_CLASSES
            or candidate.update_policy.rule_class in STRUCTURAL_RULE_CLASSES
            or previous.update_policy.rule_class != candidate.update_policy.rule_class
        ):
            change_class = ChangeClass.STRUCTURAL_CHANGE
        elif _numeric_shape(payload_left) != _numeric_shape(payload_right):
            change_class = ChangeClass.STRUCTURAL_CHANGE
        elif vigency_paths:
            change_class = ChangeClass.EFFECTIVE_DATE_CHANGE
        else:
            change_class = ChangeClass.PARAMETER_CHANGE
    elif vigency_paths:
        change_class = ChangeClass.EFFECTIVE_DATE_CHANGE
    else:
        change_class = ChangeClass.SOURCE_REFRESH_NO_CHANGE

    return RuleDiffV1(
        rule_id=candidate.rule_id,
        occurrence=occurrence,
        change_class=change_class,
        changed_paths=changed_paths,
        previous_rule_version=previous.rule_version,
        candidate_rule_version=candidate.rule_version,
        declared_change_class=candidate.change_class,
        rule_class=rule_class,
        auto_publish_declared=candidate.update_policy.auto_publish,
    )


def _added_rule_diff(rule: FiscalRuleV1, occurrence: int) -> RuleDiffV1:
    return RuleDiffV1(
        rule_id=rule.rule_id,
        occurrence=occurrence,
        change_class=ChangeClass.RULE_ADDED,
        changed_paths=("$rule",),
        previous_rule_version=None,
        candidate_rule_version=rule.rule_version,
        declared_change_class=rule.change_class,
        rule_class=rule.update_policy.rule_class,
        auto_publish_declared=rule.update_policy.auto_publish,
    )


def _removed_rule_diff(rule: FiscalRuleV1, occurrence: int) -> RuleDiffV1:
    return RuleDiffV1(
        rule_id=rule.rule_id,
        occurrence=occurrence,
        change_class=ChangeClass.RULE_REMOVED,
        changed_paths=("$rule",),
        previous_rule_version=rule.rule_version,
        candidate_rule_version=None,
        declared_change_class=None,
        rule_class=rule.update_policy.rule_class,
        auto_publish_declared=False,
    )


def _contract_semantic_view(contract: FiscalContractV1) -> dict[str, Any]:
    data = contract.model_dump(mode="json", exclude_none=True)
    for key in _CONTRACT_IGNORED_FOR_SEMANTICS:
        data.pop(key, None)
    return data


def diff_contracts(
    previous: FiscalContractV1 | None,
    candidate: FiscalContractV1,
) -> ContractDiffV1:
    if candidate.status != ContractStatus.CANDIDATE:
        raise SemanticDiffError("Phase 5 diff input must be a CANDIDATE contract")

    candidate_groups = _group_rules(candidate)
    if previous is None:
        rule_diffs = tuple(
            _added_rule_diff(rule, occurrence)
            for rule_id in sorted(candidate_groups)
            for occurrence, rule in enumerate(candidate_groups[rule_id])
        )
        return ContractDiffV1(
            previous_release_id=None,
            candidate_release_id=candidate.release_id,
            contract_changed_paths=(),
            rule_diffs=rule_diffs,
            immutable_payload_changed=True,
        )

    if previous.status != ContractStatus.PUBLISHED:
        raise SemanticDiffError("previous contract must be PUBLISHED")

    contract_changed_paths = tuple(
        sorted(_diff_paths(_contract_semantic_view(previous), _contract_semantic_view(candidate)))
    )

    previous_groups = _group_rules(previous)
    diffs: list[RuleDiffV1] = []

    for rule_id in sorted(set(previous_groups) | set(candidate_groups)):
        before = previous_groups.get(rule_id, [])
        after = candidate_groups.get(rule_id, [])
        paired = min(len(before), len(after))
        for occurrence in range(paired):
            diffs.append(
                classify_rule_change(before[occurrence], after[occurrence], occurrence=occurrence)
            )
        for occurrence in range(paired, len(after)):
            diffs.append(_added_rule_diff(after[occurrence], occurrence))
        for occurrence in range(paired, len(before)):
            diffs.append(_removed_rule_diff(before[occurrence], occurrence))

    previous_payload = previous.immutable_payload_dict().copy()
    candidate_payload = candidate.immutable_payload_dict().copy()
    # Supersession is lineage, not a semantic rule delta.
    previous_payload.pop("supersedes_release_id", None)
    candidate_payload.pop("supersedes_release_id", None)

    return ContractDiffV1(
        previous_release_id=previous.release_id,
        candidate_release_id=candidate.release_id,
        contract_changed_paths=contract_changed_paths,
        rule_diffs=tuple(diffs),
        immutable_payload_changed=previous_payload != candidate_payload,
    )


def _rule_version_reason(diff: RuleDiffV1) -> str | None:
    change = diff.change_class
    previous = diff.previous_rule_version
    current = diff.candidate_rule_version

    if change in {ChangeClass.RULE_ADDED, ChangeClass.RULE_REMOVED}:
        return None
    if previous is None or current is None:
        return f"{diff.rule_id}: missing rule version for comparable rule"

    try:
        bump = semver_bump(previous, current)
    except ValueError as exc:
        return f"{diff.rule_id}: invalid version transition: {exc}"

    if not diff.changed_paths:
        if bump is not None:
            return f"{diff.rule_id}: unchanged rule must preserve rule_version"
        return None

    if change in {
        ChangeClass.SOURCE_REFRESH_NO_CHANGE,
        ChangeClass.PARAMETER_CHANGE,
        ChangeClass.EFFECTIVE_DATE_CHANGE,
    }:
        if bump != VersionBump.PATCH:
            return f"{diff.rule_id}: {change.value} requires PATCH rule_version bump"
    elif change == ChangeClass.STRUCTURAL_CHANGE:
        if bump not in {VersionBump.MINOR, VersionBump.MAJOR}:
            return f"{diff.rule_id}: STRUCTURAL_CHANGE requires MINOR or MAJOR rule_version bump"
    return None


def _iter_candidate_rules(contract: FiscalContractV1) -> Iterable[FiscalRuleV1]:
    return sorted(contract.rules, key=lambda rule: (rule.rule_id, *_rule_sort_key(rule)))


def _changed_candidate_rule(
    candidate: FiscalContractV1,
    diff: RuleDiffV1,
) -> FiscalRuleV1 | None:
    if diff.change_class == ChangeClass.RULE_REMOVED:
        return None
    grouped = _group_rules(candidate)
    rules = grouped.get(diff.rule_id, [])
    if diff.occurrence >= len(rules):
        return None
    return rules[diff.occurrence]


def assess_promotion(
    previous: FiscalContractV1 | None,
    candidate: FiscalContractV1,
) -> PromotionAssessmentV1:
    diff = diff_contracts(previous, candidate)
    blockers: list[str] = []
    review_reasons: list[str] = []

    for rule in _iter_candidate_rules(candidate):
        if rule.quality.status != QualityStatus.VALIDATED:
            blockers.append(
                f"{rule.rule_id}: quality is {rule.quality.status.value}, expected VALIDATED"
            )
        available_hashed = [
            evidence
            for evidence in rule.provenance
            if evidence.status == SourceObservationStatus.AVAILABLE
            and evidence.snapshot_sha256
        ]
        if not available_hashed:
            blockers.append(f"{rule.rule_id}: no AVAILABLE hashed source evidence")

    for item in diff.rule_diffs:
        version_reason = _rule_version_reason(item)
        if version_reason:
            blockers.append(version_reason)

        if item.change_class != ChangeClass.RULE_REMOVED and item.changed:
            if item.declared_change_class != item.change_class:
                blockers.append(
                    f"{item.rule_id}: declared change_class={item.declared_change_class.value if item.declared_change_class else None} "
                    f"does not match computed {item.change_class.value}"
                )

    if blockers:
        return PromotionAssessmentV1(
            outcome=PromotionOutcome.BLOCKED,
            reasons=tuple(sorted(set(blockers))),
            diff=diff,
        )

    review_classes = {
        ChangeClass.EFFECTIVE_DATE_CHANGE,
        ChangeClass.STRUCTURAL_CHANGE,
        ChangeClass.RULE_ADDED,
        ChangeClass.RULE_REMOVED,
    }

    if diff.contract_changed_paths:
        review_reasons.append(
            "contract-level semantic/governance fields changed: "
            + ", ".join(diff.contract_changed_paths)
        )

    for item in diff.rule_diffs:
        if not item.changed:
            continue
        if item.change_class in review_classes:
            review_reasons.append(
                f"{item.rule_id}: {item.change_class.value} requires human review"
            )
            continue

        candidate_rule = _changed_candidate_rule(candidate, item)
        if candidate_rule is None:
            continue

        if (
            item.change_class == ChangeClass.SOURCE_REFRESH_NO_CHANGE
            and item.rule_class in STRUCTURAL_RULE_CLASSES
        ):
            review_reasons.append(
                f"{item.rule_id}: source refresh on structural rule requires human review"
            )
            continue

        if item.change_class == ChangeClass.PARAMETER_CHANGE and not item.auto_publish_declared:
            review_reasons.append(
                f"{item.rule_id}: parameter change is not declared auto-publishable"
            )
            continue

        if item.change_class in {
            ChangeClass.SOURCE_REFRESH_NO_CHANGE,
            ChangeClass.PARAMETER_CHANGE,
        }:
            parser_backed = any(
                evidence.status == SourceObservationStatus.AVAILABLE
                and evidence.snapshot_sha256
                and evidence.parser_id
                and evidence.parser_version
                for evidence in candidate_rule.provenance
            )
            if not parser_backed:
                review_reasons.append(
                    f"{item.rule_id}: automatic promotion requires parser-backed hashed evidence"
                )

    if review_reasons:
        return PromotionAssessmentV1(
            outcome=PromotionOutcome.REVIEW_REQUIRED,
            reasons=tuple(sorted(set(review_reasons))),
            diff=diff,
        )

    if not diff.immutable_payload_changed:
        return PromotionAssessmentV1(
            outcome=PromotionOutcome.NO_PUBLISH_REQUIRED,
            reasons=("immutable release payload is unchanged",),
            diff=diff,
        )

    return PromotionAssessmentV1(
        outcome=PromotionOutcome.AUTO_PUBLISH_ALLOWED,
        reasons=("only validated parser-backed auto-publishable refresh/parameter changes detected",),
        diff=diff,
    )
