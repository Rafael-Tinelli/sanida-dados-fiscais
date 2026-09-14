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
        raise SystemExit(f"Phase 5 foundation: {message}")


def main() -> None:
    policy = _load("docs/phase5-semantic-diff-policy-v1.json")
    _require(policy.get("status") == "phase5_foundation", "policy status drift")

    classes = policy.get("change_classes", {})
    expected = {
        "SOURCE_REFRESH_NO_CHANGE",
        "PARAMETER_CHANGE",
        "EFFECTIVE_DATE_CHANGE",
        "STRUCTURAL_CHANGE",
        "RULE_ADDED",
        "RULE_REMOVED",
        "SOURCE_UNAVAILABLE",
        "PARSER_INCOMPATIBLE",
    }
    _require(set(classes) == expected, "change-class policy is incomplete")
    for change in ("EFFECTIVE_DATE_CHANGE", "STRUCTURAL_CHANGE", "RULE_ADDED", "RULE_REMOVED"):
        _require(classes[change].get("auto_publish_possible") is False, f"{change} became auto-publishable")

    evidence = policy.get("evidence_policy", {})
    _require(
        evidence.get("all_candidate_rules_require_at_least_one_available_hashed_snapshot_before_promotion") is True,
        "hashed evidence gate drift",
    )
    _require(
        evidence.get("automatic_parameter_or_refresh_promotion_requires_parser_id_and_parser_version") is True,
        "parser-backed auto-promotion gate drift",
    )
    _require(
        evidence.get("structural_source_refresh_requires_human_review") is True,
        "structural source refresh no longer requires review",
    )

    bootstrap = policy.get("bootstrap_policy", {})
    _require(bootstrap.get("automatic_first_publication") is False, "bootstrap may not auto-publish")
    _require(bootstrap.get("first_release_requires_human_review") is True, "bootstrap human review gate drift")

    module = _read("sanida_fiscal/semantic_diff_v1.py")
    for marker in (
        "class PromotionOutcome",
        "def classify_rule_change(",
        "def diff_contracts(",
        "def assess_promotion(",
        "_numeric_shape",
        "automatic promotion requires parser-backed hashed evidence",
        "source refresh on structural rule requires human review",
        "EFFECTIVE_DATE_CHANGE",
        "STRUCTURAL_CHANGE",
        "RULE_ADDED",
        "RULE_REMOVED",
    ):
        _require(marker in module, f"semantic diff executable marker missing: {marker}")

    tests = _read("tests/test_semantic_diff_v1.py")
    for marker in (
        "test_identical_successor_requires_no_publication",
        "test_numeric_parameter_change_is_classified_and_auto_publishable_when_parser_backed",
        "test_semantic_string_change_is_structural_even_inside_parameterizable_payload",
        "test_effective_date_change_is_separate_and_never_auto_published",
        "test_structural_rule_source_refresh_requires_human_review",
        "test_declared_change_class_must_match_computed_class",
        "test_bootstrap_release_is_human_review_only",
        "test_missing_hashed_evidence_blocks_promotion",
        "test_added_and_removed_rules_are_detected",
    ):
        _require(marker in tests, f"semantic diff regression marker missing: {marker}")

    readme = _read("README.md")
    _require("Fase 4 — Fontes e sensores\n\n**Status: CONCLUÍDA**" in readme, "Phase 4 closure regressed")
    _require("Fase 5 — Diff semântico e gates de publicação" in readme, "Phase 5 boundary missing from README")

    print("Phase 5 foundation: PASS (semantic classifier + fail-closed promotion policy anchored)")


if __name__ == "__main__":
    main()
