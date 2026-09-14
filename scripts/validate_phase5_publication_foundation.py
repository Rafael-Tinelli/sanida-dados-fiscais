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
        raise SystemExit(f"Phase 5 publication foundation: {message}")


def main() -> None:
    policy = _load("docs/phase5-publication-policy-v1.json")
    _require(policy.get("status") == "phase5_publication_foundation", "policy status drift")

    store = policy.get("canonical_store", {})
    _require(store.get("repository_root") == "releases/fiscal-v1", "canonical store root drift")
    _require(store.get("release_files_are_immutable") is True, "release immutability disabled")
    _require(store.get("current_pointer_update") == "atomic_replace", "current pointer is not atomic")

    completeness = policy.get("completeness_gate", {})
    _require(completeness.get("required_unique_rule_ids") == 32, "canonical release is not 32-rule complete")
    _require(
        completeness.get("candidate_rule_id_set_must_equal_inventory") is True,
        "release rule set no longer equals inventory",
    )
    _require(
        completeness.get("representative_21_rule_candidate_is_publishable") is False,
        "representative Phase 2 CANDIDATE became publishable",
    )
    _require(completeness.get("extra_rules_are_allowed") is False, "extra release rules became allowed")

    promotion = policy.get("promotion_gate", {})
    _require(promotion.get("BLOCKED") == "never_publish", "BLOCKED outcome may publish")
    _require(
        promotion.get("REVIEW_REQUIRED") == "require_explicit_human_approval_reference",
        "human-review gate drift",
    )
    _require(
        promotion.get("AUTO_PUBLISH_ALLOWED") == "publish_with_AUTO_VALIDATED_and_no_human_override",
        "auto-publish approval policy drift",
    )

    module = _read("sanida_fiscal/publication_v1.py")
    for marker in (
        "def assert_release_inventory_complete(",
        "def prepare_published_release(",
        "class FiscalReleaseStore",
        "candidate.supersedes_release_id != previous.release_id",
        "candidate.expected_release_id()",
        "content-addressed release path already exists with different bytes",
        "current release artifact sha256 mismatch",
    ):
        _require(marker in module, f"publication executable marker missing: {marker}")

    tests = _read("tests/test_publication_v1.py")
    for marker in (
        "test_real_phase2_representative_candidate_cannot_be_published_as_complete_release",
        "test_auto_parameter_promotion_builds_consumable_published_release",
        "test_review_required_change_cannot_publish_without_explicit_human_reference",
        "test_no_semantic_release_change_does_not_publish_duplicate",
        "test_successor_must_point_to_exact_current_release",
        "test_release_store_persists_immutable_release_and_atomic_current_pointer",
        "test_release_store_rejects_existing_release_path_with_different_bytes",
        "test_release_store_detects_post_publication_tampering",
    ):
        _require(marker in tests, f"publication regression marker missing: {marker}")

    print("Phase 5 publication foundation: PASS (32/32 completeness + immutable release store anchored)")


if __name__ == "__main__":
    main()
