from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

from sanida_fiscal.human_review_v1 import compute_review_key


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


def _candidate(*, rule_version: str, change_class: str, snapshot_sha: str) -> FakeContract:
    return FakeContract(
        {
            "schema_version": "1.2.0",
            "release_id": "candidate",
            "source_registry_version": "1.0.0",
            "governance_source_registry_version": "1.1.0",
            "consumer_compatibility": {
                "schema_version": "1.2.0",
                "contract_api_version": "1.2.0",
            },
            "supersedes_release_id": "published",
            "rules": [
                {
                    "rule_id": "fixture.rule",
                    "rule_version": rule_version,
                    "change_class": change_class,
                    "domain": "technical",
                    "calculators": ["H27"],
                    "contexts": ["technical"],
                    "description": "fixture",
                    "applies_to": "fixture",
                    "applicability": [],
                    "dependencies": [],
                    "calculation_order": 0,
                    "competence": {"basis": "rule_specific"},
                    "vigency": {"effective_from": "2026-01-01"},
                    "rounding_policy": {
                        "decimal_places": 2,
                        "mode": "ROUND_HALF_UP",
                        "stage": "per_component",
                    },
                    "payload": {"type": "policy", "value": "same-semantic-value"},
                    "provenance": [
                        {
                            "source_id": "PLANALTO_TEST",
                            "role": "normative_primary",
                            "status": "AVAILABLE",
                            "retrieval_method": "http_html",
                            "snapshot_sha256": snapshot_sha,
                        }
                    ],
                    "quality": {
                        "status": "VALIDATED",
                        "reference_case_ids": [],
                        "last_validated_at_utc": "2026-09-15T16:00:00Z",
                    },
                    "update_policy": {
                        "rule_class": "structural_rule",
                        "auto_publish": False,
                        "structural_review_required": True,
                    },
                }
            ],
        }
    )


def _diff(change_class: str, *, version: str, changed: bool = True):
    return SimpleNamespace(
        rule_id="fixture.rule",
        occurrence=0,
        change_class=SimpleNamespace(value=change_class),
        changed_paths=("provenance[0].snapshot_sha256", "rule_version", "change_class"),
        previous_rule_version="1.0.0",
        candidate_rule_version=version,
        changed=changed,
    )


def _assessment(*diffs):
    return SimpleNamespace(
        outcome=SimpleNamespace(value="REVIEW_REQUIRED"),
        diff=SimpleNamespace(contract_changed_paths=("governance_source_registry_version",), rule_diffs=tuple(diffs)),
        reasons=("fixture",),
    )


def test_review_key_ignores_refresh_transition_metadata_when_material_identity_is_same():
    normalized_fingerprint = "a" * 64
    refreshed = _candidate(
        rule_version="1.0.1",
        change_class="SOURCE_REFRESH_NO_CHANGE",
        snapshot_sha=normalized_fingerprint,
    )
    unchanged_transition = _candidate(
        rule_version="1.0.0",
        change_class="STRUCTURAL_CHANGE",
        snapshot_sha=normalized_fingerprint,
    )

    key_with_refresh_diff = compute_review_key(
        None,
        refreshed,
        _assessment(_diff("SOURCE_REFRESH_NO_CHANGE", version="1.0.1")),
    )
    key_without_refresh_diff = compute_review_key(
        None,
        unchanged_transition,
        _assessment(),
    )

    assert key_with_refresh_diff == key_without_refresh_diff


def test_review_key_still_changes_when_normalized_evidence_identity_changes():
    first = _candidate(
        rule_version="1.0.1",
        change_class="SOURCE_REFRESH_NO_CHANGE",
        snapshot_sha="a" * 64,
    )
    second = _candidate(
        rule_version="1.0.2",
        change_class="SOURCE_REFRESH_NO_CHANGE",
        snapshot_sha="b" * 64,
    )
    assessment = _assessment(_diff("SOURCE_REFRESH_NO_CHANGE", version="1.0.1"))

    assert compute_review_key(None, first, assessment) != compute_review_key(None, second, assessment)


def test_review_key_keeps_material_non_refresh_diff_metadata():
    candidate = _candidate(
        rule_version="1.1.0",
        change_class="STRUCTURAL_CHANGE",
        snapshot_sha="a" * 64,
    )
    structural = compute_review_key(
        None,
        candidate,
        _assessment(_diff("STRUCTURAL_CHANGE", version="1.1.0")),
    )
    parameter = compute_review_key(
        None,
        candidate,
        _assessment(_diff("PARAMETER_CHANGE", version="1.1.0")),
    )

    assert structural != parameter
