#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"
COVERAGE = ROOT / "docs" / "contract-coverage-v1.json"
VALIDATOR = ROOT / "scripts" / "validate_contract_v1.py"
TESTS = ROOT / "tests" / "test_contract_v1.py"


def dump(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def update_example() -> None:
    data = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    data["versioning_policy"] = {
        "schema_versioning": "semver",
        "contract_api_versioning": "semver",
        "rule_versioning": "semver",
        "release_id_strategy": "content_addressed_sha256",
        "schema_compatibility": "exact",
        "contract_api_compatibility": "exact",
        "backward_compatibility": "explicitly_tested_only",
        "forward_compatibility": "not_assumed",
    }
    data["consumer_compatibility"].update(
        {
            "schema_version": "1.0.0",
            "schema_match": "exact",
            "contract_api_match": "exact",
            "unknown_fields": "reject",
            "unknown_payload_types": "reject",
        }
    )

    competence_keys = {
        "thirteenth.accrual.twelfths": "thirteenth_accrual_reference_year_or_termination_period",
        "thirteenth.reference_remuneration": "thirteenth_reference_remuneration_cycle",
        "thirteenth.variable_remuneration": "thirteenth_variable_remuneration_cycle",
        "technical.money_decimal_and_rounding": "technical_non_temporal",
        "technical.contract_vigency_and_quality": "technical_non_temporal",
    }
    policy_assertions = {
        "deductions_by_income_type": [
            "deductions_must_match_assessment_context",
            "pension_must_not_be_silently_zeroed_when_applicable",
        ],
        "income_type_partition": [
            "monthly_thirteenth_and_vacation_are_distinct_assessments"
        ],
        "separate_social_security_assessment": [
            "thirteenth_social_security_is_separate_from_monthly"
        ],
        "exclusive_irrf_assessment": ["thirteenth_irrf_is_exclusive_assessment"],
        "separate_vacation_irrf_assessment": ["vacation_irrf_is_separate_assessment"],
        "money_decimal_and_rounding": [
            "money_uses_decimal",
            "rounding_must_be_explicit_per_formula_stage",
        ],
        "contract_vigency_and_quality": [
            "generated_at_does_not_prove_vigency",
            "consumer_rejects_incompatible_vigency_or_quality",
        ],
    }

    for rule in data["rules"]:
        rule_id = rule["rule_id"]
        if rule["competence"]["basis"] == "rule_specific":
            rule["competence"]["rule_specific_key"] = competence_keys[rule_id]

        payload = rule["payload"]
        ptype = payload["type"]
        if ptype == "progressive_table":
            payload["calculation_method"] = (
                "marginal_by_bracket"
                if rule_id == "inss.employee.progressive_table"
                else "rate_times_base_minus_deduction"
            )
        elif ptype == "affine_reduction":
            payload["full_relief_behavior"] = "max_reduction"
            payload["phaseout_formula"] = "intercept_minus_slope_times_input"
            payload["above_phaseout_behavior"] = "zero"
        elif ptype == "threshold_accrual":
            payload["method"] = "one_unit_per_month_if_days_gte_threshold"
        elif ptype == "entitlement_bands":
            payload["input_semantic"] = "unjustified_absences_in_acquisition_period"
        elif ptype in {"eligibility_matrix", "code_eligibility"}:
            payload["code_system"] = "esocial_table_19_termination_reason"
        elif ptype == "policy":
            payload.pop("values", None)
            payload["assertions"] = policy_assertions[payload["policy_kind"]]

    dump(EXAMPLE, data)


def update_coverage() -> None:
    data = json.loads(COVERAGE.read_text(encoding="utf-8"))
    competence_keys = {
        "thirteenth.accrual.twelfths": "thirteenth_accrual_reference_year_or_termination_period",
        "thirteenth.reference_remuneration": "thirteenth_reference_remuneration_cycle",
        "thirteenth.variable_remuneration": "thirteenth_variable_remuneration_cycle",
        "technical.money_decimal_and_rounding": "technical_non_temporal",
        "technical.contract_vigency_and_quality": "technical_non_temporal",
    }
    for item in data["rules"]:
        rule_id = item["rule_id"]
        if rule_id in competence_keys:
            item["competence_key"] = competence_keys[rule_id]
        if rule_id == "inss.employee.progressive_table":
            item["calculation_method"] = "marginal_by_bracket"
        elif rule_id == "irrf.monthly.progressive_table":
            item["calculation_method"] = "rate_times_base_minus_deduction"
        elif rule_id == "irrf.dependent_deduction":
            item["unit"] = "BRL_per_dependent"
        elif rule_id == "irrf.simplified_monthly_discount":
            item["unit"] = "BRL"
    dump(COVERAGE, data)


def update_validator() -> None:
    text = VALIDATOR.read_text(encoding="utf-8")

    anchor = '''        require(\n            set(calculator.value for calculator in rule.calculators)\n            == set(inventory_rule["calculators"]),\n            f"{rule.rule_id} calculator set mismatch",\n        )\n'''
    addition = anchor + '''        require(\n            rule.applies_to == inventory_rule["applies_to"],\n            f"{rule.rule_id} applies_to diverges from Phase 1 inventory",\n        )\n'''
    if 'applies_to diverges from Phase 1 inventory' not in text:
        text = text.replace(anchor, addition, 1)

    anchor = '''        require(\n            rule.competence.basis.value == coverage_rule["competence_basis"],\n            f"{rule.rule_id} competence basis mismatch with coverage map",\n        )\n'''
    addition = anchor + '''        if coverage_rule["competence_basis"] == "rule_specific":\n            require(\n                "competence_key" in coverage_rule,\n                f"{rule.rule_id} rule_specific coverage requires competence_key",\n            )\n            require(\n                rule.competence.rule_specific_key is not None\n                and rule.competence.rule_specific_key.value == coverage_rule["competence_key"],\n                f"{rule.rule_id} typed competence key mismatch",\n            )\n        else:\n            require(\n                "competence_key" not in coverage_rule,\n                f"{rule.rule_id} has competence_key without rule_specific basis",\n            )\n'''
    if 'typed competence key mismatch' not in text:
        text = text.replace(anchor, addition, 1)

    anchor = '''        require(\n            rule.payload.type == coverage_rule["payload_type"],\n            f"{rule.rule_id} payload family mismatch with coverage map",\n        )\n'''
    addition = anchor + '''        if rule.payload.type == "progressive_table":\n            require(\n                "calculation_method" in coverage_rule,\n                f"{rule.rule_id} progressive table requires calculation_method",\n            )\n            require(\n                rule.payload.calculation_method.value == coverage_rule["calculation_method"],\n                f"{rule.rule_id} progressive calculation method mismatch",\n            )\n        if rule.payload.type == "scalar":\n            require(\n                "unit" in coverage_rule,\n                f"{rule.rule_id} scalar coverage requires unit",\n            )\n            require(\n                rule.payload.unit.value == coverage_rule["unit"],\n                f"{rule.rule_id} scalar unit mismatch",\n            )\n'''
    if 'progressive calculation method mismatch' not in text:
        text = text.replace(anchor, addition, 1)

    text = text.replace(
        '    print(f"Candidate canonical sha256: {contract.content_sha256()}")\n',
        '    print(f"Candidate canonical sha256: {contract.content_sha256()}")\n'
        '    print(f"Candidate immutable payload sha256: {contract.release_payload_sha256()}")\n',
    )
    VALIDATOR.write_text(text, encoding="utf-8")


def update_tests() -> None:
    text = TESTS.read_text(encoding="utf-8")
    text = text.replace(
        '''from sanida_fiscal.contract_v1 import (\n    AssessmentContext,\n    ContractNotConsumableError,\n    FiscalContractV1,\n    QualityStatus,\n    RuleSelectionError,\n)\n''',
        '''from sanida_fiscal.contract_v1 import (\n    AssessmentContext,\n    ContractCompatibilityError,\n    ContractNotConsumableError,\n    FiscalContractV1,\n    QualityStatus,\n    ReleaseTransitionError,\n    RuleSelectionError,\n    semver_bump,\n)\n''',
    )
    text = text.replace(
        '''    PeriodStatePayload,\n    PolicyKind,\n    PolicyPayload,\n''',
        '''    PeriodStatePayload,\n    POLICY_ASSERTIONS_BY_KIND,\n    PolicyKind,\n    PolicyPayload,\n    VersionBump,\n''',
    )

    helper_anchor = '''def add_hashes(data: dict) -> None:\n    fake_hash = "a" * 64\n    for rule in data["rules"]:\n        available = next(\n            evidence\n            for evidence in rule["provenance"]\n            if evidence["status"] == "AVAILABLE"\n        )\n        available["snapshot_sha256"] = fake_hash\n        available["snapshot_path"] = (\n            f"raw/{rule['rule_id'].replace('.', '_')}/snapshot.bin"\n        )\n\n\n'''
    helper_addition = helper_anchor + '''def set_canonical_release_id(data: dict) -> None:\n    candidate = deepcopy(data)\n    candidate["status"] = "CANDIDATE"\n    candidate["lifecycle"] = {}\n    candidate["release_id"] = "candidate-for-hash"\n    contract = FiscalContractV1.model_validate(candidate)\n    data["release_id"] = contract.expected_release_id()\n\n\n'''
    if 'def set_canonical_release_id' not in text:
        text = text.replace(helper_anchor, helper_addition, 1)

    text = text.replace(
        '''    data["status"] = "PUBLISHED"\n    data["lifecycle"] = {\n        "validated_at_utc": "2026-09-13T13:00:00Z",\n        "published_at_utc": "2026-09-13T13:05:00Z",\n        "approval_mode": "AUTO_VALIDATED",\n        "approval_reference": "ci:synthetic",\n    }\n    with pytest.raises(ValidationError, match="HUMAN_REVIEWED"):\n''',
        '''    set_canonical_release_id(data)\n    data["status"] = "PUBLISHED"\n    data["lifecycle"] = {\n        "validated_at_utc": "2026-09-13T13:00:00Z",\n        "published_at_utc": "2026-09-13T13:05:00Z",\n        "approval_mode": "AUTO_VALIDATED",\n        "approval_reference": "ci:synthetic",\n    }\n    with pytest.raises(ValidationError, match="HUMAN_REVIEWED"):\n''',
        1,
    )
    text = text.replace(
        '''    data["status"] = "PUBLISHED"\n    data["lifecycle"] = {\n        "validated_at_utc": "2026-09-13T13:00:00Z",\n        "published_at_utc": "2026-09-13T13:05:00Z",\n        "approval_mode": "HUMAN_REVIEWED",\n        "approval_reference": "review:synthetic",\n    }\n    contract = FiscalContractV1.model_validate(data)\n''',
        '''    set_canonical_release_id(data)\n    data["status"] = "PUBLISHED"\n    data["lifecycle"] = {\n        "validated_at_utc": "2026-09-13T13:00:00Z",\n        "published_at_utc": "2026-09-13T13:05:00Z",\n        "approval_mode": "HUMAN_REVIEWED",\n        "approval_reference": "review:synthetic",\n    }\n    contract = FiscalContractV1.model_validate(data)\n''',
        1,
    )

    old_superseded = '''def test_superseded_release_requires_successor_metadata() -> None:\n    data = load_example()\n    add_hashes(data)\n    data["status"] = "SUPERSEDED"\n    data["lifecycle"] = {\n        "validated_at_utc": "2026-09-13T13:00:00Z",\n        "published_at_utc": "2026-09-13T13:05:00Z",\n        "approval_mode": "HUMAN_REVIEWED",\n        "approval_reference": "review:synthetic",\n    }\n    with pytest.raises(ValidationError, match="successor metadata"):\n        FiscalContractV1.model_validate(data)\n\n\n'''
    new_superseded = '''def test_superseded_is_not_a_mutable_release_status() -> None:\n    data = load_example()\n    data["status"] = "SUPERSEDED"\n    with pytest.raises(ValidationError):\n        FiscalContractV1.model_validate(data)\n\n\n'''
    text = text.replace(old_superseded, new_superseded, 1)

    text = text.replace(
        '''    payload = PolicyPayload(\n        policy_kind=kind,\n        assertions=["phase1_semantics_preserved"],\n        values={},\n    )\n''',
        '''    policy_kind = PolicyKind(kind)\n    payload = PolicyPayload(\n        policy_kind=policy_kind,\n        assertions=list(POLICY_ASSERTIONS_BY_KIND[policy_kind]),\n    )\n''',
    )

    extra = r'''


def test_versioning_policy_is_frozen_and_exact_for_v1() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    assert contract.versioning_policy.schema_versioning == "semver"
    assert contract.versioning_policy.contract_api_versioning == "semver"
    assert contract.versioning_policy.rule_versioning == "semver"
    assert contract.versioning_policy.release_id_strategy == "content_addressed_sha256"
    assert contract.versioning_policy.schema_compatibility == "exact"
    assert contract.versioning_policy.contract_api_compatibility == "exact"
    assert contract.versioning_policy.forward_compatibility == "not_assumed"


def test_semver_bump_classification() -> None:
    assert semver_bump("1.0.0", "2.0.0") == VersionBump.MAJOR
    assert semver_bump("1.0.0", "1.1.0") == VersionBump.MINOR
    assert semver_bump("1.0.0", "1.0.1") == VersionBump.PATCH
    assert semver_bump("1.0.0", "1.0.0") is None
    with pytest.raises(ValueError, match="backwards"):
        semver_bump("1.1.0", "1.0.9")


def test_reader_compatibility_is_exact_and_unknown_versions_fail() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    contract.assert_reader_compatible(
        consumer="H26", schema_version="1.0.0", contract_api_version="1.0.0"
    )
    with pytest.raises(ContractCompatibilityError, match="schema mismatch"):
        contract.assert_reader_compatible(
            consumer="H26", schema_version="1.0.1", contract_api_version="1.0.0"
        )
    with pytest.raises(ContractCompatibilityError, match="contract API mismatch"):
        contract.assert_reader_compatible(
            consumer="H26", schema_version="1.0.0", contract_api_version="1.1.0"
        )


def test_validated_release_id_is_content_addressed() -> None:
    data = load_example()
    add_hashes(data)
    data["status"] = "VALIDATED"
    data["lifecycle"] = {"validated_at_utc": "2026-09-13T13:00:00Z"}
    with pytest.raises(ValidationError, match="content-addressed"):
        FiscalContractV1.model_validate(data)
    set_canonical_release_id(data)
    contract = FiscalContractV1.model_validate(data)
    assert contract.release_id == contract.expected_release_id()
    assert contract.release_id.startswith("fiscal-v1-sha256-")


def test_published_release_artifact_is_immutable() -> None:
    data = load_example()
    add_hashes(data)
    set_canonical_release_id(data)
    data["status"] = "PUBLISHED"
    data["lifecycle"] = {
        "validated_at_utc": "2026-09-13T13:00:00Z",
        "published_at_utc": "2026-09-13T13:05:00Z",
        "approval_mode": "HUMAN_REVIEWED",
        "approval_reference": "review:immutability",
    }
    published = FiscalContractV1.model_validate(data)
    identical = FiscalContractV1.model_validate(deepcopy(data))
    identical.assert_published_immutable_against(published)

    changed = deepcopy(data)
    changed["notes"] = "mutated after publication"
    changed_contract = FiscalContractV1.model_validate(changed)
    with pytest.raises(ReleaseTransitionError, match="immutable"):
        changed_contract.assert_published_immutable_against(published)


def test_supersession_is_declared_by_successor_without_mutating_predecessor() -> None:
    data = load_example()
    add_hashes(data)
    set_canonical_release_id(data)
    data["status"] = "PUBLISHED"
    data["lifecycle"] = {
        "validated_at_utc": "2026-09-13T13:00:00Z",
        "published_at_utc": "2026-09-13T13:05:00Z",
        "approval_mode": "HUMAN_REVIEWED",
        "approval_reference": "review:predecessor",
    }
    predecessor = FiscalContractV1.model_validate(data)

    successor_data = load_example()
    successor_data["release_id"] = "candidate-successor"
    successor_data["supersedes_release_id"] = predecessor.release_id
    successor = FiscalContractV1.model_validate(successor_data)
    successor.assert_supersedes(predecessor)
    assert predecessor.status.value == "PUBLISHED"


def test_progressive_table_method_is_explicit() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = next(rule for rule in contract.rules if rule.rule_id == "irrf.monthly.progressive_table")
    assert rule.payload.calculation_method.value == "rate_times_base_minus_deduction"


def test_affine_reduction_formula_is_explicit() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = next(rule for rule in contract.rules if rule.rule_id == "irrf.reduction.2026")
    assert rule.payload.full_relief_behavior == "max_reduction"
    assert rule.payload.phaseout_formula == "intercept_minus_slope_times_input"
    assert rule.payload.above_phaseout_behavior == "zero"


def test_rule_specific_competence_uses_typed_key() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = next(rule for rule in contract.rules if rule.rule_id == "thirteenth.accrual.twelfths")
    assert (
        rule.competence.rule_specific_key.value
        == "thirteenth_accrual_reference_year_or_termination_period"
    )
'''
    if 'test_versioning_policy_is_frozen_and_exact_for_v1' not in text:
        text += extra

    TESTS.write_text(text, encoding="utf-8")


def main() -> int:
    update_example()
    update_coverage()
    update_validator()
    update_tests()
    print("Phase 2 final contract round synchronized")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
