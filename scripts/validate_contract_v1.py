#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from jsonschema import Draft202012Validator
from sanida_fiscal.contract_v1 import FiscalContractV1
from sanida_fiscal.types_v1 import (
    AssessmentContext,
    CompetenceBasis,
    Domain,
    PolicyKind,
    RULE_PAYLOAD_TYPES,
    RuleClass,
    SourceObservationStatus,
)
from scripts.generate_contract_schema import render_schema

SCHEMA_PATH = ROOT / "contracts" / "fiscal-contract-v1.schema.json"
EXAMPLE_PATH = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"
COVERAGE_PATH = ROOT / "docs" / "contract-coverage-v1.json"
SOURCE_REGISTRY_PATH = ROOT / "docs" / "source-registry-v1.json"
RULE_INVENTORY_PATH = ROOT / "docs" / "rule-inventory-v1.json"
REFERENCE_CASES_PATH = ROOT / "tests" / "reference_cases" / "phase1_reference_cases.json"


class ValidationError(RuntimeError):
    pass


def load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValidationError(
            f"cannot load {path.relative_to(ROOT)}: {exc}"
        ) from exc


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def main() -> int:
    required_paths = (
        SCHEMA_PATH,
        EXAMPLE_PATH,
        COVERAGE_PATH,
        SOURCE_REGISTRY_PATH,
        RULE_INVENTORY_PATH,
        REFERENCE_CASES_PATH,
    )
    for path in required_paths:
        require(path.is_file(), f"required file missing: {path.relative_to(ROOT)}")

    committed_schema_text = SCHEMA_PATH.read_text(encoding="utf-8")
    require(
        committed_schema_text == render_schema(),
        "committed JSON Schema is stale; run scripts/generate_contract_schema.py",
    )
    schema = json.loads(committed_schema_text)
    Draft202012Validator.check_schema(schema)

    example = load_json(EXAMPLE_PATH)
    Draft202012Validator(schema).validate(example)
    contract = FiscalContractV1.model_validate(example)

    source_registry = load_json(SOURCE_REGISTRY_PATH)
    rule_inventory = load_json(RULE_INVENTORY_PATH)
    reference_cases = load_json(REFERENCE_CASES_PATH)
    coverage = load_json(COVERAGE_PATH)

    require(
        contract.source_registry_version == source_registry["schema_version"],
        "contract/source-registry version mismatch",
    )
    require(
        contract.rule_inventory_version == rule_inventory["schema_version"],
        "contract/rule-inventory version mismatch",
    )
    require(
        coverage["inventory_version"] == rule_inventory["schema_version"],
        "coverage/inventory version mismatch",
    )

    source_by_id = {
        source["source_id"]: source for source in source_registry["sources"]
    }
    inventory_by_id = {
        rule["rule_id"]: rule for rule in rule_inventory["rules"]
    }
    reference_case_ids = {
        case["case_id"] for case in reference_cases["cases"]
    }

    coverage_rules = coverage["rules"]
    coverage_by_id = {item["rule_id"]: item for item in coverage_rules}
    require(
        len(coverage_by_id) == len(coverage_rules),
        "coverage contains duplicate rule_id",
    )
    require(
        coverage["rule_count"] == len(coverage_rules),
        "coverage rule_count does not match entries",
    )
    require(
        set(coverage_by_id) == set(inventory_by_id),
        "contract coverage must match the Phase 1 inventory exactly",
    )

    used_payload_types = set()
    used_policy_kinds = set()
    for rule_id, item in coverage_by_id.items():
        inventory_rule = inventory_by_id[rule_id]
        require(
            item["representation_status"] == "covered",
            f"{rule_id} is not marked covered",
        )
        require(
            item["rule_class"] == inventory_rule["rule_class"],
            f"{rule_id} rule_class diverges from Phase 1 inventory",
        )
        require(
            set(item["calculators"]) == set(inventory_rule["calculators"]),
            f"{rule_id} calculator coverage diverges from Phase 1 inventory",
        )
        try:
            RuleClass(item["rule_class"])
            Domain(item["domain"])
            CompetenceBasis(item["competence_basis"])
            for context in item["contexts"]:
                AssessmentContext(context)
        except ValueError as exc:
            raise ValidationError(
                f"invalid coverage enum for {rule_id}: {exc}"
            ) from exc

        payload_type = item["payload_type"]
        require(
            payload_type in RULE_PAYLOAD_TYPES,
            f"{rule_id} uses unknown payload type {payload_type}",
        )
        used_payload_types.add(payload_type)

        if payload_type == "policy":
            require(
                "policy_kind" in item,
                f"{rule_id} policy coverage requires policy_kind",
            )
            try:
                PolicyKind(item["policy_kind"])
            except ValueError as exc:
                raise ValidationError(
                    f"invalid policy_kind for {rule_id}: {item['policy_kind']}"
                ) from exc
            used_policy_kinds.add(item["policy_kind"])
        else:
            require(
                "policy_kind" not in item,
                f"{rule_id} has policy_kind but is not a policy payload",
            )

    require(
        used_payload_types == RULE_PAYLOAD_TYPES,
        "schema payload union and Phase 1 coverage families diverge",
    )
    require(
        used_policy_kinds == {kind.value for kind in PolicyKind},
        "not all typed policy kinds are anchored in Phase 1 inventory coverage",
    )

    candidate_payload_types = {rule.payload.type for rule in contract.rules}
    require(
        candidate_payload_types == RULE_PAYLOAD_TYPES,
        "CANDIDATE must materialize at least one rule for every payload family",
    )

    for rule in contract.rules:
        require(
            rule.rule_id in inventory_by_id,
            f"contract rule absent from Phase 1 inventory: {rule.rule_id}",
        )
        inventory_rule = inventory_by_id[rule.rule_id]
        coverage_rule = coverage_by_id[rule.rule_id]

        require(
            rule.update_policy.rule_class.value == inventory_rule["rule_class"],
            f"{rule.rule_id} rule_class mismatch between contract and inventory",
        )
        require(
            set(calculator.value for calculator in rule.calculators)
            == set(inventory_rule["calculators"]),
            f"{rule.rule_id} calculator set mismatch",
        )
        require(
            {context.value for context in rule.contexts}
            == set(coverage_rule["contexts"]),
            f"{rule.rule_id} context set mismatch with coverage map",
        )
        require(
            rule.competence.basis.value == coverage_rule["competence_basis"],
            f"{rule.rule_id} competence basis mismatch with coverage map",
        )
        require(
            rule.payload.type == coverage_rule["payload_type"],
            f"{rule.rule_id} payload family mismatch with coverage map",
        )
        if rule.payload.type == "policy":
            require(
                rule.payload.policy_kind.value == coverage_rule["policy_kind"],
                f"{rule.rule_id} policy_kind mismatch with coverage map",
            )

        allowed_sources = set(inventory_rule["source_ids"])
        observed_sources = {evidence.source_id for evidence in rule.provenance}
        require(observed_sources, f"{rule.rule_id} requires provenance")
        require(
            observed_sources <= allowed_sources,
            f"{rule.rule_id} provenance contains source not authorized by Phase 1 inventory",
        )

        for evidence in rule.provenance:
            require(
                evidence.source_id in source_by_id,
                f"unknown source_id {evidence.source_id} in {rule.rule_id}",
            )
            expected_role = source_by_id[evidence.source_id]["role"]
            require(
                evidence.role.value == expected_role,
                f"source role mismatch for {evidence.source_id}: "
                f"contract={evidence.role.value}, registry={expected_role}",
            )

        require(
            any(
                evidence.status == SourceObservationStatus.AVAILABLE
                for evidence in rule.provenance
            ),
            f"{rule.rule_id} candidate has no available official evidence",
        )

        for case_id in rule.quality.reference_case_ids:
            require(
                case_id in reference_case_ids,
                f"unknown reference_case_id {case_id} in {rule.rule_id}",
            )

    reduction = next(
        rule for rule in contract.rules if rule.rule_id == "irrf.reduction.2026"
    )
    require(
        reduction.applies_to
        == "taxable_income_subject_to_monthly_incidence_before_irrf_deductions",
        "A01 semantic target regressed",
    )
    require(
        getattr(reduction.payload, "input_semantic", None) == reduction.applies_to,
        "A01 payload input semantic must match applies_to",
    )

    termination = next(
        rule for rule in contract.rules if rule.rule_id == "termination.reason_scope"
    )
    require(
        {row.code for row in termination.payload.rows} == {"01", "02", "07", "33"},
        "H29 v1 termination scope changed unexpectedly",
    )

    principal = next(
        rule
        for rule in contract.rules
        if rule.rule_id == "vacation.abono.ir_exemption"
    )
    third = next(
        rule
        for rule in contract.rules
        if rule.rule_id == "vacation.abono_constitutional_third.ir_incidence"
    )
    require(
        principal.payload.components[0].irrf.value == "no",
        "abono principal IR treatment regressed",
    )
    require(
        third.payload.components[0].irrf.value == "yes",
        "abono third IR treatment regressed",
    )
    require(
        principal.payload.components[0].social_security.value == "no"
        and third.payload.components[0].social_security.value == "no",
        "abono social-security treatment regressed",
    )

    print(
        "Contract v1 validation: PASS "
        f"({len(contract.rules)} candidate rules; "
        f"{len(inventory_by_id)}/{len(inventory_by_id)} inventory rules covered; "
        f"{len(RULE_PAYLOAD_TYPES)} payload families)"
    )
    print(f"Candidate canonical sha256: {contract.content_sha256()}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValidationError as exc:
        print(f"Contract v1 validation: FAIL — {exc}")
        raise SystemExit(1)
