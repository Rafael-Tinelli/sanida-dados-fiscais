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
from scripts.generate_contract_schema import render_schema

SCHEMA_PATH = ROOT / "contracts" / "fiscal-contract-v1.schema.json"
EXAMPLE_PATH = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"
SOURCE_REGISTRY_PATH = ROOT / "docs" / "source-registry-v1.json"
RULE_INVENTORY_PATH = ROOT / "docs" / "rule-inventory-v1.json"
REFERENCE_CASES_PATH = ROOT / "tests" / "reference_cases" / "phase1_reference_cases.json"

class ValidationError(RuntimeError): pass

def load_json(path: Path) -> Any:
    try: return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc: raise ValidationError(f"cannot load {path.relative_to(ROOT)}: {exc}") from exc

def require(condition: bool, message: str) -> None:
    if not condition: raise ValidationError(message)

def main() -> int:
    for path in (SCHEMA_PATH,EXAMPLE_PATH,SOURCE_REGISTRY_PATH,RULE_INVENTORY_PATH,REFERENCE_CASES_PATH):
        require(path.is_file(), f"required file missing: {path.relative_to(ROOT)}")
    committed_schema_text = SCHEMA_PATH.read_text(encoding="utf-8")
    require(committed_schema_text == render_schema(), "committed JSON Schema is stale; run scripts/generate_contract_schema.py")
    schema = json.loads(committed_schema_text); Draft202012Validator.check_schema(schema)
    example = load_json(EXAMPLE_PATH); Draft202012Validator(schema).validate(example)
    contract = FiscalContractV1.model_validate(example)
    source_registry = load_json(SOURCE_REGISTRY_PATH)
    source_by_id = {source["source_id"]: source for source in source_registry["sources"]}
    inventory_rule_ids = {rule["rule_id"] for rule in load_json(RULE_INVENTORY_PATH)["rules"]}
    reference_case_ids = {case["case_id"] for case in load_json(REFERENCE_CASES_PATH)["cases"]}
    for rule in contract.rules:
        require(rule.rule_id in inventory_rule_ids, f"contract rule absent from Phase 1 inventory: {rule.rule_id}")
        for evidence in rule.provenance:
            require(evidence.source_id in source_by_id, f"unknown source_id {evidence.source_id} in {rule.rule_id}")
            expected_role = source_by_id[evidence.source_id]["role"]
            require(evidence.role.value == expected_role, f"source role mismatch for {evidence.source_id}: contract={evidence.role.value}, registry={expected_role}")
        for case_id in rule.quality.reference_case_ids:
            require(case_id in reference_case_ids, f"unknown reference_case_id {case_id} in {rule.rule_id}")
    reduction = next(rule for rule in contract.rules if rule.rule_id == "irrf.reduction.2026")
    require(reduction.applies_to == "taxable_income_subject_to_monthly_incidence_before_irrf_deductions", "A01 semantic target regressed")
    require(getattr(reduction.payload, "input_semantic", None) == reduction.applies_to, "A01 payload input semantic must match applies_to")
    termination = next(rule for rule in contract.rules if rule.rule_id == "termination.reason_scope")
    require({row.code for row in termination.payload.rows} == {"01","02","07","33"}, "H29 v1 termination scope changed unexpectedly")
    principal = next(rule for rule in contract.rules if rule.rule_id == "vacation.abono.ir_exemption")
    third = next(rule for rule in contract.rules if rule.rule_id == "vacation.abono_constitutional_third.ir_incidence")
    require(principal.payload.components[0].irrf.value == "no", "abono principal IR treatment regressed")
    require(third.payload.components[0].irrf.value == "yes", "abono third IR treatment regressed")
    require(principal.payload.components[0].social_security.value == "no" and third.payload.components[0].social_security.value == "no", "abono social-security treatment regressed")
    print(f"Contract v1 validation: PASS ({len(contract.rules)} rules)")
    print(f"Candidate canonical sha256: {contract.content_sha256()}")
    return 0

if __name__ == "__main__":
    try: raise SystemExit(main())
    except ValidationError as exc:
        print(f"Contract v1 validation: FAIL — {exc}")
        raise SystemExit(1)
