#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.authority_evidence_v12 import PARSER_BINDINGS, select_authority_sources
from sanida_fiscal.governance_evidence_v12 import load_governance_registry
from sanida_fiscal.release_assembler_v12 import MISSING_RULE_SPECS
from scripts.generate_contract_schema_v12 import SCHEMA_ID, build_schema


EXPECTED_GAP = {
    "inss.employee.progressive_table",
    "irrf.income_type",
    "irrf.simplified_monthly_discount",
    "technical.contract_vigency_and_quality",
    "technical.money_decimal_and_rounding",
    "termination.vacation_indemnity_tax_treatment",
    "thirteenth.inss.separate_assessment",
    "thirteenth.irrf.exclusive_assessment",
    "vacation.inss.enjoyed",
    "vacation.irrf.reduction.2026",
    "vacation.irrf.separate_assessment",
}
EXPECTED_TECHNICAL = {
    "technical.money_decimal_and_rounding",
    "technical.contract_vigency_and_quality",
}
EXPECTED_PARSER_SOURCES = {
    "RFB_IRRF_TABLE_2026",
    "INSS_TABLE_2026",
}


def _load(path: str):
    return json.loads((ROOT / path).read_text(encoding="utf-8"))


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"Phase 5 closure gate: {message}")


def validate_closure_policy() -> None:
    policy = _load("docs/phase5-closure-v1.json")
    _require(policy.get("status") == "formal_closure_authorized", "closure policy does not authorize Phase 5 closure")
    contract = policy.get("contract", {})
    _require(contract.get("historical_schema_preserved") == "1.1.0", "v1.1 history no longer preserved")
    _require(contract.get("publication_schema") == "1.2.0", "publication schema is not v1.2")
    _require(contract.get("contract_api_version") == "1.2.0", "contract API is not v1.2")
    _require(contract.get("inventory_rule_count") == 32, "release inventory is not 32")
    _require(contract.get("release_requires_exact_inventory") is True, "exact 32/32 gate disabled")
    _require(contract.get("v11_representative_candidate_publishable") is False, "21-rule v1.1 candidate became publishable")

    evidence = policy.get("evidence_boundary", {})
    _require(evidence.get("technical_rules_use_internal_governance_only") is True, "technical evidence boundary weakened")
    _require(evidence.get("legal_fiscal_rules_reject_internal_governance") is True, "internal evidence became legal authority")
    _require(evidence.get("technical_rules_reject_external_authority_as_own_governance") is True, "technical rules may misattribute official authority")

    promotion = policy.get("automatic_promotion", {})
    _require(promotion.get("requires_hashed_evidence") is True, "hashed evidence no longer required")
    _require(promotion.get("requires_parser_backing_for_automatic_changed_source") is True, "parser backing gate disabled")
    _require(set(promotion.get("current_parser_bindings", [])) == EXPECTED_PARSER_SOURCES, "automatic parser binding set drift")
    _require(promotion.get("structural_raw_snapshots_are_not_interpreted_automatically") is True, "structural source interpretation became automatic")

    publication = policy.get("publication", {})
    _require(publication.get("bootstrap_mode") == "manual_human_review_only", "bootstrap is not human-reviewed only")
    _require(publication.get("scheduled_bootstrap_allowed") is False, "scheduled bootstrap became allowed")
    _require(publication.get("release_files_immutable") is True, "release immutability disabled")
    _require(publication.get("current_pointer_atomic") is True, "current pointer is not atomic")
    _require(publication.get("writer_concurrency_group") == "sanida-dados-fiscais-writes-main", "writer concurrency group drift")
    _require(publication.get("first_production_release_status") == "pending_manual_bootstrap_after_merge", "bootstrap status drift")
    schema = policy.get("schema", {})
    _require(schema.get("canonical_file_materialization") == "publication_workflow", "public schema materialization boundary drift")
    _require(policy.get("next_phase") == "Fase 6 — Migração dos consumidores", "handoff target drift")


def validate_v12_schema_contract() -> None:
    schema = build_schema()
    _require(schema.get("$id") == SCHEMA_ID, "v1.2 schema id drift")
    _require(schema.get("title") == "Sanida Fiscal Contract v1.2", "v1.2 schema title drift")
    definitions = schema.get("$defs", {})
    _require("GovernanceEvidenceObservation" in definitions, "governance evidence missing from v1.2 schema")
    _require("ConsumerCompatibilityV12" in definitions, "v1.2 consumer compatibility missing from schema")
    properties = schema.get("properties", {})
    _require("governance_source_registry_version" in properties, "v1.2 governance registry version missing")
    schema_version = properties.get("schema_version", {})
    _require(schema_version.get("const") == "1.2.0", "v1.2 schema literal drift")

    model = _read("sanida_fiscal/contract_v1_2.py")
    for marker in (
        'schema_version: Literal["1.2.0"]',
        'contract_api_version: Literal["1.2.0"]',
        "technical contract rule requires internal governance evidence",
        "legal/fiscal rules cannot use internal governance evidence as authority",
        "technical contract rule must not misattribute external official evidence as its authority",
        'if schema_version == "1.1.0"',
        'if schema_version == "1.2.0"',
    ):
        _require(marker in model, f"v1.2 contract marker missing: {marker}")


def validate_governance_registry() -> None:
    registry = load_governance_registry(ROOT / "docs/governance-source-registry-v1.json")
    covered = {rule_id for item in registry.values() for rule_id in item["covers"]}
    _require(covered == EXPECTED_TECHNICAL, "governance registry does not cover technical rules exactly")
    for item in registry.values():
        _require(item["role"] == "internal_governance", "governance registry role drift")
        _require(item.get("repository_paths"), "governance source has no repository paths")

    builder = _read("sanida_fiscal/governance_evidence_v12.py")
    for marker in (
        "repository path escapes root",
        "governance evidence hash collision or corruption",
        "technical.money_decimal_and_rounding",
        "technical.contract_vigency_and_quality",
    ):
        _require(marker in builder, f"governance evidence marker missing: {marker}")


def validate_32_rule_assembler() -> None:
    _require(set(MISSING_RULE_SPECS) == EXPECTED_GAP, "historical 21→32 gap changed")
    coverage = _load("docs/contract-coverage-v1.json")
    inventory = _load("docs/rule-inventory-v1.json")
    coverage_ids = {item["rule_id"] for item in coverage["rules"]}
    inventory_ids = {item["rule_id"] for item in inventory["rules"]}
    _require(coverage.get("rule_count") == 32, "coverage rule_count is not 32")
    _require(len(coverage_ids) == 32, "coverage does not contain 32 unique rules")
    _require(coverage_ids == inventory_ids, "coverage and inventory rule sets diverge")

    assembler = _read("sanida_fiscal/release_assembler_v12.py")
    for marker in (
        "historical template gap drift",
        "successor automation requires current PUBLISHED schema 1.2.0",
        "assembled rule set is not 32/32",
        "_reuse_previous_evidence_if_same_hash",
        "_hydrate_parameter_rules",
        "candidate = _reconcile_declared_transition(previous, candidate)",
        "vacation.irrf.reduction.2026",
        "termination.vacation_indemnity_tax_treatment",
    ):
        _require(marker in assembler, f"assembler marker missing: {marker}")


def validate_authority_boundary() -> None:
    selected = select_authority_sources(
        source_registry_path=ROOT / "docs/source-registry-v1.json",
        rule_inventory_path=ROOT / "docs/rule-inventory-v1.json",
    )
    _require(len(selected) == 30, "authority selection does not cover exactly 30 external rules")
    _require(not (set(selected) & EXPECTED_TECHNICAL), "technical rule leaked into official authority selection")
    _require(set(PARSER_BINDINGS) == EXPECTED_PARSER_SOURCES, "automatic parser set expanded without contract review")

    collector = _read("sanida_fiscal/authority_evidence_v12.py")
    for marker in (
        "Structural sources are snapshotted but never interpreted automatically here.",
        "if source_id in PARSER_BINDINGS",
        "parser incompatible",
        "expected authority selection for 30 external rules",
    ):
        _require(marker in collector, f"authority collector marker missing: {marker}")


def validate_publication_and_workflow() -> None:
    publication = _read("sanida_fiscal/publication_v1.py")
    for marker in (
        "assert_release_inventory_complete",
        "reconcile_transition_change_classes",
        "parse_fiscal_contract(data)",
        "parse_fiscal_contract(payload)",
        "content-addressed release path already exists with different bytes",
        "current release artifact sha256 mismatch",
    ):
        _require(marker in publication, f"publication marker missing: {marker}")

    orchestrator = _read("scripts/publish_fiscal_release_v12.py")
    for marker in (
        "if previous is None and args.scheduled",
        "bootstrap pending; scheduled run cannot create first release",
        "PromotionOutcome.NO_PUBLISH_REQUIRED",
        "PromotionOutcome.REVIEW_REQUIRED",
        "--human-approval-reference",
        "store.publish(published)",
    ):
        _require(marker in orchestrator, f"orchestrator marker missing: {marker}")

    workflow = _read(".github/workflows/fiscal-release-v12.yml")
    for marker in (
        "workflow_dispatch:",
        "approval_reference:",
        'cron: "40 9 * * *"',
        "contents: write",
        "group: sanida-dados-fiscais-writes-main",
        "python scripts/generate_contract_schema_v12.py",
        "--scheduled",
        "--human-approval-reference",
        "python scripts/publish_fiscal_release_v12.py",
        "python scripts/validate_phase5_closure_gate.py",
        "contracts/fiscal-contract-v1.2.schema.json",
        "evidence/fiscal-authority-v1",
        "evidence/governance-v1",
        "releases/fiscal-v1",
    ):
        _require(marker in workflow, f"publication workflow marker missing: {marker}")


def validate_regression_tests_and_docs() -> None:
    tests = _read("tests/test_phase5_v12.py")
    for marker in (
        "test_v12_bootstrap_materializes_exact_32_rule_contract",
        "test_v12_bootstrap_requires_explicit_human_approval_and_store_reloads_v12",
        "test_v12_identical_successor_is_idempotent_even_with_new_observation_time",
        "test_v12_parser_backed_numeric_refresh_can_auto_publish",
        "test_v12_structural_authority_refresh_requires_human_review",
        "test_v12_governance_registry_builds_hash_addressed_internal_evidence",
    ):
        _require(marker in tests, f"v1.2 regression test marker missing: {marker}")

    docs = _read("docs/phase5-v12-publication.md")
    for marker in (
        "Contrato Fiscal v1.2",
        "32 regras",
        "GovernanceEvidenceObservation",
        "Bootstrap exige",
        "NO_PUBLISH_REQUIRED",
        "Fase 6 — Migração dos consumidores",
    ):
        _require(marker in docs, f"Phase 5 documentation marker missing: {marker}")

    # Like the Phase 4 gate, this gate validates Phase 5's immutable closure facts
    # without forcing the master README to keep a stale "next phase" or bootstrap
    # status after Phase 6 has legitimately advanced the project.
    readme = _read("README.md")
    for marker in (
        "### Fase 5 — Diff semântico e gates de publicação\n\n**Status: CONCLUÍDA**",
        "schema/API 1.2.0",
        "### Fase 6 — Migração dos consumidores",
        "### Fase 7 — Fechamento e operação evergreen",
    ):
        _require(marker in readme, f"README Phase 5 closure/boundary marker missing: {marker}")


def main() -> None:
    validate_closure_policy()
    validate_v12_schema_contract()
    validate_governance_registry()
    validate_32_rule_assembler()
    validate_authority_boundary()
    validate_publication_and_workflow()
    validate_regression_tests_and_docs()
    print(
        "Phase 5 closure gate: PASS (v1.2 contract + 32/32 assembler + evidence boundary + semantic promotion + immutable publication workflow anchored)"
    )


if __name__ == "__main__":
    main()
