from __future__ import annotations

from copy import deepcopy
from datetime import date
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from sanida_fiscal.contract_v1 import (
    AssessmentContext,
    ContractNotConsumableError,
    FiscalContractV1,
    QualityStatus,
    RuleSelectionError,
)

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"


def load_example() -> dict:
    return json.loads(EXAMPLE.read_text(encoding="utf-8"))


def test_example_validates() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    assert contract.schema_version == "1.0.0"
    assert contract.contract_id == "br.sanida.fiscal"
    assert len(contract.rules) >= 8


def test_decimal_values_round_trip_as_strings_in_json_mode() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    dumped = contract.model_dump(mode="json")
    irrf = next(rule for rule in dumped["rules"] if rule["rule_id"] == "irrf.monthly.progressive_table")
    assert irrf["payload"]["brackets"][0]["upper_bound"] == "2428.80"
    assert irrf["payload"]["brackets"][1]["rate"] == "0.075"


def test_a01_semantic_target_is_not_irrf_base() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = contract.select_rule("irrf.reduction.2026", date(2026, 6, 1), AssessmentContext.MONTHLY)
    assert rule.applies_to == "taxable_income_subject_to_monthly_incidence_before_irrf_deductions"
    assert rule.payload.input_semantic == rule.applies_to
    assert rule.applies_to != "irrf_tax_base_after_allowed_deductions"


def test_thirteenth_reduction_is_separate_context() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = contract.select_rule("thirteenth.irrf.reduction.2026", date(2026, 12, 20), AssessmentContext.THIRTEENTH)
    assert rule.payload.input_semantic == "thirteenth_taxable_income_before_irrf_deductions"


def test_rule_selection_fails_outside_vigency() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    with pytest.raises(RuleSelectionError):
        contract.select_rule("irrf.reduction.2026", date(2027, 1, 1), AssessmentContext.MONTHLY)


def test_candidate_contract_is_not_consumable() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    with pytest.raises(ContractNotConsumableError):
        contract.assert_consumable()


def test_published_contract_requires_hashed_available_evidence() -> None:
    data = load_example(); data["status"] = "PUBLISHED"
    with pytest.raises(ValidationError, match="hashed snapshot"):
        FiscalContractV1.model_validate(data)


def test_structural_rule_cannot_auto_publish() -> None:
    data = load_example(); rule = next(r for r in data["rules"] if r["rule_id"] == "termination.reason_scope")
    rule["update_policy"]["auto_publish"] = True
    with pytest.raises(ValidationError, match="structural rules cannot auto-publish"):
        FiscalContractV1.model_validate(data)


def test_validated_structural_rule_requires_human_review() -> None:
    data = load_example(); rule = next(r for r in data["rules"] if r["rule_id"] == "termination.reason_scope")
    rule["quality"]["reviewed_by_human"] = False
    with pytest.raises(ValidationError, match="structural rules require human review"):
        FiscalContractV1.model_validate(data)


def test_overlapping_same_rule_and_context_is_rejected() -> None:
    data = load_example(); original = next(r for r in data["rules"] if r["rule_id"] == "irrf.reduction.2026")
    duplicate = deepcopy(original); duplicate["rule_version"] = "1.0.1"; data["rules"].append(duplicate)
    with pytest.raises(ValidationError, match="overlapping rule windows"):
        FiscalContractV1.model_validate(data)


def test_missing_dependency_is_rejected() -> None:
    data = load_example(); rule = next(r for r in data["rules"] if r["rule_id"] == "vacation.abono_pecuniario")
    rule["dependencies"] = ["rule.that.does.not.exist"]
    with pytest.raises(ValidationError, match="missing dependencies"):
        FiscalContractV1.model_validate(data)


def test_last_good_requires_current_vigency_and_validated_rule() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = contract.select_rule("irrf.reduction.2026", date(2026, 6, 1), AssessmentContext.MONTHLY)
    assert contract.can_use_last_good(rule, date(2026, 6, 1), known_successor=False)
    assert not contract.can_use_last_good(rule, date(2027, 1, 1), known_successor=False)
    assert not contract.can_use_last_good(rule, date(2026, 6, 1), known_successor=True)


def test_last_good_rejects_unvalidated_rule() -> None:
    data = load_example(); rule_data = next(r for r in data["rules"] if r["rule_id"] == "irrf.reduction.2026")
    rule_data["quality"]["status"] = "UNVALIDATED"
    contract = FiscalContractV1.model_validate(data)
    rule = next(r for r in contract.rules if r.rule_id == "irrf.reduction.2026")
    assert rule.quality.status == QualityStatus.UNVALIDATED
    assert not contract.can_use_last_good(rule, date(2026, 6, 1), known_successor=False)


def test_h29_scope_is_exactly_four_supported_codes() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    rule = next(r for r in contract.rules if r.rule_id == "termination.reason_scope")
    matrix = {row.code: row.entitlements for row in rule.payload.rows}
    assert set(matrix) == {"01", "02", "07", "33"}
    assert matrix["01"].thirteenth_proportional is False
    assert matrix["01"].vacation_proportional is False
    assert matrix["02"].thirteenth_proportional is True
    assert matrix["07"].vacation_proportional is True
    assert matrix["33"].vacation_proportional is True


def test_abono_principal_and_third_have_distinct_ir_treatment() -> None:
    contract = FiscalContractV1.model_validate(load_example())
    principal = next(r for r in contract.rules if r.rule_id == "vacation.abono.ir_exemption")
    third = next(r for r in contract.rules if r.rule_id == "vacation.abono_constitutional_third.ir_incidence")
    assert principal.payload.components[0].irrf.value == "no"
    assert third.payload.components[0].irrf.value == "yes"
    assert principal.payload.components[0].social_security.value == "no"
    assert third.payload.components[0].social_security.value == "no"


def test_unknown_field_is_rejected() -> None:
    data = load_example(); data["invented_field"] = True
    with pytest.raises(ValidationError):
        FiscalContractV1.model_validate(data)
