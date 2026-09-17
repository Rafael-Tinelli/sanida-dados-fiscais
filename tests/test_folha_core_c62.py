from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import shutil
import subprocess

import pytest

from sanida_fiscal.engine_v1 import (
    IrrfAssessmentIdentity,
    IrrfIncomeType,
    assess_irrf_2026,
    build_irrf_legal_deductions,
    calculate_progressive,
    select_irrf_rule_bundle,
)
from sanida_fiscal.publication_v1 import FiscalReleaseStore
from sanida_fiscal.types_v1 import AssessmentContext, ProgressiveTablePayload, ScalarPayload


ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "consumers/frontend/folha-core.js"
NODE_RUNTIME = ROOT / "tests/js/phase6_c62_runtime.cjs"
STORE = ROOT / "releases/fiscal-v1"
TARGET_DATE = date(2026, 9, 13)


def _node_result() -> dict:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for C6.2 browser-runtime parity")

    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    completed = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(artifact)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def test_c62_decimal_boundary_rejects_binary_float_and_is_exact() -> None:
    result = _node_result()
    assert result["decimal"] == {
        "sum": "0.3",
        "half_up": "2.35",
        "half_even": "2.34",
    }
    assert result["float_rejected"] is True


def test_c62_normalize_date_rejects_impossible_civil_dates() -> None:
    result = _node_result()
    assert result["normalized_dates"] == {
        "regular": "2026-09-16",
        "leap_day": "2028-02-29",
        "date_object": "2028-02-29",
    }
    assert result["civil_date_rejections"] == {
        "2026-02-29": True,
        "2026-02-31": True,
        "2026-04-31": True,
        "2026-00-10": True,
        "2026-13-01": True,
        "2026-01-00": True,
    }
    assert result["malformed_date_rejected"] is True
    assert result["impossible_selection_date_rejected"] is True


def test_c62_rule_selection_is_fail_closed_and_auditable() -> None:
    release = FiscalReleaseStore(STORE).load_current()
    assert release is not None
    result = _node_result()

    selected = release.select_rule(
        "irrf.monthly.progressive_table",
        TARGET_DATE,
        AssessmentContext.MONTHLY,
    )
    assert result["release_id"] == release.release_id
    assert result["selected_rule"]["release_id"] == release.release_id
    assert result["selected_rule"]["rule_id"] == selected.rule_id
    assert result["selected_rule"]["rule_version"] == selected.rule_version
    assert result["selected_rule"]["context"] == AssessmentContext.MONTHLY.value
    assert result["missing_rule_rejected"] is True
    assert result["incompatible_assessment_rejected"] is True


def test_c62_progressive_and_irrf_runtime_match_python_engine() -> None:
    release = FiscalReleaseStore(STORE).load_current()
    assert release is not None
    result = _node_result()

    inss_rule = release.select_rule(
        "inss.employee.progressive_table",
        TARGET_DATE,
        AssessmentContext.MONTHLY,
    )
    assert isinstance(inss_rule.payload, ProgressiveTablePayload)
    assert inss_rule.rounding_policy is not None
    inss = calculate_progressive(
        "6000.00",
        inss_rule.payload,
        inss_rule.rounding_policy,
    )

    assessment = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.MONTHLY,
        origin_context=AssessmentContext.MONTHLY,
    )
    dependent_rule = release.select_rule(
        "irrf.dependent_deduction",
        TARGET_DATE,
        AssessmentContext.MONTHLY,
    )
    simplified_rule = release.select_rule(
        "irrf.simplified_monthly_discount",
        TARGET_DATE,
        AssessmentContext.MONTHLY,
    )
    assert isinstance(dependent_rule.payload, ScalarPayload)
    assert isinstance(simplified_rule.payload, ScalarPayload)

    bundle = select_irrf_rule_bundle(release, TARGET_DATE, assessment)
    deductions = build_irrf_legal_deductions(
        assessment=assessment,
        social_security=str(inss.amount),
        dependent_count=0,
        dependent_deduction=dependent_rule.payload,
        pension="0.00",
    )
    irrf = assess_irrf_2026(
        assessment=assessment,
        gross_taxable_income="6000.00",
        legal_deductions=deductions,
        simplified_discount=simplified_rule.payload,
        rules=bundle,
    )

    assert Decimal(result["inss"]["amount"]) == inss.amount
    assert Decimal(result["irrf"]["irrf_tax_base"]) == irrf.irrf_tax_base
    assert Decimal(result["irrf"]["pre_reduction_irrf"]) == irrf.pre_reduction_irrf
    assert Decimal(result["irrf"]["reduction_input_income"]) == irrf.reduction_input_income
    assert Decimal(result["irrf"]["reduction_amount"]) == irrf.reduction_amount
    assert Decimal(result["irrf"]["final_irrf"]) == irrf.final_irrf
    assert result["irrf"]["audit"]["release_id"] == release.release_id


def test_c62_vacation_irrf_identity_and_runtime_match_python_engine() -> None:
    release = FiscalReleaseStore(STORE).load_current()
    assert release is not None
    result = _node_result()

    assessment = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.VACATION,
        origin_context=AssessmentContext.VACATION_ENJOYED,
    )
    bundle = select_irrf_rule_bundle(release, TARGET_DATE, assessment)
    dependent_rule = release.select_rule(
        "irrf.dependent_deduction",
        TARGET_DATE,
        AssessmentContext.VACATION_ENJOYED,
    )
    simplified_rule = release.select_rule(
        "irrf.simplified_monthly_discount",
        TARGET_DATE,
        AssessmentContext.VACATION_ENJOYED,
    )
    assert isinstance(dependent_rule.payload, ScalarPayload)
    assert isinstance(simplified_rule.payload, ScalarPayload)

    deductions = build_irrf_legal_deductions(
        assessment=assessment,
        social_security="315.27",
        dependent_count=0,
        dependent_deduction=dependent_rule.payload,
        pension="0.00",
    )
    irrf = assess_irrf_2026(
        assessment=assessment,
        gross_taxable_income="4000.00",
        legal_deductions=deductions,
        simplified_discount=simplified_rule.payload,
        rules=bundle,
    )

    identity = result["vacation_identity"]
    js_irrf = result["vacation_irrf"]
    assert identity["income_type"] == "vacation"
    assert identity["origin_context"] == "vacation_enjoyed"
    assert identity["rule_context"] == "vacation_enjoyed"
    assert identity["reduction_rule_id"] == "vacation.irrf.reduction.2026"
    assert identity["reduction_input_semantic"] == (
        "taxable_vacation_income_subject_to_separate_monthly_irrf_assessment_before_deductions"
    )
    assert Decimal(js_irrf["irrf_tax_base"]) == irrf.irrf_tax_base
    assert Decimal(js_irrf["pre_reduction_irrf"]) == irrf.pre_reduction_irrf
    assert Decimal(js_irrf["reduction_amount"]) == irrf.reduction_amount
    assert Decimal(js_irrf["final_irrf"]) == irrf.final_irrf
    audit_ids = {item["rule_id"] for item in js_irrf["audit"]["rules"]}
    assert "vacation.irrf.reduction.2026" in audit_ids
    assert "irrf.reduction.2026" not in audit_ids
