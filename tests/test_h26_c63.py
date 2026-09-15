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
H26 = ROOT / "consumers/frontend/salario-liquido.js"
H26_PAGE = ROOT / "consumers/frontend/salario-liquido-clt/index.php"
NODE_RUNTIME = ROOT / "tests/js/phase6_c63_h26_runtime.cjs"
STORE = ROOT / "releases/fiscal-v1"
TARGET_DATE = date(2026, 9, 13)


def _node_result() -> dict:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for C6.3 H26 parity")
    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    completed = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(H26), str(artifact)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def _python_h26_reference():
    release = FiscalReleaseStore(STORE).load_current()
    assert release is not None

    inss_rule = release.select_rule(
        "inss.employee.progressive_table",
        TARGET_DATE,
        AssessmentContext.MONTHLY,
    )
    assert isinstance(inss_rule.payload, ProgressiveTablePayload)
    assert inss_rule.rounding_policy is not None
    inss = calculate_progressive("6000.00", inss_rule.payload, inss_rule.rounding_policy)

    assessment = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.MONTHLY,
        origin_context=AssessmentContext.MONTHLY,
    )
    dependent_rule = release.select_rule(
        "irrf.dependent_deduction", TARGET_DATE, AssessmentContext.MONTHLY
    )
    simplified_rule = release.select_rule(
        "irrf.simplified_monthly_discount", TARGET_DATE, AssessmentContext.MONTHLY
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
    return release, inss, irrf


def test_c63_h26_uses_published_release_end_to_end() -> None:
    release, inss, irrf = _python_h26_reference()
    result = _node_result()
    h26 = result["h26"]

    assert release.status.value == "PUBLISHED"
    assert result["release_id"] == release.release_id
    assert h26["fiscal_metadata"]["release_id"] == release.release_id
    assert h26["consumer"] == "H26"
    assert h26["reference_date"] == TARGET_DATE.isoformat()

    assert Decimal(h26["gross_remuneration"]) == Decimal("6000.00")
    assert Decimal(h26["inss"]) == inss.amount == Decimal("641.51")
    assert Decimal(h26["irrf"]["tax_base"]) == irrf.irrf_tax_base == Decimal("5358.49")
    assert Decimal(h26["irrf"]["pre_reduction_irrf"]) == irrf.pre_reduction_irrf == Decimal("564.85")
    assert Decimal(h26["irrf"]["reduction_input_income"]) == irrf.reduction_input_income == Decimal("6000.00")
    assert Decimal(h26["irrf"]["reduction_amount"]) == irrf.reduction_amount == Decimal("179.75")
    assert Decimal(h26["irrf"]["final_irrf"]) == irrf.final_irrf == Decimal("385.10")
    assert Decimal(h26["total_deductions"]) == Decimal("1026.61")
    assert Decimal(h26["net_salary"]) == Decimal("4973.39")


def test_c63_preserves_historical_a01_reducer_vector_without_falsifying_h26_inss() -> None:
    result = _node_result()
    a01 = result["historical_a01"]

    # This historical vector isolates the IRRF reducer with an explicitly supplied
    # social-security deduction of 649.60. It is not the end-to-end H26 result from
    # the current PUBLISHED INSS table, which computes 641.51.
    assert Decimal(a01["irrf_tax_base"]) == Decimal("5350.40")
    assert Decimal(a01["reduction_input_income"]) == Decimal("6000.00")
    assert Decimal(a01["reduction_amount"]) == Decimal("179.75")
    assert Decimal(a01["final_irrf"]) == Decimal("382.88")


def test_c63_other_deductions_do_not_rewrite_fiscal_bases() -> None:
    result = _node_result()
    base = result["h26"]
    with_other = result["h26_with_other_deduction"]

    assert with_other["inss"] == base["inss"]
    assert with_other["irrf"] == base["irrf"]
    assert Decimal(with_other["other_deductions"]) == Decimal("100.05")
    assert Decimal(with_other["net_salary"]) == Decimal(base["net_salary"]) - Decimal("100.05")


def test_c63_h26_is_fail_closed_for_unsupported_vigency_and_bad_inputs() -> None:
    result = _node_result()
    assert result["expired_reduction_rejected"] is True
    assert result["negative_input_rejected"] is True


def test_c63_h26_audit_trail_carries_rule_versions() -> None:
    release, _, _ = _python_h26_reference()
    result = _node_result()
    rules = result["h26"]["fiscal_metadata"]["rules"]
    by_id = {item["rule_id"]: item for item in rules}

    expected = {
        "inss.employee.progressive_table",
        "irrf.monthly.progressive_table",
        "irrf.reduction.2026",
        "irrf.dependent_deduction",
        "irrf.simplified_monthly_discount",
    }
    assert expected <= set(by_id)
    for rule_id in expected:
        selected = release.select_rule(rule_id, TARGET_DATE, AssessmentContext.MONTHLY)
        assert by_id[rule_id]["release_id"] == release.release_id
        assert by_id[rule_id]["rule_version"] == selected.rule_version


def test_c63_h26_source_no_longer_calls_legacy_fiscal_api() -> None:
    text = H26.read_text(encoding="utf-8")
    for forbidden in (
        "fetchData(",
        "calcINSS(",
        "calcIR(",
        "dados_fiscais.json",
        "/sfa/v1/folha",
        "607.20",
        "189.59",
        "1621.00",
        "2902.84",
        "4354.27",
        "8475.55",
    ):
        assert forbidden not in text

    for required in (
        "SFA.fetchRelease({ consumer: CONSUMER })",
        "SFA.assessInss(",
        "SFA.assessIrrf(",
        "grossTaxableIncome: gross.toString()",
        "socialSecurity: inss.amount",
        "release_id: release.release_id",
        "SFA.Decimal",
    ):
        assert required in text


def test_c63_h26_page_exposes_calculation_memory_and_release() -> None:
    page = H26_PAGE.read_text(encoding="utf-8")
    assert page.index("/financas/calculadoras/assets/folha-core.js") < page.index(
        "/financas/calculadoras/assets/salario-liquido.js"
    )
    for marker in (
        'data-row="base-ir"',
        'data-row="ir-antes-reducao"',
        'data-row="renda-redutor"',
        'data-row="reducao"',
        'data-row="referencia"',
        'data-row="release-id"',
        "Release fiscal usada",
    ):
        assert marker in page
