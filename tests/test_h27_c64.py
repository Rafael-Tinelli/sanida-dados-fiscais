from __future__ import annotations

from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import shutil
import subprocess

import pytest

from sanida_fiscal.publication_v1 import FiscalReleaseStore
from sanida_fiscal.thirteenth_v1 import (
    assess_thirteenth_fiscal_2026,
    calculate_thirteenth_accrual,
    calculate_thirteenth_advance,
    calculate_thirteenth_gross,
    select_thirteenth_reference_remuneration,
    select_thirteenth_rule_bundle,
)
from sanida_fiscal.types_v1 import AssessmentContext, ProgressiveTablePayload, ScalarPayload

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "consumers/frontend/folha-core.js"
THIRTEENTH = ROOT / "consumers/frontend/folha-thirteenth.js"
H27 = ROOT / "consumers/frontend/decimo-terceiro.js"
H27_PAGE = ROOT / "consumers/frontend/decimo-terceiro-clt/index.php"
NODE_RUNTIME = ROOT / "tests/js/phase6_c64_h27_runtime.cjs"
STORE = ROOT / "releases/fiscal-v1"
TARGET_DATE = date(2026, 12, 20)


def _node_result() -> dict:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for C6.4 H27 parity")
    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    completed = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(THIRTEENTH), str(H27), str(artifact)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def _python_standard_reference():
    release = FiscalReleaseStore(STORE).load_current()
    assert release is not None
    bundle = select_thirteenth_rule_bundle(release, TARGET_DATE, AssessmentContext.THIRTEENTH)
    accrual = calculate_thirteenth_accrual(
        employment_start=date(2020, 1, 1),
        accrual_end=date(2026, 12, 20),
        reference_year=2026,
        payload=bundle.accrual,
    )
    reference = select_thirteenth_reference_remuneration(
        origin_context=AssessmentContext.THIRTEENTH,
        payload=bundle.reference,
        december_due_remuneration="4000.00",
        variable_payload=bundle.variable,
    )
    money_rule = release.select_rule(
        "technical.money_decimal_and_rounding", TARGET_DATE, AssessmentContext.TECHNICAL
    )
    assert money_rule.rounding_policy is not None
    assert money_rule.rounding_policy.stage.value == "per_component"
    gross = calculate_thirteenth_gross(
        reference=reference,
        accrual=accrual,
        payload=bundle.accrual,
        rounding_policy=money_rule.rounding_policy,
    )
    assert bundle.advance is not None
    advance = calculate_thirteenth_advance(
        payment_date=date(2026, 11, 30),
        previous_month_salary="4000.00",
        payload=bundle.advance,
        rounding_policy=money_rule.rounding_policy,
    )
    inss_rule = release.select_rule(
        "inss.employee.progressive_table", TARGET_DATE, AssessmentContext.THIRTEENTH
    )
    dependent_rule = release.select_rule(
        "irrf.dependent_deduction", TARGET_DATE, AssessmentContext.THIRTEENTH
    )
    simplified_rule = release.select_rule(
        "irrf.simplified_monthly_discount", TARGET_DATE, AssessmentContext.THIRTEENTH
    )
    assert isinstance(inss_rule.payload, ProgressiveTablePayload)
    assert inss_rule.rounding_policy is not None
    assert isinstance(dependent_rule.payload, ScalarPayload)
    assert isinstance(simplified_rule.payload, ScalarPayload)
    fiscal = assess_thirteenth_fiscal_2026(
        contract=release,
        target_date=TARGET_DATE,
        origin_context=AssessmentContext.THIRTEENTH,
        gross_thirteenth=str(gross.gross_thirteenth),
        social_security_table=inss_rule.payload,
        social_security_rounding=inss_rule.rounding_policy,
        dependent_count=0,
        dependent_deduction=dependent_rule.payload,
        pension="0.00",
        simplified_discount=simplified_rule.payload,
    )
    return release, accrual, reference, gross, advance, fiscal


def test_c64_h27_standard_case_matches_python_engine_and_release() -> None:
    release, accrual, reference, gross, advance, fiscal = _python_standard_reference()
    result = _node_result()
    h27 = result["standard"]

    assert release.status.value == "PUBLISHED"
    assert result["release_id"] == release.release_id
    assert h27["fiscal_metadata"]["release_id"] == release.release_id
    assert h27["consumer"] == "H27"
    assert h27["reference_date"] == TARGET_DATE.isoformat()
    assert h27["accrual"]["twelfths"] == accrual.twelfths == 12
    assert Decimal(h27["reference_remuneration"]["total_reference"]) == reference.total_reference == Decimal("4000.00")
    assert Decimal(h27["gross_thirteenth"]) == gross.gross_thirteenth == Decimal("4000.00")
    assert h27["advance"]["status"] == "CALCULATED"
    assert Decimal(h27["advance"]["amount"]) == advance.amount == Decimal("2000.00")
    assert Decimal(h27["fiscal"]["inss"]) == fiscal.social_security.amount == Decimal("368.60")
    assert h27["fiscal"]["irrf"]["assessment"]["income_type"] == "thirteenth"
    assert h27["fiscal"]["irrf"]["assessment"]["origin_context"] == "thirteenth"
    assert Decimal(h27["fiscal"]["irrf"]["final_irrf"]) == fiscal.irrf.final_irrf == Decimal("0.00")
    assert Decimal(h27["second_installment_gross"]) == Decimal("2000.00")
    assert Decimal(h27["second_installment_net"]) == Decimal("1631.40")


def test_c64_h27_preserves_14_15_day_accrual_boundary() -> None:
    release = FiscalReleaseStore(STORE).load_current()
    assert release is not None
    payload = release.select_rule(
        "thirteenth.accrual.twelfths", TARGET_DATE, AssessmentContext.THIRTEENTH
    ).payload
    py14 = calculate_thirteenth_accrual(
        employment_start=date(2026, 1, 18),
        accrual_end=date(2026, 1, 31),
        reference_year=2026,
        payload=payload,
    )
    py15 = calculate_thirteenth_accrual(
        employment_start=date(2026, 1, 17),
        accrual_end=date(2026, 1, 31),
        reference_year=2026,
        payload=payload,
    )
    result = _node_result()
    assert result["boundary14"]["months"][0]["service_days"] == py14.months[0].service_days == 14
    assert result["boundary14"]["twelfths"] == py14.twelfths == 0
    assert result["boundary15"]["months"][0]["service_days"] == py15.months[0].service_days == 15
    assert result["boundary15"]["twelfths"] == py15.twelfths == 1


def test_c64_advance_uses_previous_month_salary_not_total_thirteenth_half() -> None:
    result = _node_result()["standard"]
    assert result["advance"]["source"] == "contract_standard"
    assert Decimal(result["advance"]["amount"]) == Decimal("2000.00")
    assert Decimal(result["reference_remuneration"]["total_reference"]) == Decimal("4000.00")


def test_c64_special_advance_cases_fail_explicitly_until_actual_advance_is_reported() -> None:
    result = _node_result()
    variable = result["variable_unsupported"]
    assert variable["advance"]["status"] == "UNSUPPORTED"
    assert variable["advance"]["reason"] == "thirteenth_advance_variable_unsupported"
    assert variable["second_installment_gross"] is None
    assert variable["second_installment_net"] is None

    reported = result["variable_reported"]
    assert reported["advance"]["status"] == "REPORTED"
    assert Decimal(reported["advance"]["amount"]) == Decimal("2100.00")
    assert reported["second_installment_gross"] is not None
    assert reported["second_installment_net"] is not None

    admission = result["admission_unsupported"]
    assert admission["advance"]["status"] == "UNSUPPORTED"
    assert admission["advance"]["reason"] == "thirteenth_advance_admission_unsupported"


def test_c64_h27_is_fail_closed_for_missing_advance_reference_negative_money_and_rounding() -> None:
    result = _node_result()
    assert result["missing_advance_reference_rejected"] is True
    assert result["negative_input_rejected"] is True
    assert result["technical_rounding_rejected"] is True


def test_c64_h27_audit_trail_covers_structural_parameter_and_technical_rules() -> None:
    release = FiscalReleaseStore(STORE).load_current()
    assert release is not None
    rules = _node_result()["standard"]["fiscal_metadata"]["rules"]
    by_id = {item["rule_id"]: item for item in rules}
    expected = {
        "technical.money_decimal_and_rounding",
        "thirteenth.accrual.twelfths",
        "thirteenth.reference_remuneration",
        "thirteenth.variable_remuneration",
        "thirteenth.advance",
        "thirteenth.inss.separate_assessment",
        "inss.employee.progressive_table",
        "thirteenth.irrf.exclusive_assessment",
        "irrf.monthly.progressive_table",
        "thirteenth.irrf.reduction.2026",
        "irrf.dependent_deduction",
        "irrf.simplified_monthly_discount",
    }
    assert expected <= set(by_id)
    for rule_id in expected:
        assert by_id[rule_id]["release_id"] == release.release_id
        assert by_id[rule_id]["rule_version"]
    assert by_id["technical.money_decimal_and_rounding"]["context"] == "technical"


def test_c64_h27_source_has_no_legacy_or_parallel_fiscal_formula() -> None:
    h27 = H27.read_text(encoding="utf-8")
    shared = THIRTEENTH.read_text(encoding="utf-8")
    for forbidden in (
        "fetchData(",
        "calcINSS(",
        "calcIR(",
        "dados_fiscais.json",
        "/sfa/v1/folha",
        "total13 * 0.5",
        "* 0.5",
        "607.20",
        "189.59",
        "1621.00",
        "2902.84",
        "4354.27",
        "8475.55",
    ):
        assert forbidden not in h27
        assert forbidden not in shared
    for required in (
        "SFA.fetchRelease({ consumer: CONSUMER })",
        "SFA.THIRTEENTH.calculateAccrual(",
        "SFA.THIRTEENTH.calculateAdvance(",
        "SFA.THIRTEENTH.assessFiscal(",
        "release_id: release.release_id",
    ):
        assert required in h27
    assert "ruleId: 'technical.money_decimal_and_rounding'" in shared
    assert "context: TECHNICAL_CONTEXT" in shared
    assert "policy.stage !== 'per_component'" in shared
    assert "dependencyOf: 'thirteenth.irrf.exclusive_assessment'" in shared
    assert "ruleId: 'irrf.monthly.progressive_table'" in shared
    assert "assessment.reduction_rule_id" in shared


def test_c64_h27_page_exposes_advance_inputs_and_auditable_fiscal_memory() -> None:
    page = H27_PAGE.read_text(encoding="utf-8")
    assert page.index("/financas/calculadoras/assets/folha-core.js") < page.index(
        "/financas/calculadoras/assets/folha-thirteenth.js"
    ) < page.index("/financas/calculadoras/assets/decimo-terceiro.js")
    for marker in (
        'name="data_quitacao"',
        'name="adiantamento_pago"',
        'name="salario_mes_anterior"',
        'name="data_adiantamento"',
        'name="admissao_ano"',
        'data-row="status-adiantamento"',
        'data-row="base-ir"',
        'data-row="ir-antes-reducao"',
        'data-row="renda-redutor"',
        'data-row="referencia"',
        'data-row="release-id"',
        "Release fiscal usada",
    ):
        assert marker in page
