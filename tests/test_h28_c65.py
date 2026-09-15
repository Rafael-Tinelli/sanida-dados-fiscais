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
from sanida_fiscal.money import quantize
from sanida_fiscal.publication_v1 import FiscalReleaseStore
from sanida_fiscal.types_v1 import AssessmentContext, ProgressiveTablePayload, ScalarPayload
from sanida_fiscal.vacation_v1 import (
    calculate_cash_allowance_days,
    resolve_cash_allowance_tax_treatment,
    select_vacation_rule_bundle,
    vacation_entitlement_from_absences,
)

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "consumers/frontend/folha-core.js"
VACATION = ROOT / "consumers/frontend/folha-vacation.js"
H28 = ROOT / "consumers/frontend/ferias-clt.js"
H28_PAGE = ROOT / "consumers/frontend/ferias-clt/index.php"
NODE_RUNTIME = ROOT / "tests/js/phase6_c65_h28_runtime.cjs"
STORE = ROOT / "releases/fiscal-v1"
TARGET_DATE = date(2026, 9, 15)


def _node_result() -> dict:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for C6.5 H28 parity")
    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    completed = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(VACATION), str(H28), str(artifact)],
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
    vacation = select_vacation_rule_bundle(release, TARGET_DATE)
    entitlement = vacation_entitlement_from_absences(0, vacation.entitlement)
    cash_days = calculate_cash_allowance_days(
        entitled_days=entitlement.entitled_days,
        payload=vacation.cash_allowance_fraction,
    )
    treatment = resolve_cash_allowance_tax_treatment(
        principal_payload=vacation.principal_incidence,
        constitutional_third_payload=vacation.constitutional_third_incidence,
    )
    assert treatment.principal.irrf.value == "no"
    assert treatment.principal.social_security.value == "no"
    assert treatment.constitutional_third.irrf.value == "yes"
    assert treatment.constitutional_third.social_security.value == "no"

    formula = release.select_rule(
        "vacation.remuneration_and_constitutional_third",
        TARGET_DATE,
        AssessmentContext.VACATION_ENJOYED,
    )
    assert formula.rounding_policy is not None
    policy = formula.rounding_policy
    full = Decimal("4000.00")
    cash_principal = quantize(full / Decimal(3), policy)
    enjoyed_principal = full - cash_principal
    enjoyed_third = quantize(enjoyed_principal / Decimal(3), policy)
    cash_third = quantize(cash_principal / Decimal(3), policy)

    inss_rule = release.select_rule(
        "inss.employee.progressive_table", TARGET_DATE, AssessmentContext.VACATION_ENJOYED
    )
    assert isinstance(inss_rule.payload, ProgressiveTablePayload)
    assert inss_rule.rounding_policy is not None
    social_base = enjoyed_principal + enjoyed_third
    inss = calculate_progressive(social_base, inss_rule.payload, inss_rule.rounding_policy)

    assessment = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.VACATION,
        origin_context=AssessmentContext.VACATION_ENJOYED,
    )
    bundle = select_irrf_rule_bundle(release, TARGET_DATE, assessment)
    dependent = release.select_rule(
        "irrf.dependent_deduction", TARGET_DATE, AssessmentContext.VACATION_ENJOYED
    )
    simplified = release.select_rule(
        "irrf.simplified_monthly_discount", TARGET_DATE, AssessmentContext.VACATION_ENJOYED
    )
    assert isinstance(dependent.payload, ScalarPayload)
    assert isinstance(simplified.payload, ScalarPayload)
    deductions = build_irrf_legal_deductions(
        assessment=assessment,
        social_security=inss.amount,
        dependent_count=0,
        dependent_deduction=dependent.payload,
        pension="0.00",
    )
    taxable = enjoyed_principal + enjoyed_third + cash_third
    irrf = assess_irrf_2026(
        assessment=assessment,
        gross_taxable_income=taxable,
        legal_deductions=deductions,
        simplified_discount=simplified.payload,
        rules=bundle,
    )
    gross = enjoyed_principal + enjoyed_third + cash_principal + cash_third
    net = gross - inss.amount - irrf.final_irrf
    return release, entitlement, cash_days, cash_principal, enjoyed_principal, enjoyed_third, cash_third, inss, irrf, gross, net


def test_c65_h28_standard_sale_case_matches_python_engine() -> None:
    (
        release, entitlement, cash_days, cash_principal, enjoyed_principal,
        enjoyed_third, cash_third, inss, irrf, gross, net,
    ) = _python_standard_reference()
    result = _node_result()
    h28 = result["standard"]

    assert result["release_id"] == release.release_id
    assert h28["fiscal_metadata"]["release_id"] == release.release_id
    assert h28["consumer"] == "H28"
    assert h28["entitlement"]["entitled_days"] == entitlement.entitled_days == 30
    assert h28["entitlement"]["cash_allowance_days"] == cash_days.cash_allowance_days == 10
    assert h28["entitlement"]["enjoyed_days"] == cash_days.remaining_vacation_days == 20
    assert Decimal(h28["components"]["cash_allowance_principal"]) == cash_principal == Decimal("1333.33")
    assert Decimal(h28["components"]["enjoyed_principal"]) == enjoyed_principal == Decimal("2666.67")
    assert Decimal(h28["components"]["enjoyed_constitutional_third"]) == enjoyed_third == Decimal("888.89")
    assert Decimal(h28["components"]["cash_allowance_constitutional_third"]) == cash_third == Decimal("444.44")
    assert Decimal(h28["components"]["gross_vacation_payment"]) == gross == Decimal("5333.33")
    assert Decimal(h28["fiscal"]["social_security_base"]) == Decimal("3555.56")
    assert Decimal(h28["fiscal"]["inss"]) == inss.amount == Decimal("315.27")
    assert Decimal(h28["fiscal"]["taxable_vacation_income"]) == Decimal("4000.00")
    assert Decimal(h28["fiscal"]["irrf"]["final_irrf"]) == irrf.final_irrf == Decimal("0.00")
    assert Decimal(h28["fiscal"]["net_vacation_payment"]) == net == Decimal("5018.06")


def test_c65_h28_entitlement_bands_drive_abono_without_arbitrary_rounding() -> None:
    result = _node_result()
    for row in result["bands"]:
        assert row["actual"]["entitled_days"] == row["expected_entitled"]
        assert row["actual"]["cash_allowance_days"] == row["expected_sold"]
        assert row["actual"]["enjoyed_days"] == row["expected_enjoyed"]
    assert result["unsupported_absences_rejected"] is True


def test_c65_h28_uses_dedicated_vacation_reduction_and_incidence_profiles() -> None:
    result = _node_result()
    assert result["dedicated_reduction_present"] is True
    assert result["generic_reduction_absent"] is True
    assert result["cash_principal_profile_present"] is True
    assert result["cash_third_profile_present"] is True

    rules = result["standard"]["fiscal_metadata"]["rules"]
    ids = {item["rule_id"] for item in rules}
    assert {
        "vacation.entitlement_days_by_absences",
        "vacation.abono_pecuniario",
        "vacation.remuneration_and_constitutional_third",
        "vacation.inss.enjoyed",
        "vacation.abono.ir_exemption",
        "vacation.abono_constitutional_third.ir_incidence",
        "vacation.irrf.separate_assessment",
        "vacation.irrf.reduction.2026",
        "inss.employee.progressive_table",
        "irrf.monthly.progressive_table",
        "irrf.dependent_deduction",
        "irrf.simplified_monthly_discount",
    } <= ids


def test_c65_h28_no_sale_keeps_entitlement_and_zeroes_abono() -> None:
    no_sale = _node_result()["no_sale"]
    assert no_sale["entitlement"]["entitled_days"] == 30
    assert no_sale["entitlement"]["enjoyed_days"] == 30
    assert no_sale["entitlement"]["cash_allowance_days"] == 0
    assert Decimal(no_sale["components"]["cash_allowance_principal"]) == 0
    assert Decimal(no_sale["components"]["cash_allowance_constitutional_third"]) == 0


def test_c65_h28_fails_closed_for_negative_money_and_omitted_pension() -> None:
    result = _node_result()
    assert result["negative_input_rejected"] is True
    assert result["pension_omission_rejected"] is True


def test_c65_h28_source_has_no_legacy_abono_rounding_or_tax_constants() -> None:
    shared = VACATION.read_text(encoding="utf-8")
    h28 = H28.read_text(encoding="utf-8")
    for forbidden in (
        "Math.round(dias / 3)",
        "Math.round(dias/3)",
        "dados_fiscais.json",
        "/sfa/v1/folha",
        "607.20",
        "189.59",
        "1621.00",
        "2902.84",
        "4354.27",
        "8475.55",
    ):
        assert forbidden not in shared
        assert forbidden not in h28
    assert "ruleId: 'vacation.irrf.reduction.2026'" in shared
    assert "vacation_entitled_days_and_corresponding_remuneration_components" in shared
    assert "SFA.fetchRelease({ consumer: CONSUMER })" in h28


def test_c65_h28_page_exposes_right_gozo_abono_and_audit_memory() -> None:
    page = H28_PAGE.read_text(encoding="utf-8")
    parts = ROOT / "consumers/frontend/ferias-clt/parts"
    source = page + "\n" + "\n".join(p.read_text(encoding="utf-8") for p in sorted(parts.glob("*.php")))
    assert source.index("/financas/calculadoras/assets/folha-core.js") < source.index(
        "/financas/calculadoras/assets/folha-vacation.js"
    ) < source.index("/financas/calculadoras/assets/ferias-clt.js")
    for marker in (
        'name="base_ferias"',
        'name="faltas_injustificadas"',
        'name="vender_um_terco"',
        'data-row="direito"',
        'data-row="gozo"',
        'data-row="abono-dias"',
        'data-row="abono-principal"',
        'data-row="abono-terco"',
        'data-row="base-inss"',
        'data-row="renda-ir"',
        'data-row="base-ir"',
        'data-row="renda-redutor"',
        'data-row="release-id"',
        "Release fiscal usada",
        "Fora do escopo automático",
    ):
        assert marker in source
