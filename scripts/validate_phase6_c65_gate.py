from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.publication_v1 import FiscalReleaseStore
from sanida_fiscal.types_v1 import AssessmentContext

CORE = ROOT / "consumers/frontend/folha-core.js"
VACATION = ROOT / "consumers/frontend/folha-vacation.js"
H28 = ROOT / "consumers/frontend/ferias-clt.js"
H28_DIR = ROOT / "consumers/frontend/ferias-clt"
H28_PAGE = H28_DIR / "index.php"
H28_PARTS = (
    H28_DIR / "parts/01-head-hero.php",
    H28_DIR / "parts/02-calculator.php",
    H28_DIR / "parts/03-guide.php",
    H28_DIR / "parts/04-faq-footer.php",
)
DOC = ROOT / "docs/phase6-c65-h28.md"
NODE_RUNTIME = ROOT / "tests/js/phase6_c65_h28_runtime.cjs"
PYTEST_FILE = ROOT / "tests/test_h28_c65.py"
ENTRYPOINT_TEST = ROOT / "tests/test_phase6_c65_gate.py"
CI = ROOT / ".github/workflows/remake-ci.yml"
STORE = ROOT / "releases/fiscal-v1"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C6.5 gate failed: {message}")


def assembled_page_source() -> str:
    return H28_PAGE.read_text(encoding="utf-8") + "\n" + "\n".join(
        part.read_text(encoding="utf-8") for part in H28_PARTS
    )


def main() -> int:
    for path in (CORE, VACATION, H28, H28_PAGE, *H28_PARTS, DOC, NODE_RUNTIME, PYTEST_FILE, ENTRYPOINT_TEST):
        require(path.is_file(), f"required C6.5 file missing: {path.relative_to(ROOT)}")

    release = FiscalReleaseStore(STORE).load_current()
    require(release is not None, "current fiscal release unavailable")
    assert release is not None
    require(release.status.value == "PUBLISHED", "current release is not PUBLISHED")
    require(release.schema_version == "1.2.0", "current release schema is not 1.2.0")
    require(
        release.consumer_compatibility.contract_api_version == "1.2.0",
        "current release API is not 1.2.0",
    )
    require("H28" in {item.value for item in release.consumer_compatibility.consumers}, "H28 not declared by release")
    require(len(release.rules) == 32, "current release is not 32/32")
    require(release.lifecycle.approval_mode.value == "HUMAN_REVIEWED", "C6.5 release was not human-reviewed")

    abono = release.select_rule(
        "vacation.abono_pecuniario",
        __import__("datetime").date(2026, 9, 15),
        AssessmentContext.VACATION_CASH_ALLOWANCE,
    )
    require(
        abono.applies_to == "vacation_entitled_days_and_corresponding_remuneration_components",
        "published release lacks the approved C6.5 abono semantic",
    )
    remuneration = release.select_rule(
        "vacation.remuneration_and_constitutional_third",
        __import__("datetime").date(2026, 9, 15),
        AssessmentContext.VACATION_CASH_ALLOWANCE,
    )
    require(remuneration.rule_version == "1.1.0", "cash-allowance remuneration successor not published")
    reduction = release.select_rule(
        "vacation.irrf.reduction.2026",
        __import__("datetime").date(2026, 9, 15),
        AssessmentContext.VACATION_ENJOYED,
    )
    require(
        reduction.payload.input_semantic == "taxable_vacation_income_subject_to_separate_monthly_irrf_assessment_before_deductions",
        "dedicated vacation reduction semantic is invalid",
    )

    shared = VACATION.read_text(encoding="utf-8")
    consumer = H28.read_text(encoding="utf-8")
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
        require(forbidden not in shared, f"legacy/hardcoded vacation logic remains: {forbidden}")
        require(forbidden not in consumer, f"legacy/hardcoded H28 logic remains: {forbidden}")

    for marker in (
        "vacation.entitlement_days_by_absences",
        "vacation.abono_pecuniario",
        "vacation.remuneration_and_constitutional_third",
        "vacation.inss.enjoyed",
        "vacation.abono.ir_exemption",
        "vacation.abono_constitutional_third.ir_incidence",
        "vacation.irrf.separate_assessment",
        "vacation.irrf.reduction.2026",
        "vacation_entitled_days_and_corresponding_remuneration_components",
        "O contrato não autoriza arredondar arbitrariamente dias de abono",
    ):
        require(marker in shared, f"vacation shared runtime missing marker: {marker}")
    require("SFA.fetchRelease({ consumer: CONSUMER })" in consumer, "H28 does not fetch canonical release")
    require("SFA.VACATION.calculate(" in consumer, "H28 does not use shared vacation runtime")

    page = assembled_page_source()
    require(
        page.index("/financas/calculadoras/assets/folha-core.js")
        < page.index("/financas/calculadoras/assets/folha-vacation.js")
        < page.index("/financas/calculadoras/assets/ferias-clt.js"),
        "H28 page must load core, vacation extension, then consumer",
    )
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
        'data-row="ir-antes-reducao"',
        'data-row="renda-redutor"',
        'data-row="release-id"',
        "Release fiscal usada",
        "Fora do escopo automático",
    ):
        require(marker in page, f"H28 page missing marker: {marker}")

    node = shutil.which("node")
    require(node is not None, "node is required for C6.5 validation")
    for source in (VACATION, H28):
        syntax = subprocess.run([node, "--check", str(source)], cwd=ROOT, check=False, capture_output=True, text=True)
        require(syntax.returncode == 0, f"node --check failed for {source.name}: {syntax.stderr or syntax.stdout}")

    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    runtime = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(VACATION), str(H28), str(artifact)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    require(runtime.returncode == 0, f"C6.5 Node runtime failed: {runtime.stderr or runtime.stdout}")
    payload = json.loads(runtime.stdout)
    standard = payload.get("standard") or {}
    entitlement = standard.get("entitlement") or {}
    components = standard.get("components") or {}
    fiscal = standard.get("fiscal") or {}
    irrf = fiscal.get("irrf") or {}

    require(payload.get("release_id") == release.release_id, "H28 runtime used a different release_id")
    require((standard.get("fiscal_metadata") or {}).get("release_id") == release.release_id, "H28 memory lacks release_id")
    require(entitlement.get("entitled_days") == 30, "H28 standard entitlement is not 30 days")
    require(entitlement.get("cash_allowance_days") == 10, "H28 standard abono is not 10 days")
    require(entitlement.get("enjoyed_days") == 20, "H28 standard gozo is not 20 days")
    require(Decimal(str(components.get("enjoyed_principal"))) == Decimal("2666.67"), "H28 enjoyed principal regression failed")
    require(Decimal(str(components.get("enjoyed_constitutional_third"))) == Decimal("888.89"), "H28 enjoyed third regression failed")
    require(Decimal(str(components.get("cash_allowance_principal"))) == Decimal("1333.33"), "H28 abono principal regression failed")
    require(Decimal(str(components.get("cash_allowance_constitutional_third"))) == Decimal("444.44"), "H28 abono third regression failed")
    require(Decimal(str(components.get("gross_vacation_payment"))) == Decimal("5333.33"), "H28 gross regression failed")
    require(Decimal(str(fiscal.get("social_security_base"))) == Decimal("3555.56"), "H28 INSS base regression failed")
    require(Decimal(str(fiscal.get("inss"))) == Decimal("315.27"), "H28 INSS regression failed")
    require(Decimal(str(fiscal.get("taxable_vacation_income"))) == Decimal("4000.00"), "H28 taxable income regression failed")
    require(Decimal(str(irrf.get("final_irrf"))) == Decimal("0.00"), "H28 IRRF regression failed")
    require(Decimal(str(fiscal.get("net_vacation_payment"))) == Decimal("5018.06"), "H28 net regression failed")
    require(payload.get("unsupported_absences_rejected") is True, "H28 did not reject unsupported absences")
    require(payload.get("negative_input_rejected") is True, "H28 did not reject negative money")
    require(payload.get("pension_omission_rejected") is True, "H28 did not reject omitted pension")
    require(payload.get("dedicated_reduction_present") is True, "H28 did not audit dedicated vacation reduction")
    require(payload.get("generic_reduction_absent") is True, "H28 audited generic monthly reduction")

    for band in payload.get("bands") or []:
        actual = band.get("actual") or {}
        require(actual.get("entitled_days") == band.get("expected_entitled"), "entitlement band regression")
        require(actual.get("cash_allowance_days") == band.get("expected_sold"), "abono days regression")
        require(actual.get("enjoyed_days") == band.get("expected_enjoyed"), "enjoyed days regression")

    php = shutil.which("php")
    if php:
        for source in (H28_PAGE, *H28_PARTS):
            lint = subprocess.run([php, "-l", str(source)], cwd=ROOT, check=False, capture_output=True, text=True)
            require(lint.returncode == 0, f"H28 PHP lint failed for {source.name}: {lint.stderr or lint.stdout}")

    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "**Status:** CONCLUÍDO",
        "30→10",
        "vacation.irrf.reduction.2026",
        "5333.33",
        "315.27",
        "5018.06",
        "não afirma implantação no HostGator",
        "C6.6",
    ):
        require(marker in doc, f"C6.5 document missing marker: {marker}")

    ci = CI.read_text(encoding="utf-8")
    require("python scripts/validate_phase6_c65_gate.py" in ci, "Remake CI does not execute the permanent C6.5 gate")

    print(
        "Phase 6 C6.5 H28 gate: PASS "
        f"(release={release.release_id}, entitled={entitlement['entitled_days']}, "
        f"enjoyed={entitlement['enjoyed_days']}, sold={entitlement['cash_allowance_days']}, "
        f"inss={fiscal['inss']}, irrf={irrf['final_irrf']}, net={fiscal['net_vacation_payment']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
