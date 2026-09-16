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
TERMINATION = ROOT / "consumers/frontend/folha-termination.js"
H29 = ROOT / "consumers/frontend/rescisao-clt.js"
H29_DIR = ROOT / "consumers/frontend/rescisao-clt"
H29_PAGE = H29_DIR / "index.php"
H29_PARTS = (
    H29_DIR / "parts/01-head-hero.php",
    H29_DIR / "parts/02-calculator.php",
    H29_DIR / "parts/03-guide.php",
    H29_DIR / "parts/04-faq-footer.php",
)
DOC = ROOT / "docs/phase6-c66-h29.md"
NODE_RUNTIME = ROOT / "tests/js/phase6_c66_h29_runtime.cjs"
PYTEST_FILE = ROOT / "tests/test_h29_c66.py"
ENTRYPOINT_TEST = ROOT / "tests/test_phase6_c66_gate.py"
CI = ROOT / ".github/workflows/remake-ci.yml"
STORE = ROOT / "releases/fiscal-v1"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C6.6 gate failed: {message}")


def assembled_page_source() -> str:
    return H29_PAGE.read_text(encoding="utf-8") + "\n" + "\n".join(
        part.read_text(encoding="utf-8") for part in H29_PARTS
    )


def main() -> int:
    for path in (CORE, TERMINATION, H29, H29_PAGE, *H29_PARTS, DOC, NODE_RUNTIME, PYTEST_FILE, ENTRYPOINT_TEST):
        require(path.is_file(), f"required C6.6 file missing: {path.relative_to(ROOT)}")

    release = FiscalReleaseStore(STORE).load_current()
    require(release is not None, "current fiscal release unavailable")
    assert release is not None
    require(release.status.value == "PUBLISHED", "current release is not PUBLISHED")
    require(release.schema_version == "1.2.0", "current release schema is not 1.2.0")
    require(release.consumer_compatibility.contract_api_version == "1.2.0", "current release API is not 1.2.0")
    require("H29" in {item.value for item in release.consumer_compatibility.consumers}, "H29 not declared by release")
    require(len(release.rules) == 32, "current release is not 32/32")
    require(release.lifecycle.approval_mode.value == "HUMAN_REVIEWED", "C6.6 release was not human-reviewed")

    target = __import__("datetime").date(2026, 3, 31)
    reason = release.select_rule("termination.reason_scope", target, AssessmentContext.TERMINATION)
    require(reason.payload.type == "eligibility_matrix", "reason scope is not an eligibility matrix")
    require({row.code for row in reason.payload.rows} == {"01", "02", "07", "33"}, "reason matrix is not exactly 01/02/07/33")
    scope = release.select_rule("termination.partial_output_scope", target, AssessmentContext.TERMINATION)
    require(scope.payload.promise == "partial_estimate", "H29 promise is not partial_estimate")
    salary = release.select_rule("termination.salary_balance", target, AssessmentContext.TERMINATION)
    require(salary.payload.universal_fixed_denominator is False, "salary balance still allows a universal fixed denominator")
    vacation = release.select_rule("vacation.acquisition_period", target, AssessmentContext.TERMINATION)
    require(vacation.payload.calendar_year_reset is False, "vacation acquisition period resets on calendar year")
    require(vacation.payload.proportional_qualifying_days == 15, "vacation qualifying-day threshold is not 15")

    shared = TERMINATION.read_text(encoding="utf-8")
    consumer = H29.read_text(encoding="utf-8")
    for forbidden in (
        "dados_fiscais.json",
        "/sfa/v1/folha",
        "avosFeriasProporcionais",
        "salario / 30",
        "salario/30",
        "Math.round",
    ):
        require(forbidden not in shared, f"legacy/hardcoded H29 logic remains: {forbidden}")
        require(forbidden not in consumer, f"legacy/hardcoded H29 consumer logic remains: {forbidden}")

    for marker in (
        "termination.reason_scope",
        "termination.partial_output_scope",
        "termination.salary_balance",
        "termination.thirteenth_proportional",
        "termination.vacation_proportional",
        "termination.acquired_and_overdue_vacation",
        "thirteenth.accrual.twelfths",
        "thirteenth.reference_remuneration",
        "vacation.acquisition_period",
        "technical.money_decimal_and_rounding",
        "universal_fixed_denominator !== false",
        "partial_estimate",
    ):
        require(marker in shared, f"termination runtime missing marker: {marker}")
    require("SFA.fetchRelease({ consumer: CONSUMER })" in consumer, "H29 does not fetch canonical release")
    require("SFA.TERMINATION.calculate(" in consumer, "H29 does not use shared termination runtime")

    page = assembled_page_source()
    require(
        page.index("/financas/calculadoras/assets/folha-core.js")
        < page.index("/financas/calculadoras/assets/folha-termination.js")
        < page.index("/financas/calculadoras/assets/rescisao-clt.js"),
        "H29 page must load core, termination extension, then consumer",
    )
    for marker in (
        'name="motivo_esocial"', 'value="01"', 'value="02"', 'value="07"', 'value="33"',
        'name="data_admissao"', 'name="data_desligamento"', 'name="salario_base_mensal"',
        'name="dias_computados"', 'name="remuneracao_mes_desligamento"',
        'data-row="13-avos"', 'data-row="ferias-periodo"', 'data-list="incluidos"',
        'data-list="excluidos"', 'data-row="release-id"',
        "Esta não é uma calculadora de “total da rescisão”", "Fora do cálculo automático",
    ):
        require(marker in page, f"H29 page missing marker: {marker}")

    node = shutil.which("node")
    require(node is not None, "node is required for C6.6 validation")
    for source in (TERMINATION, H29):
        syntax = subprocess.run([node, "--check", str(source)], cwd=ROOT, check=False, capture_output=True, text=True)
        require(syntax.returncode == 0, f"node --check failed for {source.name}: {syntax.stderr or syntax.stdout}")

    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    runtime = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(TERMINATION), str(H29), str(artifact)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    require(runtime.returncode == 0, f"C6.6 Node runtime failed: {runtime.stderr or runtime.stdout}")
    payload = json.loads(runtime.stdout)
    standard = payload.get("standard") or {}
    salary_result = standard.get("salary_balance") or {}
    thirteenth = standard.get("thirteenth_proportional") or {}
    vacation_result = standard.get("vacation_proportional") or {}

    require(payload.get("release_id") == release.release_id, "H29 runtime used a different release_id")
    require((standard.get("fiscal_metadata") or {}).get("release_id") == release.release_id, "H29 memory lacks release_id")
    require(standard.get("result_promise") == "partial_estimate", "H29 result is not explicitly partial")
    require(standard.get("user_disclosure_required") is True, "H29 disclosure is not mandatory")
    require((standard.get("reason") or {}).get("code") == "02", "H29 standard reason is not 02")
    require(Decimal(str(salary_result.get("amount"))) == Decimal("1000.00"), "H29 salary balance regression failed")
    require(salary_result.get("calendar_days_in_month") == 31, "H29 March denominator is not 31")
    require(thirteenth.get("twelfths") == 3, "H29 thirteenth is not 3/12")
    require(Decimal(str(thirteenth.get("gross_thirteenth"))) == Decimal("900.00"), "H29 thirteenth gross regression failed")
    require(vacation_result.get("twelfths") == 7, "H29 vacation is not 7/12")
    require((vacation_result.get("period") or {}).get("start") == "2025-09-01", "H29 acquisition start regression failed")
    require((vacation_result.get("period") or {}).get("end_inclusive") == "2026-08-31", "H29 acquisition end regression failed")
    require(Decimal(str((payload.get("march_salary_balance") or {}).get("amount"))) == Decimal("1000.00"), "March divisor regression")
    require(Decimal(str((payload.get("april_salary_balance") or {}).get("amount"))) == Decimal("1033.33"), "April divisor regression")
    require(payload.get("boundary14") == {"thirteenth_twelfths": 0, "vacation_twelfths": 0}, "14-day boundary regression")
    require(payload.get("boundary15") == {"thirteenth_twelfths": 1, "vacation_twelfths": 1}, "15-day boundary regression")
    for flag in ("unsupported_reason_rejected", "hourly_rejected", "fixed_term_rejected", "excess_days_rejected", "negative_money_rejected", "required_rules_present"):
        require(payload.get(flag) is True, f"H29 fail-closed/audit flag failed: {flag}")

    by_code = {row["code"]: row for row in payload.get("reasons") or []}
    require(set(by_code) == {"01", "02", "07", "33"}, "runtime reason inventory differs")
    require(by_code["01"]["has_salary_balance"] is True, "reason 01 lost salary balance")
    require(by_code["01"]["has_thirteenth"] is False and by_code["01"]["has_vacation"] is False, "reason 01 did not block proportionals")
    for code in ("02", "07", "33"):
        require(by_code[code]["has_thirteenth"] is True and by_code[code]["has_vacation"] is True, f"reason {code} lost proportionals")

    excluded = set(standard.get("excluded_items") or [])
    for item in ("notice_pay_or_notice_discount", "fgts_termination_fine", "fgts_withdrawal", "unemployment_insurance", "stability_indemnities", "fixed_term_contract_termination_rules", "variable_termination_items_not_explicitly_modeled"):
        require(item in excluded, f"H29 scope no longer excludes {item}")

    php = shutil.which("php")
    if php:
        for source in (H29_PAGE, *H29_PARTS):
            lint = subprocess.run([php, "-l", str(source)], cwd=ROOT, check=False, capture_output=True, text=True)
            require(lint.returncode == 0, f"H29 PHP lint failed for {source.name}: {lint.stderr or lint.stdout}")

    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "**Status:** CONCLUÍDO", "partial_estimate", "01/09/2025", "3/12", "7/12",
        "1000.00", "1033.33", "não afirma implantação no HostGator", "C6.7",
    ):
        require(marker in doc, f"C6.6 document missing marker: {marker}")

    ci = CI.read_text(encoding="utf-8")
    require("python scripts/validate_phase6_c66_gate.py" in ci, "Remake CI does not execute the permanent C6.6 gate")

    print(
        "Phase 6 C6.6 H29 gate: PASS "
        f"(release={release.release_id}, salary={salary_result['amount']}, "
        f"thirteenth={thirteenth['twelfths']}/12:{thirteenth['gross_thirteenth']}, "
        f"vacation={vacation_result['twelfths']}/12, promise={standard['result_promise']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
