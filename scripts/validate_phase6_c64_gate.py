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

CORE = ROOT / "consumers/frontend/folha-core.js"
THIRTEENTH = ROOT / "consumers/frontend/folha-thirteenth.js"
H27 = ROOT / "consumers/frontend/decimo-terceiro.js"
H27_DIR = ROOT / "consumers/frontend/decimo-terceiro-clt"
H27_PAGE = H27_DIR / "index.php"
H27_PARTS = (
    H27_DIR / "parts/01-head-hero.php",
    H27_DIR / "parts/02-calculator.php",
    H27_DIR / "parts/03-guide.php",
    H27_DIR / "parts/04-faq-footer.php",
)
DOC = ROOT / "docs/phase6-c64-h27.md"
NODE_RUNTIME = ROOT / "tests/js/phase6_c64_h27_runtime.cjs"
PYTEST_FILE = ROOT / "tests/test_h27_c64.py"
ENTRYPOINT_TEST = ROOT / "tests/test_phase6_c64_gate.py"
CI = ROOT / ".github/workflows/remake-ci.yml"
STORE = ROOT / "releases/fiscal-v1"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C6.4 gate failed: {message}")


def assembled_page_source() -> str:
    return H27_PAGE.read_text(encoding="utf-8") + "\n" + "\n".join(
        part.read_text(encoding="utf-8") for part in H27_PARTS
    )


def main() -> int:
    for path in (
        CORE,
        THIRTEENTH,
        H27,
        H27_PAGE,
        *H27_PARTS,
        DOC,
        NODE_RUNTIME,
        PYTEST_FILE,
        ENTRYPOINT_TEST,
    ):
        require(path.is_file(), f"required C6.4 file missing: {path.relative_to(ROOT)}")

    release = FiscalReleaseStore(STORE).load_current()
    require(release is not None, "current fiscal release unavailable")
    assert release is not None
    require(release.status.value == "PUBLISHED", "current release is not PUBLISHED")
    require(release.schema_version == "1.2.0", "current release schema is not 1.2.0")
    require(
        release.consumer_compatibility.contract_api_version == "1.2.0",
        "current release API is not 1.2.0",
    )
    require(
        "H27" in {item.value for item in release.consumer_compatibility.consumers},
        "H27 not declared by release",
    )
    require(len(release.rules) == 32, "current release is not 32/32")

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
        require(forbidden not in h27, f"legacy/hardcoded H27 logic remains: {forbidden}")
        require(forbidden not in shared, f"parallel hardcoded thirteenth logic remains: {forbidden}")

    for marker in (
        "SFA.fetchRelease({ consumer: CONSUMER })",
        "SFA.THIRTEENTH.calculateAccrual(",
        "SFA.THIRTEENTH.calculateAdvance(",
        "SFA.THIRTEENTH.assessFiscal(",
        "release_id: release.release_id",
        "status: 'UNSUPPORTED'",
    ):
        require(marker in h27, f"H27 missing required C6.4 marker: {marker}")

    for marker in (
        "ruleId: 'technical.money_decimal_and_rounding'",
        "context: TECHNICAL_CONTEXT",
        "policy.stage !== 'per_component'",
        "dependencyOf: 'thirteenth.irrf.exclusive_assessment'",
        "ruleId: 'irrf.monthly.progressive_table'",
        "'thirteenth.inss.separate_assessment'",
        "'thirteenth.irrf.exclusive_assessment'",
        "SFA.assessmentIdentity('thirteenth', CONTEXT)",
        "assessment.reduction_rule_id",
        "SFA.executeProgressive(",
        "SFA.executeAffineReduction(",
    ):
        require(marker in shared, f"thirteenth shared runtime missing marker: {marker}")

    page = assembled_page_source()
    require(
        page.index("/financas/calculadoras/assets/folha-core.js")
        < page.index("/financas/calculadoras/assets/folha-thirteenth.js")
        < page.index("/financas/calculadoras/assets/decimo-terceiro.js"),
        "H27 page must load core, thirteenth extension, then consumer",
    )
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
        "Regra própria",
    ):
        require(marker in page, f"H27 page missing marker: {marker}")
    require("R$ 2.425,00" not in page, "legacy static half-of-total preview remains in H27 page")

    node = shutil.which("node")
    require(node is not None, "node is required for C6.4 validation")
    for source in (THIRTEENTH, H27):
        syntax = subprocess.run(
            [node, "--check", str(source)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        require(
            syntax.returncode == 0,
            f"node --check failed for {source.name}: {syntax.stderr or syntax.stdout}",
        )

    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    runtime = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(THIRTEENTH), str(H27), str(artifact)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    require(runtime.returncode == 0, f"C6.4 Node runtime failed: {runtime.stderr or runtime.stdout}")
    payload = json.loads(runtime.stdout)
    standard = payload.get("standard") or {}
    fiscal = standard.get("fiscal") or {}
    irrf = fiscal.get("irrf") or {}
    require(payload.get("release_id") == release.release_id, "H27 runtime used a different release_id")
    require(
        (standard.get("fiscal_metadata") or {}).get("release_id") == release.release_id,
        "H27 memory lacks release_id",
    )
    require((standard.get("accrual") or {}).get("twelfths") == 12, "full-year H27 accrual is not 12/12")
    require(Decimal(str(standard.get("gross_thirteenth"))) == Decimal("4000.00"), "H27 gross standard regression failed")
    require((standard.get("advance") or {}).get("status") == "CALCULATED", "standard advance was not calculated")
    require(Decimal(str((standard.get("advance") or {}).get("amount"))) == Decimal("2000.00"), "standard advance regression failed")
    require(Decimal(str(fiscal.get("inss"))) == Decimal("368.60"), "H27 separate INSS regression failed")
    require(Decimal(str(irrf.get("final_irrf"))) == Decimal("0.00"), "H27 separate IRRF regression failed")
    require(Decimal(str(standard.get("second_installment_net"))) == Decimal("1631.40"), "H27 second installment regression failed")
    require((payload.get("boundary14") or {}).get("twelfths") == 0, "14-day boundary regressed")
    require((payload.get("boundary15") or {}).get("twelfths") == 1, "15-day boundary regressed")
    require(
        (payload.get("variable_unsupported") or {}).get("advance", {}).get("status") == "UNSUPPORTED",
        "variable advance did not fail explicitly",
    )
    require(
        (payload.get("admission_unsupported") or {}).get("advance", {}).get("status") == "UNSUPPORTED",
        "admission-year advance did not fail explicitly",
    )
    require(
        (payload.get("variable_reported") or {}).get("advance", {}).get("status") == "REPORTED",
        "reported advance was not accepted",
    )
    require(payload.get("missing_advance_reference_rejected") is True, "missing standard advance reference did not fail closed")
    require(payload.get("negative_input_rejected") is True, "negative H27 input was not rejected")
    require(payload.get("technical_rounding_rejected") is True, "missing technical rounding policy did not fail closed")

    audit = (standard.get("fiscal_metadata") or {}).get("rules") or []
    ids = {item.get("rule_id") for item in audit if isinstance(item, dict)}
    required_ids = {
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
    require(required_ids <= ids, "H27 audit trail lacks required rule identities")
    require(all(item.get("rule_version") for item in audit if isinstance(item, dict)), "H27 audit trail lacks rule_version")
    technical_audit = next(
        (item for item in audit if isinstance(item, dict) and item.get("rule_id") == "technical.money_decimal_and_rounding"),
        None,
    )
    require(technical_audit is not None and technical_audit.get("context") == "technical", "H27 technical rounding audit context is invalid")

    php = shutil.which("php")
    if php:
        for source in (H27_PAGE, *H27_PARTS):
            lint = subprocess.run(
                [php, "-l", str(source)],
                cwd=ROOT,
                check=False,
                capture_output=True,
                text=True,
            )
            require(lint.returncode == 0, f"H27 PHP lint failed for {source.name}: {lint.stderr or lint.stdout}")

    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "**Status:** CONCLUÍDO",
        "15 dias",
        "salário do mês anterior",
        "2000.00",
        "368.60",
        "1631.40",
        "UNSUPPORTED",
        "release_id",
        "não afirma implantação no HostGator",
        "C6.5",
    ):
        require(marker in doc, f"C6.4 document missing marker: {marker}")

    ci = CI.read_text(encoding="utf-8")
    require(
        "python scripts/validate_phase6_c64_gate.py" in ci,
        "Remake CI does not execute the permanent C6.4 gate",
    )

    print(
        "Phase 6 C6.4 H27 gate: PASS "
        f"(release={release.release_id}, gross={standard['gross_thirteenth']}, "
        f"advance={standard['advance']['amount']}, inss={fiscal['inss']}, "
        f"irrf={irrf['final_irrf']}, second_net={standard['second_installment_net']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
