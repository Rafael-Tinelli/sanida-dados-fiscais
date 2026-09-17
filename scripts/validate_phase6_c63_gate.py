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
H26 = ROOT / "consumers/frontend/salario-liquido.js"
H26_PAGE = ROOT / "consumers/frontend/salario-liquido-clt/index.php"
DOC = ROOT / "docs/phase6-c63-h26.md"
NODE_RUNTIME = ROOT / "tests/js/phase6_c63_h26_runtime.cjs"
PYTEST_FILE = ROOT / "tests/test_h26_c63.py"
CI = ROOT / ".github/workflows/remake-ci.yml"
STORE = ROOT / "releases/fiscal-v1"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C6.3 gate failed: {message}")


def main() -> int:
    for path in (CORE, H26, H26_PAGE, DOC, NODE_RUNTIME, PYTEST_FILE):
        require(path.is_file(), f"required C6.3 file missing: {path.relative_to(ROOT)}")

    release = FiscalReleaseStore(STORE).load_current()
    require(release is not None, "current fiscal release unavailable")
    assert release is not None
    require(release.status.value == "PUBLISHED", "current release is not PUBLISHED")
    require(release.schema_version == "1.2.0", "current release schema is not 1.2.0")
    require(
        release.consumer_compatibility.contract_api_version == "1.2.0",
        "current release API is not 1.2.0",
    )
    require("H26" in {item.value for item in release.consumer_compatibility.consumers}, "H26 not declared")
    require(len(release.rules) == 32, "current release is not 32/32")

    source = H26.read_text(encoding="utf-8")
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
        require(forbidden not in source, f"legacy/hardcoded fiscal logic remains in H26: {forbidden}")

    for marker in (
        "SFA.fetchRelease({ consumer: CONSUMER })",
        "SFA.assessInss(",
        "SFA.assessIrrf(",
        "grossTaxableIncome: gross.toString()",
        "socialSecurity: inss.amount",
        "release_id: release.release_id",
        "SFA.Decimal",
        "SFA.H26",
        "function errorMessageFor(",
        "Revise os dados informados",
        "A release fiscal necessária não pôde ser carregada ou validada",
    ):
        require(marker in source, f"H26 missing required C6.3 marker: {marker}")

    page = H26_PAGE.read_text(encoding="utf-8")
    require(
        page.index("/financas/calculadoras/assets/folha-core.js")
        < page.index("/financas/calculadoras/assets/salario-liquido.js"),
        "H26 page must load folha-core before H26 consumer",
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
        require(marker in page, f"H26 page missing auditable result marker: {marker}")
    require("time()" not in page, "H26 CSS cache key must not change on every request")
    require(page.count("?v=20260916-f06f10") == 2, "H26 CSS assets must use stable explicit version keys")

    node = shutil.which("node")
    require(node is not None, "node is required for C6.3 validation")
    syntax = subprocess.run(
        [node, "--check", str(H26)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    require(syntax.returncode == 0, f"H26 node --check failed: {syntax.stderr or syntax.stdout}")

    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    runtime = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(H26), str(artifact)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    require(runtime.returncode == 0, f"C6.3 Node runtime failed: {runtime.stderr or runtime.stdout}")
    payload = json.loads(runtime.stdout)
    h26 = payload.get("h26") or {}
    irrf = h26.get("irrf") or {}
    require(payload.get("release_id") == release.release_id, "H26 runtime used a different release_id")
    require((h26.get("fiscal_metadata") or {}).get("release_id") == release.release_id, "H26 memory lacks release_id")
    require(Decimal(str(h26.get("inss"))) == Decimal("641.51"), "canonical H26 INSS regression failed")
    require(Decimal(str(irrf.get("tax_base"))) == Decimal("5358.49"), "canonical H26 IRRF base regression failed")
    require(Decimal(str(irrf.get("reduction_input_income"))) == Decimal("6000.00"), "A01 reducer target regressed")
    require(Decimal(str(irrf.get("final_irrf"))) == Decimal("385.10"), "canonical H26 final IRRF regression failed")
    require(Decimal(str(h26.get("net_salary"))) == Decimal("4973.39"), "canonical H26 net salary regression failed")

    historical = payload.get("historical_a01") or {}
    require(Decimal(str(historical.get("irrf_tax_base"))) == Decimal("5350.40"), "historical A01 base regressed")
    require(Decimal(str(historical.get("reduction_input_income"))) == Decimal("6000.00"), "historical A01 reducer target regressed")
    require(Decimal(str(historical.get("final_irrf"))) == Decimal("382.88"), "historical A01 final IRRF regressed")
    require(payload.get("expired_reduction_rejected") is True, "unsupported vigency did not fail closed")
    require(payload.get("negative_input_rejected") is True, "negative monetary input was not rejected")
    require("release fiscal" not in str(payload.get("input_error_message", "")).lower(), "input error falsely blames fiscal release")
    require("release fiscal" in str(payload.get("fiscal_error_message", "")).lower(), "fiscal failure lacks causal release message")

    audit = (h26.get("fiscal_metadata") or {}).get("rules") or []
    ids = {item.get("rule_id") for item in audit if isinstance(item, dict)}
    require(
        {
            "inss.employee.progressive_table",
            "irrf.monthly.progressive_table",
            "irrf.reduction.2026",
            "irrf.dependent_deduction",
            "irrf.simplified_monthly_discount",
        }
        <= ids,
        "H26 audit trail lacks executed rule identities",
    )
    require(all(item.get("rule_version") for item in audit if isinstance(item, dict)), "H26 audit trail lacks rule_version")

    php = shutil.which("php")
    if php:
        php_lint = subprocess.run(
            [php, "-l", str(H26_PAGE)],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        require(php_lint.returncode == 0, f"H26 PHP lint failed: {php_lint.stderr or php_lint.stdout}")

    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "**Status:** CONCLUÍDO",
        "641.51",
        "5358.49",
        "385.10",
        "649.60",
        "5350.40",
        "382.88",
        "release_id",
        "implantação no HostGator",
        "C6.4",
    ):
        require(marker in doc, f"C6.3 document missing marker: {marker}")

    ci = CI.read_text(encoding="utf-8")
    require(
        "python scripts/validate_phase6_c63_gate.py" in ci,
        "Remake CI does not execute the permanent C6.3 gate",
    )

    print(
        "Phase 6 C6.3 H26 gate: PASS "
        f"(release={release.release_id}, inss={h26['inss']}, irrf={irrf['final_irrf']}, net={h26['net_salary']}, css=stable, errors=causal)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
