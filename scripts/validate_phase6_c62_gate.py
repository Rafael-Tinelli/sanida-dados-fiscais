from __future__ import annotations

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
DOC = ROOT / "docs/phase6-c62-folha-core.md"
NODE_RUNTIME = ROOT / "tests/js/phase6_c62_runtime.cjs"
PYTEST_FILE = ROOT / "tests/test_folha_core_c62.py"
CI = ROOT / ".github/workflows/remake-ci.yml"
STORE = ROOT / "releases/fiscal-v1"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"C6.2 gate failed: {message}")


def main() -> int:
    for path in (CORE, DOC, NODE_RUNTIME, PYTEST_FILE):
        require(path.is_file(), f"required C6.2 file missing: {path.relative_to(ROOT)}")

    release = FiscalReleaseStore(STORE).load_current()
    require(release is not None, "current fiscal release unavailable")
    assert release is not None
    require(release.status.value == "PUBLISHED", "current release is not PUBLISHED")
    require(release.schema_version == "1.2.0", "current release schema is not 1.2.0")
    require(
        release.consumer_compatibility.contract_api_version == "1.2.0",
        "current release API is not 1.2.0",
    )
    require(len(release.rules) == 32, "current release is not 32/32")

    text = CORE.read_text(encoding="utf-8")
    forbidden = (
        "dados_fiscais.json",
        "/blog/wp-json/sfa/v1/folha",
        "SFA.endpoints",
        "SFA.calcINSS",
        "SFA.calcIR",
        "startOfCurrentVacationPeriod",
        "avosFeriasProporcionais",
        "daysInTerminationMonth",
        "607.20",
        "189.59",
        "1621.00",
        "2902.84",
        "4354.27",
        "8475.55",
    )
    for token in forbidden:
        require(token not in text, f"legacy/duplicated fiscal logic remains in folha-core: {token}")

    required_markers = (
        "/blog/wp-json/sfa/v1/fiscal-release",
        "SUPPORTED_CONSUMERS",
        "unsupported_behavior !== 'hard_fail'",
        "release.status !== 'PUBLISHED'",
        "release.rules.length !== 32",
        "class DecimalValue",
        "BigInt",
        "binary_float_rejected",
        "function isValidIsoCivilDate(",
        "'date_invalid'",
        "function normalizeDate(",
        "function selectRule(",
        "rule_version",
        "function assessmentIdentity(",
        "function competenceBasisFor(",
        "function executeProgressive(",
        "function executeAffineReduction(",
        "function assessInss(",
        "function assessIrrf(",
        "release_id",
        "input_semantic",
        "vacation.irrf.reduction.2026",
        "taxable_vacation_income_subject_to_separate_monthly_irrf_assessment_before_deductions",
        "function enhanceDynamicRegions(",
        "setAttribute('role', 'alert')",
        "setAttribute('role', 'status')",
        "setAttribute('aria-live', 'assertive')",
        "setAttribute('aria-live', 'polite')",
    )
    for marker in required_markers:
        require(marker in text, f"folha-core missing required C6.2 marker: {marker}")

    doc = DOC.read_text(encoding="utf-8")
    for marker in (
        "**Status:** CONCLUÍDO",
        "/blog/wp-json/sfa/v1/fiscal-release",
        "BigInt",
        "paridade",
        "C6.3",
        "não afirma implantação no HostGator",
    ):
        require(marker in doc, f"C6.2 document missing marker: {marker}")

    node = shutil.which("node")
    require(node is not None, "node is required for C6.2 validation")
    syntax = subprocess.run(
        [node, "--check", str(CORE)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    require(syntax.returncode == 0, f"node --check failed: {syntax.stderr or syntax.stdout}")

    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    runtime = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(artifact)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    require(runtime.returncode == 0, f"C6.2 Node runtime failed: {runtime.stderr or runtime.stdout}")
    payload = json.loads(runtime.stdout)
    require(payload.get("release_id") == release.release_id, "Node runtime used a different release_id")
    require(payload.get("float_rejected") is True, "binary float was not rejected")
    require(
        payload.get("normalized_dates") == {
            "regular": "2026-09-16",
            "leap_day": "2028-02-29",
            "date_object": "2028-02-29",
        },
        "valid civil dates were not normalized deterministically",
    )
    civil_rejections = payload.get("civil_date_rejections") or {}
    for invalid_date in (
        "2026-02-29",
        "2026-02-31",
        "2026-04-31",
        "2026-00-10",
        "2026-13-01",
        "2026-01-00",
    ):
        require(civil_rejections.get(invalid_date) is True, f"impossible civil date was accepted: {invalid_date}")
    require(payload.get("malformed_date_rejected") is True, "malformed ISO date did not fail closed")
    require(
        payload.get("impossible_selection_date_rejected") is True,
        "selectRule accepted an impossible civil targetDate",
    )
    require(payload.get("missing_rule_rejected") is True, "missing rule did not fail closed")
    require(
        payload.get("incompatible_assessment_rejected") is True,
        "invalid assessment identity did not fail closed",
    )
    vacation_identity = payload.get("vacation_identity") or {}
    require(
        vacation_identity.get("reduction_rule_id") == "vacation.irrf.reduction.2026",
        "shared vacation assessment did not select dedicated reduction",
    )
    vacation_rules = ((payload.get("vacation_irrf") or {}).get("audit") or {}).get("rules") or []
    vacation_rule_ids = {item.get("rule_id") for item in vacation_rules if isinstance(item, dict)}
    require("vacation.irrf.reduction.2026" in vacation_rule_ids, "vacation runtime audit lacks dedicated reduction")
    require("irrf.reduction.2026" not in vacation_rule_ids, "vacation runtime regressed to generic monthly reduction")

    ci = CI.read_text(encoding="utf-8")
    require(
        "python scripts/validate_phase6_c62_gate.py" in ci,
        "Remake CI does not execute the permanent C6.2 gate",
    )

    print(
        "Phase 6 C6.2 folha-core gate: PASS "
        f"(release={release.release_id}, rules={len(release.rules)}, vacation_irrf=dedicated, civil_dates=strict, dynamic_regions=a11y)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
