from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "consumers/runtime/salario-liquido-runtime.js"
LEGACY = ROOT / "consumers/frontend/salario-liquido.js"
CORE = ROOT / "consumers/frontend/folha-core.js"
NODE_RUNTIME = ROOT / "tests/js/phase6_c63_h26_runtime.cjs"
STORE = ROOT / "releases/fiscal-v1"


def test_h26_split_runtime_is_pure_fiscal_and_dom_free() -> None:
    source = RUNTIME.read_text(encoding="utf-8")
    legacy = LEGACY.read_text(encoding="utf-8")

    for marker in (
        "SFA.assessInss(",
        "SFA.assessIrrf(",
        "grossTaxableIncome: gross.toString()",
        "socialSecurity: inss.amount",
        "release_id: release.release_id",
        "SFA.H26 = Object.freeze",
        "function errorMessageFor(",
    ):
        assert marker in source
        assert marker in legacy

    for forbidden in (
        "root.document",
        "querySelector(",
        ".style.",
        "addEventListener(",
        "data-kpi",
        "data-row",
        "aria-live",
    ):
        assert forbidden not in source


def test_h26_split_runtime_preserves_c63_reference_results() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node unavailable")

    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    proc = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(RUNTIME), str(artifact)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout

    payload = json.loads(proc.stdout)
    h26 = payload["h26"]
    irrf = h26["irrf"]

    assert Decimal(str(h26["inss"])) == Decimal("641.51")
    assert Decimal(str(irrf["tax_base"])) == Decimal("5358.49")
    assert Decimal(str(irrf["reduction_input_income"])) == Decimal("6000.00")
    assert Decimal(str(irrf["final_irrf"])) == Decimal("385.10")
    assert Decimal(str(h26["net_salary"])) == Decimal("4973.39")
    assert payload["expired_reduction_rejected"] is True
    assert payload["negative_input_rejected"] is True
