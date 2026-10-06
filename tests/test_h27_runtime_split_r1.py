from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "consumers/frontend/folha-core.js"
THIRTEENTH = ROOT / "consumers/frontend/folha-thirteenth.js"
H27_RUNTIME = ROOT / "consumers/runtime/decimo-terceiro-runtime.js"
NODE_RUNTIME = ROOT / "tests/js/phase6_c64_h27_runtime.cjs"
STORE = ROOT / "releases/fiscal-v1"


def _run() -> dict:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for H27 runtime split parity")
    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    completed = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(THIRTEENTH), str(H27_RUNTIME), str(artifact)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def test_h27_runtime_split_preserves_standard_c64_result() -> None:
    result = _run()["standard"]
    assert result["consumer"] == "H27"
    assert result["accrual"]["twelfths"] == 12
    assert Decimal(result["gross_thirteenth"]) == Decimal("4000.00")
    assert result["advance"]["status"] == "CALCULATED"
    assert Decimal(result["advance"]["amount"]) == Decimal("2000.00")
    assert Decimal(result["fiscal"]["inss"]) == Decimal("368.60")
    assert Decimal(result["fiscal"]["irrf"]["final_irrf"]) == Decimal("0.00")
    assert Decimal(result["second_installment_gross"]) == Decimal("2000.00")
    assert Decimal(result["second_installment_net"]) == Decimal("1631.40")
    assert result["settlement"]["status"] == "PAYABLE"


def test_h27_runtime_split_preserves_fail_closed_and_floor_zero_contract() -> None:
    result = _run()
    assert result["missing_advance_reference_rejected"] is True
    assert result["negative_input_rejected"] is True
    assert result["technical_rounding_rejected"] is True
    assert result["variable_unsupported"]["settlement"]["status"] == "PENDING_ADVANCE"
    assert result["admission_unsupported"]["settlement"]["status"] == "PENDING_ADVANCE"

    matrix = result["settlement_matrix"]
    assert matrix["avos1"]["settlement"]["status"] == "INSUFFICIENT"
    assert Decimal(matrix["avos1"]["second_installment_net"]) == Decimal("0")
    assert Decimal(matrix["avos1"]["settlement"]["insufficiency_amount"]) == Decimal("1691.67")
    assert matrix["avos7"]["settlement"]["status"] == "PAYABLE"
    assert Decimal(matrix["avos7"]["second_installment_net"]) == Decimal("147.65")


def test_h27_runtime_split_has_no_dom_or_presentation_ownership() -> None:
    text = H27_RUNTIME.read_text(encoding="utf-8")
    for forbidden in (
        "document",
        "querySelector",
        "addEventListener",
        ".style.",
        "textContent",
        "data-kpi",
        "data-row",
    ):
        assert forbidden not in text
    for required in (
        "SFA.THIRTEENTH.calculateAccrual(",
        "SFA.THIRTEENTH.calculateAdvance(",
        "SFA.THIRTEENTH.assessFiscal(",
        "buildSettlement(",
        "SFA.H27 = Object.freeze",
    ):
        assert required in text
