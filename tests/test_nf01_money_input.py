from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]
NODE_TEST = ROOT / "tests/js/test_nf01_money_input_runtime.cjs"


def _run() -> dict:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for NF01 money-input regression tests")
    completed = subprocess.run(
        [node, str(NODE_TEST)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def test_nf01_valid_human_and_canonical_money_inputs_converge() -> None:
    result = _run()
    normalized = {row["input"]: row for row in result["normalized"]}

    for value in ("4000", "4.000", "4.000,00", "R$ 4.000,00", "4000.00"):
        assert normalized[value]["accepted"] is True
        assert normalized[value]["result"] == "4000"


def test_nf01_malformed_money_inputs_fail_closed() -> None:
    result = _run()

    for row in result["rejected"]:
        assert row["accepted"] is False, row
        assert row["code"] == "decimal_format"


def test_nf01_h26_h29_preserve_equivalent_values_and_reject_garbage() -> None:
    result = _run()
    rows = result["flows"]

    for consumer in ("H26", "H27", "H28", "H29"):
        valid = [row for row in rows if row["id"] == consumer and row["input"] in {
            "4000", "4.000", "4.000,00", "R$ 4.000,00", "4000.00"
        }]
        assert len(valid) == 5
        assert all(row["accepted"] is True for row in valid), valid

        invalid = [row for row in rows if row["id"] == consumer and row["input"] in {
            "20mil", "abc", "1e3", "1,2,3", "1.2.3"
        }]
        assert len(invalid) == 5
        assert all(row["accepted"] is False for row in invalid), invalid
        assert all(row["code"] == "decimal_format" for row in invalid), invalid


def test_nf01_optional_invalid_money_never_becomes_zero() -> None:
    result = _run()
    assert result["optional_invalid"]
    assert all(row["accepted"] is False for row in result["optional_invalid"])
    assert all(row["code"] == "decimal_format" for row in result["optional_invalid"])
