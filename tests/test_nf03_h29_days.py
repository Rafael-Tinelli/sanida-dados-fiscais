from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
import shutil
import subprocess

import pytest

from sanida_fiscal.contract_v1 import FiscalContractV1
from sanida_fiscal.engine_v1 import FiscalEngineError
from sanida_fiscal.termination_v1 import calculate_salary_balance, select_termination_rule_bundle

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"
STORE = ROOT / "releases" / "fiscal-v1"
CORE = ROOT / "consumers" / "frontend" / "folha-core.js"
TERMINATION = ROOT / "consumers" / "frontend" / "folha-termination.js"
NODE_RUNTIME = ROOT / "tests" / "js" / "nf03_h29_days_runtime.cjs"


def _contract() -> FiscalContractV1:
    return FiscalContractV1.model_validate(json.loads(EXAMPLE.read_text(encoding="utf-8")))


def _salary_balance(*, employment_start: date, termination_date: date, days: int):
    bundle = select_termination_rule_bundle(_contract(), termination_date)
    return calculate_salary_balance(
        monthly_base_salary="3100.00",
        termination_date=termination_date,
        days_counted_through_termination=days,
        payload=bundle.salary_proration,
        rounding_policy=bundle.salary_rounding,
        employment_start=employment_start,
    )


def _node_result() -> dict:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for NF03 runtime regression tests")
    manifest = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    artifact = STORE / manifest["artifact"]
    completed = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(TERMINATION), str(artifact)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def test_nf03_same_month_interval_is_an_inclusive_upper_bound() -> None:
    valid = _salary_balance(
        employment_start=date(2026, 1, 18),
        termination_date=date(2026, 1, 31),
        days=14,
    )
    assert valid.days_counted_through_termination == 14
    assert valid.amount == Decimal("1400.00")

    for invalid in (15, 31):
        with pytest.raises(FiscalEngineError, match="employment interval"):
            _salary_balance(
                employment_start=date(2026, 1, 18),
                termination_date=date(2026, 1, 31),
                days=invalid,
            )


def test_nf03_prior_month_employment_still_allows_termination_day_as_upper_bound() -> None:
    result = _salary_balance(
        employment_start=date(2025, 12, 15),
        termination_date=date(2026, 1, 31),
        days=31,
    )
    assert result.days_counted_through_termination == 31
    assert result.amount == Decimal("3100.00")


@pytest.mark.parametrize(
    ("employment_start", "termination_date", "maximum"),
    [
        (date(2026, 2, 15), date(2026, 2, 28), 14),
        (date(2026, 3, 18), date(2026, 3, 31), 14),
        (date(2026, 4, 17), date(2026, 4, 30), 14),
    ],
)
def test_nf03_month_lengths_do_not_change_the_employment_interval_bound(
    employment_start: date,
    termination_date: date,
    maximum: int,
) -> None:
    assert _salary_balance(
        employment_start=employment_start,
        termination_date=termination_date,
        days=maximum,
    ).days_counted_through_termination == maximum

    with pytest.raises(FiscalEngineError, match="employment interval"):
        _salary_balance(
            employment_start=employment_start,
            termination_date=termination_date,
            days=maximum + 1,
        )


def test_nf03_js_runtime_rejects_days_outside_employment_and_preserves_accrual_boundaries() -> None:
    result = _node_result()

    assert result["same_month_14"] == {
        "days": 14,
        "amount": "1400.00",
        "fifteenth_rejected": True,
        "thirty_first_rejected": True,
    }
    assert result["prior_month_31"] == {
        "days": 31,
        "amount": "3100.00",
    }
    assert all(row["valid_days"] == row["maximum"] for row in result["month_boundaries"])
    assert all(row["excess_rejected"] is True for row in result["month_boundaries"])
    assert result["accrual_boundaries_preserved"] == {
        "fourteen": {"thirteenth": 0, "vacation": 0},
        "fifteen": {"thirteenth": 1, "vacation": 1},
    }
