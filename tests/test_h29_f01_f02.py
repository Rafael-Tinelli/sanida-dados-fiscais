from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "consumers/frontend/folha-core.js"
TERMINATION = ROOT / "consumers/frontend/folha-termination.js"
H29 = ROOT / "consumers/frontend/rescisao-clt.js"
H29_CALCULATOR = ROOT / "consumers/frontend/rescisao-clt/parts/02-calculator.php"
NODE_RUNTIME = ROOT / "tests/js/h29_f01_f02_runtime.cjs"


def _runtime_result() -> dict:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required for H29 F01/F02 runtime regression tests")
    completed = subprocess.run(
        [node, str(NODE_RUNTIME), str(CORE), str(TERMINATION), str(H29)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout)


def test_f01_required_h29_money_never_silently_becomes_zero() -> None:
    result = _runtime_result()
    assert result["required_money_rejections"] == {
        "empty": True,
        "whitespace": True,
        "zero": True,
        "zero_brl": True,
        "invalid_text": True,
    }
    assert result["positive_money"] == "3100.5"


def test_f01_wrapper_has_no_conditional_money_fallback_to_zero() -> None:
    source = H29.read_text(encoding="utf-8")
    assert "terminationMonthRemuneration: inputValue('remuneracao_mes_desligamento') || '0'" not in source
    assert "requirePositiveMoneyInput(" in source
    assert "reason === '01'" in source
    assert "h29_required_money" in source


def test_f02_default_days_respects_same_month_admission() -> None:
    result = _runtime_result()
    assert result["days"] == {
        "same_month": 11,
        "same_month_first_day": 20,
        "prior_month": 20,
        "february_non_leap": 19,
        "february_leap": 20,
    }
    assert result["invalid_order_rejected"] is True


def test_f02_wrapper_recomputes_from_both_dates_and_keeps_manual_override() -> None:
    source = H29.read_text(encoding="utf-8")
    assert "defaultDaysCounted(employmentStartField.value, terminationDateField.value)" in source
    assert "employmentStartField.addEventListener('change'" in source
    assert "terminationDateField.addEventListener('change'" in source
    assert "days.dataset.userEdited = 'true'" in source
    assert "days.value = String(Number(match[3]))" not in source


def test_h29_copy_discloses_days_assumption_and_required_bases() -> None:
    source = H29_CALCULATOR.read_text(encoding="utf-8")
    assert "dias inclusivos entre as duas datas" in source
    assert "Ajuste o campo se houve férias, afastamentos" in source
    assert "deve ser maior que zero" in source
    assert "Quando o motivo inclui 13º proporcional" in source
