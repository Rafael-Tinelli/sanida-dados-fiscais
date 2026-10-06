from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
CORE = ROOT / "consumers/frontend/folha-core.js"
THIRTEENTH = ROOT / "consumers/frontend/folha-thirteenth.js"
RUNTIME = ROOT / "consumers/runtime/decimo-terceiro-runtime.js"
NODE = ROOT / "tests/js/h27_pending_advance_runtime.cjs"
STORE = ROOT / "releases/fiscal-v1"


def test_h27_published_split_runtime_handles_unreported_advance_without_throwing() -> None:
    node = shutil.which("node")
    if not node:
        pytest.skip("node is required")

    current = json.loads((STORE / "current.json").read_text(encoding="utf-8"))
    release = STORE / current["artifact"]

    completed = subprocess.run(
        [node, str(NODE), str(CORE), str(THIRTEENTH), str(RUNTIME), str(release)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result == {
        "result": "PASS",
        "pending_status": "PENDING",
        "settlement_status": "PENDING_ADVANCE",
        "partial_input_guards": True,
        "reported_zero_supported": True,
    }
