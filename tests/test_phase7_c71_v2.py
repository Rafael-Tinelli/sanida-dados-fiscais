from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_phase7_c71_v2_gate_runs_from_repo_root() -> None:
    completed = subprocess.run(
        [sys.executable, str(ROOT / "scripts/validate_phase7_c71_v2_gate.py")],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "historical_v1=32 immutable" in completed.stdout
    assert "current_v2=33 including H25" in completed.stdout
