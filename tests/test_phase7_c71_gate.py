from __future__ import annotations

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def test_phase7_c71_gate_runs_as_direct_script_from_repo_root() -> None:
    completed = subprocess.run(
        [sys.executable, str(ROOT / "scripts/validate_phase7_c71_gate.py")],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr or completed.stdout
    assert "Phase 7 C7.1 gate: PASS" in completed.stdout
