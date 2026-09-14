from __future__ import annotations

from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]


def test_validate_financial_evidence_entrypoint_imports_package_from_repo_root() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/validate_financial_evidence_v1.py"),
            "--help",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "Validate persisted source evidence for taxas_bacen.json." in completed.stdout
