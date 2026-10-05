"""Execute the AF01–AF04/R02 candidate JS without replacing published engines."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest

ROOT=Path(__file__).resolve().parents[1]


def test_af_candidate_runtime_isolated_from_published_release():
    node=shutil.which("node")
    if not node:
        pytest.skip("node unavailable")
    r=subprocess.run(
        [node,str(ROOT/"tests/js/af_candidate_runtime.cjs")],
        cwd=ROOT,check=False,capture_output=True,text=True,
    )
    assert r.returncode == 0, r.stderr
    result=json.loads(r.stdout)
    assert result["result"]=="PASS"
    assert result["cases"]>=12
    assert result["fixture"]=="SYNTHETIC_DO_NOT_PUBLISH"


def test_frontend_vacation_runtime_is_contract_driven_across_af01_promotion():
    frontend=(ROOT/"consumers/frontend/folha-vacation.js").read_text()
    candidate=(ROOT/"consumers/candidates/af01/folha-vacation.js").read_text()
    assert "cashThirdSocialSecurity" in frontend
    assert "incidenceValue(" in frontend
    assert "socialSecurityBase = socialSecurityBase.add(cashThird)" in frontend
    assert (
        "incidenceFlag(cashThirdIncidence, 'constitutional_third_on_cash_allowance', "
        "'social_security', 'no')" not in frontend
    )
    assert "'social_security', 'yes'" in candidate
