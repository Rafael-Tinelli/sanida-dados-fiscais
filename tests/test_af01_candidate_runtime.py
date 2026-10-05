"""AF01 corrected JavaScript evaluated with an explicitly SYNTHETIC successor.

Never publish the synthetic mutated release. The immutable published release
continues to be exercised by the existing historical baseline tests.
"""
import json
from decimal import Decimal
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT=Path(__file__).resolve().parents[1]
OLD="fiscal-v1-sha256-a741aa7873950d029a5c6b1c929727267125424013f09c69137b7e80b294153e"

def test_af01_candidate_runtime_statutory_full_third(tmp_path):
    node=shutil.which("node")
    if node is None:
        pytest.skip("node unavailable")
    previous=json.loads((ROOT/"releases/fiscal-v1/releases"/f"{OLD}.json").read_text())
    assert previous["release_id"] == OLD
    rule=next(x for x in previous["rules"]
              if x["rule_id"]=="vacation.abono_constitutional_third.ir_incidence")
    original=rule["payload"]["components"][0]
    assert original["social_security"]=="no"
    original["social_security"]="yes"
    previous["release_id"]="fiscal-v1-sha256-"+("f"*64)
    previous["notes"]=["SYNTHETIC TEST FIXTURE -- NEVER PUBLISH"]
    fixture=tmp_path/"synthetic-af01-release.json"
    fixture.write_text(json.dumps(previous))
    runner=ROOT/"tests/js/phase6_c65_h28_runtime.cjs"
    args=[
        node,str(runner),
        str(ROOT/"consumers/frontend/folha-core.js"),
        str(ROOT/"consumers/candidates/af01/folha-vacation.js"),
        str(ROOT/"consumers/frontend/ferias-clt.js"),
        str(fixture),
    ]
    run=subprocess.run(args,capture_output=True,text=True,cwd=ROOT,check=False)
    assert run.returncode==0,run.stderr
    result=json.loads(run.stdout)
    sale=result["standard"]
    no_sale=result["no_sale"]
    assert Decimal(sale["fiscal"]["social_security_base"])==Decimal("4000.00")
    assert sale["components"]["cash_allowance_constitutional_third"]=="444.44"
    assert Decimal(sale["fiscal"]["taxable_vacation_income"])==Decimal("4000.00")
    assert no_sale["components"]["cash_allowance_constitutional_third"]=="0.00"
    assert Decimal(no_sale["fiscal"]["social_security_base"])==Decimal("5333.33")
