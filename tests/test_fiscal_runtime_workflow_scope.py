from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_fiscal_runtime_deploy_workflow_is_archived_not_active():
    active = ROOT / ".github/workflows/fiscal-runtime-production.yml"
    archived = ROOT / "ops/workflows/fiscal-runtime-production.yml"
    assert not active.exists()
    assert archived.exists()
    text = archived.read_text(encoding="utf-8")
    assert "HISTORICAL / MANUAL REFERENCE ONLY" in text
    assert "consumers/candidates/" not in text
