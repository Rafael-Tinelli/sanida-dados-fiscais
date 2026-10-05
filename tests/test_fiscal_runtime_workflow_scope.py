from pathlib import Path


def test_runtime_production_workflow_never_triggers_from_candidate_assets():
    text = Path(".github/workflows/fiscal-runtime-production.yml").read_text(encoding="utf-8")
    assert "consumers/**/" not in text
    for path in (
        "consumers/frontend/folha-core.js",
        "consumers/frontend/folha-thirteenth.js",
        "consumers/frontend/folha-vacation.js",
        "consumers/frontend/folha-termination.js",
        "consumers/runtime/salario-liquido-runtime.js",
        "consumers/runtime/decimo-terceiro-runtime.js",
    ):
        assert path in text
    assert "consumers/candidates/" not in text
    trigger_block = text.split("workflow_dispatch:", 1)[0]
    for non_runtime_path in (
        "scripts/deploy_fiscal_runtime_assets.py",
        "ops/fiscal-runtime-assets.htaccess",
        ".github/workflows/fiscal-runtime-production.yml",
    ):
        assert non_runtime_path not in trigger_block
