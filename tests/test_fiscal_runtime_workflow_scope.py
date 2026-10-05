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
