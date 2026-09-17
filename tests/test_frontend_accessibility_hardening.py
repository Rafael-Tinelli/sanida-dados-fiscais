from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_h26_binds_accessible_alert_and_result_status_in_runtime() -> None:
    source = _read("consumers/frontend/salario-liquido.js")
    for marker in (
        "alertBox.setAttribute('role', 'alert')",
        "alertBox.setAttribute('aria-live', 'assertive')",
        "alertBox.setAttribute('aria-atomic', 'true')",
        "result.setAttribute('role', 'status')",
        "result.setAttribute('aria-live', 'polite')",
        "result.setAttribute('aria-atomic', 'true')",
    ):
        assert marker in source


def test_h27_h28_h29_templates_expose_live_error_and_result_regions() -> None:
    paths = (
        "consumers/frontend/decimo-terceiro-clt/parts/02-calculator.php",
        "consumers/frontend/ferias-clt/parts/02-calculator.php",
        "consumers/frontend/rescisao-clt/parts/02-calculator.php",
    )
    for path in paths:
        source = _read(path)
        assert 'data-alert' in source
        assert 'role="alert"' in source
        assert 'aria-live="assertive"' in source
        assert 'aria-atomic="true"' in source
        assert 'data-result' in source
        assert 'role="status"' in source
        assert 'aria-live="polite"' in source
        assert 'aria-atomic="true"' in source
