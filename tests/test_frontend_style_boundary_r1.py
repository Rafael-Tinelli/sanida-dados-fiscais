from pathlib import Path

from scripts.audit_frontend_style_boundary import build_report


EMBEDDED_STYLE_ALLOWLIST = {
    "consumers/frontend/ferias-clt/parts/01-head-hero.php",
    "consumers/frontend/rescisao-clt/parts/01-head-hero.php",
}

INLINE_STYLE_FILE_ALLOWLIST = {
    "consumers/frontend/calculadoras/index.php",
    "consumers/frontend/salario-liquido-clt/index.php",
    "consumers/frontend/ferias-clt/parts/01-head-hero.php",
    "consumers/frontend/ferias-clt/parts/02-calculator.php",
    "consumers/frontend/ferias-clt/parts/03-guide.php",
    "consumers/frontend/ferias-clt/parts/04-faq-footer.php",
    "consumers/frontend/rescisao-clt/parts/01-head-hero.php",
    "consumers/frontend/rescisao-clt/parts/02-calculator.php",
    "consumers/frontend/rescisao-clt/parts/03-guide.php",
    "consumers/frontend/rescisao-clt/parts/04-faq-footer.php",
}

KNOWN_STYLESHEET_DEPENDENCIES = {
    "calculadoras-ui.css",
    "salario-liquido-clt.css",
    "calculadora-decimo-terceiro-ui.css",
}


def paths(items: list[dict]) -> set[str]:
    return {item["path"] for item in items}


def test_frontend_style_debt_does_not_spread_to_new_templates() -> None:
    report = build_report()

    assert paths(report["embedded_style_files"]) <= EMBEDDED_STYLE_ALLOWLIST
    assert paths(report["inline_style_files"]) <= INLINE_STYLE_FILE_ALLOWLIST
    assert paths(report["important_files"]) <= EMBEDDED_STYLE_ALLOWLIST


def test_current_stylesheet_boundary_is_explicit() -> None:
    report = build_report()
    referenced_names = {
        Path(href.split("?", 1)[0]).name
        for href in report["stylesheet_references"]
    }

    assert KNOWN_STYLESHEET_DEPENDENCIES <= referenced_names


def test_audit_report_remains_machine_readable_and_scoped() -> None:
    report = build_report()

    assert report["schema_version"] == "1.0.0"
    assert report["frontend_root"] == "consumers/frontend"
    assert report["php_files_scanned"] >= 10
    assert isinstance(report["repo_css_files"], list)
    assert isinstance(
        report["stylesheet_dependencies_without_source_file_under_consumers_frontend"],
        list,
    )
