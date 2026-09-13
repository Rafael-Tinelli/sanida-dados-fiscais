from __future__ import annotations

from pathlib import Path

import pytest

import scraper


FORBIDDEN_LEGACY_PAYROLL_TOKENS = (
    "PINNED_INSS_URLS",
    "find_inss_article_url",
    "parse_inss_gov",
    "parse_irrf_receita",
    "@@search",
    "minimal_fallback_written",
    "static_reference_values",
)


def test_scraper_no_longer_contains_legacy_rfb_inss_discovery_or_static_fallback():
    text = Path("scraper.py").read_text(encoding="utf-8")
    for token in FORBIDDEN_LEGACY_PAYROLL_TOKENS:
        assert token not in text


def test_current_year_last_good_must_match_year():
    existing = {
        "ano": 2026,
        "dep": 189.59,
        "inss": [{"limite": 1621.0, "aliquota": 0.075}] * 3,
        "irrf": {
            "tabela": [
                {"limite": 2428.8, "aliquota": 0.0, "deducao": 0.0},
                {"limite": 2826.65, "aliquota": 0.075, "deducao": 182.16},
                {"limite": 3751.05, "aliquota": 0.15, "deducao": 394.16},
                {"limite": 9_000_000_000.0, "aliquota": 0.275, "deducao": 908.73},
            ],
            "simplificado": 607.2,
        },
        "taxas": {"selic": 14.25, "cdi": 14.15},
    }
    assert scraper.is_valid_current_year_last_good(existing, 2026) is True
    assert scraper.is_valid_current_year_last_good(existing, 2027) is False


def test_scraper_refuses_to_collect_when_registered_parsers_are_for_other_year(monkeypatch):
    called = False

    def fake_run(**kwargs):
        nonlocal called
        called = True
        raise AssertionError("network pipeline must not run on unsupported reference year")

    monkeypatch.setattr(scraper, "run_registered_source_pipeline", fake_run)
    with pytest.raises(RuntimeError, match="no canonical payroll parser registered"):
        scraper.collect_payroll_source_runs(2027, scraper.now_utc())
    assert called is False


def test_requirements_install_phase4_runtime_dependencies():
    text = Path("requirements.txt").read_text(encoding="utf-8")
    assert "-r requirements-contract.txt" in text
    assert "-r requirements-sources.txt" in text
