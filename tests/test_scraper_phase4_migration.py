from __future__ import annotations

import json
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

FORBIDDEN_FINANCIAL_CONSUMER_TOKENS = (
    "SFA_TAXAS_JSON_URL",
    "TAXAS_JSON_URL_DEFAULT",
    "raw.githubusercontent.com",
    '"remote_url"',
)


def test_scraper_no_longer_contains_legacy_rfb_inss_discovery_or_static_fallback():
    text = Path("scraper.py").read_text(encoding="utf-8")
    for token in FORBIDDEN_LEGACY_PAYROLL_TOKENS:
        assert token not in text


def test_scraper_no_longer_has_remote_financial_fallback():
    text = Path("scraper.py").read_text(encoding="utf-8")
    for token in FORBIDDEN_FINANCIAL_CONSUMER_TOKENS:
        assert token not in text
    assert "verify_financial_artifact_evidence" in text
    assert "FINANCIAL_ARTIFACT_SCHEMA_VERSION" in text


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
        "taxas": {
            "selic": 14.0,
            "cdi": 13.9,
            "cdi_basis": "bcb_sgs_12_daily_compounded_252",
        },
    }
    assert scraper.is_valid_current_year_last_good(existing, 2026) is True
    assert scraper.is_valid_current_year_last_good(existing, 2027) is False


def _phase4_taxas_payload() -> dict:
    return {
        "schema_version": "1.4.0",
        "meta": {
            "generated_at_utc": "2026-09-13T22:00:00Z",
            "sources": {
                "selic": {
                    "source_id": "BCB_SELIC_META_SGS_432",
                    "series_code": 432,
                    "snapshot_sha256": "1" * 64,
                    "candidate_sha256": "2" * 64,
                    "parser_id": "bcb_selic_meta_sgs432_v1",
                    "parser_version": "1.0.0",
                    "source_value_pct": "14.00",
                },
                "cdi": {
                    "source_id": "BCB_CDI_DAILY_SGS_12",
                    "series_code": 12,
                    "snapshot_sha256": "3" * 64,
                    "candidate_sha256": "4" * 64,
                    "parser_id": "bcb_cdi_daily_sgs12_v1",
                    "parser_version": "1.0.0",
                    "annualized_value_pct": "13.90",
                },
            },
        },
        "taxas": {
            "selic": 14.0,
            "cdi": 13.9,
            "cdi_basis": "bcb_sgs_12_daily_compounded_252",
        },
    }


def test_scraper_rejects_pre_migration_taxas_1_3_even_when_numbers_look_valid():
    legacy = {
        "schema_version": "1.3.0",
        "meta": {
            "generated_at_utc": "2026-09-13T18:10:58Z",
            "sources": {"selic": {"source": "sgs_432"}, "cdi": {"source": "b3_ftp_taxa_di_txt"}},
        },
        "taxas": {"selic": 14.0, "cdi": 13.9, "cdi_basis": "b3_ftp_taxa_di_txt_aa"},
    }
    ok, errors = scraper.validate_taxas_payload(legacy)
    assert ok is False
    assert "schema_version:unexpected" in errors
    assert "taxas.cdi_basis:unexpected" in errors


def test_scraper_rejects_tampered_financial_value_that_disagrees_with_provenance():
    payload = _phase4_taxas_payload()
    payload["taxas"]["cdi"] = 12.34
    ok, errors = scraper.validate_taxas_payload(payload)
    assert ok is False
    assert "taxas.cdi:does_not_match_source_provenance" in errors


def test_load_taxas_requires_durable_financial_evidence(tmp_path: Path):
    artifact = tmp_path / "taxas_bacen.json"
    artifact.write_text(json.dumps(_phase4_taxas_payload()), encoding="utf-8")
    with pytest.raises(RuntimeError, match="evidência financeira válida"):
        scraper.load_taxas_payload(runtime_root=tmp_path / "runtime", artifact_path=artifact)


def test_scraper_attempts_current_year_collection_without_static_year_gate(monkeypatch):
    calls = []

    def fake_run(**kwargs):
        calls.append(kwargs)
        return object()

    monkeypatch.setattr(scraper, "run_registered_source_pipeline", fake_run)
    observed = scraper.now_utc().replace(year=2027)
    runs = scraper.collect_payroll_source_runs(2027, observed)

    assert set(runs) == set(scraper.PAYROLL_SOURCE_IDS)
    assert len(calls) == len(scraper.PAYROLL_SOURCE_IDS)
    assert {item["source_id"] for item in calls} == set(scraper.PAYROLL_SOURCE_IDS)
    assert all(item["observed_at_utc"].year == 2027 for item in calls)


def test_scraper_rejects_reference_year_that_disagrees_with_observation_year(monkeypatch):
    called = False

    def fake_run(**kwargs):
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(scraper, "run_registered_source_pipeline", fake_run)
    with pytest.raises(RuntimeError, match="must match observation year"):
        scraper.collect_payroll_source_runs(2027, scraper.now_utc().replace(year=2026))
    assert called is False

def test_requirements_install_phase4_runtime_dependencies():
    text = Path("requirements.txt").read_text(encoding="utf-8")
    assert "-r requirements-contract.txt" in text
    assert "-r requirements-sources.txt" in text
