from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import httpx
import pytest

from sanida_fiscal.financial_artifact_v1 import (
    FinancialArtifactBoundaryError,
    build_financial_reference_artifact,
)
from sanida_fiscal.financial_reference_v1 import (
    annualize_cdi_daily_rate_pct,
    parse_bcb_cdi_daily_sgs12_snapshot,
    parse_bcb_selic_meta_sgs432_snapshot,
)
from sanida_fiscal.financial_source_catalog_v1 import run_registered_financial_source_pipeline
from sanida_fiscal.sources_v1 import CollectionStatus, ParseStatus, ParserIncompatibleError


NOW = datetime(2026, 9, 13, 22, 0, tzinfo=timezone.utc)
SELIC_FIXTURE = Path("tests/fixtures/sources/bcb_selic_sgs432.json").read_bytes()
CDI_FIXTURE = Path("tests/fixtures/sources/bcb_cdi_sgs12.json").read_bytes()


def test_selic_parser_preserves_official_decimal_and_date():
    payload = parse_bcb_selic_meta_sgs432_snapshot(SELIC_FIXTURE)
    assert payload == {
        "observation_type": "bcb_selic_meta_sgs432",
        "series_code": 432,
        "observation_date": "2026-09-16",
        "annual_rate_pct": "14.00",
        "unit": "percent_per_year",
    }


def test_cdi_parser_preserves_daily_rate_without_pretending_it_is_annual():
    payload = parse_bcb_cdi_daily_sgs12_snapshot(CDI_FIXTURE)
    assert payload == {
        "observation_type": "bcb_cdi_daily_sgs12",
        "series_code": 12,
        "observation_date": "2026-09-10",
        "daily_rate_pct": "0.051660",
        "unit": "percent_per_business_day",
        "annualization_basis_business_days": 252,
    }


def test_cdi_daily_rate_is_annualized_at_252_business_days_for_legacy_artifact():
    assert str(annualize_cdi_daily_rate_pct("0.051660")) == "13.90"


def test_sgs_parser_rejects_ambiguous_multiple_observations():
    raw = b'[{"data":"10/09/2026","valor":"14.00"},{"data":"11/09/2026","valor":"14.00"}]'
    with pytest.raises(ParserIncompatibleError, match="exactly one observation"):
        parse_bcb_selic_meta_sgs432_snapshot(raw)


def _run(tmp_path: Path, source_id: str, body: bytes):
    def handler(request: httpx.Request):
        return httpx.Response(200, content=body, headers={"content-type": "application/json"})

    return run_registered_financial_source_pipeline(
        source_id=source_id,
        observed_at_utc=NOW,
        registry_path=Path("docs/financial-source-registry-v1.json"),
        snapshot_root=tmp_path / "snapshots",
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidates",
        transport=httpx.MockTransport(handler),
        use_http_validators=False,
    )


def test_financial_pipelines_persist_snapshot_candidate_and_state(tmp_path: Path):
    selic = _run(tmp_path, "BCB_SELIC_META_SGS_432", SELIC_FIXTURE)
    cdi = _run(tmp_path, "BCB_CDI_DAILY_SGS_12", CDI_FIXTURE)

    for run in (selic, cdi):
        assert run.collection.status == CollectionStatus.COLLECTED
        assert run.candidate is not None
        assert run.candidate.status == ParseStatus.PARSED
        assert run.state.last_candidate_sha256 is not None
        assert run.state.last_candidate_path is not None
        assert run.state.last_parsed_snapshot_path is not None
        assert (tmp_path / "candidates" / run.state.last_candidate_path).is_file()
        assert (tmp_path / "snapshots" / run.state.last_parsed_snapshot_path).is_file()


def test_financial_artifact_reproduces_legacy_annual_rates_from_canonical_inputs(tmp_path: Path):
    selic = _run(tmp_path, "BCB_SELIC_META_SGS_432", SELIC_FIXTURE)
    cdi = _run(tmp_path, "BCB_CDI_DAILY_SGS_12", CDI_FIXTURE)

    artifact = build_financial_reference_artifact(
        selic_run=selic,
        cdi_run=cdi,
        generated_at_utc="2026-09-13T22:00:00Z",
    )

    assert artifact["schema_version"] == "1.4.0"
    assert artifact["taxas"] == {
        "selic": 14.0,
        "cdi": 13.9,
        "cdi_basis": "bcb_sgs_12_daily_compounded_252",
    }
    assert artifact["meta"]["sources"]["selic"]["series_code"] == 432
    assert artifact["meta"]["sources"]["cdi"]["series_code"] == 12
    assert artifact["meta"]["sources"]["cdi"]["source_value_pct"] == "0.051660"
    assert artifact["meta"]["sources"]["cdi"]["annualized_value_pct"] == "13.90"


def test_financial_artifact_requires_current_parsed_candidate(tmp_path: Path):
    selic = _run(tmp_path, "BCB_SELIC_META_SGS_432", SELIC_FIXTURE)
    cdi = _run(tmp_path, "BCB_CDI_DAILY_SGS_12", CDI_FIXTURE)
    failed_cdi = cdi.model_copy(update={"candidate": None})

    with pytest.raises(FinancialArtifactBoundaryError, match="no current PARSED candidate"):
        build_financial_reference_artifact(
            selic_run=selic,
            cdi_run=failed_cdi,
            generated_at_utc="2026-09-13T22:00:00Z",
        )


def test_financial_source_policy_removes_ftp_and_static_fallback_from_automatic_path():
    policy = json.loads(Path("docs/phase4-financial-reference-policy-v1.json").read_text(encoding="utf-8"))
    assert policy["failure_policy"]["write_static_fallback"] is False
    assert policy["failure_policy"]["refresh_generated_at_without_new_observation"] is False
    assert policy["b3_role"]["legacy_ftp_is_automatic_fallback"] is False
    assert policy["b3_role"]["legacy_ftp_is_production_input_after_migration"] is False


def test_update_taxas_contains_no_ftp_or_static_reference_fallback():
    text = Path("update_taxas.py").read_text(encoding="utf-8")
    for token in (
        "ftplib",
        "ftp.cetip.com.br",
        "FALLBACK_SELIC",
        "FALLBACK_CDI",
        "minimal_fallback_written",
        "static_reference_values",
        "fetch_b3_cdi_ftp",
    ):
        assert token not in text
    assert "run_registered_financial_source_pipeline" in text
    assert "build_financial_reference_artifact" in text
