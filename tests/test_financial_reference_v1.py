from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path

import httpx
import pytest

from sanida_fiscal.financial_artifact_v1 import (
    CDI_MAX_OBSERVATION_AGE_DAYS,
    FinancialArtifactBoundaryError,
    build_financial_reference_artifact,
)
from sanida_fiscal.financial_reference_v1 import (
    SELIC_PARSER_VERSION,
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
        "observation_date": "2026-09-10",
        "annual_rate_pct": "14.00",
        "unit": "percent_per_year",
    }


def test_selic_parser_selects_latest_applicable_row_and_ignores_forward_rows():
    raw = json.dumps(
        [
            {"data": "12/09/2026", "valor": "14.00"},
            {"data": "13/09/2026", "valor": "14.00"},
            {"data": "14/09/2026", "valor": "14.00"},
            {"data": "15/09/2026", "valor": "14.00"},
            {"data": "16/09/2026", "valor": "14.00"},
        ]
    ).encode("utf-8")

    payload = parse_bcb_selic_meta_sgs432_snapshot(
        raw,
        as_of_date=date(2026, 9, 14),
    )

    assert payload["observation_date"] == "2026-09-14"
    assert payload["annual_rate_pct"] == "14.00"


def test_selic_parser_fails_closed_when_snapshot_has_only_future_rows():
    raw = b'[{"data":"15/09/2026","valor":"14.00"},{"data":"16/09/2026","valor":"14.00"}]'
    with pytest.raises(ParserIncompatibleError, match="no observation on or before 2026-09-14"):
        parse_bcb_selic_meta_sgs432_snapshot(raw, as_of_date=date(2026, 9, 14))


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


def test_sgs_parser_rejects_ambiguous_multiple_observations_without_as_of_date():
    raw = b'[{"data":"10/09/2026","valor":"14.00"},{"data":"11/09/2026","valor":"14.00"}]'
    with pytest.raises(ParserIncompatibleError, match="as_of_date is required"):
        parse_bcb_selic_meta_sgs432_snapshot(raw)


def _run(
    tmp_path: Path,
    source_id: str,
    body: bytes,
    *,
    observed_at_utc: datetime = NOW,
    request_assertion=None,
):
    def handler(request: httpx.Request):
        if request_assertion is not None:
            request_assertion(request)
        return httpx.Response(200, content=body, headers={"content-type": "application/json"})

    return run_registered_financial_source_pipeline(
        source_id=source_id,
        observed_at_utc=observed_at_utc,
        registry_path=Path("docs/financial-source-registry-v1.json"),
        snapshot_root=tmp_path / "snapshots",
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidates",
        transport=httpx.MockTransport(handler),
        use_http_validators=False,
    )


def test_selic_pipeline_bounds_query_to_current_brazil_calendar_date(tmp_path: Path):
    observed = datetime(2026, 9, 14, 13, 2, 28, tzinfo=timezone.utc)
    raw = json.dumps(
        [
            {"data": "13/09/2026", "valor": "14.00"},
            {"data": "14/09/2026", "valor": "14.00"},
        ]
    ).encode("utf-8")

    def assert_request(request: httpx.Request):
        assert request.url.params["dataFinal"] == "14/09/2026"
        assert request.url.params["dataInicial"] == "14/08/2026"
        assert request.url.params["formato"] == "json"
        assert "/ultimos/" not in str(request.url)

    run = _run(
        tmp_path,
        "BCB_SELIC_META_SGS_432",
        raw,
        observed_at_utc=observed,
        request_assertion=assert_request,
    )

    assert run.candidate is not None
    assert run.candidate.status == ParseStatus.PARSED
    assert run.candidate.parser_version == SELIC_PARSER_VERSION == "1.1.0"
    assert run.candidate.payload["observation_date"] == "2026-09-14"
    assert run.state.source_url.endswith("dataFinal=14%2F09%2F2026")


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
    assert artifact["meta"]["sources"]["selic"]["freshness_policy"] == "persistent_until_changed_no_max_age"
    assert artifact["meta"]["sources"]["cdi"]["series_code"] == 12
    assert artifact["meta"]["sources"]["cdi"]["source_value_pct"] == "0.051660"
    assert artifact["meta"]["sources"]["cdi"]["annualized_value_pct"] == "13.90"
    assert artifact["meta"]["sources"]["cdi"]["max_observation_age_calendar_days"] == CDI_MAX_OBSERVATION_AGE_DAYS


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


def test_stale_daily_cdi_cannot_be_relabelled_by_new_generated_timestamp(tmp_path: Path):
    stale_cdi = b'[{"data":"01/09/2026","valor":"0.051660"}]'
    selic = _run(tmp_path, "BCB_SELIC_META_SGS_432", SELIC_FIXTURE)
    cdi = _run(tmp_path, "BCB_CDI_DAILY_SGS_12", stale_cdi)

    with pytest.raises(FinancialArtifactBoundaryError, match="CDI observation is stale"):
        build_financial_reference_artifact(
            selic_run=selic,
            cdi_run=cdi,
            generated_at_utc="2026-09-13T22:00:00Z",
        )


def test_old_selic_latest_observation_is_allowed_because_rate_persists_until_changed(tmp_path: Path):
    old_selic = b'[{"data":"01/07/2026","valor":"14.00"}]'
    selic = _run(tmp_path, "BCB_SELIC_META_SGS_432", old_selic)
    cdi = _run(tmp_path, "BCB_CDI_DAILY_SGS_12", CDI_FIXTURE)

    artifact = build_financial_reference_artifact(
        selic_run=selic,
        cdi_run=cdi,
        generated_at_utc="2026-09-13T22:00:00Z",
    )
    assert artifact["taxas"]["selic"] == 14.0


def test_future_only_selic_snapshot_is_rejected_before_artifact(tmp_path: Path):
    future_selic = b'[{"data":"14/09/2026","valor":"14.00"}]'
    selic = _run(tmp_path, "BCB_SELIC_META_SGS_432", future_selic)

    assert selic.candidate is not None
    assert selic.candidate.status == ParseStatus.PARSER_INCOMPATIBLE
    assert "no observation on or before 2026-09-13" in selic.candidate.error_detail


def test_financial_source_policy_removes_ftp_and_static_fallback_from_automatic_path():
    policy = json.loads(Path("docs/phase4-financial-reference-policy-v1.json").read_text(encoding="utf-8"))
    assert policy["failure_policy"]["write_static_fallback"] is False
    assert policy["failure_policy"]["refresh_generated_at_without_new_observation"] is False
    assert policy["b3_role"]["legacy_ftp_is_automatic_fallback"] is False
    assert policy["b3_role"]["legacy_ftp_is_production_input_after_migration"] is False
    assert policy["freshness_policy"]["cdi_daily"]["max_observation_age_calendar_days"] == 7
    assert policy["freshness_policy"]["selic_meta"]["max_observation_age_calendar_days"] is None


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
