from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path

import httpx
import pytest

from sanida_fiscal.financial_series_v1 import (
    CDI_SOURCE_ID,
    FINANCIAL_SERIES_MONTHS,
    FINANCIAL_SERIES_SCHEMA_VERSION,
    SELIC_SOURCE_ID,
    FinancialSeriesBoundaryError,
    build_financial_series_artifact,
    build_financial_series_artifact_chunked,
    financial_history_chunks,
    financial_series_window,
    month_keys,
    parse_bcb_cdi_history_snapshot,
    parse_bcb_selic_history_snapshot,
    run_financial_history_source_pipeline,
    run_financial_history_source_pipeline_chunked,
    shift_months,
    validate_financial_series_artifact,
)
from sanida_fiscal.sources_v1 import ParseStatus, ParserIncompatibleError


AS_OF = date(2026, 9, 19)
OBSERVED = datetime(2026, 9, 19, 20, 0, tzinfo=timezone.utc)


def _month_dates() -> list[date]:
    start, _end = financial_series_window(AS_OF)
    out = []
    current = start
    for _ in range(FINANCIAL_SERIES_MONTHS):
        if current.year == AS_OF.year and current.month == AS_OF.month:
            out.append(date(current.year, current.month, 17))
        else:
            out.append(date(current.year, current.month, 15))
        current = shift_months(current, 1)
    return out


def _raw_selic() -> bytes:
    rows = [
        {"data": item.strftime("%d/%m/%Y"), "valor": f"{10 + (idx % 6) * 0.25:.2f}"}
        for idx, item in enumerate(_month_dates())
    ]
    return json.dumps(rows).encode("utf-8")


def _raw_cdi() -> bytes:
    rows = [
        {"data": item.strftime("%d/%m/%Y"), "valor": f"{0.040000 + (idx % 5) * 0.000500:.6f}"}
        for idx, item in enumerate(_month_dates())
    ]
    return json.dumps(rows).encode("utf-8")


def _run(tmp_path: Path, source_id: str, body: bytes):
    start, end = financial_series_window(AS_OF)

    def handler(request: httpx.Request):
        assert request.url.params["dataInicial"] == start.strftime("%d/%m/%Y")
        assert request.url.params["dataFinal"] == end.strftime("%d/%m/%Y")
        assert request.url.params["formato"] == "json"
        assert "/ultimos/" not in str(request.url)
        return httpx.Response(200, content=body, headers={"content-type": "application/json"})

    return run_financial_history_source_pipeline(
        source_id=source_id,
        observed_at_utc=OBSERVED,
        start_date=start,
        end_date=end,
        registry_path=Path("docs/financial-source-registry-v1.json"),
        snapshot_root=tmp_path / "snapshots",
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidates",
        transport=httpx.MockTransport(handler),
    )


def test_financial_series_window_is_exactly_120_calendar_months_and_below_api_limit():
    start, end = financial_series_window(AS_OF)
    assert start == date(2016, 10, 1)
    assert end == AS_OF
    assert len(month_keys(start, end)) == 120
    assert (end - start).days < 3653


def test_history_parsers_preserve_source_semantics_and_window():
    start, end = financial_series_window(AS_OF)
    selic = parse_bcb_selic_history_snapshot(_raw_selic(), start_date=start, end_date=end)
    cdi = parse_bcb_cdi_history_snapshot(_raw_cdi(), start_date=start, end_date=end)

    assert selic["source_id"] == SELIC_SOURCE_ID
    assert selic["series_code"] == 432
    assert selic["unit"] == "percent_per_year"
    assert len(selic["observations"]) == 120

    assert cdi["source_id"] == CDI_SOURCE_ID
    assert cdi["series_code"] == 12
    assert cdi["unit"] == "percent_per_business_day"
    assert cdi["annualization_basis_business_days"] == 252
    assert len(cdi["observations"]) == 120


def test_history_parser_rejects_observation_outside_requested_window():
    start, end = financial_series_window(AS_OF)
    bad = json.dumps([{"data": "30/09/2016", "valor": "14.25"}]).encode("utf-8")
    with pytest.raises(ParserIncompatibleError, match="outside requested history window"):
        parse_bcb_selic_history_snapshot(bad, start_date=start, end_date=end)


def test_history_pipeline_uses_bounded_sgs_endpoint_and_persists_evidence(tmp_path: Path):
    selic = _run(tmp_path, SELIC_SOURCE_ID, _raw_selic())
    cdi = _run(tmp_path, CDI_SOURCE_ID, _raw_cdi())

    for run in (selic, cdi):
        assert run.candidate is not None
        assert run.candidate.status == ParseStatus.PARSED
        assert run.state.last_candidate_sha256
        assert run.state.last_candidate_path
        assert run.state.last_parsed_snapshot_path
        assert (tmp_path / "candidates" / run.state.last_candidate_path).is_file()
        assert (tmp_path / "snapshots" / run.state.last_parsed_snapshot_path).is_file()


def test_build_financial_series_artifact_is_compact_monthly_and_valid(tmp_path: Path):
    selic = _run(tmp_path, SELIC_SOURCE_ID, _raw_selic())
    cdi = _run(tmp_path, CDI_SOURCE_ID, _raw_cdi())

    artifact = build_financial_series_artifact(
        selic_run=selic,
        cdi_run=cdi,
        generated_at_utc="2026-09-19T20:00:00Z",
        as_of_date=AS_OF,
    )

    assert artifact["schema_version"] == FINANCIAL_SERIES_SCHEMA_VERSION
    assert artifact["meta"]["window"] == {
        "months": 120,
        "start_date": "2016-10-01",
        "end_date": "2026-09-19",
    }
    assert len(artifact["points"]) == 120
    assert artifact["points"][0]["month"] == "2016-10"
    assert artifact["points"][-1]["month"] == "2026-09"
    assert artifact["points"][-1]["month_complete"] is False
    assert artifact["points"][-2]["month_complete"] is True
    assert artifact["points"][-1]["cdi_observation_date"] == "2026-09-17"
    assert artifact["meta"]["latest_cdi_observation_age_calendar_days"] == 2

    ok, errors = validate_financial_series_artifact(artifact, as_of_date=AS_OF)
    assert ok, errors


def test_cdi_monthly_return_compounds_all_daily_observations(tmp_path: Path):
    dates = _month_dates()
    rows = []
    for idx, item in enumerate(dates):
        rows.append({"data": item.strftime("%d/%m/%Y"), "valor": "0.050000"})
        if idx == 0:
            rows.append({"data": "16/10/2016", "valor": "0.050000"})

    selic = _run(tmp_path, SELIC_SOURCE_ID, _raw_selic())
    cdi = _run(tmp_path, CDI_SOURCE_ID, json.dumps(rows).encode("utf-8"))
    artifact = build_financial_series_artifact(
        selic_run=selic,
        cdi_run=cdi,
        generated_at_utc="2026-09-19T20:00:00Z",
        as_of_date=AS_OF,
    )

    expected = ((Decimal("1.0005") ** 2) - Decimal("1")) * Decimal("100")
    assert artifact["points"][0]["cdi_month_return_pct"] == pytest.approx(float(expected), abs=1e-6)
    assert artifact["points"][0]["cdi_observation_count"] == 2


def test_missing_calendar_month_fails_closed(tmp_path: Path):
    rows = json.loads(_raw_cdi())
    rows = rows[1:]
    selic = _run(tmp_path, SELIC_SOURCE_ID, _raw_selic())
    cdi = _run(tmp_path, CDI_SOURCE_ID, json.dumps(rows).encode("utf-8"))

    with pytest.raises(FinancialSeriesBoundaryError, match="CDI history missing calendar months"):
        build_financial_series_artifact(
            selic_run=selic,
            cdi_run=cdi,
            generated_at_utc="2026-09-19T20:00:00Z",
            as_of_date=AS_OF,
        )


def test_stale_latest_cdi_history_fails_closed(tmp_path: Path):
    rows = json.loads(_raw_cdi())
    rows[-1]["data"] = "10/09/2026"
    selic = _run(tmp_path, SELIC_SOURCE_ID, _raw_selic())
    cdi = _run(tmp_path, CDI_SOURCE_ID, json.dumps(rows).encode("utf-8"))

    with pytest.raises(FinancialSeriesBoundaryError, match="latest CDI history observation is stale"):
        build_financial_series_artifact(
            selic_run=selic,
            cdi_run=cdi,
            generated_at_utc="2026-09-19T20:00:00Z",
            as_of_date=AS_OF,
        )


def test_history_window_is_split_into_ten_bounded_12_month_chunks():
    start, end = financial_series_window(AS_OF)
    chunks = financial_history_chunks(start, end, months_per_chunk=12)
    assert len(chunks) == 10
    assert chunks[0] == (date(2016, 10, 1), date(2017, 9, 30))
    assert chunks[-1] == (date(2025, 10, 1), AS_OF)
    for (chunk_start, chunk_end), next_chunk in zip(chunks, chunks[1:]):
        assert (chunk_end - chunk_start).days <= 366
        assert next_chunk[0].toordinal() == chunk_end.toordinal() + 1


def _chunked_run(tmp_path: Path, source_id: str, all_rows: list[dict]):
    calls: list[tuple[date, date]] = []

    def handler(request: httpx.Request):
        start = datetime.strptime(request.url.params["dataInicial"], "%d/%m/%Y").date()
        end = datetime.strptime(request.url.params["dataFinal"], "%d/%m/%Y").date()
        calls.append((start, end))
        assert (end - start).days <= 366
        selected = [
            row for row in all_rows
            if start <= datetime.strptime(row["data"], "%d/%m/%Y").date() <= end
        ]
        return httpx.Response(
            200,
            content=json.dumps(selected).encode("utf-8"),
            headers={"content-type": "application/json"},
        )

    start, end = financial_series_window(AS_OF)
    bundle = run_financial_history_source_pipeline_chunked(
        source_id=source_id,
        observed_at_utc=OBSERVED,
        start_date=start,
        end_date=end,
        registry_path=Path("docs/financial-source-registry-v1.json"),
        snapshot_root=tmp_path / "snapshots",
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidates",
        transport=httpx.MockTransport(handler),
        months_per_chunk=12,
    )
    return bundle, calls


def test_chunked_history_builds_same_120_month_contract_without_long_request(tmp_path: Path):
    selic_rows = json.loads(_raw_selic())
    cdi_rows = json.loads(_raw_cdi())
    selic, selic_calls = _chunked_run(tmp_path, SELIC_SOURCE_ID, selic_rows)
    cdi, cdi_calls = _chunked_run(tmp_path, CDI_SOURCE_ID, cdi_rows)
    assert len(selic_calls) == len(cdi_calls) == 10
    assert len(selic.meta["segments"]) == len(cdi.meta["segments"]) == 10

    artifact = build_financial_series_artifact_chunked(
        selic_bundle=selic,
        cdi_bundle=cdi,
        generated_at_utc="2026-09-19T20:00:00Z",
        as_of_date=AS_OF,
    )
    ok, errors = validate_financial_series_artifact(artifact, as_of_date=AS_OF)
    assert ok, errors
    assert len(artifact["points"]) == 120
    assert artifact["meta"]["sources"]["selic"]["collection_mode"] == "chunked_12_months_v1"
    assert artifact["meta"]["sources"]["cdi"]["collection_mode"] == "chunked_12_months_v1"
    assert artifact["meta"]["methodology"]["source_collection"] == "bounded_chunks_with_immutable_segment_evidence"


def test_chunked_history_segment_states_are_isolated(tmp_path: Path):
    bundle, calls = _chunked_run(tmp_path, SELIC_SOURCE_ID, json.loads(_raw_selic()))
    assert len(calls) == 10
    for segment in bundle.meta["segments"]:
        key = f'{segment["start_date"]}_{segment["end_date"]}'
        state = tmp_path / "state" / "segments" / key / f"{SELIC_SOURCE_ID}.json"
        assert state.is_file()
        payload = json.loads(state.read_text())
        assert payload["source_url"] == segment["url"]
        assert payload["last_candidate_sha256"] == segment["candidate_sha256"]


def test_second_daily_run_reuses_closed_segments_and_fetches_only_current_chunk(tmp_path: Path):
    rows = json.loads(_raw_selic())
    first, first_calls = _chunked_run(tmp_path, SELIC_SOURCE_ID, rows)
    second, second_calls = _chunked_run(tmp_path, SELIC_SOURCE_ID, rows)
    assert len(first_calls) == 10
    assert len(second_calls) == 1
    assert first.meta["reused_segment_count"] == 0
    assert second.meta["reused_segment_count"] == 9
    assert second.meta["segment_count"] == 10
    assert sum(bool(x["reused_immutable_segment"]) for x in second.meta["segments"]) == 9
    assert second.payload == first.payload
