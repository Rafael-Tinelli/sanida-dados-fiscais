from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

from sanida_fiscal.rfb_irrf_v1 import (
    PARSER_ID,
    PARSER_VERSION,
    parse_rfb_irrf_2026_snapshot,
)
from sanida_fiscal.source_runtime_v1 import SourceStateStore, run_source_pipeline
from sanida_fiscal.sources_v1 import (
    CollectionStatus,
    HttpCollectorV1,
    ParseStatus,
    RetryPolicy,
    SnapshotStore,
    load_source_registry,
)


NOW = datetime(2026, 9, 13, 20, 0, tzinfo=timezone.utc)
FIXTURE = Path("tests/fixtures/sources/rfb_irrf_2026_fragment.html").read_bytes()


def make_runtime(tmp_path: Path, handler):
    snapshot_store = SnapshotStore(tmp_path / "snapshots")
    collector = HttpCollectorV1(
        snapshot_store=snapshot_store,
        retry_policy=RetryPolicy(max_attempts=2, timeout_seconds=1),
        transport=httpx.MockTransport(handler),
        headers={"User-Agent": "SanidaFiscaisBot/test"},
    )
    return snapshot_store, collector, SourceStateStore(tmp_path / "state")


def source():
    return load_source_registry(Path("docs/source-registry-v1.json"))["RFB_IRRF_TABLE_2026"]


def test_rfb_parser_normalizes_official_2026_values():
    payload = parse_rfb_irrf_2026_snapshot(FIXTURE)

    assert payload["reference_year"] == 2026
    assert payload["monthly_effective_from"] == "2026-01-01"
    assert payload["dependent_deduction_brl"] == "189.59"
    assert payload["simplified_discount_brl"] == "607.20"
    assert payload["monthly_table"] == [
        {"lower_bound_brl": None, "upper_bound_brl": "2428.80", "rate": "0", "deduction_brl": "0.00"},
        {"lower_bound_brl": "2428.81", "upper_bound_brl": "2826.65", "rate": "0.075", "deduction_brl": "182.16"},
        {"lower_bound_brl": "2826.66", "upper_bound_brl": "3751.05", "rate": "0.15", "deduction_brl": "394.16"},
        {"lower_bound_brl": "3751.06", "upper_bound_brl": "4664.68", "rate": "0.225", "deduction_brl": "675.49"},
        {"lower_bound_brl": "4664.68", "upper_bound_brl": None, "rate": "0.275", "deduction_brl": "908.73"},
    ]
    assert payload["monthly_reduction"] == {
        "full_relief_income_upper_brl": "5000.00",
        "max_reduction_brl": "312.89",
        "phaseout_income_lower_brl": "5000.01",
        "phaseout_income_upper_brl": "7350.00",
        "intercept_brl": "978.62",
        "slope": "0.133145",
        "input_semantic": "rendimentos_tributaveis_sujeitos_incidencia_mensal",
    }


def test_rfb_parser_discovers_reference_year_from_annual_page():
    future_fixture = FIXTURE.replace(b"2026", b"2027")
    payload = parse_rfb_irrf_2026_snapshot(future_fixture)

    assert payload["reference_year"] == 2027
    assert payload["monthly_effective_from"] == "2027-01-01"
    assert payload["observation_type"] == "rfb_irrf_2027"


def test_first_real_pipeline_persists_snapshot_candidate_and_state(tmp_path: Path):
    def handler(request: httpx.Request):
        return httpx.Response(
            200,
            content=FIXTURE,
            headers={
                "content-type": "text/html; charset=utf-8",
                "etag": '"rfb-2026-v1"',
                "last-modified": "Mon, 27 Apr 2026 20:14:00 GMT",
            },
        )

    snapshot_store, collector, state_store = make_runtime(tmp_path, handler)
    run = run_source_pipeline(
        source=source(),
        collector=collector,
        snapshot_store=snapshot_store,
        state_store=state_store,
        observed_at_utc=NOW,
        parser_id=PARSER_ID,
        parser_version=PARSER_VERSION,
        parser=parse_rfb_irrf_2026_snapshot,
    )

    assert run.collection.status == CollectionStatus.COLLECTED
    assert run.candidate is not None
    assert run.candidate.status == ParseStatus.PARSED
    assert run.state.last_candidate_sha256 is not None
    assert run.state.last_parsed_snapshot_sha256 == run.collection.snapshot.sha256
    assert run.state.etag == '"rfb-2026-v1"'
    assert state_store.load(source().source_id) == run.state


def test_second_run_uses_http_validator_and_preserves_last_good_state(tmp_path: Path):
    calls = 0

    def handler(request: httpx.Request):
        nonlocal calls
        calls += 1
        if calls == 1:
            assert "if-none-match" not in request.headers
            return httpx.Response(
                200,
                content=FIXTURE,
                headers={"content-type": "text/html", "etag": '"rfb-2026-v1"'},
            )
        assert request.headers["if-none-match"] == '"rfb-2026-v1"'
        return httpx.Response(304, headers={"etag": '"rfb-2026-v1"'})

    snapshot_store, collector, state_store = make_runtime(tmp_path, handler)
    first = run_source_pipeline(
        source=source(), collector=collector, snapshot_store=snapshot_store, state_store=state_store,
        observed_at_utc=NOW, parser_id=PARSER_ID, parser_version=PARSER_VERSION,
        parser=parse_rfb_irrf_2026_snapshot,
    )
    second = run_source_pipeline(
        source=source(), collector=collector, snapshot_store=snapshot_store, state_store=state_store,
        observed_at_utc=NOW + timedelta(hours=1), parser_id=PARSER_ID, parser_version=PARSER_VERSION,
        parser=parse_rfb_irrf_2026_snapshot,
    )

    assert second.collection.status == CollectionStatus.NOT_MODIFIED
    assert second.candidate is None
    assert second.raw_snapshot_unchanged is True
    assert second.candidate_fingerprint_unchanged is True
    assert second.state.last_candidate_sha256 == first.state.last_candidate_sha256
    assert second.state.last_parsed_snapshot_sha256 == first.state.last_parsed_snapshot_sha256


def test_same_200_body_is_operationally_unchanged_without_semantic_classification(tmp_path: Path):
    def handler(request: httpx.Request):
        return httpx.Response(200, content=FIXTURE, headers={"content-type": "text/html"})

    snapshot_store, collector, state_store = make_runtime(tmp_path, handler)
    first = run_source_pipeline(
        source=source(), collector=collector, snapshot_store=snapshot_store, state_store=state_store,
        observed_at_utc=NOW, parser_id=PARSER_ID, parser_version=PARSER_VERSION,
        parser=parse_rfb_irrf_2026_snapshot,
    )
    second = run_source_pipeline(
        source=source(), collector=collector, snapshot_store=snapshot_store, state_store=state_store,
        observed_at_utc=NOW + timedelta(hours=1), parser_id=PARSER_ID, parser_version=PARSER_VERSION,
        parser=parse_rfb_irrf_2026_snapshot,
    )

    assert second.collection.status == CollectionStatus.COLLECTED
    assert second.raw_snapshot_unchanged is True
    assert second.candidate_fingerprint_unchanged is True
    assert second.state.last_candidate_sha256 == first.state.last_candidate_sha256


def test_source_failure_keeps_previous_candidate_but_records_current_failure(tmp_path: Path):
    calls = 0

    def handler(request: httpx.Request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, content=FIXTURE, headers={"content-type": "text/html"})
        return httpx.Response(503)

    snapshot_store, collector, state_store = make_runtime(tmp_path, handler)
    first = run_source_pipeline(
        source=source(), collector=collector, snapshot_store=snapshot_store, state_store=state_store,
        observed_at_utc=NOW, parser_id=PARSER_ID, parser_version=PARSER_VERSION,
        parser=parse_rfb_irrf_2026_snapshot,
    )
    failed = run_source_pipeline(
        source=source(), collector=collector, snapshot_store=snapshot_store, state_store=state_store,
        observed_at_utc=NOW + timedelta(hours=1), parser_id=PARSER_ID, parser_version=PARSER_VERSION,
        parser=parse_rfb_irrf_2026_snapshot,
    )

    assert failed.collection.status == CollectionStatus.SOURCE_UNAVAILABLE
    assert failed.state.consecutive_source_failures == 1
    assert failed.state.last_candidate_sha256 == first.state.last_candidate_sha256
    assert failed.state.last_parsed_snapshot_sha256 == first.state.last_parsed_snapshot_sha256


def test_parser_change_forces_full_refetch_instead_of_accepting_304(tmp_path: Path):
    requests = []

    def handler(request: httpx.Request):
        requests.append(request)
        return httpx.Response(
            200,
            content=FIXTURE,
            headers={"content-type": "text/html", "etag": '"rfb-2026-v1"'},
        )

    snapshot_store, collector, state_store = make_runtime(tmp_path, handler)
    run_source_pipeline(
        source=source(), collector=collector, snapshot_store=snapshot_store, state_store=state_store,
        observed_at_utc=NOW, parser_id=PARSER_ID, parser_version=PARSER_VERSION,
        parser=parse_rfb_irrf_2026_snapshot,
    )
    run_source_pipeline(
        source=source(), collector=collector, snapshot_store=snapshot_store, state_store=state_store,
        observed_at_utc=NOW + timedelta(hours=1), parser_id=PARSER_ID, parser_version="1.1.0",
        parser=parse_rfb_irrf_2026_snapshot,
    )

    assert "if-none-match" not in requests[1].headers
