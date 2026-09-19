from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from sanida_fiscal.source_ids_v1 import INSS_SOURCE_ID, RFB_SOURCE_ID
from sanida_fiscal.source_catalog_v1 import (
    parser_binding,
    parser_for_reference_year,
    resolve_source_for_reference_year,
    run_registered_source_pipeline,
)
from sanida_fiscal.source_runtime_v1 import SourcePipelineState, SourceStateStore
from sanida_fiscal.sources_v1 import (
    CollectionStatus,
    ParseStatus,
    ParserIncompatibleError,
    load_source_registry,
)


REGISTRY = Path("docs/source-registry-v1.json")
RFB_FIXTURE = Path("tests/fixtures/sources/rfb_irrf_2026_fragment.html").read_bytes()
INSS_FIXTURE = Path("tests/fixtures/sources/inss_employee_2026_fragment.html").read_bytes()


def test_rfb_annual_url_is_resolved_from_reference_year():
    source = load_source_registry(REGISTRY)[RFB_SOURCE_ID]
    resolved = resolve_source_for_reference_year(
        source,
        source_id=source.source_id,
        reference_year=2027,
    )

    assert source.url.endswith("/tabelas/{year}")
    assert resolved.url.endswith("/tabelas/2027")
    assert resolved.source_id == source.source_id


def test_inss_operational_url_remains_stable_across_years():
    source = load_source_registry(REGISTRY)[INSS_SOURCE_ID]
    resolved = resolve_source_for_reference_year(
        source,
        source_id=source.source_id,
        reference_year=2027,
    )

    assert resolved.url == source.url
    assert resolved.source_id == source.source_id


@pytest.mark.parametrize(
    ("source_id", "fixture"),
    [
        (RFB_SOURCE_ID, RFB_FIXTURE),
        (INSS_SOURCE_ID, INSS_FIXTURE),
    ],
)
def test_current_year_parser_fails_closed_on_previous_year_snapshot(source_id, fixture):
    parser = parser_for_reference_year(parser_binding(source_id), 2027)

    with pytest.raises(ParserIncompatibleError, match="reference-year mismatch"):
        parser(fixture)


@pytest.mark.parametrize(
    ("source_id", "fixture"),
    [
        (RFB_SOURCE_ID, RFB_FIXTURE.replace(b"2026", b"2027")),
        (INSS_SOURCE_ID, INSS_FIXTURE.replace(b"2026", b"2027")),
    ],
)
def test_current_year_parser_accepts_matching_rollover_snapshot(source_id, fixture):
    parser = parser_for_reference_year(parser_binding(source_id), 2027)
    payload = parser(fixture)

    assert payload["reference_year"] == 2027


def test_rollover_does_not_reuse_http_validators_from_previous_annual_url(tmp_path: Path):
    calls = []
    fixture_2027 = RFB_FIXTURE.replace(b"2026", b"2027")

    def handler(request: httpx.Request):
        calls.append(request)
        if str(request.url).endswith("/2026"):
            return httpx.Response(
                200,
                content=RFB_FIXTURE,
                headers={"content-type": "text/html", "etag": '"rfb-2026"'},
            )
        if str(request.url).endswith("/2027"):
            assert "if-none-match" not in request.headers
            return httpx.Response(
                200,
                content=fixture_2027,
                headers={"content-type": "text/html", "etag": '"rfb-2027"'},
            )
        raise AssertionError(f"unexpected URL: {request.url}")

    transport = httpx.MockTransport(handler)
    run_registered_source_pipeline(
        source_id=RFB_SOURCE_ID,
        observed_at_utc=datetime(2026, 12, 31, 12, 0, tzinfo=timezone.utc),
        snapshot_root=tmp_path / "snapshots",
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidates",
        transport=transport,
    )
    second = run_registered_source_pipeline(
        source_id=RFB_SOURCE_ID,
        observed_at_utc=datetime(2027, 1, 1, 12, 0, tzinfo=timezone.utc),
        snapshot_root=tmp_path / "snapshots",
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidates",
        transport=transport,
    )

    assert len(calls) == 2
    assert second.collection.source_url.endswith("/2027")
    assert second.candidate is not None
    assert second.candidate.payload["reference_year"] == 2027


def test_legacy_source_ids_resolve_to_canonical_bindings():
    assert parser_binding("RFB_IRRF_TABLE_2026").source_id == RFB_SOURCE_ID
    assert parser_binding("INSS_TABLE_2026").source_id == INSS_SOURCE_ID


def test_legacy_state_is_materialized_under_canonical_id_without_deletion(tmp_path: Path):
    state_store = SourceStateStore(tmp_path / "state")
    binding = parser_binding(RFB_SOURCE_ID)
    legacy = SourcePipelineState(
        source_id="RFB_IRRF_TABLE_2026",
        source_url="https://www.gov.br/receitafederal/pt-br/assuntos/meu-imposto-de-renda/tabelas/2026",
        last_observed_at_utc=datetime(2026, 12, 31, 10, 0, tzinfo=timezone.utc),
        last_collection_status=CollectionStatus.COLLECTED,
        last_http_status=200,
        etag='"legacy-2026"',
        last_parse_status=ParseStatus.PARSED,
        parser_id=binding.parser_id,
        parser_version=binding.parser_version,
        last_successful_parser_id=binding.parser_id,
        last_successful_parser_version=binding.parser_version,
    )
    state_store.persist(legacy)

    def handler(request: httpx.Request):
        return httpx.Response(503)

    run = run_registered_source_pipeline(
        source_id=RFB_SOURCE_ID,
        observed_at_utc=datetime(2026, 12, 31, 12, 0, tzinfo=timezone.utc),
        snapshot_root=tmp_path / "snapshots",
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidates",
        transport=httpx.MockTransport(handler),
        max_attempts=1,
    )

    assert run.state.source_id == RFB_SOURCE_ID
    assert state_store.load(RFB_SOURCE_ID) is not None
    assert state_store.load("RFB_IRRF_TABLE_2026") == legacy


def test_stable_inss_url_still_forces_full_fetch_at_year_boundary(tmp_path: Path):
    calls = []
    fixture_2027 = INSS_FIXTURE.replace(b"2026", b"2027")

    def handler(request: httpx.Request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(
                200,
                content=INSS_FIXTURE,
                headers={"content-type": "text/html", "etag": '"inss-2026"'},
            )
        assert "if-none-match" not in request.headers
        return httpx.Response(
            200,
            content=fixture_2027,
            headers={"content-type": "text/html", "etag": '"inss-2027"'},
        )

    transport = httpx.MockTransport(handler)
    run_registered_source_pipeline(
        source_id=INSS_SOURCE_ID,
        observed_at_utc=datetime(2026, 12, 31, 12, 0, tzinfo=timezone.utc),
        snapshot_root=tmp_path / "snapshots",
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidates",
        transport=transport,
    )
    second = run_registered_source_pipeline(
        source_id=INSS_SOURCE_ID,
        observed_at_utc=datetime(2027, 1, 1, 12, 0, tzinfo=timezone.utc),
        snapshot_root=tmp_path / "snapshots",
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidates",
        transport=transport,
    )

    assert len(calls) == 2
    assert second.candidate is not None
    assert second.candidate.payload["reference_year"] == 2027
