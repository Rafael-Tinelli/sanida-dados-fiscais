from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import httpx

from sanida_fiscal.inss_employee_v1 import (
    PARSER_ID,
    PARSER_VERSION,
    parse_inss_employee_snapshot,
)
from sanida_fiscal.source_ids_v1 import INSS_SOURCE_ID, canonical_source_id
from sanida_fiscal.source_runtime_v1 import SourceStateStore, run_source_pipeline
from sanida_fiscal.sources_v1 import (
    CollectionStatus,
    HttpCollectorV1,
    ParseStatus,
    RetryPolicy,
    SnapshotStore,
    load_source_registry,
)


NOW = datetime(2026, 9, 13, 21, 0, tzinfo=timezone.utc)
FIXTURE = Path("tests/fixtures/sources/inss_employee_2026_fragment.html").read_bytes()
POLICY = Path("docs/phase4-inss-source-resolution-v1.json")


def source():
    return load_source_registry(Path("docs/source-registry-v1.json"))[INSS_SOURCE_ID]


def make_runtime(tmp_path: Path, handler):
    snapshot_store = SnapshotStore(tmp_path / "snapshots")
    collector = HttpCollectorV1(
        snapshot_store=snapshot_store,
        retry_policy=RetryPolicy(max_attempts=2, timeout_seconds=1),
        transport=httpx.MockTransport(handler),
        headers={"User-Agent": "SanidaFiscaisBot/test"},
    )
    return snapshot_store, collector, SourceStateStore(tmp_path / "state")


def test_inss_parser_normalizes_official_2026_employee_table():
    payload = parse_inss_employee_snapshot(FIXTURE)

    assert payload["reference_year"] == 2026
    assert payload["effective_from"] == "2026-01-01"
    assert payload["calculation_method"] == "marginal_by_bracket"
    assert payload["employment_categories"] == [
        "empregado",
        "empregado_domestico",
        "trabalhador_avulso",
    ]
    assert payload["monthly_table"] == [
        {"lower_bound_brl": None, "upper_bound_brl": "1621.00", "rate": "0.075"},
        {"lower_bound_brl": "1621.01", "upper_bound_brl": "2902.84", "rate": "0.09"},
        {"lower_bound_brl": "2902.85", "upper_bound_brl": "4354.27", "rate": "0.12"},
        {"lower_bound_brl": "4354.28", "upper_bound_brl": "8475.55", "rate": "0.14"},
    ]
    assert payload["contribution_ceiling_brl"] == "8475.55"


def test_inss_parser_captures_13th_separate_assessment_and_normative_reference():
    payload = parse_inss_employee_snapshot(FIXTURE)

    assert payload["thirteenth_assessment"] == "separate_from_monthly_remuneration"
    assert payload["normative_reference"] == {
        "act_id": "PORTARIA_INTERMINISTERIAL_MPS_MF_13_2026",
        "act_date": "2026-01-09",
    }


def test_legacy_news_fragment_is_not_accepted_as_canonical_table():
    legacy_news = b"""
    <html><body>
      <h1>Com reajuste, teto do INSS chega a R$ 8.475,55 em 2026</h1>
      <p>As aliquotas para empregados sao 7,5%, 9%, 12% e 14%.</p>
    </body></html>
    """

    from sanida_fiscal.sources_v1 import ParserIncompatibleError

    try:
        parse_inss_employee_snapshot(legacy_news)
    except ParserIncompatibleError as exc:
        assert "canonical structural markers" in str(exc)
    else:
        raise AssertionError("legacy yearly news must not be accepted by the canonical INSS parser")


def test_inss_parser_discovers_reference_year_from_source_markers():
    future_fixture = FIXTURE.replace(b"2026", b"2027")
    payload = parse_inss_employee_snapshot(future_fixture)

    assert payload["reference_year"] == 2027
    assert payload["effective_from"] == "2027-01-01"
    assert payload["observation_type"] == "inss_employee_progressive_table_2027"
    assert payload["normative_reference"]["act_id"].endswith("_2027")
    assert payload["normative_reference"]["act_date"].endswith("-01-09")


def test_source_resolution_binds_registry_url_and_forbids_legacy_fallback():
    policy = json.loads(POLICY.read_text(encoding="utf-8"))
    spec = source()

    assert policy["status"] == "resolved"
    assert canonical_source_id(policy["registry_source_id"]) == spec.source_id
    assert policy["canonical_source"]["url"] == spec.url
    assert policy["canonical_source"]["parser_id"] == PARSER_ID
    assert policy["canonical_source"]["parser_version"] == PARSER_VERSION
    assert policy["resolution"]["canonical_source_is_single_operational_input"] is True
    assert policy["resolution"]["allow_pinned_news_fallback"] is False
    assert policy["resolution"]["allow_search_discovery_fallback"] is False
    assert policy["resolution"]["canonical_failure_state"] == "SOURCE_UNAVAILABLE"


def test_inss_pipeline_fetches_registry_url_directly_and_persists_state(tmp_path: Path):
    requested_urls = []

    def handler(request: httpx.Request):
        requested_urls.append(str(request.url))
        return httpx.Response(
            200,
            content=FIXTURE,
            headers={"content-type": "text/html; charset=utf-8", "etag": '"inss-2026-v1"'},
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
        parser=parse_inss_employee_snapshot,
    )

    assert requested_urls == [source().url]
    assert run.collection.status == CollectionStatus.COLLECTED
    assert run.candidate is not None
    assert run.candidate.status == ParseStatus.PARSED
    assert run.candidate.source_url == source().url
    assert run.state.last_candidate_sha256 is not None
    assert run.state.etag == '"inss-2026-v1"'
    assert state_store.load(source().source_id) == run.state


def test_canonical_source_failure_does_not_fall_back_to_news_or_search(tmp_path: Path):
    requested_urls = []

    def handler(request: httpx.Request):
        requested_urls.append(str(request.url))
        return httpx.Response(404, content=b"missing")

    snapshot_store, collector, state_store = make_runtime(tmp_path, handler)
    run = run_source_pipeline(
        source=source(),
        collector=collector,
        snapshot_store=snapshot_store,
        state_store=state_store,
        observed_at_utc=NOW,
        parser_id=PARSER_ID,
        parser_version=PARSER_VERSION,
        parser=parse_inss_employee_snapshot,
    )

    assert requested_urls == [source().url]
    assert run.collection.status == CollectionStatus.SOURCE_UNAVAILABLE
    assert run.candidate is None
    assert run.state.last_candidate_sha256 is None
