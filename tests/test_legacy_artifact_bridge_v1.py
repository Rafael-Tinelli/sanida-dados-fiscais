from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from sanida_fiscal.legacy_artifact_v1 import (
    LegacyArtifactBoundaryError,
    build_legacy_dados_fiscais,
    build_legacy_payroll_fields,
)
from sanida_fiscal.inss_employee_v1 import (
    PARSER_VERSION as INSS_PARSER_VERSION,
    parse_inss_employee_snapshot,
)
from sanida_fiscal.rfb_irrf_v1 import (
    PARSER_VERSION as RFB_PARSER_VERSION,
    parse_rfb_irrf_snapshot,
)
from sanida_fiscal.source_catalog_v1 import run_registered_source_pipeline
from sanida_fiscal.source_ids_v1 import INSS_SOURCE_ID, RFB_SOURCE_ID


NOW = datetime(2026, 9, 13, 21, 0, tzinfo=timezone.utc)
RFB_FIXTURE = Path("tests/fixtures/sources/rfb_irrf_2026_fragment.html").read_bytes()
INSS_FIXTURE = Path("tests/fixtures/sources/inss_employee_2026_fragment.html").read_bytes()


def test_legacy_bridge_preserves_schema_2_2_values_from_canonical_payloads():
    fields = build_legacy_payroll_fields(
        rfb_payload=parse_rfb_irrf_2026_snapshot(RFB_FIXTURE),
        inss_payload=parse_inss_employee_2026_snapshot(INSS_FIXTURE),
        expected_year=2026,
    )

    assert fields["ano"] == 2026
    assert fields["dep"] == 189.59
    assert fields["inss"] == [
        {"limite": 1621.0, "aliquota": 0.075},
        {"limite": 2902.84, "aliquota": 0.09},
        {"limite": 4354.27, "aliquota": 0.12},
        {"limite": 8475.55, "aliquota": 0.14},
    ]
    assert fields["irrf"]["tabela"][-1] == {
        "limite": 9_000_000_000.0,
        "aliquota": 0.275,
        "deducao": 908.73,
    }
    assert fields["irrf"]["simplificado"] == 607.20
    assert fields["irrf"]["reducao_mensal"] == {
        "isenta_ate": 5000.0,
        "reduz_ate": 7350.0,
        "max_reducao_ate_5000": 312.89,
        "a": 978.62,
        "b": 0.133145,
    }


def test_legacy_bridge_rejects_relabeling_2026_candidates_as_2027():
    with pytest.raises(LegacyArtifactBoundaryError, match="reference-year mismatch"):
        build_legacy_payroll_fields(
            rfb_payload=parse_rfb_irrf_2026_snapshot(RFB_FIXTURE),
            inss_payload=parse_inss_employee_2026_snapshot(INSS_FIXTURE),
            expected_year=2027,
        )


def test_legacy_bridge_accepts_matching_2027_rollover_payloads():
    rfb = parse_rfb_irrf_2026_snapshot(RFB_FIXTURE.replace(b"2026", b"2027"))
    inss = parse_inss_employee_2026_snapshot(INSS_FIXTURE.replace(b"2026", b"2027"))

    fields = build_legacy_payroll_fields(
        rfb_payload=rfb,
        inss_payload=inss,
        expected_year=2027,
    )

    assert fields["ano"] == 2027


def test_legacy_bridge_rejects_malformed_observation_type_even_when_year_matches():
    rfb = parse_rfb_irrf_2026_snapshot(RFB_FIXTURE)
    inss = parse_inss_employee_2026_snapshot(INSS_FIXTURE)
    rfb["observation_type"] = "rfb_irrf_current"

    with pytest.raises(LegacyArtifactBoundaryError, match="unexpected RFB observation_type"):
        build_legacy_payroll_fields(
            rfb_payload=rfb,
            inss_payload=inss,
            expected_year=2026,
        )


def test_full_legacy_artifact_is_built_only_from_current_parsed_pipeline_runs(tmp_path: Path):
    def handler(request: httpx.Request):
        if "receitafederal" in str(request.url):
            return httpx.Response(200, content=RFB_FIXTURE, headers={"content-type": "text/html"})
        if "tabela-de-contribuicao-mensal" in str(request.url):
            return httpx.Response(200, content=INSS_FIXTURE, headers={"content-type": "text/html"})
        raise AssertionError(f"unexpected URL: {request.url}")

    transport = httpx.MockTransport(handler)
    runs = {}
    for source_id in (RFB_SOURCE_ID, INSS_SOURCE_ID):
        runs[source_id] = run_registered_source_pipeline(
            source_id=source_id,
            observed_at_utc=NOW,
            snapshot_root=tmp_path / "snapshots",
            state_root=tmp_path / "state",
            transport=transport,
            use_http_validators=False,
        )

    artifact = build_legacy_dados_fiscais(
        rfb_run=runs[RFB_SOURCE_ID],
        inss_run=runs[INSS_SOURCE_ID],
        expected_year=2026,
        taxas={"selic": 14.25, "cdi": 14.15, "cdi_basis": "fixture"},
        taxas_source_meta={"origin": "fixture"},
        generated_at_utc="2026-09-13T21:00:00Z",
    )

    assert artifact["schema_version"] == "2.2.0"
    assert artifact["ano"] == 2026
    assert artifact["meta"]["warnings"] == ["phase4_legacy_compatibility_artifact"]
    expected_parser_versions = {
        "irrf": RFB_PARSER_VERSION,
        "inss": INSS_PARSER_VERSION,
    }
    for key in ("irrf", "inss"):
        meta = artifact["meta"]["sources"][key]
        assert meta["snapshot_sha256"]
        assert meta["candidate_sha256"]
        assert meta["parser_version"] == expected_parser_versions[key]
        assert meta["collection_status"] == "COLLECTED"


def test_full_legacy_artifact_rejects_state_only_not_modified_run(tmp_path: Path):
    calls = 0

    def handler(request: httpx.Request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(200, content=RFB_FIXTURE, headers={"etag": '"v1"'})
        return httpx.Response(304, headers={"etag": '"v1"'})

    transport = httpx.MockTransport(handler)
    first = run_registered_source_pipeline(
        source_id=RFB_SOURCE_ID,
        observed_at_utc=NOW,
        snapshot_root=tmp_path / "snapshots",
        state_root=tmp_path / "state",
        transport=transport,
    )
    second = run_registered_source_pipeline(
        source_id=RFB_SOURCE_ID,
        observed_at_utc=NOW,
        snapshot_root=tmp_path / "snapshots",
        state_root=tmp_path / "state",
        transport=transport,
    )

    assert first.candidate is not None
    assert second.candidate is None
    with pytest.raises(LegacyArtifactBoundaryError, match="no current PARSED candidate"):
        build_legacy_dados_fiscais(
            rfb_run=second,
            inss_run=second,
            expected_year=2026,
            taxas={"selic": 14.25, "cdi": 14.15},
            taxas_source_meta={},
            generated_at_utc="2026-09-13T21:00:00Z",
        )


def test_registered_pipeline_accepts_legacy_alias_but_returns_canonical_identity(tmp_path: Path):
    def handler(request: httpx.Request):
        return httpx.Response(200, content=RFB_FIXTURE, headers={"content-type": "text/html"})

    run = run_registered_source_pipeline(
        source_id="RFB_IRRF_TABLE_2026",
        observed_at_utc=NOW,
        snapshot_root=tmp_path / "snapshots",
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidates",
        transport=httpx.MockTransport(handler),
        use_http_validators=False,
    )

    assert run.collection.source_id == RFB_SOURCE_ID
    assert run.state.source_id == RFB_SOURCE_ID
