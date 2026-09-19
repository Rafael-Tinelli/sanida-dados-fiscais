from __future__ import annotations

from pathlib import Path

import pytest

from sanida_fiscal.source_catalog_v1 import (
    parser_binding,
    parser_for_reference_year,
    resolve_source_for_reference_year,
)
from sanida_fiscal.sources_v1 import ParserIncompatibleError, load_source_registry


REGISTRY = Path("docs/source-registry-v1.json")
RFB_FIXTURE = Path("tests/fixtures/sources/rfb_irrf_2026_fragment.html").read_bytes()
INSS_FIXTURE = Path("tests/fixtures/sources/inss_employee_2026_fragment.html").read_bytes()


def test_rfb_annual_url_is_resolved_from_reference_year():
    source = load_source_registry(REGISTRY)["RFB_IRRF_TABLE_2026"]
    resolved = resolve_source_for_reference_year(
        source,
        source_id=source.source_id,
        reference_year=2027,
    )

    assert source.url.endswith("/2026")
    assert resolved.url.endswith("/2027")
    assert resolved.source_id == source.source_id


def test_inss_operational_url_remains_stable_across_years():
    source = load_source_registry(REGISTRY)["INSS_TABLE_2026"]
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
        ("RFB_IRRF_TABLE_2026", RFB_FIXTURE),
        ("INSS_TABLE_2026", INSS_FIXTURE),
    ],
)
def test_current_year_parser_fails_closed_on_previous_year_snapshot(source_id, fixture):
    parser = parser_for_reference_year(parser_binding(source_id), 2027)

    with pytest.raises(ParserIncompatibleError, match="reference-year mismatch"):
        parser(fixture)


@pytest.mark.parametrize(
    ("source_id", "fixture"),
    [
        ("RFB_IRRF_TABLE_2026", RFB_FIXTURE.replace(b"2026", b"2027")),
        ("INSS_TABLE_2026", INSS_FIXTURE.replace(b"2026", b"2027")),
    ],
)
def test_current_year_parser_accepts_matching_rollover_snapshot(source_id, fixture):
    parser = parser_for_reference_year(parser_binding(source_id), 2027)
    payload = parser(fixture)

    assert payload["reference_year"] == 2027
