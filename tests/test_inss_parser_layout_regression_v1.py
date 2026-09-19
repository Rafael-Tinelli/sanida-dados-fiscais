from __future__ import annotations

from pathlib import Path

from sanida_fiscal.inss_employee_v1 import PARSER_VERSION, parse_inss_employee_snapshot


FIXTURE = Path("tests/fixtures/sources/inss_employee_2026_fragment.html")


def test_inss_normative_reference_survives_html_tag_boundary_before_comma() -> None:
    html = FIXTURE.read_text(encoding="utf-8")
    html = html.replace(
        "PORTARIA INTERMINISTERIAL MPS/MF Nº 13, de 09/01/2026",
        "<a href='https://www.gov.br/previdencia/portaria-13'>PORTARIA INTERMINISTERIAL MPS/MF Nº 13</a>, de 09/01/2026",
    )

    payload = parse_inss_employee_snapshot(html.encode("utf-8"))

    assert PARSER_VERSION == "1.1.0"
    assert payload["normative_reference"] == {
        "act_id": "PORTARIA_INTERMINISTERIAL_MPS_MF_13_2026",
        "act_date": "2026-01-09",
    }
    assert payload["contribution_ceiling_brl"] == "8475.55"
    assert payload["thirteenth_assessment"] == "separate_from_monthly_remuneration"
