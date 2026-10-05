from __future__ import annotations

import json
from pathlib import Path

import pytest

from sanida_fiscal.authority_evidence_v12 import AuthorityEvidenceError, _assert_source_identity

REGISTRY=json.loads(Path("docs/source-registry-v1.json").read_text(encoding="utf-8"))
BY_ID={row["source_id"]:row for row in REGISTRY["sources"]}


def test_every_registered_authority_has_identity_markers():
    missing=[
        row["source_id"] for row in REGISTRY["sources"]
        if row["role"] in {"normative_primary","administrative_norm","official_operational","official_reference_case"}
        and not row.get("identity_markers")
    ]
    assert missing == []


def test_generic_receita_normas_shell_is_never_authority():
    generic=b"""<!doctype html><html><head><title>Normas</title></head>
    <body><app-root></app-root><script src="main.js"></script></body></html>"""
    for source_id in ("RFB_IN_1500_2014","RFB_SC_209_2021","RFB_SCI_COSIT_8_2015"):
        with pytest.raises(AuthorityEvidenceError,match="identity mismatch"):
            _assert_source_identity(
                source_id=source_id,
                registry_item=BY_ID[source_id],
                body=generic,
                media_type="text/html",
            )


def test_in1500_act_identity_accepts_matching_visible_text():
    body="""<html><body>
      Instrução Normativa RFB nº 1.500, de 29 de outubro de 2014.
      Dispõe sobre normas gerais de tributação relativas ao Imposto sobre a Renda das Pessoas Físicas.
    </body></html>""".encode()
    _assert_source_identity(
        source_id="RFB_IN_1500_2014",
        registry_item=BY_ID["RFB_IN_1500_2014"],
        body=body,
        media_type="text/html",
    )


def test_sc209_uses_direct_official_pdf_not_search_page():
    row=BY_ID["RFB_SC_209_2021"]
    assert row["url"].endswith("idArquivoBinario=64080")
    assert row["machine_readability"]=="pdf_text"
    assert {"209","abono pecuniário","terço constitucional"} <= set(row["identity_markers"])
