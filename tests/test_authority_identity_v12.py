from __future__ import annotations

import json
from pathlib import Path

import pytest

from sanida_fiscal.authority_evidence_v12 import (
    AuthorityEvidenceError,
    _validate_authority_identity,
    _validate_sci_cosit_8_2015_api_payload,
    select_authority_sources,
)


REGISTRY = Path("docs/source-registry-v1.json")
GENERIC_NORMAS_SNAPSHOT = Path(
    "evidence/fiscal-authority-v1/RFB_SC_209_2021/"
    "a2/a2d1b9ca70109427fe3b5f287799af4d34ce66fa6f89d0bc0c997b7d6a8fe0b5.html"
)


def _metadata(source_id: str) -> dict:
    registry = json.loads(REGISTRY.read_text(encoding="utf-8"))
    return next(item for item in registry["sources"] if item["source_id"] == source_id)


@pytest.mark.parametrize("source_id", ["RFB_SC_209_2021", "RFB_IN_1500_2014"])
def test_generic_normas_shell_cannot_be_available_as_distinct_legal_source(source_id: str):
    body = GENERIC_NORMAS_SNAPSHOT.read_bytes()
    with pytest.raises(AuthorityEvidenceError, match="does not prove source identity"):
        _validate_authority_identity(
            source_id=source_id,
            metadata=_metadata(source_id),
            body=body,
            media_type="text/html; charset=utf-8",
        )


def test_sc_209_identity_requires_number_year_and_subject():
    valid = b"""
    <html><body>
      Receita Federal - Solucao de Consulta Cosit 209 de 2021.
      Tratamento tributario do abono pecuniario de ferias e do terco constitucional.
    </body></html>
    """
    _validate_authority_identity(
        source_id="RFB_SC_209_2021",
        metadata=_metadata("RFB_SC_209_2021"),
        body=valid,
        media_type="text/html",
    )


def test_marker_contract_is_fail_closed_when_malformed():
    with pytest.raises(AuthorityEvidenceError, match="malformed identity marker"):
        _validate_authority_identity(
            source_id="TEST",
            metadata={"identity_marker_groups": [["ok"], []]},
            body=b"<html><body>ok</body></html>",
            media_type="text/html",
        )


def test_canonical_selection_excludes_broken_normas_spa_sources():
    selected = select_authority_sources(
        source_registry_path=REGISTRY,
        rule_inventory_path=Path("docs/rule-inventory-v1.json"),
    )
    selected_sources = set(selected.values())
    assert "RFB_IN_1500_2014" not in selected_sources
    assert "RFB_SC_209_2021" not in selected_sources
    assert selected["irrf.deductions_by_income_type"] == "ESOCIAL_TABLES_S13_NT07_2026"
    assert selected["thirteenth.irrf.exclusive_assessment"] == "ESOCIAL_TABLES_S13_NT07_2026"
    assert selected["vacation.irrf.separate_assessment"] == "RFB_QA_IRPF_2026"
    assert selected["vacation.abono.ir_exemption"] == "RFB_QA_IRPF_2026"
    assert selected["vacation.abono_constitutional_third.ir_incidence"] == "RFB_SCI_COSIT_8_2015"


@pytest.mark.parametrize(
    "source_id,html",
    [
        (
            "PLANALTO_CLT",
            "<html><body>DECRETO-LEI Nº 5.452. Consolidação das Leis do Trabalho.</body></html>",
        ),
        (
            "PLANALTO_DECRETO_10854_2021",
            "<html><body>DECRETO Nº 10.854, DE 10 DE NOVEMBRO DE 2021</body></html>",
        ),
        (
            "ESOCIAL_TABLES_S13_NT07_2026",
            "<html><body>eSocial — Tabela 03 — Tabela 19 — Tabela 21</body></html>",
        ),
    ],
)
def test_material_html_identity_contract_accepts_expected_official_markers(source_id, html):
    _validate_authority_identity(
        source_id=source_id,
        metadata=_metadata(source_id),
        body=html.encode("utf-8"),
        media_type="text/html",
    )


def test_planalto_latin1_identity_bytes_are_decoded_before_validation():
    html = (
        '<html><head><meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1"></head>'
        '<body>DECRETO-LEI Nº 5.452. Aprova a Consolidação das Leis do Trabalho.</body></html>'
    ).encode("iso-8859-1")
    _validate_authority_identity(
        source_id="PLANALTO_CLT",
        metadata=_metadata("PLANALTO_CLT"),
        body=html,
        media_type="text/html",
    )



def _sci_api_payload(*, id_ato=65843, numero="8", ano="2015", sigla="SCI", orgao="Cosit"):
    return {
        "quantidadeTotal": 1,
        "atos": [
            {
                "idAto": id_ato,
                "numeroAto": numero,
                "anoAto": ano,
                "dataAto": "12/06/2015",
                "dataPublicacao": "06/07/2015",
                "tipoAto": {
                    "idTipoAto": 75,
                    "nomeTipoAto": "Solução de Consulta Interna",
                    "siglaTipoAto": sigla,
                },
                "orgaos": [
                    {
                        "idOrgao": 111,
                        "siglaOrgao": orgao,
                        "nomeOrgao": "Coordenação-Geral de Tributação",
                    }
                ],
                "ementa": (
                    "Contribuição previdenciária incide sobre o valor integral do "
                    "terço constitucional de férias, mesmo quando houver conversão "
                    "de parte do período de férias em abono pecuniário. Férias "
                    "indenizadas permanecem fora da hipótese descrita. Para o imposto "
                    "sobre a renda, o valor do adicional constitucional, inclusive "
                    "o incidente sobre abono pecuniário, é tributado pelo imposto "
                    "sobre a renda."
                ),
            }
        ],
    }


def test_sci_8_official_api_record_proves_exact_material_identity():
    raw = json.dumps(_sci_api_payload(), ensure_ascii=False).encode("utf-8")
    _validate_sci_cosit_8_2015_api_payload(raw)


@pytest.mark.parametrize(
    "mutation",
    [
        {"idAto": 99999},
        {"numeroAto": "9"},
        {"anoAto": "2016"},
        {"siglaTipoAto": "SC"},
        {"siglaOrgao": "Disit"},
    ],
)
def test_sci_8_official_api_record_rejects_wrong_identity(mutation):
    payload = _sci_api_payload()
    row = payload["atos"][0]
    if "siglaTipoAto" in mutation:
        row["tipoAto"]["siglaTipoAto"] = mutation["siglaTipoAto"]
    elif "siglaOrgao" in mutation:
        row["orgaos"][0]["siglaOrgao"] = mutation["siglaOrgao"]
        row["orgaos"][0]["nomeOrgao"] = "Divisão de Tributação"
    else:
        row.update(mutation)
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    with pytest.raises(AuthorityEvidenceError, match="did not identify exactly one"):
        _validate_sci_cosit_8_2015_api_payload(raw)


def test_sci_8_official_api_record_rejects_incomplete_legal_ementa():
    payload = _sci_api_payload()
    payload["atos"][0]["ementa"] = "Solução de Consulta Interna 8/2015 sobre férias."
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    with pytest.raises(AuthorityEvidenceError, match="lacks mandatory legal markers"):
        _validate_sci_cosit_8_2015_api_payload(raw)
