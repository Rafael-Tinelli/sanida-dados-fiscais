"""The official Câmara CLT may be selected without misattributing provenance."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sanida_fiscal.authority_evidence_v12 import (
    AuthorityEvidenceError,
    _validate_authority_identity,
    select_authority_sources,
)
from sanida_fiscal.sources_v1 import load_source_registry

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "docs/source-registry-v1.json"
INVENTORY = ROOT / "docs/rule-inventory-v1.json"
CAMARA = "CAMARA_CLT_ATUALIZADA"
CLT_RULES = {
    "vacation.acquisition_period",
    "vacation.entitlement_days_by_absences",
    "vacation.remuneration_and_constitutional_third",
    "vacation.abono_pecuniario",
    "termination.vacation_proportional",
    "termination.acquired_and_overdue_vacation",
}


def test_camara_is_official_primary_with_own_source_identity():
    raw = json.loads(REGISTRY.read_text(encoding="utf-8"))
    row = next(x for x in raw["sources"] if x["source_id"] == CAMARA)
    assert row["authority"].startswith("Câmara dos Deputados")
    assert row["url"].startswith("https://www2.camara.leg.br/legin/")
    assert row["role"] == "normative_primary"
    assert set(row["covers"]) == CLT_RULES
    assert CAMARA in load_source_registry(REGISTRY)
    assert "PLANALTO_CLT" in load_source_registry(REGISTRY)


def test_all_clt_rules_are_approved_for_camara_and_selected():
    raw = json.loads(INVENTORY.read_text(encoding="utf-8"))
    for rule in raw["rules"]:
        if rule["rule_id"] in CLT_RULES:
            assert "PLANALTO_CLT" in rule["source_ids"]
            assert CAMARA in rule["source_ids"]
    selected = select_authority_sources(
        source_registry_path=REGISTRY, rule_inventory_path=INVENTORY
    )
    for rid in CLT_RULES:
        assert selected[rid] == CAMARA
    assert len(selected) == 30


def test_camara_requires_legal_identity_and_relevant_articles():
    row = next(x for x in json.loads(REGISTRY.read_text(encoding="utf-8"))["sources"]
               if x["source_id"] == CAMARA)
    valid = (
        "CÂMARA DOS DEPUTADOS DECRETO-LEI Nº 5.452 de 1943. "
        "Consolidação das Leis do Trabalho. "
        + " ".join(f"Art. {n}." for n in (129,130,134,137,143,146,147))
    ).encode("utf-8")
    _validate_authority_identity(
        source_id=CAMARA, metadata=row, body=valid, media_type="text/html"
    )
    with pytest.raises(AuthorityEvidenceError, match="does not prove source identity"):
        _validate_authority_identity(
            source_id=CAMARA, metadata=row,
            body=valid.replace(b"Art. 143.", b"Art. 142."),
            media_type="text/html",
        )
