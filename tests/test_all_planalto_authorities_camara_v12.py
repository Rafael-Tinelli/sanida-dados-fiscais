"""All currently used Planalto norms have independent Câmara provenance."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sanida_fiscal.authority_evidence_v12 import (
    AuthorityEvidenceError,
    PREFERRED_RULE_SOURCES,
    _validate_authority_identity,
    select_authority_sources,
)
from sanida_fiscal.sources_v1 import load_source_registry

ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "docs/source-registry-v1.json"
INVENTORY = ROOT / "docs/rule-inventory-v1.json"

REPLACEMENTS = {
    "PLANALTO_DECRETO_10854_2021": "CAMARA_DECRETO_10854_2021",
    "PLANALTO_LEI_4090_1962": "CAMARA_LEI_4090_1962",
    "PLANALTO_LEI_4749_1965": "CAMARA_LEI_4749_1965",
    "PLANALTO_LEI_15270_2025": "CAMARA_LEI_15270_2025",
}
SELECTED = {
    "thirteenth.accrual.twelfths": "CAMARA_DECRETO_10854_2021",
    "thirteenth.reference_remuneration": "CAMARA_DECRETO_10854_2021",
    "thirteenth.variable_remuneration": "CAMARA_DECRETO_10854_2021",
    "thirteenth.advance": "CAMARA_DECRETO_10854_2021",
    "termination.thirteenth_proportional": "CAMARA_DECRETO_10854_2021",
    "thirteenth.irrf.reduction.2026": "CAMARA_LEI_15270_2025",
    "vacation.irrf.reduction.2026": "CAMARA_LEI_15270_2025",
}


def _row(source_id):
    data = json.loads(REGISTRY.read_text(encoding="utf-8"))
    return next(row for row in data["sources"] if row["source_id"] == source_id)


def test_all_legacy_norms_remain_registered_with_camara_alternatives():
    registry = load_source_registry(REGISTRY)
    inventory = json.loads(INVENTORY.read_text(encoding="utf-8"))["rules"]
    for legacy, current in REPLACEMENTS.items():
        assert legacy in registry
        assert current in registry
        meta = _row(current)
        assert meta["authority"].startswith("Câmara dos Deputados")
        assert meta["url"].startswith("https://www2.camara.leg.br/legin/fed/")
        assert meta["role"] == "normative_primary"
        assert len(meta["identity_marker_groups"]) >= 3
        assert any(legacy in r.get("source_ids", []) for r in inventory)
        for rule in inventory:
            if legacy in rule.get("source_ids", []):
                assert current in rule["source_ids"]


def test_closed_selection_uses_no_planalto_without_changing_norms():
    selected = select_authority_sources(
        source_registry_path=REGISTRY, rule_inventory_path=INVENTORY
    )
    assert len(selected) == 30
    assert not any(s.startswith("PLANALTO_") for s in selected.values())
    for rule_id, source_id in SELECTED.items():
        assert selected[rule_id] == source_id
        assert PREFERRED_RULE_SOURCES[rule_id] == source_id
    assert selected["vacation.acquisition_period"] == "CAMARA_CLT_ATUALIZADA"
    assert selected["irrf.reduction.2026"] == "RFB_IRRF_TABLE_CURRENT"
    # These two norms were also researched and registered, but do not silently
    # replace the pre-existing choice of Decree 10.854 for these rules.
    assert "CAMARA_LEI_4090_1962" not in selected.values()
    assert "CAMARA_LEI_4749_1965" not in selected.values()


@pytest.mark.parametrize("source_id", tuple(REPLACEMENTS.values()))
def test_authority_identity_is_source_specific_and_fails_closed(source_id):
    meta = _row(source_id)
    all_markers = " ".join(
        " ".join(group) for group in meta["identity_marker_groups"]
    )
    valid = ("<html><body>" + all_markers + "</body></html>").encode("utf-8")
    _validate_authority_identity(
        source_id=source_id, metadata=meta, body=valid,
        media_type="text/html",
    )
    last_marker = meta["identity_marker_groups"][-1][-1].encode("utf-8")
    corrupt = valid.replace(last_marker, b"UNRELATED_NORM")
    with pytest.raises(AuthorityEvidenceError, match="does not prove source identity"):
        _validate_authority_identity(
            source_id=source_id, metadata=meta, body=corrupt,
            media_type="text/html",
        )


def test_original_publications_are_not_described_as_consolidated():
    for source_id in ("CAMARA_LEI_4749_1965", "CAMARA_LEI_15270_2025"):
        source = _row(source_id)
        assert "publicacaooriginal" in source["url"]
        assert "Publicação original" in source["notes"]
        assert "NÃO é texto consolidado" in source["notes"]
    for source_id in ("CAMARA_LEI_4090_1962", "CAMARA_DECRETO_10854_2021"):
        assert "normaatualizada" in _row(source_id)["url"]
