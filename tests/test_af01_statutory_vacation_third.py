"""AF01: full statutory vacation third enters CP even after partial conversion."""
from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path

from sanida_fiscal.types_v1 import IncidenceProfilePayload
from sanida_fiscal.vacation_v1 import (
    cash_allowance_tax_bases, resolve_cash_allowance_tax_treatment,
)

def test_full_statutory_third_on_converted_days_has_cp_and_irrf():
    principal = IncidenceProfilePayload.model_validate({
        "type": "incidence_profile", "components": [{
            "component": "cash_allowance_principal", "social_security": "no", "irrf": "no"
        }]
    })
    third = IncidenceProfilePayload.model_validate({
        "type": "incidence_profile", "components": [{
            "component": "constitutional_third_on_cash_allowance",
            "social_security": "yes", "irrf": "yes"
        }]
    })
    treatment = resolve_cash_allowance_tax_treatment(
        principal_payload=principal, constitutional_third_payload=third
    )
    bases = cash_allowance_tax_bases(
        principal_amount="1000.00", constitutional_third_amount="333.33",
        treatment=treatment,
    )
    assert bases.social_security_base == Decimal("333.33")
    assert bases.irrf_taxable_amount == Decimal("333.33")

def test_official_cosit_source_registered_without_new_rule():
    registry=json.loads(Path("docs/source-registry-v1.json").read_text())
    inventory=json.loads(Path("docs/rule-inventory-v1.json").read_text())
    source_id="RFB_SCI_COSIT_8_2015"
    assert any(x["source_id"] == source_id and x["url"].endswith("idArquivoBinario=36769") for x in registry["sources"])
    assert len(inventory["rules"]) == 32
    assert source_id in next(x for x in inventory["rules"] if x["rule_id"]=="vacation.abono_constitutional_third.ir_incidence")["source_ids"]

def test_assembler_marks_third_yes_only_in_candidate_overlay():
    from sanida_fiscal.release_assembler_v12 import _apply_structural_successor_overlays
    # The published v1 example is intentionally immutable and CP=no.
    raw=json.loads(Path("contracts/examples/fiscal-contract-v1.example.json").read_text())
    rules={r["rule_id"]:deepcopy(r) for r in raw["rules"]}
    rules["technical.money_decimal_and_rounding"]={"rounding_policy":{}}
    _apply_structural_successor_overlays(rules)
    row=next(c for c in rules["vacation.abono_constitutional_third.ir_incidence"]["payload"]["components"] if c["component"]=="constitutional_third_on_cash_allowance")
    assert row["social_security"]=="yes" and row["irrf"]=="yes"
    assert next(r for r in raw["rules"] if r["rule_id"]=="vacation.abono_constitutional_third.ir_incidence")["payload"]["components"][0]["social_security"]=="no"

def test_release_gate_requires_material_official_cosit_snapshot():
    from types import SimpleNamespace
    from sanida_fiscal.publication_v1 import _assert_af01_source_evidence, PromotionBlockedError
    from sanida_fiscal.types_v1 import IncidenceProfilePayload, SourceObservationStatus
    from pytest import raises
    profile=IncidenceProfilePayload.model_validate({
        "type":"incidence_profile",
        "components":[{"component":"constitutional_third_on_cash_allowance","irrf":"yes","social_security":"yes"}]
    })
    rule=SimpleNamespace(
        rule_id="vacation.abono_constitutional_third.ir_incidence",
        payload=profile,
        provenance=[SimpleNamespace(
            source_id="RFB_SC_209_2021",
            status=SourceObservationStatus.AVAILABLE,
            snapshot_sha256="a"*64,
            snapshot_path="snapshots/sc-209.pdf",
        )]
    )
    with raises(PromotionBlockedError, match="RFB_SCI_COSIT_8_2015"):
        _assert_af01_source_evidence(SimpleNamespace(rules=[rule]))

    for evidence in (
        SimpleNamespace(
            source_id="RFB_SCI_COSIT_8_2015",
            status=SourceObservationStatus.UNAVAILABLE,
            snapshot_sha256=None,
            snapshot_path=None,
        ),
        SimpleNamespace(
            source_id="RFB_SCI_COSIT_8_2015",
            status=SourceObservationStatus.AVAILABLE,
            snapshot_sha256=None,
            snapshot_path=None,
        ),
        SimpleNamespace(
            source_id="RFB_SCI_COSIT_8_2015",
            status=SourceObservationStatus.AVAILABLE,
            snapshot_sha256="b"*64,
            snapshot_path=None,
        ),
    ):
        rule.provenance=[evidence]
        with raises(PromotionBlockedError, match="immutable snapshot"):
            _assert_af01_source_evidence(SimpleNamespace(rules=[rule]))

    rule.provenance=[SimpleNamespace(
        source_id="RFB_SCI_COSIT_8_2015",
        status=SourceObservationStatus.AVAILABLE,
        snapshot_sha256="c"*64,
        snapshot_path="snapshots/authority/rfb-sci-cosit-8-2015.pdf",
    )]
    _assert_af01_source_evidence(SimpleNamespace(rules=[rule]))


def test_esocial_simplified_abono_source_cannot_remain_authority():
    registry=json.loads(Path("docs/source-registry-v1.json").read_text())
    inventory=json.loads(Path("docs/rule-inventory-v1.json").read_text())
    assert not any(x["source_id"] == "ESOCIAL_SIMPLIFIED_ABONO_2026" for x in registry["sources"])
    for rule_id in (
        "vacation.abono.ir_exemption",
        "vacation.abono_constitutional_third.ir_incidence",
    ):
        rule=next(x for x in inventory["rules"] if x["rule_id"] == rule_id)
        assert "ESOCIAL_SIMPLIFIED_ABONO_2026" not in rule["source_ids"]
        assert "ESOCIAL_TABLES_S13_NT07_2026" in rule["source_ids"]
