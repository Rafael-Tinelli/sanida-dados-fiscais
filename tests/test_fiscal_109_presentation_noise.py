"""Regression: fiscal review #109 must not publish a new release for website noise."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from sanida_fiscal.contract_v1_2 import FiscalContractV12
from sanida_fiscal.presentation_noise_v1 import (
    _material_html_fingerprint,
    analyze_nonmaterial_refresh,
    is_presentation_only_refresh,
)
from sanida_fiscal.semantic_diff_v1 import PromotionOutcome, assess_promotion

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "evidence/fiscal-authority-v1"
RELEASE = ROOT / "releases/fiscal-v1/releases/fiscal-v1-sha256-a720ba6ccf9683371c0bc8e6dad868f4267ba42bac00935c7efbc2c5cbeea648.json"
REVIEW = ROOT / "tests/fixtures/fiscal_review_109_noise.json"
REVIEW_111 = ROOT / "tests/fixtures/fiscal_review_111_noise.json"
REVIEW_114 = ROOT / "tests/fixtures/fiscal_review_114_scoped_clt.json"


def _archived_case(review_path: Path):
    original = json.loads(RELEASE.read_text(encoding="utf-8"))
    packet = json.loads(review_path.read_text(encoding="utf-8"))
    assert packet["review_key"] in {
        "79fe2476621a293163e876d5be8356a40def06511e1b3d0bee80852b75dc1c14",
        "96415528f583833d0cd4b5fa04daee0da3c1a34d0800de9e416b4bc63d551037",
        "7397b71e7ccc06788a39eb509f142605d0153a884bda78b6196a64ecaa648f55",
    }

    previous = FiscalContractV12.model_validate(original)
    next_data = previous.model_dump(mode="json", exclude_none=True)
    next_data["release_id"] = "candidate-v1-2"
    next_data["status"] = "CANDIDATE"
    next_data["generated_at_utc"] = packet["created_at_utc"]
    next_data["supersedes_release_id"] = previous.release_id
    next_data["lifecycle"] = {}
    by_id = {rule["rule_id"]: rule for rule in next_data["rules"]}

    for entry in packet["rules"]:
        rule = by_id[entry["rule_id"]]
        rule["provenance"] = entry["evidence_after"]
        rule["rule_version"] = entry["candidate_rule_version"]
        rule["change_class"] = entry["change_class"]
        rule["quality"]["last_validated_at_utc"] = packet["created_at_utc"]

    candidate = FiscalContractV12.model_validate(next_data)
    assessment = assess_promotion(previous, candidate)
    return previous, candidate, assessment, packet


def _issue_109_contracts():
    return _archived_case(REVIEW)


@pytest.mark.parametrize(
    ("source", "expected_rules", "expected_reasons"),
    [(REVIEW, 16, 15), (REVIEW_111, 20, 16), (REVIEW_114, 20, 16)],
)
def test_review_is_verified_transport_noise_without_release(
    source: Path, expected_rules: int, expected_reasons: int
) -> None:
    previous, candidate, assessment, packet = _archived_case(source)
    assert assessment.outcome == PromotionOutcome.REVIEW_REQUIRED
    assert len([r for r in assessment.diff.rule_diffs if r.changed]) == expected_rules
    assert packet["expected_reasons"] == expected_reasons
    assert is_presentation_only_refresh(
        previous=previous,
        candidate=candidate,
        assessment=assessment,
        authority_snapshot_root=EVIDENCE,
    )
    audit = analyze_nonmaterial_refresh(
        previous=previous, candidate=candidate, assessment=assessment,
        authority_snapshot_root=EVIDENCE,
    )
    assert audit is not None
    assert audit["out_of_scope_legal_sources"] == (
        ["PLANALTO_CLT"] if source == REVIEW_114 else []
    )
    assert len(audit["sources"]) == len({
        entry["evidence_after"][0]["source_id"] for entry in packet["rules"]
    })


def test_material_change_in_a_fiscal_parameter_is_never_suppressed() -> None:
    previous, candidate, _, _ = _issue_109_contracts()
    data = candidate.model_dump(mode="json", exclude_none=True)
    target = next(r for r in data["rules"] if r["rule_id"] == "inss.employee.progressive_table")
    target["payload"]["brackets"][0]["upper_bound"] = "1622.00"
    # The candidate must reconcile the parameter-change label/version before
    # assessment can validate it; the bypass itself is prohibited either way.
    target["change_class"] = "PARAMETER_CHANGE"
    changed = FiscalContractV12.model_validate(data)
    assessment = assess_promotion(previous, changed)
    assert assessment.outcome != PromotionOutcome.NO_PUBLISH_REQUIRED
    assert not is_presentation_only_refresh(
        previous=previous,
        candidate=changed,
        assessment=assessment,
        authority_snapshot_root=EVIDENCE,
    )


def test_legal_text_change_in_mte_article_is_material() -> None:
    root = EVIDENCE / "MTE_TERMINATION_FAQ"
    old = (root / "af/af97a38840f693df3c5e8cd1517f077549655a6663004941173163ddcf336dcc.html").read_bytes()
    current = (root / "ef/ef7a349987a55aea54a8c1ed990e52b003b1fa579dc4313a3c8b98e442a046d1.html").read_bytes()
    first = _material_html_fingerprint(old, "MTE_TERMINATION_FAQ")
    assert first == _material_html_fingerprint(current, "MTE_TERMINATION_FAQ")
    assert b"Pedido de demiss" in current
    altered = current.replace(b"Pedido de demiss", b"Dispensa com erro", 1)
    assert first != _material_html_fingerprint(altered, "MTE_TERMINATION_FAQ")
    assert _material_html_fingerprint(b"<html><body>no article</body></html>", "MTE_TERMINATION_FAQ") is None


def test_changed_parser_identity_is_never_suppressed() -> None:
    previous, candidate, assessment, _ = _issue_109_contracts()
    data = candidate.model_dump(mode="json", exclude_none=True)
    target = next(r for r in data["rules"] if r["rule_id"] == "inss.employee.progressive_table")
    target["provenance"][0]["parser_version"] = "1.1.99"
    altered = FiscalContractV12.model_validate(data)
    assert not is_presentation_only_refresh(
        previous=previous,
        candidate=altered,
        assessment=assessment,
        authority_snapshot_root=EVIDENCE,
    )


@pytest.mark.parametrize(
    ("source_id", "old_hash", "new_hash"),
    [
        ("RFB_IRRF_TABLE_CURRENT", "3518438a94a7b74e2be144c7e4401173e674e5649219ebc909e9baccfaef4b7f", "c097f81cff409c05d692c52b79cabe443d6e4eb02020b29a4d1584020710831c"),
        ("RFB_CP_INCIDENCE_TABLE", "b8ad0e5da5079ca78b9e78bddef029be65abdc2c22b55f91845d3bd5683830be", "7920398387cdf37168655b77c23ff3a4f2f815667f91bc7ce4b01d40d671fde0"),
    ],
)
def test_rfb_cnir_navigation_noise_is_not_a_legal_change(
    source_id: str, old_hash: str, new_hash: str
) -> None:
    folder = EVIDENCE / source_id
    old = (folder / old_hash[:2] / (old_hash + ".html")).read_bytes()
    new = (folder / new_hash[:2] / (new_hash + ".html")).read_bytes()
    fingerprint = _material_html_fingerprint(old, source_id)
    assert fingerprint is not None
    assert fingerprint == _material_html_fingerprint(new, source_id)
    soup = BeautifulSoup(new, "html.parser")
    article = soup.select_one("#content-core")
    assert article is not None
    article.append("Alteração material simulada da regra fiscal para teste.")
    assert fingerprint != _material_html_fingerprint(str(soup).encode("utf-8"), source_id)
