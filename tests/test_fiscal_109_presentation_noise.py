"""Regression: fiscal review #109 must not publish a new release for website noise."""
from __future__ import annotations

import json
from pathlib import Path

from sanida_fiscal.contract_v1_2 import FiscalContractV12
from sanida_fiscal.presentation_noise_v1 import (
    _material_html_fingerprint,
    is_presentation_only_refresh,
)
from sanida_fiscal.semantic_diff_v1 import PromotionOutcome, assess_promotion

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "evidence/fiscal-authority-v1"
RELEASE = ROOT / "releases/fiscal-v1/releases/fiscal-v1-sha256-a720ba6ccf9683371c0bc8e6dad868f4267ba42bac00935c7efbc2c5cbeea648.json"
REVIEW = ROOT / "state/fiscal-release-v12-review.json"


def _issue_109_contracts():
    original = json.loads(RELEASE.read_text(encoding="utf-8"))
    packet = json.loads(REVIEW.read_text(encoding="utf-8"))
    assert packet["review_key"] == "79fe2476621a293163e876d5be8356a40def06511e1b3d0bee80852b75dc1c14"

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


def test_issue_109_is_verified_transport_noise_without_release() -> None:
    previous, candidate, assessment, packet = _issue_109_contracts()
    assert assessment.outcome == PromotionOutcome.REVIEW_REQUIRED
    assert len([r for r in assessment.diff.rule_diffs if r.changed]) == 16
    assert len(packet["reasons"]) == 15
    assert is_presentation_only_refresh(
        previous=previous,
        candidate=candidate,
        assessment=assessment,
        authority_snapshot_root=EVIDENCE,
    )


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
