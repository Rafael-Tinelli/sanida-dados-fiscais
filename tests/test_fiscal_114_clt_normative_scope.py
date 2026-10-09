"""Fail-closed normative scoping: CLT arts. 129–147 vs unrelated ADC 80 notes."""
from __future__ import annotations

import json
from pathlib import Path

from bs4 import BeautifulSoup

from sanida_fiscal.presentation_noise_v1 import (
    _clt_vacation_scope_html,
    _material_html_fingerprint,
)
from sanida_fiscal.review_evidence_identity_v1 import canonical_html_review_fingerprint

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOTS = ROOT / "evidence/fiscal-authority-v1/PLANALTO_CLT"
OLD = SNAPSHOTS / "9f/9f9e535624098bf086652169adfe149fe1f1b4bbacd9b916cfd4b18d65c60dc4.html"
NEW = SNAPSHOTS / "d1/d14abfc44d415252193eb2598405948b1923097874777382f7e49da545c92dc3.html"


def _changed_article(soup: BeautifulSoup, marker: str) -> str:
    anchor = soup.find("a", attrs={"name": marker})
    assert anchor is not None
    paragraph = anchor.find_parent("p")
    assert paragraph is not None
    paragraph.append(" MUDANÇA JURÍDICA MATERIAL SIMULADA.")
    return str(soup)


def test_registry_contract_limits_clt_to_vacation_articles() -> None:
    registry = json.loads((ROOT / "docs/source-registry-v1.json").read_text(encoding="utf-8"))
    clt = next(row for row in registry["sources"] if row["source_id"] == "PLANALTO_CLT")
    assert clt["notes"].startswith("Arts. 129–147:")
    assert "vacation.acquisition_period" in clt["covers"]
    assert "vacation.abono_pecuniario" in clt["covers"]


def test_real_adc80_annotations_change_full_clt_but_not_registered_section() -> None:
    before, after = OLD.read_bytes(), NEW.read_bytes()
    assert canonical_html_review_fingerprint(before) != canonical_html_review_fingerprint(after)
    snippet1, snippet2 = _clt_vacation_scope_html(before), _clt_vacation_scope_html(after)
    assert snippet1 is not None and snippet2 is not None
    assert canonical_html_review_fingerprint(snippet1) == canonical_html_review_fingerprint(snippet2)
    assert _material_html_fingerprint(before, "PLANALTO_CLT") == _material_html_fingerprint(after, "PLANALTO_CLT")
    text = BeautifulSoup(snippet2, "html.parser").get_text(" ", strip=True)
    assert "Art. 129" in text and "Art. 147" in text
    assert "Art. 148" not in text
    assert "ADC 80" not in text


def test_in_scope_legal_text_mutation_blocks_equivalence() -> None:
    before = OLD.read_bytes()
    after = BeautifulSoup(NEW.read_bytes(), "html.parser")
    changed = _changed_article(after, "art143")
    assert _material_html_fingerprint(before, "PLANALTO_CLT") != _material_html_fingerprint(
        changed.encode("utf-8"), "PLANALTO_CLT",
    )


def test_in_scope_link_change_blocks_equivalence() -> None:
    source = NEW.read_bytes()
    before = _material_html_fingerprint(source, "PLANALTO_CLT")
    soup = BeautifulSoup(source, "html.parser")
    article = soup.find("a", attrs={"name": "art143"}).find_parent("p")
    link = article.find("a", href=True)
    assert link is not None
    link["href"] = "https://example.invalid/fake-amendment"
    assert _material_html_fingerprint(str(soup).encode("utf-8"), "PLANALTO_CLT") != before


def test_out_of_scope_legal_text_does_not_trigger_fiscal_release() -> None:
    source = NEW.read_bytes()
    before = _material_html_fingerprint(source, "PLANALTO_CLT")
    soup = BeautifulSoup(source, "html.parser")
    changed = _changed_article(soup, "art790.")
    assert canonical_html_review_fingerprint(source) != canonical_html_review_fingerprint(changed.encode("utf-8"))
    assert _material_html_fingerprint(changed.encode("utf-8"), "PLANALTO_CLT") == before


def test_missing_duplicate_or_reordered_clt_anchors_fail_closed() -> None:
    source = NEW.read_bytes()
    soup = BeautifulSoup(source, "html.parser")
    soup.find("a", attrs={"name": "art148"}).decompose()
    assert _clt_vacation_scope_html(str(soup).encode("utf-8")) is None

    soup = BeautifulSoup(source, "html.parser")
    duplicate = soup.new_tag("a")
    duplicate["name"] = "art129"
    soup.body.append(duplicate)
    assert _clt_vacation_scope_html(str(soup).encode("utf-8")) is None

    soup = BeautifulSoup(source, "html.parser")
    soup.find("a", attrs={"name": "art147"})["name"] = "art147renumbered"
    assert _clt_vacation_scope_html(str(soup).encode("utf-8")) is None
