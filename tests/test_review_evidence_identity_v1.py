from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

from sanida_fiscal.review_evidence_identity_v1 import (
    HTML_REVIEW_FINGERPRINT_VERSION,
    PARSED_REVIEW_FINGERPRINT_VERSION,
    RAW_REVIEW_FINGERPRINT_VERSION,
    ReviewEvidenceIdentityError,
    build_review_identity_candidate,
    canonical_html_review_fingerprint,
    canonical_parsed_review_fingerprint,
)


class FakeCandidate:
    def __init__(self, rules):
        self.rules = rules

    def model_copy(self, *, deep: bool = False):
        return deepcopy(self) if deep else FakeCandidate(list(self.rules))


def _observation(
    *,
    source_id: str,
    snapshot_path: str,
    snapshot_sha256: str,
    parser_id: str | None = None,
    parser_version: str | None = None,
):
    return SimpleNamespace(
        source_id=source_id,
        retrieval_method=SimpleNamespace(value="http_html"),
        snapshot_path=snapshot_path,
        snapshot_sha256=snapshot_sha256,
        parser_id=parser_id,
        parser_version=parser_version,
    )


def _normalized(
    *,
    source_id: str,
    snapshot_sha256: str,
    payload: dict,
    parser_id: str = "fixture_parser",
    parser_version: str = "1.0.0",
):
    return SimpleNamespace(
        source_id=source_id,
        source_url=f"https://example.gov/{source_id}",
        observed_at_utc=None,
        snapshot_sha256=snapshot_sha256,
        snapshot_path=f"{source_id}/{snapshot_sha256}.html",
        parser_id=parser_id,
        parser_version=parser_version,
        status=SimpleNamespace(value="PARSED"),
        payload=payload,
    )


def _candidate(observation):
    return FakeCandidate([SimpleNamespace(provenance=[observation])])


def test_html_review_fingerprint_ignores_markup_whitespace_attributes_scripts_and_comments():
    first = b"""
    <html><head><script>nonce = 'one'</script></head><body>
      <!-- transport marker one -->
      <p class='x' data-request='123'>Art. 1 - Texto juridico.</p>
      <a class='button' href='/norma#art1'> Consulte a norma </a>
    </body></html>
    """
    second = b"""
    <html><body><p style='color:red'> Art. 1   -   Texto juridico. </p>
      <a href='/norma#art1' data-request='999'>Consulte a norma</a>
      <script>nonce = 'two'</script>
    </body></html>
    """

    assert canonical_html_review_fingerprint(first) == canonical_html_review_fingerprint(second)


def test_html_review_fingerprint_changes_when_visible_text_or_link_target_changes():
    baseline = b"<p>Art. 1 - regra A</p><a href='/lei#art1'>Lei</a>"
    changed_text = b"<p>Art. 1 - regra B</p><a href='/lei#art1'>Lei</a>"
    changed_link = b"<p>Art. 1 - regra A</p><a href='/lei#art2'>Lei</a>"

    fingerprint = canonical_html_review_fingerprint(baseline)
    assert fingerprint != canonical_html_review_fingerprint(changed_text)
    assert fingerprint != canonical_html_review_fingerprint(changed_link)


def test_unparsed_html_candidate_uses_material_fingerprint_but_preserves_original_candidate(tmp_path: Path):
    body = b"<html><body><p>Conteudo oficial</p><a href='/x'>X</a></body></html>"
    relative = "PLANALTO_TEST/aa/snapshot.html"
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_bytes(body)
    raw_sha = sha256(body).hexdigest()
    observation = _observation(
        source_id="PLANALTO_TEST",
        snapshot_path=relative,
        snapshot_sha256=raw_sha,
    )
    original = _candidate(observation)

    review_candidate, identities = build_review_identity_candidate(
        original,
        authority_snapshot_root=tmp_path,
    )

    expected = canonical_html_review_fingerprint(body)
    assert original.rules[0].provenance[0].snapshot_sha256 == raw_sha
    assert review_candidate.rules[0].provenance[0].snapshot_sha256 == expected
    assert identities["PLANALTO_TEST"] == {
        "mode": HTML_REVIEW_FINGERPRINT_VERSION,
        "review_fingerprint_sha256": expected,
        "raw_snapshot_sha256": raw_sha,
    }


def test_parser_backed_html_uses_normalized_payload_identity_and_preserves_raw_candidate(tmp_path: Path):
    raw_sha = "a" * 64
    observation = _observation(
        source_id="RFB_TEST",
        snapshot_path="RFB_TEST/aa/snapshot.html",
        snapshot_sha256=raw_sha,
        parser_id="fixture_parser",
        parser_version="1.0.0",
    )
    normalized = _normalized(
        source_id="RFB_TEST",
        snapshot_sha256=raw_sha,
        payload={"rate": "0.275", "deduction": "908.73"},
    )
    original = _candidate(observation)

    review_candidate, identities = build_review_identity_candidate(
        original,
        authority_snapshot_root=tmp_path,
        normalized_candidates={"RFB_TEST": normalized},
    )

    expected = canonical_parsed_review_fingerprint(normalized)
    assert original.rules[0].provenance[0].snapshot_sha256 == raw_sha
    assert review_candidate.rules[0].provenance[0].snapshot_sha256 == expected
    assert identities["RFB_TEST"] == {
        "mode": PARSED_REVIEW_FINGERPRINT_VERSION,
        "review_fingerprint_sha256": expected,
        "raw_snapshot_sha256": raw_sha,
    }


def test_parser_backed_review_identity_is_stable_across_raw_html_churn_when_parsed_payload_is_same(tmp_path: Path):
    identities = []
    for raw_sha in ("a" * 64, "b" * 64):
        observation = _observation(
            source_id="RFB_TEST",
            snapshot_path=f"RFB_TEST/{raw_sha}.html",
            snapshot_sha256=raw_sha,
            parser_id="fixture_parser",
            parser_version="1.0.0",
        )
        normalized = _normalized(
            source_id="RFB_TEST",
            snapshot_sha256=raw_sha,
            payload={"table": [{"limit": "5000.00", "rate": "0.15"}]},
        )
        review_candidate, metadata = build_review_identity_candidate(
            _candidate(observation),
            authority_snapshot_root=tmp_path,
            normalized_candidates={"RFB_TEST": normalized},
        )
        identities.append((review_candidate.rules[0].provenance[0].snapshot_sha256, metadata))

    assert identities[0][0] == identities[1][0]
    assert identities[0][1]["RFB_TEST"]["raw_snapshot_sha256"] != identities[1][1]["RFB_TEST"]["raw_snapshot_sha256"]


def test_parser_backed_review_identity_changes_on_normalized_payload_or_parser_version():
    baseline = _normalized(
        source_id="RFB_TEST",
        snapshot_sha256="a" * 64,
        payload={"value": "607.20"},
    )
    changed_payload = _normalized(
        source_id="RFB_TEST",
        snapshot_sha256="b" * 64,
        payload={"value": "608.00"},
    )
    changed_parser = _normalized(
        source_id="RFB_TEST",
        snapshot_sha256="c" * 64,
        payload={"value": "607.20"},
        parser_version="1.0.1",
    )

    fingerprint = canonical_parsed_review_fingerprint(baseline)
    assert fingerprint != canonical_parsed_review_fingerprint(changed_payload)
    assert fingerprint != canonical_parsed_review_fingerprint(changed_parser)


def test_parser_backed_missing_or_mismatched_normalized_candidate_fails_closed(tmp_path: Path):
    observation = _observation(
        source_id="RFB_TEST",
        snapshot_path="RFB_TEST/a.html",
        snapshot_sha256="a" * 64,
        parser_id="fixture_parser",
        parser_version="1.0.0",
    )

    with pytest.raises(ReviewEvidenceIdentityError, match="no normalized candidate"):
        build_review_identity_candidate(
            _candidate(observation),
            authority_snapshot_root=tmp_path,
            normalized_candidates={},
        )

    mismatched = _normalized(
        source_id="RFB_TEST",
        snapshot_sha256="b" * 64,
        payload={"value": "607.20"},
    )
    with pytest.raises(ReviewEvidenceIdentityError, match="raw snapshot mismatch"):
        build_review_identity_candidate(
            _candidate(observation),
            authority_snapshot_root=tmp_path,
            normalized_candidates={"RFB_TEST": mismatched},
        )


def test_unparsed_html_review_identity_is_stable_across_presentation_only_raw_hash_churn(tmp_path: Path):
    first = b"<p class='a'>Lei 1</p><a href='/lei'>Texto</a><script>one()</script>"
    second = b"<p id='b'> Lei 1 </p><a data-x='2' href='/lei'>Texto</a><script>two()</script>"

    candidates = []
    for index, body in enumerate((first, second), start=1):
        relative = f"PLANALTO_TEST/{index}/snapshot.html"
        path = tmp_path / relative
        path.parent.mkdir(parents=True)
        path.write_bytes(body)
        raw_sha = sha256(body).hexdigest()
        observation = _observation(
            source_id="PLANALTO_TEST",
            snapshot_path=relative,
            snapshot_sha256=raw_sha,
        )
        review_candidate, _ = build_review_identity_candidate(
            _candidate(observation),
            authority_snapshot_root=tmp_path,
        )
        candidates.append(review_candidate)

    assert first != second
    assert sha256(first).hexdigest() != sha256(second).hexdigest()
    assert (
        candidates[0].rules[0].provenance[0].snapshot_sha256
        == candidates[1].rules[0].provenance[0].snapshot_sha256
    )


def test_non_html_non_parser_evidence_remains_raw_sha_bound(tmp_path: Path):
    observation = SimpleNamespace(
        source_id="PDF_TEST",
        retrieval_method=SimpleNamespace(value="http_pdf"),
        snapshot_path="PDF_TEST/a.pdf",
        snapshot_sha256="c" * 64,
        parser_id=None,
        parser_version=None,
    )
    review_candidate, identities = build_review_identity_candidate(
        _candidate(observation),
        authority_snapshot_root=tmp_path,
    )
    assert review_candidate.rules[0].provenance[0].snapshot_sha256 == "c" * 64
    assert identities["PDF_TEST"]["mode"] == RAW_REVIEW_FINGERPRINT_VERSION


def test_snapshot_integrity_mismatch_fails_closed(tmp_path: Path):
    relative = "PLANALTO_TEST/aa/snapshot.html"
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_bytes(b"<p>real bytes</p>")
    observation = _observation(
        source_id="PLANALTO_TEST",
        snapshot_path=relative,
        snapshot_sha256="a" * 64,
    )

    with pytest.raises(ReviewEvidenceIdentityError, match="integrity mismatch"):
        build_review_identity_candidate(
            _candidate(observation),
            authority_snapshot_root=tmp_path,
        )
