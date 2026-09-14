from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

from sanida_fiscal.review_evidence_identity_v1 import (
    HTML_REVIEW_FINGERPRINT_VERSION,
    RAW_REVIEW_FINGERPRINT_VERSION,
    ReviewEvidenceIdentityError,
    build_review_identity_candidate,
    canonical_html_review_fingerprint,
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
):
    return SimpleNamespace(
        source_id=source_id,
        retrieval_method=SimpleNamespace(value="http_html"),
        snapshot_path=snapshot_path,
        snapshot_sha256=snapshot_sha256,
        parser_id=parser_id,
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


def test_parser_backed_html_remains_bound_to_exact_raw_snapshot_hash(tmp_path: Path):
    body = b"<html><body>Tabela oficial</body></html>"
    relative = "RFB_TEST/aa/snapshot.html"
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_bytes(body)
    raw_sha = sha256(body).hexdigest()
    observation = _observation(
        source_id="RFB_TEST",
        snapshot_path=relative,
        snapshot_sha256=raw_sha,
        parser_id="fixture_parser",
    )

    review_candidate, identities = build_review_identity_candidate(
        _candidate(observation),
        authority_snapshot_root=tmp_path,
    )

    assert review_candidate.rules[0].provenance[0].snapshot_sha256 == raw_sha
    assert identities["RFB_TEST"]["mode"] == RAW_REVIEW_FINGERPRINT_VERSION
    assert identities["RFB_TEST"]["review_fingerprint_sha256"] == raw_sha


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
