from __future__ import annotations

import json
from pathlib import Path

import pytest

from sanida_fiscal.authority_evidence_v12 import (
    AuthorityEvidenceError,
    _validate_authority_identity,
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
      Tratamento tributario do abono pecuniario de ferias.
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


def test_identity_markers_accept_at_least_one_existing_official_html_snapshot_per_protected_source():
    registry = json.loads(REGISTRY.read_text(encoding="utf-8"))
    protected = [
        item for item in registry["sources"]
        if item.get("identity_marker_groups")
        and item.get("machine_readability") != "pdf_text"
    ]
    assert protected
    for metadata in protected:
        source_id = metadata["source_id"]
        root = Path("evidence/fiscal-authority-v1") / source_id
        snapshots = sorted(
            p for p in root.rglob("*")
            if p.is_file() and p.suffix.lower() in {".html", ".htm", ".txt"}
        )
        # Some newly registered authorities may not have a historical snapshot
        # yet; they remain fail-closed at collection time.
        if not snapshots:
            continue
        accepted = []
        for snapshot in snapshots:
            try:
                _validate_authority_identity(
                    source_id=source_id,
                    metadata=metadata,
                    body=snapshot.read_bytes(),
                    media_type="text/html",
                )
            except AuthorityEvidenceError:
                continue
            accepted.append(snapshot)
        assert accepted, f"{source_id}: no existing official snapshot satisfies identity markers"
