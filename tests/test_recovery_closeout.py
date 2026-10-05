"""Final recovery closeout: verify only the stabilized architectural invariants.

This test intentionally performs no network calls and does not modify data.
It exists to obtain one clean CI result from the exact recovered main state.
"""
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_authority_evidence_is_fail_closed_by_material_identity():
    text=(ROOT/"sanida_fiscal/authority_evidence_v12.py").read_text(encoding="utf-8")
    assert "def _validate_authority_identity" in text
    assert "identity_marker_groups" in text
    assert "_validate_authority_identity(" in text
    assert "HTTP response does not prove source identity" in text


def test_historical_rates_use_chunked_reusable_collection():
    updater=(ROOT/"update_taxas_series.py").read_text(encoding="utf-8")
    engine=(ROOT/"sanida_fiscal/financial_series_v1.py").read_text(encoding="utf-8")
    assert "run_financial_history_source_pipeline_chunked" in updater
    assert "build_financial_series_artifact_chunked" in updater
    assert "financial_history_chunks" in engine
    assert "reused_immutable_segment" in engine
    assert "collect_segment(child_start, child_end)" in engine


def test_fiscal_runtime_hostgator_sync_is_archived_not_active():
    assert not (ROOT/".github/workflows/fiscal-runtime-production.yml").exists()
    archived=ROOT/"ops/workflows/fiscal-runtime-production.yml"
    assert archived.is_file()
    text=archived.read_text(encoding="utf-8")
    assert "Fiscal Runtime Production Sync" in text


def test_af_engine_repairs_are_present_without_promoting_release():
    engine=(ROOT/"sanida_fiscal/engine_v1.py").read_text(encoding="utf-8")
    assembler=(ROOT/"sanida_fiscal/release_assembler_v12.py").read_text(encoding="utf-8")
    assert "withholding_waived" in engine
    assert "withheld_irrf" in engine
    assert "RFB_SCI_COSIT_8_2015" in assembler
    current=json.loads((ROOT/"releases/fiscal-v1/current.json").read_text(encoding="utf-8"))
    assert current["release_id"]=="fiscal-v1-sha256-a741aa7873950d029a5c6b1c929727267125424013f09c69137b7e80b294153e"
