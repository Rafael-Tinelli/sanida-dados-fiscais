"""Historical snapshots remain auditable after subsequent successful collection."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import httpx
import pytest
from sanida_fiscal.financial_artifact_v1 import build_financial_reference_artifact
from sanida_fiscal.financial_evidence_v1 import (
    FinancialEvidenceError, verify_financial_artifact_evidence,
    verify_preserved_financial_last_good_artifact_evidence,
)
from sanida_fiscal.financial_source_catalog_v1 import run_registered_financial_source_pipeline

NOW=datetime(2026,9,13,22,tzinfo=timezone.utc)
FILES={
    "BCB_SELIC_META_SGS_432":"bcb_selic_sgs432.json",
    "BCB_CDI_DAILY_SGS_12":"bcb_cdi_sgs12.json",
}
def collect(tmp,source,when=NOW, status=200):
    body=Path("tests/fixtures/sources",FILES[source]).read_bytes()
    def handler(req):
        if status == "network":
            raise httpx.ConnectError("simulated outage")
        return httpx.Response(status, content=body,headers={"content-type":"application/json"})
    return run_registered_financial_source_pipeline(
        source_id=source, observed_at_utc=when,
        registry_path=Path("docs/financial-source-registry-v1.json"),
        snapshot_root=tmp/"snapshots",state_root=tmp/"state",
        candidate_root=tmp/"candidates",transport=httpx.MockTransport(handler),
        max_attempts=1,timeout_seconds=1,use_http_validators=False,
    )
def published(tmp):
    a=collect(tmp,"BCB_SELIC_META_SGS_432")
    b=collect(tmp,"BCB_CDI_DAILY_SGS_12")
    artifact=build_financial_reference_artifact(
        selic_run=a,cdi_run=b,generated_at_utc="2026-09-13T22:00:00Z")
    p=tmp/"taxas_bacen.json";p.write_text(json.dumps(artifact))
    return p

def test_archived_reference_survives_newer_successful_state(tmp_path):
    path=published(tmp_path)
    collect(tmp_path,"BCB_SELIC_META_SGS_432",when=NOW+timedelta(minutes=50))
    collect(tmp_path,"BCB_CDI_DAILY_SGS_12",when=NOW+timedelta(minutes=50))
    with pytest.raises(FinancialEvidenceError,match="observed_at"):
        verify_financial_artifact_evidence(artifact_path=path,runtime_root=tmp_path)
    assert len(verify_preserved_financial_last_good_artifact_evidence(
        artifact_path=path,runtime_root=tmp_path))==2

def test_archived_reference_rejects_tampered_snapshots(tmp_path):
    path=published(tmp_path)
    v=verify_preserved_financial_last_good_artifact_evidence(
        artifact_path=path,runtime_root=tmp_path)
    snapshot=tmp_path/"snapshots"/v["BCB_SELIC_META_SGS_432"]["snapshot_path"]
    snapshot.write_bytes(b"adulterated")
    with pytest.raises(FinancialEvidenceError,match="sha256"):
        verify_preserved_financial_last_good_artifact_evidence(
            artifact_path=path,runtime_root=tmp_path)

def test_archived_reference_rejects_renamed_source_and_amount(tmp_path):
    path=published(tmp_path)
    d=json.loads(path.read_text())
    d["meta"]["sources"]["selic"]["source_value_pct"]="33.00"
    path.write_text(json.dumps(d))
    with pytest.raises(FinancialEvidenceError,match="value mismatch"):
        verify_preserved_financial_last_good_artifact_evidence(
            artifact_path=path,runtime_root=tmp_path)

def test_failed_next_day_bcb_probe_does_not_rewrite_proven_url(tmp_path):
    prior=collect(tmp_path,"BCB_SELIC_META_SGS_432")
    failed=collect(tmp_path,"BCB_SELIC_META_SGS_432",
        when=NOW+timedelta(hours=12),status="network")
    assert failed.state.source_url==prior.state.source_url
    assert failed.collection.source_url!=prior.collection.source_url
