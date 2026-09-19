from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import httpx
import pytest

from sanida_fiscal.financial_artifact_v1 import build_financial_reference_artifact
from sanida_fiscal.financial_source_catalog_v1 import run_registered_financial_source_pipeline
from sanida_fiscal.legacy_artifact_v1 import build_legacy_dados_fiscais
from sanida_fiscal.production_evidence_v1 import (
    ProductionEvidenceError,
    verify_legacy_artifact_evidence,
)
from sanida_fiscal.source_catalog_v1 import run_registered_source_pipeline
from sanida_fiscal.source_ids_v1 import INSS_SOURCE_ID, RFB_SOURCE_ID
from sanida_fiscal.source_runtime_v1 import SourceStateStore


NOW = datetime(2026, 9, 13, 22, 0, tzinfo=timezone.utc)
RFB_FIXTURE = Path("tests/fixtures/sources/rfb_irrf_2026_fragment.html").read_bytes()
INSS_FIXTURE = Path("tests/fixtures/sources/inss_employee_2026_fragment.html").read_bytes()
SELIC_FIXTURE = Path("tests/fixtures/sources/bcb_selic_sgs432.json").read_bytes()
CDI_FIXTURE = Path("tests/fixtures/sources/bcb_cdi_sgs12.json").read_bytes()


def _runtime_root(tmp_path: Path) -> Path:
    return tmp_path / "evidence" / "source-runtime-v1"


def _run_source(tmp_path: Path, source_id: str, body: bytes, observed_at: datetime = NOW):
    def handler(request: httpx.Request):
        return httpx.Response(200, content=body, headers={"content-type": "text/html; charset=utf-8"})

    runtime_root = _runtime_root(tmp_path)
    return run_registered_source_pipeline(
        source_id=source_id,
        observed_at_utc=observed_at,
        snapshot_root=runtime_root / "snapshots",
        state_root=runtime_root / "state",
        candidate_root=runtime_root / "candidates",
        transport=httpx.MockTransport(handler),
        timeout_seconds=1,
        max_attempts=1,
        use_http_validators=False,
    )


def _run_financial(tmp_path: Path, source_id: str, body: bytes):
    def handler(request: httpx.Request):
        return httpx.Response(200, content=body, headers={"content-type": "application/json"})

    runtime_root = _runtime_root(tmp_path)
    return run_registered_financial_source_pipeline(
        source_id=source_id,
        observed_at_utc=NOW,
        snapshot_root=runtime_root / "snapshots",
        state_root=runtime_root / "state",
        candidate_root=runtime_root / "candidates",
        transport=httpx.MockTransport(handler),
        timeout_seconds=1,
        max_attempts=1,
        use_http_validators=False,
    )


def _build_artifact(tmp_path: Path):
    rfb = _run_source(tmp_path, RFB_SOURCE_ID, RFB_FIXTURE)
    inss = _run_source(tmp_path, INSS_SOURCE_ID, INSS_FIXTURE)
    selic = _run_financial(tmp_path, "BCB_SELIC_META_SGS_432", SELIC_FIXTURE)
    cdi = _run_financial(tmp_path, "BCB_CDI_DAILY_SGS_12", CDI_FIXTURE)

    financial = build_financial_reference_artifact(
        selic_run=selic,
        cdi_run=cdi,
        generated_at_utc=NOW.isoformat().replace("+00:00", "Z"),
    )
    financial_meta = financial["meta"]
    taxas_source_meta = {
        "origin": "local_file",
        "origin_ref": "taxas_bacen.json",
        "schema_version": financial["schema_version"],
        "generated_at_utc": financial_meta["generated_at_utc"],
        "source_meta": financial_meta["sources"],
    }

    artifact = build_legacy_dados_fiscais(
        rfb_run=rfb,
        inss_run=inss,
        expected_year=2026,
        taxas=financial["taxas"],
        taxas_source_meta=taxas_source_meta,
        generated_at_utc=NOW.isoformat().replace("+00:00", "Z"),
    )
    artifact_path = tmp_path / "dados_fiscais.json"
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    return _runtime_root(tmp_path), artifact_path, rfb, inss, selic, cdi


def test_production_evidence_resolves_all_four_source_chains(tmp_path: Path):
    runtime_root, artifact_path, rfb, inss, selic, cdi = _build_artifact(tmp_path)

    verified = verify_legacy_artifact_evidence(
        artifact_path=artifact_path,
        runtime_root=runtime_root,
    )

    assert set(verified) == {
        RFB_SOURCE_ID,
        INSS_SOURCE_ID,
        "BCB_SELIC_META_SGS_432",
        "BCB_CDI_DAILY_SGS_12",
    }
    assert verified[RFB_SOURCE_ID]["snapshot_sha256"] == rfb.candidate.snapshot_sha256
    assert verified[INSS_SOURCE_ID]["candidate_sha256"] == inss.state.last_candidate_sha256
    assert verified["BCB_SELIC_META_SGS_432"]["candidate_sha256"] == selic.state.last_candidate_sha256
    assert verified["BCB_CDI_DAILY_SGS_12"]["snapshot_sha256"] == cdi.candidate.snapshot_sha256


def test_production_evidence_rejects_missing_snapshot_file(tmp_path: Path):
    runtime_root, artifact_path, rfb, *_ = _build_artifact(tmp_path)
    snapshot = runtime_root / "snapshots" / rfb.state.last_parsed_snapshot_path
    snapshot.unlink()

    with pytest.raises(ProductionEvidenceError, match="snapshot evidence file does not exist"):
        verify_legacy_artifact_evidence(artifact_path=artifact_path, runtime_root=runtime_root)


def test_production_evidence_rejects_candidate_tampering(tmp_path: Path):
    runtime_root, artifact_path, rfb, *_ = _build_artifact(tmp_path)
    candidate = runtime_root / "candidates" / rfb.state.last_candidate_path
    candidate.write_text('{"tampered":true}', encoding="utf-8")

    with pytest.raises(ProductionEvidenceError, match="candidate sha256 mismatch"):
        verify_legacy_artifact_evidence(artifact_path=artifact_path, runtime_root=runtime_root)


def test_production_evidence_rejects_financial_provenance_tampering(tmp_path: Path):
    runtime_root, artifact_path, *_ = _build_artifact(tmp_path)
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["meta"]["sources"]["taxas"]["source_meta"]["cdi"]["candidate_sha256"] = "0" * 64
    artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

    with pytest.raises(ProductionEvidenceError, match="financial provenance verification failed"):
        verify_legacy_artifact_evidence(artifact_path=artifact_path, runtime_root=runtime_root)


def test_production_evidence_rejects_nonlocal_financial_origin(tmp_path: Path):
    runtime_root, artifact_path, *_ = _build_artifact(tmp_path)
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["meta"]["sources"]["taxas"]["origin"] = "remote_url"
    artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

    with pytest.raises(ProductionEvidenceError, match="local evidence-gated taxas_bacen"):
        verify_legacy_artifact_evidence(artifact_path=artifact_path, runtime_root=runtime_root)


def test_current_source_failure_preserves_last_good_evidence_pointers(tmp_path: Path):
    first = _run_source(tmp_path, RFB_SOURCE_ID, RFB_FIXTURE)
    runtime_root = _runtime_root(tmp_path)

    def failing_handler(request: httpx.Request):
        return httpx.Response(503)

    failed = run_registered_source_pipeline(
        source_id=RFB_SOURCE_ID,
        observed_at_utc=NOW + timedelta(hours=1),
        snapshot_root=runtime_root / "snapshots",
        state_root=runtime_root / "state",
        candidate_root=runtime_root / "candidates",
        transport=httpx.MockTransport(failing_handler),
        timeout_seconds=1,
        max_attempts=1,
        use_http_validators=False,
    )

    assert failed.state.consecutive_source_failures == 1
    assert failed.state.last_candidate_sha256 == first.state.last_candidate_sha256
    assert failed.state.last_candidate_path == first.state.last_candidate_path
    assert failed.state.last_parsed_snapshot_sha256 == first.state.last_parsed_snapshot_sha256
    assert failed.state.last_parsed_snapshot_path == first.state.last_parsed_snapshot_path
    assert failed.state.last_parsed_at_utc == first.state.last_parsed_at_utc
    assert failed.state.last_successful_parser_id == first.state.last_successful_parser_id
    assert failed.state.last_successful_parser_version == first.state.last_successful_parser_version


def test_state_store_materializes_candidate_and_last_good_paths(tmp_path: Path):
    run = _run_source(tmp_path, INSS_SOURCE_ID, INSS_FIXTURE)
    runtime_root = _runtime_root(tmp_path)
    state = SourceStateStore(runtime_root / "state").load(INSS_SOURCE_ID)

    assert state is not None
    assert state.schema_version == "1.1.0"
    assert state.last_parsed_snapshot_path == run.candidate.snapshot_path
    assert state.last_candidate_path is not None
    assert (runtime_root / "candidates" / state.last_candidate_path).is_file()


def test_production_workflow_uses_tracked_runtime_and_never_stages_unverified_artifact():
    workflow = Path(".github/workflows/main.yml").read_text(encoding="utf-8")

    assert "SFA_SOURCE_RUNTIME_ROOT: evidence/source-runtime-v1" in workflow
    assert "scripts/validate_production_evidence_v1.py" in workflow
    assert 'git add dados_fiscais.json "$SFA_SOURCE_RUNTIME_ROOT"' in workflow
    assert 'git restore --source=HEAD --staged --worktree dados_fiscais.json' in workflow
    assert 'git add "$SFA_SOURCE_RUNTIME_ROOT"' in workflow
    assert "Enforce production gate" in workflow


def test_production_persistence_policy_is_git_backed_and_has_no_auto_pruning():
    policy = json.loads(Path("docs/phase4-production-persistence-v1.json").read_text(encoding="utf-8"))

    assert policy["persistence_backend"]["kind"] == "git_tracked_repository_path"
    assert policy["persistence_backend"]["runtime_root"] == "evidence/source-runtime-v1"
    assert policy["retention_policy"]["automatic_pruning"] is False
    assert policy["integrity_gates"]["snapshot_file_sha256_must_equal_artifact_provenance"] is True
    assert policy["integrity_gates"]["candidate_file_sha256_must_equal_artifact_provenance"] is True


def test_legacy_year_qualified_artifact_provenance_remains_verifiable(tmp_path: Path):
    runtime_root, artifact_path, *_ = _build_artifact(tmp_path)
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["meta"]["sources"]["irrf"]["source_id"] = "RFB_IRRF_TABLE_2026"
    artifact["meta"]["sources"]["inss"]["source_id"] = "INSS_TABLE_2026"
    artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

    verified = verify_legacy_artifact_evidence(
        artifact_path=artifact_path,
        runtime_root=runtime_root,
    )

    assert "RFB_IRRF_TABLE_2026" in verified
    assert "INSS_TABLE_2026" in verified


def test_unknown_payroll_source_identity_is_rejected(tmp_path: Path):
    runtime_root, artifact_path, *_ = _build_artifact(tmp_path)
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["meta"]["sources"]["irrf"]["source_id"] = "RFB_IRRF_TABLE_UNKNOWN"
    artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

    with pytest.raises(ProductionEvidenceError, match="source_id mismatch"):
        verify_legacy_artifact_evidence(
            artifact_path=artifact_path,
            runtime_root=runtime_root,
        )
