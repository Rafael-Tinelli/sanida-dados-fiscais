from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import httpx
import pytest

from sanida_fiscal.legacy_artifact_v1 import build_legacy_dados_fiscais
from sanida_fiscal.production_evidence_v1 import (
    ProductionEvidenceError,
    verify_legacy_artifact_evidence,
)
from sanida_fiscal.source_catalog_v1 import run_registered_source_pipeline
from sanida_fiscal.source_runtime_v1 import SourceStateStore


NOW = datetime(2026, 9, 13, 22, 0, tzinfo=timezone.utc)
RFB_FIXTURE = Path("tests/fixtures/sources/rfb_irrf_2026_fragment.html").read_bytes()
INSS_FIXTURE = Path("tests/fixtures/sources/inss_employee_2026_fragment.html").read_bytes()


def _run_source(tmp_path: Path, source_id: str, body: bytes, observed_at: datetime = NOW):
    def handler(request: httpx.Request):
        return httpx.Response(200, content=body, headers={"content-type": "text/html; charset=utf-8"})

    runtime_root = tmp_path / "evidence" / "source-runtime-v1"
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


def _build_artifact(tmp_path: Path):
    rfb = _run_source(tmp_path, "RFB_IRRF_TABLE_2026", RFB_FIXTURE)
    inss = _run_source(tmp_path, "INSS_TABLE_2026", INSS_FIXTURE)
    artifact = build_legacy_dados_fiscais(
        rfb_run=rfb,
        inss_run=inss,
        expected_year=2026,
        taxas={"selic": 14.25, "cdi": 14.15},
        taxas_source_meta={"origin": "test"},
        generated_at_utc=NOW.isoformat().replace("+00:00", "Z"),
    )
    artifact_path = tmp_path / "dados_fiscais.json"
    artifact_path.write_text(json.dumps(artifact, ensure_ascii=False, indent=2), encoding="utf-8")
    return tmp_path / "evidence" / "source-runtime-v1", artifact_path, rfb, inss


def test_production_evidence_resolves_snapshot_candidate_and_state(tmp_path: Path):
    runtime_root, artifact_path, rfb, inss = _build_artifact(tmp_path)

    verified = verify_legacy_artifact_evidence(
        artifact_path=artifact_path,
        runtime_root=runtime_root,
    )

    assert set(verified) == {"RFB_IRRF_TABLE_2026", "INSS_TABLE_2026"}
    assert verified["RFB_IRRF_TABLE_2026"]["snapshot_sha256"] == rfb.candidate.snapshot_sha256
    assert verified["INSS_TABLE_2026"]["candidate_sha256"] == inss.state.last_candidate_sha256
    assert (runtime_root / "candidates" / rfb.state.last_candidate_path).is_file()
    assert (runtime_root / "snapshots" / inss.state.last_parsed_snapshot_path).is_file()


def test_production_evidence_rejects_missing_snapshot_file(tmp_path: Path):
    runtime_root, artifact_path, rfb, _ = _build_artifact(tmp_path)
    snapshot = runtime_root / "snapshots" / rfb.state.last_parsed_snapshot_path
    snapshot.unlink()

    with pytest.raises(ProductionEvidenceError, match="snapshot evidence file does not exist"):
        verify_legacy_artifact_evidence(artifact_path=artifact_path, runtime_root=runtime_root)


def test_production_evidence_rejects_candidate_tampering(tmp_path: Path):
    runtime_root, artifact_path, rfb, _ = _build_artifact(tmp_path)
    candidate = runtime_root / "candidates" / rfb.state.last_candidate_path
    candidate.write_text('{"tampered":true}', encoding="utf-8")

    with pytest.raises(ProductionEvidenceError, match="candidate sha256 mismatch"):
        verify_legacy_artifact_evidence(artifact_path=artifact_path, runtime_root=runtime_root)


def test_current_source_failure_preserves_last_good_evidence_pointers(tmp_path: Path):
    first = _run_source(tmp_path, "RFB_IRRF_TABLE_2026", RFB_FIXTURE)
    runtime_root = tmp_path / "evidence" / "source-runtime-v1"

    def failing_handler(request: httpx.Request):
        return httpx.Response(503)

    failed = run_registered_source_pipeline(
        source_id="RFB_IRRF_TABLE_2026",
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
    run = _run_source(tmp_path, "INSS_TABLE_2026", INSS_FIXTURE)
    runtime_root = tmp_path / "evidence" / "source-runtime-v1"
    state = SourceStateStore(runtime_root / "state").load("INSS_TABLE_2026")

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
