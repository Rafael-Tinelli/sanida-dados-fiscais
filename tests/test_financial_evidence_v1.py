from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

import httpx
import pytest

from sanida_fiscal.financial_artifact_v1 import build_financial_reference_artifact
from sanida_fiscal.financial_evidence_v1 import (
    FinancialEvidenceError,
    PARSER_INCOMPATIBLE_LAST_GOOD_POLICY,
    verify_financial_artifact_evidence,
    verify_preserved_financial_last_good_artifact_evidence,
)
from sanida_fiscal.financial_source_catalog_v1 import run_registered_financial_source_pipeline
from sanida_fiscal.sources_v1 import ParseStatus


NOW = datetime(2026, 9, 13, 22, 0, tzinfo=timezone.utc)
LATER = datetime(2026, 9, 13, 23, 0, tzinfo=timezone.utc)
SELIC_FIXTURE = Path("tests/fixtures/sources/bcb_selic_sgs432.json").read_bytes()
CDI_FIXTURE = Path("tests/fixtures/sources/bcb_cdi_sgs12.json").read_bytes()


def _run(runtime_root: Path, source_id: str, body: bytes, *, observed_at=NOW):
    def handler(request: httpx.Request):
        return httpx.Response(200, content=body, headers={"content-type": "application/json"})

    return run_registered_financial_source_pipeline(
        source_id=source_id,
        observed_at_utc=observed_at,
        registry_path=Path("docs/financial-source-registry-v1.json"),
        snapshot_root=runtime_root / "snapshots",
        state_root=runtime_root / "state",
        candidate_root=runtime_root / "candidates",
        transport=httpx.MockTransport(handler),
        use_http_validators=False,
    )


def _materialize(tmp_path: Path):
    runtime_root = tmp_path / "runtime"
    selic = _run(runtime_root, "BCB_SELIC_META_SGS_432", SELIC_FIXTURE)
    cdi = _run(runtime_root, "BCB_CDI_DAILY_SGS_12", CDI_FIXTURE)
    artifact = build_financial_reference_artifact(
        selic_run=selic,
        cdi_run=cdi,
        generated_at_utc="2026-09-13T22:00:00Z",
    )
    artifact_path = tmp_path / "taxas_bacen.json"
    artifact_path.write_text(json.dumps(artifact, indent=2, ensure_ascii=False), encoding="utf-8")
    return runtime_root, artifact_path, selic, cdi


def test_financial_evidence_verifier_resolves_both_provenance_hashes(tmp_path: Path):
    runtime_root, artifact_path, _selic, _cdi = _materialize(tmp_path)
    verified = verify_financial_artifact_evidence(
        artifact_path=artifact_path,
        runtime_root=runtime_root,
    )
    assert set(verified) == {"BCB_SELIC_META_SGS_432", "BCB_CDI_DAILY_SGS_12"}


def test_financial_evidence_verifier_rejects_snapshot_corruption(tmp_path: Path):
    runtime_root, artifact_path, selic, _cdi = _materialize(tmp_path)
    snapshot_path = runtime_root / "snapshots" / selic.state.last_parsed_snapshot_path
    snapshot_path.write_bytes(b"corrupted")

    with pytest.raises(FinancialEvidenceError, match="sha256 mismatch"):
        verify_financial_artifact_evidence(
            artifact_path=artifact_path,
            runtime_root=runtime_root,
        )


def test_financial_evidence_verifier_rejects_provenance_state_divergence(tmp_path: Path):
    runtime_root, artifact_path, _selic, _cdi = _materialize(tmp_path)
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["meta"]["sources"]["cdi"]["candidate_sha256"] = "0" * 64
    artifact_path.write_text(json.dumps(artifact), encoding="utf-8")

    with pytest.raises(FinancialEvidenceError, match="candidate provenance differs"):
        verify_financial_artifact_evidence(
            artifact_path=artifact_path,
            runtime_root=runtime_root,
        )


def test_A05_parser_incompatible_preserves_last_good_but_blocks_new_consumption(tmp_path: Path):
    runtime_root, artifact_path, _selic, cdi_good = _materialize(tmp_path)
    previous_candidate_sha = cdi_good.state.last_candidate_sha256
    previous_candidate_path = cdi_good.state.last_candidate_path
    previous_parsed_snapshot_sha = cdi_good.state.last_parsed_snapshot_sha256

    incompatible = _run(
        runtime_root,
        "BCB_CDI_DAILY_SGS_12",
        b'{"new_layout":true}',
        observed_at=LATER,
    )
    assert incompatible.candidate is not None
    assert incompatible.candidate.status == ParseStatus.PARSER_INCOMPATIBLE
    assert incompatible.state.last_parse_status == ParseStatus.PARSER_INCOMPATIBLE
    assert incompatible.state.last_candidate_sha256 == previous_candidate_sha
    assert incompatible.state.last_candidate_path == previous_candidate_path
    assert incompatible.state.last_parsed_snapshot_sha256 == previous_parsed_snapshot_sha
    assert PARSER_INCOMPATIBLE_LAST_GOOD_POLICY == "preserve_auditable_block_new_consumption"

    preserved = verify_preserved_financial_last_good_artifact_evidence(
        artifact_path=artifact_path,
        runtime_root=runtime_root,
    )
    assert "BCB_CDI_DAILY_SGS_12" in preserved

    with pytest.raises(FinancialEvidenceError, match="PARSER_INCOMPATIBLE"):
        verify_financial_artifact_evidence(
            artifact_path=artifact_path,
            runtime_root=runtime_root,
        )


def test_taxas_workflow_requires_durable_evidence_gate():
    text = Path(".github/workflows/taxas.yml").read_text(encoding="utf-8")
    assert "SFA_SOURCE_RUNTIME_ROOT: evidence/source-runtime-v1" in text
    assert "validate_financial_evidence_v1.py" in text
    assert 'git add taxas_bacen.json "$SFA_SOURCE_RUNTIME_ROOT"' in text
    assert "git restore --source=HEAD --staged --worktree taxas_bacen.json" in text
