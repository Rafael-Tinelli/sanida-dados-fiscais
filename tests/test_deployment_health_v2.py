from __future__ import annotations

from pathlib import Path

import pytest

from scripts.deployment_health_v2 import (
    DeliveryHealthError,
    FINAL_HEALTHY_STATUS,
    ORIGIN_PENDING_STATUS,
    finalize_external_health,
    mark_origin_healthy_pending_external,
)

ROOT = Path(__file__).resolve().parents[1]
DEPLOY_V2 = ROOT / "scripts/run_phase7_c74_controlled_deploy_v2.py"
FINALIZE_V2 = ROOT / "scripts/finalize_phase7_c75_external_health_v2.py"
DOC = ROOT / "docs/post-remake-maintenance-deployment-v2.md"

AUTHORIZED_COMMIT = "a" * 40


def _legacy_success() -> dict:
    return {
        "checkpoint": "C7.4",
        "status": "APPLIED_HEALTHY",
        "production_deployed": True,
        "rollback_performed": False,
        "completed_at_utc": "2026-09-16T20:00:00+00:00",
        "health": {"fiscal_health": {"status": "healthy"}},
    }


def _external_pass(commit: str = AUTHORIZED_COMMIT) -> dict:
    assets = []
    for index in range(2):
        sha = (str(index + 1) * 64)[:64]
        assets.append(
            {
                "asset": f"asset-{index}.js",
                "expected_http_status": 200,
                "public_http_status": 200,
                "expected_sha256": sha,
                "public_sha256": sha,
                "match": True,
            }
        )
    return {
        "schema_version": "1.0.0",
        "checkpoint": "C7.5",
        "mode": "external_client_public_delivery_validation",
        "status": "PASS",
        "production_mutated": False,
        "authorized_commit": commit,
        "assets_expected": 2,
        "assets_matching": 2,
        "block_reasons": [],
        "observed_at_utc": "2026-09-16T20:05:00Z",
        "assets": assets,
    }


def test_origin_health_is_intermediate_not_final() -> None:
    pending = mark_origin_healthy_pending_external(_legacy_success(), authorized_commit=AUTHORIZED_COMMIT)
    assert pending["status"] == ORIGIN_PENDING_STATUS
    assert pending["legacy_origin_status"] == "APPLIED_HEALTHY"
    assert pending["production_deployed"] is True
    assert pending["origin_health_verified"] is True
    assert pending["external_delivery_verified"] is False
    assert pending["final_health_status"] == "PENDING_EXTERNAL_PROOF"


def test_external_pass_is_required_for_final_healthy() -> None:
    pending = mark_origin_healthy_pending_external(_legacy_success(), authorized_commit=AUTHORIZED_COMMIT)
    blocked = _external_pass()
    blocked["status"] = "BLOCKED"
    blocked["assets_matching"] = 1
    blocked["block_reasons"] = ["public_asset_mismatch:asset-1.js"]
    with pytest.raises(DeliveryHealthError):
        finalize_external_health(pending, blocked)
    assert pending["status"] == ORIGIN_PENDING_STATUS


def test_external_evidence_must_be_bound_to_same_authorized_commit() -> None:
    pending = mark_origin_healthy_pending_external(_legacy_success(), authorized_commit=AUTHORIZED_COMMIT)
    with pytest.raises(DeliveryHealthError):
        finalize_external_health(pending, _external_pass("b" * 40))


def test_matching_external_bytes_promote_to_final_healthy() -> None:
    pending = mark_origin_healthy_pending_external(_legacy_success(), authorized_commit=AUTHORIZED_COMMIT)
    final = finalize_external_health(pending, _external_pass())
    assert final["status"] == FINAL_HEALTHY_STATUS
    assert final["external_delivery_verified"] is True
    assert final["final_health_status"] == "HEALTHY"
    assert final["external_delivery"]["assets_matching"] == 2


def test_post_remake_operational_surface_enforces_two_stage_health() -> None:
    deploy = DEPLOY_V2.read_text(encoding="utf-8")
    finalize = FINALIZE_V2.read_text(encoding="utf-8")
    doc = DOC.read_text(encoding="utf-8")
    assert "mark_origin_healthy_pending_external" in deploy
    assert "APPLIED_ORIGIN_HEALTHY_PENDING_EXTERNAL" not in deploy or "ORIGIN_PENDING_STATUS" in deploy
    assert "finalize_external_health" in finalize
    assert "APPLIED_EXTERNALLY_HEALTHY" in doc
    assert "APPLIED_ORIGIN_HEALTHY_PENDING_EXTERNAL" in doc
    assert "não é estado final saudável" in doc
