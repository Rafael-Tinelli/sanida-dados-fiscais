#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import tempfile


class DeliveryHealthError(RuntimeError):
    pass


ORIGIN_PENDING_STATUS = "APPLIED_ORIGIN_HEALTHY_PENDING_EXTERNAL"
FINAL_HEALTHY_STATUS = "APPLIED_EXTERNALLY_HEALTHY"


def atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    tmp = Path(raw)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink()


def mark_origin_healthy_pending_external(state: dict, *, authorized_commit: str) -> dict:
    if not isinstance(state, dict):
        raise DeliveryHealthError("deployment state must be an object")
    if state.get("status") != "APPLIED_HEALTHY" or state.get("production_deployed") is not True:
        raise DeliveryHealthError("legacy C7.4 result is not an applied origin-healthy deployment")
    commit = str(authorized_commit or "").strip()
    if len(commit) != 40 or any(ch not in "0123456789abcdef" for ch in commit.lower()):
        raise DeliveryHealthError("authorized_commit must be a 40-character hexadecimal commit SHA")

    out = dict(state)
    out["legacy_origin_status"] = state["status"]
    out["status"] = ORIGIN_PENDING_STATUS
    out["authorized_commit"] = commit.lower()
    out["origin_health_verified"] = True
    out["external_delivery_verified"] = False
    out["final_health_status"] = "PENDING_EXTERNAL_PROOF"
    out["origin_health_completed_at_utc"] = state.get("completed_at_utc")
    out["external_health_completed_at_utc"] = None
    out["external_delivery"] = None
    return out


def validate_external_evidence(state: dict, evidence: dict) -> dict:
    if state.get("status") != ORIGIN_PENDING_STATUS:
        raise DeliveryHealthError("deployment is not awaiting external delivery proof")
    if state.get("production_deployed") is not True or state.get("origin_health_verified") is not True:
        raise DeliveryHealthError("origin deployment has not been verified")
    if state.get("external_delivery_verified") is not False:
        raise DeliveryHealthError("external delivery state is not pending")
    if not isinstance(evidence, dict):
        raise DeliveryHealthError("external evidence must be an object")
    if evidence.get("checkpoint") != "C7.5":
        raise DeliveryHealthError("external evidence checkpoint must be C7.5")
    if evidence.get("mode") != "external_client_public_delivery_validation":
        raise DeliveryHealthError("external evidence mode is not the public-delivery probe")
    if evidence.get("status") != "PASS":
        raise DeliveryHealthError("external delivery evidence is not PASS")
    if evidence.get("production_mutated") is not False:
        raise DeliveryHealthError("external delivery probe must be read-only")
    if str(evidence.get("authorized_commit") or "").lower() != str(state.get("authorized_commit") or "").lower():
        raise DeliveryHealthError("external evidence is bound to a different authorized commit")

    expected = evidence.get("assets_expected")
    matching = evidence.get("assets_matching")
    if not isinstance(expected, int) or expected <= 0:
        raise DeliveryHealthError("external evidence has invalid assets_expected")
    if matching != expected:
        raise DeliveryHealthError("not all externally delivered assets match")
    if evidence.get("block_reasons") not in ([], None):
        raise DeliveryHealthError("external evidence contains block reasons")

    assets = evidence.get("assets")
    if not isinstance(assets, list) or len(assets) != expected:
        raise DeliveryHealthError("external evidence asset inventory is incomplete")
    names: set[str] = set()
    for item in assets:
        if not isinstance(item, dict):
            raise DeliveryHealthError("external evidence contains an invalid asset record")
        name = str(item.get("asset") or "")
        if not name or name in names:
            raise DeliveryHealthError("external evidence contains an empty or duplicate asset name")
        names.add(name)
        expected_sha = str(item.get("expected_sha256") or "")
        public_sha = str(item.get("public_sha256") or "")
        if len(expected_sha) != 64 or len(public_sha) != 64 or expected_sha != public_sha:
            raise DeliveryHealthError(f"external byte mismatch for {name}")
        if item.get("match") is not True:
            raise DeliveryHealthError(f"external match flag is false for {name}")
        if item.get("public_http_status") != 200 or item.get("expected_http_status") != 200:
            raise DeliveryHealthError(f"external HTTP validation failed for {name}")

    return {
        "status": "PASS",
        "authorized_commit": str(evidence["authorized_commit"]).lower(),
        "assets_expected": expected,
        "assets_matching": matching,
        "observed_at_utc": evidence.get("observed_at_utc"),
        "production_mutated": False,
    }


def finalize_external_health(state: dict, evidence: dict) -> dict:
    summary = validate_external_evidence(state, evidence)
    out = dict(state)
    out["status"] = FINAL_HEALTHY_STATUS
    out["external_delivery_verified"] = True
    out["final_health_status"] = "HEALTHY"
    out["external_health_completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    out["external_delivery"] = summary
    return out
