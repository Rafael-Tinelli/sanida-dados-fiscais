#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.deployment_health_v2 import DeliveryHealthError, validate_external_evidence

EXPECTED_PAGES = {"H25", "H26", "H27", "H28", "H29"}


class ExternalEvidenceError(RuntimeError):
    pass


def load_json(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ExternalEvidenceError(f"invalid evidence JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise ExternalEvidenceError("evidence must be a JSON object")
    return data


def validate(*, evidence: dict, authorized_commit: str, release_id: str, cache_key: str) -> dict:
    if evidence.get("schema_version") != "1.1.0":
        raise ExternalEvidenceError("external evidence schema_version must be 1.1.0")
    if evidence.get("collector") != "operator_external_client":
        raise ExternalEvidenceError("external evidence collector is not operator_external_client")
    if str(evidence.get("authorized_commit") or "").lower() != authorized_commit.lower():
        raise ExternalEvidenceError("external evidence is bound to another commit")
    if evidence.get("expected_release_id") != release_id:
        raise ExternalEvidenceError("external evidence release binding mismatch")
    if evidence.get("expected_cache_key") != cache_key:
        raise ExternalEvidenceError("external evidence cache-key binding mismatch")

    # Reuse the canonical C7.5 asset-integrity contract: 8/8 assets, HTTP 200,
    # exact SHA equality, PASS and no block reasons.
    pending_state = {
        "status": "APPLIED_ORIGIN_HEALTHY_PENDING_EXTERNAL",
        "production_deployed": True,
        "origin_health_verified": True,
        "external_delivery_verified": False,
        "authorized_commit": authorized_commit.lower(),
    }
    try:
        asset_summary = validate_external_evidence(pending_state, evidence)
    except DeliveryHealthError as exc:
        raise ExternalEvidenceError(str(exc)) from exc

    pages = evidence.get("pages")
    if not isinstance(pages, dict) or set(pages) != EXPECTED_PAGES:
        raise ExternalEvidenceError("external evidence must contain exactly H25-H29")
    for name in sorted(EXPECTED_PAGES):
        page = pages[name]
        if not isinstance(page, dict):
            raise ExternalEvidenceError(f"invalid page evidence for {name}")
        if page.get("http_status") != 200:
            raise ExternalEvidenceError(f"{name} public HTTP status is not 200")
        if page.get("block_reasons") not in ([], None):
            raise ExternalEvidenceError(f"{name} contains public-delivery block reasons")
        count = page.get("cache_key_occurrences")
        if not isinstance(count, int) or count < 1:
            raise ExternalEvidenceError(f"{name} cache key was not observed")
        if name == "H26" and count != 2:
            raise ExternalEvidenceError("H26 must expose the stable cache key exactly twice")

    fiscal = evidence.get("fiscal")
    if not isinstance(fiscal, dict):
        raise ExternalEvidenceError("fiscal evidence is missing")
    health = fiscal.get("health") or {}
    release = fiscal.get("release") or {}
    legacy = fiscal.get("legacy_folha") or {}
    if health.get("http_status") != 200 or health.get("status") != "healthy" or health.get("release_id") != release_id or health.get("pass") is not True:
        raise ExternalEvidenceError("public fiscal-health validation failed")
    if release.get("http_status") != 200 or release.get("release_id") != release_id or release.get("pass") is not True:
        raise ExternalEvidenceError("public fiscal-release validation failed")
    if legacy.get("http_status") != 410 or legacy.get("pass") is not True:
        raise ExternalEvidenceError("legacy /folha endpoint is not HTTP 410")

    return {
        "status": "PASS",
        "authorized_commit": authorized_commit.lower(),
        "release_id": release_id,
        "cache_key": cache_key,
        "assets_expected": asset_summary["assets_expected"],
        "assets_matching": asset_summary["assets_matching"],
        "pages_verified": len(EXPECTED_PAGES),
        "fiscal_verified": True,
        "production_mutated": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate C7.5 operator-collected external public-delivery evidence")
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--authorized-commit", required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--cache-key", required=True)
    args = parser.parse_args()
    try:
        summary = validate(
            evidence=load_json(args.evidence),
            authorized_commit=args.authorized_commit,
            release_id=args.release_id,
            cache_key=args.cache_key,
        )
    except (ExternalEvidenceError, DeliveryHealthError) as exc:
        print(json.dumps({"checkpoint": "C7.5", "status": "BLOCKED", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 3
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    print("C7.5_OPERATOR_EXTERNAL_EVIDENCE=PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
