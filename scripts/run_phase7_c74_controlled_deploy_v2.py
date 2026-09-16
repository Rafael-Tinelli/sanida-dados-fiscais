#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.deployment_health_v2 import (
    ORIGIN_PENDING_STATUS,
    atomic_json,
    mark_origin_healthy_pending_external,
)
from scripts.run_phase7_c74_controlled_deploy import DeploymentError, run_controlled_deployment


def run_controlled_deployment_v2(*, authorized_commit: str, **kwargs) -> dict:
    state = run_controlled_deployment(**kwargs)
    if state.get("status") == "APPLIED_HEALTHY" and state.get("production_deployed") is True:
        state = mark_origin_healthy_pending_external(state, authorized_commit=authorized_commit)
        journal_dir = Path(kwargs["journal_dir"]).resolve()
        atomic_json(journal_dir / "deployment-state.json", state)
    return state


def main() -> int:
    parser = argparse.ArgumentParser(
        description="C7.4 maintenance deployment: origin health is intermediate until C7.5 external proof"
    )
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--site-root", type=Path, required=True)
    parser.add_argument("--wordpress-plugin-dir", type=Path, required=True)
    parser.add_argument("--c73-evidence", type=Path, required=True)
    parser.add_argument("--authorization-file", type=Path, required=True)
    parser.add_argument("--authorization-id", required=True)
    parser.add_argument("--authorized-commit", required=True)
    parser.add_argument("--journal-dir", type=Path, required=True)
    parser.add_argument("--health-base-url", required=True)
    args = parser.parse_args()

    try:
        result = run_controlled_deployment_v2(
            bundle_dir=args.bundle_dir,
            site_root=args.site_root,
            wordpress_plugin_dir=args.wordpress_plugin_dir,
            c73_evidence_path=args.c73_evidence,
            authorization_path=args.authorization_file,
            authorization_id=args.authorization_id,
            authorized_commit=args.authorized_commit,
            journal_dir=args.journal_dir,
            health_base_url=args.health_base_url,
        )
    except (DeploymentError, SystemExit, ValueError) as exc:
        print(json.dumps({"checkpoint": "C7.4", "status": "BLOCKED_BEFORE_WRITE", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 3

    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if result.get("status") == ORIGIN_PENDING_STATUS and result.get("production_deployed") is True:
        return 0
    if result.get("status") == "ROLLED_BACK":
        return 4
    return 5


if __name__ == "__main__":
    raise SystemExit(main())
