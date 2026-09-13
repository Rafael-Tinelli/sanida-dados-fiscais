from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from sanida_fiscal.source_catalog_v1 import run_registered_source_pipeline
from sanida_fiscal.source_runtime_v1 import SourceStateStore


SUPPORTED_SOURCE_IDS = (
    "RFB_IRRF_TABLE_2026",
    "INSS_TABLE_2026",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one Phase 4 source pipeline without publishing anything.")
    parser.add_argument("--source-id", default="RFB_IRRF_TABLE_2026", choices=SUPPORTED_SOURCE_IDS)
    parser.add_argument("--registry", type=Path, default=Path("docs/source-registry-v1.json"))
    parser.add_argument("--snapshot-root", type=Path, default=Path(".source-runtime/snapshots"))
    parser.add_argument("--state-root", type=Path, default=Path(".source-runtime/state"))
    parser.add_argument("--candidate-root", type=Path, default=Path(".source-runtime/candidates"))
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--attempts", type=int, default=3)
    args = parser.parse_args()

    result = run_registered_source_pipeline(
        source_id=args.source_id,
        observed_at_utc=utc_now(),
        registry_path=args.registry,
        snapshot_root=args.snapshot_root,
        state_root=args.state_root,
        candidate_root=args.candidate_root,
        timeout_seconds=args.timeout,
        max_attempts=args.attempts,
    )

    state_store = SourceStateStore(args.state_root)
    summary = {
        "source_id": args.source_id,
        "collection_status": result.collection.status.value,
        "parse_status": result.candidate.status.value if result.candidate else result.state.last_parse_status.value if result.state.last_parse_status else None,
        "snapshot_sha256": result.state.last_collected_snapshot_sha256,
        "last_parsed_snapshot_sha256": result.state.last_parsed_snapshot_sha256,
        "last_parsed_snapshot_path": result.state.last_parsed_snapshot_path,
        "candidate_sha256": result.state.last_candidate_sha256,
        "candidate_path": result.state.last_candidate_path,
        "raw_snapshot_unchanged": result.raw_snapshot_unchanged,
        "candidate_fingerprint_unchanged": result.candidate_fingerprint_unchanged,
        "consecutive_source_failures": result.state.consecutive_source_failures,
        "consecutive_parser_failures": result.state.consecutive_parser_failures,
        "state_path": str(state_store.path_for(args.source_id)),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
