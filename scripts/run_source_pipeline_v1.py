from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from sanida_fiscal.inss_employee_v1 import (
    PARSER_ID as INSS_EMPLOYEE_PARSER_ID,
    PARSER_VERSION as INSS_EMPLOYEE_PARSER_VERSION,
    parse_inss_employee_2026_snapshot,
)
from sanida_fiscal.rfb_irrf_v1 import (
    PARSER_ID as RFB_IRRF_PARSER_ID,
    PARSER_VERSION as RFB_IRRF_PARSER_VERSION,
    parse_rfb_irrf_2026_snapshot,
)
from sanida_fiscal.source_runtime_v1 import SourceStateStore, run_source_pipeline
from sanida_fiscal.sources_v1 import HttpCollectorV1, RetryPolicy, SnapshotStore, load_source_registry


PARSERS = {
    "RFB_IRRF_TABLE_2026": (
        RFB_IRRF_PARSER_ID,
        RFB_IRRF_PARSER_VERSION,
        parse_rfb_irrf_2026_snapshot,
    ),
    "INSS_TABLE_2026": (
        INSS_EMPLOYEE_PARSER_ID,
        INSS_EMPLOYEE_PARSER_VERSION,
        parse_inss_employee_2026_snapshot,
    ),
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one Phase 4 source pipeline without publishing anything.")
    parser.add_argument("--source-id", default="RFB_IRRF_TABLE_2026", choices=sorted(PARSERS))
    parser.add_argument("--registry", type=Path, default=Path("docs/source-registry-v1.json"))
    parser.add_argument("--snapshot-root", type=Path, default=Path(".source-runtime/snapshots"))
    parser.add_argument("--state-root", type=Path, default=Path(".source-runtime/state"))
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--attempts", type=int, default=3)
    args = parser.parse_args()

    registry = load_source_registry(args.registry)
    if args.source_id not in registry:
        raise SystemExit(f"unknown source_id: {args.source_id}")

    source = registry[args.source_id]
    parser_id, parser_version, parser_fn = PARSERS[args.source_id]
    snapshot_store = SnapshotStore(args.snapshot_root)
    state_store = SourceStateStore(args.state_root)
    collector = HttpCollectorV1(
        snapshot_store=snapshot_store,
        retry_policy=RetryPolicy(max_attempts=args.attempts, timeout_seconds=args.timeout),
        headers={
            "User-Agent": "SanidaFiscaisBot/4.0 (+https://sanida.com.br)",
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
        },
    )

    result = run_source_pipeline(
        source=source,
        collector=collector,
        snapshot_store=snapshot_store,
        state_store=state_store,
        observed_at_utc=utc_now(),
        parser_id=parser_id,
        parser_version=parser_version,
        parser=parser_fn,
    )

    summary = {
        "source_id": source.source_id,
        "source_url": source.url,
        "collection_status": result.collection.status.value,
        "parse_status": result.candidate.status.value if result.candidate else result.state.last_parse_status.value if result.state.last_parse_status else None,
        "snapshot_sha256": result.state.last_collected_snapshot_sha256,
        "candidate_sha256": result.state.last_candidate_sha256,
        "raw_snapshot_unchanged": result.raw_snapshot_unchanged,
        "candidate_fingerprint_unchanged": result.candidate_fingerprint_unchanged,
        "consecutive_source_failures": result.state.consecutive_source_failures,
        "consecutive_parser_failures": result.state.consecutive_parser_failures,
        "state_path": str(state_store.path_for(source.source_id)),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
