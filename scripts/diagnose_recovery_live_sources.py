from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.authority_evidence_v12 import collect_authority_evidence
from sanida_fiscal.financial_series_v1 import (
    CDI_SOURCE_ID,
    SELIC_SOURCE_ID,
    build_financial_series_artifact_chunked,
    financial_series_window,
    run_financial_history_source_pipeline_chunked,
    validate_financial_series_artifact,
)


def main() -> int:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    report = {
        "observed_at_utc": now.isoformat().replace("+00:00", "Z"),
        "authority": {},
        "financial_history": {},
    }
    failures: list[str] = []

    with tempfile.TemporaryDirectory() as tmp:
        tmp_root = Path(tmp)

        try:
            bundle = collect_authority_evidence(
                source_registry_path=ROOT / "docs/source-registry-v1.json",
                rule_inventory_path=ROOT / "docs/rule-inventory-v1.json",
                snapshot_root=tmp_root / "authority",
                observed_at_utc=now,
            )
            report["authority"] = {
                "status": "PASS",
                "source_ids": sorted(bundle.evidence_by_source),
                "selected_rule_count": len(bundle.selected_source_by_rule),
                "selected_source_by_rule": dict(sorted(bundle.selected_source_by_rule.items())),
            }
        except Exception as exc:
            report["authority"] = {
                "status": "FAIL",
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
            failures.append("authority")

        try:
            as_of = now.astimezone(ZoneInfo("America/Sao_Paulo")).date()
            start, end = financial_series_window(as_of)
            bundles = {}
            source_report = {}

            for source_id in (SELIC_SOURCE_ID, CDI_SOURCE_ID):
                source_bundle = run_financial_history_source_pipeline_chunked(
                    source_id=source_id,
                    observed_at_utc=now,
                    start_date=start,
                    end_date=end,
                    registry_path=ROOT / "docs/financial-source-registry-v1.json",
                    snapshot_root=tmp_root / "series/snapshots",
                    state_root=tmp_root / "series/state",
                    candidate_root=tmp_root / "series/candidates",
                    timeout_seconds=35.0,
                    max_attempts=3,
                    months_per_chunk=12,
                )
                bundles[source_id] = source_bundle
                source_report[source_id] = {
                    "status": "PASS",
                    "segment_count": source_bundle.meta["segment_count"],
                    "reused_segment_count": source_bundle.meta["reused_segment_count"],
                    "segments": [
                        {
                            "start_date": row["start_date"],
                            "end_date": row["end_date"],
                            "url": row["url"],
                            "http_status": row["http_code"],
                            "snapshot_sha256": row["snapshot_sha256"],
                            "candidate_sha256": row["candidate_sha256"],
                            "reused_immutable_segment": row["reused_immutable_segment"],
                        }
                        for row in source_bundle.meta["segments"]
                    ],
                }

            artifact = build_financial_series_artifact_chunked(
                selic_bundle=bundles[SELIC_SOURCE_ID],
                cdi_bundle=bundles[CDI_SOURCE_ID],
                generated_at_utc=now.isoformat().replace("+00:00", "Z"),
                as_of_date=as_of,
            )
            ok, errors = validate_financial_series_artifact(artifact, as_of_date=as_of)
            if not ok:
                raise RuntimeError(f"chunked artifact validation failed: {errors}")
            report["financial_history"] = {
                "status": "PASS",
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "points": len(artifact["points"]),
                "latest_month": artifact["points"][-1]["month"],
                "sources": source_report,
            }
        except Exception as exc:
            report["financial_history"] = {
                "status": "FAIL",
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
            failures.append("financial_history")

    out = ROOT / ".validation/recovery-live-sources.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
