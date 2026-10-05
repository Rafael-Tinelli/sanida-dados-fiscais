from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0,str(ROOT))

from sanida_fiscal.authority_evidence_v12 import collect_authority_evidence
from sanida_fiscal.financial_series_v1 import (
    SELIC_SOURCE_ID,
    financial_series_window,
    run_financial_history_source_segments,
)



def main() -> int:
    now=datetime.now(timezone.utc).replace(microsecond=0)
    report={"observed_at_utc":now.isoformat().replace("+00:00","Z"),"authority":{},"selic_history":{}}
    failures=[]
    with tempfile.TemporaryDirectory() as tmp:
        tmp_root=Path(tmp)
        try:
            bundle=collect_authority_evidence(
                source_registry_path=ROOT/"docs/source-registry-v1.json",
                rule_inventory_path=ROOT/"docs/rule-inventory-v1.json",
                snapshot_root=tmp_root/"authority",
                observed_at_utc=now,
            )
            report["authority"]={
                "status":"PASS",
                "source_ids":sorted(bundle.evidence_by_source),
                "selected_rule_count":len(bundle.selected_source_by_rule),
            }
        except Exception as exc:
            report["authority"]={"status":"FAIL","error_type":type(exc).__name__,"error":str(exc)}
            failures.append("authority")

        try:
            as_of=now.astimezone(ZoneInfo("America/Sao_Paulo")).date()
            start,end=financial_series_window(as_of)
            runs=run_financial_history_source_segments(
                source_id=SELIC_SOURCE_ID,
                observed_at_utc=now,
                start_date=start,
                end_date=end,
                registry_path=ROOT/"docs/financial-source-registry-v1.json",
                snapshot_root=tmp_root/"series/snapshots",
                state_root=tmp_root/"series/state",
                candidate_root=tmp_root/"series/candidates",
                timeout_seconds=25.0,
                max_attempts=2,
                months_per_segment=12,
            )
            report["selic_history"]={
                "status":"PASS",
                "segment_count":len(runs),
                "start_date":start.isoformat(),
                "end_date":end.isoformat(),
                "segments":[
                    {
                        "url":run.collection.source_url,
                        "http_status":run.collection.http_status,
                        "snapshot_sha256":run.candidate.snapshot_sha256 if run.candidate else None,
                        "candidate_sha256":run.state.last_candidate_sha256,
                    }
                    for run in runs
                ],
            }
        except Exception as exc:
            report["selic_history"]={"status":"FAIL","error_type":type(exc).__name__,"error":str(exc)}
            failures.append("selic_history")

    out=ROOT/".validation/recovery-live-sources.json"
    out.parent.mkdir(parents=True,exist_ok=True)
    out.write_text(json.dumps(report,ensure_ascii=False,indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print(json.dumps(report,ensure_ascii=False,sort_keys=True))
    return 1 if failures else 0


if __name__=="__main__":
    raise SystemExit(main())
