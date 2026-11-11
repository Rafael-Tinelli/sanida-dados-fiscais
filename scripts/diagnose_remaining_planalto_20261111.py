"""Read-only real collector checks for all remaining fiscal Planalto norms."""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
import json

from bs4 import BeautifulSoup
from sanida_fiscal.authority_evidence_v12 import AUTHORITY_HEADERS, _validate_authority_identity, _normalize_visible_text
from sanida_fiscal.sources_v1 import HttpCollectorV1, RetryPolicy, SnapshotStore, SourceSpec, CollectionStatus

SOURCES = {
    "CAMARA_DECRETO_10854_2021": {
        "url": "https://www2.camara.leg.br/legin/fed/decret/2021/decreto-10854-10-novembro-2021-791950-normaatualizada-pe.html",
        "groups": [["decreto", "10.854"], ["2021"], ["art. 76", "art. 78", "art. 80", "art. 82"]],
        "min_bytes": 20000,
    },
    "CAMARA_LEI_4090_1962": {
        "url": "https://www2.camara.leg.br/legin/fed/lei/1960-1969/lei-4090-13-julho-1962-353863-normaatualizada-pl.html",
        "groups": [["lei", "4.090"], ["1962"], ["1/12", "15 (quinze)", "art. 3"]],
        "min_bytes": 3000,
    },
    "CAMARA_LEI_4749_1965": {
        "url": "https://www2.camara.leg.br/legin/fed/lei/1960-1969/lei-4749-12-agosto-1965-377862-publicacaooriginal-1-pl.html",
        "groups": [["lei", "4.749"], ["1965"], ["art. 2", "fevereiro", "novembro", "ferias", "janeiro"]],
        "min_bytes": 3000,
    },
    "CAMARA_LEI_15270_2025": {
        "url": "https://www2.camara.leg.br/legin/fed/lei/2025/lei-15270-26-novembro-2025-798354-publicacaooriginal-177117-pl.html",
        "groups": [["lei", "15.270"], ["2025"], ["art. 3", "2026", "5.000", "7.350", "decimo terceiro salario"]],
        "min_bytes": 10000,
    },
}


def main() -> int:
    rows = []
    with TemporaryDirectory() as tmp:
        store = SnapshotStore(Path(tmp))
        collector = HttpCollectorV1(
            snapshot_store=store,
            retry_policy=RetryPolicy(max_attempts=1, timeout_seconds=25.0),
            headers=AUTHORITY_HEADERS,
        )
        for source_id, meta in SOURCES.items():
            source = SourceSpec(
                source_id=source_id, url=meta["url"],
                role="normative_primary", machine_readability="html",
            )
            row = {"source_id": source_id, "url": source.url}
            try:
                result = collector.collect(source, observed_at_utc=datetime.now(timezone.utc))
                row.update(status=result.status.value, http_status=result.http_status)
                if result.status != CollectionStatus.COLLECTED or result.snapshot is None:
                    row["error"] = result.error_detail or "unavailable"
                else:
                    body = store.read(result.snapshot)
                    _validate_authority_identity(
                        source_id=source_id,
                        metadata={"identity_marker_groups": meta["groups"]},
                        body=body,
                        media_type=result.snapshot.media_type,
                    )
                    assert len(body) >= meta["min_bytes"], "unusually short document"
                    assert body.lstrip().lower().startswith((b"<!doctype", b"<html", b"<?xml")), "unexpected media"
                    assert result.snapshot.source_url == source.url, "provenance URL mismatch"
                    assert sha256(body).hexdigest() == result.snapshot.sha256, "digest mismatch"
                    row.update(
                        result="PASS", bytes=len(body), sha256=result.snapshot.sha256,
                        title=(BeautifulSoup(body, "html.parser").title.get_text(" ", strip=True)[:120]
                               if BeautifulSoup(body, "html.parser").title else ""),
                    )
            except Exception as exc:
                row.update(result="FAIL", error=type(exc).__name__ + ": " + str(exc)[:300])
            rows.append(row)
            print("OFFICIAL_NORM", json.dumps(row, ensure_ascii=False, sort_keys=True), flush=True)

    report = {
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "results": rows, "all_pass": all(row.get("result") == "PASS" for row in rows),
    }
    out = Path(".validation/remaining-planalto-sources.json")
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("SUMMARY", json.dumps({"all_pass": report["all_pass"], "sources": len(rows)}))
    return 0 if report["all_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
