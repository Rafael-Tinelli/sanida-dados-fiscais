from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from sanida_fiscal.financial_series_v1 import (
    CDI_SOURCE_ID,
    FINANCIAL_SERIES_TIMEZONE,
    SELIC_SOURCE_ID,
    FinancialSeriesBoundaryError,
    build_financial_series_artifact,
    financial_series_window,
    run_financial_history_source_pipeline,
    validate_financial_series_artifact,
)


OUTPUT_FILE = Path("taxas_bacen_series.json")
FINANCIAL_SOURCE_REGISTRY = Path("docs/financial-source-registry-v1.json")
SOURCE_RUNTIME_ROOT = Path(
    os.getenv("SFA_FINANCIAL_SERIES_RUNTIME_ROOT", "evidence/financial-series-v1").strip()
)
TIMEOUT = int(os.getenv("SFA_TIMEOUT", "25").strip())
RETRIES = int(os.getenv("SFA_RETRIES", "3").strip())

HEADERS = {
    "User-Agent": "SanidaFiscaisBot/4.0 (+https://sanida.com.br)",
    "Accept": "application/json",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
}


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def write_json_atomic(data: dict) -> None:
    tmp = OUTPUT_FILE.with_suffix(OUTPUT_FILE.suffix + ".tmp")
    tmp.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp, OUTPUT_FILE)


def main() -> None:
    observed_at_utc = now_utc()
    as_of_date = observed_at_utc.astimezone(FINANCIAL_SERIES_TIMEZONE).date()
    start_date, end_date = financial_series_window(as_of_date)

    try:
        runs = {}
        for source_id in (SELIC_SOURCE_ID, CDI_SOURCE_ID):
            runs[source_id] = run_financial_history_source_pipeline(
                source_id=source_id,
                observed_at_utc=observed_at_utc,
                start_date=start_date,
                end_date=end_date,
                registry_path=FINANCIAL_SOURCE_REGISTRY,
                snapshot_root=SOURCE_RUNTIME_ROOT / "snapshots",
                state_root=SOURCE_RUNTIME_ROOT / "state",
                candidate_root=SOURCE_RUNTIME_ROOT / "candidates",
                timeout_seconds=float(TIMEOUT),
                max_attempts=RETRIES,
                headers=HEADERS,
            )

        artifact = build_financial_series_artifact(
            selic_run=runs[SELIC_SOURCE_ID],
            cdi_run=runs[CDI_SOURCE_ID],
            generated_at_utc=observed_at_utc.isoformat().replace("+00:00", "Z"),
            as_of_date=as_of_date,
        )
        ok, errors = validate_financial_series_artifact(
            artifact,
            as_of_date=as_of_date,
        )
        if not ok:
            raise FinancialSeriesBoundaryError(
                f"financial series artifact validation failed: {errors}"
            )

        write_json_atomic(artifact)
        print(
            "OK: taxas_bacen_series.json atualizado "
            f"({start_date.isoformat()}..{end_date.isoformat()}, 120 meses)."
        )
    except Exception as exc:
        print(
            "ERRO: série histórica financeira não atualizada; "
            "taxas_bacen_series.json permanece inalterado."
        )
        print("Erro:", str(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
