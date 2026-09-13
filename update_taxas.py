from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

from sanida_fiscal.financial_artifact_v1 import (
    FINANCIAL_ARTIFACT_SCHEMA_VERSION,
    FinancialArtifactBoundaryError,
    build_financial_reference_artifact,
)
from sanida_fiscal.financial_source_catalog_v1 import run_registered_financial_source_pipeline


OUTPUT_FILE = "taxas_bacen.json"
FINANCIAL_SOURCE_REGISTRY = Path("docs/financial-source-registry-v1.json")
SOURCE_RUNTIME_ROOT = Path(os.getenv("SFA_SOURCE_RUNTIME_ROOT", ".source-runtime").strip())
FINANCIAL_SOURCE_IDS = (
    "BCB_SELIC_META_SGS_432",
    "BCB_CDI_DAILY_SGS_12",
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


def validate_payload(d: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errs: List[str] = []
    if not isinstance(d, dict):
        return False, ["payload:not_dict"]

    if d.get("schema_version") != FINANCIAL_ARTIFACT_SCHEMA_VERSION:
        errs.append("schema_version:unexpected")

    meta = d.get("meta")
    if not isinstance(meta, dict):
        errs.append("meta:missing_or_bad")
    else:
        if not isinstance(meta.get("generated_at_utc"), str):
            errs.append("meta.generated_at_utc:missing_or_bad")
        sources = meta.get("sources")
        if not isinstance(sources, dict):
            errs.append("meta.sources:missing_or_bad")
        else:
            for key, source_id in (
                ("selic", "BCB_SELIC_META_SGS_432"),
                ("cdi", "BCB_CDI_DAILY_SGS_12"),
            ):
                item = sources.get(key)
                if not isinstance(item, dict) or item.get("source_id") != source_id:
                    errs.append(f"meta.sources.{key}:missing_or_bad")

    taxas = d.get("taxas")
    if not isinstance(taxas, dict):
        errs.append("taxas:missing_or_bad")
    else:
        for key in ("selic", "cdi"):
            value = taxas.get(key)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                errs.append(f"taxas.{key}:missing_or_bad")
            elif not (0 <= value <= 60):
                errs.append(f"taxas.{key}:out_of_range")
        if taxas.get("cdi_basis") != "bcb_sgs_12_daily_compounded_252":
            errs.append("taxas.cdi_basis:unexpected")

    return (len(errs) == 0), errs


def write_json_atomic(data: Dict[str, Any]) -> None:
    target = Path(OUTPUT_FILE)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    os.replace(tmp, target)


def collect_financial_source_runs(observed_at_utc: dt.datetime):
    runs = {}
    for source_id in FINANCIAL_SOURCE_IDS:
        runs[source_id] = run_registered_financial_source_pipeline(
            source_id=source_id,
            observed_at_utc=observed_at_utc,
            registry_path=FINANCIAL_SOURCE_REGISTRY,
            snapshot_root=SOURCE_RUNTIME_ROOT / "snapshots",
            state_root=SOURCE_RUNTIME_ROOT / "state",
            candidate_root=SOURCE_RUNTIME_ROOT / "candidates",
            timeout_seconds=float(TIMEOUT),
            max_attempts=RETRIES,
            headers=HEADERS,
            # A new compatibility artifact requires current PARSED candidates.
            # Prior state/304 may prove last-good evidence, but cannot refresh the
            # artifact timestamp or relabel an old financial observation as current.
            use_http_validators=False,
        )
    return runs


def main() -> None:
    observed_at_utc = now_utc()

    try:
        runs = collect_financial_source_runs(observed_at_utc)
        payload = build_financial_reference_artifact(
            selic_run=runs["BCB_SELIC_META_SGS_432"],
            cdi_run=runs["BCB_CDI_DAILY_SGS_12"],
            generated_at_utc=observed_at_utc.isoformat().replace("+00:00", "Z"),
        )
        ok, errors = validate_payload(payload)
        if not ok:
            raise FinancialArtifactBoundaryError(
                f"financial artifact validation failed: {errors}"
            )
        write_json_atomic(payload)
        print("OK: taxas_bacen.json atualizado via BCB SGS 432 + SGS 12.")
    except Exception as exc:
        print("ERRO: referência financeira não atualizada; taxas_bacen.json permanece inalterado.")
        print("Erro:", str(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
