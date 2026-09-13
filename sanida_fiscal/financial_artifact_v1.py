from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from .financial_reference_v1 import annualize_cdi_daily_rate_pct
from .source_runtime_v1 import SourcePipelineRun
from .sources_v1 import ParseStatus


FINANCIAL_ARTIFACT_SCHEMA_VERSION = "1.4.0"


class FinancialArtifactBoundaryError(RuntimeError):
    pass


def _require_payload(run: SourcePipelineRun, source_id: str) -> dict[str, Any]:
    if run.collection.source_id != source_id:
        raise FinancialArtifactBoundaryError(
            f"expected {source_id}, got {run.collection.source_id}"
        )
    candidate = run.candidate
    if candidate is None or candidate.status != ParseStatus.PARSED or candidate.payload is None:
        raise FinancialArtifactBoundaryError(
            f"{source_id} has no current PARSED candidate for financial artifact generation"
        )
    return candidate.payload


def _require_decimal(value: Any, label: str) -> Decimal:
    if not isinstance(value, str):
        raise FinancialArtifactBoundaryError(f"{label} must be a canonical decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise FinancialArtifactBoundaryError(f"{label} is not a valid decimal string") from exc
    if not parsed.is_finite():
        raise FinancialArtifactBoundaryError(f"{label} must be finite")
    return parsed


def _source_meta(run: SourcePipelineRun, payload: Mapping[str, Any]) -> dict[str, Any]:
    candidate = run.candidate
    if candidate is None or candidate.status != ParseStatus.PARSED or candidate.payload is None:
        raise FinancialArtifactBoundaryError(
            f"{run.collection.source_id} has no current PARSED candidate metadata"
        )
    if not isinstance(run.state.last_candidate_sha256, str):
        raise FinancialArtifactBoundaryError(
            f"{run.collection.source_id} has no persisted candidate fingerprint"
        )
    return {
        "source_id": run.collection.source_id,
        "url": run.collection.source_url,
        "http_code": run.collection.http_status,
        "collection_status": run.collection.status.value,
        "snapshot_sha256": candidate.snapshot_sha256,
        "candidate_sha256": run.state.last_candidate_sha256,
        "parser_id": candidate.parser_id,
        "parser_version": candidate.parser_version,
        "observed_at_utc": candidate.observed_at_utc.isoformat().replace("+00:00", "Z"),
        "source_observation_date": payload.get("observation_date"),
        "series_code": payload.get("series_code"),
    }


def build_financial_reference_artifact(
    *,
    selic_run: SourcePipelineRun,
    cdi_run: SourcePipelineRun,
    generated_at_utc: str,
) -> dict[str, Any]:
    selic = _require_payload(selic_run, "BCB_SELIC_META_SGS_432")
    cdi = _require_payload(cdi_run, "BCB_CDI_DAILY_SGS_12")

    if selic.get("observation_type") != "bcb_selic_meta_sgs432":
        raise FinancialArtifactBoundaryError("unexpected Selic observation_type")
    if selic.get("series_code") != 432 or selic.get("unit") != "percent_per_year":
        raise FinancialArtifactBoundaryError("unexpected Selic series/unit")
    if cdi.get("observation_type") != "bcb_cdi_daily_sgs12":
        raise FinancialArtifactBoundaryError("unexpected CDI observation_type")
    if cdi.get("series_code") != 12 or cdi.get("unit") != "percent_per_business_day":
        raise FinancialArtifactBoundaryError("unexpected CDI series/unit")
    if cdi.get("annualization_basis_business_days") != 252:
        raise FinancialArtifactBoundaryError("unexpected CDI annualization basis")

    selic_rate = _require_decimal(selic.get("annual_rate_pct"), "selic.annual_rate_pct")
    cdi_daily = _require_decimal(cdi.get("daily_rate_pct"), "cdi.daily_rate_pct")
    cdi_annual = annualize_cdi_daily_rate_pct(format(cdi_daily, "f"))

    if selic_rate < 0 or selic_rate > Decimal("60"):
        raise FinancialArtifactBoundaryError("Selic annual rate outside accepted artifact range")
    if cdi_annual < 0 or cdi_annual > Decimal("60"):
        raise FinancialArtifactBoundaryError("CDI annualized rate outside accepted artifact range")

    selic_meta = _source_meta(selic_run, selic)
    selic_meta["source_value_pct"] = format(selic_rate, "f")
    selic_meta["source_unit"] = "percent_per_year"

    cdi_meta = _source_meta(cdi_run, cdi)
    cdi_meta["source_value_pct"] = format(cdi_daily, "f")
    cdi_meta["source_unit"] = "percent_per_business_day"
    cdi_meta["annualization_basis_business_days"] = 252
    cdi_meta["annualized_value_pct"] = format(cdi_annual, "f")

    return {
        "schema_version": FINANCIAL_ARTIFACT_SCHEMA_VERSION,
        "meta": {
            "generated_at_utc": generated_at_utc,
            "sources": {
                "selic": selic_meta,
                "cdi": cdi_meta,
            },
            "errors": [],
            "warnings": [],
        },
        "taxas": {
            "selic": float(selic_rate),
            "cdi": float(cdi_annual),
            "cdi_basis": "bcb_sgs_12_daily_compounded_252",
        },
    }
