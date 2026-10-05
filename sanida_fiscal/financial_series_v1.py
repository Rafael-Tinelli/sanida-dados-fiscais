from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlencode
from zoneinfo import ZoneInfo
import re

import httpx

from .financial_artifact_v1 import CDI_MAX_OBSERVATION_AGE_DAYS
from .financial_reference_v1 import _parse_sgs_observations, annualize_cdi_daily_rate_pct
from .source_runtime_v1 import CandidateStore, SourcePipelineRun, SourceStateStore, run_source_pipeline
from .sources_v1 import (
    HttpCollectorV1,
    ParseStatus,
    ParserIncompatibleError,
    RetryPolicy,
    SnapshotStore,
    SourceSpec,
    load_source_registry,
)


FINANCIAL_SERIES_SCHEMA_VERSION = "1.0.0"
FINANCIAL_SERIES_MONTHS = 120
FINANCIAL_SERIES_TIMEZONE = ZoneInfo("America/Sao_Paulo")

SELIC_SOURCE_ID = "BCB_SELIC_META_SGS_432"
CDI_SOURCE_ID = "BCB_CDI_DAILY_SGS_12"

SELIC_HISTORY_PARSER_ID = "bcb_selic_meta_sgs432_history_v1"
SELIC_HISTORY_PARSER_VERSION = "1.0.0"
CDI_HISTORY_PARSER_ID = "bcb_cdi_daily_sgs12_history_v1"
CDI_HISTORY_PARSER_VERSION = "1.0.0"

DEFAULT_HEADERS = {
    "User-Agent": "SanidaFiscaisBot/4.0 (+https://sanida.com.br)",
    "Accept": "application/json",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
}


class FinancialSeriesBoundaryError(RuntimeError):
    pass


def month_start(value: date) -> date:
    return value.replace(day=1)


def shift_months(value: date, delta: int) -> date:
    base = value.year * 12 + (value.month - 1) + delta
    year, month0 = divmod(base, 12)
    return date(year, month0 + 1, 1)


def financial_series_window(as_of_date: date) -> tuple[date, date]:
    start = shift_months(month_start(as_of_date), -(FINANCIAL_SERIES_MONTHS - 1))
    return start, as_of_date


def financial_history_segments(
    start_date: date,
    end_date: date,
    *,
    months_per_segment: int = 12,
) -> list[tuple[date, date]]:
    """Split a long SGS window into contiguous bounded month ranges."""
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")
    if months_per_segment < 1 or months_per_segment > 12:
        raise ValueError("months_per_segment must be within 1..12")
    segments: list[tuple[date, date]] = []
    current = start_date
    while current <= end_date:
        next_boundary = shift_months(month_start(current), months_per_segment)
        segment_end = min(end_date, next_boundary - timedelta(days=1))
        if segment_end < current:
            raise FinancialSeriesBoundaryError("invalid segmented history boundary")
        segments.append((current, segment_end))
        current = segment_end + timedelta(days=1)
    for index, (seg_start, seg_end) in enumerate(segments):
        if index and seg_start != segments[index - 1][1] + timedelta(days=1):
            raise FinancialSeriesBoundaryError("history segments are not contiguous")
        if len(month_keys(seg_start, seg_end)) > months_per_segment:
            raise FinancialSeriesBoundaryError("history segment exceeds bounded month window")
    return segments


def month_keys(start_date: date, end_date: date) -> list[str]:
    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")
    current = month_start(start_date)
    last = month_start(end_date)
    out: list[str] = []
    while current <= last:
        out.append(current.strftime("%Y-%m"))
        current = shift_months(current, 1)
    return out


def _history_url(source: SourceSpec, start_date: date, end_date: date) -> str:
    raw = source.url.split("?", 1)[0]
    base = re.sub(r"/dados(?:/ultimos/\d+)?$", "/dados", raw)
    if base == raw and not raw.endswith("/dados"):
        raise FinancialSeriesBoundaryError(
            f"{source.source_id}: cannot derive bounded SGS history endpoint"
        )
    query = urlencode(
        {
            "dataInicial": start_date.strftime("%d/%m/%Y"),
            "dataFinal": end_date.strftime("%d/%m/%Y"),
            "formato": "json",
        }
    )
    return f"{base}?{query}"


def _history_payload(
    raw: bytes,
    *,
    source_id: str,
    series_code: int,
    start_date: date,
    end_date: date,
    value_key: str,
    unit: str,
) -> dict[str, Any]:
    observations = _parse_sgs_observations(raw, series_code=series_code)
    normalized: list[dict[str, str]] = []
    previous: date | None = None
    for observation_date, _value, canonical in sorted(observations, key=lambda item: item[0]):
        if observation_date < start_date or observation_date > end_date:
            raise ParserIncompatibleError(
                f"{source_id}: observation {observation_date.isoformat()} outside requested history window"
            )
        if previous is not None and observation_date <= previous:
            raise ParserIncompatibleError(f"{source_id}: observations are not strictly increasing")
        previous = observation_date
        normalized.append(
            {
                "date": observation_date.isoformat(),
                value_key: canonical,
            }
        )
    if not normalized:
        raise ParserIncompatibleError(f"{source_id}: empty normalized history")
    return {
        "observation_type": "bcb_financial_history",
        "source_id": source_id,
        "series_code": series_code,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "unit": unit,
        "observations": normalized,
    }


def parse_bcb_selic_history_snapshot(
    raw: bytes,
    *,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    return _history_payload(
        raw,
        source_id=SELIC_SOURCE_ID,
        series_code=432,
        start_date=start_date,
        end_date=end_date,
        value_key="annual_rate_pct",
        unit="percent_per_year",
    )


def parse_bcb_cdi_history_snapshot(
    raw: bytes,
    *,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    payload = _history_payload(
        raw,
        source_id=CDI_SOURCE_ID,
        series_code=12,
        start_date=start_date,
        end_date=end_date,
        value_key="daily_rate_pct",
        unit="percent_per_business_day",
    )
    payload["annualization_basis_business_days"] = 252
    return payload


def run_financial_history_source_pipeline(
    *,
    source_id: str,
    observed_at_utc: datetime,
    start_date: date,
    end_date: date,
    registry_path: Path = Path("docs/financial-source-registry-v1.json"),
    snapshot_root: Path = Path(".financial-series-runtime/snapshots"),
    state_root: Path = Path(".financial-series-runtime/state"),
    candidate_root: Path = Path(".financial-series-runtime/candidates"),
    timeout_seconds: float = 25.0,
    max_attempts: int = 3,
    transport: httpx.BaseTransport | None = None,
    headers: dict[str, str] | None = None,
) -> SourcePipelineRun:
    registry = load_source_registry(registry_path)
    if source_id not in registry:
        raise FinancialSeriesBoundaryError(f"unknown financial source_id: {source_id}")

    source = registry[source_id]
    resolved = source.model_copy(update={"url": _history_url(source, start_date, end_date)})

    if source_id == SELIC_SOURCE_ID:
        parser_id = SELIC_HISTORY_PARSER_ID
        parser_version = SELIC_HISTORY_PARSER_VERSION

        def parser(raw: bytes) -> dict[str, Any]:
            return parse_bcb_selic_history_snapshot(
                raw,
                start_date=start_date,
                end_date=end_date,
            )
    elif source_id == CDI_SOURCE_ID:
        parser_id = CDI_HISTORY_PARSER_ID
        parser_version = CDI_HISTORY_PARSER_VERSION

        def parser(raw: bytes) -> dict[str, Any]:
            return parse_bcb_cdi_history_snapshot(
                raw,
                start_date=start_date,
                end_date=end_date,
            )
    else:
        raise FinancialSeriesBoundaryError(f"unsupported financial history source_id: {source_id}")

    snapshot_store = SnapshotStore(snapshot_root)
    state_store = SourceStateStore(state_root)
    candidate_store = CandidateStore(candidate_root)
    collector = HttpCollectorV1(
        snapshot_store=snapshot_store,
        retry_policy=RetryPolicy(max_attempts=max_attempts, timeout_seconds=timeout_seconds),
        transport=transport,
        headers=headers or DEFAULT_HEADERS,
    )
    return run_source_pipeline(
        source=resolved,
        collector=collector,
        snapshot_store=snapshot_store,
        state_store=state_store,
        observed_at_utc=observed_at_utc,
        parser_id=parser_id,
        parser_version=parser_version,
        parser=parser,
        candidate_store=candidate_store,
        use_http_validators=False,
    )


def run_financial_history_source_segments(
    *,
    source_id: str,
    observed_at_utc: datetime,
    start_date: date,
    end_date: date,
    registry_path: Path = Path("docs/financial-source-registry-v1.json"),
    snapshot_root: Path = Path(".financial-series-runtime/snapshots"),
    state_root: Path = Path(".financial-series-runtime/state"),
    candidate_root: Path = Path(".financial-series-runtime/candidates"),
    timeout_seconds: float = 25.0,
    max_attempts: int = 3,
    transport: httpx.BaseTransport | None = None,
    headers: dict[str, str] | None = None,
    months_per_segment: int = 12,
) -> list[SourcePipelineRun]:
    runs: list[SourcePipelineRun] = []
    for segment_start, segment_end in financial_history_segments(
        start_date, end_date, months_per_segment=months_per_segment
    ):
        run = run_financial_history_source_pipeline(
            source_id=source_id,
            observed_at_utc=observed_at_utc,
            start_date=segment_start,
            end_date=segment_end,
            registry_path=registry_path,
            snapshot_root=snapshot_root,
            state_root=state_root,
            candidate_root=candidate_root,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
            transport=transport,
            headers=headers,
        )
        if run.candidate is None or run.candidate.status != ParseStatus.PARSED:
            detail = run.state.last_source_error or run.state.last_parser_error or "not_parsed"
            raise FinancialSeriesBoundaryError(
                f"{source_id}: history segment {segment_start.isoformat()}.."
                f"{segment_end.isoformat()} failed closed: {detail}"
            )
        runs.append(run)
    return runs


def _require_payload(run: SourcePipelineRun, source_id: str) -> dict[str, Any]:
    if run.collection.source_id != source_id:
        raise FinancialSeriesBoundaryError(
            f"expected source {source_id}, got {run.collection.source_id}"
        )
    candidate = run.candidate
    if candidate is None or candidate.status != ParseStatus.PARSED or candidate.payload is None:
        raise FinancialSeriesBoundaryError(
            f"{source_id} has no current PARSED candidate for history artifact generation"
        )
    return dict(candidate.payload)


def _decimal(value: Any, label: str) -> Decimal:
    if not isinstance(value, str):
        raise FinancialSeriesBoundaryError(f"{label} must be a canonical decimal string")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise FinancialSeriesBoundaryError(f"{label} is invalid") from exc
    if not parsed.is_finite():
        raise FinancialSeriesBoundaryError(f"{label} must be finite")
    return parsed


def _group_by_month(
    observations: list[dict[str, Any]],
    *,
    value_key: str,
) -> dict[str, list[tuple[date, Decimal, str]]]:
    grouped: dict[str, list[tuple[date, Decimal, str]]] = defaultdict(list)
    for item in observations:
        if not isinstance(item, dict):
            raise FinancialSeriesBoundaryError("history observation must be an object")
        date_raw = item.get("date")
        if not isinstance(date_raw, str):
            raise FinancialSeriesBoundaryError("history observation date missing")
        try:
            observation_date = date.fromisoformat(date_raw)
        except ValueError as exc:
            raise FinancialSeriesBoundaryError("history observation date invalid") from exc
        canonical = item.get(value_key)
        value = _decimal(canonical, value_key)
        grouped[observation_date.strftime("%Y-%m")].append(
            (observation_date, value, canonical)
        )
    return grouped


def _cdi_month_return_pct(
    observations: list[tuple[date, Decimal, str]],
) -> Decimal:
    factor = Decimal("1")
    for _observation_date, value, _canonical in observations:
        if value < 0 or value > Decimal("10"):
            raise FinancialSeriesBoundaryError("CDI daily rate outside accepted range")
        factor *= Decimal("1") + (value / Decimal("100"))
    return ((factor - Decimal("1")) * Decimal("100")).quantize(
        Decimal("0.000001"),
        rounding=ROUND_HALF_UP,
    )


def _merge_segment_payloads(
    runs: list[SourcePipelineRun],
    *,
    source_id: str,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    if not runs:
        raise FinancialSeriesBoundaryError(f"{source_id}: no segmented history runs")
    observations: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    expected_start = start_date
    for index, run in enumerate(runs):
        payload = _require_payload(run, source_id)
        try:
            seg_start = date.fromisoformat(str(payload.get("start_date")))
            seg_end = date.fromisoformat(str(payload.get("end_date")))
        except ValueError as exc:
            raise FinancialSeriesBoundaryError(
                f"{source_id}: segment {index} has invalid boundaries"
            ) from exc
        if seg_start != expected_start or seg_end < seg_start:
            raise FinancialSeriesBoundaryError(
                f"{source_id}: segment {index} is not contiguous"
            )
        if len(month_keys(seg_start, seg_end)) > 12:
            raise FinancialSeriesBoundaryError(
                f"{source_id}: segment {index} exceeds twelve calendar months"
            )
        if seg_end > end_date:
            raise FinancialSeriesBoundaryError(
                f"{source_id}: segment {index} exceeds requested history window"
            )
        expected_start = seg_end + timedelta(days=1)
        for item in list(payload.get("observations") or []):
            raw_date = item.get("date") if isinstance(item, dict) else None
            if not isinstance(raw_date, str) or raw_date in seen_dates:
                raise FinancialSeriesBoundaryError(
                    f"{source_id}: duplicate/invalid segmented observation date"
                )
            seen_dates.add(raw_date)
            observations.append(dict(item))
    if expected_start != end_date + timedelta(days=1):
        raise FinancialSeriesBoundaryError(
            f"{source_id}: segmented history does not cover full requested window"
        )
    observations.sort(key=lambda item: item["date"])
    return {
        "source_id": source_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "observations": observations,
    }


def _segmented_source_meta(
    runs: list[SourcePipelineRun],
    *,
    source_id: str,
    merged_payload: Mapping[str, Any],
) -> dict[str, Any]:
    if not runs:
        raise FinancialSeriesBoundaryError(f"{source_id}: no history segments")
    segment_meta: list[dict[str, Any]] = []
    for run in runs:
        payload = _require_payload(run, source_id)
        meta = _source_meta(run, payload)
        meta["start_date"] = payload["start_date"]
        meta["end_date"] = payload["end_date"]
        segment_meta.append(meta)
    top = dict(segment_meta[-1])
    top.update({
        "source_id": source_id,
        "collection_mode": "segmented_bounded_windows_v1",
        "segment_count": len(segment_meta),
        "observation_count": len(merged_payload.get("observations") or []),
        "segments": segment_meta,
    })
    return top


def _source_meta(run: SourcePipelineRun, payload: Mapping[str, Any]) -> dict[str, Any]:
    candidate = run.candidate
    if candidate is None or candidate.status != ParseStatus.PARSED:
        raise FinancialSeriesBoundaryError("history source candidate metadata unavailable")
    if not isinstance(run.state.last_candidate_sha256, str):
        raise FinancialSeriesBoundaryError("history source candidate fingerprint unavailable")
    return {
        "source_id": run.collection.source_id,
        "url": run.collection.source_url,
        "http_code": run.collection.http_status,
        "snapshot_sha256": candidate.snapshot_sha256,
        "candidate_sha256": run.state.last_candidate_sha256,
        "snapshot_path": candidate.snapshot_path,
        "candidate_path": run.state.last_candidate_path,
        "parser_id": candidate.parser_id,
        "parser_version": candidate.parser_version,
        "observed_at_utc": candidate.observed_at_utc.isoformat().replace("+00:00", "Z"),
        "observation_count": len(payload.get("observations") or []),
    }


def build_financial_series_artifact_from_segments(
    *,
    selic_runs: list[SourcePipelineRun],
    cdi_runs: list[SourcePipelineRun],
    generated_at_utc: str,
    as_of_date: date,
) -> dict[str, Any]:
    try:
        generated_at = datetime.fromisoformat(generated_at_utc.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise FinancialSeriesBoundaryError("generated_at_utc is not ISO-8601") from exc
    if generated_at.tzinfo is None or generated_at.utcoffset() != timezone.utc.utcoffset(generated_at):
        raise FinancialSeriesBoundaryError("generated_at_utc must be UTC")

    start_date, end_date = financial_series_window(as_of_date)
    expected_months = month_keys(start_date, end_date)
    selic = _merge_segment_payloads(
        selic_runs, source_id=SELIC_SOURCE_ID, start_date=start_date, end_date=end_date
    )
    cdi = _merge_segment_payloads(
        cdi_runs, source_id=CDI_SOURCE_ID, start_date=start_date, end_date=end_date
    )
    return _build_financial_series_from_payloads(
        selic=selic,
        cdi=cdi,
        selic_meta=_segmented_source_meta(
            selic_runs, source_id=SELIC_SOURCE_ID, merged_payload=selic
        ),
        cdi_meta=_segmented_source_meta(
            cdi_runs, source_id=CDI_SOURCE_ID, merged_payload=cdi
        ),
        generated_at_utc=generated_at_utc,
        as_of_date=as_of_date,
        expected_months=expected_months,
    )


def _build_financial_series_from_payloads(
    *,
    selic: Mapping[str, Any],
    cdi: Mapping[str, Any],
    selic_meta: Mapping[str, Any],
    cdi_meta: Mapping[str, Any],
    generated_at_utc: str,
    as_of_date: date,
    expected_months: list[str],
) -> dict[str, Any]:
    start_date, end_date = financial_series_window(as_of_date)
    if len(expected_months) != FINANCIAL_SERIES_MONTHS:
        raise FinancialSeriesBoundaryError("history window is not exactly 120 calendar months")
    for payload, source_id in ((selic, SELIC_SOURCE_ID), (cdi, CDI_SOURCE_ID)):
        if payload.get("source_id") != source_id:
            raise FinancialSeriesBoundaryError(f"{source_id}: payload source_id mismatch")
        if payload.get("start_date") != start_date.isoformat():
            raise FinancialSeriesBoundaryError(f"{source_id}: history start_date mismatch")
        if payload.get("end_date") != end_date.isoformat():
            raise FinancialSeriesBoundaryError(f"{source_id}: history end_date mismatch")

    selic_groups = _group_by_month(list(selic.get("observations") or []), value_key="annual_rate_pct")
    cdi_groups = _group_by_month(list(cdi.get("observations") or []), value_key="daily_rate_pct")
    missing_selic = [month for month in expected_months if month not in selic_groups]
    missing_cdi = [month for month in expected_months if month not in cdi_groups]
    if missing_selic:
        raise FinancialSeriesBoundaryError(f"Selic history missing calendar months: {missing_selic}")
    if missing_cdi:
        raise FinancialSeriesBoundaryError(f"CDI history missing calendar months: {missing_cdi}")

    current_month = as_of_date.strftime("%Y-%m")
    points: list[dict[str, Any]] = []
    for month in expected_months:
        selic_last = max(selic_groups[month], key=lambda item: item[0])
        cdi_month = sorted(cdi_groups[month], key=lambda item: item[0])
        cdi_last = cdi_month[-1]
        selic_value = selic_last[1]
        if selic_value < 0 or selic_value > Decimal("60"):
            raise FinancialSeriesBoundaryError("Selic history rate outside accepted range")
        cdi_annualized = annualize_cdi_daily_rate_pct(cdi_last[2])
        cdi_month_return = _cdi_month_return_pct(cdi_month)
        points.append({
            "month": month,
            "month_complete": month < current_month,
            "selic_observation_date": selic_last[0].isoformat(),
            "selic_annual_rate_pct": float(selic_value),
            "cdi_observation_date": cdi_last[0].isoformat(),
            "cdi_daily_rate_pct": float(cdi_last[1]),
            "cdi_annualized_rate_pct": float(cdi_annualized),
            "cdi_month_return_pct": float(cdi_month_return),
            "cdi_observation_count": len(cdi_month),
        })

    latest = points[-1]
    cdi_latest_date = date.fromisoformat(latest["cdi_observation_date"])
    cdi_age_days = (as_of_date - cdi_latest_date).days
    if cdi_age_days < 0:
        raise FinancialSeriesBoundaryError("latest CDI history observation is in the future")
    if cdi_age_days > CDI_MAX_OBSERVATION_AGE_DAYS:
        raise FinancialSeriesBoundaryError(
            f"latest CDI history observation is stale: age_days={cdi_age_days}"
        )
    return {
        "schema_version": FINANCIAL_SERIES_SCHEMA_VERSION,
        "meta": {
            "generated_at_utc": generated_at_utc,
            "timezone": "America/Sao_Paulo",
            "window": {
                "months": FINANCIAL_SERIES_MONTHS,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
            "methodology": {
                "selic_monthly": "last_SGS_432_observation_in_calendar_month_percent_per_year",
                "cdi_monthly_level": "last_SGS_12_daily_observation_in_calendar_month_annualized_by_compounding_252_business_days",
                "cdi_monthly_return": "compound_all_SGS_12_daily_percent_observations_in_calendar_month",
                "current_month": "month_to_date_and_marked_incomplete",
                "source_collection": "segmented_bounded_windows_v1",
            },
            "sources": {"selic": dict(selic_meta), "cdi": dict(cdi_meta)},
            "latest_cdi_observation_age_calendar_days": cdi_age_days,
            "max_latest_cdi_observation_age_calendar_days": CDI_MAX_OBSERVATION_AGE_DAYS,
            "errors": [],
            "warnings": [],
        },
        "points": points,
    }


def build_financial_series_artifact(
    *,
    selic_run: SourcePipelineRun,
    cdi_run: SourcePipelineRun,
    generated_at_utc: str,
    as_of_date: date,
) -> dict[str, Any]:
    try:
        generated_at = datetime.fromisoformat(generated_at_utc.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise FinancialSeriesBoundaryError("generated_at_utc is not ISO-8601") from exc
    if generated_at.tzinfo is None or generated_at.utcoffset() != timezone.utc.utcoffset(generated_at):
        raise FinancialSeriesBoundaryError("generated_at_utc must be UTC")

    start_date, end_date = financial_series_window(as_of_date)
    expected_months = month_keys(start_date, end_date)
    selic = _require_payload(selic_run, SELIC_SOURCE_ID)
    cdi = _require_payload(cdi_run, CDI_SOURCE_ID)
    return _build_financial_series_from_payloads(
        selic=selic,
        cdi=cdi,
        selic_meta=_source_meta(selic_run, selic),
        cdi_meta=_source_meta(cdi_run, cdi),
        generated_at_utc=generated_at_utc,
        as_of_date=as_of_date,
        expected_months=expected_months,
    )


def validate_financial_series_artifact(
    artifact: Mapping[str, Any],
    *,
    as_of_date: date | None = None,
) -> tuple[bool, list[str]]:
    errors: list[str] = []
    if not isinstance(artifact, Mapping):
        return False, ["artifact:not_object"]
    if artifact.get("schema_version") != FINANCIAL_SERIES_SCHEMA_VERSION:
        errors.append("schema_version:unexpected")

    meta = artifact.get("meta")
    if not isinstance(meta, Mapping):
        return False, errors + ["meta:missing_or_bad"]

    window = meta.get("window")
    if not isinstance(window, Mapping):
        errors.append("meta.window:missing_or_bad")
        return False, errors

    try:
        start_date = date.fromisoformat(str(window.get("start_date")))
        end_date = date.fromisoformat(str(window.get("end_date")))
    except ValueError:
        errors.append("meta.window:invalid_dates")
        return False, errors

    if window.get("months") != FINANCIAL_SERIES_MONTHS:
        errors.append("meta.window.months:unexpected")
    expected = month_keys(start_date, end_date)
    if len(expected) != FINANCIAL_SERIES_MONTHS:
        errors.append("meta.window:not_120_months")

    points = artifact.get("points")
    if not isinstance(points, list) or len(points) != FINANCIAL_SERIES_MONTHS:
        errors.append("points:unexpected_count")
        return False, errors

    point_months: list[str] = []
    for index, point in enumerate(points):
        if not isinstance(point, Mapping):
            errors.append(f"points.{index}:not_object")
            continue
        month = point.get("month")
        if not isinstance(month, str):
            errors.append(f"points.{index}.month:missing")
            continue
        point_months.append(month)

        for key in (
            "selic_annual_rate_pct",
            "cdi_daily_rate_pct",
            "cdi_annualized_rate_pct",
            "cdi_month_return_pct",
        ):
            value = point.get(key)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                errors.append(f"points.{index}.{key}:not_numeric")
        for key in ("selic_observation_date", "cdi_observation_date"):
            raw = point.get(key)
            try:
                parsed = date.fromisoformat(str(raw))
            except ValueError:
                errors.append(f"points.{index}.{key}:invalid")
                continue
            if parsed.strftime("%Y-%m") != month:
                errors.append(f"points.{index}.{key}:month_mismatch")

    if point_months != expected:
        errors.append("points:non_contiguous_months")

    if points:
        latest = points[-1]
        check_date = as_of_date or end_date
        try:
            latest_cdi = date.fromisoformat(str(latest.get("cdi_observation_date")))
        except ValueError:
            errors.append("points.latest.cdi_observation_date:invalid")
        else:
            age = (check_date - latest_cdi).days
            if age < 0:
                errors.append("points.latest.cdi_observation_date:future")
            elif age > CDI_MAX_OBSERVATION_AGE_DAYS:
                errors.append("points.latest.cdi_observation_date:stale")

    sources = meta.get("sources")
    if not isinstance(sources, Mapping):
        errors.append("meta.sources:missing_or_bad")
    else:
        if (sources.get("selic") or {}).get("source_id") != SELIC_SOURCE_ID:
            errors.append("meta.sources.selic:wrong_source")
        if (sources.get("cdi") or {}).get("source_id") != CDI_SOURCE_ID:
            errors.append("meta.sources.cdi:wrong_source")

    methodology = meta.get("methodology")
    if not isinstance(methodology, Mapping):
        errors.append("meta.methodology:missing_or_bad")

    return (len(errors) == 0), errors
