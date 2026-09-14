from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
from typing import Any

from .sources_v1 import ParserIncompatibleError


SELIC_PARSER_ID = "bcb_selic_meta_sgs432_v1"
SELIC_PARSER_VERSION = "1.1.0"
CDI_PARSER_ID = "bcb_cdi_daily_sgs12_v1"
CDI_PARSER_VERSION = "1.0.0"


def _parse_sgs_observations(
    raw: bytes,
    *,
    series_code: int,
) -> list[tuple[date, Decimal, str]]:
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ParserIncompatibleError(f"SGS {series_code}: invalid JSON") from exc

    if not isinstance(payload, list) or not payload:
        raise ParserIncompatibleError(f"SGS {series_code}: expected at least one observation")

    observations: list[tuple[date, Decimal, str]] = []
    seen_dates: set[date] = set()
    for item in payload:
        if not isinstance(item, dict):
            raise ParserIncompatibleError(f"SGS {series_code}: observation must be an object")

        date_raw = item.get("data")
        value_raw = item.get("valor")
        if not isinstance(date_raw, str) or not isinstance(value_raw, str):
            raise ParserIncompatibleError(
                f"SGS {series_code}: observation missing data/valor strings"
            )

        try:
            observation_date = datetime.strptime(date_raw, "%d/%m/%Y").date()
        except ValueError as exc:
            raise ParserIncompatibleError(
                f"SGS {series_code}: invalid DD/MM/YYYY date"
            ) from exc

        if observation_date in seen_dates:
            raise ParserIncompatibleError(
                f"SGS {series_code}: duplicate observation date {observation_date.isoformat()}"
            )
        seen_dates.add(observation_date)

        normalized_raw = value_raw.strip().replace(",", ".")
        try:
            value = Decimal(normalized_raw)
        except InvalidOperation as exc:
            raise ParserIncompatibleError(
                f"SGS {series_code}: invalid decimal value"
            ) from exc

        if not value.is_finite() or value < 0 or value > Decimal("100"):
            raise ParserIncompatibleError(
                f"SGS {series_code}: value out of accepted range"
            )

        observations.append((observation_date, value, format(value, "f")))

    return observations


def _parse_single_sgs_observation(
    raw: bytes,
    *,
    series_code: int,
) -> tuple[str, Decimal, str]:
    observations = _parse_sgs_observations(raw, series_code=series_code)
    if len(observations) != 1:
        raise ParserIncompatibleError(
            f"SGS {series_code}: expected exactly one observation"
        )
    observation_date, value, canonical = observations[0]
    return observation_date.isoformat(), value, canonical


def parse_bcb_selic_meta_sgs432_snapshot(
    raw: bytes,
    *,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    """Normalize the Selic target observation applicable on ``as_of_date``.

    SGS 432 is a daily policy-rate series and may legitimately publish rows dated
    after the collection day. Those rows describe the current target carried
    forward; they are not evidence that a future date is already current. When a
    bounded multi-row snapshot is supplied, Phase 4 therefore selects the latest
    row whose date is not after the Brazilian as-of date.
    """
    observations = _parse_sgs_observations(raw, series_code=432)

    if as_of_date is None:
        if len(observations) != 1:
            raise ParserIncompatibleError(
                "SGS 432: as_of_date is required when snapshot contains multiple observations"
            )
        selected = observations[0]
    else:
        applicable = [item for item in observations if item[0] <= as_of_date]
        if not applicable:
            raise ParserIncompatibleError(
                f"SGS 432: no observation on or before {as_of_date.isoformat()}"
            )
        selected = max(applicable, key=lambda item: item[0])

    observation_date, _value, canonical = selected
    return {
        "observation_type": "bcb_selic_meta_sgs432",
        "series_code": 432,
        "observation_date": observation_date.isoformat(),
        "annual_rate_pct": canonical,
        "unit": "percent_per_year",
    }


def parse_bcb_cdi_daily_sgs12_snapshot(raw: bytes) -> dict[str, Any]:
    observation_date, _value, canonical = _parse_single_sgs_observation(
        raw,
        series_code=12,
    )
    return {
        "observation_type": "bcb_cdi_daily_sgs12",
        "series_code": 12,
        "observation_date": observation_date,
        "daily_rate_pct": canonical,
        "unit": "percent_per_business_day",
        "annualization_basis_business_days": 252,
    }


def annualize_cdi_daily_rate_pct(daily_rate_pct: str) -> Decimal:
    try:
        daily = Decimal(daily_rate_pct)
    except InvalidOperation as exc:
        raise ValueError("daily_rate_pct must be a decimal string") from exc
    if not daily.is_finite() or daily < 0 or daily > Decimal("10"):
        raise ValueError("daily_rate_pct outside accepted range")

    factor = Decimal("1") + (daily / Decimal("100"))
    annual_pct = ((factor ** 252) - Decimal("1")) * Decimal("100")
    return annual_pct.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
