from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import httpx
from pydantic import JsonValue

from .financial_reference_v1 import (
    CDI_PARSER_ID,
    CDI_PARSER_VERSION,
    SELIC_PARSER_ID,
    SELIC_PARSER_VERSION,
    parse_bcb_cdi_daily_sgs12_snapshot,
    parse_bcb_selic_meta_sgs432_snapshot,
)
from .source_runtime_v1 import CandidateStore, SourcePipelineRun, SourceStateStore, run_source_pipeline
from .sources_v1 import HttpCollectorV1, RetryPolicy, SnapshotStore, SourceSpec, load_source_registry


@dataclass(frozen=True)
class FinancialParserBinding:
    source_id: str
    parser_id: str
    parser_version: str
    parser: Callable[[bytes], dict[str, JsonValue]]


FINANCIAL_PARSER_BINDINGS = {
    "BCB_SELIC_META_SGS_432": FinancialParserBinding(
        source_id="BCB_SELIC_META_SGS_432",
        parser_id=SELIC_PARSER_ID,
        parser_version=SELIC_PARSER_VERSION,
        parser=parse_bcb_selic_meta_sgs432_snapshot,
    ),
    "BCB_CDI_DAILY_SGS_12": FinancialParserBinding(
        source_id="BCB_CDI_DAILY_SGS_12",
        parser_id=CDI_PARSER_ID,
        parser_version=CDI_PARSER_VERSION,
        parser=parse_bcb_cdi_daily_sgs12_snapshot,
    ),
}

DEFAULT_HEADERS = {
    "User-Agent": "SanidaFiscaisBot/4.0 (+https://sanida.com.br)",
    "Accept": "application/json",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
}

SELIC_SOURCE_ID = "BCB_SELIC_META_SGS_432"
SELIC_CALENDAR_TIMEZONE = ZoneInfo("America/Sao_Paulo")
SELIC_LOOKBACK_DAYS = 31


def financial_parser_binding(source_id: str) -> FinancialParserBinding:
    try:
        return FINANCIAL_PARSER_BINDINGS[source_id]
    except KeyError as exc:
        raise ValueError(f"no Phase 4 financial parser binding for source_id: {source_id}") from exc


def _resolve_runtime_source_and_parser(
    *,
    source_id: str,
    source: SourceSpec,
    binding: FinancialParserBinding,
    observed_at_utc: datetime,
) -> tuple[SourceSpec, Callable[[bytes], dict[str, JsonValue]]]:
    if source_id != SELIC_SOURCE_ID:
        return source, binding.parser

    local_date = observed_at_utc.astimezone(SELIC_CALENDAR_TIMEZONE).date()
    start_date = local_date - timedelta(days=SELIC_LOOKBACK_DAYS)
    base_url = source.url.split("?", 1)[0]
    query = urlencode(
        {
            "formato": "json",
            "dataInicial": start_date.strftime("%d/%m/%Y"),
            "dataFinal": local_date.strftime("%d/%m/%Y"),
        }
    )
    resolved_source = source.model_copy(update={"url": f"{base_url}?{query}"})

    def parse_current_selic(raw: bytes) -> dict[str, JsonValue]:
        return parse_bcb_selic_meta_sgs432_snapshot(raw, as_of_date=local_date)

    return resolved_source, parse_current_selic


def run_registered_financial_source_pipeline(
    *,
    source_id: str,
    observed_at_utc: datetime,
    registry_path: Path = Path("docs/financial-source-registry-v1.json"),
    snapshot_root: Path = Path(".source-runtime/snapshots"),
    state_root: Path = Path(".source-runtime/state"),
    candidate_root: Path = Path(".source-runtime/candidates"),
    timeout_seconds: float = 20.0,
    max_attempts: int = 3,
    transport: httpx.BaseTransport | None = None,
    headers: dict[str, str] | None = None,
    use_http_validators: bool = True,
) -> SourcePipelineRun:
    registry = load_source_registry(registry_path)
    if source_id not in registry:
        raise ValueError(f"unknown financial source_id: {source_id}")

    binding = financial_parser_binding(source_id)
    source, parser = _resolve_runtime_source_and_parser(
        source_id=source_id,
        source=registry[source_id],
        binding=binding,
        observed_at_utc=observed_at_utc,
    )
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
        source=source,
        collector=collector,
        snapshot_store=snapshot_store,
        state_store=state_store,
        observed_at_utc=observed_at_utc,
        parser_id=binding.parser_id,
        parser_version=binding.parser_version,
        parser=parser,
        candidate_store=candidate_store,
        use_http_validators=use_http_validators,
    )
