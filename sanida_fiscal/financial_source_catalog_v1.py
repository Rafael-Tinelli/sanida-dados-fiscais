from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

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
from .sources_v1 import HttpCollectorV1, RetryPolicy, SnapshotStore, load_source_registry


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


def financial_parser_binding(source_id: str) -> FinancialParserBinding:
    try:
        return FINANCIAL_PARSER_BINDINGS[source_id]
    except KeyError as exc:
        raise ValueError(f"no Phase 4 financial parser binding for source_id: {source_id}") from exc


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
    source = registry[source_id]
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
        parser=binding.parser,
        candidate_store=candidate_store,
        use_http_validators=use_http_validators,
    )
