from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

import httpx
from pydantic import JsonValue

from .inss_employee_v1 import (
    PARSER_ID as INSS_PARSER_ID,
    PARSER_VERSION as INSS_PARSER_VERSION,
    REFERENCE_YEAR as INSS_REFERENCE_YEAR,
    parse_inss_employee_snapshot,
)
from .rfb_irrf_v1 import (
    PARSER_ID as RFB_PARSER_ID,
    PARSER_VERSION as RFB_PARSER_VERSION,
    REFERENCE_YEAR as RFB_REFERENCE_YEAR,
    parse_rfb_irrf_snapshot,
)
from .source_runtime_v1 import (
    CandidateStore,
    SourcePipelineRun,
    SourceStateStore,
    run_source_pipeline,
)
from .sources_v1 import (
    HttpCollectorV1,
    ParserIncompatibleError,
    RetryPolicy,
    SnapshotStore,
    SourceSpec,
    load_source_registry,
)


@dataclass(frozen=True)
class ParserBinding:
    source_id: str
    parser_id: str
    parser_version: str
    reference_year: int
    parser: Callable[[bytes], dict[str, JsonValue]]


PARSER_BINDINGS = {
    "RFB_IRRF_TABLE_2026": ParserBinding(
        source_id="RFB_IRRF_TABLE_2026",
        parser_id=RFB_PARSER_ID,
        parser_version=RFB_PARSER_VERSION,
        reference_year=RFB_REFERENCE_YEAR,
        parser=parse_rfb_irrf_snapshot,
    ),
    "INSS_TABLE_2026": ParserBinding(
        source_id="INSS_TABLE_2026",
        parser_id=INSS_PARSER_ID,
        parser_version=INSS_PARSER_VERSION,
        reference_year=INSS_REFERENCE_YEAR,
        parser=parse_inss_employee_snapshot,
    ),
}

DEFAULT_HEADERS = {
    "User-Agent": "SanidaFiscaisBot/4.0 (+https://sanida.com.br)",
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
}


def parser_binding(source_id: str) -> ParserBinding:
    try:
        return PARSER_BINDINGS[source_id]
    except KeyError as exc:
        raise ValueError(f"no Phase 4 parser binding for source_id: {source_id}") from exc


def registered_reference_year(source_id: str) -> int:
    """Historical compatibility helper; runtime discovery is year-aware."""
    return parser_binding(source_id).reference_year


def resolve_source_for_reference_year(
    source: SourceSpec,
    *,
    source_id: str,
    reference_year: int,
) -> SourceSpec:
    """Resolve only the year-varying part of a known official source URL.

    Source IDs stay unchanged in this first migration layer so historical
    evidence and contracts remain addressable. The annual RFB table path is
    resolved from the execution year; the INSS canonical table URL is stable.
    """
    if source_id == "RFB_IRRF_TABLE_2026":
        url = source.url
        if not url.rstrip("/").endswith("/2026"):
            raise ValueError("RFB annual source URL no longer has the registered year suffix")
        url = url.rstrip("/")[:-4] + str(reference_year)
        return source.model_copy(update={"url": url})
    return source


def parser_for_reference_year(binding: ParserBinding, reference_year: int):
    def parse(body: bytes) -> dict[str, JsonValue]:
        payload = binding.parser(body)
        observed_year = payload.get("reference_year")
        if observed_year != reference_year:
            raise ParserIncompatibleError(
                f"reference-year mismatch: expected={reference_year} observed={observed_year}"
            )
        return payload

    return parse


def run_registered_source_pipeline(
    *,
    source_id: str,
    observed_at_utc: datetime,
    registry_path: Path = Path("docs/source-registry-v1.json"),
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
        raise ValueError(f"unknown source_id: {source_id}")

    binding = parser_binding(source_id)
    reference_year = observed_at_utc.year
    source = resolve_source_for_reference_year(
        registry[source_id],
        source_id=source_id,
        reference_year=reference_year,
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
        parser=parser_for_reference_year(binding, reference_year),
        candidate_store=candidate_store,
        use_http_validators=use_http_validators,
    )
