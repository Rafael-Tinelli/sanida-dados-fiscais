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
    parse_inss_employee_snapshot,
)
from .rfb_irrf_v1 import (
    PARSER_ID as RFB_PARSER_ID,
    PARSER_VERSION as RFB_PARSER_VERSION,
    parse_rfb_irrf_snapshot,
)
from .source_ids_v1 import (
    INSS_SOURCE_ID,
    RFB_SOURCE_ID,
    canonical_source_id,
    legacy_source_ids_for,
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
    parser: Callable[[bytes], dict[str, JsonValue]]


PARSER_BINDINGS = {
    RFB_SOURCE_ID: ParserBinding(
        source_id=RFB_SOURCE_ID,
        parser_id=RFB_PARSER_ID,
        parser_version=RFB_PARSER_VERSION,
        parser=parse_rfb_irrf_snapshot,
    ),
    INSS_SOURCE_ID: ParserBinding(
        source_id=INSS_SOURCE_ID,
        parser_id=INSS_PARSER_ID,
        parser_version=INSS_PARSER_VERSION,
        parser=parse_inss_employee_snapshot,
    ),
}

DEFAULT_HEADERS = {
    "User-Agent": "SanidaFiscaisBot/4.0 (+https://sanida.com.br)",
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7",
}


def parser_binding(source_id: str) -> ParserBinding:
    canonical = canonical_source_id(source_id)
    try:
        return PARSER_BINDINGS[canonical]
    except KeyError as exc:
        raise ValueError(f"no payroll parser binding for source_id: {source_id}") from exc


def _migrate_legacy_state_once(
    state_store: SourceStateStore,
    *,
    source_id: str,
) -> None:
    """Materialize canonical state without rewriting immutable legacy evidence."""
    if state_store.load(source_id) is not None:
        return
    for legacy_id in legacy_source_ids_for(source_id):
        legacy = state_store.load(legacy_id)
        if legacy is None:
            continue
        state_store.persist(legacy.model_copy(update={"source_id": source_id}))
        return


def resolve_source_for_reference_year(
    source: SourceSpec,
    *,
    source_id: str,
    reference_year: int,
) -> SourceSpec:
    """Resolve the execution-year URL for a canonical evergreen source."""
    canonical = canonical_source_id(source_id)
    if canonical == RFB_SOURCE_ID:
        if "{year}" not in source.url:
            raise ValueError("RFB annual source URL template lost {year} placeholder")
        return source.model_copy(update={"url": source.url.replace("{year}", str(reference_year))})
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
    source_id = canonical_source_id(source_id)
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
    _migrate_legacy_state_once(state_store, source_id=source_id)
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
