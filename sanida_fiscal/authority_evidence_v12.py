from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Mapping

from .inss_employee_v1 import (
    PARSER_ID as INSS_PARSER_ID,
    PARSER_VERSION as INSS_PARSER_VERSION,
    parse_inss_employee_2026_snapshot,
)
from .rfb_irrf_v1 import (
    PARSER_ID as RFB_PARSER_ID,
    PARSER_VERSION as RFB_PARSER_VERSION,
    parse_rfb_irrf_2026_snapshot,
)
from .sources_v1 import (
    CollectionStatus,
    HttpCollectorV1,
    NormalizedSourceCandidate,
    ParseStatus,
    RetryPolicy,
    SnapshotStore,
    SourceSpec,
    load_source_registry,
    parse_snapshot,
)
from .types_v1 import EvidenceObservation, RetrievalMethod, SourceObservationStatus, SourceRole


class AuthorityEvidenceError(RuntimeError):
    pass


RFB_SOURCE_ID = "RFB_IRRF_TABLE_2026"
INSS_SOURCE_ID = "INSS_TABLE_2026"


# Parameter parsers are deliberately limited to Phase 4's known source structures.
# Structural sources are snapshotted but never interpreted automatically here.
PARSER_BINDINGS = {
    RFB_SOURCE_ID: (
        RFB_PARSER_ID,
        RFB_PARSER_VERSION,
        parse_rfb_irrf_2026_snapshot,
    ),
    INSS_SOURCE_ID: (
        INSS_PARSER_ID,
        INSS_PARSER_VERSION,
        parse_inss_employee_2026_snapshot,
    ),
}


PREFERRED_RULE_SOURCES = {
    "inss.employee.progressive_table": INSS_SOURCE_ID,
    "thirteenth.inss.separate_assessment": INSS_SOURCE_ID,
    "irrf.monthly.progressive_table": RFB_SOURCE_ID,
    "irrf.dependent_deduction": RFB_SOURCE_ID,
    "irrf.simplified_monthly_discount": RFB_SOURCE_ID,
    "irrf.reduction.2026": RFB_SOURCE_ID,
    "vacation.irrf.reduction.2026": "PLANALTO_LEI_15270_2025",
}


@dataclass(frozen=True)
class AuthorityEvidenceBundle:
    evidence_by_source: dict[str, EvidenceObservation]
    normalized_candidates: dict[str, NormalizedSourceCandidate]
    selected_source_by_rule: dict[str, str]


def _require_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise AuthorityEvidenceError("authority evidence timestamp must be UTC")


def _load_inventory(path: Path) -> dict[str, dict[str, Any]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    rules = raw.get("rules")
    if not isinstance(rules, list):
        raise AuthorityEvidenceError("rule inventory has no rules list")
    indexed: dict[str, dict[str, Any]] = {}
    for item in rules:
        if not isinstance(item, dict) or not isinstance(item.get("rule_id"), str):
            raise AuthorityEvidenceError("rule inventory entry is invalid")
        indexed[item["rule_id"]] = item
    if len(indexed) != 32:
        raise AuthorityEvidenceError("authority collection requires closed 32-rule inventory")
    return indexed


def _registry_metadata(path: Path) -> dict[str, dict[str, Any]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    sources = raw.get("sources")
    if not isinstance(sources, list):
        raise AuthorityEvidenceError("source registry has no sources list")
    return {
        item["source_id"]: item
        for item in sources
        if isinstance(item, dict) and isinstance(item.get("source_id"), str)
    }


def _source_priority(source_id: str, registry: Mapping[str, Mapping[str, Any]]) -> tuple[int, int, str]:
    item = registry[source_id]
    readability = str(item.get("machine_readability", ""))
    role = str(item.get("role", ""))
    # Prefer stable text/HTML over PDFs for durable bootstrap evidence when both
    # sources are already authorized by the Phase 1 inventory.
    readability_rank = {
        "html": 0,
        "html_table": 0,
        "html_search_result": 1,
        "api": 1,
        "dataset": 1,
        "pdf_text": 2,
    }.get(readability, 3)
    role_rank = {
        "normative_primary": 0,
        "administrative_norm": 1,
        "official_operational": 2,
        "official_reference_case": 3,
    }.get(role, 4)
    return readability_rank, role_rank, source_id


def select_authority_sources(
    *,
    source_registry_path: Path,
    rule_inventory_path: Path,
) -> dict[str, str]:
    inventory = _load_inventory(rule_inventory_path)
    registry = _registry_metadata(source_registry_path)
    selected: dict[str, str] = {}

    for rule_id, rule in sorted(inventory.items()):
        if rule.get("rule_class") == "technical_contract_rule":
            continue
        source_ids = rule.get("source_ids")
        if not isinstance(source_ids, list) or not source_ids:
            raise AuthorityEvidenceError(f"{rule_id}: legal/fiscal rule has no authorized source_ids")
        if any(source_id not in registry for source_id in source_ids):
            missing = sorted(source_id for source_id in source_ids if source_id not in registry)
            raise AuthorityEvidenceError(f"{rule_id}: source_ids absent from registry: {missing}")

        preferred = PREFERRED_RULE_SOURCES.get(rule_id)
        if preferred is not None:
            if preferred not in source_ids:
                raise AuthorityEvidenceError(
                    f"{rule_id}: preferred source {preferred} is not authorized by inventory"
                )
            selected[rule_id] = preferred
            continue

        selected[rule_id] = min(
            (str(source_id) for source_id in source_ids),
            key=lambda source_id: _source_priority(source_id, registry),
        )

    if len(selected) != 30:
        raise AuthorityEvidenceError(
            f"expected authority selection for 30 external rules, got {len(selected)}"
        )
    return selected


def _retrieval_method(snapshot_media_type: str | None) -> RetrievalMethod:
    media = (snapshot_media_type or "").lower()
    if "pdf" in media:
        return RetrievalMethod.HTTP_PDF
    return RetrievalMethod.HTTP_HTML


def collect_authority_evidence(
    *,
    source_registry_path: Path,
    rule_inventory_path: Path,
    snapshot_root: Path,
    observed_at_utc: datetime,
    retry_policy: RetryPolicy | None = None,
) -> AuthorityEvidenceBundle:
    """Collect one authorized official snapshot per external rule.

    Sources are deduplicated across rules. Only RFB_IRRF_TABLE_2026 and
    INSS_TABLE_2026 are parsed; every other source remains uninterpreted raw
    evidence and therefore cannot silently change structural semantics.
    """
    _require_utc(observed_at_utc)
    selected = select_authority_sources(
        source_registry_path=source_registry_path,
        rule_inventory_path=rule_inventory_path,
    )
    registry = load_source_registry(source_registry_path)
    required_source_ids = sorted(set(selected.values()) | set(PARSER_BINDINGS))

    store = SnapshotStore(Path(snapshot_root))
    collector = HttpCollectorV1(
        snapshot_store=store,
        retry_policy=retry_policy or RetryPolicy(max_attempts=3, timeout_seconds=30.0),
        headers={
            "User-Agent": "SanidaFiscalEvidence/1.2 (+https://sanida.com.br/)",
            "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.5",
        },
    )

    evidence: dict[str, EvidenceObservation] = {}
    candidates: dict[str, NormalizedSourceCandidate] = {}

    for source_id in required_source_ids:
        source = registry.get(source_id)
        if source is None:
            raise AuthorityEvidenceError(f"source absent from registry: {source_id}")
        result = collector.collect(source, observed_at_utc=observed_at_utc)
        if result.status != CollectionStatus.COLLECTED or result.snapshot is None:
            detail = result.error_detail or result.failure_kind.value if result.failure_kind else result.status.value
            raise AuthorityEvidenceError(f"{source_id}: official source unavailable: {detail}")

        parser_id = None
        parser_version = None
        if source_id in PARSER_BINDINGS:
            parser_id, parser_version, parser = PARSER_BINDINGS[source_id]
            candidate = parse_snapshot(
                snapshot_store=store,
                collection=result,
                parser_id=parser_id,
                parser_version=parser_version,
                parser=parser,
            )
            if candidate.status != ParseStatus.PARSED:
                raise AuthorityEvidenceError(
                    f"{source_id}: parser incompatible: {candidate.error_detail}"
                )
            candidates[source_id] = candidate

        evidence[source_id] = EvidenceObservation(
            source_id=source_id,
            role=SourceRole(source.role),
            observed_at_utc=observed_at_utc,
            status=SourceObservationStatus.AVAILABLE,
            retrieval_method=_retrieval_method(result.snapshot.media_type),
            snapshot_sha256=result.snapshot.sha256,
            snapshot_path=result.snapshot.relative_path,
            parser_id=parser_id,
            parser_version=parser_version,
        )

    for parser_source in PARSER_BINDINGS:
        if parser_source not in candidates:
            raise AuthorityEvidenceError(f"required normalized candidate missing: {parser_source}")

    return AuthorityEvidenceBundle(
        evidence_by_source=evidence,
        normalized_candidates=candidates,
        selected_source_by_rule=selected,
    )
