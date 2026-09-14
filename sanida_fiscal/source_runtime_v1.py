from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Callable

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

from .sources_v1 import (
    CollectionResult,
    CollectionStatus,
    HttpCollectorV1,
    NormalizedSourceCandidate,
    ParseStatus,
    SnapshotStore,
    SourceSpec,
    parse_snapshot,
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, validate_assignment=True)


class SourcePipelineState(StrictModel):
    schema_version: str = "1.1.0"
    source_id: str
    source_url: str
    last_observed_at_utc: datetime
    last_collection_status: CollectionStatus
    last_http_status: int | None = None
    etag: str | None = None
    last_modified: str | None = None
    consecutive_source_failures: int = Field(default=0, ge=0)
    last_source_error: str | None = None
    last_collected_snapshot_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    last_collected_snapshot_path: str | None = None
    last_parse_status: ParseStatus | None = None
    parser_id: str | None = None
    parser_version: str | None = None
    consecutive_parser_failures: int = Field(default=0, ge=0)
    last_parser_error: str | None = None
    last_parsed_at_utc: datetime | None = None
    last_parsed_snapshot_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    last_parsed_snapshot_path: str | None = None
    last_successful_parser_id: str | None = None
    last_successful_parser_version: str | None = None
    last_candidate_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    last_candidate_path: str | None = None

    @field_validator("last_observed_at_utc", "last_parsed_at_utc")
    @classmethod
    def utc_only(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
            raise ValueError("operational timestamps must be UTC")
        return value


class SourcePipelineRun(StrictModel):
    collection: CollectionResult
    candidate: NormalizedSourceCandidate | None = None
    state: SourcePipelineState
    raw_snapshot_unchanged: bool | None = None
    candidate_fingerprint_unchanged: bool | None = None


class SourceStateStore:
    """Atomic JSON persistence for operational collection/parser state."""

    def __init__(self, root: Path):
        self.root = Path(root)

    @staticmethod
    def _safe_source_id(source_id: str) -> str:
        return "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in source_id)

    def path_for(self, source_id: str) -> Path:
        return self.root / f"{self._safe_source_id(source_id)}.json"

    def load(self, source_id: str) -> SourcePipelineState | None:
        path = self.path_for(source_id)
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        state = SourcePipelineState.model_validate(data)
        if state.source_id != source_id:
            raise RuntimeError("state file source_id mismatch")
        return state

    def persist(self, state: SourcePipelineState) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        target = self.path_for(state.source_id)
        tmp = target.with_suffix(target.suffix + ".tmp")
        payload = state.model_dump(mode="json")
        tmp.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(tmp, target)


def canonical_candidate_bytes(candidate: NormalizedSourceCandidate) -> bytes:
    if candidate.status != ParseStatus.PARSED or candidate.payload is None:
        raise ValueError("candidate fingerprint requires PARSED payload")
    return json.dumps(
        candidate.payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_candidate_sha256(candidate: NormalizedSourceCandidate) -> str:
    return sha256(canonical_candidate_bytes(candidate)).hexdigest()


class CandidateStore:
    """Content-addressed immutable storage for normalized candidate payloads."""

    def __init__(self, root: Path):
        self.root = Path(root)

    @staticmethod
    def relative_path(source_id: str, digest: str) -> str:
        safe_source = SourceStateStore._safe_source_id(source_id)
        return f"{safe_source}/{digest[:2]}/{digest}.json"

    def persist(self, candidate: NormalizedSourceCandidate) -> str:
        body = canonical_candidate_bytes(candidate)
        digest = sha256(body).hexdigest()
        relative = self.relative_path(candidate.source_id, digest)
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists():
            existing = target.read_bytes()
            if existing != body:
                raise RuntimeError("candidate hash collision or immutable candidate corruption")
        else:
            with target.open("xb") as fh:
                fh.write(body)
        return relative

    def read(self, *, relative_path: str, expected_sha256: str) -> dict[str, JsonValue]:
        body = (self.root / relative_path).read_bytes()
        if sha256(body).hexdigest() != expected_sha256:
            raise RuntimeError("candidate integrity check failed")
        payload = json.loads(body.decode("utf-8"))
        if not isinstance(payload, dict):
            raise RuntimeError("candidate evidence payload must be an object")
        return payload


def run_source_pipeline(
    *,
    source: SourceSpec,
    collector: HttpCollectorV1,
    snapshot_store: SnapshotStore,
    state_store: SourceStateStore,
    observed_at_utc: datetime,
    parser_id: str,
    parser_version: str,
    parser: Callable[[bytes], dict[str, JsonValue]],
    candidate_store: CandidateStore | None = None,
    use_http_validators: bool = True,
) -> SourcePipelineRun:
    previous = state_store.load(source.source_id)
    can_use_http_validators = bool(
        use_http_validators
        and previous
        and previous.last_parse_status == ParseStatus.PARSED
        and previous.parser_id == parser_id
        and previous.parser_version == parser_version
    )
    collection = collector.collect(
        source,
        observed_at_utc=observed_at_utc,
        conditional_etag=previous.etag if can_use_http_validators else None,
        conditional_last_modified=previous.last_modified if can_use_http_validators else None,
    )

    previous_snapshot_sha = previous.last_collected_snapshot_sha256 if previous else None
    previous_candidate_sha = previous.last_candidate_sha256 if previous else None

    if collection.status == CollectionStatus.SOURCE_UNAVAILABLE:
        state = SourcePipelineState(
            source_id=source.source_id,
            source_url=source.url,
            last_observed_at_utc=observed_at_utc,
            last_collection_status=collection.status,
            last_http_status=collection.http_status,
            etag=collection.etag or (previous.etag if previous else None),
            last_modified=collection.last_modified or (previous.last_modified if previous else None),
            consecutive_source_failures=(previous.consecutive_source_failures if previous else 0) + 1,
            last_source_error=collection.error_detail or (collection.failure_kind.value if collection.failure_kind else None),
            last_collected_snapshot_sha256=previous_snapshot_sha,
            last_collected_snapshot_path=previous.last_collected_snapshot_path if previous else None,
            last_parse_status=previous.last_parse_status if previous else None,
            parser_id=previous.parser_id if previous else None,
            parser_version=previous.parser_version if previous else None,
            consecutive_parser_failures=previous.consecutive_parser_failures if previous else 0,
            last_parser_error=previous.last_parser_error if previous else None,
            last_parsed_at_utc=previous.last_parsed_at_utc if previous else None,
            last_parsed_snapshot_sha256=previous.last_parsed_snapshot_sha256 if previous else None,
            last_parsed_snapshot_path=previous.last_parsed_snapshot_path if previous else None,
            last_successful_parser_id=previous.last_successful_parser_id if previous else None,
            last_successful_parser_version=previous.last_successful_parser_version if previous else None,
            last_candidate_sha256=previous_candidate_sha,
            last_candidate_path=previous.last_candidate_path if previous else None,
        )
        state_store.persist(state)
        return SourcePipelineRun(collection=collection, state=state)

    if collection.status == CollectionStatus.NOT_MODIFIED:
        if previous is None:
            raise RuntimeError("NOT_MODIFIED without previous operational state")
        state = previous.model_copy(
            update={
                "last_observed_at_utc": observed_at_utc,
                "last_collection_status": collection.status,
                "last_http_status": collection.http_status,
                "etag": collection.etag or previous.etag,
                "last_modified": collection.last_modified or previous.last_modified,
                "consecutive_source_failures": 0,
                "last_source_error": None,
            }
        )
        state_store.persist(state)
        return SourcePipelineRun(
            collection=collection,
            state=state,
            raw_snapshot_unchanged=True,
            candidate_fingerprint_unchanged=True if previous_candidate_sha else None,
        )

    if collection.snapshot is None:
        raise AssertionError("COLLECTED result must include snapshot")

    raw_unchanged = previous_snapshot_sha == collection.snapshot.sha256 if previous_snapshot_sha else None
    candidate = parse_snapshot(
        snapshot_store=snapshot_store,
        collection=collection,
        parser_id=parser_id,
        parser_version=parser_version,
        parser=parser,
    )

    if candidate.status == ParseStatus.PARSER_INCOMPATIBLE:
        state = SourcePipelineState(
            source_id=source.source_id,
            source_url=source.url,
            last_observed_at_utc=observed_at_utc,
            last_collection_status=collection.status,
            last_http_status=collection.http_status,
            etag=collection.etag,
            last_modified=collection.last_modified,
            consecutive_source_failures=0,
            last_source_error=None,
            last_collected_snapshot_sha256=collection.snapshot.sha256,
            last_collected_snapshot_path=collection.snapshot.relative_path,
            last_parse_status=candidate.status,
            parser_id=parser_id,
            parser_version=parser_version,
            consecutive_parser_failures=(previous.consecutive_parser_failures if previous else 0) + 1,
            last_parser_error=candidate.error_detail,
            last_parsed_at_utc=previous.last_parsed_at_utc if previous else None,
            last_parsed_snapshot_sha256=previous.last_parsed_snapshot_sha256 if previous else None,
            last_parsed_snapshot_path=previous.last_parsed_snapshot_path if previous else None,
            last_successful_parser_id=previous.last_successful_parser_id if previous else None,
            last_successful_parser_version=previous.last_successful_parser_version if previous else None,
            last_candidate_sha256=previous_candidate_sha,
            last_candidate_path=previous.last_candidate_path if previous else None,
        )
        state_store.persist(state)
        return SourcePipelineRun(
            collection=collection,
            candidate=candidate,
            state=state,
            raw_snapshot_unchanged=raw_unchanged,
        )

    candidate_sha = canonical_candidate_sha256(candidate)
    candidate_path = candidate_store.persist(candidate) if candidate_store is not None else None
    candidate_unchanged = previous_candidate_sha == candidate_sha if previous_candidate_sha else None
    state = SourcePipelineState(
        source_id=source.source_id,
        source_url=source.url,
        last_observed_at_utc=observed_at_utc,
        last_collection_status=collection.status,
        last_http_status=collection.http_status,
        etag=collection.etag,
        last_modified=collection.last_modified,
        consecutive_source_failures=0,
        last_source_error=None,
        last_collected_snapshot_sha256=collection.snapshot.sha256,
        last_collected_snapshot_path=collection.snapshot.relative_path,
        last_parse_status=candidate.status,
        parser_id=parser_id,
        parser_version=parser_version,
        consecutive_parser_failures=0,
        last_parser_error=None,
        last_parsed_at_utc=observed_at_utc,
        last_parsed_snapshot_sha256=collection.snapshot.sha256,
        last_parsed_snapshot_path=collection.snapshot.relative_path,
        last_successful_parser_id=parser_id,
        last_successful_parser_version=parser_version,
        last_candidate_sha256=candidate_sha,
        last_candidate_path=candidate_path,
    )
    state_store.persist(state)
    return SourcePipelineRun(
        collection=collection,
        candidate=candidate,
        state=state,
        raw_snapshot_unchanged=raw_unchanged,
        candidate_fingerprint_unchanged=candidate_unchanged,
    )
