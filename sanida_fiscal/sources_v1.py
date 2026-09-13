from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from hashlib import sha256
import json
from pathlib import Path
from typing import Callable, Mapping

import httpx
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, validate_assignment=True)


class CollectionStatus(str, Enum):
    COLLECTED = "COLLECTED"
    NOT_MODIFIED = "NOT_MODIFIED"
    SOURCE_UNAVAILABLE = "SOURCE_UNAVAILABLE"


class FailureKind(str, Enum):
    TIMEOUT = "TIMEOUT"
    NETWORK_ERROR = "NETWORK_ERROR"
    HTTP_CLIENT_ERROR = "HTTP_CLIENT_ERROR"
    HTTP_SERVER_ERROR = "HTTP_SERVER_ERROR"


class ParseStatus(str, Enum):
    PARSED = "PARSED"
    PARSER_INCOMPATIBLE = "PARSER_INCOMPATIBLE"


class SourceSpec(StrictModel):
    source_id: str = Field(min_length=1)
    url: str = Field(min_length=1)
    role: str = Field(min_length=1)
    machine_readability: str = Field(min_length=1)


class RawSnapshot(StrictModel):
    source_id: str
    source_url: str
    observed_at_utc: datetime
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    relative_path: str
    media_type: str | None = None
    byte_length: int = Field(ge=0)

    @field_validator("observed_at_utc")
    @classmethod
    def utc_only(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
            raise ValueError("observed_at_utc must be UTC")
        return value


class CollectionResult(StrictModel):
    source_id: str
    source_url: str
    observed_at_utc: datetime
    status: CollectionStatus
    attempts: int = Field(ge=1)
    http_status: int | None = None
    failure_kind: FailureKind | None = None
    error_detail: str | None = None
    snapshot: RawSnapshot | None = None

    @field_validator("observed_at_utc")
    @classmethod
    def utc_only(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
            raise ValueError("observed_at_utc must be UTC")
        return value

    @model_validator(mode="after")
    def consistent(self):
        if self.status == CollectionStatus.COLLECTED and self.snapshot is None:
            raise ValueError("COLLECTED requires snapshot")
        if self.status != CollectionStatus.COLLECTED and self.snapshot is not None:
            raise ValueError("non-COLLECTED result cannot claim snapshot")
        if self.status == CollectionStatus.SOURCE_UNAVAILABLE and self.failure_kind is None:
            raise ValueError("SOURCE_UNAVAILABLE requires failure_kind")
        if self.status != CollectionStatus.SOURCE_UNAVAILABLE and self.failure_kind is not None:
            raise ValueError("failure_kind only applies to SOURCE_UNAVAILABLE")
        return self


class NormalizedSourceCandidate(StrictModel):
    source_id: str
    source_url: str
    observed_at_utc: datetime
    snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    snapshot_path: str
    parser_id: str = Field(min_length=1)
    parser_version: str = Field(min_length=1)
    status: ParseStatus
    payload: dict[str, JsonValue] | None = None
    error_detail: str | None = None

    @field_validator("observed_at_utc")
    @classmethod
    def utc_only(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
            raise ValueError("observed_at_utc must be UTC")
        return value

    @model_validator(mode="after")
    def consistent(self):
        if self.status == ParseStatus.PARSED and self.payload is None:
            raise ValueError("PARSED requires payload")
        if self.status == ParseStatus.PARSER_INCOMPATIBLE and not self.error_detail:
            raise ValueError("PARSER_INCOMPATIBLE requires error_detail")
        return self


class ParserIncompatibleError(ValueError):
    pass


class SnapshotStore:
    """Content-addressed immutable storage for raw source bytes."""

    def __init__(self, root: Path):
        self.root = Path(root)

    @staticmethod
    def relative_path(source_id: str, digest: str, suffix: str = ".bin") -> str:
        safe_source = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in source_id)
        return f"{safe_source}/{digest[:2]}/{digest}{suffix}"

    def persist(
        self,
        *,
        source: SourceSpec,
        observed_at_utc: datetime,
        body: bytes,
        media_type: str | None,
        suffix: str = ".bin",
    ) -> RawSnapshot:
        digest = sha256(body).hexdigest()
        relative = self.relative_path(source.source_id, digest, suffix)
        target = self.root / relative
        target.parent.mkdir(parents=True, exist_ok=True)

        if target.exists():
            existing = target.read_bytes()
            if existing != body:
                raise RuntimeError("snapshot hash collision or immutable snapshot corruption")
        else:
            with target.open("xb") as fh:
                fh.write(body)

        return RawSnapshot(
            source_id=source.source_id,
            source_url=source.url,
            observed_at_utc=observed_at_utc,
            sha256=digest,
            relative_path=relative,
            media_type=media_type,
            byte_length=len(body),
        )

    def read(self, snapshot: RawSnapshot) -> bytes:
        body = (self.root / snapshot.relative_path).read_bytes()
        if sha256(body).hexdigest() != snapshot.sha256:
            raise RuntimeError("snapshot integrity check failed")
        return body


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    timeout_seconds: float = 20.0

    def __post_init__(self):
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")


class HttpCollectorV1:
    """HTTP collector only: it never parses legal/fiscal meaning."""

    def __init__(
        self,
        *,
        snapshot_store: SnapshotStore,
        retry_policy: RetryPolicy | None = None,
        transport: httpx.BaseTransport | None = None,
        headers: Mapping[str, str] | None = None,
    ):
        self.snapshot_store = snapshot_store
        self.retry_policy = retry_policy or RetryPolicy()
        self.transport = transport
        self.headers = dict(headers or {})

    def collect(self, source: SourceSpec, *, observed_at_utc: datetime) -> CollectionResult:
        last_kind: FailureKind | None = None
        last_detail: str | None = None
        last_status: int | None = None

        with httpx.Client(
            transport=self.transport,
            headers=self.headers,
            timeout=self.retry_policy.timeout_seconds,
            follow_redirects=True,
        ) as client:
            for attempt in range(1, self.retry_policy.max_attempts + 1):
                try:
                    response = client.get(source.url)
                    last_status = response.status_code

                    if response.status_code == 304:
                        return CollectionResult(
                            source_id=source.source_id,
                            source_url=source.url,
                            observed_at_utc=observed_at_utc,
                            status=CollectionStatus.NOT_MODIFIED,
                            attempts=attempt,
                            http_status=304,
                        )

                    if 200 <= response.status_code < 300:
                        content_type = response.headers.get("content-type")
                        suffix = _suffix_for_content_type(content_type)
                        snapshot = self.snapshot_store.persist(
                            source=source,
                            observed_at_utc=observed_at_utc,
                            body=response.content,
                            media_type=content_type,
                            suffix=suffix,
                        )
                        return CollectionResult(
                            source_id=source.source_id,
                            source_url=source.url,
                            observed_at_utc=observed_at_utc,
                            status=CollectionStatus.COLLECTED,
                            attempts=attempt,
                            http_status=response.status_code,
                            snapshot=snapshot,
                        )

                    if response.status_code == 429 or 500 <= response.status_code < 600:
                        last_kind = FailureKind.HTTP_SERVER_ERROR
                        last_detail = f"http_{response.status_code}"
                        if attempt < self.retry_policy.max_attempts:
                            continue
                    else:
                        return CollectionResult(
                            source_id=source.source_id,
                            source_url=source.url,
                            observed_at_utc=observed_at_utc,
                            status=CollectionStatus.SOURCE_UNAVAILABLE,
                            attempts=attempt,
                            http_status=response.status_code,
                            failure_kind=FailureKind.HTTP_CLIENT_ERROR,
                            error_detail=f"http_{response.status_code}",
                        )

                except httpx.TimeoutException as exc:
                    last_kind = FailureKind.TIMEOUT
                    last_detail = type(exc).__name__
                    if attempt < self.retry_policy.max_attempts:
                        continue
                except httpx.RequestError as exc:
                    last_kind = FailureKind.NETWORK_ERROR
                    last_detail = type(exc).__name__
                    if attempt < self.retry_policy.max_attempts:
                        continue

                return CollectionResult(
                    source_id=source.source_id,
                    source_url=source.url,
                    observed_at_utc=observed_at_utc,
                    status=CollectionStatus.SOURCE_UNAVAILABLE,
                    attempts=attempt,
                    http_status=last_status,
                    failure_kind=last_kind,
                    error_detail=last_detail,
                )

        raise AssertionError("collector loop ended unexpectedly")


def parse_snapshot(
    *,
    snapshot_store: SnapshotStore,
    collection: CollectionResult,
    parser_id: str,
    parser_version: str,
    parser: Callable[[bytes], dict[str, JsonValue]],
) -> NormalizedSourceCandidate:
    if collection.status != CollectionStatus.COLLECTED or collection.snapshot is None:
        raise ValueError("only COLLECTED results can be parsed")

    body = snapshot_store.read(collection.snapshot)
    try:
        payload = parser(body)
    except ParserIncompatibleError as exc:
        return NormalizedSourceCandidate(
            source_id=collection.source_id,
            source_url=collection.source_url,
            observed_at_utc=collection.observed_at_utc,
            snapshot_sha256=collection.snapshot.sha256,
            snapshot_path=collection.snapshot.relative_path,
            parser_id=parser_id,
            parser_version=parser_version,
            status=ParseStatus.PARSER_INCOMPATIBLE,
            error_detail=str(exc),
        )

    return NormalizedSourceCandidate(
        source_id=collection.source_id,
        source_url=collection.source_url,
        observed_at_utc=collection.observed_at_utc,
        snapshot_sha256=collection.snapshot.sha256,
        snapshot_path=collection.snapshot.relative_path,
        parser_id=parser_id,
        parser_version=parser_version,
        status=ParseStatus.PARSED,
        payload=payload,
    )


def load_source_registry(path: Path) -> dict[str, SourceSpec]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    sources = raw.get("sources")
    if not isinstance(sources, list):
        raise ValueError("source registry must contain sources list")

    registry: dict[str, SourceSpec] = {}
    for item in sources:
        if not isinstance(item, dict):
            raise ValueError("source registry entries must be objects")
        spec = SourceSpec(
            source_id=item.get("source_id"),
            url=item.get("url"),
            role=item.get("role"),
            machine_readability=item.get("machine_readability"),
        )
        if spec.source_id in registry:
            raise ValueError(f"duplicate source_id: {spec.source_id}")
        registry[spec.source_id] = spec
    return registry


def _suffix_for_content_type(content_type: str | None) -> str:
    value = (content_type or "").lower()
    if "json" in value:
        return ".json"
    if "html" in value:
        return ".html"
    if "pdf" in value:
        return ".pdf"
    if "text" in value:
        return ".txt"
    return ".bin"
