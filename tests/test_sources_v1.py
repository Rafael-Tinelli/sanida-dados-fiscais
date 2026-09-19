from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path

import httpx
import pytest

from sanida_fiscal.sources_v1 import (
    CollectionStatus,
    FailureKind,
    HttpCollectorV1,
    ParseStatus,
    ParserIncompatibleError,
    RetryPolicy,
    SnapshotStore,
    SourceSpec,
    load_source_registry,
    parse_snapshot,
)


NOW = datetime(2026, 9, 13, 20, 0, tzinfo=timezone.utc)
SOURCE = SourceSpec(
    source_id="RFB_IRRF_TABLE_CURRENT",
    url="https://example.invalid/irrf",
    role="official_operational",
    machine_readability="html",
)


def collector(tmp_path: Path, handler, *, attempts: int = 3) -> HttpCollectorV1:
    return HttpCollectorV1(
        snapshot_store=SnapshotStore(tmp_path / "raw"),
        retry_policy=RetryPolicy(max_attempts=attempts, timeout_seconds=1),
        transport=httpx.MockTransport(handler),
    )


def test_snapshot_is_content_addressed_and_idempotent(tmp_path: Path):
    store = SnapshotStore(tmp_path)
    body = b"official-source-v1"
    first = store.persist(
        source=SOURCE,
        observed_at_utc=NOW,
        body=body,
        media_type="text/html",
        suffix=".html",
    )
    second = store.persist(
        source=SOURCE,
        observed_at_utc=NOW,
        body=body,
        media_type="text/html",
        suffix=".html",
    )

    assert first.sha256 == sha256(body).hexdigest()
    assert first.relative_path == second.relative_path
    assert (tmp_path / first.relative_path).read_bytes() == body


def test_snapshot_integrity_is_verified_on_read(tmp_path: Path):
    store = SnapshotStore(tmp_path)
    snapshot = store.persist(
        source=SOURCE,
        observed_at_utc=NOW,
        body=b"original",
        media_type="text/plain",
        suffix=".txt",
    )
    (tmp_path / snapshot.relative_path).write_bytes(b"tampered")

    with pytest.raises(RuntimeError, match="integrity"):
        store.read(snapshot)


def test_successful_collection_persists_raw_before_parsing(tmp_path: Path):
    def handler(request: httpx.Request):
        return httpx.Response(200, content=b"<html>ok</html>", headers={"content-type": "text/html"})

    c = collector(tmp_path, handler)
    result = c.collect(SOURCE, observed_at_utc=NOW)

    assert result.status == CollectionStatus.COLLECTED
    assert result.attempts == 1
    assert result.snapshot is not None
    assert result.snapshot.relative_path.endswith(".html")
    assert (tmp_path / "raw" / result.snapshot.relative_path).exists()


def test_http_404_is_fail_closed_without_retry_or_snapshot(tmp_path: Path):
    calls = 0

    def handler(request: httpx.Request):
        nonlocal calls
        calls += 1
        return httpx.Response(404, content=b"missing")

    result = collector(tmp_path, handler).collect(SOURCE, observed_at_utc=NOW)

    assert calls == 1
    assert result.status == CollectionStatus.SOURCE_UNAVAILABLE
    assert result.failure_kind == FailureKind.HTTP_CLIENT_ERROR
    assert result.snapshot is None


def test_http_5xx_retries_and_never_fabricates_snapshot(tmp_path: Path):
    calls = 0

    def handler(request: httpx.Request):
        nonlocal calls
        calls += 1
        return httpx.Response(503, content=b"temporarily unavailable")

    result = collector(tmp_path, handler, attempts=3).collect(SOURCE, observed_at_utc=NOW)

    assert calls == 3
    assert result.status == CollectionStatus.SOURCE_UNAVAILABLE
    assert result.failure_kind == FailureKind.HTTP_SERVER_ERROR
    assert result.http_status == 503
    assert result.snapshot is None


def test_timeout_is_distinct_from_http_failure(tmp_path: Path):
    calls = 0

    def handler(request: httpx.Request):
        nonlocal calls
        calls += 1
        raise httpx.ReadTimeout("timeout", request=request)

    result = collector(tmp_path, handler, attempts=2).collect(SOURCE, observed_at_utc=NOW)

    assert calls == 2
    assert result.status == CollectionStatus.SOURCE_UNAVAILABLE
    assert result.failure_kind == FailureKind.TIMEOUT
    assert result.snapshot is None


def test_parser_incompatible_preserves_raw_snapshot_provenance(tmp_path: Path):
    def handler(request: httpx.Request):
        return httpx.Response(200, content=b"new incompatible layout", headers={"content-type": "text/html"})

    c = collector(tmp_path, handler)
    collection = c.collect(SOURCE, observed_at_utc=NOW)

    def parser(body: bytes):
        raise ParserIncompatibleError("expected table not found")

    candidate = parse_snapshot(
        snapshot_store=c.snapshot_store,
        collection=collection,
        parser_id="rfb-irrf-table",
        parser_version="1.0.0",
        parser=parser,
    )

    assert candidate.status == ParseStatus.PARSER_INCOMPATIBLE
    assert candidate.snapshot_sha256 == collection.snapshot.sha256
    assert candidate.payload is None
    assert "expected table" in candidate.error_detail


def test_parser_success_produces_normalized_candidate_with_snapshot_identity(tmp_path: Path):
    def handler(request: httpx.Request):
        return httpx.Response(200, content=b'{"valor":"15.00"}', headers={"content-type": "application/json"})

    c = collector(tmp_path, handler)
    collection = c.collect(SOURCE, observed_at_utc=NOW)

    candidate = parse_snapshot(
        snapshot_store=c.snapshot_store,
        collection=collection,
        parser_id="example-json",
        parser_version="1.0.0",
        parser=lambda body: {"value": body.decode("utf-8")},
    )

    assert candidate.status == ParseStatus.PARSED
    assert candidate.snapshot_path.endswith(".json")
    assert candidate.snapshot_sha256 == collection.snapshot.sha256
    assert candidate.payload == {"value": '{"valor":"15.00"}'}


def test_failed_collection_cannot_enter_parser(tmp_path: Path):
    def handler(request: httpx.Request):
        return httpx.Response(404)

    c = collector(tmp_path, handler)
    collection = c.collect(SOURCE, observed_at_utc=NOW)

    with pytest.raises(ValueError, match="only COLLECTED"):
        parse_snapshot(
            snapshot_store=c.snapshot_store,
            collection=collection,
            parser_id="x",
            parser_version="1.0.0",
            parser=lambda body: {},
        )


def test_source_registry_loads_phase1_sources_without_loosening_schema():
    registry = load_source_registry(Path("docs/source-registry-v1.json"))

    assert "RFB_IRRF_TABLE_CURRENT" in registry
    assert registry["RFB_IRRF_TABLE_CURRENT"].role == "official_operational"
    assert registry["INSS_TABLE_CURRENT"].machine_readability == "html"
