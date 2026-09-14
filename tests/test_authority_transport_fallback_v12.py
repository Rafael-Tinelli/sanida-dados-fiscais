from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
import requests

from sanida_fiscal.authority_evidence_v12 import (
    AUTHORITY_HEADERS,
    PLANALTO_BROWSER_FALLBACK_SOURCE_IDS,
    PLANALTO_BROWSER_HEADERS,
    PLANALTO_CLT_SOURCE_ID,
    _collect_authority_source,
)
from sanida_fiscal.sources_v1 import (
    CollectionResult,
    CollectionStatus,
    FailureKind,
    RetryPolicy,
    SnapshotStore,
    SourceSpec,
)


NOW = datetime(2026, 9, 14, 13, 45, tzinfo=timezone.utc)
PLANALTO_SOURCES = {
    "PLANALTO_LEI_15270_2025": "https://www.planalto.gov.br/ccivil_03/_ato2023-2026/2025/lei/l15270.htm",
    PLANALTO_CLT_SOURCE_ID: "https://www.planalto.gov.br/ccivil_03/decreto-lei/del5452compilado.htm",
    "PLANALTO_LEI_4090_1962": "https://www.planalto.gov.br/ccivil_03/leis/l4090.htm",
    "PLANALTO_LEI_4749_1965": "https://www.planalto.gov.br/ccivil_03/leis/l4749.htm",
    "PLANALTO_DECRETO_10854_2021": "https://www.planalto.gov.br/ccivil_03/_ato2019-2022/2021/decreto/d10854.htm",
}


class _PrimaryRemoteProtocolFailure:
    def collect(self, source: SourceSpec, *, observed_at_utc: datetime) -> CollectionResult:
        return CollectionResult(
            source_id=source.source_id,
            source_url=source.url,
            observed_at_utc=observed_at_utc,
            status=CollectionStatus.SOURCE_UNAVAILABLE,
            attempts=3,
            failure_kind=FailureKind.NETWORK_ERROR,
            error_detail="RemoteProtocolError",
        )


class _PrimaryOtherFailure:
    def collect(self, source: SourceSpec, *, observed_at_utc: datetime) -> CollectionResult:
        return CollectionResult(
            source_id=source.source_id,
            source_url=source.url,
            observed_at_utc=observed_at_utc,
            status=CollectionStatus.SOURCE_UNAVAILABLE,
            attempts=3,
            failure_kind=FailureKind.NETWORK_ERROR,
            error_detail="ConnectError",
        )


class _Response:
    status_code = 200
    headers = {"content-type": "text/html; charset=windows-1252"}
    content = b"<html><body>Planalto official snapshot</body></html>"


def _source(source_id: str = PLANALTO_CLT_SOURCE_ID, url: str | None = None) -> SourceSpec:
    return SourceSpec(
        source_id=source_id,
        url=url or PLANALTO_SOURCES[source_id],
        role="normative_primary",
        machine_readability="html",
    )


@pytest.mark.parametrize("source_id,url", sorted(PLANALTO_SOURCES.items()))
def test_planalto_remote_protocol_error_retries_same_url_with_browser_profile(
    monkeypatch,
    tmp_path: Path,
    source_id: str,
    url: str,
) -> None:
    calls: list[tuple[str, dict[str, str]]] = []

    def fake_get(request_url, *, headers, timeout, allow_redirects):
        calls.append((request_url, headers))
        assert timeout == 1
        assert allow_redirects is True
        return _Response()

    monkeypatch.setattr(requests, "get", fake_get)
    store = SnapshotStore(tmp_path / "snapshots")

    result = _collect_authority_source(
        source_id=source_id,
        source=_source(source_id, url),
        collector=_PrimaryRemoteProtocolFailure(),
        store=store,
        observed_at_utc=NOW,
        retry_policy=RetryPolicy(max_attempts=2, timeout_seconds=1),
        headers=AUTHORITY_HEADERS,
    )

    assert result.status == CollectionStatus.COLLECTED
    assert result.source_url == url
    assert calls == [(url, PLANALTO_BROWSER_HEADERS)]
    assert PLANALTO_BROWSER_HEADERS != AUTHORITY_HEADERS
    assert result.snapshot is not None
    assert result.snapshot.source_url == url
    assert result.snapshot.relative_path.endswith(".html")
    assert (tmp_path / "snapshots" / result.snapshot.relative_path).read_bytes() == _Response.content


def test_planalto_fallback_allowlist_matches_closed_registry_sources() -> None:
    assert PLANALTO_BROWSER_FALLBACK_SOURCE_IDS == frozenset(PLANALTO_SOURCES)


def test_transport_fallback_is_not_used_for_other_network_errors(monkeypatch, tmp_path: Path) -> None:
    def forbidden_get(*args, **kwargs):
        raise AssertionError("requests fallback must not run for non-allowlisted error")

    monkeypatch.setattr(requests, "get", forbidden_get)
    store = SnapshotStore(tmp_path / "snapshots")

    result = _collect_authority_source(
        source_id=PLANALTO_CLT_SOURCE_ID,
        source=_source(),
        collector=_PrimaryOtherFailure(),
        store=store,
        observed_at_utc=NOW,
        retry_policy=RetryPolicy(max_attempts=2, timeout_seconds=1),
        headers=AUTHORITY_HEADERS,
    )

    assert result.status == CollectionStatus.SOURCE_UNAVAILABLE
    assert result.error_detail == "ConnectError"


def test_remote_protocol_fallback_is_not_used_for_non_planalto_source(monkeypatch, tmp_path: Path) -> None:
    def forbidden_get(*args, **kwargs):
        raise AssertionError("browser fallback must remain Planalto-only")

    monkeypatch.setattr(requests, "get", forbidden_get)
    source = SourceSpec(
        source_id="RFB_TEST_SOURCE",
        url="https://www.gov.br/receitafederal/teste",
        role="official_operational",
        machine_readability="html",
    )

    result = _collect_authority_source(
        source_id=source.source_id,
        source=source,
        collector=_PrimaryRemoteProtocolFailure(),
        store=SnapshotStore(tmp_path / "snapshots"),
        observed_at_utc=NOW,
        retry_policy=RetryPolicy(max_attempts=1, timeout_seconds=1),
        headers=AUTHORITY_HEADERS,
    )

    assert result.status == CollectionStatus.SOURCE_UNAVAILABLE
    assert result.error_detail == "RemoteProtocolError"


def test_transport_fallback_never_changes_authority_url(monkeypatch, tmp_path: Path) -> None:
    requested: list[str] = []

    def fake_get(url, *, headers, timeout, allow_redirects):
        requested.append(url)
        assert headers == PLANALTO_BROWSER_HEADERS
        return _Response()

    monkeypatch.setattr(requests, "get", fake_get)
    source = _source("PLANALTO_DECRETO_10854_2021")

    result = _collect_authority_source(
        source_id=source.source_id,
        source=source,
        collector=_PrimaryRemoteProtocolFailure(),
        store=SnapshotStore(tmp_path / "snapshots"),
        observed_at_utc=NOW,
        retry_policy=RetryPolicy(max_attempts=1, timeout_seconds=1),
        headers=AUTHORITY_HEADERS,
    )

    assert requested == [source.url]
    assert result.source_url == source.url
