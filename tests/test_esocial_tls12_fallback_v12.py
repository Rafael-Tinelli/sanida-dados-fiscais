from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
import ssl

import httpx
import pytest

import sanida_fiscal.authority_evidence_v12 as authority
from sanida_fiscal.sources_v1 import (
    CollectionResult,
    CollectionStatus,
    FailureKind,
    RetryPolicy,
    SnapshotStore,
    SourceSpec,
    load_source_registry,
)

NOW = datetime(2026, 11, 25, 20, 0, tzinfo=timezone.utc)
SOURCE_ID = "ESOCIAL_TABLES_S13_NT07_2026"
SOURCE_URL = (
    "https://www.gov.br/esocial/pt-br/documentacao-tecnica/"
    "leiautes-esocial-versao-s-1-3-nt-07-2026/tabelas.html"
)
SOURCE = SourceSpec(
    source_id=SOURCE_ID,
    url=SOURCE_URL,
    role="official_operational",
    machine_readability="html_table",
)
MEI_SOURCE_ID = "ESOCIAL_WEB_MEI_2026"
MEI_SOURCE = load_source_registry(Path("docs/source-registry-v1.json"))[MEI_SOURCE_ID]


@pytest.fixture(params=[SOURCE, MEI_SOURCE], ids=["tables", "manual-mei"])
def authorized_source(request):
    return request.param


def test_esocial_tls12_allowlist_is_explicit_and_matches_registered_sources():
    assert authority.ESOCIAL_TLS12_FALLBACK_SOURCE_IDS == frozenset({SOURCE_ID, MEI_SOURCE_ID})


class Primary:
    def __init__(
        self,
        *,
        status: CollectionStatus = CollectionStatus.SOURCE_UNAVAILABLE,
        failure_kind: FailureKind | None = FailureKind.NETWORK_ERROR,
        error_detail: str | None = "ConnectError",
        http_status: int | None = None,
    ):
        self.status = status
        self.failure_kind = failure_kind
        self.error_detail = error_detail
        self.http_status = http_status
        self.calls = 0

    def collect(self, source, *, observed_at_utc):
        self.calls += 1
        if self.status == CollectionStatus.COLLECTED:
            raise AssertionError("success source must use a real mock collector")
        return CollectionResult(
            source_id=source.source_id,
            source_url=source.url,
            observed_at_utc=observed_at_utc,
            status=self.status,
            attempts=3,
            http_status=self.http_status,
            failure_kind=self.failure_kind,
            error_detail=self.error_detail,
        )


def _collect(source, primary, tmp_path):
    store = SnapshotStore(tmp_path / "snapshots")
    result = authority._collect_authority_source(
        source_id=source.source_id,
        source=source,
        collector=primary,
        store=store,
        observed_at_utc=NOW,
        retry_policy=RetryPolicy(max_attempts=3, timeout_seconds=1),
        headers=authority.AUTHORITY_HEADERS,
    )
    return result, store


def _stub_transport(monkeypatch, handler):
    seen = []

    def transport_factory(*, verify, trust_env):
        assert isinstance(verify, ssl.SSLContext)
        assert verify.verify_mode == ssl.CERT_REQUIRED
        assert verify.check_hostname is True
        assert verify.minimum_version == ssl.TLSVersion.TLSv1_2
        assert verify.maximum_version == ssl.TLSVersion.TLSv1_2
        assert trust_env is False
        seen.append("TLS12")
        return httpx.MockTransport(handler)

    monkeypatch.setattr(authority.httpx, "HTTPTransport", transport_factory)
    return seen


def test_esocial_connect_error_retries_same_official_url_once_with_verified_tls12(
    monkeypatch, tmp_path, capsys, authorized_source
):
    body = b"<html><body>eSocial Tabela 03 Tabela 19 Tabela 21</body></html>"
    requests_seen = []

    def handler(req):
        requests_seen.append((str(req.url), req.headers.get("user-agent")))
        return httpx.Response(200, content=body, headers={"content-type": "text/html"})

    context_seen = _stub_transport(monkeypatch, handler)
    primary = Primary()
    result, store = _collect(authorized_source, primary, tmp_path)

    assert primary.calls == 1
    assert context_seen == ["TLS12"]
    assert requests_seen == [(authorized_source.url, authority.AUTHORITY_HEADERS["User-Agent"])]
    assert result.status == CollectionStatus.COLLECTED
    assert result.attempts == 4  # 3 standard attempts + exactly 1 fallback
    assert result.http_status == 200
    assert result.snapshot is not None
    assert result.source_url == authorized_source.url
    assert result.snapshot.source_url == authorized_source.url
    assert result.snapshot.sha256 == sha256(body).hexdigest()
    assert store.read(result.snapshot) == body
    log = capsys.readouterr().out
    assert "ESOCIAL_TLS12_FALLBACK=COLLECTED" in log
    assert f"source_id={authorized_source.source_id}" in log


@pytest.mark.parametrize(
    ("kind", "detail", "http_status"),
    [
        (FailureKind.NETWORK_ERROR, "RemoteProtocolError", None),
        (FailureKind.TIMEOUT, "ConnectTimeout", None),
        (FailureKind.HTTP_CLIENT_ERROR, "http_403", 403),
        (FailureKind.HTTP_CLIENT_ERROR, "http_406", 406),
        (FailureKind.HTTP_SERVER_ERROR, "http_429", 429),
        (FailureKind.HTTP_SERVER_ERROR, "http_503", 503),
        (FailureKind.NETWORK_ERROR, "ConnectError", 503),
    ],
)
def test_esocial_only_connect_error_without_http_response_triggers_fallback(
    monkeypatch, tmp_path, kind, detail, http_status, authorized_source
):
    monkeypatch.setattr(
        authority.httpx,
        "HTTPTransport",
        lambda **kwargs: pytest.fail("TLS12 transport must not be constructed"),
    )
    result, _ = _collect(
        authorized_source, Primary(failure_kind=kind, error_detail=detail, http_status=http_status), tmp_path
    )
    assert result.status == CollectionStatus.SOURCE_UNAVAILABLE
    assert result.error_detail == detail
    assert result.attempts == 3


@pytest.mark.parametrize("source_id", ["ESOCIAL_UNREGISTERED", "RFB_QA_IRPF_2026", "INSS_TABLE_CURRENT"])
def test_other_authorities_do_not_retry_tls12(monkeypatch, tmp_path, source_id):
    monkeypatch.setattr(
        authority.httpx,
        "HTTPTransport",
        lambda **kwargs: pytest.fail("TLS12 is restricted to the two explicitly authorized source IDs"),
    )
    source = SOURCE.model_copy(update={"source_id": source_id})
    result, _ = _collect(source, Primary(), tmp_path)
    assert result.status == CollectionStatus.SOURCE_UNAVAILABLE
    assert result.error_detail == "ConnectError"


def test_esocial_url_cannot_be_substituted_with_other_authority_id(monkeypatch, tmp_path):
    monkeypatch.setattr(
        authority.httpx,
        "HTTPTransport",
        lambda **kwargs: pytest.fail("Source ID mismatch cannot trigger fallback"),
    )
    source = SOURCE.model_copy(update={"source_id": "ESOCIAL_WEB_MEI_2026"})
    result = authority._collect_authority_source(
        source_id=SOURCE_ID,
        source=source,
        collector=Primary(),
        store=SnapshotStore(tmp_path),
        observed_at_utc=NOW,
        retry_policy=RetryPolicy(max_attempts=1, timeout_seconds=1),
        headers=authority.AUTHORITY_HEADERS,
    )
    assert result.status == CollectionStatus.SOURCE_UNAVAILABLE


@pytest.mark.parametrize(
    ("response", "expected_kind", "expected_detail"),
    [
        (httpx.Response(403, content=b"blocked"), FailureKind.HTTP_CLIENT_ERROR, "http_403"),
        (httpx.Response(429, content=b"slow down"), FailureKind.HTTP_SERVER_ERROR, "http_429"),
    ],
)
def test_esocial_tls12_http_rejection_is_fail_closed(
    monkeypatch, tmp_path, capsys, response, expected_kind, expected_detail, authorized_source
):
    seen = _stub_transport(monkeypatch, lambda req: response)
    result, _ = _collect(authorized_source, Primary(), tmp_path)
    assert seen == ["TLS12"]
    assert result.status == CollectionStatus.SOURCE_UNAVAILABLE
    assert result.snapshot is None
    assert result.attempts == 4
    assert result.http_status == response.status_code
    assert result.failure_kind == expected_kind
    assert result.error_detail == expected_detail
    log = capsys.readouterr().out
    assert "ESOCIAL_TLS12_FALLBACK=BLOCKED" in log
    assert f"source_id={authorized_source.source_id}" in log
    assert list((tmp_path / "snapshots").rglob("*")) == [] if (tmp_path / "snapshots").exists() else True


def test_esocial_tls12_network_failure_is_fail_closed_without_snapshot(monkeypatch, tmp_path, authorized_source):
    def reset(req):
        raise httpx.ConnectError("[Errno 104] Connection reset by peer", request=req)

    seen = _stub_transport(monkeypatch, reset)
    result, store = _collect(authorized_source, Primary(), tmp_path)
    assert seen == ["TLS12"]
    assert result.status == CollectionStatus.SOURCE_UNAVAILABLE
    assert result.snapshot is None
    assert result.attempts == 4
    assert result.failure_kind == FailureKind.NETWORK_ERROR
    assert result.error_detail == "ConnectError"
    assert not store.root.exists()


def test_esocial_successful_first_attempt_does_not_use_tls12(monkeypatch, tmp_path, authorized_source):
    monkeypatch.setattr(
        authority.httpx,
        "HTTPTransport",
        lambda **kwargs: pytest.fail("no TLS12 fallback on healthy primary"),
    )
    body = b"<html>eSocial Tabela 03 Tabela 19 Tabela 21</html>"
    primary = authority.HttpCollectorV1(
        snapshot_store=SnapshotStore(tmp_path / "snapshots"),
        retry_policy=RetryPolicy(max_attempts=1, timeout_seconds=1),
        transport=httpx.MockTransport(lambda req: httpx.Response(200, content=body)),
        headers=authority.AUTHORITY_HEADERS,
    )
    result, _ = _collect(authorized_source, primary, tmp_path)
    assert result.status == CollectionStatus.COLLECTED
    assert result.attempts == 1
