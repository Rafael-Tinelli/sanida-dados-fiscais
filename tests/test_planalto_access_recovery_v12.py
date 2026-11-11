"""Planalto access-denied recovery remains bounded and fail-closed."""

from datetime import datetime, timezone

import pytest
import requests

from sanida_fiscal.authority_evidence_v12 import (
    AUTHORITY_HEADERS, PLANALTO_BROWSER_HEADERS, _collect_authority_source,
    _is_planalto_access_denied_html, _validate_authority_identity,
)
from sanida_fiscal.sources_v1 import (
    CollectionResult, CollectionStatus, FailureKind, RetryPolicy,
    SnapshotStore, SourceSpec,
)

NOW = datetime(2026, 11, 9, 20, tzinfo=timezone.utc)
URL = "https://www.planalto.gov.br/ccivil_03/decreto-lei/del5452compilado.htm"
DENIED = (
    b"<html><head><title>Acesso Negado!</title></head><body>"
    b"Esta requisicao foi bloqueada. Diretoria de Tecnologia da Presidencia."
    b"</body></html>"
)
VALID = (
    "<html><body>DECRETO-LEI Nº 5.452. Aprova a "
    "Consolidação das Leis do Trabalho.</body></html>"
).encode("utf-8")
UNKNOWN = b"<html><body>Generic homepage</body></html>"
MARKERS = {"identity_marker_groups": [
    ["consolidacao das leis do trabalho"], ["decreto-lei", "5.452"],
]}


def _source(name="PLANALTO_CLT"):
    return SourceSpec(
        source_id=name, url=URL, role="normative_primary",
        machine_readability="html",
    )


class Primary:
    def __init__(self, store, source, body=None, status=200):
        self.source, self.status = source, status
        self.snapshot = (
            store.persist(source=source, observed_at_utc=NOW, body=body,
                          media_type="text/html", suffix=".html")
            if body is not None else None
        )

    def collect(self, source, *, observed_at_utc):
        assert source == self.source and observed_at_utc == NOW
        collected = self.snapshot is not None
        return CollectionResult(
            source_id=source.source_id, source_url=source.url,
            observed_at_utc=NOW,
            status=(CollectionStatus.COLLECTED if collected
                    else CollectionStatus.SOURCE_UNAVAILABLE),
            attempts=1, http_status=self.status,
            snapshot=self.snapshot,
            failure_kind=(None if collected else (
                FailureKind.HTTP_SERVER_ERROR if self.status == 429
                else FailureKind.HTTP_CLIENT_ERROR)),
            error_detail=None if collected else f"http_{self.status}",
        )


class Response:
    def __init__(self, body):
        self.status_code = 200
        self.content = body
        self.headers = {"content-type": "text/html; charset=utf-8"}


def _run(source, primary, store):
    return _collect_authority_source(
        source_id=source.source_id, source=source, collector=primary,
        store=store, observed_at_utc=NOW,
        retry_policy=RetryPolicy(max_attempts=3, timeout_seconds=1),
        headers=AUTHORITY_HEADERS,
    )


def test_denial_detection_is_specific():
    assert _is_planalto_access_denied_html(DENIED, "text/html")
    assert not _is_planalto_access_denied_html(VALID, "text/html")
    assert not _is_planalto_access_denied_html(UNKNOWN, "text/html")
    assert not _is_planalto_access_denied_html(
        b"<html><body>Acesso negado em processo judicial.</body></html>", "text/html"
    )
    assert not _is_planalto_access_denied_html(DENIED, "application/pdf")


def test_http_200_denial_retries_same_official_url_once(monkeypatch, tmp_path):
    source, store = _source(), SnapshotStore(tmp_path)
    calls = []

    def get(url, *, headers, timeout, allow_redirects):
        calls.append((url, headers, timeout, allow_redirects))
        return Response(VALID)

    monkeypatch.setattr(requests, "get", get)
    result = _run(source, Primary(store, source, DENIED), store)
    assert result.status == CollectionStatus.COLLECTED
    assert calls == [(URL, PLANALTO_BROWSER_HEADERS, 1, True)]
    raw = store.read(result.snapshot)
    assert raw == VALID
    _validate_authority_identity(
        source_id=source.source_id, metadata=MARKERS,
        body=raw, media_type=result.snapshot.media_type,
    )


def test_repeated_denial_cannot_be_published(monkeypatch, tmp_path):
    source, store = _source(), SnapshotStore(tmp_path)
    calls = []
    def get(url, *, headers, timeout, allow_redirects):
        calls.append(url)
        return Response(DENIED)
    monkeypatch.setattr(requests, "get", get)
    result = _run(source, Primary(store, source, DENIED), store)
    assert calls == [URL]
    assert result.status == CollectionStatus.SOURCE_UNAVAILABLE
    assert result.snapshot is None
    assert result.error_detail == "planalto_access_denied_html"


@pytest.mark.parametrize("http_status", [403, 406])
def test_explicit_denials_retry_once(monkeypatch, tmp_path, http_status):
    calls = []
    def get(url, *, headers, timeout, allow_redirects):
        calls.append(url)
        return Response(VALID)
    monkeypatch.setattr(requests, "get", get)
    source, store = _source(), SnapshotStore(tmp_path)
    result = _run(source, Primary(store, source, status=http_status), store)
    assert result.status == CollectionStatus.COLLECTED
    assert calls == [URL]


def test_rate_limit_never_triggers_browser_retry(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("429 retried"))
    source, store = _source(), SnapshotStore(tmp_path)
    result = _run(source, Primary(store, source, status=429), store)
    assert result.http_status == 429
    assert result.status == CollectionStatus.SOURCE_UNAVAILABLE


def test_generic_wrong_document_does_not_retry(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("wrong document retried"))
    source, store = _source(), SnapshotStore(tmp_path)
    result = _run(source, Primary(store, source, UNKNOWN), store)
    assert result.status == CollectionStatus.COLLECTED
    with pytest.raises(Exception, match="does not prove source identity"):
        _validate_authority_identity(
            source_id=source.source_id, metadata=MARKERS,
            body=store.read(result.snapshot), media_type=result.snapshot.media_type,
        )


def test_non_planalto_is_never_retried(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", lambda *a, **k: pytest.fail("wrong source retried"))
    source, store = _source("NOT_PLANALTO"), SnapshotStore(tmp_path)
    result = _run(source, Primary(store, source, DENIED), store)
    assert result.status == CollectionStatus.COLLECTED
