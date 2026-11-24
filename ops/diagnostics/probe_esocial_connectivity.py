"""Read-only transport diagnosis of the registered eSocial authority URL.

No snapshots, publication, state updates, proxies, cookies or retries.
Only prints transport-layer observations; never prints proxy credentials.
"""
from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import socket
import ssl
import subprocess
import sys
import time
from urllib.parse import urlsplit

import httpx
import requests


ROOT = Path(__file__).resolve().parents[2]
SOURCE_ID = "ESOCIAL_TABLES_S13_NT07_2026"
registry = json.loads((ROOT / "docs/source-registry-v1.json").read_text("utf-8"))
source = next(row for row in registry["sources"] if row["source_id"] == SOURCE_ID)
URL = source["url"]
HOST = urlsplit(URL).hostname
assert HOST == "www.gov.br"


def summarize_exception(exc: BaseException) -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen and len(items) < 8:
        seen.add(id(current))
        # The strerror/args can identify DNS/TCP/TLS/proxy failures; scrub
        # embedded proxy URLs to avoid exposing authentication credentials.
        message = str(current)
        for env_key in ("HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY", "https_proxy", "http_proxy", "all_proxy"):
            secret = os.environ.get(env_key)
            if secret:
                message = message.replace(secret, "[REDACTED_PROXY]")
        row: dict[str, object] = {
            "type": type(current).__module__ + "." + type(current).__name__,
            "message": message[:360],
        }
        if isinstance(current, OSError):
            row["errno"] = current.errno
        if isinstance(current, ssl.SSLError):
            row["ssl_reason"] = current.reason
        items.append(row)
        current = current.__cause__ or current.__context__
    return items


def run(label: str, fn) -> None:
    print(f"=== {label} ===", flush=True)
    start = time.monotonic()
    try:
        result = fn()
        print(json.dumps({"status": "OK", "elapsed_s": round(time.monotonic()-start, 2), **result}, ensure_ascii=False), flush=True)
    except Exception as exc:
        print(json.dumps({
            "status": "ERROR",
            "elapsed_s": round(time.monotonic()-start, 2),
            "chain": summarize_exception(exc),
        }, ensure_ascii=False), flush=True)


def dns() -> dict[str, object]:
    resolved = socket.getaddrinfo(HOST, 443, type=socket.SOCK_STREAM)
    addr = sorted({(socket.AddressFamily(family).name, sock[0]) for family, _, _, _, sock in resolved})
    print("DNS_ADDRESSES=" + json.dumps(addr, ensure_ascii=False), flush=True)
    return {"addresses": len(addr)}


def tcp() -> dict[str, object]:
    resolved = socket.getaddrinfo(HOST, 443, type=socket.SOCK_STREAM)
    unique: list[tuple[int, tuple]] = []
    seen: set[tuple[int, str]] = set()
    for family, _, _, _, sockaddr in resolved:
        key = (family, sockaddr[0])
        if key in seen:
            continue
        seen.add(key)
        unique.append((family, sockaddr))
    observations = []
    # One representative IPv4 and one IPv6 address at most.
    attempted: set[int] = set()
    for family, sockaddr in unique:
        if family in attempted:
            continue
        attempted.add(family)
        start = time.monotonic()
        try:
            with socket.socket(family, socket.SOCK_STREAM) as conn:
                conn.settimeout(7)
                conn.connect(sockaddr)
            observations.append({"family": socket.AddressFamily(family).name, "result": "CONNECTED", "elapsed_s": round(time.monotonic()-start,2)})
        except Exception as exc:
            observations.append({"family": socket.AddressFamily(family).name, "result": "FAILED", "elapsed_s": round(time.monotonic()-start,2), "chain": summarize_exception(exc)})
    print("TCP_OBSERVATIONS=" + json.dumps(observations, ensure_ascii=False), flush=True)
    return {"attempts": len(observations)}


def tls() -> dict[str, object]:
    ctx = ssl.create_default_context()
    with socket.create_connection((HOST, 443), timeout=9) as sock:
        sock.settimeout(9)
        with ctx.wrap_socket(sock, server_hostname=HOST) as conn:
            cert = conn.getpeercert()
            return {
                "tls_version": conn.version(),
                "cipher": conn.cipher()[0],
                "cert_expiry": cert.get("notAfter"),
                "peer": conn.getpeername()[0],
            }


def httpx_get(*, trust_env: bool, user_agent: str | None = None) -> dict[str, object]:
    headers = {"User-Agent": user_agent} if user_agent else None
    with httpx.Client(timeout=15.0, follow_redirects=True, trust_env=trust_env) as client:
        response = client.get(URL, headers=headers)
        return {
            "status_code": response.status_code,
            "final_host": urlsplit(str(response.url)).hostname,
            "redirects": len(response.history),
            "content_type": response.headers.get("content-type"),
            "content_bytes": len(response.content),
        }


def requests_get() -> dict[str, object]:
    with requests.Session() as client:
        with client.get(URL, timeout=15.0, allow_redirects=True, stream=True) as response:
            return {
                "status_code": response.status_code,
                "final_host": urlsplit(response.url).hostname,
                "redirects": len(response.history),
                "content_type": response.headers.get("content-type"),
            }


def curl_get() -> dict[str, object]:
    proc = subprocess.run(
        ["curl", "--silent", "--show-error", "--location", "--max-time", "25",
         "--connect-timeout", "10", "--output", "/dev/null", "--write-out",
         "STATUS=%{http_code} HOST=%{remote_ip} SSL_VERIFY=%{ssl_verify_result} FINAL=%{url_effective}",
         URL],
        capture_output=True, text=True, timeout=30, check=False,
    )
    return {
        "exit_code": proc.returncode,
        "stdout": proc.stdout[:500],
        "stderr": proc.stderr[:400],
    }


print(json.dumps({
    "kind": "eSocial GitHub Actions transport diagnosis",
    "at_utc": datetime.now(timezone.utc).isoformat(),
    "source_id": SOURCE_ID,
    "url": URL,
    "runner_python": sys.version.split()[0],
    "platform": platform.platform(),
    "httpx_version": httpx.__version__,
    "requests_version": requests.__version__,
    "proxy_environment_present": {
        key: bool(os.environ.get(key)) for key in
        ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy")
    },
}), flush=True)

run("DNS", dns)
run("TCP IPv4/IPv6", tcp)
run("TLS direct, hostname-verified", tls)
run("httpx same as production, trust_env=True", lambda: httpx_get(trust_env=True))
run("httpx direct, trust_env=False", lambda: httpx_get(trust_env=False))
run("httpx browser UA, same registered URL", lambda: httpx_get(trust_env=True, user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"))
run("requests official registered URL", requests_get)
run("curl official registered URL", curl_get)
print("DIAGNOSTIC_FINISHED=TRUE", flush=True)
