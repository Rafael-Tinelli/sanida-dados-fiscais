"""Identical, read-only eSocial transport probe; run via JSON bundle on each network.

The input bundle pins the same host, destination IPv4, HTTPS path, CA PEM,
and TLS versions for the GitHub runner and HostGator. No insecure TLS mode,
no proxy, no modifications to production, and no source publication.
"""
import hashlib
import json
import os
import platform
import socket
import ssl
import sys
import time


def digest(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def details(exc):
    return {
        "type": type(exc).__module__ + "." + type(exc).__name__,
        "message": str(exc)[:250],
        "errno": getattr(exc, "errno", None),
        "ssl_reason": getattr(exc, "reason", None),
    }


def dns(host):
    try:
        addresses = sorted({
            row[4][0] for row in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
        })
        return {"ok": True, "addresses": addresses}
    except Exception as exc:
        return {"ok": False, "error": details(exc)}


def tcp(ip):
    start = time.monotonic()
    try:
        with socket.create_connection((ip, 443), timeout=8):
            pass
        return {"ok": True, "duration_seconds": round(time.monotonic() - start, 2)}
    except Exception as exc:
        return {
            "ok": False,
            "duration_seconds": round(time.monotonic() - start, 2),
            "error": details(exc),
        }


def https(host, ip, path, ca_pem, version):
    start = time.monotonic()
    stage = "tls_configuration"
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.minimum_version = version
        ctx.maximum_version = version
        ctx.verify_mode = ssl.CERT_REQUIRED
        ctx.check_hostname = True
        ctx.load_verify_locations(cadata=ca_pem)
        stage = "tcp_connect"
        with socket.create_connection((ip, 443), timeout=10) as raw:
            raw.settimeout(12)
            stage = "tls_handshake"
            with ctx.wrap_socket(raw, server_hostname=host) as secure:
                certificate = secure.getpeercert(binary_form=True)
                result = {
                    "tls_version": secure.version(),
                    "cipher": secure.cipher()[0],
                    "peer_cert_sha256": hashlib.sha256(certificate).hexdigest(),
                    "sni": host,
                    "verified": True,
                }
                stage = "http_request"
                request = (
                    "GET " + path + " HTTP/1.1\r\n"
                    + "Host: " + host + "\r\n"
                    + "User-Agent: Sanida-Esocial-Transport-Diagnosis/1.0\r\n"
                    + "Accept: text/html\r\n"
                    + "Connection: close\r\n\r\n"
                )
                secure.sendall(request.encode("ascii"))
                stage = "http_first_bytes"
                response = secure.recv(1024)
                status_line = response.split(b"\r\n", 1)[0].decode("latin-1", errors="replace")
                result["http_status_line"] = status_line[:140]
                result["bytes_received"] = len(response)
                result["ok"] = bool(response)
                result["duration_seconds"] = round(time.monotonic() - start, 2)
                return result
    except Exception as exc:
        return {
            "ok": False,
            "failed_stage": stage,
            "duration_seconds": round(time.monotonic() - start, 2),
            "error": details(exc),
        }


bundle = BUNDLE
assert bundle["host"] == "www.gov.br"
assert bundle["ip"].count(".") == 3
assert bundle["path"].startswith("/esocial/")
ca = bundle["ca_pem"]
host = bundle["host"]
ip = bundle["ip"]
result = {
    "network": os.environ.get("NETWORK_LABEL", "origin_not_specified"),
    "utc_timestamp": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
    "hostname": host,
    "pinned_ipv4": ip,
    "request_path": bundle["path"],
    "ca_sha256": digest(ca),
    "probe_sha256": digest(bundle["source"]),
    "python": sys.version.split()[0],
    "openssl": ssl.OPENSSL_VERSION,
    "system": platform.system(),
    "dns": dns(host),
    "tcp_ipv4": tcp(ip),
    "tls12": https(host, ip, bundle["path"], ca, ssl.TLSVersion.TLSv1_2),
    "tls13": https(host, ip, bundle["path"], ca, ssl.TLSVersion.TLSv1_3),
}
print(json.dumps(result, sort_keys=True, ensure_ascii=False), flush=True)
