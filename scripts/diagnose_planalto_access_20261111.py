#!/usr/bin/env python3
"""Read-only HTTP/browser diagnostics for official CLT sources.

Does not publish, approve, update contracts, evade CAPTCHAs or rotate proxies.
"""
from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
import json
import os
from pathlib import Path
import re
import sys
import unicodedata
from urllib.parse import urlparse

import httpx
import requests
from bs4 import BeautifulSoup

PLANALTO = "https://www.planalto.gov.br/ccivil_03/decreto-lei/del5452compilado.htm"
CAMARA_HTML = (
    "https://www2.camara.leg.br/legin/fed/declei/1940-1949/"
    "decreto-lei-5452-1-maio-1943-415500-normaatualizada-pe.html"
)
CAMARA_PDF = CAMARA_HTML.replace(".html", ".pdf")
SENADO_INDEX = "https://legis.senado.leg.br/norma/530547"
URLS = {
    "planalto": PLANALTO,
    "planalto_query": PLANALTO + "?utm_source=chatgpt.com",
    "camara_html": CAMARA_HTML,
    "camara_pdf": CAMARA_PDF,
    "senado_index": SENADO_INDEX,
}
PROFILES = {
    "identified": {"User-Agent": "SanidaFiscalEvidence/1.2 (+https://sanida.com.br/)",
                   "Accept": "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.5"},
    "browser_headers": {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.7"
    },
}
ARTICLE_NUMBERS = (129, 130, 134, 137, 143, 146, 147)


def normalize(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", value.lower()).strip()


def diagnose(source: str, status: int | None, final_url: str, raw: bytes, content_type: str) -> dict:
    result = {
        "status_code": status, "final_url": final_url,
        "bytes": len(raw), "sha256": sha256(raw).hexdigest(),
        "content_type": content_type[:95],
    }
    if raw.startswith(b"%PDF-"):
        result.update(document_kind="pdf", pdf_signature=True, identity_verified=False,
                      note="PDF transport only; legal article validation not run")
        return result
    soup = BeautifulSoup(raw[:4_000_000], "html.parser")
    visible = normalize(soup.get_text(" ", strip=True))
    title = normalize(soup.title.get_text(" ", strip=True) if soup.title else "")
    blocked = "esta requisicao foi bloqueada" in visible or (
        title in {"acesso negado", "acesso negado!", "access denied"}
    )
    markers = all(fragment in visible for fragment in (
        "consolidacao das leis do trabalho", "decreto-lei", "5.452"))
    articles = [num for num in ARTICLE_NUMBERS
                if re.search(r"\bart\.\s*" + str(num) + r"\b", visible)]
    legal_source = source in {"planalto", "planalto_query", "camara_html"}
    official_domain = urlparse(final_url).hostname in {
        "www.planalto.gov.br", "www2.camara.leg.br"
    }
    result.update(
        document_kind="html", title=title[:110], access_denied=blocked,
        identity_markers=markers, articles_found=articles,
        identity_verified=bool(
            status == 200 and legal_source and official_domain and
            not blocked and markers and len(articles) >= 5 and len(raw) > 20000
        ),
    )
    return result


def http_probe(name: str, url: str, client: str, profile: str, out: list[dict]) -> None:
    row = {"source": name, "transport": client, "profile": profile}
    try:
        if client == "requests":
            r = requests.get(url, headers=PROFILES[profile], timeout=(8, 22), allow_redirects=True)
            row.update(diagnose(name, r.status_code, r.url, r.content, r.headers.get("content-type", "")))
        else:
            with httpx.Client(headers=PROFILES[profile], timeout=22,
                              follow_redirects=True, http2=(client == "httpx_http2")) as c:
                r = c.get(url)
                row.update(diagnose(name, r.status_code, str(r.url), r.content, r.headers.get("content-type", "")))
    except Exception as exc:
        row["error"] = type(exc).__name__ + ": " + str(exc)[:220]
    out.append(row)
    print("PROBE", json.dumps(row, ensure_ascii=False), flush=True)


def chrome_probe(name: str, url: str, out: list[dict]) -> None:
    row = {"source": name, "transport": "selenium_chromium_headless", "profile": "real_browser"}
    driver = None
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1280,900")
        options.page_load_strategy = "eager"
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(32)
        driver.get(url)
        html = driver.page_source.encode("utf-8")
        row.update(diagnose(name, 200, driver.current_url, html, "text/html; rendered"))
        row["note"] = "Browser HTTP code not visible; 200 is only a synthetic placeholder for document validation"
        row["document_title"] = driver.title[:140]
    except Exception as exc:
        row["error"] = type(exc).__name__ + ": " + str(exc)[:250]
    finally:
        if driver is not None:
            driver.quit()
    out.append(row)
    print("PROBE", json.dumps(row, ensure_ascii=False), flush=True)


def main() -> int:
    out: list[dict] = []
    for name in ("planalto", "planalto_query", "camara_html", "camara_pdf", "senado_index"):
        http_probe(name, URLS[name], "requests", "identified", out)
    for name in ("planalto", "camara_html"):
        http_probe(name, URLS[name], "requests", "browser_headers", out)
        http_probe(name, URLS[name], "httpx_http1", "identified", out)
        http_probe(name, URLS[name], "httpx_http2", "identified", out)
    for name in ("planalto", "camara_html"):
        chrome_probe(name, URLS[name], out)
    report = {
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "github_runner": {
            "name": os.getenv("RUNNER_NAME"), "os": os.getenv("RUNNER_OS"),
            "sha": os.getenv("GITHUB_SHA"), "ref": os.getenv("GITHUB_REF"),
        },
        "probes": out,
        "verified_sources": sorted({x["source"] for x in out if x.get("identity_verified")}),
    }
    path = Path(".validation/planalto-access-diagnostic.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("SUMMARY", json.dumps({"verified_sources": report["verified_sources"],
          "total": len(out)}, ensure_ascii=False), flush=True)
    summary_path = os.getenv("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as f:
            f.write("### CLT official-source access probes\n\n")
            f.write("| Source | Transport | Status | Identity | Blocked / Error |\n|---|---|---|---|---|\n")
            for x in out:
                f.write("| " + " | ".join([
                    x["source"], x["transport"], str(x.get("status_code","—")),
                    "PASS" if x.get("identity_verified") else "NO",
                    ("DENIED" if x.get("access_denied") else
                     str(x.get("error","—"))[:55].replace("|", "/"))
                ]) + " |\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
