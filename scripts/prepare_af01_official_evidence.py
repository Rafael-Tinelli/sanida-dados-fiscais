"""Capture and independently verify the official AF01 SCI Cosit 8/2015 PDF.

Read-only: does not publish a fiscal release, change source state, create issues
or push to Git. For blocked/HTML responses it exits non-zero without claiming
that official binary evidence was verified.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import re
import unicodedata

import requests

SOURCE_ID = "RFB_SCI_COSIT_8_2015"
SOURCE_URL = (
    "https://normas.receita.fazenda.gov.br/sijut2consulta/"
    "anexoOutros.action?idArquivoBinario=36769"
)
REQUIRED = (
    "solucao de consulta interna",
    "terco constitucional de ferias",
    "contribuicao previdenciaria incide sobre o valor integral",
    "conversao de parte do periodo de ferias em abono pecuniario",
    "ferias indenizadas",
    "o valor do adicional constitucional",
    "inclusive o incidente sobre abono pecuniario",
    "tributados pelo imposto sobre a renda",
)


def normalize(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", value.lower()).strip()


def validate_official_pdf(pdf_bytes: bytes) -> tuple[str, int]:
    if not pdf_bytes.startswith(b"%PDF-") or len(pdf_bytes) < 1000:
        raise ValueError("official endpoint did not return a PDF document")
    try:
        from pypdf import PdfReader
    except ImportError as exc:
        raise RuntimeError("Install pypdf to validate the official PDF") from exc
    pdf = PdfReader(BytesIO(pdf_bytes), strict=True)
    if pdf.is_encrypted:
        raise ValueError("encrypted official source requires manual review")
    text = normalize(" ".join(page.extract_text() or "" for page in pdf.pages))
    missing = [marker for marker in REQUIRED if marker not in text]
    if missing:
        raise ValueError("official PDF text does not contain mandatory legal markers: " + "; ".join(missing))
    if "2015" not in text or "8" not in text:
        raise ValueError("SCI date/number cannot be confirmed from the PDF")
    return text, len(pdf.pages)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("/tmp/af01-official-evidence"))
    parser.add_argument("--from-file", type=Path, help="Verify previously captured official PDF; never substitute scraped text")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if args.from_file:
        pdf_bytes = args.from_file.read_bytes()
    else:
        response = requests.get(
            SOURCE_URL,
            headers={"Accept": "application/pdf", "User-Agent": "SanidaLegalEvidence/1.0 (+https://sanida.com.br)"},
            timeout=(8, 20),
            allow_redirects=True,
        )
        response.raise_for_status()
        pdf_bytes = response.content
    _text, pages = validate_official_pdf(pdf_bytes)
    digest = sha256(pdf_bytes).hexdigest()
    path = output / (digest + ".pdf")
    if path.exists() and sha256(path.read_bytes()).hexdigest() != digest:
        raise RuntimeError("immutable AF01 PDF filename/hash mismatch")
    path.write_bytes(pdf_bytes)
    report = {
        "status": "BINARY_AND_SEMANTICALLY_VERIFIED",
        "source_id": SOURCE_ID,
        "canonical_url": SOURCE_URL,
        "observed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "sha256": digest,
        "pdf_filename": path.name,
        "page_count": pages,
        "mandatory_markers": list(REQUIRED),
        "official_text_extracted": True,
        "operational_note": "Review eSocial rubric 1023 separately; do not double-count constitutional third.",
        "publication_approved": False,
    }
    (output / "af01-evidence.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print("AF01_PDF_VERIFIED " + json.dumps({key: report[key] for key in ("source_id", "sha256", "page_count", "observed_at_utc")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
