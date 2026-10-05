"""Capture and independently verify official AF01 SCI Cosit 8/2015 evidence.

The original Receita PDF remains preferred. If its legacy binary URL now serves
only the generic Normas SPA, the tool may fall back to Receita's current Normas
API, but only after exact structured identity and material legal-marker checks.

Read-only: does not publish a fiscal release, change source state, create issues
or push to Git. Every accepted response is stored byte-for-byte with SHA-256.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
import re
import sys
import unicodedata

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.authority_evidence_v12 import (
    RFB_NORMAS_API_SEARCH_URL,
    _validate_sci_cosit_8_2015_api_payload,
)

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


def _api_search_body() -> dict:
    return {
        "tipoData": "dataPublicacao",
        "dataInicio": "",
        "dataFim": "",
        "anoAto": "2015",
        "numeroAto": "8",
        "apenasAtosVigentes": False,
        "apenasAtosInternos": False,
        "publicado": True,
        "internet": True,
        "orgaosSelecionados": "",
        "tiposAtosSelecionados": "",
        "refino": {},
        "paginacaoPaginaAtual": 1,
        "paginacaoQuantidadePorPagina": 50,
        "skipAggregations": False,
        "ordenacaoColuna": "",
        "ordenacaoDirecao": "",
        "tipoPesquisa": "formulario",
        "termo": "",
    }


def _persist_immutable(output: Path, body: bytes, suffix: str) -> tuple[str, Path]:
    digest = sha256(body).hexdigest()
    path = output / f"{digest}{suffix}"
    if path.exists() and sha256(path.read_bytes()).hexdigest() != digest:
        raise RuntimeError("immutable AF01 evidence filename/hash mismatch")
    if not path.exists():
        path.write_bytes(body)
    return digest, path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("/tmp/af01-official-evidence"))
    parser.add_argument("--from-file", type=Path, help="Verify previously captured official PDF; never substitute scraped text")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)

    observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    legacy_probe = None

    if args.from_file:
        pdf_bytes = args.from_file.read_bytes()
        _text, pages = validate_official_pdf(pdf_bytes)
        digest, path = _persist_immutable(output, pdf_bytes, ".pdf")
        report = {
            "status": "BINARY_AND_SEMANTICALLY_VERIFIED",
            "source_id": SOURCE_ID,
            "canonical_url": SOURCE_URL,
            "retrieval_method": "manual_official_pdf",
            "observed_at_utc": observed_at,
            "sha256": digest,
            "evidence_filename": path.name,
            "page_count": pages,
            "mandatory_markers": list(REQUIRED),
            "official_text_extracted": True,
            "publication_approved": False,
        }
    else:
        response = requests.get(
            SOURCE_URL,
            headers={
                "Accept": "application/pdf",
                "User-Agent": "SanidaLegalEvidence/1.1 (+https://sanida.com.br)",
            },
            timeout=(8, 20),
            allow_redirects=True,
        )
        response.raise_for_status()
        try:
            _text, pages = validate_official_pdf(response.content)
        except ValueError:
            legacy_probe = {
                "http_status": response.status_code,
                "content_type": response.headers.get("content-type", ""),
                "received_bytes": len(response.content),
                "response_sha256": sha256(response.content).hexdigest(),
                "redirected_from_official": response.url != SOURCE_URL,
                "requested_official_url": SOURCE_URL,
            }
            print("AF01_PDF_ENDPOINT_NOT_BINARY " + json.dumps(legacy_probe, sort_keys=True), flush=True)

            api_response = requests.post(
                RFB_NORMAS_API_SEARCH_URL,
                json=_api_search_body(),
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Content-Type": "application/json; charset=UTF-8",
                    "User-Agent": "SanidaLegalEvidence/1.1 (+https://sanida.com.br)",
                },
                timeout=(8, 30),
                allow_redirects=True,
            )
            api_response.raise_for_status()
            _validate_sci_cosit_8_2015_api_payload(api_response.content)
            digest, path = _persist_immutable(output, api_response.content, ".json")
            report = {
                "status": "OFFICIAL_API_RECORD_AND_SEMANTICALLY_VERIFIED",
                "source_id": SOURCE_ID,
                "canonical_legacy_pdf_url": SOURCE_URL,
                "official_api_url": RFB_NORMAS_API_SEARCH_URL,
                "official_api_idAto": 65843,
                "retrieval_method": "api",
                "observed_at_utc": observed_at,
                "sha256": digest,
                "evidence_filename": path.name,
                "mandatory_identity": {
                    "numeroAto": "8",
                    "anoAto": "2015",
                    "tipoAto": "SCI",
                    "orgao": "Cosit",
                    "dataAto": "12/06/2015",
                    "dataPublicacao": "06/07/2015",
                },
                "legacy_pdf_endpoint_probe": legacy_probe,
                "publication_approved": False,
            }
        else:
            digest, path = _persist_immutable(output, response.content, ".pdf")
            report = {
                "status": "BINARY_AND_SEMANTICALLY_VERIFIED",
                "source_id": SOURCE_ID,
                "canonical_url": SOURCE_URL,
                "retrieval_method": "http_pdf",
                "observed_at_utc": observed_at,
                "sha256": digest,
                "evidence_filename": path.name,
                "page_count": pages,
                "mandatory_markers": list(REQUIRED),
                "official_text_extracted": True,
                "publication_approved": False,
            }

    report["operational_note"] = (
        "Review eSocial rubric 1023 separately; do not double-count constitutional third."
    )
    (output / "af01-evidence.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    )
    print(
        "AF01_OFFICIAL_EVIDENCE_VERIFIED "
        + json.dumps(
            {
                "source_id": report["source_id"],
                "status": report["status"],
                "sha256": report["sha256"],
                "observed_at_utc": report["observed_at_utc"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
