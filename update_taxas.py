import os
import re
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple
from ftplib import FTP

import requests


OUTPUT_FILE = "taxas_bacen.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SanidaTaxasBot/1.3; +https://sanida.com.br)",
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}

SSLVERIFY = os.getenv("SFA_SSLVERIFY", "1").strip() not in ("0", "false", "False")
TIMEOUT = int(os.getenv("SFA_TIMEOUT", "25").strip())
RETRIES = int(os.getenv("SFA_RETRIES", "3").strip())

# Mantido propositalmente como fallback estático por enquanto.
FALLBACK_SELIC = 15.00
FALLBACK_CDI = 14.90


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def round_tree(obj):
    if isinstance(obj, dict):
        return {k: round_tree(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [round_tree(v) for v in obj]
    if isinstance(obj, float):
        return round(obj, 6)
    return obj


def fetch(url: str) -> Tuple[bool, int, str]:
    last_err = ""
    for i in range(1, RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT, verify=SSLVERIFY)
            code = int(r.status_code)
            if code == 200:
                return True, code, r.text
            if 500 <= code < 600:
                last_err = f"http_{code}"
                time.sleep(0.4 * i)
                continue
            return False, code, r.text[:500]
        except Exception as e:
            last_err = f"exc_{type(e).__name__}"
            time.sleep(0.4 * i)
            continue
    return False, 0, last_err


def fetch_json(url: str) -> Tuple[bool, int, Any]:
    ok, code, body = fetch(url)
    if not ok:
        return False, code, body
    try:
        return True, code, json.loads(body)
    except Exception:
        return False, code, {"error": "invalid_json", "body_sample": body[:200]}


def sgs_last(code: int) -> float:
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados/ultimos/1?formato=json"
    ok, http_code, data = fetch_json(url)
    if not ok:
        raise RuntimeError(f"BCB: falha SGS {code} (status={http_code})")
    if not isinstance(data, list) or not data or "valor" not in data[0]:
        raise RuntimeError(f"BCB: shape inválido SGS {code}")
    v = str(data[0]["valor"]).replace(",", ".")
    return float(v)


def parse_b3_numeric_rate(raw: str) -> float:
    """
    Exemplo esperado:
    000002320  -> 23,20%
    000001465  -> 14,65%
    """
    text = (raw or "").strip()
    m = re.search(r"(\d{7,9})", text)
    if not m:
        raise RuntimeError(f"B3 FTP: formato inesperado: {text[:120]!r}")

    value = int(m.group(1)) / 100.0
    if not (0 <= value <= 60):
        raise RuntimeError(f"B3 FTP: CDI fora de faixa: {value}")

    return round(value, 2)


def ftp_read_text(host: str, path: str, filename: str) -> str:
    ftp = FTP()
    ftp.connect(host=host, port=21, timeout=TIMEOUT)
    ftp.login()
    ftp.cwd(path)

    chunks: List[bytes] = []
    ftp.retrbinary(f"RETR {filename}", chunks.append)
    ftp.quit()

    return b"".join(chunks).decode("latin-1", errors="ignore").strip()


def fetch_b3_cdi_ftp() -> Dict[str, Any]:
    """
    Teste objetivo:
    ftp://ftp.cetip.com.br/MediaCDI/TAXA_DI.TXT

    Se falhar, tenta /Public/TAXA_DI.TXT só por segurança.
    """
    host = "ftp.cetip.com.br"

    candidates = [
        ("/MediaCDI", "TAXA_DI.TXT"),
        ("/Public", "TAXA_DI.TXT"),
    ]

    last_exc = None

    for path, filename in candidates:
        for i in range(1, RETRIES + 1):
            try:
                raw = ftp_read_text(host, path, filename)
                if not raw:
                    raise RuntimeError(f"B3 FTP: arquivo vazio em {path}/{filename}")

                value = parse_b3_numeric_rate(raw)

                return {
                    "value": value,
                    "ftp_host": host,
                    "ftp_path": path,
                    "ftp_filename": filename,
                    "raw_sample": raw[:120],
                }

            except Exception as e:
                last_exc = e
                time.sleep(0.4 * i)
                continue

    raise RuntimeError(f"B3 FTP: não consegui obter a Taxa DI Over ({last_exc})")


def fetch_rates() -> Dict[str, Any]:
    selic = sgs_last(432)
    cdi_info = fetch_b3_cdi_ftp()

    return {
        "selic": round(selic, 2),
        "cdi": round(float(cdi_info["value"]), 2),
        "cdi_basis": "b3_ftp_taxa_di_txt_aa",
        "sources": {
            "selic": "sgs_432",
            "cdi": "b3_ftp_taxa_di_txt",
        },
        "source_meta": {
            "cdi_ftp_host": cdi_info["ftp_host"],
            "cdi_ftp_path": cdi_info["ftp_path"],
            "cdi_ftp_filename": cdi_info["ftp_filename"],
            "cdi_raw_sample": cdi_info["raw_sample"],
        },
    }


def validate_payload(d: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errs: List[str] = []

    if not isinstance(d, dict):
        return False, ["payload:not_dict"]

    if "meta" not in d or not isinstance(d["meta"], dict):
        errs.append("meta:missing_or_bad")
    else:
        if not isinstance(d["meta"].get("generated_at_utc"), str):
            errs.append("meta.generated_at_utc:missing_or_bad")

    taxas = d.get("taxas")
    if not isinstance(taxas, dict):
        errs.append("taxas:missing_or_bad")
    else:
        if not isinstance(taxas.get("selic"), (int, float)):
            errs.append("taxas.selic:missing_or_bad")
        if not isinstance(taxas.get("cdi"), (int, float)):
            errs.append("taxas.cdi:missing_or_bad")

        if isinstance(taxas.get("selic"), (int, float)) and not (0 <= taxas["selic"] <= 60):
            errs.append("taxas.selic:out_of_range")
        if isinstance(taxas.get("cdi"), (int, float)) and not (0 <= taxas["cdi"] <= 60):
            errs.append("taxas.cdi:out_of_range")

    return (len(errs) == 0), errs


def read_existing() -> Optional[Dict[str, Any]]:
    if not os.path.exists(OUTPUT_FILE):
        return None
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def write_json_atomic(data: Dict[str, Any]) -> None:
    tmp = OUTPUT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, OUTPUT_FILE)


def main():
    existing = read_existing()
    existing_ok = False
    if isinstance(existing, dict):
        existing_ok, _ = validate_payload(existing)

    errors: List[str] = []
    warnings: List[str] = []
    sources: Dict[str, Any] = {}

    try:
        taxas = fetch_rates()

        sources["selic"] = {"source": taxas["sources"]["selic"]}
        sources["cdi"] = {
            "source": taxas["sources"]["cdi"],
            "ftp_host": taxas["source_meta"]["cdi_ftp_host"],
            "ftp_path": taxas["source_meta"]["cdi_ftp_path"],
            "ftp_filename": taxas["source_meta"]["cdi_ftp_filename"],
            "raw_sample": taxas["source_meta"]["cdi_raw_sample"],
        }

        payload = {
            "schema_version": "1.3.0",
            "meta": {
                "generated_at_utc": now_utc_iso(),
                "sources": sources,
                "errors": [],
                "warnings": [],
            },
            "taxas": {
                "selic": float(taxas["selic"]),
                "cdi": float(taxas["cdi"]),
                "cdi_basis": taxas.get("cdi_basis"),
            },
        }

        payload = round_tree(payload)
        ok, verrs = validate_payload(payload)

        if ok:
            write_json_atomic(payload)
            print("OK: taxas_bacen.json atualizado.")
            return

        errors.extend(verrs)
    except Exception as e:
        errors.append(str(e))

    if existing_ok:
        print("WARN: coleta falhou, mantendo last-good (nenhuma alteração no JSON).")
        print("Erros:", errors)
        return

    fallback_payload = {
        "schema_version": "1.3.0",
        "meta": {
            "generated_at_utc": now_utc_iso(),
            "sources": sources,
            "errors": errors,
            "warnings": warnings + ["minimal_fallback_written", "static_reference_values"],
        },
        "taxas": {
            "selic": FALLBACK_SELIC,
            "cdi": FALLBACK_CDI,
            "cdi_basis": "fallback",
        },
    }

    write_json_atomic(round_tree(fallback_payload))
    print("WARN: sem last-good; escrevi fallback mínimo em taxas_bacen.json.")


if __name__ == "__main__":
    main()
