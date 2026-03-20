import os
import re
import json
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


OUTPUT_FILE = "taxas_bacen.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SanidaTaxasBot/1.1; +https://sanida.com.br)",
    "Accept": "application/json,text/plain,text/html,application/xhtml+xml,*/*",
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


def parse_percent_number(text: str) -> Optional[float]:
    s = (text or "").strip()
    s = s.replace("\xa0", " ")
    s = s.replace("%", "").strip()
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^0-9.]", "", s)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def fetch_b3_cdi_aa() -> Dict[str, Any]:
    url = "https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/consultas/mercado-de-derivativos/indicadores/indicadores-financeiros/"
    ok, code, html = fetch(url)
    if not ok:
        raise RuntimeError(f"B3: falha ao buscar indicadores financeiros (status={code})")

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    # Ex.: "TAXA CDI CETIP - (a.a.). 14,65 %. Atualizado em: 19/03/2026."
    m = re.search(
        r"TAXA\s+CDI\s+CETIP\s*-\s*\(a\.a\.\)\.?\s*([\d\.,]+)\s*%\s*\.?\s*Atualizado\s+em:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})",
        text,
        re.IGNORECASE,
    )

    if not m:
        # fallback mais tolerante
        m = re.search(
            r"TAXA\s+CDI\s+CETIP.*?([\d\.,]+)\s*%.*?Atualizado\s+em:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})",
            text,
            re.IGNORECASE,
        )

    if not m:
        raise RuntimeError("B3: não consegui localizar a TAXA CDI CETIP (a.a.) na página")

    value = parse_percent_number(m.group(1))
    updated_at = m.group(2)

    if value is None:
        raise RuntimeError("B3: valor do CDI inválido")

    return {
        "value": round(value, 2),
        "updated_at_brt": updated_at,
        "url": url,
    }


def fetch_rates() -> Dict[str, Any]:
    selic = sgs_last(432)
    cdi_info = fetch_b3_cdi_aa()

    return {
        "selic": round(selic, 2),
        "cdi": round(float(cdi_info["value"]), 2),
        "cdi_basis": "b3_cdi_cetip_aa",
        "sources": {
            "selic": "sgs_432",
            "cdi": "b3_indicadores_financeiros",
        },
        "source_meta": {
            "cdi_updated_at_brt": cdi_info["updated_at_brt"],
            "cdi_url": cdi_info["url"],
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
            "url": taxas["source_meta"]["cdi_url"],
            "updated_at_brt": taxas["source_meta"]["cdi_updated_at_brt"],
        }

        payload = {
            "schema_version": "1.1.0",
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
        "schema_version": "1.1.0",
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
