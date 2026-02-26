import os
import re
import json
import math
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup


OUTPUT_FILE = "dados_fiscais.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SanidaFiscaisBot/2.1; +https://sanida.com.br)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}

# Em Actions, SSL ok. Mantemos verify=True por padrão.
SSLVERIFY = os.getenv("SFA_SSLVERIFY", "1").strip() not in ("0", "false", "False")
TIMEOUT = int(os.getenv("SFA_TIMEOUT", "25").strip())
RETRIES = int(os.getenv("SFA_RETRIES", "3").strip())


def now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def br_money_to_float(s: str) -> float:
    # "1.234,56" -> 1234.56
    s = (s or "").strip()
    s = s.replace("\xa0", " ")
    s = s.replace("R$", "").strip()
    s = s.replace(".", "").replace(",", ".")
    s = re.sub(r"[^0-9\.]", "", s)
    return float(s) if s else 0.0


def br_percent_to_rate(s: str) -> float:
    # "7,5%" -> 0.075
    s = (s or "").strip()
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9\.]", "", s)
    v = float(s) if s else 0.0
    return v / 100.0


def fetch(url: str, expect: str = "text") -> Tuple[bool, int, str]:
    """
    Return: (ok, status_code, body_text)
    Retry on 5xx and network errors.
    """
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
            # 4xx: não adianta retry infinito
            return False, code, r.text[:500]
        except Exception as e:
            last_err = f"exc_{type(e).__name__}"
            time.sleep(0.4 * i)
            continue
    return False, 0, last_err


def fetch_json(url: str) -> Tuple[bool, int, Any]:
    ok, code, body = fetch(url, expect="text")
    if not ok:
        return False, code, body
    try:
        return True, code, json.loads(body)
    except Exception:
        return False, code, {"error": "invalid_json", "body_sample": body[:200]}


def parse_irrf_receita(year: int) -> Dict[str, Any]:
    """
    Fonte oficial: Receita Federal - Tabelas do ano.
    Extrai:
      - Tabela de Incidência Mensal (limite, aliquota, deducao)
      - Dedução mensal por dependente
      - Limite mensal de desconto simplificado
      - Tabela de Redução Mensal (parâmetros)
    """
    url = f"https://www.gov.br/receitafederal/pt-br/assuntos/meu-imposto-de-renda/tabelas/{year}"
    ok, code, html = fetch(url)
    if not ok:
        raise RuntimeError(f"IRRF: falha ao buscar {url} (status={code})")

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    # Incidência Mensal (linhas na página, não necessariamente <table>)
    # Ex:
    # "Até R$ 2.428,80 - -"
    # "De R$ 2.428,81 até R$ 2.826,65 7,5% R$ 182,16"
    # ...
    brackets: List[Dict[str, float]] = []

    m0 = re.search(r"Até\s*R\$\s*([\d\.\,]+)\s*-\s*-", text, re.IGNORECASE)
    if m0:
        brackets.append({"limite": br_money_to_float(m0.group(1)), "aliquota": 0.0, "deducao": 0.0})

    for m in re.finditer(
        r"De\s*R\$\s*([\d\.\,]+)\s*até\s*R\$\s*([\d\.\,]+)\s*([\d\.\,]+)%\s*R\$\s*([\d\.\,]+)",
        text,
        re.IGNORECASE,
    ):
        upper = br_money_to_float(m.group(2))
        rate = br_percent_to_rate(m.group(3))
        ded = br_money_to_float(m.group(4))
        brackets.append({"limite": upper, "aliquota": rate, "deducao": ded})

    m_last = re.search(
        r"Acima\s*de\s*R\$\s*([\d\.\,]+)\s*([\d\.\,]+)%\s*R\$\s*([\d\.\,]+)",
        text,
        re.IGNORECASE,
    )
    if m_last:
        rate = br_percent_to_rate(m_last.group(2))
        ded = br_money_to_float(m_last.group(3))
        brackets.append({"limite": 9e9, "aliquota": rate, "deducao": ded})

    if len(brackets) < 4:
        raise RuntimeError("IRRF: não consegui extrair as faixas de incidência mensal")

    # Ordena por limite
    brackets = sorted(brackets, key=lambda x: x["limite"])

    # --- FIX: manter SOMENTE tabela mensal ---
    # Remove faixas anuais (limites muito altos como 33k/45k/55k)
    monthly = [b for b in brackets if (b["limite"] <= 10000) or (b["limite"] >= 1e9)]
    monthly = sorted(monthly, key=lambda x: x["limite"])

    if len(monthly) < 5:
        raise RuntimeError(f"IRRF: tabela mensal inválida após filtro (len={len(monthly)})")

    has_top = any(abs(b.get("aliquota", 0) - 0.275) < 1e-9 and b.get("limite", 0) >= 1e9 for b in monthly)
    if not has_top:
        raise RuntimeError("IRRF: não encontrei a faixa final 27,5% (infinita) na tabela mensal")

    brackets = monthly

    # Dependente e simplificado
    dep = None
    simpl = None

    md = re.search(r"Dedução\s+mensal\s+por\s+dependente:\s*R\$\s*([\d\.\,]+)", text, re.IGNORECASE)
    if md:
        dep = br_money_to_float(md.group(1))

    ms = re.search(r"Limite\s+mensal\s+de\s+desconto\s+simplificado:\s*R\$\s*([\d\.\,]+)", text, re.IGNORECASE)
    if ms:
        simpl = br_money_to_float(ms.group(1))

    if dep is None or simpl is None:
        raise RuntimeError("IRRF: falha ao extrair dep/simplificado")

    # Redução mensal
    # "até R$ 5.000,00 até R$ 312,89 ..."
    # "de R$ 5.000,01 até R$ 7.350,00 R$ 978,62 - (0,133145 x rendimentos ...)"
    red = {
        "isenta_ate": 5000.00,
        "reduz_ate": 7350.00,
        "max_reducao_ate_5000": None,
        "a": None,
        "b": None,
    }

    mr1 = re.search(r"até\s*R\$\s*5\.000,00\s*até\s*R\$\s*([\d\.\,]+)", text, re.IGNORECASE)
    if mr1:
        red["max_reducao_ate_5000"] = br_money_to_float(mr1.group(1))

    mr2 = re.search(r"R\$\s*([\d\.\,]+)\s*-\s*\(\s*([\d\.\,]+)\s*x\s*rendimentos", text, re.IGNORECASE)
    if mr2:
        red["a"] = br_money_to_float(mr2.group(1))
        # b pode vir como "0,133145"
        btxt = mr2.group(2).replace(",", ".")
        red["b"] = float(re.sub(r"[^0-9\.]", "", btxt)) if btxt else None

    # Não torna obrigatório, mas ajuda muito para 2026
    # Se não conseguir, ainda mantemos incidência + simplificado + dependentes.
    return {
        "url": url,
        "http_code": code,
        "tabela": brackets,
        "dep": dep,
        "simplificado": simpl,
        "reducao_mensal": red,
    }


def parse_inss_gov_2026() -> Dict[str, Any]:
    """
    Fonte oficial (INSS): notícia com teto e faixas 2026.
    """
    url = "https://www.gov.br/inss/pt-br/assuntos/com-reajuste-de-3-9-teto-do-inss-chega-a-r-8-475-55-em-2026"
    ok, code, html = fetch(url)
    if not ok:
        raise RuntimeError(f"INSS: falha ao buscar {url} (status={code})")

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    # Captura faixas em formato textual
    # "7,5% para quem ganha até R$ 1.621,00"
    # "9% para quem ganha entre R$ 1.621,01 e R$ 2.902,84"
    # "12% para quem ganha entre R$ 2.902,85 e R$ 4.354,27"
    # "14% para quem ganha de R$ 4.354,28 até R$ 8.475,55"
    brackets: List[Dict[str, float]] = []

    m1 = re.search(r"([\d\.,]+)%\s*para\s*quem\s*ganha\s*até\s*R\$\s*([\d\.\,]+)", text, re.IGNORECASE)
    if m1:
        brackets.append({"limite": br_money_to_float(m1.group(2)), "aliquota": br_percent_to_rate(m1.group(1))})

    for m in re.finditer(
        r"([\d\.,]+)%\s*para\s*quem\s*ganha\s*entre\s*R\$\s*([\d\.\,]+)\s*e\s*R\$\s*([\d\.\,]+)",
        text,
        re.IGNORECASE,
    ):
        upper = br_money_to_float(m.group(3))
        rate = br_percent_to_rate(m.group(1))
        brackets.append({"limite": upper, "aliquota": rate})

    m_last = re.search(
        r"([\d\.,]+)%\s*para\s*quem\s*ganha\s*de\s*R\$\s*([\d\.\,]+)\s*até\s*R\$\s*([\d\.\,]+)",
        text,
        re.IGNORECASE,
    )
    if m_last:
        brackets.append({"limite": br_money_to_float(m_last.group(3)), "aliquota": br_percent_to_rate(m_last.group(1))})

    brackets = sorted(brackets, key=lambda x: x["limite"])

    if len(brackets) < 3:
        raise RuntimeError("INSS: não consegui extrair as faixas")

    teto = brackets[-1]["limite"]

    return {
        "url": url,
        "http_code": code,
        "tabela": brackets,
        "teto": teto,
    }


def fetch_bcb_rates() -> Dict[str, Any]:
    """
    Selic meta anual (SGS 432) e CDI diário (SGS 12) anualizado em 252 dias úteis.
    """
    def sgs_last(code: int) -> float:
        url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados/ultimos/1?formato=json"
        ok, http_code, data = fetch_json(url)
        if not ok:
            raise RuntimeError(f"BCB: falha SGS {code} (status={http_code})")
        v = str(data[0]["valor"]).replace(",", ".")
        return float(v)

    selic = sgs_last(432)
    cdi_d = sgs_last(12)
    cdi_aa = (pow(1.0 + (cdi_d / 100.0), 252) - 1.0) * 100.0

    return {
        "selic": round(selic, 2),
        "cdi": round(cdi_aa, 2),
        "cdi_basis": "sgs_12_daily_annualized_252",
        "sources": {
            "selic": "sgs_432",
            "cdi_daily": "sgs_12",
        },
    }


def validate_payload(d: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errs: List[str] = []

    # trava anti-tabela-anual misturada
    irrf = d.get("irrf", {})
    if isinstance(irrf, dict):
        tab_check = irrf.get("tabela", [])
        for f in tab_check if isinstance(tab_check, list) else []:
            lim = f.get("limite")
            if isinstance(lim, (int, float)) and (10000 < lim < 1e9):
                errs.append("irrf.tabela:contains_annual_rows")
                break

    # campos base exigidos pelo plugin atual
    for k in ("ano", "dep", "inss", "irrf", "taxas"):
        if k not in d:
            errs.append(f"missing:{k}")

    if not isinstance(d.get("inss"), list) or len(d["inss"]) < 3:
        errs.append("inss:bad_shape")

    irrf = d.get("irrf", {})
    if not isinstance(irrf, dict):
        errs.append("irrf:bad_shape")
    else:
        tab = irrf.get("tabela", [])
        if not isinstance(tab, list) or len(tab) < 4:
            errs.append("irrf.tabela:bad_shape")
        if "simplificado" not in irrf or not isinstance(irrf.get("simplificado"), (int, float)):
            errs.append("irrf.simplificado:missing_or_bad")

    taxas = d.get("taxas", {})
    if not isinstance(taxas, dict):
        errs.append("taxas:bad_shape")
    else:
        for k in ("selic", "cdi"):
            if k not in taxas or not isinstance(taxas.get(k), (int, float)):
                errs.append(f"taxas.{k}:missing_or_bad")

    # sanidade
    if isinstance(d.get("dep"), (int, float)) and not (0 < d["dep"] < 500):
        errs.append("dep:out_of_range")

    # INSS sanidade
    if isinstance(d.get("inss"), list):
        for f in d["inss"]:
            if not (isinstance(f.get("limite"), (int, float)) and isinstance(f.get("aliquota"), (int, float))):
                errs.append("inss:row_bad")
                break
            if not (0 <= f["aliquota"] <= 0.3):
                errs.append("inss:aliquota_out_of_range")
                break

    # IRRF sanidade
    if isinstance(irrf, dict) and isinstance(irrf.get("tabela"), list):
        for f in irrf["tabela"]:
            if not all(k in f for k in ("limite", "aliquota", "deducao")):
                errs.append("irrf:tabela_row_missing")
                break

    # taxas sanidade
    if isinstance(taxas, dict):
        if isinstance(taxas.get("selic"), (int, float)) and not (0 <= taxas["selic"] <= 60):
            errs.append("selic:out_of_range")
        if isinstance(taxas.get("cdi"), (int, float)) and not (0 <= taxas["cdi"] <= 60):
            errs.append("cdi:out_of_range")

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

    year = int(dt.datetime.utcnow().year)

    errors = []
    sources = {}

    try:
        irrf = parse_irrf_receita(year)
        sources["irrf"] = {"url": irrf["url"], "http_code": irrf["http_code"]}
    except Exception as e:
        errors.append(f"irrf:{e}")
        irrf = None

    try:
        inss = parse_inss_gov_2026()
        sources["inss"] = {"url": inss["url"], "http_code": inss["http_code"]}
    except Exception as e:
        errors.append(f"inss:{e}")
        inss = None

    try:
        taxas = fetch_bcb_rates()
        sources["bcb"] = {"sgs": taxas.get("sources", {})}
    except Exception as e:
        errors.append(f"bcb:{e}")
        taxas = None

    if irrf and inss and taxas:
        payload = {
            "schema_version": "2.1.0",
            "meta": {
                "generated_at_utc": now_utc_iso(),
                "sources": sources,
                "errors": [],
                "warnings": [],
            },
            "ano": year,
            "dep": float(irrf["dep"]),
            "inss": inss["tabela"],
            "irrf": {
                "tabela": irrf["tabela"],
                "simplificado": float(irrf["simplificado"]),
                "reducao_mensal": irrf.get("reducao_mensal", {}),
            },
            "taxas": {
                "selic": float(taxas["selic"]),
                "cdi": float(taxas["cdi"]),
                "cdi_basis": taxas.get("cdi_basis"),
            },
        }

        ok, verrs = validate_payload(payload)
        if ok:
            write_json_atomic(payload)
            print("OK: dados_fiscais.json atualizado.")
            return

        print("ERRO: payload inválido -> NÃO sobrescrevi o last-good.")
        print("Detalhes:", verrs)

    # fallback: não sobrescreve arquivo bom
    if existing_ok:
        print("WARN: coleta falhou, mantendo last-good (nenhuma alteração no JSON).")
        print("Erros:", errors)
        return

    # fallback final (primeira execução sem last-good): cria um mínimo funcional para não quebrar.
    # (Mantém 2026 por segurança; você ainda terá o Action rodando para substituir por dados oficiais)
    minimal = {
        "schema_version": "2.1.0",
        "meta": {"generated_at_utc": now_utc_iso(), "sources": sources, "errors": errors, "warnings": ["minimal_fallback_written"]},
        "ano": year,
        "dep": 189.59,
        "inss": [
            {"limite": 1621.00, "aliquota": 0.075},
            {"limite": 2902.84, "aliquota": 0.09},
            {"limite": 4354.27, "aliquota": 0.12},
            {"limite": 8475.55, "aliquota": 0.14},
        ],
        "irrf": {
            "tabela": [
                {"limite": 2428.80, "aliquota": 0.0, "deducao": 0.0},
                {"limite": 2826.65, "aliquota": 0.075, "deducao": 182.16},
                {"limite": 3751.05, "aliquota": 0.15, "deducao": 394.16},
                {"limite": 4664.68, "aliquota": 0.225, "deducao": 675.49},
                {"limite": 9e9, "aliquota": 0.275, "deducao": 908.73},
            ],
            "simplificado": 607.20,
            "reducao_mensal": {"isenta_ate": 5000.00, "reduz_ate": 7350.00, "max_reducao_ate_5000": 312.89, "a": 978.62, "b": 0.133145},
        },
        "taxas": {"selic": 15.00, "cdi": 14.90, "cdi_basis": "fallback"},
    }
    write_json_atomic(minimal)
    print("WARN: sem last-good; escrevi fallback mínimo para evitar quebra.")


if __name__ == "__main__":
    main()


