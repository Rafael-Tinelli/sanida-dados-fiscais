import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from sanida_fiscal.legacy_artifact_v1 import (
    LegacyArtifactBoundaryError,
    build_legacy_dados_fiscais,
)
from sanida_fiscal.source_catalog_v1 import (
    registered_reference_year,
    run_registered_source_pipeline,
)


OUTPUT_FILE = "dados_fiscais.json"
TAXAS_FILE_LOCAL = "taxas_bacen.json"
TAXAS_JSON_URL_DEFAULT = "https://raw.githubusercontent.com/Rafael-Tinelli/sanida-dados-fiscais/main/taxas_bacen.json"
SOURCE_REGISTRY = Path("docs/source-registry-v1.json")
SOURCE_RUNTIME_ROOT = Path(os.getenv("SFA_SOURCE_RUNTIME_ROOT", ".source-runtime").strip())
PAYROLL_SOURCE_IDS = ("RFB_IRRF_TABLE_2026", "INSS_TABLE_2026")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SanidaFiscaisBot/4.0; +https://sanida.com.br)",
    "Accept": "text/html,application/xhtml+xml,application/xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}

SSLVERIFY = os.getenv("SFA_SSLVERIFY", "1").strip() not in ("0", "false", "False")
TIMEOUT = int(os.getenv("SFA_TIMEOUT", "25").strip())
RETRIES = int(os.getenv("SFA_RETRIES", "3").strip())


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0)


def now_utc_iso() -> str:
    return now_utc().isoformat().replace("+00:00", "Z")


def round_fiscal_number(x):
    if isinstance(x, float):
        return round(x, 6)
    return x


def round_fiscal_tree(obj):
    if isinstance(obj, dict):
        return {k: round_fiscal_tree(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [round_fiscal_tree(v) for v in obj]
    return round_fiscal_number(obj)


def taxas_json_url() -> str:
    return os.getenv("SFA_TAXAS_JSON_URL", TAXAS_JSON_URL_DEFAULT).strip()


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


def read_json_file(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def validate_taxas_payload(d: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errs: List[str] = []

    if not isinstance(d, dict):
        return False, ["taxas_payload:not_dict"]

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

    meta = d.get("meta")
    if meta is not None and not isinstance(meta, dict):
        errs.append("meta:bad_shape")

    return (len(errs) == 0), errs


def load_taxas_payload() -> Tuple[Dict[str, Any], str, str]:
    """
    Domínio financial_reference ainda legado nesta etapa.

    Fonte prioritária:
    1) arquivo local taxas_bacen.json (commitado no repo)
    2) raw GitHub do mesmo arquivo
    """
    local = read_json_file(TAXAS_FILE_LOCAL)
    ok_local, errs_local = validate_taxas_payload(local) if isinstance(local, dict) else (False, ["local:not_found_or_bad"])
    if ok_local:
        return local, "local_file", TAXAS_FILE_LOCAL

    remote_url = taxas_json_url()
    ok_remote, http_code, remote_data = fetch_json(remote_url)
    if ok_remote and isinstance(remote_data, dict):
        ok_payload, errs_payload = validate_taxas_payload(remote_data)
        if ok_payload:
            return remote_data, "remote_url", remote_url
        raise RuntimeError(f"taxas remoto inválido: {errs_payload}")

    raise RuntimeError(f"taxas indisponível: local={errs_local}; remote_status={http_code}; remote_error={remote_data}")


def validate_payload(d: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errs: List[str] = []

    irrf = d.get("irrf", {})
    if isinstance(irrf, dict):
        tab_check = irrf.get("tabela", [])
        for f in tab_check if isinstance(tab_check, list) else []:
            lim = f.get("limite")
            if isinstance(lim, (int, float)) and (10000 < lim < 1e9):
                errs.append("irrf.tabela:contains_annual_rows")
                break

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

    if isinstance(d.get("dep"), (int, float)) and not (0 < d["dep"] < 500):
        errs.append("dep:out_of_range")

    if isinstance(d.get("inss"), list):
        for f in d["inss"]:
            if not (isinstance(f.get("limite"), (int, float)) and isinstance(f.get("aliquota"), (int, float))):
                errs.append("inss:row_bad")
                break
            if not (0 <= f["aliquota"] <= 0.3):
                errs.append("inss:aliquota_out_of_range")
                break

    if isinstance(irrf, dict) and isinstance(irrf.get("tabela"), list):
        for f in irrf["tabela"]:
            if not all(k in f for k in ("limite", "aliquota", "deducao")):
                errs.append("irrf:tabela_row_missing")
                break

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


def is_valid_current_year_last_good(existing: Optional[Dict[str, Any]], year: int) -> bool:
    if not isinstance(existing, dict) or existing.get("ano") != year:
        return False
    ok, _ = validate_payload(existing)
    return ok


def write_json_atomic(data: Dict[str, Any]) -> None:
    tmp = OUTPUT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, OUTPUT_FILE)


def collect_payroll_source_runs(reference_year: int, observed_at_utc: dt.datetime):
    registered_years = {
        source_id: registered_reference_year(source_id) for source_id in PAYROLL_SOURCE_IDS
    }
    mismatched = {
        source_id: year
        for source_id, year in registered_years.items()
        if year != reference_year
    }
    if mismatched:
        raise RuntimeError(
            f"no canonical payroll parser registered for current year {reference_year}: {mismatched}"
        )

    runs = {}
    for source_id in PAYROLL_SOURCE_IDS:
        runs[source_id] = run_registered_source_pipeline(
            source_id=source_id,
            observed_at_utc=observed_at_utc,
            registry_path=SOURCE_REGISTRY,
            snapshot_root=SOURCE_RUNTIME_ROOT / "snapshots",
            state_root=SOURCE_RUNTIME_ROOT / "state",
            candidate_root=SOURCE_RUNTIME_ROOT / "candidates",
            timeout_seconds=float(TIMEOUT),
            max_attempts=RETRIES,
            headers=HEADERS,
            # Legacy artifact generation needs a current PARSED candidate in this
            # execution. It must not turn a 304/state-only observation into a new
            # published compatibility artifact.
            use_http_validators=False,
        )
    return runs


def main():
    observed_at_utc = now_utc()
    year = observed_at_utc.year
    existing = read_existing()
    existing_ok = is_valid_current_year_last_good(existing, year)

    errors: List[str] = []
    warnings: List[str] = []

    try:
        payroll_runs = collect_payroll_source_runs(year, observed_at_utc)
    except Exception as e:
        errors.append(f"payroll_sources:{e}")
        payroll_runs = None

    try:
        taxas_doc, taxas_origin, taxas_ref = load_taxas_payload()
        taxas_meta = taxas_doc.get("meta", {}) if isinstance(taxas_doc.get("meta"), dict) else {}
        taxas_sources = taxas_meta.get("sources", {}) if isinstance(taxas_meta.get("sources"), dict) else {}
        taxas_source_meta = {
            "origin": taxas_origin,
            "origin_ref": taxas_ref,
            "generated_at_utc": taxas_meta.get("generated_at_utc"),
            "source_meta": taxas_sources,
        }
        taxas = taxas_doc.get("taxas", {})
    except Exception as e:
        errors.append(f"taxas:{e}")
        taxas = None
        taxas_source_meta = {}

    if payroll_runs and taxas:
        try:
            payload = build_legacy_dados_fiscais(
                rfb_run=payroll_runs["RFB_IRRF_TABLE_2026"],
                inss_run=payroll_runs["INSS_TABLE_2026"],
                expected_year=year,
                taxas=taxas,
                taxas_source_meta=taxas_source_meta,
                generated_at_utc=observed_at_utc.isoformat().replace("+00:00", "Z"),
            )
            payload["meta"]["warnings"].extend(warnings)
            payload = round_fiscal_tree(payload)
            ok, verrs = validate_payload(payload)
            if not ok:
                raise LegacyArtifactBoundaryError(f"legacy artifact validation failed: {verrs}")
            write_json_atomic(payload)
            print("OK: dados_fiscais.json atualizado via pipelines canônicos RFB + INSS.")
            return
        except Exception as e:
            errors.append(f"legacy_artifact:{e}")

    if existing_ok:
        print("WARN: coleta/bridge falhou; mantendo last-good da mesma competência anual sem alteração.")
        print("Erros:", errors)
        return

    print("ERRO: sem candidato canônico atual e sem last-good válido para o ano corrente; nenhum JSON foi escrito.")
    print("Erros:", errors)
    raise SystemExit(1)


if __name__ == "__main__":
    main()
