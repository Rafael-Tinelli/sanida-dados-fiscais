from __future__ import annotations

import datetime as dt
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sanida_fiscal.financial_artifact_v1 import FINANCIAL_ARTIFACT_SCHEMA_VERSION
from sanida_fiscal.financial_evidence_v1 import (
    FinancialEvidenceError,
    verify_financial_artifact_evidence,
)
from sanida_fiscal.legacy_artifact_v1 import (
    LegacyArtifactBoundaryError,
    build_legacy_dados_fiscais,
)
from sanida_fiscal.source_catalog_v1 import run_registered_source_pipeline


OUTPUT_FILE = "dados_fiscais.json"
TAXAS_FILE_LOCAL = "taxas_bacen.json"
SOURCE_REGISTRY = Path("docs/source-registry-v1.json")
SOURCE_RUNTIME_ROOT = Path(os.getenv("SFA_SOURCE_RUNTIME_ROOT", ".source-runtime").strip())
PAYROLL_SOURCE_IDS = ("RFB_IRRF_TABLE_2026", "INSS_TABLE_2026")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; SanidaFiscaisBot/4.0; +https://sanida.com.br)",
    "Accept": "text/html,application/xhtml+xml,application/xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
}

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


def read_json_file(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _decimal_equal_number(canonical: Any, number: Any) -> bool:
    if not isinstance(canonical, str):
        return False
    if not isinstance(number, (int, float)) or isinstance(number, bool):
        return False
    try:
        return Decimal(canonical) == Decimal(str(number))
    except (InvalidOperation, ValueError):
        return False


def validate_taxas_payload(d: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """Validate the Phase 4 financial compatibility contract consumed by payroll.

    A01: old 1.3/B3-FTP documents and arbitrary remote JSON must never be accepted
    merely because they contain numeric `selic`/`cdi` fields.
    """
    errs: List[str] = []

    if not isinstance(d, dict):
        return False, ["taxas_payload:not_dict"]
    if d.get("schema_version") != FINANCIAL_ARTIFACT_SCHEMA_VERSION:
        errs.append("schema_version:unexpected")

    meta = d.get("meta")
    sources = None
    if not isinstance(meta, dict):
        errs.append("meta:missing_or_bad")
    else:
        if not isinstance(meta.get("generated_at_utc"), str):
            errs.append("meta.generated_at_utc:missing_or_bad")
        sources = meta.get("sources")
        if not isinstance(sources, dict):
            errs.append("meta.sources:missing_or_bad")

    expected_sources = {
        "selic": ("BCB_SELIC_META_SGS_432", 432),
        "cdi": ("BCB_CDI_DAILY_SGS_12", 12),
    }
    if isinstance(sources, dict):
        for key, (source_id, series_code) in expected_sources.items():
            source = sources.get(key)
            if not isinstance(source, dict):
                errs.append(f"meta.sources.{key}:missing_or_bad")
                continue
            if source.get("source_id") != source_id:
                errs.append(f"meta.sources.{key}.source_id:unexpected")
            if source.get("series_code") != series_code:
                errs.append(f"meta.sources.{key}.series_code:unexpected")
            for hash_key in ("snapshot_sha256", "candidate_sha256"):
                value = source.get(hash_key)
                if not isinstance(value, str) or len(value) != 64:
                    errs.append(f"meta.sources.{key}.{hash_key}:missing_or_bad")
            if not isinstance(source.get("parser_id"), str) or not isinstance(source.get("parser_version"), str):
                errs.append(f"meta.sources.{key}.parser:missing_or_bad")

    taxas = d.get("taxas")
    if not isinstance(taxas, dict):
        errs.append("taxas:missing_or_bad")
    else:
        for key in ("selic", "cdi"):
            value = taxas.get(key)
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                errs.append(f"taxas.{key}:missing_or_bad")
            elif not (0 <= value <= 60):
                errs.append(f"taxas.{key}:out_of_range")
        if taxas.get("cdi_basis") != "bcb_sgs_12_daily_compounded_252":
            errs.append("taxas.cdi_basis:unexpected")

        if isinstance(sources, dict):
            selic_meta = sources.get("selic")
            cdi_meta = sources.get("cdi")
            if isinstance(selic_meta, dict) and not _decimal_equal_number(
                selic_meta.get("source_value_pct"), taxas.get("selic")
            ):
                errs.append("taxas.selic:does_not_match_source_provenance")
            if isinstance(cdi_meta, dict) and not _decimal_equal_number(
                cdi_meta.get("annualized_value_pct"), taxas.get("cdi")
            ):
                errs.append("taxas.cdi:does_not_match_source_provenance")

    return (len(errs) == 0), errs


def load_taxas_payload(
    *,
    runtime_root: Path = SOURCE_RUNTIME_ROOT,
    artifact_path: Path = Path(TAXAS_FILE_LOCAL),
) -> Tuple[Dict[str, Any], str, str]:
    """Load only the local, evidence-gated Phase 4 financial artifact.

    There is intentionally no raw-GitHub or environment URL fallback. `taxas.yml`
    is the sole producer and its persisted snapshot/candidate/state chain must be
    verifiable before payroll may consume the artifact.
    """
    artifact_path = Path(artifact_path)
    local = read_json_file(str(artifact_path))
    ok_local, errs_local = (
        validate_taxas_payload(local)
        if isinstance(local, dict)
        else (False, ["local:not_found_or_bad"])
    )
    if not ok_local:
        raise RuntimeError(f"taxas local incompatível com Phase 4: {errs_local}")

    try:
        verify_financial_artifact_evidence(
            artifact_path=artifact_path,
            runtime_root=Path(runtime_root),
        )
    except FinancialEvidenceError as exc:
        raise RuntimeError(f"taxas sem evidência financeira válida: {exc}") from exc

    return local, "local_file", TAXAS_FILE_LOCAL


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
        if taxas.get("cdi_basis") != "bcb_sgs_12_daily_compounded_252":
            errs.append("taxas.cdi_basis:unexpected")

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
    if observed_at_utc.year != reference_year:
        raise RuntimeError(
            f"payroll reference year must match observation year: "
            f"reference={reference_year} observed={observed_at_utc.year}"
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
            "schema_version": taxas_doc.get("schema_version"),
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
            print("OK: dados_fiscais.json atualizado via RFB + INSS + taxas financeiras evidence-gated.")
            return
        except Exception as e:
            errors.append(f"legacy_artifact:{e}")

    if existing_ok:
        print("WARN: coleta/bridge falhou; mantendo last-good da mesma competência anual sem alteração.")
        print("Erros:", errors)
        return

    print("ERRO: sem candidatos/evidência atuais e sem last-good válido para o ano corrente; nenhum JSON foi escrito.")
    print("Erros:", errors)
    raise SystemExit(1)


if __name__ == "__main__":
    main()
