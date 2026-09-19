from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from .source_ids_v1 import INSS_SOURCE_ID, RFB_SOURCE_ID
from .source_runtime_v1 import SourcePipelineRun
from .sources_v1 import ParseStatus


LEGACY_SCHEMA_VERSION = "2.2.0"
LEGACY_TOP_SENTINEL = 9_000_000_000.0


class LegacyArtifactBoundaryError(RuntimeError):
    pass


def _require_decimal_string(value: Any, label: str) -> float:
    if not isinstance(value, str):
        raise LegacyArtifactBoundaryError(f"{label} must be canonical decimal string")
    try:
        return float(Decimal(value))
    except (InvalidOperation, ValueError) as exc:
        raise LegacyArtifactBoundaryError(f"{label} is not a valid decimal string") from exc


def _require_payload(run: SourcePipelineRun, source_id: str) -> dict[str, Any]:
    if run.collection.source_id != source_id:
        raise LegacyArtifactBoundaryError(
            f"expected {source_id}, got {run.collection.source_id}"
        )
    candidate = run.candidate
    if candidate is None or candidate.status != ParseStatus.PARSED or candidate.payload is None:
        raise LegacyArtifactBoundaryError(
            f"{source_id} has no current PARSED candidate for legacy artifact generation"
        )
    return candidate.payload


def build_legacy_payroll_fields(
    *,
    rfb_payload: Mapping[str, Any],
    inss_payload: Mapping[str, Any],
    expected_year: int,
) -> dict[str, Any]:
    rfb_year = rfb_payload.get("reference_year")
    inss_year = inss_payload.get("reference_year")

    if rfb_payload.get("observation_type") != f"rfb_irrf_{rfb_year}":
        raise LegacyArtifactBoundaryError("unexpected RFB observation_type")
    if (
        inss_payload.get("observation_type")
        != f"inss_employee_progressive_table_{inss_year}"
    ):
        raise LegacyArtifactBoundaryError("unexpected INSS observation_type")
    if rfb_year != inss_year or rfb_year != expected_year:
        raise LegacyArtifactBoundaryError(
            f"reference-year mismatch: expected={expected_year} rfb={rfb_year} inss={inss_year}"
        )

    rfb_bands = rfb_payload.get("monthly_table")
    inss_bands = inss_payload.get("monthly_table")
    reduction = rfb_payload.get("monthly_reduction")
    if not isinstance(rfb_bands, list) or len(rfb_bands) != 5:
        raise LegacyArtifactBoundaryError("RFB legacy bridge requires exactly five monthly bands")
    if not isinstance(inss_bands, list) or len(inss_bands) != 4:
        raise LegacyArtifactBoundaryError("INSS legacy bridge requires exactly four employee bands")
    if not isinstance(reduction, Mapping):
        raise LegacyArtifactBoundaryError("RFB monthly reduction is missing")

    legacy_irrf = []
    for index, band in enumerate(rfb_bands):
        if not isinstance(band, Mapping):
            raise LegacyArtifactBoundaryError("RFB band must be an object")
        upper = band.get("upper_bound_brl")
        legacy_irrf.append(
            {
                "limite": LEGACY_TOP_SENTINEL
                if upper is None
                else _require_decimal_string(upper, f"rfb.monthly_table[{index}].upper_bound_brl"),
                "aliquota": _require_decimal_string(
                    band.get("rate"), f"rfb.monthly_table[{index}].rate"
                ),
                "deducao": _require_decimal_string(
                    band.get("deduction_brl"), f"rfb.monthly_table[{index}].deduction_brl"
                ),
            }
        )

    legacy_inss = []
    for index, band in enumerate(inss_bands):
        if not isinstance(band, Mapping):
            raise LegacyArtifactBoundaryError("INSS band must be an object")
        upper = band.get("upper_bound_brl")
        if upper is None:
            raise LegacyArtifactBoundaryError("INSS employee band cannot be unbounded")
        legacy_inss.append(
            {
                "limite": _require_decimal_string(
                    upper, f"inss.monthly_table[{index}].upper_bound_brl"
                ),
                "aliquota": _require_decimal_string(
                    band.get("rate"), f"inss.monthly_table[{index}].rate"
                ),
            }
        )

    return {
        "ano": expected_year,
        "dep": _require_decimal_string(
            rfb_payload.get("dependent_deduction_brl"), "rfb.dependent_deduction_brl"
        ),
        "inss": legacy_inss,
        "irrf": {
            "tabela": legacy_irrf,
            "simplificado": _require_decimal_string(
                rfb_payload.get("simplified_discount_brl"), "rfb.simplified_discount_brl"
            ),
            "reducao_mensal": {
                "isenta_ate": _require_decimal_string(
                    reduction.get("full_relief_income_upper_brl"),
                    "rfb.monthly_reduction.full_relief_income_upper_brl",
                ),
                "reduz_ate": _require_decimal_string(
                    reduction.get("phaseout_income_upper_brl"),
                    "rfb.monthly_reduction.phaseout_income_upper_brl",
                ),
                "max_reducao_ate_5000": _require_decimal_string(
                    reduction.get("max_reduction_brl"),
                    "rfb.monthly_reduction.max_reduction_brl",
                ),
                "a": _require_decimal_string(
                    reduction.get("intercept_brl"),
                    "rfb.monthly_reduction.intercept_brl",
                ),
                "b": _require_decimal_string(
                    reduction.get("slope"), "rfb.monthly_reduction.slope"
                ),
            },
        },
    }


def pipeline_source_meta(run: SourcePipelineRun) -> dict[str, Any]:
    candidate = run.candidate
    if candidate is None or candidate.status != ParseStatus.PARSED or candidate.payload is None:
        raise LegacyArtifactBoundaryError(
            f"{run.collection.source_id} has no current PARSED candidate metadata"
        )
    return {
        "source_id": run.collection.source_id,
        "url": run.collection.source_url,
        "http_code": run.collection.http_status,
        "collection_status": run.collection.status.value,
        "snapshot_sha256": candidate.snapshot_sha256,
        "candidate_sha256": run.state.last_candidate_sha256,
        "parser_id": candidate.parser_id,
        "parser_version": candidate.parser_version,
        "observed_at_utc": candidate.observed_at_utc.isoformat().replace("+00:00", "Z"),
    }


def build_legacy_dados_fiscais(
    *,
    rfb_run: SourcePipelineRun,
    inss_run: SourcePipelineRun,
    expected_year: int,
    taxas: Mapping[str, Any],
    taxas_source_meta: Mapping[str, Any],
    generated_at_utc: str,
) -> dict[str, Any]:
    rfb_payload = _require_payload(rfb_run, RFB_SOURCE_ID)
    inss_payload = _require_payload(inss_run, INSS_SOURCE_ID)
    payroll = build_legacy_payroll_fields(
        rfb_payload=rfb_payload,
        inss_payload=inss_payload,
        expected_year=expected_year,
    )

    for key in ("selic", "cdi"):
        if not isinstance(taxas.get(key), (int, float)):
            raise LegacyArtifactBoundaryError(f"taxas.{key} missing or invalid")

    return {
        "schema_version": LEGACY_SCHEMA_VERSION,
        "meta": {
            "generated_at_utc": generated_at_utc,
            "sources": {
                "irrf": pipeline_source_meta(rfb_run),
                "inss": pipeline_source_meta(inss_run),
                "taxas": dict(taxas_source_meta),
            },
            "errors": [],
            "warnings": ["phase4_legacy_compatibility_artifact"],
        },
        **payroll,
        "taxas": {
            "selic": float(taxas["selic"]),
            "cdi": float(taxas["cdi"]),
            "cdi_basis": taxas.get("cdi_basis"),
        },
    }
