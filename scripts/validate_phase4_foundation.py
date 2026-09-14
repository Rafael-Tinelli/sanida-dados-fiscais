from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.financial_reference_v1 import (
    annualize_cdi_daily_rate_pct,
    parse_bcb_cdi_daily_sgs12_snapshot,
    parse_bcb_selic_meta_sgs432_snapshot,
)
from sanida_fiscal.financial_source_catalog_v1 import FINANCIAL_PARSER_BINDINGS
from sanida_fiscal.inss_employee_v1 import parse_inss_employee_2026_snapshot
from sanida_fiscal.legacy_artifact_v1 import build_legacy_payroll_fields
from sanida_fiscal.rfb_irrf_v1 import parse_rfb_irrf_2026_snapshot
from sanida_fiscal.source_catalog_v1 import PARSER_BINDINGS
from sanida_fiscal.source_runtime_v1 import SourcePipelineState
from sanida_fiscal.sources_v1 import load_source_registry


REQUIRED = {
    ".gitignore",
    "sanida_fiscal/sources_v1.py",
    "sanida_fiscal/source_runtime_v1.py",
    "sanida_fiscal/source_catalog_v1.py",
    "sanida_fiscal/rfb_irrf_v1.py",
    "sanida_fiscal/inss_employee_v1.py",
    "sanida_fiscal/legacy_artifact_v1.py",
    "sanida_fiscal/production_evidence_v1.py",
    "sanida_fiscal/financial_reference_v1.py",
    "sanida_fiscal/financial_source_catalog_v1.py",
    "sanida_fiscal/financial_artifact_v1.py",
    "sanida_fiscal/financial_evidence_v1.py",
    "tests/test_sources_v1.py",
    "tests/test_rfb_source_pipeline_v1.py",
    "tests/test_inss_source_pipeline_v1.py",
    "tests/test_legacy_artifact_bridge_v1.py",
    "tests/test_scraper_phase4_migration.py",
    "tests/test_production_evidence_v1.py",
    "tests/test_financial_reference_v1.py",
    "tests/test_financial_evidence_v1.py",
    "tests/fixtures/sources/rfb_irrf_2026_fragment.html",
    "tests/fixtures/sources/inss_employee_2026_fragment.html",
    "tests/fixtures/sources/bcb_selic_sgs432.json",
    "tests/fixtures/sources/bcb_cdi_sgs12.json",
    "scripts/run_source_pipeline_v1.py",
    "scripts/validate_production_evidence_v1.py",
    "scripts/validate_financial_evidence_v1.py",
    "docs/phase4-sources-sensors-v1.md",
    "docs/phase4-collection-surface-v1.json",
    "docs/phase4-inss-source-resolution-v1.json",
    "docs/phase4-legacy-artifact-boundary-v1.json",
    "docs/phase4-production-persistence-v1.json",
    "docs/financial-source-registry-v1.json",
    "docs/phase4-financial-reference-policy-v1.json",
    "evidence/source-runtime-v1/README.md",
    "requirements-sources.txt",
}

ALLOWED_WORKFLOWS = {"main.yml", "taxas.yml", "remake-ci.yml"}
FORBIDDEN_SCRAPER_TOKENS = {
    "PINNED_INSS_URLS",
    "find_inss_article_url",
    "parse_inss_gov",
    "parse_irrf_receita",
    "@@search",
    "minimal_fallback_written",
    "static_reference_values",
}
FORBIDDEN_FINANCIAL_TOKENS = {
    "ftplib",
    "ftp.cetip.com.br",
    "FALLBACK_SELIC",
    "FALLBACK_CDI",
    "fetch_b3_cdi_ftp",
    "minimal_fallback_written",
    "static_reference_values",
}


def main() -> None:
    missing = sorted(path for path in REQUIRED if not (ROOT / path).is_file())
    if missing:
        raise SystemExit(f"Phase 4 foundation: missing files: {missing}")

    registry = load_source_registry(ROOT / "docs/source-registry-v1.json")
    financial_registry = load_source_registry(ROOT / "docs/financial-source-registry-v1.json")
    surface = json.loads((ROOT / "docs/phase4-collection-surface-v1.json").read_text(encoding="utf-8"))
    entries = surface.get("entries")
    if not isinstance(entries, list) or not entries:
        raise SystemExit("Phase 4 foundation: collection surface is empty")

    collector_ids = [entry.get("collector_id") for entry in entries]
    if len(collector_ids) != len(set(collector_ids)):
        raise SystemExit("Phase 4 foundation: duplicate collector_id")

    for entry in entries:
        registry_source_id = entry.get("registry_source_id")
        registry_path = entry.get("registry")
        if registry_source_id is not None:
            target_registry = (
                financial_registry
                if registry_path == "docs/financial-source-registry-v1.json"
                else registry
            )
            if registry_source_id not in target_registry:
                raise SystemExit(
                    f"Phase 4 foundation: unknown registry source {registry_source_id}"
                )
        if entry.get("current_behavior") == entry.get("target_behavior"):
            raise SystemExit(
                f"Phase 4 foundation: {entry.get('collector_id')} does not describe a migration boundary"
            )

    workflow_dir = ROOT / ".github" / "workflows"
    actual_workflows = {path.name for path in workflow_dir.glob("*.yml")}
    unexpected = sorted(actual_workflows - ALLOWED_WORKFLOWS)
    if unexpected:
        raise SystemExit(f"Phase 4 foundation: unexpected workflows: {unexpected}")

    required_sources = {"RFB_IRRF_TABLE_2026", "INSS_TABLE_2026"}
    if not required_sources.issubset(registry):
        raise SystemExit("Phase 4 foundation: canonical RFB/INSS sources are missing")
    if set(PARSER_BINDINGS) != required_sources:
        raise SystemExit("Phase 4 foundation: payroll parser catalog drift")

    required_financial_sources = {"BCB_SELIC_META_SGS_432", "BCB_CDI_DAILY_SGS_12"}
    if set(financial_registry) != required_financial_sources:
        raise SystemExit("Phase 4 foundation: financial source registry drift")
    if set(FINANCIAL_PARSER_BINDINGS) != required_financial_sources:
        raise SystemExit("Phase 4 foundation: financial parser catalog drift")

    rfb_fixture = (ROOT / "tests/fixtures/sources/rfb_irrf_2026_fragment.html").read_bytes()
    rfb_payload = parse_rfb_irrf_2026_snapshot(rfb_fixture)
    if rfb_payload.get("dependent_deduction_brl") != "189.59":
        raise SystemExit("Phase 4 foundation: RFB parser dependent deduction drift")
    if rfb_payload.get("simplified_discount_brl") != "607.20":
        raise SystemExit("Phase 4 foundation: RFB parser simplified discount drift")
    reduction = rfb_payload.get("monthly_reduction")
    if not isinstance(reduction, dict) or reduction.get("intercept_brl") != "978.62":
        raise SystemExit("Phase 4 foundation: RFB parser reduction drift")

    inss_fixture = (ROOT / "tests/fixtures/sources/inss_employee_2026_fragment.html").read_bytes()
    inss_payload = parse_inss_employee_2026_snapshot(inss_fixture)
    if inss_payload.get("contribution_ceiling_brl") != "8475.55":
        raise SystemExit("Phase 4 foundation: INSS parser ceiling drift")
    if inss_payload.get("thirteenth_assessment") != "separate_from_monthly_remuneration":
        raise SystemExit("Phase 4 foundation: INSS 13th assessment drift")

    selic_fixture = (ROOT / "tests/fixtures/sources/bcb_selic_sgs432.json").read_bytes()
    selic_payload = parse_bcb_selic_meta_sgs432_snapshot(selic_fixture)
    if selic_payload.get("series_code") != 432 or selic_payload.get("annual_rate_pct") != "14.00":
        raise SystemExit("Phase 4 foundation: BCB Selic parser drift")

    cdi_fixture = (ROOT / "tests/fixtures/sources/bcb_cdi_sgs12.json").read_bytes()
    cdi_payload = parse_bcb_cdi_daily_sgs12_snapshot(cdi_fixture)
    if cdi_payload.get("series_code") != 12 or cdi_payload.get("daily_rate_pct") != "0.051660":
        raise SystemExit("Phase 4 foundation: BCB CDI parser drift")
    if str(annualize_cdi_daily_rate_pct(cdi_payload["daily_rate_pct"])) != "13.90":
        raise SystemExit("Phase 4 foundation: CDI 252-business-day annualization drift")

    policy = json.loads((ROOT / "docs/phase4-inss-source-resolution-v1.json").read_text(encoding="utf-8"))
    if policy.get("registry_source_id") != "INSS_TABLE_2026":
        raise SystemExit("Phase 4 foundation: INSS source policy registry id drift")
    resolution = policy.get("resolution", {})
    if resolution.get("allow_pinned_news_fallback") is not False or resolution.get("allow_search_discovery_fallback") is not False:
        raise SystemExit("Phase 4 foundation: INSS discovery fallback re-enabled")
    if policy.get("canonical_source", {}).get("url") != registry["INSS_TABLE_2026"].url:
        raise SystemExit("Phase 4 foundation: INSS canonical URL differs from source registry")

    financial_policy = json.loads((ROOT / "docs/phase4-financial-reference-policy-v1.json").read_text(encoding="utf-8"))
    if financial_policy.get("failure_policy", {}).get("write_static_fallback") is not False:
        raise SystemExit("Phase 4 foundation: static financial fallback re-enabled")
    if financial_policy.get("failure_policy", {}).get("refresh_generated_at_without_new_observation") is not False:
        raise SystemExit("Phase 4 foundation: failed financial observation may relabel old data")
    b3_role = financial_policy.get("b3_role", {})
    if b3_role.get("legacy_ftp_is_automatic_fallback") is not False:
        raise SystemExit("Phase 4 foundation: B3 FTP automatic fallback re-enabled")
    if b3_role.get("legacy_ftp_is_production_input_after_migration") is not False:
        raise SystemExit("Phase 4 foundation: B3 FTP remains a production input")

    boundary = json.loads((ROOT / "docs/phase4-legacy-artifact-boundary-v1.json").read_text(encoding="utf-8"))
    write_policy = boundary.get("write_policy", {})
    if write_policy.get("allow_static_payroll_fallback") is not False:
        raise SystemExit("Phase 4 foundation: static payroll fallback re-enabled")
    if write_policy.get("allow_preserve_prior_year_artifact_as_current") is not False:
        raise SystemExit("Phase 4 foundation: prior-year relabeling allowed")
    if boundary.get("legacy_artifact", {}).get("canonical_contract_release") is not False:
        raise SystemExit("Phase 4 foundation: legacy artifact mislabeled as canonical release")

    persistence = json.loads((ROOT / "docs/phase4-production-persistence-v1.json").read_text(encoding="utf-8"))
    backend = persistence.get("persistence_backend", {})
    if backend.get("kind") != "git_tracked_repository_path":
        raise SystemExit("Phase 4 foundation: production evidence backend is not durable Git storage")
    if backend.get("runtime_root") != "evidence/source-runtime-v1":
        raise SystemExit("Phase 4 foundation: production evidence runtime root drift")
    retention = persistence.get("retention_policy", {})
    if retention.get("automatic_pruning") is not False:
        raise SystemExit("Phase 4 foundation: production evidence auto-pruning unexpectedly enabled")
    integrity = persistence.get("integrity_gates", {})
    if integrity.get("snapshot_file_sha256_must_equal_artifact_provenance") is not True:
        raise SystemExit("Phase 4 foundation: snapshot provenance hash gate disabled")
    if integrity.get("candidate_file_sha256_must_equal_artifact_provenance") is not True:
        raise SystemExit("Phase 4 foundation: candidate provenance hash gate disabled")
    included_sources = set(persistence.get("scope", {}).get("included_sources", []))
    if included_sources != required_sources | required_financial_sources:
        raise SystemExit("Phase 4 foundation: persisted production source set drift")

    state_fields = SourcePipelineState.model_fields
    required_state_fields = set(persistence.get("state_contract", {}).get("required_last_good_fields", []))
    if not required_state_fields.issubset(state_fields):
        raise SystemExit("Phase 4 foundation: production state contract fields are not materialized")

    legacy = build_legacy_payroll_fields(
        rfb_payload=rfb_payload,
        inss_payload=inss_payload,
        expected_year=2026,
    )
    if legacy.get("dep") != 189.59 or legacy.get("inss", [])[-1].get("limite") != 8475.55:
        raise SystemExit("Phase 4 foundation: legacy compatibility bridge drift")

    scraper_text = (ROOT / "scraper.py").read_text(encoding="utf-8")
    leaked = sorted(token for token in FORBIDDEN_SCRAPER_TOKENS if token in scraper_text)
    if leaked:
        raise SystemExit(f"Phase 4 foundation: legacy payroll path still present in scraper: {leaked}")
    if "run_registered_source_pipeline" not in scraper_text or "build_legacy_dados_fiscais" not in scraper_text:
        raise SystemExit("Phase 4 foundation: scraper is not wired through canonical pipelines and compatibility bridge")
    if 'candidate_root=SOURCE_RUNTIME_ROOT / "candidates"' not in scraper_text:
        raise SystemExit("Phase 4 foundation: scraper does not persist normalized candidate evidence")

    financial_text = (ROOT / "update_taxas.py").read_text(encoding="utf-8")
    financial_leaks = sorted(token for token in FORBIDDEN_FINANCIAL_TOKENS if token in financial_text)
    if financial_leaks:
        raise SystemExit(f"Phase 4 foundation: legacy financial path still present: {financial_leaks}")
    if "run_registered_financial_source_pipeline" not in financial_text:
        raise SystemExit("Phase 4 foundation: financial producer is not wired through registered source pipelines")
    if "build_financial_reference_artifact" not in financial_text:
        raise SystemExit("Phase 4 foundation: financial producer bypasses compatibility bridge")

    main_workflow = (workflow_dir / "main.yml").read_text(encoding="utf-8")
    main_markers = {
        "SFA_SOURCE_RUNTIME_ROOT: evidence/source-runtime-v1",
        "scripts/validate_production_evidence_v1.py",
        'git add dados_fiscais.json "$SFA_SOURCE_RUNTIME_ROOT"',
        'git add "$SFA_SOURCE_RUNTIME_ROOT"',
        "Enforce production gate",
    }
    missing_main_markers = sorted(marker for marker in main_markers if marker not in main_workflow)
    if missing_main_markers:
        raise SystemExit(f"Phase 4 foundation: payroll workflow persistence drift: {missing_main_markers}")

    taxas_workflow = (workflow_dir / "taxas.yml").read_text(encoding="utf-8")
    taxas_markers = {
        "SFA_SOURCE_RUNTIME_ROOT: evidence/source-runtime-v1",
        "scripts/validate_financial_evidence_v1.py",
        'git add taxas_bacen.json "$SFA_SOURCE_RUNTIME_ROOT"',
        'git add "$SFA_SOURCE_RUNTIME_ROOT"',
        "Enforce financial production gate",
    }
    missing_taxas_markers = sorted(marker for marker in taxas_markers if marker not in taxas_workflow)
    if missing_taxas_markers:
        raise SystemExit(f"Phase 4 foundation: financial workflow persistence drift: {missing_taxas_markers}")

    requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")
    if "-r requirements-contract.txt" not in requirements or "-r requirements-sources.txt" not in requirements:
        raise SystemExit("Phase 4 foundation: production requirements do not install Phase 4 runtime")

    print(
        "Phase 4 foundation: PASS "
        f"({len(entries)} collection-surface entries; {len(registry)} payroll/legal sources; "
        f"{len(financial_registry)} financial sources; RFB+INSS pipelines anchored; "
        "Selic SGS 432 + CDI SGS 12 migrated; static financial fallback removed; "
        "production evidence persistence anchored)"
    )


if __name__ == "__main__":
    main()
