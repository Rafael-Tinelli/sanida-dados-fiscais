from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.inss_employee_v1 import (
    PARSER_ID as INSS_PARSER_ID,
    PARSER_VERSION as INSS_PARSER_VERSION,
    parse_inss_employee_2026_snapshot,
)
from sanida_fiscal.rfb_irrf_v1 import parse_rfb_irrf_2026_snapshot
from sanida_fiscal.sources_v1 import load_source_registry


REQUIRED = {
    ".gitignore",
    "sanida_fiscal/sources_v1.py",
    "sanida_fiscal/source_runtime_v1.py",
    "sanida_fiscal/rfb_irrf_v1.py",
    "sanida_fiscal/inss_employee_v1.py",
    "tests/test_sources_v1.py",
    "tests/test_rfb_source_pipeline_v1.py",
    "tests/test_inss_source_pipeline_v1.py",
    "tests/fixtures/sources/rfb_irrf_2026_fragment.html",
    "tests/fixtures/sources/inss_employee_2026_fragment.html",
    "scripts/run_source_pipeline_v1.py",
    "docs/phase4-sources-sensors-v1.md",
    "docs/phase4-collection-surface-v1.json",
    "docs/phase4-inss-source-resolution-v1.json",
    "requirements-sources.txt",
}

ALLOWED_WORKFLOWS = {"main.yml", "taxas.yml", "remake-ci.yml"}


def main() -> None:
    missing = sorted(path for path in REQUIRED if not (ROOT / path).is_file())
    if missing:
        raise SystemExit(f"Phase 4 foundation: missing files: {missing}")

    registry = load_source_registry(ROOT / "docs/source-registry-v1.json")
    surface = json.loads((ROOT / "docs/phase4-collection-surface-v1.json").read_text(encoding="utf-8"))
    entries = surface.get("entries")
    if not isinstance(entries, list) or not entries:
        raise SystemExit("Phase 4 foundation: collection surface is empty")

    collector_ids = [entry.get("collector_id") for entry in entries]
    if len(collector_ids) != len(set(collector_ids)):
        raise SystemExit("Phase 4 foundation: duplicate collector_id")

    for entry in entries:
        registry_source_id = entry.get("registry_source_id")
        if registry_source_id is not None and registry_source_id not in registry:
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

    if "RFB_IRRF_TABLE_2026" not in registry or "INSS_TABLE_2026" not in registry:
        raise SystemExit("Phase 4 foundation: canonical RFB/INSS sources are missing")

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
        raise SystemExit("Phase 4 foundation: INSS parser contribution ceiling drift")
    inss_table = inss_payload.get("monthly_table")
    if not isinstance(inss_table, list) or len(inss_table) != 4:
        raise SystemExit("Phase 4 foundation: INSS parser band-count drift")
    if [band.get("rate") for band in inss_table if isinstance(band, dict)] != [
        "0.075",
        "0.09",
        "0.12",
        "0.14",
    ]:
        raise SystemExit("Phase 4 foundation: INSS parser rate drift")
    if inss_payload.get("thirteenth_assessment") != "separate_from_monthly_remuneration":
        raise SystemExit("Phase 4 foundation: INSS 13th assessment marker drift")

    policy = json.loads(
        (ROOT / "docs/phase4-inss-source-resolution-v1.json").read_text(encoding="utf-8")
    )
    inss_source = registry["INSS_TABLE_2026"]
    canonical = policy.get("canonical_source")
    resolution = policy.get("resolution")
    if policy.get("status") != "resolved" or policy.get("registry_source_id") != inss_source.source_id:
        raise SystemExit("Phase 4 foundation: INSS source resolution is not closed")
    if not isinstance(canonical, dict) or canonical.get("url") != inss_source.url:
        raise SystemExit("Phase 4 foundation: INSS canonical URL diverges from source registry")
    if canonical.get("parser_id") != INSS_PARSER_ID or canonical.get("parser_version") != INSS_PARSER_VERSION:
        raise SystemExit("Phase 4 foundation: INSS parser registration diverges from source policy")
    if not isinstance(resolution, dict):
        raise SystemExit("Phase 4 foundation: INSS source resolution payload missing")
    if resolution.get("allow_pinned_news_fallback") is not False:
        raise SystemExit("Phase 4 foundation: pinned INSS news fallback must stay disabled")
    if resolution.get("allow_search_discovery_fallback") is not False:
        raise SystemExit("Phase 4 foundation: INSS search discovery fallback must stay disabled")
    if resolution.get("canonical_failure_state") != "SOURCE_UNAVAILABLE":
        raise SystemExit("Phase 4 foundation: INSS canonical failure must fail closed")

    inss_surface = next(
        (entry for entry in entries if entry.get("collector_id") == "legacy.inss.employee_table"),
        None,
    )
    if not isinstance(inss_surface, dict) or inss_surface.get("gap_status") != "resolved":
        raise SystemExit("Phase 4 foundation: INSS collection-surface gap is not marked resolved")
    if inss_surface.get("resolution_artifact") != "docs/phase4-inss-source-resolution-v1.json":
        raise SystemExit("Phase 4 foundation: INSS resolution artifact is not anchored")

    print(
        "Phase 4 foundation: PASS "
        f"({len(entries)} collection-surface entries; {len(registry)} registered official sources; "
        "RFB+INSS pipelines anchored; INSS source divergence resolved)"
    )


if __name__ == "__main__":
    main()
