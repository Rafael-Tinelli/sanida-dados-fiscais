from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.rfb_irrf_v1 import parse_rfb_irrf_2026_snapshot
from sanida_fiscal.sources_v1 import load_source_registry


REQUIRED = {
    ".gitignore",
    "sanida_fiscal/sources_v1.py",
    "sanida_fiscal/source_runtime_v1.py",
    "sanida_fiscal/rfb_irrf_v1.py",
    "tests/test_sources_v1.py",
    "tests/test_rfb_source_pipeline_v1.py",
    "tests/fixtures/sources/rfb_irrf_2026_fragment.html",
    "scripts/run_source_pipeline_v1.py",
    "docs/phase4-sources-sensors-v1.md",
    "docs/phase4-collection-surface-v1.json",
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

    fixture = (ROOT / "tests/fixtures/sources/rfb_irrf_2026_fragment.html").read_bytes()
    payload = parse_rfb_irrf_2026_snapshot(fixture)
    if payload.get("dependent_deduction_brl") != "189.59":
        raise SystemExit("Phase 4 foundation: RFB parser dependent deduction drift")
    if payload.get("simplified_discount_brl") != "607.20":
        raise SystemExit("Phase 4 foundation: RFB parser simplified discount drift")
    reduction = payload.get("monthly_reduction")
    if not isinstance(reduction, dict) or reduction.get("intercept_brl") != "978.62":
        raise SystemExit("Phase 4 foundation: RFB parser reduction drift")

    print(
        "Phase 4 foundation: PASS "
        f"({len(entries)} collection-surface entries; {len(registry)} registered official sources; RFB pipeline anchored)"
    )


if __name__ == "__main__":
    main()
