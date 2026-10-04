"""Read-only 32-rule AF01 successor *semantic preview*, not a fiscal release.

The legal CP=yes overlay is calculated against the exact published current
release without forging SCI PDF provenance, new RFB/INSS observations,
approval, release_id or source timestamps. Canonical publication requires a
new source-bound CANDIDATE from the full evidence collector.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

from sanida_fiscal.publication_v1 import FiscalReleaseStore
from sanida_fiscal.release_assembler_v12 import _apply_structural_successor_overlays


def changed_paths(before, after, prefix=""):
    if isinstance(before, dict) and isinstance(after, dict):
        result = []
        for key in sorted(set(before) | set(after)):
            result.extend(changed_paths(
                before.get(key), after.get(key),
                f"{prefix}.{key}" if prefix else key,
            ))
        return result
    if before != after:
        return [prefix]
    return []


def preview(store_path: Path) -> dict:
    previous = FiscalReleaseStore(store_path).load_current()
    if previous is None:
        raise ValueError("fiscal release current.json is missing; no preview")
    previous.assert_consumable()
    source = previous.model_dump(mode="json", exclude_none=True)
    current = {rule["rule_id"]: deepcopy(rule) for rule in source["rules"]}
    if len(current) != 32:
        raise ValueError("not a unique closed 32-rule published baseline")
    staged = deepcopy(current)
    _apply_structural_successor_overlays(staged)
    changed = {
        rule_id: {
            "paths": changed_paths(current[rule_id], staged[rule_id]),
            "old_fields": {
                "social_security": current[rule_id]["payload"]["components"][0]["social_security"],
            } if rule_id == "vacation.abono_constitutional_third.ir_incidence" else {},
            "new_fields": {
                "social_security": staged[rule_id]["payload"]["components"][0]["social_security"],
            } if rule_id == "vacation.abono_constitutional_third.ir_incidence" else {},
        }
        for rule_id in sorted(current)
        if current[rule_id] != staged[rule_id]
    }
    third = current["vacation.abono_constitutional_third.ir_incidence"]
    successor = staged["vacation.abono_constitutional_third.ir_incidence"]
    before = third["payload"]["components"][0]
    after = successor["payload"]["components"][0]
    if before["social_security"] != "no" or after["social_security"] != "yes" or after["irrf"] != "yes":
        raise ValueError("AF01 published/preview constitutional-third CP and IR invariant drift")
    if any(x not in changed for x in ("vacation.abono_constitutional_third.ir_incidence",)):
        raise ValueError("AF01 semantic successor contains no material third change")
    expected = {"vacation.abono_constitutional_third.ir_incidence"}
    unexpected = set(changed) - expected
    if unexpected:
        raise ValueError("successor has unrelated structural changes needing review: " + repr(sorted(unexpected)))
    return {
        "status": "SEMANTIC_PREVIEW_ONLY_NOT_A_FISCAL_RELEASE",
        "published_baseline_release_id": previous.release_id,
        "published_rule_count": len(current),
        "preview_rule_count": len(staged),
        "added_rules": sorted(set(staged)-set(current)),
        "removed_rules": sorted(set(current)-set(staged)),
        "material_diff": changed,
        "AF01": {
            "converted_principal": "CP=no, IRRF=no",
            "converted_days_statutory_third": "CP=yes, IRRF=yes",
            "vacation_indemnified_third": "CP=no, IRRF=no",
            "double_count_prohibited": True,
        },
        "approval": {
            "official_pdf_sha256": None,
            "official_binary_evidence_homologated": False,
            "human_review_performed": False,
            "approval_reference": None,
            "candidate_can_be_published": False,
            "next_step": "capture official original PDF SHA, reconcile current eSocial rubrics, collect new official RFB/INSS/governance snapshots, assemble 32-rule canonical candidate and generate exact review_key",
        },
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00","Z"),
        "no_modified_repository_release_files": True,
    }


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--output",type=Path,default=Path(".validation/af01-successor-preview.json"))
    args=ap.parse_args()
    result=preview(Path("releases/fiscal-v1"))
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2,sort_keys=True)+"\n")
    print("AF01_SUCCESSOR_PREVIEW " + json.dumps({
        "status":result["status"],
        "baseline":result["published_baseline_release_id"],
        "changed_rule_ids":sorted(result["material_diff"]),
        "blocked":not result["approval"]["candidate_can_be_published"],
    },sort_keys=True))


if __name__=="__main__":
    main()
