#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"
INVENTORY = ROOT / "docs" / "rule-inventory-v1.json"


def main() -> int:
    candidate = json.loads(EXAMPLE.read_text(encoding="utf-8"))
    inventory = json.loads(INVENTORY.read_text(encoding="utf-8"))
    inventory_by_id = {rule["rule_id"]: rule for rule in inventory["rules"]}

    for rule in candidate["rules"]:
        rule_id = rule["rule_id"]
        if rule_id not in inventory_by_id:
            raise SystemExit(f"candidate rule absent from Phase 1 inventory: {rule_id}")
        rule["applies_to"] = inventory_by_id[rule_id]["applies_to"]

    EXAMPLE.write_text(
        json.dumps(candidate, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print("Candidate applies_to targets synchronized with Phase 1 inventory")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
