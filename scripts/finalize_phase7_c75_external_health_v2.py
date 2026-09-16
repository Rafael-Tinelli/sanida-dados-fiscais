#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.deployment_health_v2 import (
    DeliveryHealthError,
    FINAL_HEALTHY_STATUS,
    atomic_json,
    finalize_external_health,
)


def load(path: Path) -> dict:
    if not path.is_file():
        raise DeliveryHealthError(f"required JSON missing: {path}")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise DeliveryHealthError(f"JSON root must be object: {path}")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Promote a maintenance deployment to externally healthy only after C7.5 PASS evidence"
    )
    parser.add_argument("--deployment-state", type=Path, required=True)
    parser.add_argument("--external-evidence", type=Path, required=True)
    args = parser.parse_args()

    try:
        state = load(args.deployment_state)
        evidence = load(args.external_evidence)
        final = finalize_external_health(state, evidence)
        atomic_json(args.deployment_state, final)
    except (DeliveryHealthError, json.JSONDecodeError, OSError) as exc:
        print(json.dumps({"checkpoint": "C7.5", "status": "BLOCKED", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 3

    print(json.dumps(final, ensure_ascii=False, sort_keys=True))
    return 0 if final.get("status") == FINAL_HEALTHY_STATUS else 5


if __name__ == "__main__":
    raise SystemExit(main())
