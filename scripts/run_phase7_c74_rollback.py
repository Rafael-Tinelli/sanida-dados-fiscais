#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path

from scripts.run_phase7_c74_controlled_deploy import DeploymentError, atomic_json, rollback


def load_json(path: Path) -> dict:
    if not path.is_file():
        raise DeploymentError(f"required journal file missing: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser(description="C7.4 manual recovery rollback from an existing deployment journal")
    parser.add_argument("--site-root", type=Path, required=True)
    parser.add_argument("--wordpress-plugin-dir", type=Path, required=True)
    parser.add_argument("--journal-dir", type=Path, required=True)
    args = parser.parse_args()

    roots = {
        "site_root": args.site_root.resolve(),
        "wordpress_plugin_dir": args.wordpress_plugin_dir.resolve(),
    }
    journal = args.journal_dir.resolve()
    try:
        state = load_json(journal / "deployment-state.json")
        snapshot = load_json(journal / "snapshot.json")
        if state.get("checkpoint") != "C7.4" or snapshot.get("checkpoint") != "C7.4":
            raise DeploymentError("journal is not a C7.4 deployment journal")
        if state.get("status") in {"ROLLED_BACK", "MANUAL_ROLLBACK_COMPLETE"}:
            print(json.dumps(state, ensure_ascii=False, sort_keys=True))
            return 0
        created_dirs = state.get("created_directories") or []
        result = rollback(snapshot, roots, journal, created_dirs)
        state["rollback_performed"] = True
        state["rollback"] = result
        state["production_deployed"] = False
        state["status"] = "MANUAL_ROLLBACK_COMPLETE" if result["rollback_verified"] else "ROLLBACK_INCOMPLETE"
        state["manual_rollback_at_utc"] = datetime.now(timezone.utc).isoformat()
        atomic_json(journal / "deployment-state.json", state)
        print(json.dumps(state, ensure_ascii=False, sort_keys=True))
        return 0 if result["rollback_verified"] else 5
    except Exception as exc:
        print(json.dumps({"checkpoint": "C7.4", "status": "ROLLBACK_BLOCKED", "error": str(exc)}, ensure_ascii=False, sort_keys=True))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
