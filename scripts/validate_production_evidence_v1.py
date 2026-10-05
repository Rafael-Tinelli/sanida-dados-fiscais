from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.production_evidence_v1 import (
    verify_legacy_artifact_evidence,
    verify_preserved_legacy_artifact_evidence,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate that dados_fiscais.json provenance resolves to durable Phase 4 evidence."
    )
    parser.add_argument("--artifact", type=Path, default=Path("dados_fiscais.json"))
    parser.add_argument(
        "--runtime-root",
        type=Path,
        default=Path("evidence/source-runtime-v1"),
    )
    parser.add_argument(
        "--preserved",
        action="store_true",
        help="Verify immutable evidence for the held previous artifact; never infer fresh collection.",
    )
    args = parser.parse_args()

    validator = (
        verify_preserved_legacy_artifact_evidence
        if args.preserved else verify_legacy_artifact_evidence
    )
    verified = validator(
        artifact_path=args.artifact,
        runtime_root=args.runtime_root,
    )
    print(
        ("Preserved historical source evidence: PASS (not a new collection)\\n"
         if args.preserved else "Production source evidence: PASS\\n")
        + json.dumps(verified, ensure_ascii=False, indent=2, sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
