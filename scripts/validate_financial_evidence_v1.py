from __future__ import annotations

import argparse
from pathlib import Path

from sanida_fiscal.financial_evidence_v1 import verify_financial_artifact_evidence


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate persisted source evidence for taxas_bacen.json.")
    parser.add_argument("--artifact", type=Path, default=Path("taxas_bacen.json"))
    parser.add_argument(
        "--runtime-root",
        type=Path,
        default=Path("evidence/source-runtime-v1"),
    )
    args = parser.parse_args()

    verified = verify_financial_artifact_evidence(
        artifact_path=args.artifact,
        runtime_root=args.runtime_root,
    )
    print(
        "Financial production evidence: PASS ("
        + ", ".join(sorted(verified))
        + ")"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
