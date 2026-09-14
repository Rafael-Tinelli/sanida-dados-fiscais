#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sanida_fiscal.contract_v1_2 import FiscalContractV12


DEFAULT_OUT = ROOT / "contracts" / "fiscal-contract-v1.2.schema.json"
SCHEMA_ID = "https://sanida.com.br/schemas/fiscal-contract-v1.2.schema.json"


def build_schema() -> dict:
    schema = FiscalContractV12.model_json_schema()
    schema["$id"] = SCHEMA_ID
    schema["title"] = "Sanida Fiscal Contract v1.2"
    schema["description"] = (
        "Contrato canônico versionado para regras jurídico-fiscais H26-H29, "
        "incluindo trilha separada de evidência de governança para regras técnicas."
    )
    return schema


def render_schema() -> str:
    return json.dumps(
        build_schema(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate/check Fiscal Contract v1.2 JSON Schema")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()

    expected = render_schema()
    output = args.output
    if not output.is_absolute():
        output = ROOT / output

    if args.check:
        if not output.is_file():
            print(f"schema missing: {output}", file=sys.stderr)
            return 1
        if output.read_text(encoding="utf-8") != expected:
            print(f"schema drift: {output}", file=sys.stderr)
            return 1
        print(f"Fiscal Contract v1.2 schema: PASS ({output})")
        return 0

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(expected, encoding="utf-8")
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
