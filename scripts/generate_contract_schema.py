#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sanida_fiscal.contract_v1 import FiscalContractV1

OUT = ROOT / "contracts" / "fiscal-contract-v1.schema.json"


def build_schema() -> dict:
    schema = FiscalContractV1.model_json_schema()
    schema["$id"] = "https://sanida.com.br/schemas/fiscal-contract-v1.schema.json"
    schema["title"] = "Sanida Fiscal Contract v1"
    schema["description"] = "Contrato canônico versionado para regras jurídico-fiscais consumidas por H26-H29."
    return schema


def render_schema() -> str:
    return json.dumps(build_schema(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def main() -> int:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(render_schema(), encoding="utf-8")
    print(OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
