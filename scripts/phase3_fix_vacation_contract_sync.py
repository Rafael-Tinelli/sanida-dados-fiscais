from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

coverage_path = ROOT / "docs" / "contract-coverage-v1.json"
coverage = json.loads(coverage_path.read_text(encoding="utf-8"))
coverage["inventory_version"] = "1.1.0"
coverage_path.write_text(
    json.dumps(coverage, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
)

test_path = ROOT / "tests" / "test_contract_v1.py"
text = test_path.read_text(encoding="utf-8")
old = '''    with pytest.raises(ContractCompatibilityError, match="schema mismatch"):\n        contract.assert_reader_compatible(\n            consumer="H26", schema_version="1.1.0", contract_api_version="1.0.0"\n        )\n    with pytest.raises(ContractCompatibilityError, match="contract API mismatch"):\n        contract.assert_reader_compatible(\n            consumer="H26", schema_version="1.1.0", contract_api_version="1.0.0"\n        )\n'''
new = '''    with pytest.raises(ContractCompatibilityError, match="schema mismatch"):\n        contract.assert_reader_compatible(\n            consumer="H26", schema_version="1.0.0", contract_api_version="1.1.0"\n        )\n    with pytest.raises(ContractCompatibilityError, match="contract API mismatch"):\n        contract.assert_reader_compatible(\n            consumer="H26", schema_version="1.1.0", contract_api_version="1.0.0"\n        )\n'''
if old in text:
    text = text.replace(old, new, 1)
elif new not in text:
    raise SystemExit("compatibility regression block not found")
test_path.write_text(text, encoding="utf-8")

print("coverage and exact-compatibility tests aligned to v1.1")
