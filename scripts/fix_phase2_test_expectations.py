#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "tests" / "test_contract_v1.py"


def main() -> int:
    text = TESTS.read_text(encoding="utf-8")
    old = 'with pytest.raises(ValidationError, match="requires description"):\n        CompetencePolicy(basis="rule_specific")'
    new = 'with pytest.raises(ValidationError, match="requires rule_specific_key"):\n        CompetencePolicy(basis="rule_specific")'
    if old not in text and new not in text:
        raise SystemExit("rule-specific competence test anchor not found")
    text = text.replace(old, new, 1)
    TESTS.write_text(text, encoding="utf-8")
    print("Rule-specific competence test expectation synchronized")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
