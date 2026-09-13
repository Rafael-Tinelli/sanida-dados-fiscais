from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if old not in text:
        if new in text:
            return
        raise SystemExit(f"pattern not found in {path}: {old[:80]!r}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


# Schema/API minor bump: Phase 3 found an additive semantic field required to execute
# the already-closed A02 rule without hardcoding legal meaning in the engine.
types = ROOT / "sanida_fiscal" / "types_v1.py"
replace_once(
    types,
    '    schema_version: Literal["1.0.0"] = "1.0.0"\n    contract_api_version: Literal["1.0.0"] = "1.0.0"',
    '    schema_version: Literal["1.1.0"] = "1.1.0"\n    contract_api_version: Literal["1.1.0"] = "1.1.0"',
)
replace_once(
    types,
    '''class PeriodRulePayload(StrictModel):\n    type: Literal["period_rule"] = "period_rule"\n    duration_months: int = Field(gt=0)\n    anchor: Literal["employment_start_anniversary"]\n    calendar_year_reset: Literal[False] = False\n''',
    '''class PeriodRulePayload(StrictModel):\n    type: Literal["period_rule"] = "period_rule"\n    duration_months: int = Field(gt=0)\n    anchor: Literal["employment_start_anniversary"]\n    calendar_year_reset: Literal[False] = False\n    proportional_accrual_method: Literal[\n        "one_twelfth_per_acquisition_month_or_fraction_gte_days"\n    ] = "one_twelfth_per_acquisition_month_or_fraction_gte_days"\n    proportional_qualifying_days: Literal[15] = 15\n''',
)

contract = ROOT / "sanida_fiscal" / "contract_v1.py"
replace_once(
    contract,
    '    schema_version: Literal["1.0.0"] = "1.0.0"',
    '    schema_version: Literal["1.1.0"] = "1.1.0"',
)

candidate_path = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"
candidate = json.loads(candidate_path.read_text(encoding="utf-8"))
candidate["schema_version"] = "1.1.0"
candidate["rule_inventory_version"] = "1.1.0"
candidate["consumer_compatibility"]["schema_version"] = "1.1.0"
candidate["consumer_compatibility"]["contract_api_version"] = "1.1.0"
acq = next(rule for rule in candidate["rules"] if rule["rule_id"] == "vacation.acquisition_period")
acq["rule_version"] = "1.1.0"
acq["description"] = (
    "Período aquisitivo de doze meses ancorado no vínculo, sem reset no ano civil; "
    "proporcionalidade conta 1/12 por mês aquisitivo ou fração de pelo menos 15 dias."
)
acq["payload"]["proportional_accrual_method"] = (
    "one_twelfth_per_acquisition_month_or_fraction_gte_days"
)
acq["payload"]["proportional_qualifying_days"] = 15
acq["change_class"] = "STRUCTURAL_CHANGE"
candidate_path.write_text(
    json.dumps(candidate, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
)

inventory_path = ROOT / "docs" / "rule-inventory-v1.json"
inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
inventory["schema_version"] = "1.1.0"
for rule in inventory["rules"]:
    if rule["rule_id"] == "vacation.acquisition_period":
        rule["decision"] = (
            "Período aquisitivo é de 12 meses contado do vínculo e não reinicia em 1º de janeiro. "
            "Para férias proporcionais, cada mês aquisitivo completo vale 1/12 e a fração com "
            "pelo menos 15 dias também vale 1/12."
        )
    if rule["rule_id"] == "termination.vacation_proportional":
        rule["decision"] = (
            "Devida apenas nos motivos suportados 02/07/33; não em 01. A contagem deve permanecer "
            "ancorada no período aquisitivo e usar 1/12 por mês aquisitivo completo ou fração com "
            "pelo menos 15 dias."
        )
inventory_path.write_text(
    json.dumps(inventory, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
)

# Exact reader compatibility follows the schema/API minor bump.
test_contract = ROOT / "tests" / "test_contract_v1.py"
text = test_contract.read_text(encoding="utf-8")
text = text.replace('assert contract.schema_version == "1.0.0"', 'assert contract.schema_version == "1.1.0"')
text = text.replace(
    'consumer="H26", schema_version="1.0.0", contract_api_version="1.0.0"',
    'consumer="H26", schema_version="1.1.0", contract_api_version="1.1.0"',
)
text = text.replace(
    'consumer="H26", schema_version="1.0.1", contract_api_version="1.0.0"',
    'consumer="H26", schema_version="1.0.0", contract_api_version="1.1.0"',
)
text = text.replace(
    'consumer="H26", schema_version="1.0.0", contract_api_version="1.1.0"',
    'consumer="H26", schema_version="1.1.0", contract_api_version="1.0.0"',
    1,
)

# Add an executable contract regression for the A02 semantic field.
marker = '\ndef test_decimal_values_round_trip_as_strings_in_json_mode() -> None:\n'
addition = '''\ndef test_vacation_period_rule_carries_proportional_15_day_semantics() -> None:\n    contract = FiscalContractV1.model_validate(load_example())\n    rule = contract.select_rule(\n        "vacation.acquisition_period",\n        date(2026, 3, 31),\n        AssessmentContext.TERMINATION,\n    )\n    assert rule.rule_version == "1.1.0"\n    assert (\n        rule.payload.proportional_accrual_method\n        == "one_twelfth_per_acquisition_month_or_fraction_gte_days"\n    )\n    assert rule.payload.proportional_qualifying_days == 15\n\n'''
if addition not in text:
    if marker not in text:
        raise SystemExit("test insertion marker not found")
    text = text.replace(marker, addition + marker, 1)
test_contract.write_text(text, encoding="utf-8")

handoff = ROOT / "docs" / "phase2-to-phase3-handoff.md"
handoff_text = handoff.read_text(encoding="utf-8")
amendment = '''\n## 10. Correção aditiva descoberta na Fase 3 — A02\n\nAo implementar férias proporcionais, a Fase 3 encontrou uma lacuna objetiva entre o inventário e a forma computacional do payload `period_rule`: o contrato dizia que a contagem era ancorada no período aquisitivo, mas não transportava para a máquina o limiar de 15 dias da fração proporcional.\n\nAplicando o próprio gate deste handoff, a semântica **não foi hardcoded no engine**. O Contrato v1 foi corrigido de forma aditiva:\n\n```text\nschema_version       1.0.0 -> 1.1.0\ncontract_api_version 1.0.0 -> 1.1.0\nrule_inventory       1.0.0 -> 1.1.0\nvacation.acquisition_period.rule_version 1.0.0 -> 1.1.0\n```\n\nO `PeriodRulePayload` passa a declarar explicitamente:\n\n```text\nproportional_accrual_method = one_twelfth_per_acquisition_month_or_fraction_gte_days\nproportional_qualifying_days = 15\n```\n\nA política de compatibilidade continua `exact`: consumidores 1.0.0 não devem aceitar silenciosamente o documento 1.1.0. A mudança é aditiva em capacidade, mas só é consumível depois de teste explícito da versão nova.\n'''
if amendment not in handoff_text:
    handoff.write_text(handoff_text.rstrip() + "\n" + amendment, encoding="utf-8")

print("vacation contract amendment applied")
