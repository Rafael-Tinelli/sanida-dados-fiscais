from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Remove accidental duplicate declarations left by the prior A02 patch. They are
# semantically identical and Python keeps only the last annotation, but the source
# must remain canonical and auditable.
types_path = ROOT / "sanida_fiscal" / "types_v1.py"
text = types_path.read_text(encoding="utf-8")
start = text.index("class PeriodRulePayload(StrictModel):")
end = text.index("\n\nclass FormulaComponent(StrictModel):", start)
canonical = '''class PeriodRulePayload(StrictModel):
    type: Literal["period_rule"] = "period_rule"
    duration_months: int = Field(gt=0)
    anchor: Literal["employment_start_anniversary"]
    calendar_year_reset: Literal[False] = False
    proportional_accrual_method: Literal[
        "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    ] = "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    proportional_qualifying_days: Literal[15] = 15
'''
text = text[:start] + canonical + text[end:]
types_path.write_text(text, encoding="utf-8")

candidate_path = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"
data = json.loads(candidate_path.read_text(encoding="utf-8"))

if not any(rule["rule_id"] == "termination.vacation_proportional" for rule in data["rules"]):
    vacation_rule = {
        "rule_id": "termination.vacation_proportional",
        "rule_version": "1.0.0",
        "domain": "termination",
        "calculators": ["H29"],
        "contexts": ["termination"],
        "description": (
            "Elegibilidade das férias proporcionais no período aquisitivo corrente "
            "por motivo de desligamento suportado."
        ),
        "applies_to": "current_vacation_acquisition_period_at_termination",
        "applicability": [],
        "dependencies": [
            "termination.reason_scope",
            "vacation.acquisition_period",
        ],
        "calculation_order": 31,
        "competence": {"basis": "termination_date"},
        "vigency": {"effective_from": "2026-01-01"},
        "payload": {
            "type": "code_eligibility",
            "eligible_codes": ["02", "07", "33"],
            "ineligible_codes": ["01"],
            "unsupported_behavior": "UNSUPPORTED",
            "code_system": "esocial_table_19_termination_reason",
        },
        "provenance": [
            {
                "source_id": "PLANALTO_CLT",
                "role": "normative_primary",
                "observed_at_utc": "2026-09-13T12:00:00Z",
                "status": "AVAILABLE",
                "retrieval_method": "manual",
            },
            {
                "source_id": "MTE_TERMINATION_FAQ",
                "role": "official_operational",
                "observed_at_utc": "2026-09-13T12:00:00Z",
                "status": "AVAILABLE",
                "retrieval_method": "manual",
            },
        ],
        "quality": {
            "status": "VALIDATED",
            "reference_case_ids": [
                "vacation_period_cross_year_A02_regression",
                "termination_reason_01_just_cause",
                "termination_reason_02_without_just_cause",
                "termination_reason_07_resignation",
                "termination_reason_33_mutual_agreement",
            ],
            "reviewed_by_human": True,
        },
        "change_class": "RULE_ADDED",
        "update_policy": {
            "rule_class": "structural_rule",
            "auto_publish": False,
            "structural_review_required": True,
        },
    }
    insert_at = next(
        index
        for index, rule in enumerate(data["rules"])
        if rule["rule_id"] == "termination.acquired_and_overdue_vacation"
    )
    data["rules"].insert(insert_at, vacation_rule)

candidate_path.write_text(
    json.dumps(data, ensure_ascii=False, indent=2) + "\n",
    encoding="utf-8",
)

handoff_path = ROOT / "docs" / "phase2-to-phase3-handoff.md"
handoff = handoff_path.read_text(encoding="utf-8")
handoff = handoff.replace(
    "O `CANDIDATE` materializa 20 regras representativas e cobre as 18 famílias.",
    "O `CANDIDATE` materializa 21 regras representativas e cobre as 18 famílias; a regra `termination.vacation_proportional` foi materializada na Fase 3 para execução da matriz H29 já fechada no inventário.",
)
handoff_path.write_text(handoff, encoding="utf-8")

print("H29 candidate patch applied")
