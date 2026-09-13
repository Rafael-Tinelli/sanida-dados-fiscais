#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"
COVERAGE = ROOT / "docs" / "contract-coverage-v1.json"
SOURCES = ROOT / "docs" / "source-registry-v1.json"
OBSERVED_AT = "2026-09-13T12:00:00Z"


def load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    data = load(EXAMPLE)
    coverage = load(COVERAGE)
    source_registry = load(SOURCES)
    coverage_by_id = {item["rule_id"]: item for item in coverage["rules"]}
    source_by_id = {item["source_id"]: item for item in source_registry["sources"]}

    data.setdefault("lifecycle", {})
    current = {rule["rule_id"]: rule for rule in data["rules"]}

    for rule_id, rule in current.items():
        spec = coverage_by_id[rule_id]
        rule["calculators"] = spec["calculators"]
        rule["contexts"] = spec["contexts"]
        rule["competence"]["basis"] = spec["competence_basis"]
        if spec["competence_basis"] == "rule_specific":
            rule["competence"].setdefault(
                "description", "Rule-specific competence defined by the Phase 1 decision."
            )
        rule["update_policy"]["rule_class"] = spec["rule_class"]
        for evidence in rule["provenance"]:
            evidence.setdefault("retrieval_method", "manual")

    def provenance(source_ids):
        return [
            {
                "source_id": source_id,
                "role": source_by_id[source_id]["role"],
                "observed_at_utc": OBSERVED_AT,
                "status": "AVAILABLE",
                "retrieval_method": "manual",
            }
            for source_id in source_ids
        ]

    def build(
        rule_id,
        description,
        applies_to,
        payload,
        source_ids,
        effective_from,
        *,
        dependencies=None,
        order=10,
        reference_case_ids=None,
        rounding_policy=None,
    ):
        spec = coverage_by_id[rule_id]
        structural = spec["rule_class"] in {
            "structural_rule",
            "product_scope_rule",
            "technical_contract_rule",
        }
        competence = {"basis": spec["competence_basis"]}
        if spec["competence_basis"] == "rule_specific":
            competence["description"] = (
                "Rule-specific competence defined by the Phase 1 decision."
            )
        result = {
            "rule_id": rule_id,
            "rule_version": "1.0.0",
            "domain": spec["domain"],
            "calculators": spec["calculators"],
            "contexts": spec["contexts"],
            "description": description,
            "applies_to": applies_to,
            "applicability": [],
            "dependencies": dependencies or [],
            "calculation_order": order,
            "competence": competence,
            "vigency": {"effective_from": effective_from},
            "payload": payload,
            "provenance": provenance(source_ids),
            "quality": {
                "status": "VALIDATED",
                "reference_case_ids": reference_case_ids or [],
                "reviewed_by_human": structural,
            },
            "change_class": "RULE_ADDED",
            "update_policy": {
                "rule_class": spec["rule_class"],
                "auto_publish": not structural,
                "structural_review_required": structural,
            },
        }
        if rounding_policy:
            result["rounding_policy"] = rounding_policy
        return result

    additions = [
        build(
            "irrf.dependent_deduction",
            "Dedução unitária por dependente para a apuração correspondente.",
            "legal_deductions_for_matching_income_type",
            {"type": "scalar", "value": "189.59", "unit": "BRL_per_dependent"},
            ["RFB_IRRF_TABLE_2026", "ESOCIAL_TABLES_S13_NT07_2026"],
            "2026-01-01",
            order=15,
        ),
        build(
            "irrf.deductions_by_income_type",
            "Deduções de IR tipadas por contexto de apuração.",
            "monthly_or_thirteenth_or_vacation_irrf_assessment",
            {
                "type": "policy",
                "policy_kind": "deductions_by_income_type",
                "assertions": [
                    "deductions_must_match_assessment_context",
                    "pension_must_not_be_silently_zeroed_when_applicable",
                ],
                "values": {
                    "contexts": [
                        "monthly",
                        "thirteenth",
                        "vacation_enjoyed",
                        "termination",
                    ]
                },
            },
            ["RFB_IN_1500_2014", "ESOCIAL_TABLES_S13_NT07_2026"],
            "2026-01-01",
            order=12,
        ),
        build(
            "thirteenth.reference_remuneration",
            "Referência remuneratória do 13º anual e rescisório.",
            "thirteenth_gross_reference",
            {
                "type": "remuneration_reference",
                "annual_reference": "december_due_remuneration",
                "termination_reference": "termination_month_remuneration",
            },
            ["PLANALTO_DECRETO_10854_2021", "MTE_TERMINATION_FAQ"],
            "2021-11-10",
            order=11,
        ),
        build(
            "thirteenth.variable_remuneration",
            "Tratamento canônico da remuneração variável do 13º.",
            "variable_component_of_thirteenth",
            {
                "type": "variable_remuneration",
                "accrual_through_month": 11,
                "include_december_in_final_revision": True,
                "revision_deadline_month": 1,
                "revision_deadline_day": 10,
                "precomputed_average_allowed_only_as_external_input": True,
            },
            ["PLANALTO_DECRETO_10854_2021"],
            "2021-11-10",
            order=12,
        ),
        build(
            "thirteenth.advance",
            "Adiantamento do 13º com referência fixa no salário do mês anterior e exceções estruturais.",
            "first_installment_of_thirteenth",
            {
                "type": "thirteenth_advance",
                "fixed_reference": "previous_month_salary",
                "fraction_numerator": 1,
                "fraction_denominator": 2,
                "payment_window_start_month": 2,
                "payment_window_end_month": 11,
                "special_handling": ["admission_in_year", "variable_remuneration"],
            },
            ["PLANALTO_LEI_4749_1965", "PLANALTO_DECRETO_10854_2021"],
            "1965-08-12",
            order=20,
            reference_case_ids=["thirteenth_fixed_advance_previous_month_4000"],
        ),
        build(
            "vacation.acquisition_period",
            "Período aquisitivo de doze meses ancorado no vínculo, sem reset no ano civil.",
            "vacation_entitlement_period",
            {
                "type": "period_rule",
                "duration_months": 12,
                "anchor": "employment_start_anniversary",
                "calendar_year_reset": False,
            },
            ["PLANALTO_CLT"],
            "1943-05-01",
            reference_case_ids=["vacation_period_cross_year_A02_regression"],
        ),
        build(
            "vacation.entitlement_days_by_absences",
            "Faixas de dias de férias por faltas injustificadas conforme inventário fechado.",
            "vacation_entitled_days",
            {
                "type": "entitlement_bands",
                "bands": [
                    {"min_absences": 0, "max_absences": 5, "entitled_days": 30},
                    {"min_absences": 6, "max_absences": 14, "entitled_days": 24},
                    {"min_absences": 15, "max_absences": 23, "entitled_days": 18},
                    {"min_absences": 24, "max_absences": 32, "entitled_days": 12},
                ],
                "outside_behavior": "UNSUPPORTED",
            },
            ["PLANALTO_CLT"],
            "1943-05-01",
            order=11,
            reference_case_ids=[
                "vacation_entitlement_absences_5",
                "vacation_entitlement_absences_6",
                "vacation_entitlement_absences_15",
                "vacation_entitlement_absences_24",
                "vacation_entitlement_absences_32",
            ],
        ),
        build(
            "vacation.remuneration_and_constitutional_third",
            "Componentes da remuneração de férias e adicional constitucional.",
            "vacation_remuneration",
            {
                "type": "component_formula",
                "components": [
                    {
                        "component": "vacation_remuneration",
                        "basis": "vacation_pay_base",
                        "multiplier": "1",
                    },
                    {
                        "component": "constitutional_third",
                        "basis": "vacation_remuneration",
                        "multiplier": "0.3333333333333333333333333333",
                    },
                ],
            },
            ["PLANALTO_CLT", "RFB_QA_IRPF_2026"],
            "1988-10-05",
            order=20,
            rounding_policy={
                "decimal_places": 2,
                "mode": "ROUND_HALF_UP",
                "stage": "per_component",
            },
        ),
        build(
            "termination.partial_output_scope",
            "H29 declara resultado parcial enquanto verbas fora do escopo não forem modeladas.",
            "termination_result_promise",
            {
                "type": "scope_declaration",
                "promise": "partial_estimate",
                "included_items": [
                    "salary_balance",
                    "thirteenth_proportional",
                    "vacation_proportional",
                    "acquired_or_overdue_vacation",
                ],
                "excluded_items": [
                    "notice_pay_or_notice_discount",
                    "fgts_termination_fine",
                    "fgts_withdrawal",
                    "unemployment_insurance",
                    "stability_indemnities",
                    "collective_bargaining_specific_items",
                    "fixed_term_contract_termination_rules",
                    "indirect_termination_without_judicially_resolved_context",
                    "variable_termination_items_not_explicitly_modeled",
                ],
                "require_user_disclosure": True,
            },
            ["MTE_TERMINATION_FAQ"],
            "2026-01-01",
            dependencies=["termination.reason_scope"],
            order=5,
        ),
        build(
            "termination.salary_balance",
            "Prorrateio de saldo para mensalista/quinzenalista no escopo v1.",
            "monthly_employee_salary_balance",
            {
                "type": "proration",
                "formula": "base_times_numerator_over_denominator",
                "base_semantic": "monthly_base_salary",
                "numerator_semantic": "days_counted_through_termination",
                "denominator_semantic": "calendar_days_in_month",
                "universal_fixed_denominator": False,
            },
            ["ESOCIAL_WEB_MEI_2026", "MTE_TERMINATION_FAQ"],
            "2026-01-01",
            dependencies=["termination.reason_scope"],
            order=20,
            reference_case_ids=["termination_salary_balance_monthly_31_days"],
            rounding_policy={
                "decimal_places": 2,
                "mode": "ROUND_HALF_UP",
                "stage": "salary_balance_result",
            },
        ),
        build(
            "termination.thirteenth_proportional",
            "Elegibilidade do 13º proporcional por motivo suportado.",
            "termination_thirteenth_entitlement",
            {
                "type": "code_eligibility",
                "eligible_codes": ["02", "07", "33"],
                "ineligible_codes": ["01"],
                "unsupported_behavior": "UNSUPPORTED",
            },
            [
                "PLANALTO_LEI_4090_1962",
                "PLANALTO_DECRETO_10854_2021",
                "MTE_TERMINATION_FAQ",
            ],
            "2026-01-01",
            dependencies=["termination.reason_scope", "thirteenth.accrual.twelfths"],
            order=30,
        ),
        build(
            "termination.acquired_and_overdue_vacation",
            "Estados de períodos integrais adquiridos e vencidos na rescisão.",
            "completed_vacation_acquisition_periods",
            {
                "type": "period_state",
                "states": [
                    "acquired_within_concession_period",
                    "overdue_beyond_concession_period",
                ],
                "overdue_double_requires_explicit_rule": True,
                "initial_consumer_may_limit_to_one_period_if_disclosed": True,
            },
            ["PLANALTO_CLT", "MTE_TERMINATION_FAQ"],
            "2026-01-01",
            dependencies=["termination.reason_scope"],
            order=35,
        ),
    ]

    for new_rule in additions:
        current[new_rule["rule_id"]] = new_rule

    ordered_ids = [rule["rule_id"] for rule in data["rules"]]
    for new_rule in additions:
        if new_rule["rule_id"] not in ordered_ids:
            ordered_ids.append(new_rule["rule_id"])
    data["rules"] = [current[rule_id] for rule_id in ordered_ids]
    data["notes"] = (
        "CANDIDATE de cobertura de schema da Fase 2. Não é release de produção "
        "e não contém snapshots reais."
    )

    EXAMPLE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "generate_contract_schema.py")],
        check=True,
    )
    print(f"candidate rules: {len(data['rules'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
