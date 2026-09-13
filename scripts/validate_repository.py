#!/usr/bin/env python3
"""Gates determinísticos do repositório sanida-dados-fiscais.

Este script não acessa a internet, não executa scrapers e não altera arquivos.
Enquanto o Contrato Fiscal Canônico v1 ainda não existe, ele protege o baseline,
os artefatos de fechamento da Fase 1 e os casos de referência que serão a
entrada formal da Fase 2.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
CALCULATORS = {"H26", "H27", "H28", "H29"}
PHASE1_REQUIRED_RULES = {
    "inss.employee.progressive_table",
    "irrf.monthly.progressive_table",
    "irrf.simplified_monthly_discount",
    "irrf.deductions_by_income_type",
    "irrf.reduction.2026",
    "irrf.income_type",
    "thirteenth.accrual.twelfths",
    "thirteenth.variable_remuneration",
    "thirteenth.advance",
    "thirteenth.irrf.reduction.2026",
    "vacation.acquisition_period",
    "vacation.entitlement_days_by_absences",
    "vacation.abono_pecuniario",
    "vacation.abono.ir_exemption",
    "vacation.abono_constitutional_third.ir_incidence",
    "vacation.irrf.separate_assessment",
    "vacation.irrf.reduction.2026",
    "termination.reason_scope",
    "termination.partial_output_scope",
    "termination.salary_balance",
    "termination.thirteenth_proportional",
    "termination.vacation_proportional",
    "technical.money_decimal_and_rounding",
    "technical.contract_vigency_and_quality",
}
PHASE1_REQUIRED_CASES = {
    "rfd_irrf_2026_salary_6000_A01_regression",
    "thirteenth_accrual_14_days_no_avo",
    "thirteenth_accrual_15_days_one_avo",
    "thirteenth_fixed_advance_previous_month_4000",
    "vacation_period_cross_year_A02_regression",
    "vacation_abono_entitlement_30_days",
    "vacation_abono_principal_tax_treatment",
    "vacation_abono_constitutional_third_tax_treatment_P0",
    "termination_reason_01_just_cause",
    "termination_reason_02_without_just_cause",
    "termination_reason_07_resignation",
    "termination_reason_33_mutual_agreement",
    "termination_salary_balance_monthly_31_days",
}


class ValidationError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValidationError(message)


def load_json(relative_path: str) -> Any:
    path = ROOT / relative_path
    require(path.is_file(), f"arquivo obrigatório ausente: {relative_path}")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValidationError(f"JSON inválido em {relative_path}: {exc}") from exc


def is_finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def validate_required_files() -> None:
    required = [
        "README.md",
        "scraper.py",
        "update_taxas.py",
        "dados_fiscais.json",
        "taxas_bacen.json",
        "docs/inventario-juridico-fiscal-v1.md",
        "docs/rule-inventory-v1.json",
        "docs/source-registry-v1.json",
        "docs/phase1-closure.md",
        "tests/reference_cases/phase1_reference_cases.json",
    ]
    for relative_path in required:
        require(
            (ROOT / relative_path).is_file(),
            f"arquivo obrigatório ausente: {relative_path}",
        )


def validate_monotonic_table(
    rows: list[dict[str, Any]], *, name: str, rate_key: str = "aliquota"
) -> None:
    require(rows, f"{name}: tabela vazia")
    previous_limit = -math.inf
    previous_rate = -math.inf
    for index, row in enumerate(rows):
        require(isinstance(row, dict), f"{name}[{index}]: item deve ser objeto")
        limit = row.get("limite")
        rate = row.get(rate_key)
        require(is_finite_number(limit), f"{name}[{index}].limite inválido")
        require(is_finite_number(rate), f"{name}[{index}].{rate_key} inválida")
        require(
            float(limit) > previous_limit,
            f"{name}: limites devem ser estritamente crescentes",
        )
        require(0 <= float(rate) <= 1, f"{name}[{index}].{rate_key} fora de 0..1")
        require(
            float(rate) >= previous_rate,
            f"{name}: alíquotas devem ser não decrescentes",
        )
        previous_limit = float(limit)
        previous_rate = float(rate)


def validate_dados_fiscais() -> None:
    data = load_json("dados_fiscais.json")
    require(isinstance(data, dict), "dados_fiscais.json deve conter objeto")
    require(isinstance(data.get("schema_version"), str), "schema_version ausente")
    require(isinstance(data.get("ano"), int), "ano deve ser inteiro")

    meta = data.get("meta")
    require(isinstance(meta, dict), "meta ausente")
    require(
        isinstance(meta.get("generated_at_utc"), str),
        "meta.generated_at_utc ausente",
    )
    require(isinstance(meta.get("sources"), dict), "meta.sources ausente")
    require(isinstance(meta.get("errors"), list), "meta.errors deve ser lista")
    require(isinstance(meta.get("warnings"), list), "meta.warnings deve ser lista")

    dep = data.get("dep")
    require(is_finite_number(dep) and float(dep) >= 0, "dep inválido")

    inss = data.get("inss")
    require(
        isinstance(inss, list) and len(inss) >= 4,
        "tabela INSS ausente/incompleta",
    )
    validate_monotonic_table(inss, name="inss")

    irrf = data.get("irrf")
    require(isinstance(irrf, dict), "irrf ausente")
    tabela = irrf.get("tabela")
    require(
        isinstance(tabela, list) and len(tabela) >= 4,
        "tabela IRRF ausente/incompleta",
    )
    validate_monotonic_table(tabela, name="irrf.tabela")
    for index, row in enumerate(tabela):
        require(
            is_finite_number(row.get("deducao")),
            f"irrf.tabela[{index}].deducao inválida",
        )
        require(float(row["deducao"]) >= 0, f"irrf.tabela[{index}].deducao negativa")

    simplificado = irrf.get("simplificado")
    require(
        is_finite_number(simplificado) and float(simplificado) >= 0,
        "irrf.simplificado inválido",
    )

    reducao = irrf.get("reducao_mensal")
    require(isinstance(reducao, dict), "irrf.reducao_mensal ausente")
    for key in ("isenta_ate", "reduz_ate", "max_reducao_ate_5000", "a", "b"):
        require(
            is_finite_number(reducao.get(key)),
            f"irrf.reducao_mensal.{key} inválido",
        )
    require(
        float(reducao["isenta_ate"]) <= float(reducao["reduz_ate"]),
        "limites do redutor invertidos",
    )

    taxas = data.get("taxas")
    require(isinstance(taxas, dict), "taxas ausente em dados_fiscais.json")
    for key in ("selic", "cdi"):
        require(is_finite_number(taxas.get(key)), f"taxas.{key} inválida")


def validate_taxas_bacen() -> None:
    data = load_json("taxas_bacen.json")
    require(isinstance(data, dict), "taxas_bacen.json deve conter objeto")
    meta = data.get("meta")
    require(isinstance(meta, dict), "taxas_bacen.meta ausente")
    require(
        isinstance(meta.get("generated_at_utc"), str),
        "taxas_bacen.meta.generated_at_utc ausente",
    )
    taxas = data.get("taxas")
    require(isinstance(taxas, dict), "taxas_bacen.taxas ausente")
    for key in ("selic", "cdi"):
        require(is_finite_number(taxas.get(key)), f"taxas_bacen.taxas.{key} inválida")


def validate_source_registry() -> set[str]:
    data = load_json("docs/source-registry-v1.json")
    require(data.get("status") == "phase1_closed", "source registry não está fechado")

    policy = data.get("policy")
    require(isinstance(policy, dict), "source registry: policy ausente")
    roles = policy.get("source_roles")
    require(isinstance(roles, list) and roles, "source registry: source_roles vazio")
    allowed_roles = set(roles)

    sources = data.get("sources")
    require(isinstance(sources, list) and sources, "source registry: sources vazio")

    ids: set[str] = set()
    for index, source in enumerate(sources):
        require(isinstance(source, dict), f"sources[{index}] deve ser objeto")
        source_id = source.get("source_id")
        require(isinstance(source_id, str) and source_id, f"sources[{index}].source_id ausente")
        require(source_id not in ids, f"source_id duplicado: {source_id}")
        ids.add(source_id)

        require(source.get("role") in allowed_roles, f"{source_id}: role inválido")
        require(isinstance(source.get("authority"), str) and source["authority"], f"{source_id}: authority ausente")
        url = source.get("url")
        require(isinstance(url, str) and url.startswith("https://"), f"{source_id}: URL oficial inválida")
        host = (urlparse(url).hostname or "").lower()
        require(
            host.endswith("gov.br") or host == "planalto.gov.br",
            f"{source_id}: host não oficial no registro canônico: {host}",
        )
        require(isinstance(source.get("covers"), list), f"{source_id}: covers deve ser lista")

    return ids


def validate_rule_inventory(source_ids: set[str]) -> dict[str, dict[str, Any]]:
    data = load_json("docs/rule-inventory-v1.json")
    require(data.get("status") == "phase1_closed", "rule inventory não está fechado")
    require(set(data.get("scope", [])) == CALCULATORS, "rule inventory: escopo H26–H29 incompleto")

    policy = data.get("contract_entry_policy")
    require(isinstance(policy, dict), "rule inventory: contract_entry_policy ausente")
    require(policy.get("structural_rules_require_human_review") is True, "mudança estrutural deve exigir revisão humana")
    require(policy.get("missing_or_unvalidated_data_must_not_become_zero") is True, "ausência não pode virar zero")
    require(policy.get("historical_data_must_not_be_relabelled_as_current") is True, "dado histórico não pode ser relabelado")
    require(policy.get("money_representation") == "decimal", "money_representation deve ser decimal")

    rules = data.get("rules")
    require(isinstance(rules, list) and rules, "rule inventory: rules vazio")
    indexed: dict[str, dict[str, Any]] = {}
    for index, rule in enumerate(rules):
        require(isinstance(rule, dict), f"rules[{index}] deve ser objeto")
        rule_id = rule.get("rule_id")
        require(isinstance(rule_id, str) and rule_id, f"rules[{index}].rule_id ausente")
        require(rule_id not in indexed, f"rule_id duplicado: {rule_id}")
        indexed[rule_id] = rule

        calculators = rule.get("calculators")
        require(isinstance(calculators, list) and calculators, f"{rule_id}: calculators ausente")
        require(set(calculators) <= CALCULATORS, f"{rule_id}: calculator inválido")
        require(isinstance(rule.get("rule_class"), str) and rule["rule_class"], f"{rule_id}: rule_class ausente")
        require(isinstance(rule.get("applies_to"), str) and rule["applies_to"], f"{rule_id}: applies_to ausente")
        require(isinstance(rule.get("decision"), str) and rule["decision"], f"{rule_id}: decision ausente")

        refs = rule.get("source_ids")
        require(isinstance(refs, list), f"{rule_id}: source_ids deve ser lista")
        unknown = set(refs) - source_ids
        require(not unknown, f"{rule_id}: source_ids desconhecidos: {sorted(unknown)}")

    missing = PHASE1_REQUIRED_RULES - set(indexed)
    require(not missing, f"rule inventory perdeu regras obrigatórias: {sorted(missing)}")

    termination = data.get("termination_v1_scope")
    require(isinstance(termination, dict), "termination_v1_scope ausente")
    require(termination.get("contract_type") == "indefinite_term_only", "H29 v1 deve manter prazo indeterminado")
    reasons = termination.get("supported_esocial_reasons")
    require(isinstance(reasons, dict), "supported_esocial_reasons ausente")
    require(set(reasons) == {"01", "02", "07", "33"}, "motivos H29 v1 devem ser exatamente 01/02/07/33")

    for code in ("02", "07", "33"):
        require(reasons[code].get("thirteenth_proportional") is True, f"motivo {code}: 13º proporcional deve ser verdadeiro")
        require(reasons[code].get("vacation_proportional") is True, f"motivo {code}: férias proporcionais devem ser verdadeiras")
    require(reasons["01"].get("thirteenth_proportional") is False, "motivo 01: 13º proporcional deve ser falso")
    require(reasons["01"].get("vacation_proportional") is False, "motivo 01: férias proporcionais devem ser falsas")

    exclusions = termination.get("explicitly_excluded_from_total")
    require(isinstance(exclusions, list) and len(exclusions) >= 5, "H29 precisa de exclusões explícitas")

    abono_third = indexed["vacation.abono_constitutional_third.ir_incidence"]
    require(abono_third.get("current_state") == "new_P0_defect", "novo P0 do terço sobre abono precisa permanecer explícito")

    vacation_reduction = indexed["vacation.irrf.reduction.2026"]
    require(
        vacation_reduction.get("evidence_level") == "direct_normative_linkage_without_specific_worked_example",
        "nível de evidência do redutor em férias deve permanecer explícito",
    )

    return indexed


def validate_reference_cases(source_ids: set[str]) -> None:
    data = load_json("tests/reference_cases/phase1_reference_cases.json")
    require(data.get("status") == "phase1_closed", "casos de referência não estão fechados")
    cases = data.get("cases")
    require(isinstance(cases, list) and cases, "lista de casos de referência vazia")

    ids: set[str] = set()
    indexed: dict[str, dict[str, Any]] = {}
    for index, case in enumerate(cases):
        require(isinstance(case, dict), f"cases[{index}] deve ser objeto")
        case_id = case.get("case_id")
        require(isinstance(case_id, str) and case_id, f"cases[{index}].case_id ausente")
        require(case_id not in ids, f"case_id duplicado: {case_id}")
        ids.add(case_id)
        indexed[case_id] = case
        require(case.get("source") in source_ids, f"{case_id}: source não existe no registry")
        require(case.get("calculator_target") in CALCULATORS, f"{case_id}: calculator_target inválido")
        require(isinstance(case.get("inputs"), dict), f"{case_id}: inputs ausente")
        require(isinstance(case.get("expected"), dict), f"{case_id}: expected ausente")

    missing = PHASE1_REQUIRED_CASES - ids
    require(not missing, f"casos obrigatórios ausentes: {sorted(missing)}")

    a01 = indexed["rfd_irrf_2026_salary_6000_A01_regression"]["expected"]
    require(a01.get("irrf_tax_base") == "5350.40", "A01: base IR de referência mudou")
    require(a01.get("reduction_input_income") == "6000.00", "A01: entrada do redutor mudou")
    require(a01.get("final_irrf") == "382.88", "A01: IR final de referência mudou")
    require(a01.get("reduction_input_income") != a01.get("irrf_tax_base"), "A01: rendimento e base IR colapsaram")

    require(indexed["thirteenth_accrual_14_days_no_avo"]["expected"]["twelfths_for_month"] == 0, "13º: 14 dias não deve gerar avo")
    require(indexed["thirteenth_accrual_15_days_one_avo"]["expected"]["twelfths_for_month"] == 1, "13º: 15 dias deve gerar avo")

    a02 = indexed["vacation_period_cross_year_A02_regression"]
    require(a02["expected"].get("proportional_vacation_twelfths") == 7, "A02: referência deve permanecer 7/12")

    abono = indexed["vacation_abono_entitlement_30_days"]
    require(abono["expected"].get("cash_allowance_days") == 10, "abono: direito de 30 dias deve produzir 10 dias")

    principal = indexed["vacation_abono_principal_tax_treatment"]["expected"]
    third = indexed["vacation_abono_constitutional_third_tax_treatment_P0"]["expected"]
    require(principal == {"irrf_incidence": False, "social_security_incidence": False}, "abono principal: incidências mudaram")
    require(third == {"irrf_incidence": True, "social_security_incidence": False}, "terço sobre abono: incidências mudaram")

    termination_expectations = {
        "termination_reason_01_just_cause": (False, False),
        "termination_reason_02_without_just_cause": (True, True),
        "termination_reason_07_resignation": (True, True),
        "termination_reason_33_mutual_agreement": (True, True),
    }
    for case_id, (thirteenth, vacation) in termination_expectations.items():
        expected = indexed[case_id]["expected"]
        require(expected.get("salary_balance") is True, f"{case_id}: saldo deve permanecer suportado")
        require(expected.get("thirteenth_proportional") is thirteenth, f"{case_id}: 13º proporcional mudou")
        require(expected.get("vacation_proportional") is vacation, f"{case_id}: férias proporcionais mudaram")

    salary_balance = indexed["termination_salary_balance_monthly_31_days"]["expected"]
    require(salary_balance.get("salary_balance") == "1000.00", "saldo de salário 31 dias mudou")


def validate_inventory_and_readme() -> None:
    inventory = (ROOT / "docs/inventario-juridico-fiscal-v1.md").read_text(encoding="utf-8")
    for token in (
        "Fase 1 CONCLUÍDA",
        "A01",
        "A02",
        "vacation.abono_constitutional_third.ir_incidence",
        "01/02/07/33",
        "Contrato Fiscal Canônico v1",
    ):
        require(token in inventory, f"inventário perdeu marcador obrigatório: {token}")

    closure = (ROOT / "docs/phase1-closure.md").read_text(encoding="utf-8")
    for token in (
        "Status:** CONCLUÍDA",
        "Handoff",
        "P0 adicional",
        "Fase 2 — Contrato Fiscal Canônico v1",
    ):
        require(token.lower() in closure.lower(), f"fechamento perdeu marcador obrigatório: {token}")

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    for token in (
        "Scraper não é autoridade jurídica",
        "Fase 1 — Inventário jurídico-fiscal",
        "docs/phase1-closure.md",
        "Fase 2 — Contrato Fiscal Canônico v1",
        "Remake CI",
    ):
        require(token in readme, f"README perdeu princípio/marcador obrigatório: {token}")
    require(
        "Fase 1 — Inventário jurídico-fiscal\n\n**Status: CONCLUÍDA**" in readme,
        "README deve marcar Fase 1 como CONCLUÍDA",
    )


def main() -> int:
    validate_required_files()
    print("PASS validate_required_files")

    validate_dados_fiscais()
    print("PASS validate_dados_fiscais")

    validate_taxas_bacen()
    print("PASS validate_taxas_bacen")

    source_ids = validate_source_registry()
    print("PASS validate_source_registry")

    validate_rule_inventory(source_ids)
    print("PASS validate_rule_inventory")

    validate_reference_cases(source_ids)
    print("PASS validate_reference_cases")

    validate_inventory_and_readme()
    print("PASS validate_inventory_and_readme")

    print("Repository validation: PASS")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValidationError as exc:
        print(f"Repository validation: FAIL — {exc}")
        raise SystemExit(1)
