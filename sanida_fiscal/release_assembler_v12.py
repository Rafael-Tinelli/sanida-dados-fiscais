from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Mapping

from .contract_v1 import FiscalContractV1
from .contract_v1_2 import FiscalContractV12, GovernanceEvidenceObservation
from .semantic_diff_v1 import diff_contracts
from .sources_v1 import NormalizedSourceCandidate, ParseStatus
from .types_v1 import (
    ChangeClass,
    EvidenceObservation,
    POLICY_ASSERTIONS_BY_KIND,
    PolicyKind,
    RuleClass,
    STRUCTURAL_RULE_CLASSES,
)


class ReleaseAssemblyError(RuntimeError):
    pass


RFB_SOURCE_ID = "RFB_IRRF_TABLE_2026"
INSS_SOURCE_ID = "INSS_TABLE_2026"

TECHNICAL_MONEY_ROUNDING_POLICY: dict[str, Any] = {
    "decimal_places": 2,
    "mode": "ROUND_HALF_UP",
    "stage": "per_component",
}


EXTERNAL_EVIDENCE_PREFERENCES = {
    "inss.employee.progressive_table": INSS_SOURCE_ID,
    "thirteenth.inss.separate_assessment": INSS_SOURCE_ID,
    "irrf.monthly.progressive_table": RFB_SOURCE_ID,
    "irrf.dependent_deduction": RFB_SOURCE_ID,
    "irrf.simplified_monthly_discount": RFB_SOURCE_ID,
    "irrf.reduction.2026": RFB_SOURCE_ID,
    "vacation.irrf.reduction.2026": "PLANALTO_LEI_15270_2025",
}


# These eleven rules are the exact Phase 2 coverage gap identified by the Phase 5
# gate. Metadata already frozen in rule-inventory/contract-coverage is loaded at
# runtime. Only the typed semantic payload and rule-specific mechanics live here.
MISSING_RULE_SPECS: dict[str, dict[str, Any]] = {
    "inss.employee.progressive_table": {
        "description": "Tabela progressiva da contribuição previdenciária do segurado empregado vigente em 2026.",
        "dependencies": [],
        "calculation_order": 20,
        "vigency": {"effective_from": "2026-01-01"},
        "rounding_policy": {
            "decimal_places": 2,
            "mode": "ROUND_HALF_UP",
            "stage": "per_assessment_result",
        },
        "payload_builder": "inss_progressive",
    },
    "irrf.simplified_monthly_discount": {
        "description": "Limite do desconto simplificado comparado separadamente dentro de cada apuração de IRRF suportada.",
        "dependencies": [],
        "calculation_order": 14,
        "vigency": {"effective_from": "2026-01-01"},
        "payload_builder": "irrf_simplified",
    },
    "irrf.income_type": {
        "description": "Partição obrigatória entre apurações mensal, 13º e férias; o contexto de rescisão não cria um quarto tipo de rendimento.",
        "dependencies": [],
        "calculation_order": 1,
        "vigency": {"effective_from": "2026-01-01"},
        "payload_builder": "policy",
    },
    "thirteenth.inss.separate_assessment": {
        "description": "A contribuição previdenciária do 13º é apurada separadamente da remuneração mensal.",
        "dependencies": ["inss.employee.progressive_table"],
        "calculation_order": 25,
        "vigency": {"effective_from": "2026-01-01"},
        "payload_builder": "policy",
    },
    "thirteenth.irrf.exclusive_assessment": {
        "description": "O IRRF do 13º constitui apuração exclusiva, com deduções vinculadas à própria apuração.",
        "dependencies": ["irrf.monthly.progressive_table", "irrf.deductions_by_income_type"],
        "calculation_order": 26,
        "vigency": {"effective_from": "2026-01-01"},
        "payload_builder": "policy",
    },
    "vacation.irrf.separate_assessment": {
        "description": "Férias gozadas são apuradas separadamente para IRRF no mês do pagamento.",
        "dependencies": ["irrf.monthly.progressive_table", "irrf.deductions_by_income_type"],
        "calculation_order": 25,
        "vigency": {"effective_from": "2026-01-01"},
        "payload_builder": "policy",
    },
    "vacation.irrf.reduction.2026": {
        "description": "Redução de IR de 2026 aplicada à apuração separada das férias tributáveis, preservando o rendimento pré-deduções como alvo semântico.",
        "dependencies": ["vacation.irrf.separate_assessment", "irrf.monthly.progressive_table"],
        "calculation_order": 40,
        "vigency": {"effective_from": "2026-01-01", "effective_until": "2026-12-31"},
        "rounding_policy": {
            "decimal_places": 2,
            "mode": "ROUND_HALF_UP",
            "stage": "after_pre_reduction_vacation_irrf",
        },
        "payload_builder": "vacation_reduction",
    },
    "vacation.inss.enjoyed": {
        "description": "Férias gozadas e o respectivo terço integram a base previdenciária do empregado.",
        "dependencies": ["inss.employee.progressive_table", "vacation.remuneration_and_constitutional_third"],
        "calculation_order": 30,
        "vigency": {"effective_from": "2026-01-01"},
        "payload_builder": "vacation_enjoyed_incidence",
    },
    "termination.vacation_indemnity_tax_treatment": {
        "description": "Férias indenizadas na extinção têm natureza tributária/previdenciária explícita e não herdam incidência de férias gozadas.",
        "dependencies": ["vacation.remuneration_and_constitutional_third"],
        "calculation_order": 40,
        "vigency": {"effective_from": "2026-01-01"},
        "payload_builder": "vacation_indemnity_incidence",
    },
    "technical.money_decimal_and_rounding": {
        "description": "Contrato técnico exige Decimal para valores monetários e política explícita de arredondamento por fórmula/estágio.",
        "dependencies": [],
        "calculation_order": 0,
        "vigency": {"effective_from": "2026-01-01"},
        "rounding_policy": TECHNICAL_MONEY_ROUNDING_POLICY,
        "payload_builder": "policy",
    },
    "technical.contract_vigency_and_quality": {
        "description": "Consumidores devem validar vigência, competência, proveniência, qualidade e compatibilidade antes de usar uma regra.",
        "dependencies": [],
        "calculation_order": 0,
        "vigency": {"effective_from": "2026-01-01"},
        "payload_builder": "policy",
    },
}


def _require_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ReleaseAssemblyError("generated_at_utc must be UTC")


def _index_rules(document: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rules = document.get("rules")
    if not isinstance(rules, list):
        raise ReleaseAssemblyError("document has no rules list")
    indexed: dict[str, dict[str, Any]] = {}
    for item in rules:
        if not isinstance(item, dict) or not isinstance(item.get("rule_id"), str):
            raise ReleaseAssemblyError("invalid rule entry")
        if item["rule_id"] in indexed:
            raise ReleaseAssemblyError(f"duplicate rule_id in assembly input: {item['rule_id']}")
        indexed[item["rule_id"]] = item
    return indexed


def _policy_payload(policy_kind: str) -> dict[str, Any]:
    kind = PolicyKind(policy_kind)
    assertions = sorted(item.value for item in POLICY_ASSERTIONS_BY_KIND[kind])
    return {
        "type": "policy",
        "policy_kind": kind.value,
        "assertions": assertions,
    }


def _parsed_payload(
    candidates: Mapping[str, NormalizedSourceCandidate], source_id: str
) -> dict[str, Any]:
    candidate = candidates.get(source_id)
    if candidate is None:
        raise ReleaseAssemblyError(f"normalized candidate missing: {source_id}")
    if candidate.status != ParseStatus.PARSED or candidate.payload is None:
        raise ReleaseAssemblyError(f"normalized candidate is not PARSED: {source_id}")
    return dict(candidate.payload)


def _irrf_reduction_payload(rfb: Mapping[str, Any], *, input_semantic: str) -> dict[str, Any]:
    reduction = rfb.get("monthly_reduction")
    if not isinstance(reduction, Mapping):
        raise ReleaseAssemblyError("RFB candidate has no monthly_reduction")
    return {
        "type": "affine_reduction",
        "full_relief_income_limit": reduction["full_relief_income_upper_brl"],
        "phaseout_income_limit": reduction["phaseout_income_upper_brl"],
        "max_reduction": reduction["max_reduction_brl"],
        "intercept": reduction["intercept_brl"],
        "slope": reduction["slope"],
        "input_semantic": input_semantic,
        "floor_at_zero": True,
        "full_relief_behavior": "max_reduction",
        "phaseout_formula": "intercept_minus_slope_times_input",
        "above_phaseout_behavior": "zero",
    }


def _payload_for_missing_rule(
    rule_id: str,
    *,
    coverage_rule: Mapping[str, Any],
    inventory_rule: Mapping[str, Any],
    candidates: Mapping[str, NormalizedSourceCandidate],
) -> dict[str, Any]:
    builder = MISSING_RULE_SPECS[rule_id]["payload_builder"]
    if builder == "policy":
        policy_kind = coverage_rule.get("policy_kind")
        if not isinstance(policy_kind, str):
            raise ReleaseAssemblyError(f"{rule_id}: policy_kind missing from coverage")
        return _policy_payload(policy_kind)

    if builder == "inss_progressive":
        source = _parsed_payload(candidates, INSS_SOURCE_ID)
        rows = source.get("monthly_table")
        if not isinstance(rows, list) or not rows:
            raise ReleaseAssemblyError("INSS candidate monthly_table missing")
        brackets = []
        for row in rows:
            if not isinstance(row, Mapping):
                raise ReleaseAssemblyError("INSS monthly_table row is invalid")
            brackets.append(
                {
                    "upper_bound": row.get("upper_bound_brl"),
                    "rate": row.get("rate"),
                    "deduction": "0",
                }
            )
        return {
            "type": "progressive_table",
            "calculation_method": "marginal_by_bracket",
            "brackets": brackets,
            "cap_base": source.get("contribution_ceiling_brl"),
        }

    if builder == "irrf_simplified":
        source = _parsed_payload(candidates, RFB_SOURCE_ID)
        return {
            "type": "scalar",
            "value": source.get("simplified_discount_brl"),
            "unit": "BRL",
        }

    if builder == "vacation_reduction":
        source = _parsed_payload(candidates, RFB_SOURCE_ID)
        return _irrf_reduction_payload(
            source,
            input_semantic=str(inventory_rule["applies_to"]),
        )

    if builder == "vacation_enjoyed_incidence":
        return {
            "type": "incidence_profile",
            "components": [
                {
                    "component": "enjoyed_vacation_remuneration",
                    "irrf": "yes",
                    "social_security": "yes",
                },
                {
                    "component": "enjoyed_vacation_constitutional_third",
                    "irrf": "yes",
                    "social_security": "yes",
                },
            ],
        }

    if builder == "vacation_indemnity_incidence":
        return {
            "type": "incidence_profile",
            "components": [
                {
                    "component": "indemnified_vacation_principal",
                    "irrf": "no",
                    "social_security": "no",
                },
                {
                    "component": "indemnified_vacation_constitutional_third",
                    "irrf": "no",
                    "social_security": "no",
                },
            ],
        }

    raise ReleaseAssemblyError(f"unknown payload_builder for {rule_id}: {builder}")


def _rule_source_ids(inventory_rule: Mapping[str, Any]) -> list[str]:
    values = inventory_rule.get("source_ids")
    if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
        raise ReleaseAssemblyError("inventory source_ids are invalid")
    return list(values)


def _preferred_external_evidence(
    *,
    rule_id: str,
    inventory_rule: Mapping[str, Any],
    official_evidence: Mapping[str, EvidenceObservation],
) -> EvidenceObservation:
    allowed = _rule_source_ids(inventory_rule)
    preferred = EXTERNAL_EVIDENCE_PREFERENCES.get(rule_id)
    ordered = ([preferred] if isinstance(preferred, str) else []) + [
        source_id for source_id in allowed if source_id != preferred
    ]
    for source_id in ordered:
        evidence = official_evidence.get(source_id)
        if evidence is not None and source_id in allowed:
            return evidence
    raise ReleaseAssemblyError(
        f"{rule_id}: no collected official evidence among authorized sources {allowed}"
    )


def _reuse_previous_evidence_if_same_hash(
    previous_rule: Mapping[str, Any] | None,
    evidence: EvidenceObservation | GovernanceEvidenceObservation,
) -> dict[str, Any]:
    current = evidence.model_dump(mode="json", exclude_none=True)
    if not previous_rule:
        return current
    previous_provenance = previous_rule.get("provenance")
    if not isinstance(previous_provenance, list):
        return current
    for item in previous_provenance:
        if not isinstance(item, dict):
            continue
        if (
            item.get("source_id") == current.get("source_id")
            and item.get("snapshot_sha256") == current.get("snapshot_sha256")
        ):
            return deepcopy(item)
    return current


def _competence_from_coverage(coverage_rule: Mapping[str, Any]) -> dict[str, Any]:
    basis = coverage_rule.get("competence_basis")
    if not isinstance(basis, str):
        raise ReleaseAssemblyError("coverage competence_basis missing")
    result: dict[str, Any] = {"basis": basis}
    if basis == "rule_specific":
        key = coverage_rule.get("competence_key")
        if not isinstance(key, str):
            raise ReleaseAssemblyError("rule_specific coverage missing competence_key")
        result["rule_specific_key"] = key
    return result


def _auto_publish(rule_class: str) -> bool:
    return RuleClass(rule_class) not in STRUCTURAL_RULE_CLASSES


def _build_missing_rule(
    *,
    rule_id: str,
    coverage_rule: Mapping[str, Any],
    inventory_rule: Mapping[str, Any],
    official_evidence: Mapping[str, EvidenceObservation],
    governance_evidence: Mapping[str, GovernanceEvidenceObservation],
    candidates: Mapping[str, NormalizedSourceCandidate],
    generated_at_utc: datetime,
) -> dict[str, Any]:
    spec = MISSING_RULE_SPECS[rule_id]
    rule_class = str(inventory_rule["rule_class"])
    if rule_class == RuleClass.TECHNICAL_CONTRACT_RULE.value:
        evidence = governance_evidence.get(rule_id)
        if evidence is None:
            raise ReleaseAssemblyError(f"{rule_id}: governance evidence missing")
    else:
        evidence = _preferred_external_evidence(
            rule_id=rule_id,
            inventory_rule=inventory_rule,
            official_evidence=official_evidence,
        )

    result: dict[str, Any] = {
        "rule_id": rule_id,
        "rule_version": "1.0.0",
        "domain": coverage_rule["domain"],
        "calculators": coverage_rule["calculators"],
        "contexts": coverage_rule["contexts"],
        "description": spec["description"],
        "applies_to": inventory_rule["applies_to"],
        "applicability": [],
        "dependencies": list(spec["dependencies"]),
        "calculation_order": spec["calculation_order"],
        "competence": _competence_from_coverage(coverage_rule),
        "vigency": deepcopy(spec["vigency"]),
        "payload": _payload_for_missing_rule(
            rule_id,
            coverage_rule=coverage_rule,
            inventory_rule=inventory_rule,
            candidates=candidates,
        ),
        "provenance": [evidence.model_dump(mode="json", exclude_none=True)],
        "quality": {
            "status": "VALIDATED",
            "reference_case_ids": [],
            "last_validated_at_utc": generated_at_utc.isoformat().replace("+00:00", "Z"),
            "reviewed_by_human": True,
        },
        "change_class": "RULE_ADDED",
        "update_policy": {
            "rule_class": rule_class,
            "auto_publish": _auto_publish(rule_class),
            "structural_review_required": rule_class
            in {item.value for item in STRUCTURAL_RULE_CLASSES},
        },
    }
    if "rounding_policy" in spec:
        result["rounding_policy"] = deepcopy(spec["rounding_policy"])
    return result


def _hydrate_parameter_rules(
    rules: dict[str, dict[str, Any]],
    candidates: Mapping[str, NormalizedSourceCandidate],
) -> None:
    rfb = _parsed_payload(candidates, RFB_SOURCE_ID)
    inss = _parsed_payload(candidates, INSS_SOURCE_ID)

    monthly = rules["irrf.monthly.progressive_table"]
    rows = rfb.get("monthly_table")
    if not isinstance(rows, list) or not rows:
        raise ReleaseAssemblyError("RFB candidate monthly_table missing")
    monthly["payload"] = {
        "type": "progressive_table",
        "calculation_method": "rate_times_base_minus_deduction",
        "brackets": [
            {
                "upper_bound": row.get("upper_bound_brl"),
                "rate": row.get("rate"),
                "deduction": row.get("deduction_brl"),
            }
            for row in rows
            if isinstance(row, Mapping)
        ],
    }

    dependent = rules["irrf.dependent_deduction"]
    dependent["payload"] = {
        "type": "scalar",
        "value": rfb.get("dependent_deduction_brl"),
        "unit": "BRL_per_dependent",
    }

    simplified = rules["irrf.simplified_monthly_discount"]
    simplified["payload"] = {
        "type": "scalar",
        "value": rfb.get("simplified_discount_brl"),
        "unit": "BRL",
    }

    reduction = rules["irrf.reduction.2026"]
    reduction["payload"] = _irrf_reduction_payload(
        rfb,
        input_semantic=str(reduction["applies_to"]),
    )

    employee = rules["inss.employee.progressive_table"]
    employee_rows = inss.get("monthly_table")
    if not isinstance(employee_rows, list) or not employee_rows:
        raise ReleaseAssemblyError("INSS candidate monthly_table missing")
    employee["payload"] = {
        "type": "progressive_table",
        "calculation_method": "marginal_by_bracket",
        "brackets": [
            {
                "upper_bound": row.get("upper_bound_brl"),
                "rate": row.get("rate"),
                "deduction": "0",
            }
            for row in employee_rows
            if isinstance(row, Mapping)
        ],
        "cap_base": inss.get("contribution_ceiling_brl"),
    }


def _apply_structural_successor_overlays(rules: dict[str, dict[str, Any]]) -> None:
    """Apply canonical structural decisions approved after the first v1.2 release.

    The legal 13th-salary accrual rule stays sourced only from external authority;
    monetary quantization is supplied by the technical governance rule instead.

    C6.5 also closes the vacation cash-allowance representation gap without adding
    a 33rd canonical rule. The existing 1/3 allowance fraction is explicitly scoped
    to the corresponding remuneration components, while the existing vacation
    remuneration/constitutional-third formula becomes selectable in the cash-
    allowance context. Tax incidence remains controlled by the dedicated allowance
    principal and constitutional-third incidence rules.

    Re-applying these overlays is idempotent; semantic diff decides whether human
    review is required against the current PUBLISHED release.
    """
    technical = rules.get("technical.money_decimal_and_rounding")
    if technical is None:
        raise ReleaseAssemblyError("technical.money_decimal_and_rounding rule missing")
    technical["rounding_policy"] = deepcopy(TECHNICAL_MONEY_ROUNDING_POLICY)

    allowance = rules.get("vacation.abono_pecuniario")
    if allowance is None:
        raise ReleaseAssemblyError("vacation.abono_pecuniario rule missing")
    allowance["description"] = (
        "Conversão de 1/3 do período de férias a que o empregado tem direito e da "
        "parcela correspondente da remuneração de férias, preservando principal e "
        "terço constitucional como componentes separados."
    )
    allowance["applies_to"] = (
        "vacation_entitled_days_and_corresponding_remuneration_components"
    )

    remuneration = rules.get("vacation.remuneration_and_constitutional_third")
    if remuneration is None:
        raise ReleaseAssemblyError(
            "vacation.remuneration_and_constitutional_third rule missing"
        )
    contexts = remuneration.get("contexts")
    if not isinstance(contexts, list):
        raise ReleaseAssemblyError(
            "vacation.remuneration_and_constitutional_third contexts missing"
        )
    if "vacation_cash_allowance" not in contexts:
        contexts.append("vacation_cash_allowance")
    remuneration["description"] = (
        "Componentes da remuneração de férias e do adicional constitucional; no "
        "contexto vacation_cash_allowance, a base representa o principal do abono "
        "correspondente aos dias convertidos, mantendo o terço como componente "
        "separado."
    )
    remuneration["applies_to"] = (
        "vacation_remuneration_components_and_constitutional_third"
    )


def _bump_patch(version: str) -> str:
    major, minor, patch = (int(part) for part in version.split("."))
    return f"{major}.{minor}.{patch + 1}"


def _bump_minor(version: str) -> str:
    major, minor, _patch = (int(part) for part in version.split("."))
    return f"{major}.{minor + 1}.0"


def _reconcile_declared_transition(
    previous: FiscalContractV1,
    candidate: FiscalContractV12,
) -> FiscalContractV12:
    first_diff = diff_contracts(previous, candidate)
    data = candidate.model_dump(mode="json", exclude_none=True)
    rules = _index_rules(data)
    validated_at = candidate.generated_at_utc.isoformat().replace("+00:00", "Z")
    for item in first_diff.rule_diffs:
        if item.change_class == ChangeClass.RULE_REMOVED:
            raise ReleaseAssemblyError("v1.2 canonical assembler cannot remove inventory rules")
        rule = rules[item.rule_id]
        if not item.changed:
            continue
        rule["change_class"] = item.change_class.value
        previous_version = item.previous_rule_version
        if previous_version is None:
            rule["rule_version"] = "1.0.0"
        elif item.change_class == ChangeClass.STRUCTURAL_CHANGE:
            rule["rule_version"] = _bump_minor(previous_version)
        else:
            rule["rule_version"] = _bump_patch(previous_version)
        quality = rule.setdefault("quality", {})
        quality["last_validated_at_utc"] = validated_at
    return FiscalContractV12.model_validate(data)


def assemble_candidate_v12(
    *,
    template_v11: Mapping[str, Any],
    coverage: Mapping[str, Any],
    inventory: Mapping[str, Any],
    official_evidence: Mapping[str, EvidenceObservation],
    governance_evidence: Mapping[str, GovernanceEvidenceObservation],
    normalized_candidates: Mapping[str, NormalizedSourceCandidate],
    generated_at_utc: datetime,
    governance_source_registry_version: str,
    previous: FiscalContractV1 | None = None,
) -> FiscalContractV12:
    """Build the canonical 32-rule v1.2 CANDIDATE.

    Bootstrap starts from the audited 21-rule v1.1 representative contract and
    materializes the exact eleven-rule gap. Successors start from the current
    PUBLISHED v1.2 release, which prevents the historical template from overwriting
    later approved structural decisions.
    """
    _require_utc(generated_at_utc)
    coverage_rules = _index_rules(coverage)
    inventory_rules = _index_rules(inventory)
    required = set(coverage_rules)
    if len(required) != 32 or required != set(inventory_rules):
        raise ReleaseAssemblyError("coverage/inventory must describe the same closed 32-rule set")

    if previous is None:
        data = deepcopy(dict(template_v11))
        if data.get("schema_version") != "1.1.0":
            raise ReleaseAssemblyError("bootstrap template must be historical schema 1.1.0")
        raw_rules = _index_rules(data)
        missing = required - set(raw_rules)
        if missing != set(MISSING_RULE_SPECS):
            raise ReleaseAssemblyError(
                f"historical template gap drift; expected={sorted(MISSING_RULE_SPECS)} got={sorted(missing)}"
            )
        for rule_id in sorted(missing):
            rule = _build_missing_rule(
                rule_id=rule_id,
                coverage_rule=coverage_rules[rule_id],
                inventory_rule=inventory_rules[rule_id],
                official_evidence=official_evidence,
                governance_evidence=governance_evidence,
                candidates=normalized_candidates,
                generated_at_utc=generated_at_utc,
            )
            data["rules"].append(rule)
    else:
        if previous.schema_version != "1.2.0":
            raise ReleaseAssemblyError("successor automation requires current PUBLISHED schema 1.2.0")
        data = previous.model_dump(mode="json", exclude_none=True)

    data["schema_version"] = "1.2.0"
    data["release_id"] = "candidate-v1-2"
    data["status"] = "CANDIDATE"
    data["generated_at_utc"] = generated_at_utc.isoformat().replace("+00:00", "Z")
    data["governance_source_registry_version"] = governance_source_registry_version
    data["consumer_compatibility"]["schema_version"] = "1.2.0"
    data["consumer_compatibility"]["contract_api_version"] = "1.2.0"
    data["supersedes_release_id"] = previous.release_id if previous is not None else None
    data["lifecycle"] = {}
    data["notes"] = "Canonical 32-rule Fiscal Contract v1.2 assembled by Phase 5 publication pipeline."

    rules = _index_rules(data)
    if set(rules) != required:
        raise ReleaseAssemblyError(
            f"assembled rule set is not 32/32; missing={sorted(required-set(rules))} extra={sorted(set(rules)-required)}"
        )

    _apply_structural_successor_overlays(rules)

    # Attach one durable authorized authority snapshot to every legal/fiscal rule,
    # and one internal-governance snapshot only to technical contract rules.
    previous_rules = _index_rules(previous.model_dump(mode="json", exclude_none=True)) if previous else {}
    for rule_id, rule in rules.items():
        inventory_rule = inventory_rules[rule_id]
        rule_class = RuleClass(str(inventory_rule["rule_class"]))
        prior = previous_rules.get(rule_id)
        if rule_class == RuleClass.TECHNICAL_CONTRACT_RULE:
            evidence = governance_evidence.get(rule_id)
            if evidence is None:
                raise ReleaseAssemblyError(f"{rule_id}: governance evidence missing")
            rule["provenance"] = [_reuse_previous_evidence_if_same_hash(prior, evidence)]
        else:
            evidence = _preferred_external_evidence(
                rule_id=rule_id,
                inventory_rule=inventory_rule,
                official_evidence=official_evidence,
            )
            rule["provenance"] = [_reuse_previous_evidence_if_same_hash(prior, evidence)]
        quality = rule.setdefault("quality", {})
        quality["status"] = "VALIDATED"
        if previous is None:
            quality["last_validated_at_utc"] = generated_at_utc.isoformat().replace("+00:00", "Z")
        if rule_class in STRUCTURAL_RULE_CLASSES:
            quality["reviewed_by_human"] = True
        if previous is None:
            rule["change_class"] = "RULE_ADDED"

    _hydrate_parameter_rules(rules, normalized_candidates)
    data["rules"] = [rules[rule_id] for rule_id in sorted(rules)]

    candidate = FiscalContractV12.model_validate(data)
    if previous is not None:
        candidate = _reconcile_declared_transition(previous, candidate)
    return candidate