from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any, Mapping

from .contract_v1 import FiscalContractV1
from .semantic_diff_v1 import PromotionAssessmentV1


REVIEW_PACKET_SCHEMA_VERSION = "1.0.0"


class HumanReviewPacketError(ValueError):
    pass


class StaleReviewApprovalError(HumanReviewPacketError):
    pass


def _require_utc(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise HumanReviewPacketError("review packet timestamp must be UTC")


def _dump(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    return value


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _vigency_key(rule: Mapping[str, Any]) -> tuple[str, str]:
    vigency = rule.get("vigency") if isinstance(rule, Mapping) else None
    if not isinstance(vigency, Mapping):
        return ("", "")
    start = str(vigency.get("effective_from") or "")
    end = str(vigency.get("effective_until") or "9999-12-31")
    return start, end


def _group_rule_dicts(contract_or_mapping: Any) -> dict[str, list[dict[str, Any]]]:
    raw = _dump(contract_or_mapping)
    if not isinstance(raw, Mapping):
        return {}
    rules = raw.get("rules")
    if not isinstance(rules, list):
        return {}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in rules:
        if not isinstance(item, Mapping):
            continue
        rule_id = item.get("rule_id")
        if not isinstance(rule_id, str):
            continue
        grouped.setdefault(rule_id, []).append(dict(item))
    for values in grouped.values():
        values.sort(key=_vigency_key)
    return grouped


def _stable_evidence(item: Mapping[str, Any]) -> dict[str, Any]:
    keep = (
        "source_id",
        "role",
        "status",
        "retrieval_method",
        "snapshot_sha256",
        "parser_id",
        "parser_version",
        "locator",
        "series_code",
    )
    return {key: item[key] for key in keep if key in item}


def _review_identity_rule(rule: Mapping[str, Any]) -> dict[str, Any]:
    data = dict(rule)
    # Transition metadata is deliberately excluded from the human-decision identity.
    # It can change solely because a fresh evidence snapshot was observed, while the
    # rule semantics and normalized review evidence remain identical. Material
    # semantic/version changes are still represented by the non-refresh diff entries
    # below, and evidence materiality is represented by normalized provenance hashes.
    data.pop("change_class", None)
    data.pop("rule_version", None)

    quality = data.get("quality")
    if isinstance(quality, Mapping):
        quality = dict(quality)
        quality.pop("last_validated_at_utc", None)
        data["quality"] = quality
    provenance = data.get("provenance")
    if isinstance(provenance, list):
        data["provenance"] = [
            _stable_evidence(item)
            for item in provenance
            if isinstance(item, Mapping)
        ]
    return data


def _identity_rule_diff(item: Any) -> dict[str, Any] | None:
    if not item.changed:
        return None
    change_class = item.change_class.value
    if change_class == "SOURCE_REFRESH_NO_CHANGE":
        return None
    return {
        "rule_id": item.rule_id,
        "occurrence": item.occurrence,
        "change_class": change_class,
        "changed_paths": list(item.changed_paths),
        "previous_rule_version": item.previous_rule_version,
        "candidate_rule_version": item.candidate_rule_version,
    }


def _candidate_review_identity(candidate: FiscalContractV1, assessment: PromotionAssessmentV1) -> dict[str, Any]:
    data = candidate.model_dump(mode="json", exclude_none=True)
    rules = data.get("rules")
    stable_rules = []
    if isinstance(rules, list):
        stable_rules = [
            _review_identity_rule(rule)
            for rule in rules
            if isinstance(rule, Mapping)
        ]
        stable_rules.sort(key=lambda rule: (str(rule.get("rule_id")), *_vigency_key(rule)))

    stable_rule_diffs = []
    for item in assessment.diff.rule_diffs:
        identity_diff = _identity_rule_diff(item)
        if identity_diff is not None:
            stable_rule_diffs.append(identity_diff)

    return {
        "schema_version": data.get("schema_version"),
        "source_registry_version": data.get("source_registry_version"),
        "governance_source_registry_version": data.get("governance_source_registry_version"),
        "consumer_compatibility": data.get("consumer_compatibility"),
        "supersedes_release_id": data.get("supersedes_release_id"),
        "rules": stable_rules,
        "contract_changed_paths": list(assessment.diff.contract_changed_paths),
        "rule_diffs": stable_rule_diffs,
    }


def compute_review_key(
    previous: FiscalContractV1 | None,
    candidate: FiscalContractV1,
    assessment: PromotionAssessmentV1,
) -> str:
    body = {
        "previous_release_id": previous.release_id if previous is not None else None,
        "candidate": _candidate_review_identity(candidate, assessment),
    }
    return sha256(_canonical_bytes(body)).hexdigest()


def assert_review_approval_matches(current_review_key: str, expected_review_key: str | None) -> None:
    if not isinstance(expected_review_key, str) or not expected_review_key.strip():
        raise StaleReviewApprovalError("human approval requires the review_key that was actually reviewed")
    expected = expected_review_key.strip().lower()
    if expected != current_review_key:
        raise StaleReviewApprovalError(
            f"review approval is stale: reviewed={expected} current={current_review_key}"
        )


def _decision_view(rule: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if rule is None:
        return None
    keys = (
        "domain",
        "calculators",
        "contexts",
        "description",
        "applies_to",
        "applicability",
        "dependencies",
        "calculation_order",
        "competence",
        "vigency",
        "rounding_policy",
        "payload",
        "update_policy",
    )
    return {key: rule[key] for key in keys if key in rule}


def _diff_values(left: Any, right: Any, prefix: str = "", limit: int = 80) -> list[dict[str, Any]]:
    if left == right:
        return []
    if limit <= 0:
        return [{"path": prefix or "$root", "before": "<diff truncated>", "after": "<diff truncated>"}]
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        output: list[dict[str, Any]] = []
        for key in sorted(set(left) | set(right)):
            child = f"{prefix}.{key}" if prefix else str(key)
            if key not in left:
                output.append({"path": child, "before": None, "after": right[key]})
            elif key not in right:
                output.append({"path": child, "before": left[key], "after": None})
            else:
                output.extend(_diff_values(left[key], right[key], child, limit - len(output)))
            if len(output) >= limit:
                break
        return output[:limit]
    if isinstance(left, list) and isinstance(right, list):
        output: list[dict[str, Any]] = []
        for index in range(max(len(left), len(right))):
            child = f"{prefix}[{index}]"
            if index >= len(left):
                output.append({"path": child, "before": None, "after": right[index]})
            elif index >= len(right):
                output.append({"path": child, "before": left[index], "after": None})
            else:
                output.extend(_diff_values(left[index], right[index], child, limit - len(output)))
            if len(output) >= limit:
                break
        return output[:limit]
    return [{"path": prefix or "$root", "before": left, "after": right}]


def _evidence_summary(rule: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if rule is None:
        return []
    provenance = rule.get("provenance")
    if not isinstance(provenance, list):
        return []
    output: list[dict[str, Any]] = []
    for item in provenance:
        if not isinstance(item, Mapping):
            continue
        summary = _stable_evidence(item)
        if "observed_at_utc" in item:
            summary["observed_at_utc"] = item["observed_at_utc"]
        if "snapshot_path" in item:
            summary["snapshot_path"] = item["snapshot_path"]
        output.append(summary)
    return output


def build_review_packet(
    *,
    previous: FiscalContractV1 | None,
    candidate: FiscalContractV1,
    assessment: PromotionAssessmentV1,
    created_at_utc: datetime,
    historical_template: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _require_utc(created_at_utc)
    review_key = compute_review_key(previous, candidate, assessment)
    candidate_groups = _group_rule_dicts(candidate)
    previous_groups = _group_rule_dicts(previous) if previous is not None else {}
    historical_groups = _group_rule_dicts(historical_template or {})

    rules: list[dict[str, Any]] = []
    affected_calculators: set[str] = set()
    inherited_historical = 0
    newly_materialized = 0

    for item in assessment.diff.rule_diffs:
        if not item.changed:
            continue
        after_rules = candidate_groups.get(item.rule_id, [])
        after = after_rules[item.occurrence] if item.occurrence < len(after_rules) else None
        if previous is not None:
            before_rules = previous_groups.get(item.rule_id, [])
            before = before_rules[item.occurrence] if item.occurrence < len(before_rules) else None
            baseline = "current_published_release"
        else:
            before_rules = historical_groups.get(item.rule_id, [])
            before = before_rules[item.occurrence] if item.occurrence < len(before_rules) else None
            if before is None:
                baseline = "not_materialized_in_historical_v1.1_candidate"
                newly_materialized += 1
            else:
                baseline = "historical_v1.1_candidate"
                inherited_historical += 1

        before_view = _decision_view(before)
        after_view = _decision_view(after)
        semantic_changes = _diff_values(before_view, after_view)
        calculators = list(after.get("calculators", [])) if isinstance(after, Mapping) else []
        affected_calculators.update(str(value) for value in calculators)
        before_evidence = _evidence_summary(before)
        after_evidence = _evidence_summary(after)
        evidence_changed = [
            {"before": left, "after": right}
            for left, right in zip(before_evidence, after_evidence)
            if left.get("snapshot_sha256") != right.get("snapshot_sha256")
            or left.get("source_id") != right.get("source_id")
        ]
        if len(before_evidence) != len(after_evidence):
            evidence_changed.append({"before": before_evidence, "after": after_evidence})

        quality = after.get("quality") if isinstance(after, Mapping) else None
        reference_case_ids = []
        if isinstance(quality, Mapping) and isinstance(quality.get("reference_case_ids"), list):
            reference_case_ids = list(quality["reference_case_ids"])

        if previous is None:
            if before is None:
                historical_semantic_status = "NEWLY_MATERIALIZED"
            elif semantic_changes:
                historical_semantic_status = "CHANGED_FROM_HISTORICAL"
            else:
                historical_semantic_status = "SEMANTICALLY_INHERITED"
        else:
            historical_semantic_status = "NOT_APPLICABLE"

        rules.append(
            {
                "rule_id": item.rule_id,
                "occurrence": item.occurrence,
                "change_class": item.change_class.value,
                "baseline": baseline,
                "historical_semantic_status": historical_semantic_status,
                "previous_rule_version": item.previous_rule_version,
                "candidate_rule_version": item.candidate_rule_version,
                "changed_paths": list(item.changed_paths),
                "calculators": calculators,
                "reference_case_ids": reference_case_ids,
                "semantic_changes": semantic_changes,
                "before": before_view,
                "after": after_view,
                "evidence_before": before_evidence,
                "evidence_after": after_evidence,
                "evidence_changed": evidence_changed,
            }
        )

    compat = candidate.consumer_compatibility.model_dump(mode="json", exclude_none=True)
    return {
        "schema_version": REVIEW_PACKET_SCHEMA_VERSION,
        "review_key": review_key,
        "created_at_utc": created_at_utc.isoformat().replace("+00:00", "Z"),
        "review_kind": "BOOTSTRAP" if previous is None else "SUCCESSOR",
        "previous_release_id": previous.release_id if previous is not None else None,
        "candidate_schema_version": candidate.schema_version,
        "candidate_contract_api_version": compat.get("contract_api_version"),
        "candidate_rule_count": len(candidate.rules),
        "promotion_outcome": assessment.outcome.value,
        "reasons": list(assessment.reasons),
        "contract_changed_paths": list(assessment.diff.contract_changed_paths),
        "affected_calculators": sorted(affected_calculators),
        "bootstrap_baseline": {
            "historical_rules_present": inherited_historical,
            "newly_materialized_rules": newly_materialized,
        }
        if previous is None
        else None,
        "rules": rules,
        "approval": {
            "required_command": f"/approve {review_key}",
            "stale_review_must_block_publication": True,
        },
    }


def _escape_table(value: Any) -> str:
    text = "" if value is None else str(value)
    return text.replace("|", "\\|").replace("\n", " ")


def _short_sha(value: Any) -> str:
    text = str(value or "")
    return text[:12] + ("…" if len(text) > 12 else "")


def _json_block(value: Any, *, max_chars: int = 2600) -> str:
    text = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
    if len(text) > max_chars:
        text = text[:max_chars] + "\n… <truncated in Issue; full packet is committed in state/>"
    return text


def render_review_markdown(
    packet: Mapping[str, Any],
    *,
    regression_status: str | None = None,
    regression_summary: str | None = None,
) -> str:
    review_key = str(packet["review_key"])
    kind = str(packet.get("review_kind"))
    previous = packet.get("previous_release_id") or "nenhuma — bootstrap"
    lines = [
        f"<!-- sanida-fiscal-review-key:{review_key} -->",
        "<!-- sanida-fiscal-review-current -->",
        f"# Revisão fiscal humana — {kind}",
        "",
        "> **Publicação bloqueada até decisão humana explícita.** Esta Issue é o pacote de decisão canônico para este candidato.",
        "",
        f"- **Review key:** `{review_key}`",
        f"- **Release atual:** `{previous}`",
        f"- **Contrato candidato:** schema `{packet.get('candidate_schema_version')}` / API `{packet.get('candidate_contract_api_version')}`",
        f"- **Regras:** `{packet.get('candidate_rule_count')}`",
        f"- **Calculadoras afetadas:** `{', '.join(packet.get('affected_calculators') or []) or 'nenhuma identificada'}`",
    ]
    if kind == "BOOTSTRAP":
        baseline = packet.get("bootstrap_baseline") or {}
        lines.extend(
            [
                f"- **Baseline do bootstrap:** {baseline.get('historical_rules_present', 0)} regras já materializadas no candidato histórico v1.1; {baseline.get('newly_materialized_rules', 0)} regras materializadas agora para completar 32/32.",
                "- **Importante:** o bootstrap não trata artificialmente as 32 regras como se fossem 32 decisões jurídicas novas.",
            ]
        )
    lines.append("")

    if regression_status:
        lines.extend(
            [
                "## Regressões",
                "",
                f"**Status:** `{regression_status}`",
                "",
                regression_summary or "Sem resumo adicional.",
                "",
            ]
        )

    reasons = packet.get("reasons") or []
    lines.extend(["## Por que a revisão é necessária", ""])
    lines.extend(f"- {reason}" for reason in reasons)
    if not reasons:
        lines.append("- O classificador não registrou motivo textual adicional.")
    lines.append("")

    contract_paths = packet.get("contract_changed_paths") or []
    if contract_paths:
        lines.extend(["## Mudanças no contrato", "", ", ".join(f"`{path}`" for path in contract_paths), ""])

    lines.extend(
        [
            "## Resumo por regra",
            "",
            "| Regra | Classe | Baseline | Estado semântico | Versão | Consumidores | Fonte candidata |",
            "|---|---|---|---|---|---|---|",
        ]
    )
    for rule in packet.get("rules") or []:
        after_evidence = rule.get("evidence_after") or []
        sources = ", ".join(
            f"{item.get('source_id')}@{_short_sha(item.get('snapshot_sha256'))}"
            for item in after_evidence
        )
        version = f"{rule.get('previous_rule_version') or '—'} → {rule.get('candidate_rule_version') or '—'}"
        lines.append(
            "| {rule} | {change} | {baseline} | {semantic} | {version} | {calc} | {source} |".format(
                rule=_escape_table(rule.get("rule_id")),
                change=_escape_table(rule.get("change_class")),
                baseline=_escape_table(rule.get("baseline")),
                semantic=_escape_table(rule.get("historical_semantic_status")),
                version=_escape_table(version),
                calc=_escape_table(", ".join(rule.get("calculators") or [])),
                source=_escape_table(sources),
            )
        )
    lines.append("")

    lines.extend(["## Detalhes que exigem leitura", ""])
    detailed = []
    for rule in packet.get("rules") or []:
        if kind != "BOOTSTRAP" or rule.get("historical_semantic_status") != "SEMANTICALLY_INHERITED":
            detailed.append(rule)
    if not detailed:
        lines.append("Nenhuma regra herdada apresenta delta semântico contra o baseline histórico.")
        lines.append("")
    for rule in detailed:
        lines.extend(
            [
                f"<details><summary><code>{rule.get('rule_id')}</code> — {rule.get('change_class')} / {rule.get('historical_semantic_status')}</summary>",
                "",
                f"**Changed paths:** {', '.join(f'`{path}`' for path in rule.get('changed_paths') or []) or '—'}",
                "",
            ]
        )
        semantic_changes = rule.get("semantic_changes") or []
        if semantic_changes:
            lines.append("**Antes → depois (semântica/payload):**")
            lines.append("")
            for change in semantic_changes[:30]:
                lines.append(f"- `{change.get('path')}`: `{change.get('before')}` → `{change.get('after')}`")
            lines.append("")
        else:
            lines.extend(["**Sem delta semântico detectado contra o baseline selecionado.**", ""])
        lines.extend(
            [
                "**Evidência candidata:**",
                "",
                "```json",
                _json_block(rule.get("evidence_after") or []),
                "```",
                "",
            ]
        )
        if rule.get("reference_case_ids"):
            lines.append("**Casos de referência:** " + ", ".join(f"`{item}`" for item in rule["reference_case_ids"]))
            lines.append("")
        lines.extend(["</details>", ""])

    lines.extend(
        [
            "## Decisão",
            "",
            "Revise as mudanças, as fontes e o resultado das regressões. Para aprovar **exatamente este candidato**, comente nesta Issue:",
            "",
            "```text",
            f"/approve {review_key}",
            "```",
            "",
            "A automação só aceitará essa aprovação se o `review_key` recalculado no momento da publicação for idêntico. Se qualquer fonte, regra ou evidência relevante mudar, a aprovação fica stale e uma nova revisão será exigida.",
            "",
            "A Issue é atribuída ao proprietário do repositório para gerar notificação no GitHub; entrega por e-mail depende das configurações de notificações da conta GitHub.",
            "",
            f"Pacote machine-readable: `state/fiscal-release-v12-review.json` (`{review_key}`).",
        ]
    )
    body = "\n".join(lines).strip() + "\n"
    if len(body) > 64000:
        body = body[:62000] + "\n\n> Issue truncada por limite de tamanho. Consulte o pacote machine-readable versionado no repositório.\n"
        body += f"\n<!-- sanida-fiscal-review-key:{review_key} -->\n"
    return body
