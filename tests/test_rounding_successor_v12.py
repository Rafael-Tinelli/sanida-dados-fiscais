from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from sanida_fiscal.contract_v1_2 import GovernanceEvidenceObservation
from sanida_fiscal.publication_v1 import FiscalReleaseStore
from sanida_fiscal.release_assembler_v12 import assemble_candidate_v12
from sanida_fiscal.semantic_diff_v1 import PromotionOutcome, assess_promotion
from sanida_fiscal.sources_v1 import NormalizedSourceCandidate, ParseStatus
from sanida_fiscal.types_v1 import EvidenceObservation

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "releases/fiscal-v1"
TEMPLATE = ROOT / "contracts/examples/fiscal-contract-v1.example.json"
COVERAGE = ROOT / "docs/contract-coverage-v1.json"
INVENTORY = ROOT / "docs/rule-inventory-v1.json"
SOURCE_REGISTRY = ROOT / "docs/source-registry-v1.json"
GOVERNANCE_REGISTRY = ROOT / "docs/governance-source-registry-v1.json"
NOW = datetime(2026, 9, 15, 12, 0, tzinfo=timezone.utc)


def _load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _digest(seed: str) -> str:
    raw = seed.encode("utf-8").hex()
    return (raw * ((64 // len(raw)) + 1))[:64]


def _official_evidence(previous) -> dict[str, EvidenceObservation]:
    """Reuse exact official provenance from the current PUBLISHED release.

    This prevents the regression test itself from manufacturing unrelated
    authority-snapshot changes and therefore proves that only the intended
    technical-governance rule changes in this successor.
    """
    output: dict[str, EvidenceObservation] = {}
    for rule in previous.rules:
        for observation in rule.provenance:
            raw = observation.model_dump(mode="json", exclude_none=True)
            if raw.get("role") == "internal_governance":
                continue
            source_id = raw.get("source_id")
            if isinstance(source_id, str) and source_id not in output:
                output[source_id] = EvidenceObservation.model_validate(raw)

    for rule in previous.rules:
        if rule.provenance[0].role == "internal_governance":
            continue
        source_id = rule.provenance[0].source_id
        assert source_id in output, f"missing exact published evidence for {rule.rule_id}"
    return output


def _normalized_candidates(previous) -> dict[str, NormalizedSourceCandidate]:
    rules = {rule.rule_id: rule for rule in previous.rules}
    rfb_table = rules["irrf.monthly.progressive_table"].payload
    dependent = rules["irrf.dependent_deduction"].payload
    simplified = rules["irrf.simplified_monthly_discount"].payload
    reduction = rules["irrf.reduction.2026"].payload
    inss = rules["inss.employee.progressive_table"].payload

    rfb_payload = {
        "monthly_table": [
            {
                "upper_bound_brl": None if row.upper_bound is None else str(row.upper_bound),
                "rate": str(row.rate),
                "deduction_brl": str(row.deduction),
            }
            for row in rfb_table.brackets
        ],
        "dependent_deduction_brl": str(dependent.value),
        "simplified_discount_brl": str(simplified.value),
        "monthly_reduction": {
            "full_relief_income_upper_brl": str(reduction.full_relief_income_limit),
            "phaseout_income_upper_brl": str(reduction.phaseout_income_limit),
            "max_reduction_brl": str(reduction.max_reduction),
            "intercept_brl": str(reduction.intercept),
            "slope": str(reduction.slope),
        },
    }
    inss_payload = {
        "monthly_table": [
            {
                "upper_bound_brl": None if row.upper_bound is None else str(row.upper_bound),
                "rate": str(row.rate),
            }
            for row in inss.brackets
        ],
        "contribution_ceiling_brl": str(inss.cap_base),
    }

    registry = {item["source_id"]: item for item in _load(SOURCE_REGISTRY)["sources"]}
    rfb_provenance = rules["irrf.monthly.progressive_table"].provenance[0]
    inss_provenance = rules["inss.employee.progressive_table"].provenance[0]
    return {
        "RFB_IRRF_TABLE_2026": NormalizedSourceCandidate(
            source_id="RFB_IRRF_TABLE_2026",
            source_url=registry["RFB_IRRF_TABLE_2026"]["url"],
            observed_at_utc=NOW,
            snapshot_sha256=rfb_provenance.snapshot_sha256,
            snapshot_path=rfb_provenance.snapshot_path,
            parser_id=rfb_provenance.parser_id,
            parser_version=rfb_provenance.parser_version,
            status=ParseStatus.PARSED,
            payload=rfb_payload,
        ),
        "INSS_TABLE_2026": NormalizedSourceCandidate(
            source_id="INSS_TABLE_2026",
            source_url=registry["INSS_TABLE_2026"]["url"],
            observed_at_utc=NOW,
            snapshot_sha256=inss_provenance.snapshot_sha256,
            snapshot_path=inss_provenance.snapshot_path,
            parser_id=inss_provenance.parser_id,
            parser_version=inss_provenance.parser_version,
            status=ParseStatus.PARSED,
            payload=inss_payload,
        ),
    }


def _governance_evidence(previous) -> dict[str, GovernanceEvidenceObservation]:
    registry = _load(GOVERNANCE_REGISTRY)
    by_source = {item["source_id"]: item for item in registry["sources"]}
    money = by_source["SANIDA_TECHNICAL_MONEY_DECIMAL_V1"]

    previous_by_rule = {rule.rule_id: rule for rule in previous.rules}
    contract_previous = previous_by_rule["technical.contract_vigency_and_quality"].provenance[0]
    contract_exact = GovernanceEvidenceObservation.model_validate(
        contract_previous.model_dump(mode="json", exclude_none=True)
    )

    return {
        "technical.money_decimal_and_rounding": GovernanceEvidenceObservation(
            source_id=money["source_id"],
            observed_at_utc=NOW,
            snapshot_sha256=_digest("money-v1.1"),
            snapshot_path="test-governance/money-v1.1.json",
            repository_paths=money["repository_paths"],
        ),
        "technical.contract_vigency_and_quality": contract_exact,
    }


def test_rounding_successor_is_structural_and_review_required() -> None:
    previous = FiscalReleaseStore(STORE).load_current()
    assert previous is not None
    previous.assert_consumable()

    registry = _load(GOVERNANCE_REGISTRY)
    candidate = assemble_candidate_v12(
        template_v11=_load(TEMPLATE),
        coverage=_load(COVERAGE),
        inventory=_load(INVENTORY),
        official_evidence=_official_evidence(previous),
        governance_evidence=_governance_evidence(previous),
        normalized_candidates=_normalized_candidates(previous),
        generated_at_utc=NOW,
        governance_source_registry_version=registry["schema_version"],
        previous=previous,
    )

    before = {rule.rule_id: rule for rule in previous.rules}
    after = {rule.rule_id: rule for rule in candidate.rules}

    technical = after["technical.money_decimal_and_rounding"]
    assert technical.rounding_policy is not None
    assert technical.rounding_policy.decimal_places == 2
    assert technical.rounding_policy.mode == "ROUND_HALF_UP"
    assert technical.rounding_policy.stage.value == "per_component"
    assert technical.rule_version == "1.1.0"
    assert technical.change_class.value == "STRUCTURAL_CHANGE"
    assert technical.provenance[0].role == "internal_governance"
    assert "docs/technical-money-rounding-policy-v1.md" in technical.provenance[0].repository_paths

    accrual_before = before["thirteenth.accrual.twelfths"]
    accrual_after = after["thirteenth.accrual.twelfths"]
    assert accrual_before.rounding_policy is None
    assert accrual_after.rounding_policy is None
    assert accrual_after.rule_version == accrual_before.rule_version
    assert accrual_after.provenance[0].role != "internal_governance"

    assessment = assess_promotion(previous, candidate)
    assert assessment.outcome == PromotionOutcome.REVIEW_REQUIRED
    changed = [item for item in assessment.diff.rule_diffs if item.changed]
    assert [(item.rule_id, item.change_class.value) for item in changed] == [
        ("technical.money_decimal_and_rounding", "STRUCTURAL_CHANGE")
    ]


def test_governance_registry_versions_the_rounding_decision() -> None:
    registry = _load(GOVERNANCE_REGISTRY)
    assert registry["schema_version"] == "1.1.0"
    money = next(
        item for item in registry["sources"]
        if item["source_id"] == "SANIDA_TECHNICAL_MONEY_DECIMAL_V1"
    )
    assert "docs/technical-money-rounding-policy-v1.md" in money["repository_paths"]

    policy = (ROOT / "docs/technical-money-rounding-policy-v1.md").read_text(encoding="utf-8")
    for required in (
        "decimal_places = 2",
        "mode           = ROUND_HALF_UP",
        "stage          = per_component",
        "falhar fechado",
        "não pode ser apresentada como se derivasse de fonte legal",
    ):
        assert required in policy
