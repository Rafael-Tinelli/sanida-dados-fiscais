from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path

from sanida_fiscal.contract_v1_2 import GovernanceEvidenceObservation
from sanida_fiscal.publication_v1 import FiscalReleaseStore
from sanida_fiscal.release_assembler_v12 import assemble_candidate_v12
from sanida_fiscal.semantic_diff_v1 import PromotionOutcome, assess_promotion
from sanida_fiscal.sources_v1 import NormalizedSourceCandidate, ParseStatus
from sanida_fiscal.types_v1 import (
    EvidenceObservation,
    RetrievalMethod,
    SourceObservationStatus,
    SourceRole,
)

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


def _official_evidence() -> dict[str, EvidenceObservation]:
    out: dict[str, EvidenceObservation] = {}
    for source in _load(SOURCE_REGISTRY)["sources"]:
        source_id = source["source_id"]
        method = (
            RetrievalMethod.HTTP_PDF
            if source["machine_readability"] == "pdf_text"
            else RetrievalMethod.HTTP_HTML
        )
        out[source_id] = EvidenceObservation(
            source_id=source_id,
            role=SourceRole(source["role"]),
            observed_at_utc=NOW,
            status=SourceObservationStatus.AVAILABLE,
            retrieval_method=method,
            snapshot_sha256=_digest(source_id),
            snapshot_path=f"test-authority/{source_id}.bin",
        )
    return out


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
    return {
        "RFB_IRRF_TABLE_2026": NormalizedSourceCandidate(
            source_id="RFB_IRRF_TABLE_2026",
            source_url=registry["RFB_IRRF_TABLE_2026"]["url"],
            observed_at_utc=NOW,
            snapshot_sha256=_digest("RFB_IRRF_TABLE_2026"),
            snapshot_path="test-authority/RFB_IRRF_TABLE_2026.html",
            parser_id="rfb_irrf_table_v1",
            parser_version="1.0.0",
            status=ParseStatus.PARSED,
            payload=rfb_payload,
        ),
        "INSS_TABLE_2026": NormalizedSourceCandidate(
            source_id="INSS_TABLE_2026",
            source_url=registry["INSS_TABLE_2026"]["url"],
            observed_at_utc=NOW,
            snapshot_sha256=_digest("INSS_TABLE_2026"),
            snapshot_path="test-authority/INSS_TABLE_2026.html",
            parser_id="inss_employee_table_v1",
            parser_version="1.0.1",
            status=ParseStatus.PARSED,
            payload=inss_payload,
        ),
    }


def _governance_evidence() -> dict[str, GovernanceEvidenceObservation]:
    registry = _load(GOVERNANCE_REGISTRY)
    by_source = {item["source_id"]: item for item in registry["sources"]}
    money = by_source["SANIDA_TECHNICAL_MONEY_DECIMAL_V1"]
    contract = by_source["SANIDA_TECHNICAL_CONTRACT_GATE_V12"]
    return {
        "technical.money_decimal_and_rounding": GovernanceEvidenceObservation(
            source_id=money["source_id"],
            observed_at_utc=NOW,
            snapshot_sha256=_digest("money-v1.1"),
            snapshot_path="test-governance/money.json",
            repository_paths=money["repository_paths"],
        ),
        "technical.contract_vigency_and_quality": GovernanceEvidenceObservation(
            source_id=contract["source_id"],
            observed_at_utc=NOW,
            snapshot_sha256=_digest("contract-v1"),
            snapshot_path="test-governance/contract.json",
            repository_paths=contract["repository_paths"],
        ),
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
        official_evidence=_official_evidence(),
        governance_evidence=_governance_evidence(),
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
    assert any(
        item.rule_id == "technical.money_decimal_and_rounding"
        and item.change_class.value == "STRUCTURAL_CHANGE"
        for item in changed
    )


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
