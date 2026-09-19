from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from sanida_fiscal.contract_v1_2 import (
    FiscalContractV12,
    GovernanceEvidenceObservation,
)
from sanida_fiscal.governance_evidence_v12 import build_governance_evidence
from sanida_fiscal.inss_employee_v1 import (
    PARSER_ID as INSS_PARSER_ID,
    PARSER_VERSION as INSS_PARSER_VERSION,
    parse_inss_employee_snapshot,
)
from sanida_fiscal.publication_v1 import (
    FiscalReleaseStore,
    HumanReviewRequiredError,
    NoPublicationRequired,
    prepare_published_release,
)
from sanida_fiscal.release_assembler_v12 import assemble_candidate_v12
from sanida_fiscal.rfb_irrf_v1 import (
    PARSER_ID as RFB_PARSER_ID,
    PARSER_VERSION as RFB_PARSER_VERSION,
    parse_rfb_irrf_snapshot,
)
from sanida_fiscal.semantic_diff_v1 import PromotionOutcome, assess_promotion
from sanida_fiscal.sources_v1 import NormalizedSourceCandidate, ParseStatus
from sanida_fiscal.types_v1 import (
    EvidenceObservation,
    ReleaseApprovalMode,
    RetrievalMethod,
    SourceObservationStatus,
    SourceRole,
)


ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 9, 14, 3, 0, tzinfo=timezone.utc)
TEMPLATE = ROOT / "contracts/examples/fiscal-contract-v1.example.json"
COVERAGE = ROOT / "docs/contract-coverage-v1.json"
INVENTORY = ROOT / "docs/rule-inventory-v1.json"
SOURCE_REGISTRY = ROOT / "docs/source-registry-v1.json"
GOVERNANCE_REGISTRY = ROOT / "docs/governance-source-registry-v1.json"
RFB_FIXTURE = ROOT / "tests/fixtures/sources/rfb_irrf_2026_fragment.html"
INSS_FIXTURE = ROOT / "tests/fixtures/sources/inss_employee_2026_fragment.html"


def _load(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _digest(seed: str) -> str:
    # Deterministic valid test SHA without pretending to be a real production hash.
    raw = seed.encode("utf-8").hex()
    return (raw * ((64 // len(raw)) + 1))[:64]


def _official_evidence(
    *,
    observed_at: datetime = NOW,
    overrides: dict[str, str] | None = None,
) -> dict[str, EvidenceObservation]:
    overrides = overrides or {}
    registry = _load(SOURCE_REGISTRY)
    output: dict[str, EvidenceObservation] = {}
    for source in registry["sources"]:
        source_id = source["source_id"]
        parser_id = None
        parser_version = None
        if source_id == "RFB_IRRF_TABLE_CURRENT":
            parser_id = RFB_PARSER_ID
            parser_version = RFB_PARSER_VERSION
        elif source_id == "INSS_TABLE_CURRENT":
            parser_id = INSS_PARSER_ID
            parser_version = INSS_PARSER_VERSION
        method = (
            RetrievalMethod.HTTP_PDF
            if source["machine_readability"] == "pdf_text"
            else RetrievalMethod.HTTP_HTML
        )
        output[source_id] = EvidenceObservation(
            source_id=source_id,
            role=SourceRole(source["role"]),
            observed_at_utc=observed_at,
            status=SourceObservationStatus.AVAILABLE,
            retrieval_method=method,
            snapshot_sha256=overrides.get(source_id, _digest(source_id)),
            snapshot_path=f"test-authority/{source_id}.bin",
            parser_id=parser_id,
            parser_version=parser_version,
        )
    return output


def _normalized_candidates(
    *,
    observed_at: datetime = NOW,
    rfb_payload_override: dict | None = None,
    rfb_sha: str | None = None,
) -> dict[str, NormalizedSourceCandidate]:
    registry = {item["source_id"]: item for item in _load(SOURCE_REGISTRY)["sources"]}
    rfb_payload = rfb_payload_override or parse_rfb_irrf_snapshot(RFB_FIXTURE.read_bytes())
    inss_payload = parse_inss_employee_snapshot(INSS_FIXTURE.read_bytes())
    return {
        "RFB_IRRF_TABLE_CURRENT": NormalizedSourceCandidate(
            source_id="RFB_IRRF_TABLE_CURRENT",
            source_url=registry["RFB_IRRF_TABLE_CURRENT"]["url"],
            observed_at_utc=observed_at,
            snapshot_sha256=rfb_sha or _digest("RFB_IRRF_TABLE_CURRENT"),
            snapshot_path="test-authority/RFB_IRRF_TABLE_2026.html",
            parser_id=RFB_PARSER_ID,
            parser_version=RFB_PARSER_VERSION,
            status=ParseStatus.PARSED,
            payload=rfb_payload,
        ),
        "INSS_TABLE_CURRENT": NormalizedSourceCandidate(
            source_id="INSS_TABLE_CURRENT",
            source_url=registry["INSS_TABLE_CURRENT"]["url"],
            observed_at_utc=observed_at,
            snapshot_sha256=_digest("INSS_TABLE_CURRENT"),
            snapshot_path="test-authority/INSS_TABLE_2026.html",
            parser_id=INSS_PARSER_ID,
            parser_version=INSS_PARSER_VERSION,
            status=ParseStatus.PARSED,
            payload=inss_payload,
        ),
    }


def _governance_evidence(
    *,
    observed_at: datetime = NOW,
    hash_suffix: str = "",
) -> dict[str, GovernanceEvidenceObservation]:
    return {
        "technical.money_decimal_and_rounding": GovernanceEvidenceObservation(
            source_id="SANIDA_TECHNICAL_MONEY_DECIMAL_V1",
            observed_at_utc=observed_at,
            snapshot_sha256=_digest("money" + hash_suffix),
            snapshot_path="test-governance/money.json",
            repository_paths=[
                "sanida_fiscal/types_v1.py",
                "sanida_fiscal/engine_v1.py",
                "docs/phase3-library-v1.md",
            ],
        ),
        "technical.contract_vigency_and_quality": GovernanceEvidenceObservation(
            source_id="SANIDA_TECHNICAL_CONTRACT_GATE_V12",
            observed_at_utc=observed_at,
            snapshot_sha256=_digest("contract" + hash_suffix),
            snapshot_path="test-governance/contract.json",
            repository_paths=[
                "sanida_fiscal/contract_v1.py",
                "sanida_fiscal/contract_v1_2.py",
                "docs/contract-coverage-v1.json",
                "docs/rule-inventory-v1.json",
            ],
        ),
    }


def _assemble(
    *,
    previous=None,
    observed_at: datetime = NOW,
    official=None,
    normalized=None,
    governance=None,
) -> FiscalContractV12:
    return assemble_candidate_v12(
        template_v11=_load(TEMPLATE),
        coverage=_load(COVERAGE),
        inventory=_load(INVENTORY),
        official_evidence=official or _official_evidence(observed_at=observed_at),
        governance_evidence=governance or _governance_evidence(observed_at=observed_at),
        normalized_candidates=normalized or _normalized_candidates(observed_at=observed_at),
        generated_at_utc=observed_at,
        governance_source_registry_version="1.0.0",
        previous=previous,
    )


def _published_bootstrap() -> FiscalContractV12:
    candidate = _assemble()
    published = prepare_published_release(
        previous=None,
        candidate=candidate,
        coverage=_load(COVERAGE),
        published_at_utc=NOW,
        human_approval_reference="phase5-test:bootstrap-human-review",
    )
    assert isinstance(published, FiscalContractV12)
    return published


def test_v12_bootstrap_materializes_exact_32_rule_contract() -> None:
    candidate = _assemble()
    required = {item["rule_id"] for item in _load(COVERAGE)["rules"]}

    assert candidate.schema_version == "1.2.0"
    assert candidate.consumer_compatibility.schema_version == "1.2.0"
    assert candidate.consumer_compatibility.contract_api_version == "1.2.0"
    assert candidate.governance_source_registry_version == "1.0.0"
    assert len(candidate.rules) == 32
    assert {rule.rule_id for rule in candidate.rules} == required
    assert all(rule.change_class.value == "RULE_ADDED" for rule in candidate.rules)

    indexed = {rule.rule_id: rule for rule in candidate.rules}
    inss = indexed["inss.employee.progressive_table"]
    assert str(inss.payload.cap_base) == "8475.55"
    assert [str(row.upper_bound) if row.upper_bound is not None else None for row in inss.payload.brackets] == [
        "1621.00",
        "2902.84",
        "4354.27",
        "8475.55",
    ]
    simplified = indexed["irrf.simplified_monthly_discount"]
    assert str(simplified.payload.value) == "607.20"

    vacation_reduction = indexed["vacation.irrf.reduction.2026"]
    assert vacation_reduction.payload.input_semantic == (
        "taxable_vacation_income_subject_to_separate_monthly_irrf_assessment_before_deductions"
    )

    for rule_id in (
        "technical.money_decimal_and_rounding",
        "technical.contract_vigency_and_quality",
    ):
        assert indexed[rule_id].provenance[0].role == "internal_governance"

    assert indexed["irrf.monthly.progressive_table"].provenance[0].role != "internal_governance"


def test_v12_bootstrap_requires_explicit_human_approval_and_store_reloads_v12(tmp_path: Path) -> None:
    candidate = _assemble()
    with pytest.raises(HumanReviewRequiredError):
        prepare_published_release(
            previous=None,
            candidate=candidate,
            coverage=_load(COVERAGE),
            published_at_utc=NOW,
        )

    published = prepare_published_release(
        previous=None,
        candidate=candidate,
        coverage=_load(COVERAGE),
        published_at_utc=NOW,
        human_approval_reference="phase5-test:bootstrap-human-review",
    )
    assert isinstance(published, FiscalContractV12)
    assert published.lifecycle.approval_mode == ReleaseApprovalMode.HUMAN_REVIEWED
    published.assert_consumable()

    store = FiscalReleaseStore(tmp_path / "releases")
    manifest = store.publish(published)
    reloaded = store.load_current()
    assert manifest["schema_version_contract"] == "1.2.0"
    assert isinstance(reloaded, FiscalContractV12)
    assert reloaded.release_id == published.release_id


def test_v12_identical_successor_is_idempotent_even_with_new_observation_time() -> None:
    previous = _published_bootstrap()
    later = NOW + timedelta(hours=1)
    candidate = _assemble(previous=previous, observed_at=later)
    assessment = assess_promotion(previous, candidate)

    assert assessment.outcome == PromotionOutcome.NO_PUBLISH_REQUIRED
    assert assessment.diff.immutable_payload_changed is False
    with pytest.raises(NoPublicationRequired):
        prepare_published_release(
            previous=previous,
            candidate=candidate,
            coverage=_load(COVERAGE),
            published_at_utc=later,
        )


def test_v12_parser_backed_numeric_refresh_can_auto_publish() -> None:
    previous = _published_bootstrap()
    later = NOW + timedelta(hours=2)
    rfb_payload = deepcopy(parse_rfb_irrf_snapshot(RFB_FIXTURE.read_bytes()))
    rfb_payload["monthly_table"][1]["upper_bound_brl"] = "2826.66"
    changed_sha = "b" * 64
    official = _official_evidence(
        observed_at=later,
        overrides={"RFB_IRRF_TABLE_CURRENT": changed_sha},
    )
    normalized = _normalized_candidates(
        observed_at=later,
        rfb_payload_override=rfb_payload,
        rfb_sha=changed_sha,
    )
    candidate = _assemble(
        previous=previous,
        observed_at=later,
        official=official,
        normalized=normalized,
    )
    assessment = assess_promotion(previous, candidate)

    assert assessment.outcome == PromotionOutcome.AUTO_PUBLISH_ALLOWED, assessment.reasons
    changed = {
        item.rule_id: item.change_class.value
        for item in assessment.diff.rule_diffs
        if item.changed
    }
    assert changed["irrf.monthly.progressive_table"] == "PARAMETER_CHANGE"

    published = prepare_published_release(
        previous=previous,
        candidate=candidate,
        coverage=_load(COVERAGE),
        published_at_utc=later,
    )
    assert published.lifecycle.approval_mode == ReleaseApprovalMode.AUTO_VALIDATED


def test_v12_structural_authority_refresh_requires_human_review() -> None:
    previous = _published_bootstrap()
    later = NOW + timedelta(hours=3)
    official = _official_evidence(
        observed_at=later,
        overrides={"PLANALTO_CLT": "c" * 64},
    )
    candidate = _assemble(previous=previous, observed_at=later, official=official)
    assessment = assess_promotion(previous, candidate)

    assert assessment.outcome == PromotionOutcome.REVIEW_REQUIRED
    assert any("structural rule" in reason for reason in assessment.reasons)


def test_v12_governance_registry_builds_hash_addressed_internal_evidence(tmp_path: Path) -> None:
    evidence = build_governance_evidence(
        repository_root=ROOT,
        registry_path=GOVERNANCE_REGISTRY,
        evidence_root=tmp_path / "governance",
        observed_at_utc=NOW,
    )
    assert set(evidence) == {
        "technical.money_decimal_and_rounding",
        "technical.contract_vigency_and_quality",
    }
    for observation in evidence.values():
        assert observation.role == "internal_governance"
        assert (tmp_path / "governance" / observation.snapshot_path).is_file()
