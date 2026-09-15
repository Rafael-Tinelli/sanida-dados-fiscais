from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from sanida_fiscal.contract_v1_2 import FiscalContractV12
from sanida_fiscal.engine_v1 import (
    IrrfAssessmentIdentity,
    IrrfIncomeType,
    select_irrf_rule_bundle,
)
from sanida_fiscal.publication_v1 import FiscalReleaseStore
from sanida_fiscal.release_assembler_v12 import (
    _apply_structural_successor_overlays,
    _index_rules,
    _reconcile_declared_transition,
)
from sanida_fiscal.semantic_diff_v1 import PromotionOutcome, assess_promotion
from sanida_fiscal.types_v1 import AssessmentContext, ChangeClass, FractionPayload

ROOT = Path(__file__).resolve().parents[1]
STORE = ROOT / "releases/fiscal-v1"
TARGET_DATE = date(2026, 9, 15)


def _current_release():
    release = FiscalReleaseStore(STORE).load_current()
    assert release is not None
    release.assert_consumable()
    return release


def _candidate_with_c65_overlay():
    previous = _current_release()
    data = previous.model_dump(mode="json", exclude_none=True)
    data["release_id"] = "candidate-c65-vacation-successor"
    data["status"] = "CANDIDATE"
    data["generated_at_utc"] = datetime(
        2026, 9, 15, 20, 0, tzinfo=timezone.utc
    ).isoformat().replace("+00:00", "Z")
    data["supersedes_release_id"] = previous.release_id
    data["lifecycle"] = {}
    rules = _index_rules(data)
    _apply_structural_successor_overlays(rules)
    data["rules"] = [rules[rule_id] for rule_id in sorted(rules)]
    candidate = FiscalContractV12.model_validate(data)
    return previous, _reconcile_declared_transition(previous, candidate)


def test_c65_overlay_keeps_closed_32_rule_inventory_and_requires_review() -> None:
    previous, candidate = _candidate_with_c65_overlay()
    assert len(candidate.rules) == len(previous.rules) == 32

    assessment = assess_promotion(previous, candidate)
    assert assessment.outcome == PromotionOutcome.REVIEW_REQUIRED

    changed = {
        item.rule_id: item
        for item in assessment.diff.rule_diffs
        if item.changed
    }
    for rule_id in (
        "vacation.abono_pecuniario",
        "vacation.remuneration_and_constitutional_third",
    ):
        assert rule_id in changed
        assert changed[rule_id].change_class == ChangeClass.STRUCTURAL_CHANGE


def test_c65_cash_allowance_fraction_explicitly_covers_corresponding_remuneration() -> None:
    previous, candidate = _candidate_with_c65_overlay()
    old = previous.select_rule(
        "vacation.abono_pecuniario",
        TARGET_DATE,
        AssessmentContext.VACATION_CASH_ALLOWANCE,
    )
    new = candidate.select_rule(
        "vacation.abono_pecuniario",
        TARGET_DATE,
        AssessmentContext.VACATION_CASH_ALLOWANCE,
    )

    assert isinstance(new.payload, FractionPayload)
    assert new.payload.numerator == 1
    assert new.payload.denominator == 3
    assert new.payload == old.payload
    assert new.applies_to == (
        "vacation_entitled_days_and_corresponding_remuneration_components"
    )
    assert new.rule_version != old.rule_version


def test_c65_vacation_remuneration_formula_is_selectable_for_cash_allowance() -> None:
    previous, candidate = _candidate_with_c65_overlay()
    old = previous.select_rule(
        "vacation.remuneration_and_constitutional_third",
        TARGET_DATE,
        AssessmentContext.VACATION_ENJOYED,
    )
    cash = candidate.select_rule(
        "vacation.remuneration_and_constitutional_third",
        TARGET_DATE,
        AssessmentContext.VACATION_CASH_ALLOWANCE,
    )

    assert cash.payload == old.payload
    assert cash.rounding_policy == old.rounding_policy
    assert AssessmentContext.VACATION_CASH_ALLOWANCE in cash.contexts
    assert cash.applies_to == (
        "vacation_remuneration_components_and_constitutional_third"
    )
    assert cash.rule_version != old.rule_version


def test_current_v12_vacation_irrf_uses_dedicated_reduction_rule() -> None:
    release = _current_release()
    assessment = IrrfAssessmentIdentity(
        income_type=IrrfIncomeType.VACATION,
        origin_context=AssessmentContext.VACATION_ENJOYED,
    )
    rules = select_irrf_rule_bundle(release, TARGET_DATE, assessment)

    assert rules.reduction_rule_id == "vacation.irrf.reduction.2026"
    assert rules.reduction.input_semantic == (
        "taxable_vacation_income_subject_to_separate_monthly_irrf_assessment_before_deductions"
    )
