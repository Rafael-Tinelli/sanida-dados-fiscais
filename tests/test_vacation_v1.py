from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from hypothesis import given, strategies as st

from sanida_fiscal.contract_v1 import FiscalContractV1
from sanida_fiscal.engine_v1 import FiscalEngineError
from sanida_fiscal.types_v1 import AssessmentContext
from sanida_fiscal.vacation_v1 import (
    acquisition_period_for_date,
    calculate_cash_allowance_days,
    calculate_proportional_vacation_accrual,
    cash_allowance_tax_bases,
    resolve_cash_allowance_tax_treatment,
    select_vacation_acquisition_rule,
    select_vacation_rule_bundle,
    vacation_entitlement_from_absences,
)

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE_PATH = ROOT / "contracts" / "examples" / "fiscal-contract-v1.example.json"
REFERENCE_CASES_PATH = ROOT / "tests" / "reference_cases" / "phase1_reference_cases.json"


def D(value: str | int) -> Decimal:
    return Decimal(str(value))


def load_candidate() -> FiscalContractV1:
    return FiscalContractV1.model_validate(
        json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))
    )


def reference_cases() -> dict[str, dict]:
    payload = json.loads(REFERENCE_CASES_PATH.read_text(encoding="utf-8"))
    return {case["case_id"]: case for case in payload["cases"]}


def vacation_bundle():
    return select_vacation_rule_bundle(load_candidate(), date(2026, 9, 13))


def test_contract_v11_exposes_proportional_vacation_threshold() -> None:
    contract = load_candidate()
    payload = select_vacation_acquisition_rule(
        contract,
        date(2026, 3, 31),
        AssessmentContext.TERMINATION,
    )
    assert contract.schema_version == "1.1.0"
    assert contract.consumer_compatibility.contract_api_version == "1.1.0"
    assert payload.duration_months == 12
    assert payload.calendar_year_reset is False
    assert (
        payload.proportional_accrual_method
        == "one_twelfth_per_acquisition_month_or_fraction_gte_days"
    )
    assert payload.proportional_qualifying_days == 15


def test_a02_reference_case_crosses_calendar_year_without_reset() -> None:
    case = reference_cases()["vacation_period_cross_year_A02_regression"]
    payload = select_vacation_acquisition_rule(
        load_candidate(),
        date.fromisoformat(case["inputs"]["termination_date"]),
        AssessmentContext.TERMINATION,
    )
    result = calculate_proportional_vacation_accrual(
        employment_start=date.fromisoformat(case["inputs"]["admission_date"]),
        through_date=date.fromisoformat(case["inputs"]["termination_date"]),
        payload=payload,
    )
    assert result.period.start == date(2025, 9, 1)
    assert result.period.end_inclusive == date(2026, 8, 31)
    assert result.twelfths == case["expected"]["proportional_vacation_twelfths"] == 7
    assert [item.twelfth_index for item in result.slices if item.qualifies] == list(
        range(1, 8)
    )


def test_proportional_slices_are_anchored_to_employment_not_calendar_months() -> None:
    payload = select_vacation_acquisition_rule(
        load_candidate(), date(2026, 1, 10), AssessmentContext.TERMINATION
    )
    result = calculate_proportional_vacation_accrual(
        employment_start=date(2025, 9, 20),
        through_date=date(2026, 1, 10),
        payload=payload,
    )
    assert result.period.start == date(2025, 9, 20)
    assert result.twelfths == 4
    assert result.slices[-1].start == date(2025, 12, 20)
    assert result.slices[-1].service_days == 22
    assert result.slices[-1].qualifies is True


def test_partial_acquisition_month_has_exact_14_15_day_boundary() -> None:
    payload = select_vacation_acquisition_rule(
        load_candidate(), date(2026, 1, 15), AssessmentContext.TERMINATION
    )
    fourteen = calculate_proportional_vacation_accrual(
        employment_start=date(2026, 1, 1),
        through_date=date(2026, 1, 14),
        payload=payload,
    )
    fifteen = calculate_proportional_vacation_accrual(
        employment_start=date(2026, 1, 1),
        through_date=date(2026, 1, 15),
        payload=payload,
    )
    assert fourteen.slices[0].service_days == 14
    assert fourteen.twelfths == 0
    assert fifteen.slices[0].service_days == 15
    assert fifteen.twelfths == 1


def test_acquisition_period_advances_on_employment_anniversary_not_january_first() -> None:
    payload = vacation_bundle().acquisition
    period = acquisition_period_for_date(
        employment_start=date(2024, 9, 20),
        target_date=date(2026, 1, 10),
        payload=payload,
    )
    assert period.period_index == 1
    assert period.start == date(2025, 9, 20)
    assert period.end_inclusive == date(2026, 9, 19)


def test_acquisition_period_rejects_date_before_employment() -> None:
    with pytest.raises(FiscalEngineError, match="cannot precede"):
        acquisition_period_for_date(
            employment_start=date(2026, 2, 1),
            target_date=date(2026, 1, 31),
            payload=vacation_bundle().acquisition,
        )


@pytest.mark.parametrize(
    ("case_id", "expected_days"),
    [
        ("vacation_entitlement_absences_5", 30),
        ("vacation_entitlement_absences_6", 24),
        ("vacation_entitlement_absences_15", 18),
        ("vacation_entitlement_absences_24", 12),
        ("vacation_entitlement_absences_32", 12),
    ],
)
def test_entitlement_bands_reproduce_phase1_cases(case_id: str, expected_days: int) -> None:
    case = reference_cases()[case_id]
    result = vacation_entitlement_from_absences(
        case["inputs"]["unjustified_absences"], vacation_bundle().entitlement
    )
    assert result.entitled_days == expected_days
    assert result.entitled_days == case["expected"]["vacation_entitled_days"]


def test_absences_outside_supported_bands_fail_closed() -> None:
    with pytest.raises(FiscalEngineError, match="outside supported"):
        vacation_entitlement_from_absences(33, vacation_bundle().entitlement)


def test_abono_is_one_third_of_entitlement_not_selected_gozo_days() -> None:
    case = reference_cases()["vacation_abono_entitlement_30_days"]
    result = calculate_cash_allowance_days(
        entitled_days=case["inputs"]["vacation_entitled_days"],
        payload=vacation_bundle().cash_allowance_fraction,
    )
    assert result.cash_allowance_days == case["expected"]["cash_allowance_days"] == 10
    assert result.remaining_vacation_days == case["expected"]["remaining_vacation_days"] == 20


@pytest.mark.parametrize(
    ("entitled_days", "allowance_days", "remaining_days"),
    [(30, 10, 20), (24, 8, 16), (18, 6, 12), (12, 4, 8)],
)
def test_abono_fraction_is_exact_for_all_supported_entitlements(
    entitled_days: int, allowance_days: int, remaining_days: int
) -> None:
    result = calculate_cash_allowance_days(
        entitled_days=entitled_days,
        payload=vacation_bundle().cash_allowance_fraction,
    )
    assert result.cash_allowance_days == allowance_days
    assert result.remaining_vacation_days == remaining_days


def test_abono_non_integral_fraction_fails_instead_of_rounding_arbitrarily() -> None:
    with pytest.raises(FiscalEngineError, match="non-integral"):
        calculate_cash_allowance_days(
            entitled_days=25,
            payload=vacation_bundle().cash_allowance_fraction,
        )


def test_principal_and_constitutional_third_remain_distinct_tax_components() -> None:
    bundle = vacation_bundle()
    treatment = resolve_cash_allowance_tax_treatment(
        principal_payload=bundle.principal_incidence,
        constitutional_third_payload=bundle.constitutional_third_incidence,
    )
    assert treatment.principal.component.value == "cash_allowance_principal"
    assert treatment.principal.irrf.value == "no"
    assert treatment.principal.social_security.value == "no"
    assert (
        treatment.constitutional_third.component.value
        == "constitutional_third_on_cash_allowance"
    )
    assert treatment.constitutional_third.irrf.value == "yes"
    assert treatment.constitutional_third.social_security.value == "no"


def test_phase1_tax_treatment_reference_cases_are_executable() -> None:
    cases = reference_cases()
    principal_case = cases["vacation_abono_principal_tax_treatment"]
    third_case = cases["vacation_abono_constitutional_third_tax_treatment_P0"]
    treatment = resolve_cash_allowance_tax_treatment(
        principal_payload=vacation_bundle().principal_incidence,
        constitutional_third_payload=vacation_bundle().constitutional_third_incidence,
    )
    assert (treatment.principal.irrf.value == "yes") is principal_case["expected"][
        "irrf_incidence"
    ]
    assert (
        treatment.principal.social_security.value == "yes"
    ) is principal_case["expected"]["social_security_incidence"]
    assert (
        treatment.constitutional_third.irrf.value == "yes"
    ) is third_case["expected"]["irrf_incidence"]
    assert (
        treatment.constitutional_third.social_security.value == "yes"
    ) is third_case["expected"]["social_security_incidence"]


def test_cash_allowance_tax_base_contains_only_the_constitutional_third() -> None:
    treatment = resolve_cash_allowance_tax_treatment(
        principal_payload=vacation_bundle().principal_incidence,
        constitutional_third_payload=vacation_bundle().constitutional_third_incidence,
    )
    bases = cash_allowance_tax_bases(
        principal_amount="1000.00",
        constitutional_third_amount="333.33",
        treatment=treatment,
    )
    assert bases.principal_amount == D("1000.00")
    assert bases.constitutional_third_amount == D("333.33")
    assert bases.irrf_taxable_amount == D("333.33")
    assert bases.social_security_base == D("0")


def test_swapped_incidence_profiles_are_rejected_instead_of_merged() -> None:
    bundle = vacation_bundle()
    with pytest.raises(FiscalEngineError, match="component mismatch"):
        resolve_cash_allowance_tax_treatment(
            principal_payload=bundle.constitutional_third_incidence,
            constitutional_third_payload=bundle.principal_incidence,
        )


@given(st.integers(min_value=0, max_value=430))
def test_proportional_vacation_is_bounded_to_one_acquisition_period(days_after_start: int) -> None:
    start = date(2026, 1, 1)
    through = start + timedelta(days=days_after_start)
    payload = select_vacation_acquisition_rule(
        load_candidate(), through, AssessmentContext.TERMINATION
    )
    result = calculate_proportional_vacation_accrual(
        employment_start=start,
        through_date=through,
        payload=payload,
    )
    assert 0 <= result.twelfths <= 12


@given(st.sampled_from([12, 18, 24, 30]))
def test_cash_allowance_plus_remaining_always_equals_entitlement(entitled_days: int) -> None:
    result = calculate_cash_allowance_days(
        entitled_days=entitled_days,
        payload=vacation_bundle().cash_allowance_fraction,
    )
    assert result.cash_allowance_days + result.remaining_vacation_days == entitled_days
