"""Regression contract: calculated tax is distinct from effective withholding."""
from decimal import Decimal

import pytest

from sanida_fiscal.engine_v1 import (
    IrrfIncomeType,
    irrf_withholding_after_minimum,
)


@pytest.mark.parametrize(
    "amount,income,expected_waiver,expected_retained",
    [
        ("0", IrrfIncomeType.MONTHLY, "0", "0"),
        ("9.99", IrrfIncomeType.MONTHLY, "9.99", "0"),
        ("10.00", IrrfIncomeType.MONTHLY, "10.00", "0"),
        ("10.01", IrrfIncomeType.MONTHLY, "0", "10.01"),
        ("10.00", IrrfIncomeType.VACATION, "10.00", "0"),
        ("10.01", IrrfIncomeType.VACATION, "0", "10.01"),
        ("9.99", IrrfIncomeType.THIRTEENTH, "0", "9.99"),
        ("10.00", IrrfIncomeType.THIRTEENTH, "0", "10.00"),
    ],
)
def test_minimum_withholding(amount, income, expected_waiver, expected_retained):
    waived, retained = irrf_withholding_after_minimum(Decimal(amount), income)
    assert waived == Decimal(expected_waiver)
    assert retained == Decimal(expected_retained)
    assert waived + retained == Decimal(amount)


@pytest.mark.parametrize("bad", ["-0.01", "-10"])
def test_negative_calculated_tax_rejected(bad):
    with pytest.raises(Exception, match="cannot be negative"):
        irrf_withholding_after_minimum(Decimal(bad), IrrfIncomeType.MONTHLY)


def test_income_type_must_be_explicit():
    with pytest.raises(Exception, match="income_type"):
        irrf_withholding_after_minimum(Decimal("10"), "monthly")
