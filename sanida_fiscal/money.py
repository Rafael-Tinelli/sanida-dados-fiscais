from __future__ import annotations

from decimal import (
    Decimal,
    ROUND_CEILING,
    ROUND_DOWN,
    ROUND_FLOOR,
    ROUND_HALF_EVEN,
    ROUND_HALF_UP,
)
from typing import TypeAlias

from .types_v1 import RoundingPolicy

DecimalInput: TypeAlias = Decimal | int | str

ROUNDING_MODES = {
    "ROUND_HALF_UP": ROUND_HALF_UP,
    "ROUND_HALF_EVEN": ROUND_HALF_EVEN,
    "ROUND_DOWN": ROUND_DOWN,
    "ROUND_FLOOR": ROUND_FLOOR,
    "ROUND_CEILING": ROUND_CEILING,
}


class DecimalInputError(TypeError):
    """Raised when a fiscal value would enter the engine through binary float."""


def as_decimal(value: DecimalInput, *, name: str = "value") -> Decimal:
    """Convert an accepted fiscal input to Decimal and reject float/bool explicitly."""
    if isinstance(value, bool) or isinstance(value, float):
        raise DecimalInputError(f"{name} must not use binary float/bool")
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, str)):
        try:
            return Decimal(str(value))
        except Exception as exc:  # Decimal raises several concrete subclasses.
            raise DecimalInputError(f"invalid decimal input for {name}") from exc
    raise DecimalInputError(f"unsupported decimal input type for {name}: {type(value)!r}")


def quantum(decimal_places: int) -> Decimal:
    if decimal_places < 0:
        raise ValueError("decimal_places must be non-negative")
    return Decimal(1).scaleb(-decimal_places)


def quantize(value: DecimalInput, policy: RoundingPolicy) -> Decimal:
    amount = as_decimal(value)
    mode = ROUNDING_MODES[policy.mode]
    return amount.quantize(quantum(policy.decimal_places), rounding=mode)
