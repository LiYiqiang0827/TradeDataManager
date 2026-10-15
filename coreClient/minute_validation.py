"""Shared validity checks for one-minute source and persistence paths."""

from __future__ import annotations

import math
from numbers import Real
from typing import Any, Mapping


def minute_value_error(bar: Mapping[str, Any]) -> str | None:
    """Return a stable rejection reason for an unusable price/volume pair.

    There is deliberately no upper-bound heuristic: unusual positive values
    need source-specific evidence before they can be rejected.
    """
    for field in ("price", "vol"):
        value = bar.get(field)
        if isinstance(value, bool) or not isinstance(value, Real):
            return f"{field}_missing_or_non_numeric"
        if not math.isfinite(float(value)):
            return f"{field}_non_finite"

    if float(bar["price"]) <= 0:
        return "price_nonpositive"
    if float(bar["vol"]) < 0:
        return "vol_negative"
    return None


def native_minute_bar_error(bar: Mapping[str, Any]) -> str | None:
    """Validate a native OHLCV/amount minute bar without arbitrary ceilings."""
    for field in ("open", "high", "low", "close"):
        value = bar.get(field)
        if isinstance(value, bool) or not isinstance(value, Real):
            return f"{field}_missing_or_non_numeric"
        if not math.isfinite(float(value)):
            return f"{field}_non_finite"
        if float(value) <= 0:
            return f"{field}_nonpositive"

    for field in ("vol", "amount"):
        value = bar.get(field)
        if isinstance(value, bool) or not isinstance(value, Real):
            return f"{field}_missing_or_non_numeric"
        if not math.isfinite(float(value)):
            return f"{field}_non_finite"
        if float(value) < 0:
            return f"{field}_negative"

    if float(bar["high"]) < max(float(bar["open"]), float(bar["close"]), float(bar["low"])):
        return "ohlc_high_below_component"
    if float(bar["low"]) > min(float(bar["open"]), float(bar["close"]), float(bar["high"])):
        return "ohlc_low_above_component"
    if not float(bar["vol"]).is_integer():
        return "vol_not_integral_shares"
    if float(bar["vol"]) == 0 and float(bar["amount"]) != 0:
        return "amount_nonzero_with_zero_volume"
    return None


def correct_pytdx_zero_volume_sentinel(value: Any) -> Any:
    """Undo pytdx helper.get_volume(0)'s exact ``2**-127`` result.

    The decoder computes a normalized float even when the raw four-byte field
    is zero. Preserve every other value; this is an exact sentinel correction,
    not a magnitude threshold or rounding rule.
    """
    if isinstance(value, Real) and not isinstance(value, bool):
        if float(value) == math.ldexp(1.0, -127):
            return 0
    return value
