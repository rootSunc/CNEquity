"""Wire-value cleanup shared by the TDX bar adapters.

TDX packs quantities into a custom float, and ``_wire/helper.get_volume``
decodes a raw zero to ``2**-127`` (~5.88e-39) rather than to ``0.0``: with
every mantissa byte zero the exponent term ``2**(0*2 - 0x7F)`` still survives
and nothing subtracts it back out. That zero behavior remains in the vendored
decoder; correcting it here keeps the zero policy explicit. The decoder also
has a documented local patch for negative exponents: the upstream reciprocal
inflated small nonzero quantities (14 became 1544). That error must be fixed
before raw bits are lost, rather than guessed from already decoded values.

Left alone, that denormal reaches curated as a turnover of 5.9e-39 yuan on
every no-trade bar, which contradicts the lake's stated suspension convention
(``volume=0``, ``amount=0``) and quietly makes ``amount > 0`` mean "was
quoted" instead of "traded". ``int()`` happens to flatten volume by
truncation; ``amount`` is a float and keeps it.

Real quantities are integers ≥1 and real turnover is ≥0.01 yuan, so the
threshold below sits about twenty-four orders of magnitude clear of anything
genuine.
"""

from __future__ import annotations

import math

__all__ = ["DECODED_ZERO", "decoded_quantity", "decoded_quantity_or_none"]

DECODED_ZERO = 1e-6


def decoded_quantity(value) -> float:
    """A decoded wire quantity, with the decoder's denormal zero snapped to 0.0."""
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if abs(number) < DECODED_ZERO else number


def decoded_quantity_or_none(value) -> float | None:
    """Decode a present wire quantity, preserving malformed input as unknown.

    ``decoded_quantity`` keeps the historical convenience contract that a
    missing value means the protocol's numeric zero. Row parsers need a
    stricter boundary: a missing or non-numeric volume/amount must not become
    a valid-looking suspended bar and hide a source schema change.
    """
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(number):
        return None
    return decoded_quantity(number)
