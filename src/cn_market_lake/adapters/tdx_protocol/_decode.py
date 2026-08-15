"""Wire-value cleanup shared by the TDX bar adapters.

TDX packs quantities into a custom float, and ``_wire/helper.get_volume``
decodes a raw zero to ``2**-127`` (~5.88e-39) rather than to ``0.0``: with
every mantissa byte zero the exponent term ``2**(0*2 - 0x7F)`` still survives
and nothing subtracts it back out. That decoder is a vendored upstream subset
and is kept as-is; correcting the value here rather than there keeps it
diffable against its origin.

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

__all__ = ["DECODED_ZERO", "decoded_quantity"]

DECODED_ZERO = 1e-6


def decoded_quantity(value) -> float:
    """A decoded wire quantity, with the decoder's denormal zero snapped to 0.0."""
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if abs(number) < DECODED_ZERO else number
