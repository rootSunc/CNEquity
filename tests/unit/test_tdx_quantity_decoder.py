"""Raw binary regression cases for the shared K-line quantity decoder."""

import struct

import pytest

from cnequity.adapters.tdx_protocol._wire.helper import get_volume


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (0x3F800000, 1.0),
        (0x41200000, 10.0),
        (0x41600000, 14.0),  # 161728.SZ: 14 hands, not 1544 hands.
        (0x42600000, 56.0),
        (0x42C80000, 100.0),
        (0x44AF0000, 1400.0),
    ],
)
def test_quantity_decoder_preserves_small_wire_values(raw, expected):
    assert get_volume(raw) == expected


@pytest.mark.parametrize("exponent", range(-6, 31))
def test_quantity_decoder_matches_positive_binary32_across_mantissas(exponent):
    # Independently encode values, including both sides of the high-byte
    # branch and a nonzero fractional tail. Do not derive the oracle from
    # the implementation's exponent arithmetic.
    for mantissa in (1.0, 1.125, 1.5, 1.999):
        encoded = struct.pack("<f", mantissa * 2.0**exponent)
        raw = struct.unpack("<I", encoded)[0]
        expected = struct.unpack("<f", encoded)[0]
        assert get_volume(raw) == expected
