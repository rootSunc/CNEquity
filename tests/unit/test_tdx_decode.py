from cn_market_lake.adapters.tdx_protocol._decode import DECODED_ZERO, decoded_quantity


def test_denormal_zero_is_snapped_to_zero():
    assert decoded_quantity(2.0**-127) == 0.0


def test_none_and_missing_are_zero():
    assert decoded_quantity(None) == 0.0
    assert decoded_quantity(0) == 0.0


def test_real_quantities_pass_through():
    assert decoded_quantity(67700) == 67700
    assert decoded_quantity(91_450_000.0) == 91_450_000.0


def test_unparseable_value_falls_back_to_zero():
    # A non-numeric field would otherwise blow up the whole batch on one bad
    # row; the decoder's job is to make sense of wire noise, not to propagate it.
    assert decoded_quantity("not-a-number") == 0.0
    assert decoded_quantity(object()) == 0.0


def test_threshold_is_far_below_any_genuine_quantity():
    # Real turnover is >= 0.01 yuan; the snap threshold must sit well under it.
    assert DECODED_ZERO < 0.01
