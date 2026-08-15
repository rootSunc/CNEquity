from datetime import date

from cn_market_lake.steps.common import classify_daily_bar_ownership


def test_daily_bar_ownership_is_explicit_for_every_symbol():
    symbols = ["600001.SH", "600002.SH", "600003.SH", "600004.SH"]
    spans = {
        "600001.SH": (date(2000, 1, 1), None),
        "600002.SH": (date(2000, 1, 1), date(2015, 12, 31)),
        "600003.SH": (date(2000, 1, 1), date(2020, 6, 1)),
        "600004.SH": (date(2025, 1, 1), None),
    }

    result = classify_daily_bar_ownership(
        symbols,
        spans,
        date(2016, 1, 1),
        date(2024, 12, 31),
    )

    assert result.generic == ["600001.SH"]
    assert result.delegated_delisted == ["600003.SH"]
    assert result.expected_no_data == ["600002.SH", "600004.SH"]
    assert set(result.generic + result.delegated_delisted + result.expected_no_data) == set(symbols)
