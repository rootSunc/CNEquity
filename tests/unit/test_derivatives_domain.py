"""Contract identity and counting basis for futures and options."""

from __future__ import annotations

from datetime import date

import pytest

from cnequity.domain.derivatives import (
    CountingBasisError,
    format_strike,
    is_double_sided,
    parse_future_code,
    parse_option_code,
    percent_to_fraction,
    single_sided_amount,
    single_sided_count,
)
from cnequity.domain.futures_products import product_spec


@pytest.mark.parametrize(
    ("code", "exchange", "observed", "symbol"),
    [
        ("cu2511", "SHF", date(2025, 1, 2), "CU2511.SHF"),
        ("sc2502", "INE", date(2025, 1, 2), "SC2502.INE"),
        ("m2601", "DCE", date(2025, 3, 1), "M2601.DCE"),
        ("IF1005", "CFE", date(2010, 4, 16), "IF1005.CFE"),
        ("T2512", "CFE", date(2025, 9, 1), "T2512.CFE"),
        ("si2611", "GFE", date(2026, 1, 5), "SI2611.GFE"),
        # SHFE's first files spell 2002-01 as "0201".
        ("cu0201", "SHF", date(2002, 1, 7), "CU0201.SHF"),
    ],
)
def test_future_codes_map_to_canonical_symbols(code, exchange, observed, symbol):
    assert parse_future_code(code, exchange, observed).symbol == symbol


@pytest.mark.parametrize(
    ("observed", "year"),
    [
        (date(2005, 3, 1), 2006),
        (date(2015, 3, 1), 2016),
        (date(2025, 3, 1), 2026),
        # Still quoted in its own delivery month.
        (date(2026, 1, 10), 2026),
    ],
)
def test_czce_one_digit_year_resolves_to_the_decade_it_was_quoted_in(observed, year):
    contract = parse_future_code("TA601", "CZC", observed)
    assert contract.year == year
    assert contract.symbol == f"TA{year % 100:02d}01.CZC"


def test_one_digit_years_are_czce_only():
    with pytest.raises(ValueError):
        parse_future_code("cu601", "SHF", date(2025, 1, 2))


@pytest.mark.parametrize(
    ("code", "exchange", "observed", "symbol", "underlying"),
    [
        ("cu2511C80000", "SHF", date(2025, 6, 2), "CU2511C80000.SHF", "CU2511.SHF"),
        ("sc2502C460", "INE", date(2025, 1, 2), "SC2502C460.INE", "SC2502.INE"),
        ("m2601-C-3000", "DCE", date(2025, 6, 2), "M2601C3000.DCE", "M2601.DCE"),
        ("TA601C5400", "CZC", date(2025, 6, 2), "TA2601C5400.CZC", "TA2601.CZC"),
        ("si2611-P-9000", "GFE", date(2026, 1, 5), "SI2611P9000.GFE", "SI2611.GFE"),
        ("IO2512-C-4000", "CFE", date(2025, 9, 1), "IO2512C4000.CFE", "000300.SH"),
        ("MO2610-P-6200", "CFE", date(2026, 9, 1), "MO2610P6200.CFE", "000852.SH"),
        ("HO2501-C-2300", "CFE", date(2024, 12, 2), "HO2501C2300.CFE", "000016.SH"),
        # A second CZCE expiry series on the same underlying keeps its tag.
        ("CF701MSC14200", "CZC", date(2026, 9, 24), "CF2701MSC14200.CZC", "CF2701.CZC"),
    ],
)
def test_option_codes_map_to_symbol_and_underlying(code, exchange, observed, symbol, underlying):
    option = parse_option_code(code, exchange, observed)
    assert option.symbol == symbol
    assert option.underlying_symbol == underlying


def test_strikes_render_without_float_noise():
    assert format_strike(3000.0) == "3000"
    assert format_strike(2.5) == "2.5"
    assert format_strike(0.1 + 0.2) == "0.3"


def test_double_sided_counts_are_halved_before_2020_only():
    assert is_double_sided("CZC", date(2019, 12, 31))
    assert not is_double_sided("CZC", date(2020, 1, 2))
    assert not is_double_sided("CFE", date(2015, 1, 5))
    # Measured across the switch: MA004 open interest 48 → 24.
    assert single_sided_count(48, exchange="CZC", trade_date=date(2019, 12, 31), field="oi") == 24
    assert single_sided_count(24, exchange="CZC", trade_date=date(2020, 1, 2), field="oi") == 24
    assert single_sided_count(2831, exchange="CFE", trade_date=date(2010, 4, 16), field="v") == 2831


def test_an_odd_double_sided_count_is_refused_not_rounded():
    with pytest.raises(CountingBasisError):
        single_sided_count(49, exchange="SHF", trade_date=date(2019, 12, 31), field="volume")


def test_turnover_is_yuan_on_one_side():
    assert single_sided_amount(12.5, exchange="CFE", trade_date=date(2019, 1, 2)) == 125_000.0
    assert single_sided_amount(12.5, exchange="SHF", trade_date=date(2019, 1, 2)) == 62_500.0
    assert single_sided_amount(None, exchange="SHF", trade_date=date(2019, 1, 2)) is None


def test_percent_iv_becomes_a_fraction():
    assert percent_to_fraction(23.9) == pytest.approx(0.239)
    assert percent_to_fraction(None) is None


def test_product_specs_answer_by_date():
    spec = product_spec("CFE", "IF", "future", date(2020, 1, 2))
    assert spec is not None and spec.multiplier == 300
    assert product_spec("CFE", "IO", "option", date(2020, 1, 2)).exercise_style == "european"
    assert product_spec("CFE", "ZZ", "future", date(2020, 1, 2)) is None
    # Revisions apply from a named delivery month onward.
    assert product_spec("SHF", "RU", "future", delivery=date(2012, 7, 1)).multiplier == 5
    assert product_spec("SHF", "RU", "future", delivery=date(2012, 8, 1)).multiplier == 10
    assert product_spec("SHF", "FU", "future", delivery=date(2015, 1, 1)).multiplier == 50
    assert product_spec("SHF", "FU", "future", delivery=date(2019, 1, 1)).multiplier == 10
    assert (
        product_spec("SHF", "CU", "option", delivery=date(2022, 10, 1)).exercise_style == "european"
    )
    assert (
        product_spec("SHF", "CU", "option", delivery=date(2022, 11, 1)).exercise_style == "american"
    )
    assert product_spec("CZC", "TA", "option", delivery=date(2024, 1, 1)).multiplier == 5
