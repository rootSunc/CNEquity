"""Code and calendar rules that used to be copied across adapters live in `domain`."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.domain.bse_codes import current_to_legacy, legacy_symbol_map, legacy_to_current
from cnequity.domain.holidays_cn import (
    CLOSED_DATES,
    exchange_may_open,
    exchange_may_open_expr,
    session_expr,
)
from cnequity.domain.symbols import (
    from_baostock_symbol,
    infer_exchange_from_code,
    is_baostock_stock,
    to_baostock_symbol,
)


def test_the_bse_table_is_one_to_one_and_not_a_suffix_rule():
    forward = legacy_to_current()
    assert len(forward) == len(current_to_legacy()) == 248
    # Pairs a "keep the last four digits" guess would get wrong.
    assert forward["870726"] == "920926"
    assert forward["873305"] == "920505"
    assert current_to_legacy()["920454"] == "833454"
    assert legacy_symbol_map()["833454.BJ"] == "920454.BJ"


def test_baostock_serves_sh_sz_stocks_only():
    assert is_baostock_stock("600519.SH") and is_baostock_stock("688981.SH")
    assert is_baostock_stock("000001.SZ") and is_baostock_stock("300750.SZ")
    for symbol in ("000001.SH", "510300.SH", "920454.BJ", "159915.SZ", "garbage"):
        assert not is_baostock_stock(symbol), symbol


def test_baostock_codes_round_trip_including_beijing():
    for symbol in ("600519.SH", "000001.SZ", "920454.BJ"):
        assert from_baostock_symbol(to_baostock_symbol(symbol)) == symbol
    assert from_baostock_symbol("hk.00700") is None
    assert from_baostock_symbol("sh.6005") is None


def test_index_notice_codes_get_the_shared_exchange_rule():
    """Index notices wrote `"SH" if code.startswith("6") else "SZ"`: a BJ code read as SZ."""
    assert infer_exchange_from_code("920454") == "BJ"
    assert infer_exchange_from_code("600519") == "SH"
    assert infer_exchange_from_code("000001") == "SZ"


def test_session_rules_agree_scalar_and_vectorised():
    closed = date.fromisoformat(sorted(CLOSED_DATES)[0])
    days = [date(2024, 10, 1), date(2024, 10, 8), date(2024, 10, 12), closed]
    frame = pl.DataFrame({"trade_date": days, "is_trading": [True] * len(days)})
    vector = frame.select(exchange_may_open_expr())["trade_date"].to_list()
    assert vector == [exchange_may_open(day) for day in days] == [False, True, False, False]
    # A vendor's is_trading row on a closed day is still not a session.
    assert frame.filter(session_expr())["trade_date"].to_list() == [date(2024, 10, 8)]
