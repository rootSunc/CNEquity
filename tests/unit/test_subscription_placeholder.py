"""Subscription / allotment stubs must not enter instruments as securities."""

from datetime import date

import polars as pl

from cn_market_lake.adapters.tdx_protocol.client import _filter_instrument_frame
from cn_market_lake.domain.symbols import is_subscription_placeholder
from cn_market_lake.steps.delisted import _strip_subscription_placeholders


def test_recognises_exact_and_suffixed_placeholder_names():
    assert is_subscription_placeholder("认购款")
    assert is_subscription_placeholder("申购款")
    assert is_subscription_placeholder(" 认购款 ")
    assert is_subscription_placeholder("某某认购款")
    assert not is_subscription_placeholder("贵州茅台")
    assert not is_subscription_placeholder(None)
    assert not is_subscription_placeholder("")


def test_tdx_instrument_filter_drops_placeholder_rows():
    pdf = pl.DataFrame(
        {
            "code": ["510300", "515844", "600519"],
            "name": ["沪深300ETF", "认购款", "贵州茅台"],
        }
    )

    out = _filter_instrument_frame(pdf, "SH")

    assert set(out["symbol"]) == {"510300.SH", "600519.SH"}
    assert "认购款" not in out["name"].to_list()


def test_strip_removes_placeholder_rows_from_instruments_frame():
    df = pl.DataFrame(
        {
            "symbol": ["600519.SH", "515844.SH", "000001.SZ"],
            "name": ["贵州茅台", "认购款", "平安银行"],
            "exchange": ["SH", "SH", "SZ"],
            "asset_type": ["stock", "etf", "stock"],
            "list_date": [date(2001, 8, 27), None, date(1991, 4, 3)],
            "delist_date": [None, date(2026, 7, 21), None],
            "prev_symbol": [None, None, None],
        }
    )

    cleaned = _strip_subscription_placeholders(df)

    assert set(cleaned["symbol"]) == {"600519.SH", "000001.SZ"}
