"""Offline tests for SSE/SZSE ETF options from Tushare."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.tushare import etf_options as eo

BASIC = pl.DataFrame(
    {
        "ts_code": ["10011425.SH", "90008464.SZ"],
        "symbol": ["510050C2611M02800", "159915C2703M002300"],
        "exchange": ["SSE", "SZSE"],
        "per_unit": [10000.0, 10000.0],
        "opt_code": ["OP510050.SH", "OP159915.SZ"],
        "call_put": ["C", "C"],
        "exercise_type": ["欧式", "欧式"],
        "exercise_price": [2.8, 2.3],
        "s_month": ["202611", "202703"],
        "maturity_date": ["20261125", "20270324"],
        "list_date": ["20260302", "20261102"],
        "min_price_chg": [0.0001, 0.0001],
    }
)


def test_contracts_carry_the_etf_as_underlying():
    c = eo.contracts_from_basic(BASIC)
    row = c.filter(pl.col("symbol") == "10011425.SH").row(0, named=True)
    assert row["exchange"] == "SH"
    assert row["underlying_symbol"] == "510050.SH"
    assert row["product"] == "510050"
    assert row["exercise_style"] == "european"
    assert row["expiry_date"] == date(2026, 11, 25)
    assert row["expiry_month"] == date(2026, 11, 1)
    assert row["multiplier"] == 10000.0
    from cnequity.domain.schemas import validate_dataframe, with_provenance

    validate_dataframe(with_provenance(c, source="tushare", data_version="v1"), "option_contracts")


def test_daily_bars_convert_amount_and_join_terms():
    c = eo.contracts_from_basic(BASIC)
    names = [
        "ts_code",
        "trade_date",
        "exchange",
        "pre_settle",
        "pre_close",
        "open",
        "high",
        "low",
        "close",
        "settle",
        "vol",
        "amount",
        "oi",
    ]
    items = [
        [
            "10011425.SH",
            "20261104",
            "SSE",
            0.10,
            0.10,
            0.10,
            0.14,
            0.09,
            0.13,
            0.13,
            5228.0,
            620.1161,
            16972.0,
        ],
        ["10099999.SH", "20261104", "SSE", 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 1.0, 1.0, 1.0],
    ]
    bars = eo.normalize_daily(names, items, c)
    assert bars.height == 1  # a code with no contract terms is not written
    row = bars.row(0, named=True)
    assert row["amount"] == 6_201_161.0
    assert row["volume"] == 5228 and row["open_interest"] == 16972
    assert row["underlying_symbol"] == "510050.SH" and row["strike"] == 2.8
