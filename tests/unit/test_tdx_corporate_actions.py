"""Offline coverage for TDX xdxr → corporate_actions normalization."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd
import polars as pl

from cn_market_lake.adapters.tdx_protocol import corporate_actions as ca


def test_rows_from_xdxr_cash_bonus_allotment_and_skips():
    pdf = pl.DataFrame(
        [
            # skipped: incomplete date
            {
                "year": 2024,
                "month": 6,
                "day": None,
                "category": 1,
                "fenhong": 10.0,
                "songzhuangu": 0,
                "peigu": 0,
                "peigujia": 0,
            },
            # skipped: non-dividend category
            {
                "year": 2024,
                "month": 6,
                "day": 27,
                "category": 0,
                "fenhong": 10.0,
                "songzhuangu": 0,
                "peigu": 0,
                "peigujia": 0,
            },
            # cash + bonus + allotment same day
            {
                "year": 2024,
                "month": 6,
                "day": 28,
                "category": 1,
                "fenhong": 10.0,
                "songzhuangu": 5.0,
                "peigu": 3.0,
                "peigujia": 8.5,
            },
        ]
    )
    rows = ca._rows_from_xdxr("600519.SH", pdf)
    assert len(rows) == 3
    by_type = {r["action_type"]: r for r in rows}
    assert by_type["cash_dividend"]["cash_dividend"] == 1.0
    assert by_type["bonus"]["bonus_ratio"] == 0.5
    assert by_type["allotment"]["allotment_ratio"] == 0.3
    assert by_type["allotment"]["allotment_price"] == 8.5
    assert all(r["ex_date"] == date(2024, 6, 28) for r in rows)


def test_fetch_xdxr_for_symbol_empty_and_filter(monkeypatch):
    class Boom:
        def xdxr(self, symbol, market=None):
            raise RuntimeError("offline")

    assert ca.fetch_xdxr_for_symbol(Boom(), "600519.SH").is_empty()

    class Empty:
        def xdxr(self, symbol, market=None):
            return None

    assert ca.fetch_xdxr_for_symbol(Empty(), "600519.SH").is_empty()

    class Ok:
        def xdxr(self, symbol, market=None):
            return pd.DataFrame(
                [
                    {
                        "year": 2024,
                        "month": 6,
                        "day": 28,
                        "category": 1,
                        "fenhong": 10.0,
                        "songzhuangu": 0,
                        "peigu": 0,
                        "peigujia": 0,
                    },
                    {
                        "year": 2023,
                        "month": 6,
                        "day": 28,
                        "category": 1,
                        "fenhong": 5.0,
                        "songzhuangu": 0,
                        "peigu": 0,
                        "peigujia": 0,
                    },
                ]
            )

    monkeypatch.setattr(ca, "wait_spec", lambda *a, **k: None)
    df = ca.fetch_xdxr_for_symbol(Ok(), "600519.SH", on_date=date(2024, 6, 28))
    assert df.height == 1
    assert df["ex_date"].to_list() == [date(2024, 6, 28)]


def test_fetch_xdxr_for_symbol_resolves_bj_to_market_2(monkeypatch):
    """Regression: BJ symbols silently queried market=0 (深圳) and got nothing.

    ``quotes.xdxr()`` falls back to ``market_for_stock()`` when no market is
    given, and that heuristic only distinguishes SH/SZ — it has no notion of
    北交所. Every BJ symbol therefore queried the wrong market and came back
    with an empty (not erroring) result, which is indistinguishable from "this
    stock has no corporate actions". Verified live: market=0 returned 0 events
    for every BJ code sampled; market=2 returned real ones for the same codes
    (920002.BJ: 15, 920014.BJ: 34, ...). ``fetch_bars_paginated`` already
    resolves BJ to market=2 correctly for daily bars — this applies the same
    resolution to xdxr.
    """
    monkeypatch.setattr(ca, "wait_spec", lambda *a, **k: None)
    seen = {}

    class Client:
        def xdxr(self, symbol, market=None):
            seen["symbol"] = symbol
            seen["market"] = market
            return None

    ca.fetch_xdxr_for_symbol(Client(), "920055.BJ")
    assert seen == {"symbol": "920055", "market": 2}


def test_fetch_xdxr_for_symbol_still_resolves_sh_sz(monkeypatch):
    monkeypatch.setattr(ca, "wait_spec", lambda *a, **k: None)
    seen = []

    class Client:
        def xdxr(self, symbol, market=None):
            seen.append((symbol, market))
            return None

    ca.fetch_xdxr_for_symbol(Client(), "600519.SH")
    ca.fetch_xdxr_for_symbol(Client(), "000001.SZ")
    assert seen == [("600519", 1), ("000001", 0)]


def test_fetch_corporate_actions_tdx_dedupes(monkeypatch):
    monkeypatch.setattr(ca, "wait_spec", lambda *a, **k: None)
    monkeypatch.setattr(ca, "close_quotes_client", lambda client: None)

    class Client:
        def xdxr(self, symbol, market=None):
            return pd.DataFrame(
                [
                    {
                        "year": 2024,
                        "month": 6,
                        "day": 28,
                        "category": 1,
                        "fenhong": 10.0,
                        "songzhuangu": 0,
                        "peigu": 0,
                        "peigujia": 0,
                    }
                ]
            )

    out = ca.fetch_corporate_actions_tdx(
        ["600519.SH", "600519.SH"],
        trade_date=date(2024, 6, 28),
        backfill=False,
        client_factory=Client,
    )
    assert out.height == 1

    empty = ca.fetch_corporate_actions_tdx(
        ["000001.SZ"],
        trade_date=date(2024, 6, 28),
        client_factory=lambda: SimpleNamespace(xdxr=lambda symbol: None),
    )
    assert empty.is_empty()
    assert "action_type" in empty.schema
