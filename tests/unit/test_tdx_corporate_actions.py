"""Offline coverage for TDX xdxr → corporate_actions normalization."""

from __future__ import annotations

from datetime import date, datetime, timezone
from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest

from cnequity.adapters.tdx_protocol import corporate_actions as ca
from cnequity.config import Config
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive


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
            # skipped: malformed ex-date must not abort the valid row below
            {
                "year": 2024,
                "month": 13,
                "day": 28,
                "category": 1,
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


def test_rows_from_xdxr_rejects_nonfinite_numeric_fields():
    pdf = pl.DataFrame(
        [
            {
                "year": 2024,
                "month": 6,
                "day": 28,
                "category": 1,
                "fenhong": 1.0,
                "songzhuangu": 0,
                "peigu": 1.0,
                "peigujia": "nan",
            }
        ]
    )
    rows = ca._rows_from_xdxr("600519.SH", pdf)
    allotment = next(row for row in rows if row["action_type"] == "allotment")
    assert allotment["allotment_price"] is None


def test_rows_from_xdxr_does_not_turn_malformed_event_amount_into_zero():
    pdf = pl.DataFrame(
        [
            {
                "year": 2024,
                "month": 6,
                "day": 28,
                "category": 1,
                "fenhong": "not-a-number",
                "songzhuangu": 0,
                "peigu": 0,
                "peigujia": 0,
            }
        ]
    )
    assert ca._rows_from_xdxr("600519.SH", pdf) == []


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


def test_fetch_xdxr_for_symbol_dedupes_duplicate_action_rows(monkeypatch):
    monkeypatch.setattr(ca, "wait_spec", lambda *a, **k: None)

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
                    },
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
                ]
            )

    df = ca.fetch_xdxr_for_symbol(Client(), "600519.SH")
    assert df.height == 1


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


def test_fetch_corporate_actions_tdx_paces_and_reports_each_symbol(monkeypatch):
    waits = []
    progress = []
    monkeypatch.setattr(ca, "wait_spec", lambda *a, **k: waits.append(True))
    monkeypatch.setattr(ca, "close_quotes_client", lambda client: None)

    class Client:
        def xdxr(self, symbol, market=None):
            return None

    ca.fetch_corporate_actions_tdx(
        ["600519.SH", "000001.SZ", "920055.BJ"],
        client_factory=Client,
        on_progress=lambda done, total: progress.append((done, total)),
    )

    assert len(waits) == 3
    assert progress == [(1, 3), (2, 3), (3, 3)]


def test_fetch_corporate_actions_tdx_strict_rejects_symbol_fetch_error(monkeypatch):
    monkeypatch.setattr(ca, "wait_spec", lambda *a, **k: None)
    monkeypatch.setattr(ca, "close_quotes_client", lambda client: None)

    class Client:
        def xdxr(self, symbol, market=None):
            raise ConnectionError("socket reset")

    with pytest.raises(RuntimeError, match="TDX xdxr failed for 600519.SH"):
        ca.fetch_corporate_actions_tdx(
            ["600519.SH"],
            trade_date=date(2024, 6, 28),
            client_factory=Client,
            strict=True,
        )


def test_fetch_corporate_actions_tdx_archives_exact_wire_per_request(tmp_path, monkeypatch):
    monkeypatch.setattr(ca, "wait_spec", lambda *a, **k: None)
    monkeypatch.setattr(ca, "close_quotes_client", lambda client: None)

    row = {
        "year": 2024,
        "month": 6,
        "day": 28,
        "category": 1,
        "fenhong": 10.0,
        "songzhuangu": 0.0,
        "peigu": 0.0,
        "peigujia": 0.0,
    }

    class Client:
        def __init__(self):
            self.last_response_wire = None

        def xdxr(self, symbol, market=None):
            self.last_response_wire = f"wire-{symbol}".encode()
            return pd.DataFrame([row])

    config = Config(data_root=tmp_path / "lake", raw_archive_compression="none")
    frame = ca.fetch_corporate_actions_tdx(
        ["600519.SH"],
        trade_date=date(2024, 6, 28),
        client_factory=Client,
        config=config,
        run_id="run-wire",
    )

    assert frame.height == 1
    records = RawPayloadArchive(config.meta_root).records("corporate_actions")
    assert len(records) == 1
    assert RawPayloadArchive(config.meta_root).read(records[0]) == b"wire-600519"
    assert records[0].run_id == "run-wire"
    assert records[0].http_metadata["wire_exact"] is True


def test_fetch_corporate_actions_tdx_captureless_archive_fails_before_rows(tmp_path, monkeypatch):
    monkeypatch.setattr(ca, "wait_spec", lambda *a, **k: None)
    monkeypatch.setattr(ca, "close_quotes_client", lambda client: None)

    class Captureless:
        def xdxr(self, symbol, market=None):
            return pd.DataFrame(
                [
                    {
                        "year": 2024,
                        "month": 6,
                        "day": 28,
                        "category": 1,
                        "fenhong": 10.0,
                        "songzhuangu": 0.0,
                        "peigu": 0.0,
                        "peigujia": 0.0,
                    }
                ]
            )

    config = Config(data_root=tmp_path / "lake", raw_archive_compression="none")
    with pytest.raises(RawArchiveError, match="no exact wire bytes"):
        ca.fetch_corporate_actions_tdx(
            ["600519.SH"],
            trade_date=date(2024, 6, 28),
            client_factory=Captureless,
            config=config,
            run_id="run-missing-wire",
        )
    assert not (config.meta_root / "raw").exists()


def test_fund_unit_split_becomes_an_action_and_non_tradable_contraction_does_not():
    """A 份额折算 restates every quoted price, so it is an ex-event.

    159327.SZ went 3.348 → 1.052 on 2026-07-20 with nothing on record: the
    adapter kept category 1 only, so the one source that serves the ratio
    (xdxr category 11, ``suogu=3.0``) was dropped and the audit carried a
    permanent unexplained-divergence warning. Category 12 restates
    non-tradable shares only and must stay out — adjusting a traded price by
    it would corrupt the series it does not touch.
    """
    pdf = pl.DataFrame(
        [
            {
                "year": 2026,
                "month": 7,
                "day": 20,
                "category": 11,
                "fenhong": None,
                "songzhuangu": None,
                "peigu": None,
                "peigujia": None,
                "suogu": 3.0,
            },
            {
                "year": 2026,
                "month": 7,
                "day": 21,
                "category": 12,
                "fenhong": None,
                "songzhuangu": None,
                "peigu": None,
                "peigujia": None,
                "suogu": 0.5,
            },
            # no usable ratio: inventing one would restate the whole series
            {
                "year": 2026,
                "month": 7,
                "day": 22,
                "category": 11,
                "fenhong": None,
                "songzhuangu": None,
                "peigu": None,
                "peigujia": None,
                "suogu": 1.0,
            },
        ]
    )

    rows = ca._rows_from_xdxr("159327.SZ", pdf)

    assert rows == [
        {
            "symbol": "159327.SZ",
            "ex_date": date(2026, 7, 20),
            "action_type": "unit_split",
            "cash_dividend": 0.0,
            "bonus_ratio": 0.0,
            "transfer_ratio": 0.0,
            "allotment_ratio": None,
            "allotment_price": None,
            "split_factor": 3.0,
        }
    ]


def test_a_unit_split_row_passes_the_corporate_action_contract():
    """The schema rejects a split without a ratio, and a dividend with one."""
    from cnequity.domain.schemas import validate_dataframe

    pdf = pl.DataFrame(
        [
            {
                "year": 2026,
                "month": 7,
                "day": 20,
                "category": 11,
                "fenhong": None,
                "songzhuangu": None,
                "peigu": None,
                "peigujia": None,
                "suogu": 3.0,
            },
            {
                "year": 2024,
                "month": 6,
                "day": 28,
                "category": 1,
                "fenhong": 10.0,
                "songzhuangu": 0,
                "peigu": 0,
                "peigujia": 0,
                "suogu": None,
            },
        ]
    )

    frame = pl.DataFrame(ca._rows_from_xdxr("159327.SZ", pdf)).with_columns(
        pl.lit("tdx_protocol").alias("source"),
        pl.lit("v1").alias("data_version"),
        pl.lit(datetime(2026, 7, 21, tzinfo=timezone.utc)).alias("fetched_at"),
    )

    validated = validate_dataframe(frame, "corporate_actions")

    assert validated.sort("ex_date")["split_factor"].to_list() == [1.0, 3.0]
