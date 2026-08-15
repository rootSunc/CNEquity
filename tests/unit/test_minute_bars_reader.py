"""load() semantics for intraday bars: adjustment, ordering, windows."""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.domain.schemas import with_provenance
from cn_market_lake.query.reader import list_datasets, load

DAY = date(2026, 7, 31)
PREV = date(2026, 7, 30)


@pytest.fixture
def cfg(tmp_path):
    config = Config(data_root=tmp_path / "lake")
    config.curated_root.mkdir(parents=True, exist_ok=True)
    config.derived_root.mkdir(parents=True, exist_ok=True)
    return config


def _write_minute_bars(cfg: Config, rows: list[dict]):
    df = with_provenance(pl.DataFrame(rows), source="tdx_protocol", data_version="v1")
    for key, part in df.partition_by("trade_date", as_dict=True).items():
        value = (key[0] if isinstance(key, tuple) else key).isoformat()
        out = cfg.curated_root / "minute_bars" / f"trade_date={value}"
        out.mkdir(parents=True, exist_ok=True)
        part.write_parquet(out / "part-merged.parquet")


def _bar(symbol: str, stamp: datetime, close: float = 10.0) -> dict:
    return {
        "symbol": symbol,
        "trade_date": stamp.date(),
        "bar_time": stamp,
        "frequency": "1m",
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 100,
        "amount": close * 100,
    }


def _write_adj_factors(cfg: Config, rows: list[dict]):
    df = with_provenance(pl.DataFrame(rows), source="sina", data_version="v1")
    for key, part in df.partition_by("trade_date", as_dict=True).items():
        value = (key[0] if isinstance(key, tuple) else key).isoformat()
        out = cfg.derived_root / "adj_factors" / f"trade_date={value}"
        out.mkdir(parents=True, exist_ok=True)
        part.write_parquet(out / "part-merged.parquet")


def test_rows_sort_by_symbol_then_timestamp(cfg):
    # Written out of order on purpose: trade_date alone cannot order a session.
    _write_minute_bars(
        cfg,
        [
            _bar("600519.SH", datetime(2026, 7, 31, 15, 0)),
            _bar("000001.SZ", datetime(2026, 7, 31, 9, 31)),
            _bar("600519.SH", datetime(2026, 7, 31, 9, 31)),
            _bar("000001.SZ", datetime(2026, 7, 31, 14, 59)),
        ],
    )
    df = load("minute_bars", config=cfg)
    assert df["symbol"].to_list() == [
        "000001.SZ",
        "000001.SZ",
        "600519.SH",
        "600519.SH",
    ]
    assert df["bar_time"].to_list() == [
        datetime(2026, 7, 31, 9, 31),
        datetime(2026, 7, 31, 14, 59),
        datetime(2026, 7, 31, 9, 31),
        datetime(2026, 7, 31, 15, 0),
    ]


def test_date_window_filters_on_trade_date(cfg):
    _write_minute_bars(
        cfg,
        [
            _bar("600519.SH", datetime(2026, 7, 30, 9, 31)),
            _bar("600519.SH", datetime(2026, 7, 31, 9, 31)),
        ],
    )
    df = load("minute_bars", start=DAY, end=DAY, config=cfg)
    assert df.height == 1
    assert df["trade_date"].to_list() == [DAY]


def test_hfq_adjustment_applies_the_days_factor_to_every_bar(cfg):
    _write_minute_bars(
        cfg,
        [
            _bar("600519.SH", datetime(2026, 7, 30, 9, 31), close=10.0),
            _bar("600519.SH", datetime(2026, 7, 30, 15, 0), close=11.0),
            _bar("600519.SH", datetime(2026, 7, 31, 9, 31), close=12.0),
        ],
    )
    _write_adj_factors(
        cfg,
        [
            {"symbol": "600519.SH", "trade_date": PREV, "adjust_type": "hfq", "factor": 2.0},
            {"symbol": "600519.SH", "trade_date": DAY, "adjust_type": "hfq", "factor": 4.0},
        ],
    )

    df = load("minute_bars", adjust="hfq", config=cfg).sort("bar_time")

    # A corporate action applies to a whole session, so both of the 07-30 bars
    # take that day's factor — the join is on (symbol, trade_date).
    assert df["adj_close"].to_list() == [20.0, 22.0, 48.0]
    assert df["adj_is_exact"].to_list() == [True, True, True]


def test_qfq_anchors_on_the_latest_bar_date(cfg):
    _write_minute_bars(
        cfg,
        [
            _bar("600519.SH", datetime(2026, 7, 30, 9, 31), close=10.0),
            _bar("600519.SH", datetime(2026, 7, 31, 9, 31), close=12.0),
        ],
    )
    _write_adj_factors(
        cfg,
        [
            {"symbol": "600519.SH", "trade_date": PREV, "adjust_type": "hfq", "factor": 2.0},
            {"symbol": "600519.SH", "trade_date": DAY, "adjust_type": "hfq", "factor": 4.0},
        ],
    )

    df = load("minute_bars", adjust="qfq", config=cfg).sort("bar_time")

    # factor / anchor, anchor = 4.0 at the latest date in scope.
    assert df["adj_close"].to_list() == [5.0, 12.0]


def test_missing_factors_are_marked_inexact_not_silently_scaled(cfg):
    _write_minute_bars(cfg, [_bar("600519.SH", datetime(2026, 7, 31, 9, 31), close=12.0)])
    _write_adj_factors(
        cfg,
        [{"symbol": "000001.SZ", "trade_date": DAY, "adjust_type": "hfq", "factor": 3.0}],
    )
    df = load("minute_bars", adjust="hfq", config=cfg)
    assert df["adj_is_exact"].to_list() == [False]
    assert df["adj_close"].to_list() == [12.0]


def test_catalog_reports_the_source_horizon(cfg):
    catalog = list_datasets(config=cfg)
    row = catalog.filter(pl.col("dataset") == "minute_bars").to_dicts()[0]
    assert row["history_horizon_days"] == 95
    assert row["history_mode"] == "by_date"
    # Unbounded sources must stay None rather than reporting a made-up ceiling.
    daily = catalog.filter(pl.col("dataset") == "daily_bars").to_dicts()[0]
    assert daily["history_horizon_days"] is None
