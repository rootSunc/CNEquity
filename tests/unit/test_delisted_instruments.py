"""Delisted-symbol recovery: baostock basics adapter + instruments merge."""

from datetime import date

import polars as pl
import pytest

from cn_market_lake.adapters.baostock.instruments import fetch_instrument_basics
from cn_market_lake.config import Config
from cn_market_lake.steps.delisted import known_delisted_instruments
from cn_market_lake.steps.reference import _merge_delisted_instruments
from cn_market_lake.storage.instruments import compact_instruments
from cn_market_lake.storage.parquet import StagingWriter


class _FakeResultSet:
    """Mimics baostock's cursor: next() then get_row_data()."""

    def __init__(self, rows, error_code="0", error_msg=""):
        self._rows = list(rows)
        self._i = -1
        self.error_code = error_code
        self.error_msg = error_msg

    def next(self):
        self._i += 1
        return self._i < len(self._rows)

    def get_row_data(self):
        return list(self._rows[self._i])


class _FakeBaostock:
    def __init__(self, rows, error_code="0"):
        self._rows = rows
        self._error_code = error_code
        self.logged_out = False

    def login(self):
        return type("R", (), {"error_code": "0", "error_msg": ""})()

    def logout(self):
        self.logged_out = True

    def query_stock_basic(self):
        return _FakeResultSet(self._rows, error_code=self._error_code)


# code, code_name, ipoDate, outDate, type, status
_ROWS = [
    ("sh.600519", "贵州茅台", "2001-08-27", "", "1", "1"),
    ("sz.000001", "平安银行", "1991-04-03", "", "1", "1"),
    ("sh.600001", "邯郸钢铁", "1998-01-22", "2009-12-25", "1", "0"),
    ("sz.000003", "PT金田A", "1991-01-14", "2002-06-14", "1", "0"),
    ("sh.510300", "沪深300ETF", "2012-05-28", "", "5", "1"),
    ("sh.000001", "上证综指", "1991-07-15", "", "2", "1"),  # index — not modelled
    ("bj.430047", "诺思兰德", "2014-01-24", "", "1", "1"),
]


# --- adapter ----------------------------------------------------------------


def test_fetch_basics_maps_symbols_dates_and_asset_types():
    df = fetch_instrument_basics(bs=_FakeBaostock(_ROWS), sleep=lambda _s: None)

    by_symbol = {r["symbol"]: r for r in df.iter_rows(named=True)}
    assert "000001.SH" not in by_symbol  # index dropped by type
    assert by_symbol["600519.SH"]["asset_type"] == "stock"
    assert by_symbol["510300.SH"]["asset_type"] == "etf"
    assert by_symbol["430047.BJ"]["exchange"] == "BJ"
    assert by_symbol["600519.SH"]["list_date"] == date(2001, 8, 27)


def test_fetch_basics_sets_delist_date_only_for_delisted_rows():
    df = fetch_instrument_basics(bs=_FakeBaostock(_ROWS), sleep=lambda _s: None)

    delisted = df.filter(pl.col("delist_date").is_not_null())
    assert set(delisted["symbol"]) == {"600001.SH", "000003.SZ"}
    assert df.filter(pl.col("symbol") == "600519.SH")["delist_date"].item() is None


def test_fetch_basics_raises_on_query_error():
    bs = _FakeBaostock(_ROWS, error_code="10001")
    with pytest.raises(RuntimeError, match="query_stock_basic failed"):
        fetch_instrument_basics(bs=bs, sleep=lambda _s: None)
    assert bs.logged_out


# --- merge into the live snapshot -------------------------------------------


def _live_snapshot(symbols, list_dates=None):
    list_dates = list_dates or {}
    return pl.DataFrame(
        {
            "symbol": symbols,
            "name": [s for s in symbols],
            "exchange": [s.split(".")[1] for s in symbols],
            "asset_type": ["stock"] * len(symbols),
            "list_date": pl.Series([list_dates.get(s) for s in symbols], dtype=pl.Date),
            "delist_date": pl.Series([None] * len(symbols), dtype=pl.Date),
            "prev_symbol": [None] * len(symbols),
            "source": ["tdx_protocol"] * len(symbols),
            "data_version": ["v1"] * len(symbols),
            "fetched_at": [None] * len(symbols),
        }
    )


def _config_with_baostock(tmp_path, enabled=True):
    return Config(data_root=tmp_path / "data", sources={"baostock": enabled})


def test_merge_appends_delisted_and_fills_list_dates(tmp_path, monkeypatch):
    cfg = _config_with_baostock(tmp_path)
    monkeypatch.setattr(
        "cn_market_lake.adapters.baostock.instruments.import_baostock",
        lambda: _FakeBaostock(_ROWS),
    )
    live = _live_snapshot(["600519.SH", "000001.SZ"])

    merged = _merge_delisted_instruments(cfg, live)

    assert set(merged["symbol"]) == {"600519.SH", "000001.SZ", "600001.SH", "000003.SZ"}
    dead = merged.filter(pl.col("symbol") == "600001.SH")
    assert dead["delist_date"].item() == date(2009, 12, 25)
    assert dead["source"].item() == "baostock"
    # list_date backfilled onto the still-listed name from baostock's ipoDate.
    assert merged.filter(pl.col("symbol") == "600519.SH")["list_date"].item() == date(2001, 8, 27)
    assert known_delisted_instruments(cfg, date(2026, 1, 1)) == {
        "000003.SZ": date(2002, 6, 14),
        "600001.SH": date(2009, 12, 25),
    }


def test_inferred_instrument_delist_date_is_not_formal_identity(tmp_path):
    cfg = _config_with_baostock(tmp_path)
    path = cfg.curated_root / "instruments" / "part-merged.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    _live_snapshot(["600001.SH"]).with_columns(
        pl.lit(date(2009, 12, 25), dtype=pl.Date).alias("delist_date")
    ).write_parquet(path)

    assert known_delisted_instruments(cfg, date(2026, 1, 1)) == {}


def test_merge_skips_names_baostock_calls_listed_but_snapshot_omits(tmp_path, monkeypatch):
    """Ambiguous rows must not inject untradable symbols into all_a."""
    cfg = _config_with_baostock(tmp_path)
    monkeypatch.setattr(
        "cn_market_lake.adapters.baostock.instruments.import_baostock",
        lambda: _FakeBaostock(_ROWS),
    )
    live = _live_snapshot(["600519.SH"])

    merged = _merge_delisted_instruments(cfg, live)

    # 000001.SZ is listed per baostock but absent from the snapshot — not added.
    assert "000001.SZ" not in set(merged["symbol"])
    assert {"600001.SH", "000003.SZ"} <= set(merged["symbol"])


def test_merge_is_a_noop_when_baostock_disabled(tmp_path):
    cfg = _config_with_baostock(tmp_path, enabled=False)
    live = _live_snapshot(["600519.SH"])

    assert _merge_delisted_instruments(cfg, live).equals(live)


# --- compact keeps the recovered delist_date --------------------------------


def _stage(cfg, run_id, df):
    StagingWriter(cfg.staging_root).write_batch("instruments", run_id, "batch-0", df)


def test_compact_preserves_explicit_delist_date(tmp_path):
    """A baostock delist_date must survive compact, not be blanket-nulled."""
    cfg = Config(data_root=tmp_path / "data")
    df = _live_snapshot(["600519.SH", "600001.SH"]).with_columns(
        pl.Series("delist_date", [None, date(2009, 12, 25)], dtype=pl.Date)
    )
    _stage(cfg, "run-1", df)

    compact_instruments(cfg.staging_root, cfg.curated_root, "run-1", date(2026, 7, 21))

    out = pl.read_parquet(cfg.curated_root / "instruments" / "part-merged.parquet")
    assert out.filter(pl.col("symbol") == "600001.SH")["delist_date"].item() == date(2009, 12, 25)
    assert out.filter(pl.col("symbol") == "600519.SH")["delist_date"].item() is None


def test_compact_keeps_delist_date_sticky_across_runs(tmp_path):
    """A later live snapshot carries no delist_date; it must not resurrect a name."""
    cfg = Config(data_root=tmp_path / "data")
    first = _live_snapshot(["600519.SH", "600001.SH"]).with_columns(
        pl.Series("delist_date", [None, date(2009, 12, 25)], dtype=pl.Date)
    )
    _stage(cfg, "run-1", first)
    compact_instruments(cfg.staging_root, cfg.curated_root, "run-1", date(2026, 7, 21))

    # Second run re-reports both symbols with no delist information at all.
    _stage(cfg, "run-2", _live_snapshot(["600519.SH", "600001.SH"]))
    compact_instruments(cfg.staging_root, cfg.curated_root, "run-2", date(2026, 7, 22))

    out = pl.read_parquet(cfg.curated_root / "instruments" / "part-merged.parquet")
    assert out.filter(pl.col("symbol") == "600001.SH")["delist_date"].item() == date(2009, 12, 25)
