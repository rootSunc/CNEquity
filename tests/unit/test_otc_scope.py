from datetime import date, datetime, timezone

import duckdb
import polars as pl

from cnequity.config import Config
from cnequity.query import load
from cnequity.query.views import ensure_duckdb_views

FETCHED = datetime(2026, 9, 30, tzinfo=timezone.utc)
LISTED = date(2021, 6, 1)
BJ_DAYS = [date(2019, 6, 3), date(2021, 5, 31), LISTED, date(2022, 1, 4)]
SH_DAYS = [date(2019, 6, 3), date(2022, 1, 4)]


def _lake(tmp_path) -> Config:
    cfg = Config(data_root=tmp_path)
    rows = [("920001.BJ", d) for d in BJ_DAYS] + [("600519.SH", d) for d in SH_DAYS]
    for symbol, day in rows:
        part = cfg.curated_root / "daily_bars" / f"trade_date={day}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": [symbol],
                "trade_date": [day],
                "open": [10.0],
                "high": [10.0],
                "low": [10.0],
                "close": [10.0],
                "volume": [100],
                "amount": [1000.0],
                "source": ["tdx_protocol"],
                "data_version": ["v1"],
                "fetched_at": [FETCHED],
            }
        ).write_parquet(part / f"{symbol}.parquet")
    inst = cfg.curated_root / "instruments"
    inst.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["920001.BJ", "600519.SH"],
            "name": ["A", "B"],
            "exchange": ["BJ", "SH"],
            "asset_type": ["stock", "stock"],
            "list_date": [LISTED, date(2001, 8, 27)],
            "delist_date": [None, None],
            "prev_symbol": [None, None],
            "source": ["bse", "tdx"],
            "data_version": ["v1", "v1"],
            "fetched_at": [FETCHED, FETCHED],
        },
        schema_overrides={"delist_date": pl.Date, "prev_symbol": pl.Utf8},
    ).write_parquet(inst / "part-merged.parquet")
    return cfg


def _keys(frame: pl.DataFrame) -> list[tuple]:
    return sorted(frame.select("symbol", "trade_date").rows())


def test_default_reads_leave_out_beijing_quotes_from_before_the_exchange(tmp_path):
    cfg = _lake(tmp_path)
    assert _keys(load("daily_bars", config=cfg)) == [
        ("600519.SH", SH_DAYS[0]),
        ("600519.SH", SH_DAYS[1]),
        ("920001.BJ", LISTED),
        ("920001.BJ", BJ_DAYS[3]),
    ]
    assert len(load("daily_bars", config=cfg, include_otc=True)) == 6


def test_the_universe_starts_a_beijing_security_at_its_exchange_listing(tmp_path):
    cfg = _lake(tmp_path)
    bj = load("daily_bars", config=cfg, profile="legacy_all_a", include_otc=True).filter(
        pl.col("symbol") == "920001.BJ"
    )
    assert bj["trade_date"].to_list() == [LISTED, BJ_DAYS[3]]


def test_sql_views_match_the_python_default(tmp_path):
    cfg = _lake(tmp_path)
    path = ensure_duckdb_views(cfg)
    with duckdb.connect(str(path), read_only=True) as con:
        public = con.execute("SELECT symbol, trade_date FROM daily_bars").fetchall()
        everything = con.execute("SELECT count(*) FROM daily_bars_including_otc").fetchone()[0]
    assert sorted(public) == _keys(load("daily_bars", config=cfg))
    assert everything == 6


def test_factor_checks_judge_beijing_only_from_its_exchange_start(tmp_path):
    from cnequity.quality.cross_checks import factor_action_contradictions

    cfg = _lake(tmp_path)
    for day in BJ_DAYS:
        part = cfg.derived_root / "adj_factors" / f"trade_date={day}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": ["920001.BJ"],
                "trade_date": [day],
                "adjust_type": ["hfq"],
                "factor": [1.0],  # never steps
                "source": ["sina"],
                "data_version": ["v1"],
                "fetched_at": [FETCHED],
            }
        ).write_parquet(part / "part-0.parquet")
    actions = cfg.curated_root / "corporate_actions" / "ex_date=2021"
    actions.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["920001.BJ", "920001.BJ"],
            # A NEEQ-era dividend, and one after the exchange listing.
            "ex_date": [BJ_DAYS[1], BJ_DAYS[3]],
            "action_type": ["cash_dividend", "cash_dividend"],
            "cash_dividend": [0.1, 0.1],
            "bonus_ratio": [0.0, 0.0],
            "transfer_ratio": [0.0, 0.0],
            "allotment_ratio": [None, None],
            "allotment_price": [None, None],
            "source": ["tdx_protocol", "tdx_protocol"],
            "data_version": ["v1", "v1"],
            "fetched_at": [FETCHED, FETCHED],
        },
        schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
    ).write_parquet(actions / "part-0.parquet")

    contradictions, _ = factor_action_contradictions(cfg)
    assert contradictions.select("symbol", "ex_date").rows() == [("920001.BJ", BJ_DAYS[3])]
