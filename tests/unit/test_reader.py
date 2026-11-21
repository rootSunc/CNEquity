from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.query.reader import MissingAdjustmentError, ReaderError, load, resolve_config
from cnequity.query.universe import apply_universe_filter


def _prov(source: str = "test") -> dict:
    return {
        "source": source,
        "data_version": "v1",
        "fetched_at": datetime(2024, 6, 28, tzinfo=timezone.utc),
    }


@pytest.fixture
def lake(tmp_path):
    root = tmp_path / "data"
    curated = root / "curated"
    derived = root / "derived"

    (curated / "instruments").mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ", "300750.SZ"],
            "name": ["Moutai", "PingAn", "CATL"],
            "exchange": ["SH", "SZ", "SZ"],
            "asset_type": ["stock", "stock", "stock"],
            "list_date": [date(2001, 8, 27), date(1991, 4, 3), date(2017, 6, 11)],
            "delist_date": [None, None, None],
            **_prov(),
        }
    ).write_parquet(curated / "instruments" / "part-merged.parquet")

    bars_dir = curated / "daily_bars" / "trade_date=2024-06-27"
    bars_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ", "300750.SZ"],
            "trade_date": [date(2024, 6, 27)] * 3,
            "open": [10.0, 20.0, 30.0],
            "high": [11.0, 21.0, 31.0],
            "low": [9.0, 19.0, 29.0],
            "close": [10.5, 20.5, 30.5],
            "volume": [1000, 2000, 3000],
            "amount": [10500.0, 41000.0, 91500.0],
            **_prov(),
        }
    ).write_parquet(bars_dir / "part-0.parquet")

    status_dir = curated / "trading_status" / "trade_date=2024-06-27"
    status_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ", "300750.SZ"],
            "trade_date": [date(2024, 6, 27)] * 3,
            "is_trading": [True, True, False],
            "status": ["normal", "st", "suspended"],
            **_prov("eastmoney"),
        }
    ).write_parquet(status_dir / "part-0.parquet")

    adj_dir = derived / "adj_factors" / "trade_date=2024-06-27"
    adj_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "trade_date": [date(2024, 6, 27)] * 2,
            "adjust_type": ["hfq", "hfq"],
            "factor": [2.0, 3.0],
            **_prov("sina"),
        }
    ).write_parquet(adj_dir / "part-0.parquet")

    fsi_dir = curated / "financial_statement_items" / "report_period=2024Q1"
    fsi_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "report_period": ["2024Q1", "2024Q1"],
            "statement_type": ["income", "income"],
            "item_code": ["roe", "revenue"],
            "item_value": [0.25, 1_000_000.0],
            "announce_date": [date(2024, 4, 28), date(2024, 5, 15)],
            "source": ["test", "test"],
            "data_version": ["v1", "v1"],
            "fetched_at": [
                datetime(2024, 4, 28, tzinfo=timezone.utc),
                datetime(2024, 5, 15, tzinfo=timezone.utc),
            ],
        }
    ).write_parquet(fsi_dir / "part-0.parquet")

    return Config(data_root=root)


def test_load_daily_bars_with_adjustment(lake):
    df = load(
        "daily_bars",
        start="2024-06-27",
        end="2024-06-27",
        adjust="hfq",
        config=lake,
    )
    assert df.height == 3
    exact = dict(zip(df["symbol"].to_list(), df["adj_is_exact"].to_list(), strict=True))
    assert exact["600519.SH"] is True
    assert exact["000001.SZ"] is True
    assert exact["300750.SZ"] is False
    moutai = df.filter(pl.col("symbol") == "600519.SH")
    assert moutai["adj_close"][0] == pytest.approx(21.0)
    assert moutai["adj_is_exact"][0] is True


def test_load_dedupes_duplicate_primary_keys_and_keeps_latest(lake):
    bars_dir = lake.curated_root / "daily_bars" / "trade_date=2024-06-27"
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 27)],
            "open": [12.0],
            "high": [12.0],
            "low": [12.0],
            "close": [12.0],
            "volume": [1200],
            "amount": [14400.0],
            "source": ["eastmoney"],
            "data_version": ["v2"],
            "fetched_at": [datetime(2024, 6, 28, 1, tzinfo=timezone.utc)],
        }
    ).write_parquet(bars_dir / "part-duplicate.parquet")

    df = load("daily_bars", start="2024-06-27", end="2024-06-27", config=lake)
    assert df.height == 3
    moutai = df.filter(pl.col("symbol") == "600519.SH")
    assert moutai["close"].to_list() == [12.0]
    assert moutai["source"].to_list() == ["eastmoney"]


def test_load_strict_adj_raises_when_factor_missing(lake):
    with pytest.raises(MissingAdjustmentError, match="missing adj_factors"):
        load(
            "daily_bars",
            start="2024-06-27",
            end="2024-06-27",
            adjust="hfq",
            strict_adj=True,
            config=lake,
        )


def test_load_daily_bars_universe_filters_st_and_suspended(lake):
    df = load(
        "daily_bars",
        start="2024-06-27",
        end="2024-06-27",
        profile="legacy_all_a",
        config=lake,
    )
    assert set(df["symbol"].to_list()) == {"600519.SH"}


def test_load_universe_drops_invalid_price_placeholders_before_validation(lake):
    bars_dir = lake.curated_root / "daily_bars" / "trade_date=2024-06-27"
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 27)],
            "open": [0.0],
            "high": [0.0],
            "low": [0.0],
            "close": [0.0],
            "volume": [0],
            "amount": [0.0],
            **_prov("eastmoney"),
        }
    ).write_parquet(bars_dir / "part-invalid.parquet")

    df = load(
        "daily_bars",
        start="2024-06-27",
        end="2024-06-27",
        profile="legacy_all_a",
        config=lake,
    )
    assert set(df["symbol"].to_list()) == {"600519.SH"}
    assert df["close"].to_list() == [10.5]


def test_load_universe_excludes_cdr_despite_missing_factors(lake):
    """CDR bars without adj_factors must not break strict_adj all_a loads."""
    inst_path = lake.curated_root / "instruments" / "part-merged.parquet"
    inst = pl.read_parquet(inst_path)
    cdr = pl.DataFrame(
        {
            "symbol": ["689009.SH"],
            "name": ["Ninebot"],
            "exchange": ["SH"],
            "asset_type": ["cdr"],
            "list_date": [date(2020, 10, 29)],
            "delist_date": [None],
            **_prov(),
        }
    )
    pl.concat([inst, cdr], how="diagonal_relaxed").write_parquet(inst_path)
    bars_dir = lake.curated_root / "daily_bars" / "trade_date=2024-06-27"
    pl.DataFrame(
        {
            "symbol": ["689009.SH"],
            "trade_date": [date(2024, 6, 27)],
            "open": [40.0],
            "high": [41.0],
            "low": [39.0],
            "close": [40.5],
            "volume": [4000],
            "amount": [162000.0],
            **_prov(),
        }
    ).write_parquet(bars_dir / "part-1.parquet")

    df = load(
        "daily_bars",
        start="2024-06-27",
        end="2024-06-27",
        adjust="hfq",
        profile="legacy_all_a",
        strict_adj=True,
        config=lake,
    )
    assert set(df["symbol"].to_list()) == {"600519.SH"}

    # direct symbol queries still work, honestly flagged as inexact
    direct = load(
        "daily_bars",
        start="2024-06-27",
        end="2024-06-27",
        adjust="hfq",
        symbols=["689009.SH"],
        config=lake,
    )
    assert direct["adj_is_exact"].to_list() == [False]


def test_all_a_filter_does_not_bypass_empty_valid_catalog(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["510300.SH"],
            "name": ["CSI 300 ETF"],
            "exchange": ["SH"],
            "asset_type": ["etf"],
            "list_date": [date(2012, 5, 28)],
            "delist_date": [None],
        }
    ).write_parquet(instruments / "part-merged.parquet")
    frame = pl.DataFrame(
        {"symbol": ["510300.SH"], "trade_date": [date(2024, 6, 28)], "close": [4.0]}
    )

    out = apply_universe_filter(frame, cfg, universe="all_a")

    assert out.is_empty()


def test_sh_sz_universe_explicitly_excludes_bj_symbols(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ", "920001.BJ"],
            "list_date": [date(2001, 1, 1)] * 3,
            "delist_date": [None] * 3,
        }
    ).write_parquet(instruments / "part-merged.parquet")
    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ", "920001.BJ"],
            "trade_date": [date(2024, 6, 28)] * 3,
            "close": [10.0, 20.0, 30.0],
        }
    )

    out = apply_universe_filter(frame, cfg, universe="all_a_sh_sz")

    assert set(out["symbol"].to_list()) == {"600519.SH", "000001.SZ"}


def test_universe_filter_bounds_status_scan_to_bar_window(monkeypatch, tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "list_date": [date(2001, 1, 1)],
            "delist_date": [None],
        }
    ).write_parquet(instruments / "part-merged.parquet")
    status = cfg.curated_root / "trading_status" / "trade_date=2024-06-28"
    status.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "is_trading": [True],
            "status": ["normal"],
        }
    ).write_parquet(status / "part-0.parquet")

    from cnequity.query import universe as universe_mod

    requested: dict[str, object] = {}
    original = universe_mod.scan_parquet_root

    def bounded_scan(*args, **kwargs):
        requested.update(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(universe_mod, "scan_parquet_root", bounded_scan)

    out = apply_universe_filter(
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [date(2024, 6, 28)],
                "close": [1800.0],
            }
        ),
        cfg,
        universe="all_a",
    )

    assert out["symbol"].to_list() == ["600519.SH"]
    assert requested["start"] == date(2024, 6, 28)
    assert requested["end"] == date(2024, 6, 28)


def test_all_a_filter_rejects_missing_date_column(tmp_path):
    with pytest.raises(ValueError, match="requires date column 'trade_date'"):
        apply_universe_filter(
            pl.DataFrame({"symbol": ["600519.SH"], "close": [1700.0]}),
            Config(data_root=tmp_path / "data"),
            universe="all_a",
        )


def test_strict_all_a_filter_rejects_partial_status_coverage(lake):
    status_dir = lake.curated_root / "trading_status" / "trade_date=2024-06-27"
    (status_dir / "part-0.parquet").unlink()
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "trade_date": [date(2024, 6, 27)] * 2,
            "is_trading": [True, True],
            "status": ["normal", "normal"],
            "source": ["eastmoney", "eastmoney"],
            "data_version": ["v1", "v1"],
            "fetched_at": [datetime(2024, 6, 28, 1, tzinfo=timezone.utc)] * 2,
        }
    ).write_parquet(status_dir / "part-partial.parquet")

    with pytest.raises(ValueError, match="missing 1 symbol-date row"):
        load(
            "daily_bars",
            start="2024-06-27",
            end="2024-06-27",
            profile="legacy_all_a",
            strict_universe=True,
            config=lake,
        )


def test_strict_all_a_filter_requires_historical_st_evidence(lake):
    """Rows alone are not proof that an historical ST sweep was complete."""
    with pytest.raises(ValueError, match="historical ST evidence"):
        load(
            "daily_bars",
            start="2024-06-27",
            end="2024-06-27",
            profile="legacy_all_a",
            strict_universe=True,
            config=lake,
        )


def test_load_daily_bars_qfq_derived_from_hfq(lake):
    bars_dir = lake.curated_root / "daily_bars" / "trade_date=2024-06-26"
    bars_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 26)],
            "open": [10.0],
            "high": [10.0],
            "low": [10.0],
            "close": [10.0],
            "volume": [1000],
            "amount": [10000.0],
            **_prov(),
        }
    ).write_parquet(bars_dir / "part-0.parquet")

    for td, factor in ((date(2024, 6, 26), 2.0), (date(2024, 6, 27), 4.0)):
        adj_dir = lake.derived_root / "adj_factors" / f"trade_date={td.isoformat()}"
        adj_dir.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [td],
                "adjust_type": ["hfq"],
                "factor": [factor],
                **_prov("sina"),
            }
        ).write_parquet(adj_dir / "part-0.parquet")

    df = load(
        "daily_bars",
        start="2024-06-26",
        end="2024-06-27",
        adjust="qfq",
        config=lake,
    )
    moutai = df.filter(pl.col("symbol") == "600519.SH").sort("trade_date")
    assert moutai["adj_close"][0] == pytest.approx(5.0)  # 10 * (2/4)
    assert moutai["adj_close"][1] == pytest.approx(10.5)  # anchor date
    assert moutai["adj_is_exact"].all()


def test_load_financial_statement_items_pit(lake):
    df = load(
        "financial_statement_items",
        as_of="2024-04-30",
        items=["roe"],
        config=lake,
    )
    assert df.height == 1
    assert df["item_code"][0] == "roe"
    assert df["announce_date"][0] == date(2024, 4, 28)


def test_load_pit_date_range_honors_temporal_column(lake):
    root = lake.curated_root / "announcement_index"
    for announce_date in (date(2024, 1, 5), date(2024, 6, 28)):
        part = root / f"announce_date={announce_date.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "announcement_id": [f"id-{announce_date}"],
                "symbol": ["600519.SH"],
                "title": ["公告"],
                "announce_date": [announce_date],
                "category": ["定期报告"],
                "url": ["https://example.test"],
                **_prov("cninfo"),
            }
        ).write_parquet(part / "part-0.parquet")

    df = load(
        "announcement_index",
        start="2024-06-01",
        end="2024-06-30",
        as_of="2024-06-30",
        config=lake,
    )
    assert df["announce_date"].to_list() == [date(2024, 6, 28)]


def test_load_pit_date_range_maps_financial_periods(lake):
    part = lake.curated_root / "financial_statement_items" / "report_period=2024Q2"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "report_period": ["2024Q2"],
            "statement_type": ["income"],
            "item_code": ["roe"],
            "item_value": [0.3],
            "announce_date": [date(2024, 7, 28)],
            **_prov(),
        }
    ).write_parquet(part / "part-0.parquet")

    df = load(
        "financial_statement_items",
        start="2024-04-01",
        end="2024-06-30",
        as_of="2024-07-31",
        items=["roe"],
        config=lake,
    )
    assert df["report_period"].to_list() == ["2024Q2"]


def test_load_financial_statement_items_requires_as_of(lake):
    with pytest.raises(ReaderError, match="requires as_of"):
        load("financial_statement_items", config=lake)


def test_load_raises_when_dataset_has_no_parquet_files(lake):
    with pytest.raises(ReaderError, match="no parquet data for dataset 'corporate_actions'"):
        load("corporate_actions", config=lake)


def test_resolve_config_raises_without_cnequity_toml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ReaderError, match="No config found"):
        resolve_config()


def test_load_raises_when_data_root_has_no_dataset(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    with pytest.raises(ReaderError, match="no parquet data for dataset 'daily_bars'"):
        load("daily_bars", config=cfg)


def test_load_index_bars_rejects_universe_filter(lake):
    with pytest.raises(ReaderError, match="universe filter applies to daily_bars only"):
        load("index_bars", profile="legacy_all_a", config=lake)


@pytest.mark.parametrize("dataset", ["minute_bars", "minute_bars_5m", "fund_flow"])
def test_load_rejects_universe_filter_for_non_daily_datasets(lake, dataset):
    with pytest.raises(ReaderError, match="universe filter applies to daily_bars only"):
        load(dataset, profile="legacy_all_a", config=lake)


def test_strict_all_a_filter_rejects_empty_bar_scope(lake):
    with pytest.raises(ValueError, match="cannot prove coverage.*returned no rows"):
        load(
            "daily_bars",
            start="2024-06-28",
            end="2024-06-28",
            profile="legacy_all_a",
            strict_universe=True,
            config=lake,
        )


def test_load_index_bars_rejects_stock_adjustment(lake):
    with pytest.raises(ReaderError, match="index_bars levels are not adjustable"):
        load("index_bars", adjust="hfq", config=lake)


def test_scan_returns_lazyframe_with_pushdown(tmp_path):
    import polars as pl

    from cnequity.config import Config
    from cnequity.query.reader import ReaderError, scan

    cfg = Config(data_root=tmp_path)
    out_dir = tmp_path / "curated" / "daily_bars" / "trade_date=2024-06-28"
    out_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["000001.SZ"],
            "trade_date": [date(2024, 6, 28)],
            "close": [10.5],
        }
    ).write_parquet(out_dir / "part-0.parquet")

    lf = scan("daily_bars", config=cfg)
    assert isinstance(lf, pl.LazyFrame)
    assert lf.collect().height == 1

    lf = scan("daily_bars", config=cfg, end="2024-06-27")
    assert lf.collect().height == 0

    try:
        scan("nope", config=cfg)
        raise AssertionError("expected ReaderError")
    except ReaderError:
        pass


def test_list_datasets_catalog(tmp_path):
    import polars as pl

    from cnequity.config import Config
    from cnequity.query.reader import list_datasets

    cfg = Config(data_root=tmp_path)
    out_dir = tmp_path / "curated" / "daily_bars" / "trade_date=2024-06-28"
    out_dir.mkdir(parents=True)
    pl.DataFrame({"symbol": ["000001.SZ"]}).write_parquet(out_dir / "part-0.parquet")
    fsi_dir = tmp_path / "curated" / "financial_statement_items" / "report_period=2016Q1"
    fsi_dir.mkdir(parents=True)
    pl.DataFrame({"symbol": ["000001.SZ"]}).write_parquet(fsi_dir / "part-0.parquet")

    df = list_datasets(config=cfg)
    assert df.height >= 26
    row = df.filter(pl.col("dataset") == "daily_bars").to_dicts()[0]
    assert row["has_data"] is True
    assert row["coverage_start"] == date(2024, 6, 28)
    assert row["history_mode"] == "by_date"
    assert row["backfill_source"] is None
    row = df.filter(pl.col("dataset") == "fund_flow").to_dicts()[0]
    assert row["fetch_semantics"] == "snapshot"
    assert row["history_mode"] == "snapshot_only"
    assert row["backfill_source"] is None
    assert row["has_data"] is False
    row = df.filter(pl.col("dataset") == "valuation_metrics").to_dicts()[0]
    assert row["history_mode"] == "snapshot_with_backfill"
    assert row["backfill_source"] == "baostock"
    row = df.filter(pl.col("dataset") == "financial_statement_items").to_dicts()[0]
    assert row["has_data"] is True
    assert row["coverage_start"] == date(2016, 1, 1)
    assert row["coverage_end"] == date(2016, 3, 31)


def test_list_datasets_uses_real_dates_inside_coarse_partitions(tmp_path):
    """A year partition must not claim its calendar end as the data tip."""
    import polars as pl

    from cnequity.config import Config
    from cnequity.query.reader import list_datasets

    cfg = Config(data_root=tmp_path)
    out_dir = tmp_path / "curated" / "index_bars" / "trade_date=2026"
    out_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["000001.SZ"],
            "trade_date": [date(2026, 7, 20)],
        }
    ).write_parquet(out_dir / "part-0.parquet")

    row = list_datasets(config=cfg).filter(pl.col("dataset") == "index_bars").to_dicts()[0]

    assert row["coverage_start"] == date(2026, 7, 20)
    assert row["coverage_end"] == date(2026, 7, 20)


def test_list_datasets_keeps_catalog_readable_when_coarse_partition_is_corrupt(tmp_path):
    import polars as pl

    from cnequity.config import Config
    from cnequity.query.reader import list_datasets

    cfg = Config(data_root=tmp_path)
    root = cfg.curated_root / "index_bars" / "trade_date=2026"
    root.mkdir(parents=True)
    (root / "broken.parquet").write_bytes(b"not a parquet file")

    row = list_datasets(config=cfg).filter(pl.col("dataset") == "index_bars").to_dicts()[0]

    assert row["has_data"] is True
    assert row["coverage_start"] is None
    assert row["coverage_end"] is None


def test_list_datasets_does_not_count_empty_daily_bar_partition_as_coverage(tmp_path):
    import polars as pl

    from cnequity.config import Config
    from cnequity.query.reader import list_datasets

    cfg = Config(data_root=tmp_path)
    root = cfg.curated_root / "daily_bars" / "trade_date=2024-06-28"
    root.mkdir(parents=True)
    pl.DataFrame(
        schema={"symbol": pl.String, "trade_date": pl.Date, "volume": pl.Int64}
    ).write_parquet(root / "empty.parquet")

    row = list_datasets(config=cfg).filter(pl.col("dataset") == "daily_bars").to_dicts()[0]

    assert row["has_data"] is True
    assert row["coverage_start"] is None
    assert row["coverage_end"] is None


def test_daily_bar_coverage_stops_at_the_last_traded_session(tmp_path):
    import polars as pl

    from cnequity.config import Config
    from cnequity.query.reader import list_datasets

    cfg = Config(data_root=tmp_path)
    traded = date(2024, 6, 27)
    empty = date(2024, 6, 28)
    for day, volume in ((traded, 100), (empty, 0)):
        root = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
        root.mkdir(parents=True)
        pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [day],
                "volume": [volume],
            }
        ).write_parquet(root / "part.parquet")

    row = list_datasets(config=cfg).filter(pl.col("dataset") == "daily_bars").to_dicts()[0]

    assert row["coverage_start"] == traded
    assert row["coverage_end"] == traded


def test_list_datasets_clamps_partial_derived_dense_tip(tmp_path):
    from cnequity.query.reader import list_datasets

    cfg = Config(data_root=tmp_path)
    calendar = cfg.curated_root / "trading_calendar" / "trade_date=2026"
    calendar.mkdir(parents=True)
    sessions = [date(2026, 8, 3), date(2026, 8, 4)]
    pl.DataFrame(
        {
            "trade_date": sessions,
            "is_trading": [True, True],
        }
    ).write_parquet(calendar / "part-0.parquet")

    root = cfg.curated_root / "market_breadth" / "trade_date=2026"
    root.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": [sessions[0]] * 7 + [sessions[1]],
            "metric_id": [
                "advance_count",
                "decline_count",
                "flat_count",
                "limit_up_count",
                "limit_down_count",
                "advance_ratio",
                "total_count",
                "advance_count",
            ],
            "value": [1.0] * 8,
        }
    ).write_parquet(root / "part-0.parquet")

    row = list_datasets(config=cfg).filter(pl.col("dataset") == "market_breadth").to_dicts()[0]

    assert row["coverage_start"] == sessions[0]
    assert row["coverage_end"] == sessions[0]


def test_dataset_schema_contract():
    import polars as pl

    from cnequity.query.reader import dataset_schema

    schema = dataset_schema("daily_bars")
    assert schema["trade_date"] == pl.Date
    assert "close" in schema


def _write_daily(cfg: Config, day: date, *, close: float, volume: int) -> None:
    root = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [day],
            "open": [close],
            "high": [close],
            "low": [close],
            "close": [close],
            "volume": [volume],
            "amount": [close * volume],
            **_prov(),
        }
    ).write_parquet(root / "part-0.parquet")


def _write_factor(cfg: Config, day: date, factor: float) -> None:
    root = cfg.derived_root / "adj_factors" / f"trade_date={day.isoformat()}"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [day],
            "adjust_type": ["hfq"],
            "factor": [factor],
            **_prov(),
        }
    ).write_parquet(root / "part-0.parquet")


def test_zero_volume_gap_keeps_the_previous_hfq_factor(tmp_path, caplog):
    """A suspended placeholder must not be published as raw-priced hfq.

    The derive sometimes stops writing factors a few sessions after volume
    hits zero. The last published factor still applies; a later resume factor
    must not be pulled backwards onto the suspension.
    """
    import logging

    cfg = Config(data_root=tmp_path / "lake")
    _write_daily(cfg, date(2024, 6, 26), close=10.0, volume=1000)
    _write_daily(cfg, date(2024, 6, 27), close=10.0, volume=0)
    _write_daily(cfg, date(2024, 6, 28), close=11.0, volume=800)
    _write_factor(cfg, date(2024, 6, 26), 2.0)
    _write_factor(cfg, date(2024, 6, 28), 4.0)

    with caplog.at_level(logging.WARNING):
        df = load(
            "daily_bars",
            start="2024-06-26",
            end="2024-06-28",
            adjust="hfq",
            config=cfg,
        ).sort("trade_date")

    assert "missing adj_factors" not in caplog.text
    assert df["adj_is_exact"].to_list() == [True, True, True]
    assert df["adj_close"].to_list() == pytest.approx([20.0, 20.0, 44.0])


def test_suspended_gap_before_the_window_still_uses_the_last_factor(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_daily(cfg, date(2024, 6, 26), close=10.0, volume=1000)
    _write_daily(cfg, date(2024, 6, 27), close=10.0, volume=0)
    _write_factor(cfg, date(2024, 6, 26), 2.5)

    df = load(
        "daily_bars",
        start="2024-06-27",
        end="2024-06-27",
        adjust="hfq",
        strict_adj=True,
        config=cfg,
    )

    assert df["adj_is_exact"].to_list() == [True]
    assert df["adj_close"].to_list() == pytest.approx([25.0])


def test_traded_bar_without_a_factor_stays_inexact(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_daily(cfg, date(2024, 6, 26), close=10.0, volume=1000)
    _write_daily(cfg, date(2024, 6, 27), close=12.0, volume=50)
    _write_factor(cfg, date(2024, 6, 26), 2.0)

    df = load(
        "daily_bars",
        start="2024-06-26",
        end="2024-06-27",
        adjust="hfq",
        config=cfg,
    ).sort("trade_date")

    assert df["adj_is_exact"].to_list() == [True, False]
    assert df["adj_close"].to_list() == pytest.approx([20.0, 12.0])


def test_qfq_prices_a_suspended_gap_like_hfq(tmp_path):
    """Both modes read the same carried factor; qfq only rescales it."""
    cfg = Config(data_root=tmp_path / "lake")
    _write_daily(cfg, date(2024, 6, 26), close=10.0, volume=1000)
    _write_daily(cfg, date(2024, 6, 27), close=10.0, volume=0)
    _write_daily(cfg, date(2024, 6, 28), close=11.0, volume=800)
    _write_factor(cfg, date(2024, 6, 26), 2.0)
    _write_factor(cfg, date(2024, 6, 28), 4.0)

    frames = {
        adjust: load(
            "daily_bars",
            start="2024-06-26",
            end="2024-06-28",
            adjust=adjust,
            strict_adj=True,
            config=cfg,
        ).sort("trade_date")
        for adjust in ("hfq", "qfq")
    }

    assert frames["qfq"]["adj_is_exact"].to_list() == [True, True, True]
    assert frames["qfq"]["adj_close"].to_list() == pytest.approx([5.0, 5.0, 11.0])
    hfq = frames["hfq"]["adj_close"].to_list()
    assert [price / 4.0 for price in hfq] == pytest.approx(frames["qfq"]["adj_close"].to_list())


def test_qfq_window_inside_a_suspension_anchors_on_the_carried_factor(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    _write_daily(cfg, date(2024, 6, 26), close=10.0, volume=1000)
    _write_daily(cfg, date(2024, 6, 27), close=10.0, volume=0)
    _write_factor(cfg, date(2024, 6, 26), 2.5)

    df = load(
        "daily_bars",
        start="2024-06-27",
        end="2024-06-27",
        adjust="qfq",
        strict_adj=True,
        config=cfg,
    )

    assert df["adj_is_exact"].to_list() == [True]
    assert df["adj_close"].to_list() == pytest.approx([10.0])
