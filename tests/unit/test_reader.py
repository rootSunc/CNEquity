from datetime import date, datetime, timezone

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.query.reader import ReaderError, load, resolve_config


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
            **_prov(),
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


def test_load_strict_adj_raises_when_factor_missing(lake):
    with pytest.raises(ReaderError, match="missing adj_factors"):
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
        universe="all_a",
        config=lake,
    )
    assert set(df["symbol"].to_list()) == {"600519.SH"}


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
        universe="all_a",
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


def test_load_financial_statement_items_requires_as_of(lake):
    with pytest.raises(ReaderError, match="requires as_of"):
        load("financial_statement_items", config=lake)


def test_load_raises_when_dataset_has_no_parquet_files(lake):
    with pytest.raises(ReaderError, match="no parquet data for dataset 'corporate_actions'"):
        load("corporate_actions", config=lake)


def test_resolve_config_raises_without_cn_market_lake_toml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ReaderError, match="No config found"):
        resolve_config()


def test_load_raises_when_data_root_has_no_dataset(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    with pytest.raises(ReaderError, match="no parquet data for dataset 'daily_bars'"):
        load("daily_bars", config=cfg)


def test_load_index_bars_rejects_universe_filter(lake):
    with pytest.raises(ReaderError, match="index symbols are not in all_a"):
        load("index_bars", universe="all_a", config=lake)


def test_scan_returns_lazyframe_with_pushdown(tmp_path):
    import polars as pl

    from cn_market_lake.config import Config
    from cn_market_lake.query.reader import ReaderError, scan

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

    from cn_market_lake.config import Config
    from cn_market_lake.query.reader import list_datasets

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


def test_dataset_schema_contract():
    import polars as pl

    from cn_market_lake.query.reader import dataset_schema

    schema = dataset_schema("daily_bars")
    assert schema["trade_date"] == pl.Date
    assert "close" in schema
