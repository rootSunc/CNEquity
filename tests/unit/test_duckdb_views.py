"""DuckDB / polars view paths must stay POSIX-form so Windows ``\\`` never hits globs."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.query.canonical import dedupe_by_primary_key, dedupe_lazy_by_primary_key
from cnequity.query.parquet_scan import parquet_glob
from cnequity.query.views import _view_glob, ensure_duckdb_views


@pytest.mark.parametrize(
    "dataset", ["financial_statement_items", "index_constituents", "daily_bars"]
)
def test_original_financial_and_dated_weight_evidence_match_all_readers(tmp_path, dataset):
    older = datetime(2026, 10, 1, tzinfo=timezone.utc)
    newer = datetime(2026, 10, 2, tzinfo=timezone.utc)
    if dataset == "financial_statement_items":
        common = dict(
            symbol="600519.SH",
            report_period="2016Q4",
            statement_type="balance",
            item_code="total_assets",
            announce_date=date(2017, 4, 15),
            data_version="v1",
        )
        entries = [
            {**common, "item_value": 100.0, "source": "cninfo_original", "fetched_at": older},
            {**common, "item_value": 110.0, "source": "eastmoney_backfill", "fetched_at": newer},
            {
                **common,
                "announce_date": date(2018, 4, 15),
                "item_value": 120.0,
                "source": "eastmoney",
                "fetched_at": newer,
            },
        ]
        cols = ["announce_date", "item_value"]
        expected = [(date(2017, 4, 15), 100.0), (date(2018, 4, 15), 120.0)]
    elif dataset == "index_constituents":
        common = dict(index_symbol="000688.SH", symbol="688001.SH", data_version="v1")
        entries = [
            {
                **common,
                "as_of_date": date(2026, 9, 30),
                "weight": 0.02,
                "source": "exchange",
                "fetched_at": older,
            },
            {
                **common,
                "as_of_date": date(2026, 9, 30),
                "weight": None,
                "source": "eastmoney",
                "fetched_at": newer,
            },
            {
                **common,
                "as_of_date": date(2026, 10, 1),
                "weight": None,
                "source": "eastmoney",
                "fetched_at": newer,
            },
        ]
        cols = ["as_of_date", "weight"]
        expected = [(date(2026, 9, 30), 0.02), (date(2026, 10, 1), None)]
    else:
        common = dict(
            symbol="511950.SH",
            open=99.981,
            high=99.984,
            low=99.981,
            close=99.984,
            data_version="v2",
        )
        entries = [
            {
                **common,
                "trade_date": date(2017, 4, 6),
                "volume": 51,
                "amount": 5099.0,
                "source": "exchange",
                "fetched_at": older,
            },
            {
                **common,
                "trade_date": date(2017, 4, 6),
                "volume": 0,
                "amount": 5099.0,
                "source": "tdx_protocol",
                "fetched_at": newer,
            },
            # No second observation: the unresolved row remains visible.
            {
                **common,
                "trade_date": date(2017, 4, 7),
                "volume": 0,
                "amount": 5099.0,
                "source": "tdx_protocol",
                "fetched_at": newer,
            },
        ]
        cols = ["trade_date", "volume"]
        expected = [(date(2017, 4, 6), 51), (date(2017, 4, 7), 0)]
    frame = pl.DataFrame(entries, schema=DATASET_SCHEMAS[dataset])
    root = tmp_path / "curated" / dataset
    root.mkdir(parents=True)
    frame.write_parquet(root / "legacy.parquet")
    eager = dedupe_by_primary_key(frame, dataset).select(cols).sort(cols[0]).rows()
    lazy = (
        dedupe_lazy_by_primary_key(frame.lazy(), dataset)
        .collect()
        .select(cols)
        .sort(cols[0])
        .rows()
    )
    db = ensure_duckdb_views(Config(data_root=tmp_path))
    with duckdb.connect(str(db), read_only=True) as con:
        sql = con.execute(f"SELECT {', '.join(cols)} FROM {dataset} ORDER BY {cols[0]}").fetchall()
    assert eager == lazy == sql == expected


@pytest.mark.parametrize(
    "older,newer,expected",
    [
        (
            {"payment_source": "issuer_notice:original"},
            {"cash_dividend": 0.11038, "payment_date": None, "payment_source": None},
            (0.11, date(2024, 6, 28)),
        ),
        (
            {"payment_source": "vendor:date"},
            {"payment_date": None, "payment_source": None},
            (0.11, date(2024, 6, 28)),
        ),
        (
            {"payment_source": "issuer_notice:original"},
            {"cash_dividend": 0.12, "payment_source": "issuer_notice:correction"},
            (0.12, date(2024, 6, 28)),
        ),
        (
            {"payment_source": "", "payment_date": date(2024, 6, 29)},
            {"payment_date": None, "payment_source": None},
            (0.11, date(2024, 6, 29)),
        ),
        (
            {"payment_source": "", "payment_date": date(2024, 6, 27)},
            {"payment_date": None, "payment_source": None},
            (0.11, None),
        ),
        (
            {"payment_source": ""},
            {"cash_dividend": 0.12, "payment_date": None, "payment_source": None},
            (0.12, None),
        ),
    ],
)
def test_corporate_action_evidence_matches_all_readers(tmp_path, older, newer, expected):
    common = dict(
        symbol="600519.SH",
        ex_date=date(2024, 6, 28),
        action_type="cash_dividend",
        cash_dividend=0.11,
        bonus_ratio=0.0,
        transfer_ratio=0.0,
        payment_date=date(2024, 6, 28),
        payment_source=None,
        source="tdx_protocol",
        data_version="v1",
    )
    frame = pl.DataFrame(
        [
            {**common, **older, "fetched_at": "2024-06-29T00:00:00Z"},
            {**common, **newer, "fetched_at": "2024-06-30T00:00:00Z"},
        ],
        schema_overrides={"payment_date": pl.Date, "payment_source": pl.String},
    )
    root = tmp_path / "curated/corporate_actions"
    root.mkdir(parents=True)
    frame.write_parquet(root / "legacy-fragments.parquet")
    eager = dedupe_by_primary_key(frame, "corporate_actions")
    lazy = dedupe_lazy_by_primary_key(frame.lazy(), "corporate_actions").collect()
    db = ensure_duckdb_views(Config(data_root=tmp_path))
    cols = ["cash_dividend", "payment_date", "payment_source"]
    with duckdb.connect(str(db), read_only=True) as con:
        sql = con.execute(f"SELECT {', '.join(cols)} FROM corporate_actions").fetchall()
    assert eager.select(cols).rows() == lazy.select(cols).rows() == sql
    assert sql[0][:2] == expected


def test_view_glob_uses_forward_slashes():
    glob_path, hive = _view_glob("C:/Users/测试/lake", DATASETS["daily_bars"])
    assert "\\" not in glob_path
    assert glob_path.startswith("C:/Users/测试/lake/curated/daily_bars/")
    assert hive is True


def test_reviewed_stock_terms_outrank_newer_vendor_in_sql(tmp_path):
    rows = pl.DataFrame(
        {
            "symbol": ["920395.BJ"] * 2,
            "ex_date": [date(2024, 4, 17)] * 2,
            "action_type": ["transfer"] * 2,
            "cash_dividend": [0.0] * 2,
            "bonus_ratio": [0.0] * 2,
            "transfer_ratio": [0.4, 0.8],
            "payment_date": [None] * 2,
            "payment_source": [None] * 2,
            "source": ["tdx_protocol"] * 2,
            "fetched_at": ["2024-04-18T00:00:00Z", "2024-04-19T00:00:00Z"],
        },
        schema_overrides={"payment_date": pl.Date, "payment_source": pl.String},
    )
    root = tmp_path / "curated/corporate_actions"
    root.mkdir(parents=True)
    rows.write_parquet(root / "legacy.parquet")
    db = ensure_duckdb_views(Config(data_root=tmp_path))
    with duckdb.connect(str(db), read_only=True) as con:
        assert con.execute("SELECT transfer_ratio FROM corporate_actions").fetchall() == [(0.4,)]
    assert dedupe_by_primary_key(rows, "corporate_actions")["transfer_ratio"].to_list() == [0.4]


def test_merge_style_view_glob_is_recursive():
    glob_path, hive = _view_glob("/tmp/lake", DATASETS["delisting_events"])
    assert glob_path.endswith("/derived/delisting_events/**/*.parquet")
    assert hive is False


def test_parquet_glob_is_posix(tmp_path):
    pattern = parquet_glob(tmp_path / "curated" / "daily_bars")
    assert "\\" not in pattern
    assert pattern.endswith("/**/*.parquet")


def test_ensure_duckdb_views_accepts_native_windows_style_root(tmp_path):
    # Build a root whose str() would contain backslashes on Windows; on Unix
    # resolve().as_posix() is a no-op, so the assertion still holds.
    data_root = tmp_path / "cnequity"
    cfg = Config(data_root=data_root)
    db = ensure_duckdb_views(cfg)
    assert db.exists()
    # The helper itself must never feed a backslash into the SQL it builds —
    # re-check the path form used for globs.
    posix = data_root.resolve().as_posix()
    assert "\\" not in posix or Path(posix).as_posix() == posix


def test_duckdb_views_dedupe_duplicate_primary_keys(tmp_path):
    data_root = tmp_path / "data"
    partition = data_root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    partition.mkdir(parents=True)
    day = date(2024, 6, 28)
    base = {
        "symbol": ["600519.SH"],
        "trade_date": [day],
        "open": [1790.0],
        "high": [1810.0],
        "low": [1780.0],
        "close": [1800.0],
        "volume": [1000],
        "amount": [1_000_000.0],
        "source": ["tdx_protocol"],
        "data_version": ["v2"],
        "fetched_at": [datetime(2024, 6, 28, tzinfo=timezone.utc)],
    }
    pl.DataFrame(base).write_parquet(partition / "part-old.parquet")
    newer = {
        **base,
        "close": [1900.0],
        "fetched_at": [datetime(2024, 6, 28, 1, tzinfo=timezone.utc)],
    }
    pl.DataFrame(newer).write_parquet(partition / "part-new.parquet")

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db)) as con:
        rows = con.execute("SELECT symbol, close FROM daily_bars ORDER BY symbol").fetchall()
    assert rows == [("600519.SH", 1900.0)]


def test_canonical_dedupe_prefers_primary_source_on_same_timestamp():
    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "trade_date": [date(2024, 6, 28)] * 2,
            "close": [1800.0, 1900.0],
            "source": ["eastmoney", "tdx_protocol"],
            "data_version": ["v9", "v1"],
            "fetched_at": [datetime(2024, 6, 28, tzinfo=timezone.utc)] * 2,
        }
    )

    eager = dedupe_by_primary_key(frame, "daily_bars")
    lazy = dedupe_lazy_by_primary_key(frame.lazy(), "daily_bars").collect()

    assert eager.select("source", "close").to_dicts() == [
        {"source": "tdx_protocol", "close": 1900.0}
    ]
    assert lazy.select("source", "close").to_dicts() == [
        {"source": "tdx_protocol", "close": 1900.0}
    ]


def test_canonical_dedupe_prefers_primary_source_without_fetch_time():
    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "trade_date": [date(2024, 6, 28)] * 2,
            "close": [1800.0, 1900.0],
            "source": ["tdx_protocol", "eastmoney"],
            "data_version": ["v1", "v9"],
        }
    )

    eager = dedupe_by_primary_key(frame, "daily_bars")
    lazy = dedupe_lazy_by_primary_key(frame.lazy(), "daily_bars").collect()

    assert eager.select("source", "close").to_dicts() == [
        {"source": "tdx_protocol", "close": 1800.0}
    ]
    assert lazy.select("source", "close").to_dicts() == [
        {"source": "tdx_protocol", "close": 1800.0}
    ]


def test_canonical_dedupe_does_not_let_null_fetch_time_override_known_row():
    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "trade_date": [date(2024, 6, 28)] * 2,
            "close": [1900.0, 1800.0],
            "source": ["tdx_protocol", "eastmoney"],
            "data_version": ["v1", "v1"],
            "fetched_at": [None, datetime(2024, 6, 28, tzinfo=timezone.utc)],
        }
    )

    eager = dedupe_by_primary_key(frame, "daily_bars")
    lazy = dedupe_lazy_by_primary_key(frame.lazy(), "daily_bars").collect()

    assert eager.select("source", "close").to_dicts() == [{"source": "eastmoney", "close": 1800.0}]
    assert lazy.select("source", "close").to_dicts() == [{"source": "eastmoney", "close": 1800.0}]


def test_duckdb_views_dedupe_prefers_primary_source_on_same_timestamp(tmp_path):
    data_root = tmp_path / "data"
    partition = data_root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    partition.mkdir(parents=True)
    common = {
        "symbol": ["600519.SH"],
        "trade_date": [date(2024, 6, 28)],
        "open": [1790.0],
        "high": [1810.0],
        "low": [1780.0],
        "volume": [1000],
        "amount": [1_000_000.0],
        "fetched_at": [datetime(2024, 6, 28, tzinfo=timezone.utc)],
    }
    pl.DataFrame(
        {**common, "close": [1800.0], "source": ["eastmoney"], "data_version": ["v9"]}
    ).write_parquet(partition / "part-backup.parquet")
    pl.DataFrame(
        {**common, "close": [1900.0], "source": ["tdx_protocol"], "data_version": ["v1"]}
    ).write_parquet(partition / "part-primary.parquet")

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db), read_only=True) as con:
        rows = con.execute("SELECT source, close FROM daily_bars").fetchall()

    assert rows == [("tdx_protocol", 1900.0)]


def test_duckdb_views_dedupe_legacy_rows_by_source_without_fetch_time(tmp_path):
    data_root = tmp_path / "data"
    partition = data_root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    partition.mkdir(parents=True)
    common = {
        "symbol": ["600519.SH"],
        "trade_date": [date(2024, 6, 28)],
        "open": [1790.0],
        "high": [1810.0],
        "low": [1780.0],
        "volume": [1000],
        "amount": [1_000_000.0],
        "data_version": ["v1"],
    }
    pl.DataFrame({**common, "close": [1800.0], "source": ["eastmoney"]}).write_parquet(
        partition / "part-backup.parquet"
    )
    pl.DataFrame({**common, "close": [1900.0], "source": ["tdx_protocol"]}).write_parquet(
        partition / "part-primary.parquet"
    )

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db), read_only=True) as con:
        rows = con.execute("SELECT source, close FROM daily_bars").fetchall()

    assert rows == [("tdx_protocol", 1900.0)]


def test_duckdb_trading_status_view_accepts_legacy_text_fetch_time(tmp_path):
    data_root = tmp_path / "data"
    partition = data_root / "curated" / "trading_status" / "trade_date=2024-06"
    partition.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "is_trading": [True],
            "status": ["normal"],
            "risk_warning": [False],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T08:00:00+00:00"],
        }
    ).write_parquet(partition / "legacy-text-time.parquet")

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db), read_only=True) as con:
        rows = con.execute("SELECT symbol, fetched_at FROM trading_status").fetchall()

    assert rows == [("600519.SH", "2024-06-28T08:00:00+00:00")]


def test_duckdb_views_merge_optional_columns_by_name(tmp_path):
    data_root = tmp_path / "data"
    partition = data_root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    partition.mkdir(parents=True)
    common = {
        "trade_date": [date(2024, 6, 28)],
        "open": [10.0],
        "high": [10.0],
        "low": [10.0],
        "close": [10.0],
        "volume": [100],
        "source": ["tdx_protocol"],
        "data_version": ["v2"],
        "fetched_at": [datetime(2024, 6, 28, tzinfo=timezone.utc)],
    }
    pl.DataFrame({"symbol": ["600519.SH"], "amount": [1000.0], **common}).write_parquet(
        partition / "part-with-amount.parquet"
    )
    pl.DataFrame({"symbol": ["000001.SZ"], **common}).write_parquet(
        partition / "part-without-amount.parquet"
    )

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db)) as con:
        rows = con.execute("SELECT symbol, amount FROM daily_bars ORDER BY symbol").fetchall()
    assert rows == [("000001.SZ", None), ("600519.SH", 1000.0)]


def test_duckdb_qfq_views_use_the_correct_anchor(tmp_path):
    data_root = tmp_path / "data"
    (data_root / "curated" / "daily_bars").mkdir(parents=True)
    (data_root / "derived" / "adj_factors").mkdir(parents=True)
    day = [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]
    pl.DataFrame(
        {
            "symbol": ["600519.SH"] * 3,
            "trade_date": day,
            "open": [9.0, 11.0, 13.0],
            "high": [11.0, 13.0, 15.0],
            "low": [8.0, 10.0, 12.0],
            "close": [10.0, 12.0, 14.0],
            "volume": [100, 100, 100],
            "amount": [1000.0, 1200.0, 1400.0],
            "source": ["test"] * 3,
            "data_version": ["v1"] * 3,
            "fetched_at": [datetime(2024, 1, 4, tzinfo=timezone.utc)] * 3,
        }
    ).write_parquet(data_root / "curated" / "daily_bars" / "part.parquet")
    pl.DataFrame(
        {
            "symbol": ["600519.SH"] * 3,
            "trade_date": day,
            "adjust_type": ["hfq"] * 3,
            "factor": [2.0, 3.0, 4.0],
            "source": ["test"] * 3,
            "data_version": ["v1"] * 3,
            "fetched_at": [datetime(2024, 1, 4, tzinfo=timezone.utc)] * 3,
        }
    ).write_parquet(data_root / "derived" / "adj_factors" / "part.parquet")

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db), read_only=True) as con:
        full = con.execute(
            "SELECT trade_date, qfq_close FROM daily_bars_adj ORDER BY trade_date"
        ).fetchall()
        bounded = con.execute(
            """
            SELECT trade_date, qfq_close
            FROM daily_bars_qfq(DATE '2024-01-01', DATE '2024-01-02')
            ORDER BY trade_date
            """
        ).fetchall()

    # The static view anchors to the latest bar in the lake (factor=4), while
    # the macro anchors to the latest bar in its explicit two-day window
    # (factor=3), matching load(..., adjust='qfq', end='2024-01-02').
    assert [row[0] for row in full] == day
    assert [row[1] for row in full] == pytest.approx([5.0, 9.0, 14.0])
    assert [row[0] for row in bounded] == day[:2]
    assert [row[1] for row in bounded] == pytest.approx([20.0 / 3.0, 12.0])


def test_apostrophe_in_lake_path_preserves_real_data(tmp_path):
    root = tmp_path / "owner's lake"
    partition = root / "curated/daily_bars/trade_date=2026-01-01"
    partition.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [date(2026, 1, 1)],
            "open": [10.0],
            "high": [10.0],
            "low": [10.0],
            "close": [10.0],
        }
    ).write_parquet(partition / "part.parquet")
    db = ensure_duckdb_views(Config(data_root=root))
    with duckdb.connect(str(db), read_only=True) as con:
        assert con.execute("SELECT close FROM daily_bars").fetchall() == [(10.0,)]


def test_central_bank_rates_joins_calendar_rows_by_series_code(tmp_path):
    data_root = tmp_path / "data"
    partition = data_root / "curated" / "economic_calendar" / "event_date=2026"
    partition.mkdir(parents=True)
    pl.DataFrame(
        {
            "event_id": ["jin10:1", "jin10:2"],
            "indicator_code": ["jin10:77", "jin10:117"],
            "event_date": [date(2026, 9, 17), date(2026, 9, 17)],
            "event_time": ["02:00", "19:00"],
            "country": ["美国", "美国"],
            "indicator": ["美联储利率决定(上限)（至9月17日）", "当周MBA30年期固定抵押贷款利率"],
            "importance": [5, 2],
            "forecast": [4.0, None],
            "previous": [3.75, 7.3],
            "actual": [4.25, 7.4],
            "unit": ["%", "%"],
            "source": ["jin10", "jin10"],
            "data_version": ["v1", "v1"],
            "fetched_at": [datetime(2026, 9, 17, tzinfo=timezone.utc)] * 2,
        }
    ).write_parquet(partition / "part.parquet")

    db = ensure_duckdb_views(Config(data_root=data_root))
    with duckdb.connect(str(db)) as con:
        rows = con.execute(
            "SELECT bank, policy, actual, change, surprise FROM central_bank_rates"
        ).fetchall()
    # A mortgage rate is not a policy decision; only the Fed row joins.
    assert rows == [("Fed", True, 4.25, 0.5, 0.25)]


def test_central_bank_series_codes_are_unique():
    from cnequity.domain.central_banks import CENTRAL_BANK_RATES

    codes = [s.indicator_code for s in CENTRAL_BANK_RATES]
    assert len(codes) == len(set(codes))
