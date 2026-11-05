"""The Python reader and the DuckDB views must pick the same canonical rows.

``domain.canonical.dedupe_by_primary_key`` (``load()``) and the SQL built by
``query.views._view_select_sql`` (DuckDB views, MCP ``run_sql``) are two
implementations of one contract. Each case below writes colliding
observations of one key to Parquet and feeds the same file to both.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import duckdb
import polars as pl
import pytest

from cnequity.domain.canonical import EVIDENCE_RULES, dedupe_by_primary_key
from cnequity.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS, validate_dataframe
from cnequity.query.views import _view_select_sql

T1 = datetime(2024, 6, 28, 1, tzinfo=timezone.utc)
T2 = datetime(2024, 6, 28, 2, tzinfo=timezone.utc)
DAY = date(2024, 6, 28)


def _default(dtype: pl.DataType):
    if dtype == pl.Date:
        return DAY
    if isinstance(dtype, pl.Datetime):
        return T1
    if dtype.is_float():
        return 1.0
    if dtype.is_integer():
        return 1
    if dtype == pl.Boolean:
        return False
    return "x"


# A bar row must satisfy low <= open/close <= high; other numerics default to 1.
_BAR_PRICES = {"open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5}


def _frame(dataset: str, rows: list[dict]) -> pl.DataFrame:
    schema = DATASET_SCHEMAS[dataset]
    filled = [
        {col: row.get(col, _BAR_PRICES.get(col, _default(dtype))) for col, dtype in schema.items()}
        for row in rows
    ]
    return validate_dataframe(pl.DataFrame(filled, schema=schema), dataset)


def _both(tmp_path, dataset: str, frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    path = tmp_path / "part-0.parquet"
    frame.write_parquet(path)
    con = duckdb.connect()
    try:
        glob = path.as_posix()
        columns = {
            row[0]: row[1]
            for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{glob}')").fetchall()
        }
        sql = pl.from_arrow(
            con.sql(_view_select_sql(dataset, glob, False, columns=columns)).arrow()
        )
    finally:
        con.close()
    python = dedupe_by_primary_key(pl.read_parquet(path), dataset)
    keys = PRIMARY_KEYS[dataset]
    compare = [c for c in frame.columns if c in sql.columns]
    return (
        python.select(compare).sort(keys),
        sql.select(compare).cast(python.select(compare).schema).sort(keys),
    )


def _assert_same(tmp_path, dataset, rows, **expected):
    python, sql = _both(tmp_path, dataset, _frame(dataset, rows))
    assert python.to_dicts() == sql.to_dicts()
    for column, value in expected.items():
        assert python.get_column(column).to_list() == value, column


def test_latest_fetch_wins(tmp_path):
    _assert_same(
        tmp_path,
        "fund_flow",
        [
            {"symbol": "600519.SH", "main_net_inflow": 1.0, "fetched_at": T1},
            {"symbol": "600519.SH", "main_net_inflow": 2.0, "fetched_at": T2},
        ],
        main_net_inflow=[2.0],
    )


def test_primary_source_breaks_a_timestamp_tie(tmp_path):
    _assert_same(
        tmp_path,
        "daily_bars",
        [
            {"symbol": "600519.SH", "close": 10.0, "source": "eastmoney", "fetched_at": T1},
            {"symbol": "600519.SH", "close": 10.5, "source": "tdx_protocol", "fetched_at": T1},
        ],
        source=["tdx_protocol"],
    )


def test_unranked_sources_tie_break_by_name(tmp_path):
    _assert_same(
        tmp_path,
        "daily_bars",
        [
            {"symbol": "600519.SH", "close": 10.0, "source": "exchange", "fetched_at": T1},
            {"symbol": "600519.SH", "close": 10.5, "source": "sina", "fetched_at": T1},
        ],
        source=["sina"],
    )


def test_issuer_original_outranks_a_later_backfill(tmp_path):
    _assert_same(
        tmp_path,
        "financial_statement_items",
        [
            {
                "symbol": "600519.SH",
                "item_value": 1.0,
                "source": "cninfo_original",
                "fetched_at": T1,
            },
            {"symbol": "600519.SH", "item_value": 2.0, "source": "em_backfill", "fetched_at": T2},
        ],
        source=["cninfo_original"],
    )


def test_a_real_index_weight_is_not_erased_by_membership_only(tmp_path):
    _assert_same(
        tmp_path,
        "index_constituents",
        [
            {"index_symbol": "000300.SH", "symbol": "600519.SH", "weight": 5.0, "fetched_at": T1},
            {"index_symbol": "000300.SH", "symbol": "600519.SH", "weight": None, "fetched_at": T2},
        ],
        weight=[5.0],
    )


def test_an_official_book_replaces_a_vendor_book(tmp_path):
    _assert_same(
        tmp_path,
        "index_constituents",
        [
            {"index_symbol": "000300.SH", "symbol": "600519.SH", "source": "exchange"},
            {"index_symbol": "000300.SH", "symbol": "000001.SZ", "source": "eastmoney"},
        ],
        symbol=["600519.SH"],
    )


def test_a_rounded_zero_volume_does_not_win(tmp_path):
    _assert_same(
        tmp_path,
        "daily_bars",
        [
            {"symbol": "600519.SH", "volume": 1000, "amount": 10.0, "fetched_at": T1},
            {"symbol": "600519.SH", "volume": 0, "amount": 10.0, "fetched_at": T2},
        ],
        volume=[1000],
    )


@pytest.mark.parametrize("dataset", ["daily_bars", "index_bars"])
def test_the_previous_close_survives_a_refresh_without_one(tmp_path, dataset):
    """Used to differ: Python carried ``pre_close``, the SQL views returned null."""
    _assert_same(
        tmp_path,
        dataset,
        [
            {"symbol": "600519.SH", "close": 10.0, "pre_close": 9.9, "fetched_at": T1},
            {"symbol": "600519.SH", "close": 10.5, "pre_close": None, "fetched_at": T2},
        ],
        close=[10.5],
        pre_close=[9.9],
    )


def test_a_winners_own_previous_close_is_kept(tmp_path):
    _assert_same(
        tmp_path,
        "daily_bars",
        [
            {"symbol": "600519.SH", "pre_close": 9.9, "fetched_at": T1},
            {"symbol": "600519.SH", "pre_close": 8.8, "fetched_at": T2},
        ],
        pre_close=[8.8],
    )


@pytest.mark.parametrize("dataset", sorted(EVIDENCE_RULES))
def test_every_evidence_rule_has_both_forms(dataset):
    """A rule present in one read path only would be a silent divergence."""
    rule = EVIDENCE_RULES[dataset]
    schema = pl.Schema(DATASET_SCHEMAS[dataset])
    assert (rule.polars(schema) is None) == (rule.sql(schema.names()) is None)
