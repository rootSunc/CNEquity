"""A 转增 that TDX files as 送 and EastMoney as 转 is stored once, whichever run wrote each."""

from __future__ import annotations

from datetime import date

import polars as pl

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.steps.finalize import step_compact
from cnequity.storage import StagingWriter

EX = date(2026, 11, 10)


def _action(symbol, action_type, *, bonus=0.0, transfer=0.0, cash=0.0, source, day=EX):
    return {
        "symbol": symbol,
        "ex_date": day,
        "action_type": action_type,
        "cash_dividend": cash,
        "bonus_ratio": bonus,
        "transfer_ratio": transfer,
        "allotment_ratio": None,
        "allotment_price": None,
        "source": source,
        "data_version": "v1",
        "fetched_at": "2026-11-13T00:00:00+00:00",
    }


def _frame(rows) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
    ).with_columns(pl.col("fetched_at").str.to_datetime(time_unit="us", time_zone="UTC"))


def _kept(frame):
    return sorted(
        dedupe_by_primary_key(_frame(frame), "corporate_actions")
        .select("symbol", "action_type", "source")
        .rows()
    )


def test_a_split_from_another_source_replaces_the_combined_row():
    assert _kept(
        [
            _action("603596.SH", "bonus", bonus=0.42, source="tdx_protocol"),
            _action("603596.SH", "transfer", transfer=0.42, source="eastmoney"),
        ]
    ) == [("603596.SH", "transfer", "eastmoney")]
    # 送 and 转 together, split over two rows.
    assert _kept(
        [
            _action("600000.SH", "bonus", bonus=0.5, source="tdx_protocol"),
            _action("600000.SH", "bonus", bonus=0.2, source="eastmoney"),
            _action("600000.SH", "transfer", transfer=0.3, source="eastmoney"),
        ]
    ) == [("600000.SH", "bonus", "eastmoney"), ("600000.SH", "transfer", "eastmoney")]


def test_the_combined_row_stays_where_nobody_agrees_with_it():
    rows = [
        _action("603596.SH", "bonus", bonus=0.42, source="tdx_protocol"),
        _action("603596.SH", "transfer", transfer=0.3, source="eastmoney"),
        _action("603596.SH", "cash_dividend", cash=0.1, source="eastmoney"),
        _action("000001.SZ", "bonus", bonus=0.42, source="tdx_protocol"),
    ]
    assert _kept(rows) == [
        ("000001.SZ", "bonus", "tdx_protocol"),
        ("603596.SH", "bonus", "tdx_protocol"),
        ("603596.SH", "cash_dividend", "eastmoney"),
        ("603596.SH", "transfer", "eastmoney"),
    ]


def test_two_sources_filing_the_same_split_still_replace_it():
    assert _kept(
        [
            _action("603596.SH", "bonus", bonus=0.42, source="tdx_protocol"),
            _action("603596.SH", "transfer", transfer=0.42, source="eastmoney"),
            _action("603596.SH", "transfer", transfer=0.42, source="baostock"),
        ]
    ) == [("603596.SH", "transfer", "eastmoney")]


def test_a_later_run_s_split_removes_a_combined_row_already_published(tmp_path):
    from cnequity.query import catalog_read, load

    cfg = Config(data_root=tmp_path / "data")
    writer = StagingWriter(cfg.staging_root)
    writer.write_batch(
        "corporate_actions",
        "r1",
        "batch-0",
        _frame(
            [
                _action("603596.SH", "bonus", bonus=0.42, source="tdx_protocol"),
                _action("000001.SZ", "cash_dividend", cash=0.2, source="tdx_protocol"),
            ]
        ),
    )
    step_compact(cfg, EX, "r1", {})
    writer.write_batch(
        "corporate_actions",
        "r2",
        "batch-0",
        _frame([_action("603596.SH", "transfer", transfer=0.42, source="eastmoney")]),
    )
    step_compact(cfg, EX, "r2", {})
    catalog_read.close_all()
    stored = load("corporate_actions", config=cfg).select("symbol", "action_type", "source")
    assert sorted(stored.rows()) == [
        ("000001.SZ", "cash_dividend", "tdx_protocol"),
        ("603596.SH", "transfer", "eastmoney"),
    ]
