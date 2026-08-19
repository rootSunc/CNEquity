"""Offline coverage for L5 structure steps (sector / index / industry)."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.steps import structure as st
from cnequity.storage.state import StateStore


@pytest.fixture
def cfg(tmp_path):
    c = Config(data_root=tmp_path / "data")
    c.staging_root.mkdir(parents=True)
    return c


def test_month_end_trading_days(cfg, monkeypatch):
    days = [
        date(2024, 1, 30),
        date(2024, 1, 31),
        date(2024, 2, 28),
        date(2024, 2, 29),
    ]
    monkeypatch.setattr(st, "list_trading_dates", lambda *a, **k: days)
    assert st._month_end_trading_days(cfg, date(2024, 1, 1), date(2024, 2, 29)) == [
        date(2024, 1, 31),
        date(2024, 2, 29),
    ]


def test_existing_as_of_dates_empty_and_populated(cfg):
    assert st._existing_as_of_dates(cfg, "index_constituents") == set()
    part = cfg.curated_root / "index_constituents" / "as_of_date=2024-06"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "index_symbol": ["399001.SZ"],
            "symbol": ["000001.SZ"],
            "as_of_date": [date(2024, 6, 28)],
        }
    ).write_parquet(part / "part-000.parquet")
    assert st._existing_as_of_dates(cfg, "index_constituents") == {date(2024, 6, 28)}


def test_existing_cni_as_of_date_requires_every_backfill_index(cfg):
    part = cfg.curated_root / "index_constituents" / "as_of_date=2024-06"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "index_symbol": ["399001.SZ"],
            "symbol": ["000001.SZ"],
            "as_of_date": [date(2024, 6, 28)],
        }
    ).write_parquet(part / "part-000.parquet")
    required = ("399001.SZ", "399006.SZ")
    assert (
        st._existing_as_of_dates(
            cfg,
            "index_constituents",
            required_index_symbols=required,
            min_members_per_index=50,
        )
        == set()
    )

    rows = []
    for index_symbol in required:
        rows.extend(
            {
                "index_symbol": index_symbol,
                "symbol": f"{i:06d}.SZ",
                "as_of_date": date(2024, 6, 28),
            }
            for i in range(50)
        )
    pl.DataFrame(rows).write_parquet(part / "part-001.parquet")
    assert st._existing_as_of_dates(
        cfg,
        "index_constituents",
        required_index_symbols=required,
        min_members_per_index=50,
    ) == {date(2024, 6, 28)}


def test_sector_members_disabled_source(cfg):
    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="eastmoney source disabled"):
        st.step_sector_members(cfg, date(2024, 6, 28), "run-1", {})


def test_sector_members_writes_staging(cfg, monkeypatch):
    StateStore(cfg.meta_root).set_date("sector_members", date(2024, 6, 27))

    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "symbol": [f"{i:06d}.SZ" for i in range(10_000)],
                "sector_code": [f"BK{i:05d}" for i in range(10_000)],
                "sector_name": ["板块"] * 10_000,
                "as_of_date": [trade_date] * 10_000,
            }
        )

    monkeypatch.setattr(st, "fetch_sector_members", fake_fetch)
    result = st.step_sector_members(cfg, date(2024, 6, 28), "run-1", {})
    assert result["rows_written"] == 10_000
    assert list(cfg.staging_root.glob("sector_members/**/*.parquet"))


def test_sector_members_rejects_partial_snapshot(cfg, monkeypatch):
    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "sector_code": ["BK0001"],
                "sector_name": ["白酒"],
                "as_of_date": [trade_date],
            }
        )

    monkeypatch.setattr(st, "fetch_sector_members", fake_fetch)
    with pytest.raises(RuntimeError, match="incomplete daily snapshot"):
        st.step_sector_members(cfg, date(2024, 6, 28), "run-partial", {})


def test_sector_members_rejects_a_single_board_with_enough_rows(cfg, monkeypatch):
    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "symbol": [f"{i:06d}.SZ" for i in range(10_000)],
                "sector_code": ["BK0001"] * 10_000,
                "sector_name": ["白酒"] * 10_000,
                "as_of_date": [trade_date] * 10_000,
            }
        )

    monkeypatch.setattr(st, "fetch_sector_members", fake_fetch)
    with pytest.raises(RuntimeError, match=r"sector_code=1 \(minimum 50\)"):
        st.step_sector_members(cfg, date(2024, 6, 28), "run-one-board", {})


def test_index_constituents_daily_writes(cfg, monkeypatch):
    StateStore(cfg.meta_root).set_date("index_constituents", date(2024, 6, 27))

    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "index_symbol": ["000300.SH"] * 50,
                "symbol": [f"{i:06d}.SH" for i in range(50)],
                "as_of_date": [trade_date] * 50,
                "weight": [1.0] * 50,
            }
        )

    monkeypatch.setattr(st, "fetch_index_constituents", fake_fetch)
    result = st.step_index_constituents(cfg, date(2024, 6, 28), "run-1", {})
    assert result["rows_written"] == 50


def test_fetch_index_constituents_uses_latest_change_date(monkeypatch):
    from cnequity.adapters.eastmoney.index_constituents import fetch_index_constituents

    class _Client:
        def close(self):
            pass

    rows = [
        {"INDEX_CODE": "000300", "SECURITY_CODE": "600519", "TRADE_MARKET": "SH",
         "TRADE_DATE": "2024-01-10"},
        {"INDEX_CODE": "000300", "SECURITY_CODE": "000001", "TRADE_MARKET": "SZ",
         "TRADE_DATE": "2023-06-01"},
        {"INDEX_CODE": "000300", "SECURITY_CODE": "000002", "TRADE_MARKET": "SZ",
         "TRADE_DATE": "2024-07-01"},
        {"INDEX_CODE": "000001", "SECURITY_CODE": "600000", "TRADE_MARKET": "SH",
         "TRADE_DATE": "2024-01-10"},
    ]
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.index_constituents.fetch_datacenter",
        lambda *a, **k: rows,
    )
    df = fetch_index_constituents(date(2024, 6, 28), indices=["000300.SH"], client=_Client())
    assert set(df["symbol"].to_list()) == {"600519.SH", "000001.SZ"}
    assert df["as_of_date"].to_list() == [date(2024, 6, 28)] * 2


def test_index_constituents_daily_rejects_partial_snapshot(cfg, monkeypatch):
    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "index_symbol": ["000300.SH"],
                "symbol": ["600519.SH"],
                "as_of_date": [trade_date],
                "weight": [1.0],
            }
        )

    monkeypatch.setattr(st, "fetch_index_constituents", fake_fetch)

    with pytest.raises(RuntimeError, match="incomplete daily snapshot"):
        st.step_index_constituents(cfg, date(2024, 6, 28), "run-partial", {})


def test_index_constituents_backfill_already_present(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 1, 31)
    monkeypatch.setattr(st, "_month_end_trading_days", lambda *a, **k: [date(2024, 1, 31)])
    monkeypatch.setattr(st, "_existing_as_of_dates", lambda *a, **k: {date(2024, 1, 31)})
    result = st.step_index_constituents(cfg, date(2024, 1, 31), "run-bf", {})
    assert result["rows_written"] == 0
    assert "already present" in result["note"]


def test_index_constituents_backfill_writes_cni(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 1, 31)
    todo = [date(2024, 1, 31)]
    monkeypatch.setattr(st, "_month_end_trading_days", lambda *a, **k: todo)
    monkeypatch.setattr(st, "_existing_as_of_dates", lambda *a, **k: set())

    adj = pl.DataFrame({"index_symbol": ["399001.SZ"], "dummy": [1]})
    expanded = pl.DataFrame(
        {
            "index_symbol": ["399001.SZ"] * 50,
            "symbol": [f"{i:06d}.SZ" for i in range(50)],
            "as_of_date": [date(2024, 1, 31)] * 50,
            "weight": pl.Series("weight", [None] * 50, dtype=pl.Float64),
        }
    )

    def fake_adj(index_symbol):
        if index_symbol == "399001.SZ":
            return adj
        return pl.DataFrame()

    monkeypatch.setattr(st, "fetch_cni_index_adjustments", fake_adj)
    monkeypatch.setattr(st, "expand_cni_constituents_as_of", lambda a, days: expanded)
    result = st.step_index_constituents(cfg, date(2024, 1, 31), "run-cni", {})
    assert result["rows_written"] == 50
    assert result["status"] == "warning"
    assert result["as_of_dates"] == 1
    findings = result["context_updates"]["audit_findings"]
    assert findings[0]["code"] == "cni_index_backfill_incomplete"


def test_index_constituents_backfill_rejects_thin_nonempty_index(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 1, 31)
    monkeypatch.setattr(st, "_month_end_trading_days", lambda *a, **k: [date(2024, 1, 31)])
    monkeypatch.setattr(st, "_existing_as_of_dates", lambda *a, **k: set())

    adjustment = pl.DataFrame({"index_symbol": ["399001.SZ"], "dummy": [1]})
    thin = pl.DataFrame(
        {
            "index_symbol": ["399001.SZ"],
            "symbol": ["000001.SZ"],
            "as_of_date": [date(2024, 1, 31)],
            "weight": pl.Series("weight", [None], dtype=pl.Float64),
        }
    )

    monkeypatch.setattr(st, "fetch_cni_index_adjustments", lambda _: adjustment)
    monkeypatch.setattr(st, "expand_cni_constituents_as_of", lambda *_: thin)

    with pytest.raises(RuntimeError, match="below the minimum 50"):
        st.step_index_constituents(cfg, date(2024, 1, 31), "run-cni-thin", {})


def test_industry_members_disabled(cfg):
    cfg.sources["eastmoney"] = False
    with pytest.raises(RuntimeError, match="eastmoney source disabled"):
        st.step_industry_members(cfg, date(2024, 6, 28), "run-1", {})


def test_industry_members_daily_writes(cfg, monkeypatch):
    StateStore(cfg.meta_root).set_date("industry_members", date(2024, 6, 27))

    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "symbol": [f"{i:06d}.SZ" for i in range(1000)],
                "classification_system": ["em"] * 1000,
                "industry_code": [f"BK{i % 50:04d}" for i in range(1000)],
                "industry_name": [f"行业{i % 50}" for i in range(1000)],
                "as_of_date": [trade_date] * 1000,
            }
        )

    monkeypatch.setattr(st, "fetch_industry_members", fake_fetch)
    result = st.step_industry_members(cfg, date(2024, 6, 28), "run-1", {})
    assert result["rows_written"] == 1000


def test_industry_members_rejects_partial_snapshot(cfg, monkeypatch):
    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "classification_system": ["em"],
                "industry_code": ["白酒"],
                "industry_name": ["白酒"],
                "as_of_date": [trade_date],
            }
        )

    monkeypatch.setattr(st, "fetch_industry_members", fake_fetch)
    with pytest.raises(RuntimeError, match="incomplete daily snapshot"):
        st.step_industry_members(cfg, date(2024, 6, 28), "run-partial", {})


def test_industry_members_rejects_a_single_industry_with_enough_symbols(cfg, monkeypatch):
    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "symbol": [f"{i:06d}.SZ" for i in range(1000)],
                "classification_system": ["em"] * 1000,
                "industry_code": ["BK0001"] * 1000,
                "industry_name": ["白酒"] * 1000,
                "as_of_date": [trade_date] * 1000,
            }
        )

    monkeypatch.setattr(st, "fetch_industry_members", fake_fetch)
    with pytest.raises(RuntimeError, match=r"industry_code=1 \(minimum 50\)"):
        st.step_industry_members(cfg, date(2024, 6, 28), "run-one-industry", {})


def test_industry_members_backfill_already_present(cfg, monkeypatch):
    cfg._backfill = True
    monkeypatch.setattr(st, "_month_end_trading_days", lambda *a, **k: [date(2024, 1, 31)])
    monkeypatch.setattr(st, "_existing_sw_as_of_dates", lambda *a, **k: {date(2024, 1, 31)})
    monkeypatch.setattr(st, "_existing_as_of_dates", lambda *a, **k: {date(2024, 1, 31)})
    result = st.step_industry_members(cfg, date(2024, 1, 31), "run-sw", {})
    assert "already present" in result["note"]


def test_industry_members_backfill_rejects_thin_month(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 1, 31)
    monkeypatch.setattr(st, "_month_end_trading_days", lambda *a, **k: [date(2024, 1, 31)])
    monkeypatch.setattr(st, "_existing_sw_as_of_dates", lambda *a, **k: set())
    monkeypatch.setattr(st, "_existing_as_of_dates", lambda *a, **k: set())
    monkeypatch.setattr(st, "fetch_sw_industry_intervals", lambda: pl.DataFrame({"x": [1]}))

    # Far fewer than the 1000-name floor must not be staged as a queryable
    # partial snapshot.
    thin = pl.DataFrame(
        {
            "symbol": [f"{i:06d}.SH" for i in range(5)],
            "classification_system": ["sw"] * 5,
            "industry_code": ["240301"] * 5,
            "industry_name": ["铝"] * 5,
            "as_of_date": [date(2024, 1, 31)] * 5,
        }
    )
    monkeypatch.setattr(st, "expand_sw_industry_as_of", lambda intervals, todo: thin)
    with pytest.raises(RuntimeError, match="all requested Shenwan as-of snapshots"):
        st.step_industry_members(cfg, date(2024, 1, 31), "run-thin", {})


def test_industry_members_backfill_drops_thin_month_and_keeps_healthy_month(cfg, monkeypatch):
    cfg._backfill = True
    cfg._backfill_start = date(2024, 1, 1)
    cfg._backfill_end = date(2024, 2, 29)
    healthy_date = date(2024, 2, 29)
    thin_date = date(2024, 1, 31)
    missing_date = date(2024, 2, 28)
    monkeypatch.setattr(
        st,
        "_month_end_trading_days",
        lambda *a, **k: [thin_date, missing_date, healthy_date],
    )
    monkeypatch.setattr(st, "_existing_sw_as_of_dates", lambda *a, **k: set())
    monkeypatch.setattr(st, "_existing_as_of_dates", lambda *a, **k: set())
    monkeypatch.setattr(st, "fetch_sw_industry_intervals", lambda: pl.DataFrame({"x": [1]}))

    healthy = pl.DataFrame(
        {
            "symbol": [f"{i:06d}.SH" for i in range(1000)],
            "classification_system": ["sw"] * 1000,
            "industry_code": ["240301"] * 1000,
            "industry_name": ["铝"] * 1000,
            "as_of_date": [healthy_date] * 1000,
        }
    )
    thin = healthy.head(5).with_columns(pl.lit(thin_date).alias("as_of_date"))
    monkeypatch.setattr(
        st, "expand_sw_industry_as_of", lambda intervals, todo: pl.concat([thin, healthy])
    )

    result = st.step_industry_members(cfg, date(2024, 2, 29), "run-mixed", {})
    assert result["rows_written"] == 1000
    assert result["as_of_dates"] == 1
    assert result["status"] == "warning"
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["code"] == "sw_industry_thin_months"
    assert finding["thin_as_of_dates"] == [thin_date.isoformat()]
    assert finding["missing_as_of_dates"] == [missing_date.isoformat()]


def test_existing_sw_as_of_dates_filters_source(cfg):
    part = cfg.curated_root / "industry_members" / "as_of_date=2024-06"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "classification_system": ["em", "sw"],
            "industry_code": ["白酒", "240301"],
            "industry_name": ["白酒", "铝"],
            "as_of_date": [date(2024, 6, 28), date(2024, 6, 28)],
            "source": ["eastmoney", "sw"],
            "data_version": ["v1", "v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"] * 2,
        }
    ).write_parquet(part / "part-000.parquet")
    assert st._existing_sw_as_of_dates(cfg) == {date(2024, 6, 28)}
    assert st._existing_sw_as_of_dates(cfg, min_rows=2) == set()
    assert st._existing_sw_as_of_dates(cfg, min_rows=1) == {date(2024, 6, 28)}


def test_existing_sw_as_of_dates_does_not_count_duplicate_members(cfg):
    part = cfg.curated_root / "industry_members" / "as_of_date=2024-06"
    part.mkdir(parents=True)
    unique = [f"{i:06d}.SH" for i in range(999)]
    symbols = unique + [unique[0]]
    pl.DataFrame(
        {
            "symbol": symbols,
            "classification_system": ["sw"] * len(symbols),
            "industry_code": ["240301"] * len(symbols),
            "industry_name": ["铝"] * len(symbols),
            "as_of_date": [date(2024, 6, 28)] * len(symbols),
            "source": ["sw"] * len(symbols),
            "data_version": ["v1"] * len(symbols),
            "fetched_at": ["2024-06-28T00:00:00+00:00"] * len(symbols),
        }
    ).write_parquet(part / "part-000.parquet")

    assert st._existing_sw_as_of_dates(cfg, min_rows=1000) == set()
