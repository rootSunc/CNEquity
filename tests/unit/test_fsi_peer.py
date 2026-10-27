"""The daily check of new statements against 通达信's quarterly package."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.steps import fsi_peer

DAY = date(2026, 10, 28)
_SCHEMA = {
    "symbol": pl.Utf8,
    "report_period": pl.Utf8,
    "statement_type": pl.Utf8,
    "item_code": pl.Utf8,
    "item_value": pl.Float64,
    "announce_date": pl.Date,
    "source": pl.Utf8,
}


def _rows(rows):
    return pl.DataFrame(rows, schema=_SCHEMA, orient="row")


def test_compare_reports_disagreements_and_filings_the_lake_lacks():
    lake = _rows(
        [
            ("600519.SH", "2026Q3", "income", "net_profit", 100.0, date(2026, 10, 25), "eastmoney"),
            ("000001.SZ", "2026Q3", "income", "net_profit", 50.0, date(2026, 10, 24), "eastmoney"),
            # Old filing: outside the recent window, so not compared.
            ("000002.SZ", "2026Q3", "income", "net_profit", 7.0, date(2026, 9, 1), "eastmoney"),
        ]
    )
    tdx = _rows(
        [
            ("600519.SH", "2026Q3", "income", "net_profit", 100.05, date(2026, 10, 25), "tdx_fin"),
            ("000001.SZ", "2026Q3", "income", "net_profit", 55.0, date(2026, 10, 24), "tdx_fin"),
            ("000002.SZ", "2026Q3", "income", "net_profit", 9.0, date(2026, 9, 1), "tdx_fin"),
            ("300750.SZ", "2026Q3", "income", "net_profit", 1.0, date(2026, 10, 27), "tdx_fin"),
            # Filed today: may still be on its way, so not called missing.
            ("601318.SH", "2026Q3", "income", "net_profit", 1.0, DAY, "tdx_fin"),
        ]
    )

    finding = fsi_peer.compare(lake, tdx, DAY)

    assert finding["check"] == "financial_statement_tdx_peer"
    assert finding["severity"] == "warning"
    assert finding["compared"] == 2
    assert finding["disagreed_sample"] == ["000001.SZ@2026Q3:net_profit"]
    assert finding["lacking_sample"] == ["300750.SZ@2026Q3 (2026-10-27)"]


def test_rows_the_lake_took_from_tdx_are_not_compared_with_tdx():
    lake = _rows(
        [("600519.SH", "2026Q3", "income", "net_profit", 1.0, date(2026, 10, 25), "tdx_fin")]
    )
    tdx = _rows(
        [("600519.SH", "2026Q3", "income", "net_profit", 1.0, date(2026, 10, 25), "tdx_fin")]
    )

    assert fsi_peer.compare(lake, tdx, DAY) is None


def test_a_package_is_downloaded_once_per_md5(tmp_path, monkeypatch):
    from cnequity.adapters.tdx_fin import packages

    cfg = Config(data_root=tmp_path / "lake")
    calls = []
    monkeypatch.setattr(
        packages, "fetch_package", lambda period, **_k: calls.append(period) or b"zip"
    )
    index = {"gpcw20260930.zip": "aaa"}

    assert fsi_peer._cached_package(cfg, "2026Q3", index) == b"zip"
    assert fsi_peer._cached_package(cfg, "2026Q3", index) == b"zip"
    assert calls == ["2026Q3"]
    # A new MD5 is a regenerated package: fetched again, and the old copy goes.
    fsi_peer._cached_package(cfg, "2026Q3", {"gpcw20260930.zip": "bbb"})
    assert calls == ["2026Q3", "2026Q3"]
    cached = sorted(p.name for p in (cfg.meta_root / "source_cache" / "tdx_fin").iterdir())
    assert cached == ["gpcw20260930.zip.bbb"]


def test_revenue_disagrees_only_when_tdx_operating_income_exceeds_the_total():
    lake = _rows(
        [
            ("300059.SZ", "2026Q3", "income", "revenue", 100.0, date(2026, 10, 25), "eastmoney"),
            ("600519.SH", "2026Q3", "income", "revenue", 50.0, date(2026, 10, 25), "eastmoney"),
        ]
    )
    income = pl.DataFrame(
        {
            "symbol": ["300059.SZ", "600519.SH"],
            "report_period": ["2026Q3", "2026Q3"],
            # 营业收入 below 营业总收入 is a finance subsidiary, not an error;
            # above it cannot be right.
            "item_value": [22.0, 60.0],
        }
    )

    finding = fsi_peer.compare(lake, lake.clear(), DAY, income)

    assert finding["compared"] == 2
    assert finding["disagreed_sample"] == ["600519.SH@2026Q3:revenue"]


def test_backup_revenue_only_for_companies_whose_last_period_matched(tmp_path, monkeypatch):
    from cnequity.adapters.tdx_fin import packages
    from cnequity.steps import fundamentals

    cfg = Config(data_root=tmp_path / "lake")
    part = cfg.curated_root / "financial_statement_items" / "report_period=2026Q2"
    part.mkdir(parents=True)
    _rows(
        [
            ("600519.SH", "2026Q2", "income", "revenue", 80.0, date(2026, 8, 20), "eastmoney"),
            ("300059.SZ", "2026Q2", "income", "revenue", 90.0, date(2026, 8, 20), "eastmoney"),
        ]
    ).with_columns(
        pl.lit("v1").alias("data_version"),
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("fetched_at"),
    ).write_parquet(part / "part-merged.parquet")
    incomes = {
        "2026Q2": {"600519.SH": 80.0, "300059.SZ": 20.0},
        "2026Q3": {"600519.SH": 120.0, "300059.SZ": 30.0},
    }
    monkeypatch.setattr(packages, "operating_income", lambda period, _content: incomes[period])
    monkeypatch.setattr(fsi_peer, "_cached_package", lambda *_a: b"prior")
    frame = _rows(
        [
            ("600519.SH", "2026Q3", "income", "net_profit", 9.0, date(2026, 10, 25), "tdx_fin"),
            ("300059.SZ", "2026Q3", "income", "net_profit", 3.0, date(2026, 10, 25), "tdx_fin"),
        ]
    )

    rows = fundamentals._tdx_fin_revenue_rows(cfg, "2026Q3", b"now", frame, {})

    assert rows.select("symbol", "item_code", "item_value", "announce_date", "source").rows() == [
        ("600519.SH", "revenue", 120.0, date(2026, 10, 25), "tdx_fin")
    ]
