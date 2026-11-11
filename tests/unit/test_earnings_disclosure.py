"""Offline tests for EastMoney earnings disclosure schedule adapter."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.adapters.eastmoney.common import report_period_from_date
from cnequity.adapters.eastmoney.earnings_disclosure import (
    _active_report_dates,
    _backfill_report_dates,
    _parse_rows,
    fetch_earnings_disclosure_schedule,
)
from cnequity.config import Config
from cnequity.domain.datasets import get_dataset
from cnequity.domain.schemas import validate_dataframe
from cnequity.run_scope import set_backfill


def test_report_period_from_date():
    assert report_period_from_date("2026-03-31 00:00:00") == "2026Q1"
    assert report_period_from_date("2024-06-30") == "2024Q2"
    assert report_period_from_date("2023-09-30") == "2023Q3"
    assert report_period_from_date("2022-12-31") == "2022Q4"
    assert report_period_from_date(None) is None
    assert report_period_from_date("2024-06-31") is None
    assert report_period_from_date("2024-13-31") is None


def test_active_report_dates_covers_nearby_quarters():
    dates = _active_report_dates(date(2026, 7, 16))
    assert "2026-03-31" in dates
    assert "2026-06-30" in dates


def test_backfill_report_dates_floor_is_2006_not_2016():
    """Measured 2026-08: RPT_PUBLIC_BS_APPOIN returns real rows at 2006-12-31
    and is empty at 2005-12-31 — 2016 was an unverified guess."""
    dates = _backfill_report_dates(date(2026, 7, 16))
    assert "2006-12-31" in dates
    assert "2005-12-31" not in dates
    assert "2026-09-30" not in dates


def test_backfill_report_dates_honor_explicit_backfill_window():
    dates = _backfill_report_dates(date(2026, 7, 16), start=date(2024, 1, 1), end=date(2024, 6, 30))
    assert dates == ["2024-03-31", "2024-06-30"]


def test_parse_rows_maps_a_share_and_skips_neeq():
    rows = _parse_rows(
        [
            {
                "SECUCODE": "600519.SH",
                "SECURITY_CODE": "600519",
                "REPORT_DATE": "2026-06-30 00:00:00",
                "APPOINT_PUBLISH_DATE": "2026-08-20",
                "FIRST_APPOINT_DATE": "2026-07-01",
                "ACTUAL_PUBLISH_DATE": None,
            },
            {
                "SECUCODE": "834948.NQ",
                "SECURITY_CODE": "834948",
                "REPORT_DATE": "2026-06-30 00:00:00",
                "APPOINT_PUBLISH_DATE": "2026-08-01",
                "FIRST_APPOINT_DATE": "2026-08-01",
                "ACTUAL_PUBLISH_DATE": None,
            },
        ]
    )
    assert len(rows) == 1
    assert rows[0]["symbol"] == "600519.SH"
    assert rows[0]["report_period"] == "2026Q2"
    assert rows[0]["scheduled_date"] == date(2026, 8, 20)
    assert rows[0]["first_scheduled_date"] == date(2026, 7, 1)
    assert rows[0]["actual_date"] is None


def test_fetch_earnings_disclosure_schedule_parses(monkeypatch):
    class _Client:
        def close(self):
            pass

    def fake_dc(client, report, columns, **kwargs):
        assert report == "RPT_PUBLIC_BS_APPOIN"
        filt = kwargs.get("filter_expr", "")
        if "2026-06-30" not in filt:
            return []
        return [
            {
                "SECUCODE": "000001.SZ",
                "SECURITY_CODE": "000001",
                "REPORT_DATE": "2026-06-30 00:00:00",
                "APPOINT_PUBLISH_DATE": "2026-08-15",
                "FIRST_APPOINT_DATE": "2026-08-15",
                "ACTUAL_PUBLISH_DATE": "2026-08-15",
            }
        ]

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.earnings_disclosure.fetch_datacenter",
        fake_dc,
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.earnings_disclosure.EastMoneyClient",
        lambda **kwargs: _Client(),
    )
    df = fetch_earnings_disclosure_schedule(date(2026, 7, 16))
    assert df.height >= 1
    row = df.filter(pl.col("symbol") == "000001.SZ")
    assert row["report_period"][0] == "2026Q2"
    assert row["actual_date"][0] == date(2026, 8, 15)
    out = validate_dataframe(
        row.with_columns(
            source=pl.lit("eastmoney"),
            data_version=pl.lit("v1"),
            fetched_at=pl.lit("2026-07-16T00:00:00+00:00"),
        ),
        "earnings_disclosure_schedule",
    )
    assert out.height == 1


def test_fetch_earnings_disclosure_rejects_rows_from_another_report_period(monkeypatch):
    class _Client:
        def close(self):
            pass

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.earnings_disclosure._active_report_dates",
        lambda _trade_date: ["2026-06-30"],
    )

    def fake_dc(client, report, columns, **kwargs):
        return [
            {
                "SECUCODE": "000001.SZ",
                "SECURITY_CODE": "000001",
                "REPORT_DATE": "2026-03-31 00:00:00",
                "APPOINT_PUBLISH_DATE": "2026-05-01",
                "FIRST_APPOINT_DATE": "2026-05-01",
                "ACTUAL_PUBLISH_DATE": "2026-05-01",
            }
        ]

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.earnings_disclosure.fetch_datacenter",
        fake_dc,
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.earnings_disclosure.EastMoneyClient",
        lambda **kwargs: _Client(),
    )

    with pytest.raises(RuntimeError, match="REPORT_DATE row for 2026-06-30"):
        fetch_earnings_disclosure_schedule(date(2026, 7, 16))


def test_earnings_disclosure_uses_report_period_without_watermark():
    spec = get_dataset("earnings_disclosure_schedule")
    assert spec.partition_col == "report_period"
    assert spec.watermark is False


def test_earnings_disclosure_backfill_surfaces_missing_periods(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, active=True, start=date(2020, 1, 1), end=date(2020, 6, 30))
    monkeypatch.setattr(
        "cnequity.steps.events.fetch_earnings_disclosure_schedule",
        lambda *args, **kwargs: pl.DataFrame(),
    )

    from cnequity.steps.events import step_earnings_disclosure_schedule

    result = step_earnings_disclosure_schedule(cfg, date(2026, 6, 30), "run-gap", {})

    assert result["status"] == "warning"
    assert result["missing_periods"] == 2
    assert result["context_updates"]["audit_findings"][0]["check"] == (
        "backfill_missing_report_periods"
    )


def test_earnings_disclosure_step_rejects_partial_period(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.steps.events.fetch_earnings_disclosure_schedule",
        lambda *args, **kwargs: pl.DataFrame(
            {
                "symbol": ["000001.SZ"],
                "report_period": ["2026Q2"],
                "scheduled_date": [date(2026, 8, 15)],
                "first_scheduled_date": [date(2026, 8, 15)],
                "actual_date": [date(2026, 8, 15)],
            }
        ),
    )

    from cnequity.steps.events import step_earnings_disclosure_schedule

    with pytest.raises(
        RuntimeError, match="earnings_disclosure_schedule: incomplete report-period snapshot"
    ):
        step_earnings_disclosure_schedule(cfg, date(2026, 8, 16), "run-partial", {})
