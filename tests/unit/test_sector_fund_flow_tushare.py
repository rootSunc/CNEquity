"""Offline tests for the Tushare moneyflow_ind_dc route into sector_fund_flow."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.adapters.tushare import sector_fund_flow as tsf
from cnequity.config import Config
from cnequity.orchestrator.outcomes import SourcePayloadError
from cnequity.storage.parquet import StagingWriter

FIELDS = tsf._FIELDS.split(",")


def _row(code, kind="行业", day="20260703", name="板块", pct=1.2, net=3.5e8):
    return [day, kind, code, name, pct, net]


def test_normalize_maps_board_types_and_keeps_yuan():
    frame = tsf.normalize(
        FIELDS,
        [
            _row("BK0475.DC", "行业", name="银行"),
            _row("BK0145.DC", "地域", name="上海板块"),
            _row("BK1027.DC", "概念", name="小金属"),
            _row("BK9999.DC", "其他"),
        ],
    )
    assert frame.select("sector_code", "board_type").rows() == [
        ("BK0475", "industry"),
        ("BK0145", "region"),
        ("BK1027", "concept"),
    ]
    assert frame["main_net_inflow"][0] == 3.5e8
    assert frame["turnover_pct"].null_count() == 3


def _cfg(tmp_path, **kw):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"tushare": True},
        source_intervals={"tushare": 0},
        tushare_token="tok",
        **kw,
    )
    for day in (date(2026, 7, 2), date(2026, 7, 3)):
        part = cfg.curated_root / "trading_calendar" / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True)
        pl.DataFrame(
            {
                "trade_date": [day],
                "is_trading": [True],
                "source": ["seed"],
                "data_version": ["v1"],
                "fetched_at": ["2026-07-03T00:00:00+00:00"],
            }
        ).write_parquet(part / "part-merged.parquet")
    return cfg


def _session(day: date, industries=500, concepts=500):
    stamp = day.strftime("%Y%m%d")
    rows = [_row(f"BK{i:04d}.DC", "行业", stamp) for i in range(industries)]
    rows += [_row(f"BK{5000 + i:04d}.DC", "概念", stamp) for i in range(concepts)]
    return tsf.normalize(FIELDS, rows)


def test_backfill_adds_only_boards_the_day_lacks(tmp_path, monkeypatch):
    from cnequity.steps import rotation

    cfg = _cfg(tmp_path)
    held = cfg.curated_root / "sector_fund_flow" / "trade_date=2026-07"
    held.mkdir(parents=True)
    pl.DataFrame(
        {
            "sector_code": ["BK0001"],
            "sector_name": ["东财"],
            "board_type": ["industry"],
            "trade_date": [date(2026, 7, 3)],
            "main_net_inflow": [1.0],
            "change_pct": [0.1],
            "turnover_pct": [0.5],
            "source": ["eastmoney"],
            "data_version": ["v1"],
        }
    ).with_columns(
        pl.lit("2026-07-03T09:00:00+00:00").str.to_datetime(time_zone="UTC").alias("fetched_at")
    ).write_parquet(held / "part-merged.parquet")
    cfg._backfill = True
    cfg._backfill_start = date(2026, 7, 2)
    cfg._backfill_end = date(2026, 7, 3)
    monkeypatch.setattr(
        tsf, "fetch_sector_fund_flow", lambda d, *, config, client=None: _session(d, 2, 1)
    )

    rotation.step_sector_fund_flow(cfg, date(2026, 7, 3), "run-bf", {})

    staged = pl.concat(
        [
            pl.read_parquet(p)
            for p in StagingWriter(cfg.staging_root).list_run_files("sector_fund_flow", "run-bf")
        ]
    )
    keys = sorted(staged.select("sector_code", "trade_date").iter_rows())
    assert ("BK0001", date(2026, 7, 3)) not in keys
    assert len(keys) == 5
    assert set(staged["source"]) == {"tushare"}


def test_paused_push2_takes_the_session_from_tushare(tmp_path, monkeypatch):
    from cnequity.steps import rotation

    cfg = _cfg(tmp_path, eastmoney_push2_paused=True)
    monkeypatch.setattr(
        tsf, "fetch_sector_fund_flow", lambda d, *, config, client=None: _session(d)
    )
    result = rotation.step_sector_fund_flow(cfg, date(2026, 7, 3), "run-d", {})
    assert result["rows_written"] == 1000


def test_a_session_tushare_has_not_finished_is_not_written(tmp_path, monkeypatch):
    from cnequity.steps import rotation

    cfg = _cfg(tmp_path, eastmoney_push2_paused=True)
    monkeypatch.setattr(
        tsf, "fetch_sector_fund_flow", lambda d, *, config, client=None: _session(d, 86, 0)
    )
    monkeypatch.setattr("cnequity.steps.ths_fallback.stage_ths_fallback", lambda *a, **k: None)
    with pytest.raises((SourcePayloadError, RuntimeError)):
        rotation.step_sector_fund_flow(cfg, date(2026, 7, 3), "run-d", {})
    assert not StagingWriter(cfg.staging_root).list_run_files("sector_fund_flow", "run-d")
