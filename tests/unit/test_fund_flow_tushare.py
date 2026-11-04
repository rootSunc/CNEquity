"""Offline tests for the Tushare moneyflow_dc route into fund_flow."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.adapters.tushare import fund_flow as tff
from cnequity.config import Config
from cnequity.storage.parquet import StagingWriter

FIELDS = tff._FIELDS.split(",")


def _row(code, day="20260703", main=1.5, elg=1.0, lg=0.5, md=-0.7, sm=-0.8):
    return [code, day, main, elg, lg, md, sm]


def test_normalize_converts_to_cny_and_drops_untraded_codes():
    frame = tff.normalize(
        FIELDS,
        [
            _row("600519.SH"),
            _row("600087.SH", main=0.0, elg=0.0, lg=0.0, md=0.0, sm=0.0),
            _row("600519.SH", main=2.0),
        ],
    )
    assert frame.height == 1
    row = frame.row(0, named=True)
    assert row["trade_date"] == date(2026, 7, 3)
    assert row["main_net_inflow"] == 20_000.0  # 万元 -> CNY, last duplicate wins
    assert row["medium_net_inflow"] == -7_000.0
    assert row["small_net_inflow"] == -8_000.0


def test_legacy_beijing_codes_are_stored_under_the_current_code():
    frame = tff.normalize(FIELDS, [_row("430017.BJ", "20240603")])
    assert frame["symbol"].to_list() == ["920017.BJ"]


def _lake(tmp_path):
    root = tmp_path / "data"
    cfg = Config(
        data_root=root,
        sources={"tushare": True},
        source_intervals={"tushare": 0},
        tushare_token="tok",
    )
    days = [date(2026, 7, 2), date(2026, 7, 3)]
    for day in days:
        part = root / "curated" / "trading_calendar" / f"trade_date={day.isoformat()}"
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
    inst = root / "curated" / "instruments"
    inst.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "list_date": [date(2001, 8, 27), date(1991, 4, 3)],
            "delist_date": [None, None],
            "source": ["tdx"] * 2,
            "data_version": ["v1"] * 2,
            "fetched_at": ["2026-07-03T00:00:00+00:00"] * 2,
        }
    ).write_parquet(inst / "part-merged.parquet")
    held = root / "curated" / "fund_flow" / "trade_date=2026-07-03"
    held.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2026, 7, 3)],
            "main_net_inflow": [123.0],
            "super_large_net_inflow": [100.0],
            "large_net_inflow": [23.0],
            "medium_net_inflow": [None],
            "small_net_inflow": [None],
            "source": ["eastmoney"],
            "data_version": ["v1"],
        }
    ).with_columns(
        pl.lit("2026-07-03T09:00:00+00:00").str.to_datetime(time_zone="UTC").alias("fetched_at")
    ).write_parquet(held / "part-merged.parquet")
    return cfg


def test_backfill_adds_only_missing_stocks_and_never_replaces_eastmoney(tmp_path, monkeypatch):
    from cnequity.steps import capital

    cfg = _lake(tmp_path)
    cfg._backfill = True
    cfg._backfill_start = date(2026, 7, 2)
    cfg._backfill_end = date(2026, 7, 3)
    asked: list[date] = []

    def fake(day, *, config, client=None):
        asked.append(day)
        stamp = day.strftime("%Y%m%d")
        return tff.normalize(
            FIELDS,
            [_row("600519.SH", stamp), _row("000001.SZ", stamp), _row("200429.SZ", stamp)],
        )

    monkeypatch.setattr(tff, "fetch_fund_flow", fake)
    result = capital.step_fund_flow(cfg, date(2026, 7, 3), "run-bf", {})

    assert asked == [date(2026, 7, 2), date(2026, 7, 3)]
    staged = pl.concat(
        [
            pl.read_parquet(p)
            for p in StagingWriter(cfg.staging_root).list_run_files("fund_flow", "run-bf")
        ]
    )
    keys = sorted(staged.select("symbol", "trade_date").iter_rows())
    # 600519 on 07-03 stays EastMoney's; the B share is outside the universe.
    assert keys == [
        ("000001.SZ", date(2026, 7, 2)),
        ("000001.SZ", date(2026, 7, 3)),
        ("600519.SH", date(2026, 7, 2)),
    ]
    assert set(staged["source"]) == {"tushare"}
    assert result["rows_written"] == 3


def test_history_is_replayable_only_with_a_token(tmp_path):
    from cnequity.domain.datasets import DATASETS, history_mode_for

    spec = DATASETS["fund_flow"]
    assert history_mode_for(spec) == "snapshot_only"
    assert history_mode_for(spec, Config(data_root=tmp_path)) == "snapshot_only"
    assert history_mode_for(spec, _lake(tmp_path)) == "snapshot_with_backfill"
