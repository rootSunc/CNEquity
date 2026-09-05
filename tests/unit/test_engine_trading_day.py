from datetime import date

import polars as pl

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.orchestrator.engine import JobEngine
from cnequity.steps.common import is_trading_day
from cnequity.storage.layout import init_data_layout


def _seed_calendar(cfg: Config, rows: list[dict]) -> None:
    path = cfg.curated_root / "trading_calendar" / "part-merged.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(path)


def test_is_trading_day_reads_curated_calendar(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    _seed_calendar(
        cfg,
        [
            {"trade_date": date(2024, 6, 28), "is_trading": True},
            {"trade_date": date(2024, 6, 29), "is_trading": False},
        ],
    )
    assert is_trading_day(cfg, date(2024, 6, 28)) is True
    assert is_trading_day(cfg, date(2024, 6, 29)) is False


def test_run_job_skips_non_trading_day(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    _seed_calendar(
        cfg,
        [{"trade_date": date(2024, 6, 29), "is_trading": False}],
    )
    engine = JobEngine(cfg)
    result = engine.run_job("daily", date(2024, 6, 29))
    assert result["status"] == "skipped_non_trading_day"
    assert result["trade_date"] == "2024-06-29"
    assert not list(cfg.staging_root.glob("**/*.parquet"))


def test_run_job_backfill_does_not_skip_weekend(tmp_path, monkeypatch):
    from cnequity.steps import capital as cap

    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    # Weekend job date must still run; margin backfill walks trading days only.
    _seed_calendar(
        cfg,
        [
            {"trade_date": date(2024, 6, 28), "is_trading": True},
            {"trade_date": date(2024, 6, 29), "is_trading": False},
        ],
    )
    cfg._backfill_start = date(2024, 6, 28)
    cfg._backfill_end = date(2024, 6, 29)
    calls: list[date] = []

    def fake_fetch(trade_date, **kwargs):
        calls.append(trade_date)
        return pl.DataFrame(
            {
                "symbol": [f"{600000 + i:06d}.SH" for i in range(50)],
                "trade_date": [trade_date] * 50,
                "margin_balance": [1.0] * 50,
                "margin_buy": [0.0] * 50,
                "short_balance": [0.0] * 50,
                "short_sell_volume": [0.0] * 50,
            }
        )

    # About the trading-day walk, not the vendor: pin the source so this does
    # not reach the exchanges (the default) for a 2024 session.
    cfg.margin_trading_source = "eastmoney"
    monkeypatch.setattr(cap, "fetch_margin_trading", fake_fetch)
    engine = JobEngine(cfg)
    result = engine.run_job(
        "backfill",
        date(2024, 6, 29),
        steps=["margin_trading"],
        backfill=True,
    )
    assert result["status"] == "success"
    assert calls == [date(2024, 6, 28)]


def test_run_init_not_skipped_on_weekend(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    _seed_calendar(
        cfg,
        [{"trade_date": date(2024, 6, 29), "is_trading": False}],
    )
    engine = JobEngine(cfg)
    result = engine.run_job(
        "init",
        date(2024, 6, 29),
        steps=["trading_calendar"],
    )
    assert result["status"] != "skipped_non_trading_day"


def test_event_groups_run_on_weekend_while_daily_groups_gated(tmp_path, monkeypatch):
    from cnequity.config import ScheduleGroup
    from cnequity.orchestrator.registry import register_step

    cfg = Config(
        data_root=tmp_path / "data",
        schedule_groups={"capital": ScheduleGroup(at="17:00", steps=["test_dummy_step"])},
        event_groups={
            "corporate_events": ScheduleGroup(at="events", steps=["test_dummy_step"]),
            "news_wire": ScheduleGroup(at="events", steps=["test_dummy_step"]),
        },
    )
    init_data_layout(cfg)
    _seed_calendar(
        cfg,
        [{"trade_date": date(2024, 6, 29), "is_trading": False}],
    )

    executed: list[str] = []

    @register_step("test_dummy_step", group="corporate_events")
    def _dummy(config, trade_date, run_id, context):
        executed.append(f"{run_id}:{trade_date}")
        return {"status": "success", "rows_read": 0, "rows_written": 0}

    engine = JobEngine(cfg)
    weekend = date(2024, 6, 29)

    # 1. Daily group must be gated and skipped on weekend
    daily_res = engine.run_job(
        "daily:capital",
        weekend,
        steps=["test_dummy_step"],
    )
    assert daily_res["status"] == "skipped_non_trading_day"
    assert len(executed) == 0

    # 2. Event group must bypass is_trading_day gate on weekend
    event_res = engine.run_job(
        "events:corporate_events",
        weekend,
        steps=["test_dummy_step"],
    )
    assert event_res["status"] == "success"
    assert len(executed) == 1

    # 3. Verify Manifest records distinct job type
    run_record = engine.manifest.get_run(event_res["run_id"])
    assert run_record is not None
    assert run_record["job_name"] == "events:corporate_events"

    # 4. Daily group with ignore_calendar=True also bypasses gate
    daily_ignore_res = engine.run_job(
        "daily:capital",
        weekend,
        steps=["test_dummy_step"],
        ignore_calendar=True,
    )
    assert daily_ignore_res["status"] == "success"
    assert len(executed) == 2

