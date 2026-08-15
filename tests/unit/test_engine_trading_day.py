from datetime import date

import polars as pl

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.steps.common import is_trading_day
from cn_market_lake.storage.layout import init_data_layout


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
    from cn_market_lake.steps import capital as cap

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
                "symbol": ["600519.SH"],
                "trade_date": [trade_date],
                "margin_balance": [1.0],
                "margin_buy": [0.0],
                "short_balance": [0.0],
                "short_sell_volume": [0.0],
            }
        )

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
