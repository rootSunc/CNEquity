from datetime import date

import polars as pl

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.orchestrator.engine import JobEngine
from cnequity.run_scope import set_backfill
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
    set_backfill(cfg, start=date(2024, 6, 28), end=date(2024, 6, 29))
    calls: list[date] = []

    def fake_fetch(trade_date, **kwargs):
        calls.append(trade_date)
        return pl.DataFrame(
            {
                "symbol": [
                    f"{600000 + i:06d}.SH" if i % 2 == 0 else f"{i:06d}.SZ" for i in range(50)
                ],
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
