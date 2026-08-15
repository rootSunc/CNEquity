from datetime import date

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.steps.ticks import (
    DATASET,
    TradeTicksScopeError,
    _sessions,
    capture_trade_ticks,
    resolve_scope,
)


def _config(tmp_path, **over) -> Config:
    config = Config(data_root=tmp_path)
    config.trade_ticks_enabled = True
    config.trade_ticks_scope = "watchlist"
    config.trade_ticks_symbols = ["600519.SH", "000001.SZ"]
    for key, value in over.items():
        setattr(config, key, value)
    return config


def _write_calendar(config: Config, days: list[date], holidays: list[date] = ()) -> None:
    root = config.curated_root / "trading_calendar" / "trade_date=2026"
    root.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": [*days, *holidays],
            "is_trading": [True] * len(days) + [False] * len(holidays),
        }
    ).write_parquet(root / "part-0.parquet")


def test_watchlist_scope_returns_exactly_what_was_configured(tmp_path):
    assert resolve_scope(_config(tmp_path)) == ["600519.SH", "000001.SZ"]


def test_an_empty_watchlist_is_an_error(tmp_path):
    with pytest.raises(TradeTicksScopeError, match="symbols is empty"):
        resolve_scope(_config(tmp_path, trade_ticks_symbols=[]))


def test_an_unknown_scope_is_an_error(tmp_path):
    with pytest.raises(TradeTicksScopeError, match="unknown"):
        resolve_scope(_config(tmp_path, trade_ticks_scope="all"))


def test_beijing_symbols_are_dropped_rather_than_failing_every_session(tmp_path):
    # TDX has no Beijing tick route and the adapter raises on those, so leaving
    # them in a scope would turn one config typo into a wall of failures.
    config = _config(tmp_path, trade_ticks_symbols=["600519.SH", "920003.BJ"])
    assert resolve_scope(config) == ["600519.SH"]


def test_the_ceiling_stops_a_scope_before_a_single_request(tmp_path):
    config = _config(
        tmp_path,
        trade_ticks_symbols=[f"6000{i:02d}.SH" for i in range(10)],
        trade_ticks_max_symbols=5,
    )
    with pytest.raises(TradeTicksScopeError) as excinfo:
        resolve_scope(config)
    message = str(excinfo.value)
    assert "over the max_symbols ceiling of 5" in message
    # The cost is quoted so the number is a decision, not a guess.
    assert "requests" in message and "MB per session" in message


def test_sessions_come_from_the_calendar_not_from_weekdays(tmp_path):
    config = _config(tmp_path)
    config._backfill = True
    config._backfill_start = date(2026, 7, 27)
    config._backfill_end = date(2026, 7, 31)
    # 2026-07-29 is marked non-trading; a weekday fallback would include it.
    trading = [date(2026, 7, 27), date(2026, 7, 28), date(2026, 7, 30), date(2026, 7, 31)]
    _write_calendar(config, trading, holidays=[date(2026, 7, 29)])
    assert _sessions(config, date(2026, 7, 31)) == trading


def test_sessions_clamp_to_the_history_floor(tmp_path, caplog):
    config = _config(tmp_path)
    config._backfill = True
    config._backfill_start = date(2020, 1, 1)  # long before the floor
    config._backfill_end = date(2024, 1, 3)
    _write_calendar(config, [date(2024, 1, 2), date(2024, 1, 3)])
    sessions = _sessions(config, date(2026, 7, 31))
    assert sessions == [date(2024, 1, 2), date(2024, 1, 3)]
    assert "clamping" in caplog.text


def test_sessions_are_empty_when_the_window_inverts(tmp_path):
    config = _config(tmp_path)
    config._backfill = True
    config._backfill_start = date(2026, 7, 31)
    config._backfill_end = date(2026, 7, 1)
    assert _sessions(config, date(2026, 7, 31)) == []


def test_a_disabled_capture_does_nothing_and_says_so(tmp_path):
    config = _config(tmp_path, trade_ticks_enabled=False)
    result = capture_trade_ticks(config, date(2026, 7, 31), "run-1")
    assert result["rows_written"] == 0
    assert "disabled" in result["note"]


def test_no_sessions_in_window_is_reported_not_raised(tmp_path):
    config = _config(tmp_path)
    config._backfill = True
    config._backfill_start = date(2026, 7, 31)
    config._backfill_end = date(2026, 7, 1)
    result = capture_trade_ticks(config, date(2026, 7, 31), "run-1")
    assert result["rows_written"] == 0
    assert "no trading sessions" in result["note"]


def test_failed_symbol_days_become_an_audit_finding(tmp_path, monkeypatch):
    config = _config(tmp_path)
    config._backfill = True
    config._backfill_start = date(2026, 7, 30)
    config._backfill_end = date(2026, 7, 31)
    _write_calendar(config, [date(2026, 7, 30), date(2026, 7, 31)])

    rows = [
        {
            "symbol": "600519.SH",
            "trade_date": date(2026, 7, 30),
            "tick_seq": 0,
            "trade_time": None,
            "price": 1.0,
            "volume": 100,
            "direction": "buy",
        }
    ]

    def _fake_batch(symbols, sessions, **kwargs):
        return pl.DataFrame(rows), ["000001.SZ@2026-07-30"]

    monkeypatch.setattr("cn_market_lake.steps.ticks.fetch_trade_ticks_batch", _fake_batch)
    result = capture_trade_ticks(config, date(2026, 7, 31), "run-1")
    assert result["failed_symbol_days"] == 1
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["dataset"] == DATASET
    assert finding["check"] == "trade_ticks_symbol_day_fetch"
    assert "000001.SZ@2026-07-30" in finding["message"]


def test_a_sweep_that_returns_nothing_at_all_raises(tmp_path, monkeypatch):
    # Distinct from "some symbols were quiet": every symbol coming back empty
    # means the source or the window is wrong, not that nobody traded.
    config = _config(tmp_path)
    config._backfill = True
    config._backfill_start = date(2026, 7, 30)
    config._backfill_end = date(2026, 7, 31)
    _write_calendar(config, [date(2026, 7, 30), date(2026, 7, 31)])
    monkeypatch.setattr(
        "cn_market_lake.steps.ticks.fetch_trade_ticks_batch",
        lambda symbols, sessions, **kwargs: (pl.DataFrame(), []),
    )
    with pytest.raises(RuntimeError, match="no rows for any of"):
        capture_trade_ticks(config, date(2026, 7, 31), "run-1")


def test_a_batch_failing_outright_costs_only_that_batch(tmp_path, monkeypatch):
    config = _config(tmp_path)
    config._backfill = True
    config._backfill_start = date(2026, 7, 30)
    config._backfill_end = date(2026, 7, 30)
    _write_calendar(config, [date(2026, 7, 30)])
    monkeypatch.setattr(
        "cn_market_lake.steps.ticks.fetch_trade_ticks_batch",
        lambda symbols, sessions, **kwargs: (_ for _ in ()).throw(ConnectionError("boom")),
    )
    with pytest.raises(RuntimeError, match="no rows for any of"):
        capture_trade_ticks(config, date(2026, 7, 31), "run-1")
