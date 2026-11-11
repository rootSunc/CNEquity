"""Partial index facts are deliverable without certifying missing series."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import polars as pl
import pytest

from cnequity.adapters.tdx_protocol.client import INDEX_SYMBOLS, TdxSourceError, fetch_index_bars
from cnequity.config import Config
from cnequity.steps.bars import step_index_bars
from cnequity.storage import StagingWriter
from cnequity.storage.state import StateStore


@pytest.mark.parametrize("allow_partial", [False, True])
def test_fetch_index_bars_preserves_partial_rows_when_requested(monkeypatch, allow_partial):
    """Command callers retain the surviving series; strict callers can reject it."""

    def fake_paginated(client, sym, start, end, **kwargs):
        if sym == "000852.SH":
            return [
                {
                    "symbol": sym,
                    "trade_date": start,
                    "open": 1.0,
                    "high": 1.0,
                    "low": 1.0,
                    "close": 1.0,
                    "volume": 1,
                    "amount": 1.0,
                }
            ]
        raise RuntimeError(f"tdx down for {sym}")

    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client.fetch_bars_paginated",
        fake_paginated,
    )
    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client._quotes_client",
        lambda config=None: MagicMock(),
    )
    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client._close_quotes_client",
        lambda client: None,
    )
    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client.reset_tdx_server_cache",
        lambda: None,
    )
    # Exhaust retries quickly without sleeping on mock path.
    monkeypatch.setattr(
        "cnequity.adapters.tdx_protocol.client._TDX_FETCH_ATTEMPTS",
        1,
    )

    if allow_partial:
        frame = fetch_index_bars(
            date(2026, 7, 10), date(2026, 7, 10), allow_partial=True, backfill=True
        )
        assert frame["symbol"].to_list() == ["000852.SH"]
    else:
        with pytest.raises(TdxSourceError, match="000001.SH"):
            fetch_index_bars(date(2026, 7, 10), date(2026, 7, 10), allow_mock=False)


def test_index_bar_step_stages_valid_rows_and_records_interior_symbol_session_gap(
    tmp_path, monkeypatch
):
    cfg = Config(data_root=tmp_path / "data")
    symbols = [f"{code}.{exchange}" for code, exchange in INDEX_SYMBOLS]
    days = [date(2024, 6, 3), date(2024, 6, 4), date(2024, 6, 5)]
    rows = [
        {
            "symbol": symbol,
            "trade_date": day,
            "frequency": "1d",
            "open": 1.0,
            "high": 1.0,
            "low": 1.0,
            "close": 1.0,
            "volume": 1,
            "amount": 1.0,
        }
        for symbol in symbols
        for day in days
        if not (symbol == "000300.SH" and day == date(2024, 6, 4))
    ]
    frame = pl.DataFrame(rows)
    monkeypatch.setattr("cnequity.steps.bars.daily.incremental_window", lambda *args: days[0])
    monkeypatch.setattr(
        "cnequity.steps.bars.index_history.incremental_window", lambda *args: days[0]
    )
    monkeypatch.setattr("cnequity.steps.bars.admission.list_trading_dates", lambda *args: days)
    monkeypatch.setattr("cnequity.steps.bars.bar_scope.list_trading_dates", lambda *args: days)
    monkeypatch.setattr("cnequity.steps.bars.daily.list_trading_dates", lambda *args: days)
    monkeypatch.setattr("cnequity.steps.bars.finish.list_trading_dates", lambda *args: days)
    monkeypatch.setattr("cnequity.steps.bars.gapfill.list_trading_dates", lambda *args: days)
    monkeypatch.setattr("cnequity.steps.bars.gapfill_chain.list_trading_dates", lambda *args: days)
    monkeypatch.setattr("cnequity.steps.bars.index_history.list_trading_dates", lambda *args: days)
    monkeypatch.setattr("cnequity.steps.bars.sina_bars.list_trading_dates", lambda *args: days)
    monkeypatch.setattr("cnequity.steps.bars.staging.list_trading_dates", lambda *args: days)
    monkeypatch.setattr("cnequity.steps.bars.tip_fetch.list_trading_dates", lambda *args: days)
    monkeypatch.setattr(
        "cnequity.steps.bars.index_history.fetch_index_bars", lambda *args, **kwargs: frame
    )

    result = step_index_bars(cfg, days[-1], "index-gap", {})

    assert result["status"] == "warning"
    assert result["coverage_status"] == "partial"
    assert result["missing_keys"] == 1
    assert result["rows_written"] == len(rows)
    staged = pl.read_parquet(
        StagingWriter(cfg.staging_root).list_run_files("index_bars", "index-gap")
    )
    assert set(staged.select("symbol", "trade_date").iter_rows()) == {
        (row["symbol"], row["trade_date"]) for row in rows
    }
    outstanding = StateStore(cfg.meta_root).get_outstanding_keys("index_bars")
    assert [(row["symbol"], row["trade_date"]) for row in outstanding] == [
        ("000300.SH", "2024-06-04")
    ]


def test_step_index_bars_applies_finality_guard_before_fetch(config, monkeypatch):
    events: list[str] = []

    def stop_before_fetch(*args, **kwargs):
        events.append("guard")
        raise RuntimeError("stop before fetch")

    monkeypatch.setattr(
        "cnequity.steps.bars.bar_scope._reject_unfinished_daily_bar_window", stop_before_fetch
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.daily.incremental_window", lambda *args, **kwargs: date(2026, 8, 10)
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.index_history.incremental_window",
        lambda *args, **kwargs: date(2026, 8, 10),
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.index_history.fetch_index_bars",
        lambda *args, **kwargs: events.append("fetch"),
    )

    with pytest.raises(RuntimeError, match="stop before fetch"):
        step_index_bars(config, date(2026, 8, 10), "run-index-guard", {})

    assert events == ["guard"]
