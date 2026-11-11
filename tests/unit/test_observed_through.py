"""An event dataset is as fresh as the last time its source was asked.

Its newest row is the newest *event*, and a quiet window has none. Judging
freshness by that row alone made a demo lake's corporate actions stale for
good: `verify` offered a backfill, the backfill found nothing new (correctly),
and the next `verify` offered the same backfill again.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config import load_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.run_scope import backfill_of
from cnequity.steps import events
from cnequity.storage.state import StateStore

ANCHOR = date(2026, 10, 16)
LAST_EVENT = date(2026, 6, 26)
SYMBOLS = ["600519.SH", "000001.SZ", "000858.SZ", "300750.SZ", "601318.SH"]


def _event(symbol: str, ex_date: date) -> dict:
    return {
        "symbol": symbol,
        "ex_date": ex_date,
        "action_type": "cash_dividend",
        "cash_dividend": 1.0,
        "bonus_ratio": 0.0,
        "transfer_ratio": 0.0,
        "allotment_ratio": None,
        "allotment_price": None,
    }


@pytest.fixture
def demo_lake(tmp_path, monkeypatch):
    cfg_path = tmp_path / "demo.toml"
    cfg_path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "data")}"\nprofile = "demo"\n\n'
        "[orchestrator]\nworkers = 1\n\n[raw_archive]\nenabled = false\n",
        encoding="utf-8",
    )
    calls: list[tuple[date, date, list[str]]] = []
    history = [_event("600519.SH", date(2025, 6, 27)), _event("600519.SH", LAST_EVENT)]

    def fake_tdx(trade_date, *, symbols, config, **_kwargs):
        # Same window contract as the TDX adapter: rows outside it are dropped.
        start = backfill_of(config).start or date(1990, 1, 1)
        end = backfill_of(config).end or trade_date
        calls.append((start, end, list(symbols)))
        rows = [r for r in history if r["symbol"] in symbols and start <= r["ex_date"] <= end]
        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows).with_columns(pl.lit("tdx_protocol").alias("source"))

    monkeypatch.setattr(events, "load_symbols", lambda _cfg: list(SYMBOLS))
    monkeypatch.setattr(events, "fetch_corporate_actions", fake_tdx)
    monkeypatch.setattr(events, "_funds_ingested", lambda _cfg: True)
    monkeypatch.setattr("cnequity.cli.quality_cmds.last_trading_day", lambda cfg, today: ANCHOR)
    monkeypatch.setattr("cnequity.orchestrator.run_window.current_anchor", lambda cfg: ANCHOR)
    monkeypatch.setattr("cnequity.orchestrator.engine.shanghai_today", lambda: ANCHOR)
    monkeypatch.setattr("cnequity.cli.backfill_cmds.shanghai_today", lambda: ANCHOR)

    from cnequity.cli.backfill_cmds import _run_backfill

    cfg = load_config(cfg_path)
    first = _run_backfill(cfg, "corporate_actions", None, None)
    assert first["status"] == "success", first
    return cfg_path, calls


def _verify(cfg_path, *extra):
    return CliRunner().invoke(cli, ["verify", "--config", str(cfg_path), *extra])


def test_a_backfill_that_finds_nothing_new_still_closes_the_stale_gap(demo_lake):
    cfg_path, calls = demo_lake
    cfg = load_config(cfg_path)
    state = StateStore(cfg.meta_root)
    assert state.get_date("corporate_actions") == LAST_EVENT
    # The first sweep asked every symbol through the anchor and got a clean
    # answer, so the lake has looked through the anchor, events or not.
    assert state.get_date("corporate_actions", field="observed_through") == ANCHOR

    checked = _verify(cfg_path)
    assert checked.exit_code == 0, checked.output
    assert "[stale] corporate_actions" not in checked.output
    assert len(calls) == 1, "nothing was due, so nothing was fetched"


def test_verify_offers_a_repair_that_actually_clears_the_gap(demo_lake):
    """The loop itself: a lake that last looked months ago is stale; the
    backfill `verify` suggests looks again and so must close the gap."""
    cfg_path, calls = demo_lake
    cfg = load_config(cfg_path)
    state = StateStore(cfg.meta_root)
    state.set_date("corporate_actions", LAST_EVENT, field="observed_through")

    before = _verify(cfg_path)
    assert before.exit_code == 1, before.output
    assert "[stale] corporate_actions" in before.output
    assert f"--start {LAST_EVENT.isoformat()}" in before.output

    repaired = _verify(cfg_path, "--repair")
    assert repaired.exit_code == 0, repaired.output
    assert calls[-1][:2] == (LAST_EVENT, ANCHOR)
    assert state.get_date("corporate_actions") == LAST_EVENT, "no event was invented"

    after = _verify(cfg_path)
    assert after.exit_code == 0, after.output
    assert "[stale] corporate_actions" not in after.output


def test_a_failed_symbol_leaves_the_observation_where_it_was(demo_lake, monkeypatch):
    cfg_path, _calls = demo_lake
    cfg = load_config(cfg_path)
    state = StateStore(cfg.meta_root)
    state.set_date("corporate_actions", LAST_EVENT, field="observed_through")
    inner = events.fetch_corporate_actions

    def flaky(trade_date, *, symbols, **kwargs):
        if "300750.SZ" in symbols:
            raise RuntimeError("simulated TDX outage")
        return inner(trade_date, symbols=symbols, **kwargs)

    monkeypatch.setattr(events, "fetch_corporate_actions", flaky)
    _verify(cfg_path, "--repair")

    assert state.get_date("corporate_actions", field="observed_through") == LAST_EVENT
    assert "[stale] corporate_actions" in _verify(cfg_path).output


def test_a_scoped_sweep_proves_nothing_about_the_rest_of_the_lake(demo_lake):
    cfg_path, _calls = demo_lake
    cfg = load_config(cfg_path)
    state = StateStore(cfg.meta_root)
    state.set_date("corporate_actions", LAST_EVENT, field="observed_through")
    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            "corporate_actions",
            "--config",
            str(cfg_path),
            "--symbols",
            "600519.SH",
            "--start",
            LAST_EVENT.isoformat(),
            "--end",
            ANCHOR.isoformat(),
        ],
    )
    assert result.exit_code == 0, result.output
    assert state.get_date("corporate_actions", field="observed_through") == LAST_EVENT


def test_a_window_that_skips_unobserved_days_does_not_advance(demo_lake):
    """Looking at October says nothing about July to September."""
    cfg_path, _calls = demo_lake
    cfg = load_config(cfg_path)
    state = StateStore(cfg.meta_root)
    state.set_date("corporate_actions", LAST_EVENT, field="observed_through")
    from cnequity.cli.backfill_cmds import _run_backfill

    _run_backfill(cfg, "corporate_actions", date(2026, 10, 1), ANCHOR)
    assert state.get_date("corporate_actions", field="observed_through") == LAST_EVENT


def test_status_reads_freshness_from_the_observation(demo_lake):
    cfg_path, _calls = demo_lake
    result = CliRunner().invoke(
        cli, ["status", "--datasets", "--no-scope", "--config", str(cfg_path)]
    )
    rows = {
        line.split("┆")[0].strip("│ "): line for line in result.output.splitlines() if "┆" in line
    }
    assert "STALE" not in rows["corporate_actions"]
    assert "fresh" in rows["corporate_actions"]
