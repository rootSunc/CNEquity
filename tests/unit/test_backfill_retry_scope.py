"""Fallback retries preserve the accepted one-off fetch request."""

from datetime import date

import pytest

from cnequity.config import Config
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.outcomes import SourceUnavailableError
from cnequity.orchestrator.registry import StepEntry
from cnequity.run_scope import (
    LEGACY_BACKFILL_ATTRS,
    LEGACY_RUN_FLAG_ATTRS,
    backfill_of,
    run_flags_of,
    set_backfill,
    set_run_flags,
)

DAY = date(2024, 6, 28)


def _set(config, name, value):
    if name in LEGACY_BACKFILL_ATTRS:
        set_backfill(config, **{LEGACY_BACKFILL_ATTRS[name]: value})
    elif name in LEGACY_RUN_FLAG_ATTRS:
        set_run_flags(config, **{LEGACY_RUN_FLAG_ATTRS[name]: value})
    else:
        setattr(config, name, value)


def _setting(config, name):
    if name in LEGACY_BACKFILL_ATTRS:
        return getattr(backfill_of(config), LEGACY_BACKFILL_ATTRS[name])
    if name in LEGACY_RUN_FLAG_ATTRS:
        return getattr(run_flags_of(config), LEGACY_RUN_FLAG_ATTRS[name])
    return getattr(config, name, None)


@pytest.mark.parametrize(
    "dataset, overrides",
    [
        (
            "minute_bars_5m",
            {
                "minute_bars_enabled": True,
                "minute_bars_scope": "watchlist",
                "minute_bars_symbols": ["600519.SH"],
                "minute_bars_frequencies": ["5m"],
            },
        ),
        (
            "trade_ticks",
            {
                "trade_ticks_enabled": True,
                "trade_ticks_scope": "watchlist",
                "trade_ticks_symbols": ["600519.SH"],
                "trade_ticks_max_symbols": 250,
            },
        ),
        ("margin_trading", {"margin_trading_source": "eastmoney", "_backfill_workers": 2}),
        (
            "futures_minute_bars",
            {
                "futures_enabled": True,
                "futures_minute_enabled": True,
                "futures_minute_contracts": ["CU2407.SHF"],
                "futures_minute_products": [],
                "futures_minute_max_contracts": 150,
                "futures_exchanges": ["SHF"],
            },
        ),
        (
            "corporate_actions",
            {
                "_corporate_actions_payment_repair": True,
                "_corporate_actions_issuer_notice_only": True,
                "_corporate_actions_eastmoney_date_repair": [DAY],
            },
        ),
        ("valuation_metrics", {"_valuation_fill_em_outage": True}),
        (
            "futures_bars",
            {"futures_enabled": True, "futures_exchanges": ["SHF"], "_derivatives_refresh": True},
        ),
        ("daily_bars", {"ingest_universe": "all_a_sh_sz"}),
        ("daily_bars", {"_turnover_repair": True, "_turnover_peer_review": True}),
    ],
)
def test_retry_from_fresh_config_replays_one_off_scope(tmp_path, monkeypatch, dataset, overrides):
    from cnequity.orchestrator import engine as module

    cfg = Config(data_root=tmp_path / "lake", retry_backoff_seconds=0)
    set_backfill(cfg, start=DAY, end=DAY, symbols=["600519.SH"])
    for name, value in overrides.items():
        _set(cfg, name, value)
    requests = []

    def fetch(config, day, run_id, context):
        requests.append(
            {
                "date": day,
                "start": backfill_of(config).start,
                "end": backfill_of(config).end,
                "symbols": backfill_of(config).symbol_list(),
                **{name: _setting(config, name) for name in overrides},
            }
        )
        if len(requests) == 1:
            raise SourceUnavailableError("fixture provider unavailable")
        return {"rows_written": 0, "already_covered": True, "coverage_complete": True}

    original = module.get_step
    monkeypatch.setattr(module, "shanghai_today", lambda: DAY)
    monkeypatch.setattr(
        module,
        "get_step",
        lambda name: StepEntry(fn=fetch, group="core") if name == dataset else original(name),
    )
    initial = JobEngine(cfg).run_job("backfill", DAY, steps=[dataset], backfill=True)
    assert initial["status"] == "degraded"
    assert initial["fallback"][0]["retry_command"]

    resumed = JobEngine(Config(data_root=cfg.data_root, retry_backoff_seconds=0))
    result = resumed.run_job("retry", run_id=initial["run_id"], retry_failed_only=True)

    assert len(requests) == 2
    assert requests[1] == requests[0]
    assert result["execution_status"] == "completed"
    assert result["status"] == "success"


def test_legacy_scope_restores_dates_without_overwriting_unrecorded_settings(tmp_path):
    cfg = Config(data_root=tmp_path / "lake", margin_trading_source="eastmoney")
    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run(
        "backfill",
        {
            "trade_date": DAY.isoformat(),
            "backfill": True,
            "backfill_scope": {
                "start": DAY.isoformat(),
                "end": DAY.isoformat(),
                "symbols": ["600519.SH"],
                "minute_bars_scope": "watchlist",
                "minute_bars_symbols": ["600519.SH"],
            },
            "planned_steps": [],
        },
    )
    engine.run_job("retry", run_id=run_id, retry_failed_only=True)
    assert backfill_of(cfg).start == backfill_of(cfg).end == DAY
    assert backfill_of(cfg).symbol_list() == ["600519.SH"]
    assert cfg.minute_bars_symbols == ["600519.SH"]
    assert cfg.margin_trading_source == "eastmoney"


def test_scope_is_a_snapshot_and_only_restores_whitelisted_settings(tmp_path):
    from cnequity.orchestrator.backfill_scope import capture_backfill_scope, restore_backfill_scope

    cfg = Config(data_root=tmp_path / "lake", minute_bars_symbols=["600519.SH"])
    set_run_flags(cfg, ca_eastmoney_date_repair=[DAY], sector_bars_force=True)
    scope = capture_backfill_scope(cfg)
    cfg.minute_bars_symbols.append("000001.SZ")
    set_run_flags(cfg, ca_eastmoney_date_repair=[])
    assert scope["minute_bars_symbols"] == ["600519.SH"]
    assert scope["settings"]["_corporate_actions_eastmoney_date_repair"] == [DAY.isoformat()]
    assert "_sector_bars_force" not in scope["settings"]

    scope["settings"]["data_root"] = "/wrong/lake"
    fresh = Config(data_root=cfg.data_root)
    restore_backfill_scope(fresh, scope)
    assert fresh.data_root == cfg.data_root
    assert run_flags_of(fresh).ca_eastmoney_date_repair == (DAY,)
    assert run_flags_of(fresh).sector_bars_force is False
    restore_backfill_scope(cfg, scope)
    assert run_flags_of(cfg).sector_bars_force is False
