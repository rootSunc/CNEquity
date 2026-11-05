"""Persist the non-secret fetch scope needed by a later CLI retry."""

from __future__ import annotations

from copy import deepcopy
from datetime import date

from cnequity.config import Config
from cnequity.run_scope import backfill_of, set_backfill

_REPAIR_FLAGS = {
    "baostock_repair": "_corporate_actions_baostock_repair",
    "ths_repair": "_corporate_actions_ths_repair",
    "bse_tip_repair": "_bse_tip_repair",
    "tdx_amount_repair": "_tdx_amount_repair",
    "turnover_repair": "_turnover_repair",
    "turnover_peer_review": "_turnover_peer_review",
}
_SETTINGS = (
    "ingest_universe",
    "minute_bars_enabled",
    "minute_bars_frequencies",
    "trade_ticks_enabled",
    "trade_ticks_scope",
    "trade_ticks_symbols",
    "trade_ticks_max_symbols",
    "margin_trading_source",
    "futures_enabled",
    "futures_exchanges",
    "futures_options",
    "futures_minute_enabled",
    "futures_minute_products",
    "futures_minute_contracts",
    "futures_minute_max_contracts",
)
#: Stored under its old attribute name so runs recorded before the backfill
#: window became one value restore the same way.
_WORKERS_KEY = "_backfill_workers"
_OPTIONAL_SETTINGS = {
    "_corporate_actions_payment_repair": False,
    "_corporate_actions_issuer_notice_only": False,
    "_corporate_actions_eastmoney_date_repair": None,
    "_valuation_fill_em_outage": False,
    "_derivatives_refresh": False,
}


def capture_backfill_scope(config: Config) -> dict:
    # Keep the historical keys readable by older tools. Explicitly whitelist
    # operational scope: credentials, proxies and transport policy stay in the
    # current config. A sector --force checkpoint reset is a one-time action;
    # retry resumes that checkpoint instead of resetting it again.
    window = backfill_of(config)
    scope = {
        "start": window.start,
        "end": window.end,
        "symbols": window.symbol_list(),
        "minute_bars_scope": config.minute_bars_scope,
        "minute_bars_symbols": list(config.minute_bars_symbols),
        **{key: bool(getattr(config, attr, False)) for key, attr in _REPAIR_FLAGS.items()},
        "settings": {
            **{name: deepcopy(getattr(config, name)) for name in _SETTINGS},
            **{
                name: deepcopy(getattr(config, name, default))
                for name, default in _OPTIONAL_SETTINGS.items()
            },
            _WORKERS_KEY: window.workers,
        },
    }
    for key in ("start", "end"):
        if scope[key] is not None:
            scope[key] = scope[key].isoformat()
    dates = scope["settings"]["_corporate_actions_eastmoney_date_repair"]
    if dates is not None:
        scope["settings"]["_corporate_actions_eastmoney_date_repair"] = [
            day.isoformat() for day in dates
        ]
    return scope


def restore_backfill_scope(config: Config, scope: dict) -> None:
    if "minute_bars_scope" in scope:
        config.minute_bars_scope = scope["minute_bars_scope"]
        config.minute_bars_symbols = list(scope.get("minute_bars_symbols") or [])
    start, end = scope.get("start"), scope.get("end")
    window = {
        "start": date.fromisoformat(start) if start else None,
        "end": date.fromisoformat(end) if end else None,
        "symbols": deepcopy(scope.get("symbols")),
    }
    for key, attr in _REPAIR_FLAGS.items():
        setattr(config, attr, bool(scope.get(key, False)))
    settings = scope.get("settings") or {}
    if _WORKERS_KEY in settings:
        window["workers"] = settings[_WORKERS_KEY]
    set_backfill(config, **window)
    # Legacy runs did not record these fields; keep their current config.
    # Unknown metadata must never set arbitrary config attributes.
    for name in (*_SETTINGS, *_OPTIONAL_SETTINGS):
        if name not in settings:
            continue
        value = deepcopy(settings[name])
        if name == "_corporate_actions_eastmoney_date_repair" and value is not None:
            value = [date.fromisoformat(day) for day in value]
        setattr(config, name, value)
    config._sector_bars_force = False
