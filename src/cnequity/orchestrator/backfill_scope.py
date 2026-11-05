"""Persist the non-secret fetch scope needed by a later CLI retry."""

from __future__ import annotations

from copy import deepcopy
from datetime import date

from cnequity.config import Config
from cnequity.run_scope import backfill_of, run_flags_of, set_backfill, set_run_flags

#: Recorded key → ``RunFlags`` field. Always restored (absent means off).
_REPAIR_FLAGS = {
    "baostock_repair": "ca_baostock_repair",
    "ths_repair": "ca_ths_repair",
    "bse_tip_repair": "bse_tip_repair",
    "tdx_amount_repair": "tdx_amount_repair",
    "turnover_repair": "turnover_repair",
    "turnover_peer_review": "turnover_peer_review",
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
#: Recorded under ``settings`` by their old attribute names → ``RunFlags``
#: field. Restored only when recorded: legacy runs keep the current value.
_OPTIONAL_FLAGS = {
    "_corporate_actions_payment_repair": "ca_payment_repair",
    "_corporate_actions_issuer_notice_only": "ca_issuer_notice_only",
    "_corporate_actions_eastmoney_date_repair": "ca_eastmoney_date_repair",
    "_valuation_fill_em_outage": "valuation_fill_em_outage",
    "_derivatives_refresh": "derivatives_refresh",
}
_DATES_KEY = "_corporate_actions_eastmoney_date_repair"


def capture_backfill_scope(config: Config) -> dict:
    # Keep the historical keys readable by older tools. Explicitly whitelist
    # operational scope: credentials, proxies and transport policy stay in the
    # current config. A sector --force checkpoint reset is a one-time action;
    # retry resumes that checkpoint instead of resetting it again.
    window = backfill_of(config)
    flags = run_flags_of(config)
    optional = {name: getattr(flags, field) for name, field in _OPTIONAL_FLAGS.items()}
    if optional[_DATES_KEY] is not None:
        optional[_DATES_KEY] = [day.isoformat() for day in optional[_DATES_KEY]]
    return {
        "start": window.start.isoformat() if window.start else None,
        "end": window.end.isoformat() if window.end else None,
        "symbols": window.symbol_list(),
        "minute_bars_scope": config.minute_bars_scope,
        "minute_bars_symbols": list(config.minute_bars_symbols),
        **{key: bool(getattr(flags, field)) for key, field in _REPAIR_FLAGS.items()},
        "settings": {
            **{name: deepcopy(getattr(config, name)) for name in _SETTINGS},
            **optional,
            _WORKERS_KEY: window.workers,
        },
    }


def restore_backfill_scope(config: Config, scope: dict) -> None:
    if "minute_bars_scope" in scope:
        config.minute_bars_scope = scope["minute_bars_scope"]
        config.minute_bars_symbols = list(scope.get("minute_bars_symbols") or [])
    settings = scope.get("settings") or {}
    start, end = scope.get("start"), scope.get("end")
    window = {
        "start": date.fromisoformat(start) if start else None,
        "end": date.fromisoformat(end) if end else None,
        "symbols": deepcopy(scope.get("symbols")),
    }
    if _WORKERS_KEY in settings:
        window["workers"] = settings[_WORKERS_KEY]
    set_backfill(config, **window)
    # A sector --force reset is one-time: a retry resumes the checkpoint.
    flags = {field: bool(scope.get(key, False)) for key, field in _REPAIR_FLAGS.items()}
    flags["sector_bars_force"] = False
    # Legacy runs did not record these; keep their current value.
    for name, field in _OPTIONAL_FLAGS.items():
        if name not in settings:
            continue
        value = deepcopy(settings[name])
        if name == _DATES_KEY and value is not None:
            value = [date.fromisoformat(day) for day in value]
        flags[field] = value
    set_run_flags(config, **flags)
    # Unknown metadata must never set arbitrary config attributes.
    for name in _SETTINGS:
        if name in settings:
            setattr(config, name, deepcopy(settings[name]))
