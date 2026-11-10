"""Check a paid source's key with one small request and record the verdict.

The check runs whether or not the source is switched on, so a key can be
confirmed before it is enabled. It writes nothing to the lake. The request
counts against the source's own pacing like any other call.
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone

import httpx

from cnequity.storage.key_checks import configured_key, save_check

PAID_SOURCES = ("tushare", "ths_official")


def _probe_tushare(config) -> tuple[str, str]:
    from cnequity.adapters.tushare.client import (
        TushareClient,
        TusharePermissionDenied,
        TushareQuotaExhausted,
        TushareRefused,
    )

    probe = copy.copy(config)
    probe.sources = {**(config.sources or {}), "tushare": True}
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    try:
        with TushareClient(probe) as client:
            client.rows(
                "trade_cal", {"exchange": "SSE", "start_date": today, "end_date": today}, "cal_date"
            )
    except TusharePermissionDenied as exc:
        return "limited", str(exc)
    except TushareQuotaExhausted as exc:
        return "limited", str(exc)
    except TushareRefused as exc:
        return "rejected", str(exc)
    return "ok", ""


def _probe_ths_official(config) -> tuple[str, str]:
    from cnequity.adapters.ths_official.client import ThsOfficialAuthError, ThsOfficialClient

    try:
        with ThsOfficialClient(configured_key(config, "ths_official"), config=config) as client:
            client.get("/api/a-share/valuations/snapshot", thscodes="600000.SH")
    except ThsOfficialAuthError as exc:
        return ("limited" if exc.granted_elsewhere else "rejected"), str(exc)
    return "ok", ""


_PROBES = {"tushare": _probe_tushare, "ths_official": _probe_ths_official}


def check_key(config, source: str) -> dict:
    """Probe *source* with its configured key; returns and saves the verdict."""
    if source not in _PROBES:
        raise ValueError(f"{source} is not a paid source")
    if configured_key(config, source) is None:
        raise ValueError(f"{source} has no key configured")
    try:
        state, message = _PROBES[source](config)
    except (httpx.HTTPError, OSError, RuntimeError, ValueError) as exc:
        state, message = "error", str(exc) or type(exc).__name__
    checked_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    return save_check(config, source, state, message[:300], checked_at)
