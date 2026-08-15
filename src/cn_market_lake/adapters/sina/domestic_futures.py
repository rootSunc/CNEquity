"""Sina domestic futures daily K-line — main-continuous contracts.

Why this replaced EastMoney's push2his for the daily path: push2his is
intermittently unreachable in a way nothing on this side controls. Measured
over one session, a burst of ten requests came back 200 with a full payload,
then the same request failed 0/12 both directly and through a mainland exit,
and was still failing after seven minutes of silence. TLS, certificate and
routing were all verified healthy throughout, so it is an application-layer
refusal at the vendor. `commodity_bars` was the only daily consumer of that
host, and it spent every run failing 15 contracts to write one row.

Sina serves the same series, deeper, from a host that answered every probe in
this project's source-health sweeps. Per contract it returns the entire history
in one call and the caller slices — same contract as ``global_futures``.

Coverage measured 2026-08: each contract reaches back to its own listing
(CU0/AL0 2005, TA0 2006, ZN0 2007, AU0 2008, RB0 2009, J0 2011, AG0 2012,
I0 2013, JM0 2013, HC0 2014, MA0 2014, NI0 2015, SC0 2018, LC0 2023).
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Any

import httpx
import polars as pl

logger = logging.getLogger(__name__)

# JSONP: the body is `/*<script>…</script>*/ x([...])`, so the array is pulled
# out with a regex rather than parsed as JSON directly.
_URL = (
    "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/"
    "x/InnerFuturesNewService.getDailyKLine"
)
_ARRAY = re.compile(r"\[.*\]", re.S)

# (lake_symbol, sina_symbol, name, exchange) — mirrors CONTINUOUS_CONTRACTS in
# the EastMoney adapter one-for-one, so the lake's symbols do not change.
DOMESTIC_CONTRACTS: tuple[tuple[str, str, str, str], ...] = (
    ("AU0.SHF", "AU0", "沪金主连", "SHF"),
    ("AG0.SHF", "AG0", "沪银主连", "SHF"),
    ("CU0.SHF", "CU0", "沪铜主连", "SHF"),
    ("AL0.SHF", "AL0", "沪铝主连", "SHF"),
    ("ZN0.SHF", "ZN0", "沪锌主连", "SHF"),
    ("NI0.SHF", "NI0", "沪镍主连", "SHF"),
    ("RB0.SHF", "RB0", "螺纹钢主连", "SHF"),
    ("HC0.SHF", "HC0", "热卷主连", "SHF"),
    ("I0.DCE", "I0", "铁矿石主连", "DCE"),
    ("JM0.DCE", "JM0", "焦煤主连", "DCE"),
    ("J0.DCE", "J0", "焦炭主连", "DCE"),
    ("SC0.INE", "SC0", "原油主连", "INE"),
    ("LC0.GFE", "LC0", "碳酸锂主连", "GFE"),
    ("TA0.CZC", "TA0", "PTA主连", "CZC"),
    ("MA0.CZC", "MA0", "甲醇主连", "CZC"),
)


def _f(x: Any) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _parse_jsonp(text: str) -> list[dict]:
    match = _ARRAY.search(text or "")
    if not match:
        return []
    try:
        payload = json.loads(match.group(0))
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else []


def _parse_rows(
    payload: list[dict],
    *,
    symbol: str,
    name: str,
    exchange: str,
    start: date,
    end: date,
) -> list[dict]:
    rows: list[dict] = []
    for item in payload:
        raw = item.get("d")
        if not raw:
            continue
        try:
            trade_date = date.fromisoformat(str(raw)[:10])
        except ValueError:
            continue
        if trade_date < start or trade_date > end:
            continue
        close = _f(item.get("c"))
        if close is None or close <= 0:
            continue
        open_ = _f(item.get("o"))
        high = _f(item.get("h"))
        low = _f(item.get("l"))
        vol = _f(item.get("v"))
        # `p` is open interest (持仓量) on this feed; `s` is settlement.
        oi = _f(item.get("p"))
        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "exchange": exchange,
                "trade_date": trade_date,
                "open": open_ if open_ and open_ > 0 else close,
                "high": high if high and high > 0 else close,
                "low": low if low and low > 0 else close,
                "close": close,
                "volume": int(vol) if vol is not None else 0,
                # Sina serves no turnover on this endpoint. Left null rather
                # than derived from price × volume: a main-continuous series
                # splices contracts, so that product is not the session's money.
                "amount": None,
                "open_interest": oi if oi and oi > 0 else None,
                "source": "sina",
            }
        )
    return rows


def fetch_domestic_commodity_bars_range(
    start: date,
    end: date,
    *,
    contracts: tuple[tuple[str, str, str, str], ...] | None = None,
    client: httpx.Client | None = None,
    config=None,
) -> pl.DataFrame:
    """Domestic main-continuous daily OHLC for [*start*, *end*] (inclusive)."""
    if start > end:
        return pl.DataFrame()
    universe = contracts or DOMESTIC_CONTRACTS
    owns = client is None
    if client is None:
        client = httpx.Client(
            timeout=60.0,
            follow_redirects=True,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://finance.sina.com.cn",
            },
        )
    rows: list[dict] = []
    try:
        for symbol, sina_sym, name, exchange in universe:
            if config is not None:
                config.rate_limit("sina")
            try:
                resp = client.get(_URL, params={"symbol": sina_sym})
                resp.raise_for_status()
                payload = _parse_jsonp(resp.text)
                if not payload:
                    logger.warning(
                        "domestic commodity_bars: unparseable payload for %s (%s)",
                        symbol,
                        sina_sym,
                    )
                    continue
                part = _parse_rows(
                    payload,
                    symbol=symbol,
                    name=name,
                    exchange=exchange,
                    start=start,
                    end=end,
                )
                rows.extend(part)
                if not part:
                    logger.info(
                        "domestic commodity_bars: no rows for %s in %s→%s",
                        symbol,
                        start,
                        end,
                    )
            except Exception as exc:  # noqa: BLE001 — one contract must not sink the sweep
                logger.warning(
                    "domestic commodity_bars: %s (%s) failed: %s: %s",
                    symbol,
                    sina_sym,
                    type(exc).__name__,
                    exc,
                )
    finally:
        if owns:
            client.close()

    if not rows:
        return pl.DataFrame()
    return (
        pl.DataFrame(rows)
        .unique(subset=["symbol", "trade_date"], keep="last")
        .sort(["trade_date", "symbol"])
    )
