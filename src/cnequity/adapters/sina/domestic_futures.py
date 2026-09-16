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
import math
import re
from datetime import date
from typing import Any

import httpx
import polars as pl

from cnequity.adapters.numeric import finite_int64
from cnequity.domain.rate_limit import (
    SINA_FETCH_ATTEMPTS,
    SINA_RATE_LIMIT_COOLDOWN_SECONDS,
    SINA_RATE_LIMIT_STATUS_CODES,
    SINA_RETRY_STATUS_CODES,
    source_request,
)

logger = logging.getLogger(__name__)

# JSONP: the body is `/*<script>…</script>*/ x([...])`, so the array is pulled
# out with a regex rather than parsed as JSON directly.
_URL = (
    "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/"
    "x/InnerFuturesNewService.getDailyKLine"
)
_ARRAY = re.compile(r"\[.*\]", re.S)


class SinaFuturesPayloadError(RuntimeError):
    """Raised when Sina answers with a non-data page or malformed JSONP."""


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
        parsed = float(x)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _parse_jsonp(text: str) -> list[dict]:
    match = _ARRAY.search(text or "")
    if not match:
        raise SinaFuturesPayloadError("Sina domestic futures response has no JSONP array")
    try:
        payload = json.loads(match.group(0))
    except json.JSONDecodeError as exc:
        raise SinaFuturesPayloadError(
            "Sina domestic futures response contains invalid JSONP"
        ) from exc
    if not isinstance(payload, list):
        raise SinaFuturesPayloadError("Sina domestic futures response JSONP payload is not a list")
    return payload


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
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            logger.warning(
                "domestic commodity_bars: skipping non-object row %s for %s",
                index,
                symbol,
            )
            continue
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
        open_ = _f(item.get("o"))
        high = _f(item.get("h"))
        low = _f(item.get("l"))
        vol = _f(item.get("v"))
        if (
            close is None
            or open_ is None
            or high is None
            or low is None
            or vol is None
            or not all(math.isfinite(v) for v in (open_, high, low, close, vol))
            or min(open_, high, low, close) <= 0
            or vol < 0
        ):
            logger.warning("domestic commodity_bars: malformed row for %s: %r", symbol, item)
            continue
        try:
            volume = finite_int64(vol, minimum=0)
        except ValueError:
            logger.warning("domestic commodity_bars: malformed volume for %s: %r", symbol, item)
            continue
        # `p` is open interest (持仓量) on this feed; `s` is settlement.
        oi = _f(item.get("p"))
        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "exchange": exchange,
                "trade_date": trade_date,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                # Sina serves no turnover on this endpoint. Left null rather
                # than derived from price × volume: a main-continuous series
                # splices contracts, so that product is not the session's money.
                "amount": None,
                "open_interest": oi if oi and oi > 0 else None,
                "source": "sina",
            }
        )
    return rows


def _get_with_cooldown(client, sina_sym: str, *, config):
    """One contract's daily kline, backing off when Sina says to slow down.

    This sweep had no rate-limit awareness at all: an HTTP 456 raised straight
    out, and with ``strict=True`` one throttled contract failed the whole
    `commodity_bars` step — after asking a vendor that had just said "stop"
    exactly as often as the retry policy allowed, which is what earns a longer
    ban. The equity sweeps already cool the whole Sina lane through
    `defer_source`; this is the third sweep on the same budget and it now uses
    the same policy.
    """
    last: Exception | None = None
    for attempt in range(SINA_FETCH_ATTEMPTS):
        try:
            with source_request(config, "sina"):
                resp = client.get(_URL, params={"symbol": sina_sym})
            resp.raise_for_status()
            return resp
        except Exception as exc:  # noqa: BLE001 — classified on the status below
            last = exc
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status not in SINA_RETRY_STATUS_CODES or attempt + 1 >= SINA_FETCH_ATTEMPTS:
                raise
            if status in SINA_RATE_LIMIT_STATUS_CODES:
                # Cool every Sina lane, not this endpoint: the budget is
                # vendor-wide, so continuing here strands the equity sweeps too.
                if config is not None and hasattr(config, "defer_source"):
                    config.defer_source("sina", SINA_RATE_LIMIT_COOLDOWN_SECONDS)
                logger.warning(
                    "domestic commodity_bars: Sina HTTP %s for %s; cooling all Sina "
                    "lanes %.0fs before retry %d/%d",
                    status,
                    sina_sym,
                    SINA_RATE_LIMIT_COOLDOWN_SECONDS,
                    attempt + 2,
                    SINA_FETCH_ATTEMPTS,
                )
            else:
                logger.warning(
                    "domestic commodity_bars: transient HTTP %s for %s; retry %d/%d",
                    status,
                    sina_sym,
                    attempt + 2,
                    SINA_FETCH_ATTEMPTS,
                )
    raise last if last is not None else RuntimeError("unreachable")


def fetch_domestic_commodity_bars_range(
    start: date,
    end: date,
    *,
    contracts: tuple[tuple[str, str, str, str], ...] | None = None,
    client: httpx.Client | None = None,
    config=None,
    strict: bool = False,
) -> pl.DataFrame:
    """Domestic main-continuous daily OHLC for [*start*, *end*] (inclusive)."""
    if start > end:
        return pl.DataFrame()
    # ``None`` selects the default contracts; ``()`` is an intentional no-op.
    universe = DOMESTIC_CONTRACTS if contracts is None else contracts
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
            try:
                resp = _get_with_cooldown(client, sina_sym, config=config)
                payload = _parse_jsonp(resp.text)
                if not payload:
                    logger.warning(
                        "domestic commodity_bars: empty payload for %s (%s)",
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
                if strict:
                    raise RuntimeError(
                        f"domestic commodity_bars failed for {symbol} ({sina_sym})"
                    ) from exc
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
