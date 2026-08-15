"""EastMoney continuous-contract daily bars for domestic commodity futures.

Symbol convention: ``{ROOT}0.{EXCH}`` (e.g. ``AU0.SHF``, ``I0.DCE``).
``EXCH`` ∈ {SHF, DCE, CZC, INE, GFE}. Bars are exchange main-continuous
(东财「主连」), not a specific delivery month. Night-session prints roll onto
the exchange settle date of the source; watermark gaps reuse the SSE calendar
(v1 approximation).
"""

from __future__ import annotations

import logging
import time
from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from cn_market_lake.adapters.eastmoney.common import parse_em_ymd
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient, is_transport_fail_fast

if TYPE_CHECKING:
    from cn_market_lake.config import Config

logger = logging.getLogger(__name__)

_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
_MAX_RETRIES = 5

# (lake_symbol, em_secid, name, exchange)
# secid market: 113=SHFE主连, 114=DCE, 115=CZCE, 142=INE, 225=GFEX
CONTINUOUS_CONTRACTS: tuple[tuple[str, str, str, str], ...] = (
    ("AU0.SHF", "113.AUM", "沪金主连", "SHF"),
    ("AG0.SHF", "113.AGM", "沪银主连", "SHF"),
    ("CU0.SHF", "113.CUM", "沪铜主连", "SHF"),
    ("AL0.SHF", "113.ALM", "沪铝主连", "SHF"),
    ("ZN0.SHF", "113.ZNM", "沪锌主连", "SHF"),
    ("NI0.SHF", "113.NIM", "沪镍主连", "SHF"),
    ("RB0.SHF", "113.RBM", "螺纹钢主连", "SHF"),
    ("HC0.SHF", "113.HCM", "热卷主连", "SHF"),
    ("I0.DCE", "114.IM", "铁矿石主连", "DCE"),
    ("JM0.DCE", "114.JMM", "焦煤主连", "DCE"),
    ("J0.DCE", "114.JM", "焦炭主连", "DCE"),
    ("SC0.INE", "142.SCM", "原油主连", "INE"),
    ("LC0.GFE", "225.LCM", "碳酸锂主连", "GFE"),
    ("TA0.CZC", "115.TAM", "PTA主连", "CZC"),
    ("MA0.CZC", "115.MAM", "甲醇主连", "CZC"),
)

DEFAULT_BACKFILL_START = date(2020, 1, 1)


def _sina_contracts(
    universe: tuple[tuple[str, str, str, str], ...],
) -> tuple[tuple[str, str, str, str], ...]:
    """Re-key the EastMoney contract table onto Sina's symbols.

    Sina names a main-continuous contract by its bare code plus ``0`` — the
    same string the lake symbol already starts with — so the mapping is taken
    from the lake symbol rather than kept as a second hand-maintained table
    that could drift against the first.
    """
    return tuple(
        (lake_symbol, lake_symbol.split(".")[0], name, exchange)
        for lake_symbol, _secid, name, exchange in universe
    )


def _fetch_one_kline(
    client: EastMoneyClient,
    *,
    symbol: str,
    secid: str,
    name: str,
    exchange: str,
    start: date,
    end: date,
) -> list[dict]:
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "klt": "101",
        "fqt": "1",
        "beg": start.strftime("%Y%m%d"),
        "end": end.strftime("%Y%m%d"),
        "lmt": "1000000",
    }
    data: dict = {}
    for attempt in range(_MAX_RETRIES):
        try:
            resp = client.get(_KLINE_URL, params=params)
            resp.raise_for_status()
            data = resp.json().get("data") or {}
            break
        except Exception as exc:
            # The predicate was inverted here: it retried exactly the failures
            # `is_transport_fail_fast` says a retry cannot fix, and gave up at
            # once on the transient ones. With push2his refusing an egress that
            # cost 151s per daily run — 15 contracts × 5 attempts × backoff —
            # to return nothing. clist and datacenter both break on this same
            # predicate; match them.
            if is_transport_fail_fast(exc) or attempt + 1 >= _MAX_RETRIES:
                raise
            time.sleep(0.6 + attempt * 0.5)
    klines = data.get("klines") or []
    em_name = data.get("name") or name
    rows: list[dict] = []
    for line in klines:
        parts = str(line).split(",")
        if len(parts) < 7:
            continue
        try:
            trade_date = parse_em_ymd(parts[0])
            open_ = float(parts[1])
            close = float(parts[2])
            high = float(parts[3])
            low = float(parts[4])
            volume = int(float(parts[5]))
            amount = float(parts[6])
        except (ValueError, TypeError):
            continue
        if close <= 0:
            continue
        rows.append(
            {
                "symbol": symbol,
                "name": em_name,
                "exchange": exchange,
                "trade_date": trade_date,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "amount": amount,
                "open_interest": None,
                "source": "eastmoney",
            }
        )
    return rows


def _concat_frames(parts: list[pl.DataFrame]) -> pl.DataFrame:
    nonempty = [p for p in parts if not p.is_empty()]
    if not nonempty:
        return pl.DataFrame()
    return (
        pl.concat(nonempty, how="diagonal_relaxed")
        .unique(subset=["symbol", "trade_date"], keep="last")
        .sort(["trade_date", "symbol"])
    )


def fetch_commodity_bars_range(
    start: date,
    end: date,
    *,
    config: Config | None = None,
    contracts: tuple[tuple[str, str, str, str], ...] | None = None,
    include_offshore: bool = True,
) -> pl.DataFrame:
    """Fetch continuous-contract daily OHLC for [*start*, *end*] (inclusive).

    Domestic main-continuous via Sina; offshore gold (``GC0.CMX``) via Sina
    global futures (narrow v1 — research overnight lead only).

    Domestic used to come from EastMoney push2his and no longer does. That host
    refuses requests intermittently in a way nothing here controls — measured
    0/12 both directly and through a mainland exit, still failing after seven
    minutes of quiet, with TLS and routing verified healthy — so every daily run
    failed all 15 contracts and wrote the one offshore row. Sina serves the same
    series with deeper history from a host that has answered every probe. The
    EastMoney path is kept below as an explicit opt-in for comparison, not as a
    fallback: silently retrying a source known to be flaky is how the 151-second
    daily stall happened.
    """
    if start > end:
        return pl.DataFrame()
    universe = contracts or CONTINUOUS_CONTRACTS
    rows: list[dict] = []
    client_kwargs: dict = {"config": config} if config is not None else {"min_interval": 0.5}
    em_enabled = False
    if config is not None:
        em_enabled = bool(getattr(config, "_commodity_via_eastmoney", False))
    if em_enabled:
        with EastMoneyClient(**client_kwargs) as client:
            for symbol, secid, name, exchange in universe:
                try:
                    part = _fetch_one_kline(
                        client,
                        symbol=symbol,
                        secid=secid,
                        name=name,
                        exchange=exchange,
                        start=start,
                        end=end,
                    )
                    rows.extend(part)
                    if not part:
                        logger.warning(
                            "commodity_bars: empty kline for %s (%s) %s→%s",
                            symbol,
                            secid,
                            start,
                            end,
                        )
                except Exception as exc:
                    logger.warning(
                        "commodity_bars: %s (%s) failed: %s: %s",
                        symbol,
                        secid,
                        type(exc).__name__,
                        exc,
                    )
                time.sleep(0.25)
    domestic = (
        pl.DataFrame(rows)
        .unique(subset=["symbol", "trade_date"], keep="last")
        .sort(["trade_date", "symbol"])
        if rows
        else pl.DataFrame()
    )

    sina_enabled = True
    if config is not None:
        sina_enabled = bool(config.sources.get("sina", True))

    if domestic.is_empty() and sina_enabled:
        from cn_market_lake.adapters.sina.domestic_futures import (
            fetch_domestic_commodity_bars_range,
        )

        try:
            domestic = fetch_domestic_commodity_bars_range(
                start, end, contracts=_sina_contracts(universe), config=config
            )
        except Exception as exc:  # noqa: BLE001 — offshore may still be writable
            logger.warning(
                "commodity_bars: domestic fetch failed: %s: %s",
                type(exc).__name__,
                exc,
            )

    offshore = pl.DataFrame()
    if include_offshore and sina_enabled:
        from cn_market_lake.adapters.sina.global_futures import (
            fetch_offshore_commodity_bars_range,
        )

        try:
            offshore = fetch_offshore_commodity_bars_range(start, end)
        except Exception as exc:
            logger.warning(
                "commodity_bars: offshore fetch failed: %s: %s",
                type(exc).__name__,
                exc,
            )
    return _concat_frames([domestic, offshore])


def fetch_commodity_bars(trade_date: date, *, config: Config | None = None) -> pl.DataFrame:
    """Daily / backfill entrypoint.

    - Normal incremental: bars for *trade_date* only.
    - ``config._backfill``: full history from ``_backfill_start`` (default 2020-01-01)
      through ``_backfill_end`` or *trade_date*.
    """
    if config is not None and getattr(config, "_backfill", False):
        start = getattr(config, "_backfill_start", None) or DEFAULT_BACKFILL_START
        end = getattr(config, "_backfill_end", None) or trade_date
        return fetch_commodity_bars_range(start, end, config=config)
    return fetch_commodity_bars_range(trade_date, trade_date, config=config)
