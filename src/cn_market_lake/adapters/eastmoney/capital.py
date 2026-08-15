"""EastMoney L4 capital datasets: fund flow, margin, northbound, dragon tiger, block trades."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.clist import clist_rows_to_symbols, fetch_clist_pages
from cn_market_lake.adapters.eastmoney.common import (
    _to_float,
    exchange_from_datacenter,
    symbol_from_em,
    symbol_from_secucode,
)
from cn_market_lake.adapters.eastmoney.datacenter import fetch_datacenter
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.config import Config
from cn_market_lake.domain.symbols import format_symbol

logger = logging.getLogger(__name__)

_FUND_FLOW_FIELDS = "f12,f13,f62,f66,f72,f78,f84"
_MARGIN_REPORT = "RPTA_WEB_RZRQ_GGMX"
_MARGIN_COLUMNS = "DATE,SCODE,SECUCODE,RZYE,RZMRE,RQYE,RQMCL"
_NORTH_HOLD_REPORT = "RPT_MUTUAL_HOLDSTOCKNORTH_STA"
_NORTH_HOLD_COLUMNS = (
    "SECUCODE,TRADE_DATE,MUTUAL_TYPE,HOLD_SHARES,HOLD_MARKET_CAP,HOLD_SHARES_RATIO"
)
_DRAGON_REPORT = "RPT_DAILYBILLBOARD_DETAILS"
_DRAGON_COLUMNS = (
    "SECURITY_CODE,TRADE_DATE,EXPLANATION,BILLBOARD_BUY_AMT,BILLBOARD_SELL_AMT,BILLBOARD_NET_AMT"
)
_BLOCK_REPORT = "RPT_BLOCKTRADE_STA"
_BLOCK_COLUMNS = "SECURITY_CODE,TRADE_DATE,VOLUME,DEAL_AMT,AVERAGE_PRICE,PREMIUM_RATIO"
# 沪深港通资金历史 (data.eastmoney.com/hsgt). The report carries all six
# MUTUAL_TYPE channels; only the two northbound legs belong in this dataset.
_NORTH_FLOW_REPORT = "RPT_MUTUAL_DEAL_HISTORY"
_NORTH_FLOW_COLUMNS = "MUTUAL_TYPE,TRADE_DATE,NET_DEAL_AMT,BUY_AMT,SELL_AMT"
# 001 沪股通, 003 深股通. 005 (北向合计) is the sum of the two, so storing it
# would put a third row under a PK that means "one leg of one direction".
_NORTHBOUND_CHANNELS = {"001": "SH", "003": "SZ"}
# Scale from the report's amount columns to the 元 the lake stores.
#
# Calibrated against ``HOLD_MARKET_CAP`` in the same row, which is plainly in 元
# (2024-08-16, 北向合计: 1,910,745,742,832 = 1.91万亿, the published figure).
# The report therefore mixes units, so the flow columns need their own scale:
#
#   2024-08-16  沪股通  BUY_AMT = 22080.14   NET_DEAL_AMT = -2568.22
#     ×1e6 → 买入 220.8亿, 净流出 25.7亿   (北向合计净流出 67.7亿)
#     ×1e4 → 买入 2.21亿,  净流出 0.26亿   — two orders below a session whose
#            northbound turnover ran past 900亿, so 万元 cannot be right.
#
# ``QUOTA_BALANCE`` agrees: 53786.23 ×1e6 = 537.9亿 against a 520亿 daily quota
# plus that day's net sell, where ×1e4 would leave 5.4亿 of a 520亿 quota.
_AMOUNT_SCALE = 1_000_000.0
# 沪股通's first session. 深股通 opens 2016-12-05 and simply has no earlier rows.
NORTHBOUND_HISTORY_START = date(2014, 11, 17)
# The exchanges stopped publishing daily northbound net flow after this session;
# every row from 2024-08-19 on carries NET_DEAL_AMT = null. Kept as a named
# constant so the docs, the tests and the audit finding cite one date.
NORTHBOUND_LAST_PUBLISHED = date(2024, 8, 16)


def _channel(mutual_type: str | int | None) -> str:
    text = str(mutual_type or "")
    if text in {"001", "1"} or "沪" in text or "SH" in text.upper():
        return "SH"
    return "SZ"


def _margin_symbol(item: dict) -> str | None:
    sym = symbol_from_secucode(item.get("SECUCODE"))
    if sym:
        return sym
    code = str(item.get("SCODE", "")).zfill(6)
    market = str(item.get("TRADE_MARKET") or item.get("MARKET") or "")
    if "沪" in market or "科创" in market:
        exch = "SH"
    elif "京" in market or "北" in market:
        exch = "BJ"
    else:
        exch = "SZ"
    return symbol_from_em(code, 1 if exch == "SH" else (2 if exch == "BJ" else 0))


def _em_datetime_to_date(value) -> date | None:
    """``2024-08-16 00:00:00`` → ``date``; the report never sends a bare date."""
    text = str(value or "").strip()
    if len(text) < 10:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def fetch_fund_flow(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    rows_raw = fetch_clist_pages(client, fields=_FUND_FLOW_FIELDS)
    rows = []
    for sym, item in clist_rows_to_symbols(rows_raw):
        rows.append(
            {
                "symbol": sym,
                "trade_date": trade_date,
                "main_net_inflow": _to_float(item.get("f62")),
                "super_large_net_inflow": _to_float(item.get("f66")),
                "large_net_inflow": _to_float(item.get("f72")),
                "medium_net_inflow": _to_float(item.get("f78")),
                "small_net_inflow": _to_float(item.get("f84")),
            }
        )
    if owns:
        client.close()
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fetch_margin_trading(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    ds = trade_date.isoformat()
    raw = fetch_datacenter(
        client,
        _MARGIN_REPORT,
        _MARGIN_COLUMNS,
        filter_expr=f"(DATE='{ds}')",
    )
    rows = []
    for item in raw:
        sym = _margin_symbol(item)
        if not sym:
            continue
        rows.append(
            {
                "symbol": sym,
                "trade_date": trade_date,
                "margin_balance": float(item.get("RZYE") or 0),
                "margin_buy": float(item.get("RZMRE") or 0),
                "short_balance": float(item.get("RQYE") or 0),
                "short_sell_volume": float(item.get("RQMCL") or 0),
            }
        )
    if owns:
        client.close()
    return pl.DataFrame(rows) if rows else pl.DataFrame()


_NORTH_BACKFILL_START_YEAR = 2016
_NORTH_QUARTER_END_MMDD = (("03", "31"), ("06", "30"), ("09", "30"), ("12", "31"))


def _quarter_end_dates(trade_date: date) -> list[str]:
    """Quarter-end dates from 2016 through *trade_date*, most recent first."""
    out: list[str] = []
    for year in range(_NORTH_BACKFILL_START_YEAR, trade_date.year + 1):
        for mm, dd in _NORTH_QUARTER_END_MMDD:
            ds = f"{year}-{mm}-{dd}"
            if date.fromisoformat(ds) <= trade_date:
                out.append(ds)
    return sorted(out, reverse=True)


def fetch_northbound_holdings(
    trade_date: date,
    *,
    backfill: bool = False,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    # Since Aug 2024 CSRC publishes per-stock northbound holdings only on
    # quarter-ends (no daily feed), so fetch by quarter-end TRADE_DATE: daily
    # keeps the latest quarter fresh. backfill=True nominally walks every
    # quarter from 2016 (_quarter_end_dates), but do not trust that as a real
    # depth claim: measured 2026-08, RPT_MUTUAL_HOLDSTOCKNORTH_STA answers
    # 9201 "返回数据为空" for TRADE_DATE='2023-12-29' and every older quarter
    # tried, while a recent one (2026-06-30) returns 3,933 rows. The source
    # itself does not serve history here — see docs/datasets/sources.md.
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    periods = _quarter_end_dates(trade_date)
    if not backfill:
        # latest 2 quarters: the just-ended one may not be published yet, so
        # keep the last complete quarter fresh too.
        periods = periods[:2]

    rows: list[dict] = []
    try:
        for period in periods:
            if config is not None:
                config.rate_limit("eastmoney")
            raw = fetch_datacenter(
                client,
                _NORTH_HOLD_REPORT,
                _NORTH_HOLD_COLUMNS,
                filter_expr=f"(TRADE_DATE='{period}')",
            )
            period_date = date.fromisoformat(period)
            for item in raw:
                sym = symbol_from_secucode(item.get("SECUCODE"))
                if not sym:
                    continue
                rows.append(
                    {
                        "symbol": sym,
                        "trade_date": period_date,
                        "channel": _channel(item.get("MUTUAL_TYPE")),
                        "holding_shares": float(item.get("HOLD_SHARES") or 0),
                        "holding_mv": float(item.get("HOLD_MARKET_CAP") or 0),
                        "holding_ratio": float(item.get("HOLD_SHARES_RATIO") or 0),
                    }
                )
    finally:
        if owns:
            client.close()
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fetch_northbound_flows_range(
    start: date,
    end: date,
    *,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    """Northbound 沪股通 / 深股通 daily flows over [*start*, *end*].

    The whole series is one request. ``RPT_MUTUAL_DEAL_HISTORY`` rejects range
    predicates on ``TRADE_DATE`` (``InputMismatchException``), and equality
    would cost one request per session — which the daily path cannot afford,
    because the watermark stops advancing the moment the source stops
    publishing and the gap window then grows without bound. Both northbound
    legs across the full history are ~5k rows, so pulling them and slicing
    locally is one request whatever the window.

    Rows with a null ``NET_DEAL_AMT`` are dropped rather than zero-filled: the
    exchanges stopped publishing daily northbound net flow after
    ``NORTHBOUND_LAST_PUBLISHED``, and a zero would claim a flat session where
    the truth is that no figure exists.
    """
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    try:
        raw = fetch_datacenter(
            client,
            _NORTH_FLOW_REPORT,
            _NORTH_FLOW_COLUMNS,
            filter_expr='(MUTUAL_TYPE in ("001","003"))',
            sort_columns="TRADE_DATE",
            sort_types="1",
        )
    finally:
        if owns:
            client.close()

    rows: list[dict] = []
    withheld = 0
    for item in raw:
        channel = _NORTHBOUND_CHANNELS.get(str(item.get("MUTUAL_TYPE") or "").strip())
        if channel is None:
            continue
        row_date = _em_datetime_to_date(item.get("TRADE_DATE"))
        if row_date is None or not (start <= row_date <= end):
            continue
        # Read the raw value, not ``_to_float``: that helper defaults a missing
        # amount to 0.0, which is exactly the zero this dataset must not invent.
        # From 2024-08-19 the column is null on every northbound row.
        net = item.get("NET_DEAL_AMT")
        if net is None or net == "" or net == "-":
            withheld += 1
            continue
        rows.append(
            {
                "trade_date": row_date,
                "channel": channel,
                "net_buy": _to_float(net) * _AMOUNT_SCALE,
                "buy_amount": _to_float(item.get("BUY_AMT")) * _AMOUNT_SCALE,
                "sell_amount": _to_float(item.get("SELL_AMT")) * _AMOUNT_SCALE,
            }
        )

    if withheld:
        logger.info(
            "northbound_flows: %d row(s) in %s..%s carry no net amount "
            "(source stopped publishing after %s)",
            withheld,
            start.isoformat(),
            end.isoformat(),
            NORTHBOUND_LAST_PUBLISHED.isoformat(),
        )
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fetch_northbound_flows(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    return fetch_northbound_flows_range(trade_date, trade_date, client=client, config=config)


def fetch_dragon_tiger(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    ds = trade_date.isoformat()
    raw = fetch_datacenter(
        client,
        _DRAGON_REPORT,
        _DRAGON_COLUMNS,
        filter_expr=f"(TRADE_DATE='{ds}')",
    )
    rows = []
    for item in raw:
        code = str(item.get("SECURITY_CODE", "")).zfill(6)
        exch = exchange_from_datacenter(item)
        sym = format_symbol(code, exch)
        if not symbol_from_em(code, 1 if exch == "SH" else 0):
            continue
        rows.append(
            {
                "symbol": sym,
                "trade_date": trade_date,
                "reason": str(item.get("EXPLANATION") or ""),
                "buy_amount": float(item.get("BILLBOARD_BUY_AMT") or 0),
                "sell_amount": float(item.get("BILLBOARD_SELL_AMT") or 0),
                "net_amount": float(item.get("BILLBOARD_NET_AMT") or 0),
            }
        )
    if owns:
        client.close()
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fetch_block_trades(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    ds = trade_date.isoformat()
    raw = fetch_datacenter(
        client,
        _BLOCK_REPORT,
        _BLOCK_COLUMNS,
        filter_expr=f"(TRADE_DATE='{ds}')",
    )
    rows = []
    for item in raw:
        code = str(item.get("SECURITY_CODE", "")).zfill(6)
        exch = exchange_from_datacenter(item)
        sym = symbol_from_em(code, 1 if exch == "SH" else (2 if exch == "BJ" else 0))
        if not sym:
            continue
        rows.append(
            {
                "symbol": sym,
                "trade_date": trade_date,
                "price": float(item.get("AVERAGE_PRICE") or 0),
                "volume": float(item.get("VOLUME") or 0),
                "amount": float(item.get("DEAL_AMT") or 0),
                "premium_ratio": float(item.get("PREMIUM_RATIO") or 0),
            }
        )
    if owns:
        client.close()
    return pl.DataFrame(rows) if rows else pl.DataFrame()
