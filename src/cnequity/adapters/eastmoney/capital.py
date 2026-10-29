"""EastMoney L4 capital datasets: fund flow, margin, northbound, dragon tiger, block trades."""

from __future__ import annotations

import inspect
import logging
import math
from datetime import date, datetime

import polars as pl

from cnequity.adapters.eastmoney.clist import clist_rows_to_symbols, fetch_clist_pages
from cnequity.adapters.eastmoney.common import (
    _to_float,
    exchange_from_datacenter,
    symbol_from_em,
    symbol_from_secucode,
)
from cnequity.adapters.eastmoney.datacenter import fetch_datacenter
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient, rate_limit_if_unconfigured
from cnequity.adapters.eastmoney.raw import configured_archive
from cnequity.config import Config
from cnequity.domain.market_time import last_closed_session, shanghai_today
from cnequity.domain.symbols import infer_exchange_from_code
from cnequity.storage.raw_archive import RawArchiveError, captured_records

logger = logging.getLogger(__name__)

_FUND_FLOW_FIELDS = "f12,f13,f62,f66,f72,f78,f84"
# 千股千评 (data.eastmoney.com/stockcomment), served by datacenter-web rather
# than push2. It carries EastMoney's own super-large and large in/outflow, and
# PRIME_INFLOW equals their net sum exactly, which is push2's f62. It has no
# medium / small split and holds only the latest session (a past TRADE_DATE
# filter answers 9201 "返回数据为空"), refreshed during trading hours.
_FUND_FLOW_REPORT = "RPT_DMSK_TS_STOCKNEW"
_FUND_FLOW_REPORT_COLUMNS = (
    "SECUCODE,TRADE_DATE,PRIME_INFLOW,SUPERDEAL_INFLOW,SUPERDEAL_OUTFLOW,"
    "BIGDEAL_INFLOW,BIGDEAL_OUTFLOW"
)
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


def _scaled_amount(value: float | None) -> float | None:
    if value is None:
        return None
    scaled = value * _AMOUNT_SCALE
    return scaled if math.isfinite(scaled) else None


def _channel(mutual_type: str | int | None) -> str | None:
    text = str(mutual_type or "")
    if text in {"001", "1"} or "沪" in text or "SH" in text.upper():
        return "SH"
    if text in {"003", "3"} or "深" in text or "SZ" in text.upper():
        return "SZ"
    return None


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
        # Some margin rows omit both SECUCODE and the market label.  Defaulting
        # those rows to SZ mislabels 60/68xxxx Shanghai names and breaks joins
        # with daily_bars; use the same code inference as the other adapters.
        exch = infer_exchange_from_code(code)
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


def _rows_for_report_date(raw: list[dict], field: str, expected: date) -> list[dict]:
    rows = [item for item in raw if _report_row_matches_date(item, field, expected)]
    dropped = len(raw) - len(rows)
    if dropped:
        logger.warning(
            "EastMoney capital dropped %d row(s) with invalid or unexpected %s for %s",
            dropped,
            field,
            expected.isoformat(),
        )
    if raw and not rows:
        raise RuntimeError(
            f"EastMoney capital response contains no {field} row for {expected.isoformat()}"
        )
    return rows


def _report_row_matches_date(item: dict, field: str, expected: date) -> bool:
    actual = _em_datetime_to_date(item.get(field))
    if actual == expected:
        return True
    return False


def fetch_fund_flow(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
    run_id: str | None = None,
) -> pl.DataFrame:
    if getattr(config, "eastmoney_push2_paused", False):
        return fetch_fund_flow_datacenter(trade_date, client=client, config=config, run_id=run_id)
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    try:
        archive = configured_archive(
            config,
            "fund_flow",
            run_id=run_id,
            request_scope=f"daily:{trade_date.isoformat()}",
        )
        capture_required = False
        if archive is None:
            rows_raw = fetch_clist_pages(client, fields=_FUND_FLOW_FIELDS)
        else:
            try:
                parameters = inspect.signature(fetch_clist_pages).parameters.values()
            except (TypeError, ValueError):
                parameters = ()
            supports_archive = any(
                parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters
            ) or any(parameter.name == "archive" for parameter in parameters)
            if not supports_archive:
                raise RawArchiveError(
                    "fund_flow: archive is required but the pagination helper cannot "
                    "provide exact wire capture"
                )
            capture_required = True
            rows_raw = fetch_clist_pages(
                client,
                fields=_FUND_FLOW_FIELDS,
                archive=archive,
                archive_dataset="fund_flow",
                archive_run_id=run_id,
            )
        if capture_required:
            archived = captured_records(
                config,
                "fund_flow",
                str(run_id or ""),
                source="eastmoney",
                request_scope=f"daily:{trade_date.isoformat()}",
            )
            if not archived:
                raise RawArchiveError(
                    "fund_flow: archive is required but the EastMoney adapter produced "
                    "no exact wire observation"
                )
        mapped_rows = clist_rows_to_symbols(rows_raw)
        if len(mapped_rows) != len(rows_raw):
            logger.warning(
                "EastMoney fund_flow clist dropped %d non-security row(s)",
                len(rows_raw) - len(mapped_rows),
            )
        rows = []
        for sym, item in mapped_rows:
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
    finally:
        if owns:
            client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "trade_date"], keep="last")


def _net(item: dict, inflow: str, outflow: str) -> float | None:
    inflow_value, outflow_value = _to_float(item.get(inflow)), _to_float(item.get(outflow))
    if inflow_value is None or outflow_value is None:
        return None
    return inflow_value - outflow_value


def fetch_fund_flow_datacenter(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
    run_id: str | None = None,
    now: datetime | None = None,
) -> pl.DataFrame:
    """``fund_flow`` from 千股千评 when push2 is out of reach.

    Main, super-large and large match push2; medium and small stay null, since
    the report gives only their sum (the four nets sum to zero).
    """
    if trade_date >= shanghai_today(now) and last_closed_session(now) != trade_date:
        # The report is refreshed intraday under today's TRADE_DATE.
        raise RuntimeError(
            f"fund_flow: {_FUND_FLOW_REPORT} for {trade_date.isoformat()} is not final "
            "before the 15:30 close"
        )
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    try:
        archive = configured_archive(
            config,
            "fund_flow",
            run_id=run_id,
            request_scope=f"daily:{trade_date.isoformat()}",
        )
        raw = fetch_datacenter(
            client,
            _FUND_FLOW_REPORT,
            _FUND_FLOW_REPORT_COLUMNS,
            sort_columns="SECUCODE",
            sort_types="1",
            archive=archive,
            archive_dataset="fund_flow",
            archive_run_id=run_id,
            archive_request_scope=f"daily:{trade_date.isoformat()}",
        )
        rows = []
        for item in _rows_for_report_date(raw, "TRADE_DATE", trade_date):
            sym = symbol_from_secucode(item.get("SECUCODE"))
            if not sym:
                continue
            rows.append(
                {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "main_net_inflow": _to_float(item.get("PRIME_INFLOW")),
                    "super_large_net_inflow": _net(item, "SUPERDEAL_INFLOW", "SUPERDEAL_OUTFLOW"),
                    "large_net_inflow": _net(item, "BIGDEAL_INFLOW", "BIGDEAL_OUTFLOW"),
                    "medium_net_inflow": None,
                    "small_net_inflow": None,
                }
            )
    finally:
        if owns:
            client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(
        rows, schema_overrides={"medium_net_inflow": pl.Float64, "small_net_inflow": pl.Float64}
    ).unique(subset=["symbol", "trade_date"], keep="last")


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
    try:
        raw = fetch_datacenter(
            client,
            _MARGIN_REPORT,
            _MARGIN_COLUMNS,
            filter_expr=f"(DATE='{ds}')",
        )
        rows = []
        for item in _rows_for_report_date(raw, "DATE", trade_date):
            sym = _margin_symbol(item)
            if not sym:
                continue
            rows.append(
                {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "margin_balance": _to_float(item.get("RZYE")),
                    "margin_buy": _to_float(item.get("RZMRE")),
                    "short_balance": _to_float(item.get("RQYE")),
                    "short_sell_volume": _to_float(item.get("RQMCL")),
                }
            )
    finally:
        if owns:
            client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "trade_date"], keep="last")


_NORTH_BACKFILL_START_YEAR = 2016
_NORTH_QUARTER_END_MMDD = (("03", "31"), ("06", "30"), ("09", "30"), ("12", "31"))


def _quarter_end_dates(
    trade_date: date,
    *,
    start: date | None = None,
    end: date | None = None,
) -> list[str]:
    """Quarter-end dates in the requested window, most recent first."""
    lower = date(_NORTH_BACKFILL_START_YEAR, 1, 1)
    if start is not None:
        lower = max(lower, start)
    upper = trade_date if end is None else min(trade_date, end)
    if lower > upper:
        return []

    out: list[str] = []
    for year in range(lower.year, upper.year + 1):
        for mm, dd in _NORTH_QUARTER_END_MMDD:
            ds = f"{year}-{mm}-{dd}"
            if lower <= date.fromisoformat(ds) <= upper:
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
    # itself does not serve history here — see docs/zh/datasets/sources.md.
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    if backfill:
        range_start = getattr(config, "_backfill_start", None)
        range_end = getattr(config, "_backfill_end", None)
        periods = _quarter_end_dates(trade_date, start=range_start, end=range_end)
    else:
        periods = _quarter_end_dates(trade_date)
    if not backfill:
        # latest 2 quarters: the just-ended one may not be published yet, so
        # keep the last complete quarter fresh too.
        periods = periods[:2]

    rows: list[dict] = []
    try:
        for period in periods:
            rate_limit_if_unconfigured(client, config)
            raw = fetch_datacenter(
                client,
                _NORTH_HOLD_REPORT,
                _NORTH_HOLD_COLUMNS,
                filter_expr=f"(TRADE_DATE='{period}')",
            )
            period_date = date.fromisoformat(period)
            for item in _rows_for_report_date(raw, "TRADE_DATE", period_date):
                sym = symbol_from_secucode(item.get("SECUCODE"))
                channel = _channel(item.get("MUTUAL_TYPE"))
                if not sym or channel is None:
                    if channel is None:
                        logger.warning(
                            "EastMoney northbound holdings: skipping unknown MUTUAL_TYPE %r",
                            item.get("MUTUAL_TYPE"),
                        )
                    continue
                rows.append(
                    {
                        "symbol": sym,
                        "trade_date": period_date,
                        "channel": channel,
                        "holding_shares": _to_float(item.get("HOLD_SHARES")),
                        "holding_mv": _to_float(item.get("HOLD_MARKET_CAP")),
                        "holding_ratio": _to_float(item.get("HOLD_SHARES_RATIO")),
                    }
                )
    finally:
        if owns:
            client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "trade_date", "channel"], keep="last")


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
        # Read the raw value first because a missing amount is not a real zero.
        # From 2024-08-19 the column is null on every northbound row.
        net = item.get("NET_DEAL_AMT")
        if net is None or net == "" or net == "-":
            withheld += 1
            continue
        net_value = _to_float(net)
        if net_value is None:
            withheld += 1
            continue
        net_buy = _scaled_amount(net_value)
        if net_buy is None:
            withheld += 1
            continue
        buy_value = _to_float(item.get("BUY_AMT"))
        sell_value = _to_float(item.get("SELL_AMT"))
        rows.append(
            {
                "trade_date": row_date,
                "channel": channel,
                "net_buy": net_buy,
                "buy_amount": _scaled_amount(buy_value),
                "sell_amount": _scaled_amount(sell_value),
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
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["trade_date", "channel"], keep="last")


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
    try:
        raw = fetch_datacenter(
            client,
            _DRAGON_REPORT,
            _DRAGON_COLUMNS,
            filter_expr=f"(TRADE_DATE='{ds}')",
        )
        rows = []
        for item in _rows_for_report_date(raw, "TRADE_DATE", trade_date):
            code = str(item.get("SECURITY_CODE", "")).zfill(6)
            exch = exchange_from_datacenter(item)
            sym = symbol_from_em(code, 1 if exch == "SH" else (2 if exch == "BJ" else 0))
            if not sym:
                continue
            rows.append(
                {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "reason": str(item.get("EXPLANATION") or ""),
                    "buy_amount": _to_float(item.get("BILLBOARD_BUY_AMT")),
                    "sell_amount": _to_float(item.get("BILLBOARD_SELL_AMT")),
                    "net_amount": _to_float(item.get("BILLBOARD_NET_AMT")),
                }
            )
    finally:
        if owns:
            client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=["symbol", "trade_date", "reason"], keep="last")


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
    try:
        raw = fetch_datacenter(
            client,
            _BLOCK_REPORT,
            _BLOCK_COLUMNS,
            filter_expr=f"(TRADE_DATE='{ds}')",
        )
        rows = []
        for item in _rows_for_report_date(raw, "TRADE_DATE", trade_date):
            code = str(item.get("SECURITY_CODE", "")).zfill(6)
            exch = exchange_from_datacenter(item)
            sym = symbol_from_em(code, 1 if exch == "SH" else (2 if exch == "BJ" else 0))
            if not sym:
                continue
            rows.append(
                {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "price": _to_float(item.get("AVERAGE_PRICE")),
                    "volume": _to_float(item.get("VOLUME")),
                    "amount": _to_float(item.get("DEAL_AMT")),
                    "premium_ratio": _to_float(item.get("PREMIUM_RATIO")),
                }
            )
    finally:
        if owns:
            client.close()
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(
        subset=["symbol", "trade_date", "price", "volume"], keep="last"
    )
