"""CFFEX daily quotes and trading parameters, read from the exchange itself.

One CSV per session lists every live contract — index futures, treasury
futures and, since 2019-12-23, the index options — with its settlement price,
open interest and (for options) delta. It reaches back to IF1005's first
session, 2010-04-16. Each product block ends in a 「小计」 row and the file in
a 「合计」 row. Measured against the contracts above them, they agree exactly
on volume, turnover and open interest in files from 2010, 2015, 2019 and 2026,
so a file that does not add up is refused as damaged rather than written.

The header changed once: before options listed it carried an
「隐含波动率(%)」 column that was empty for futures, and from 2019-12 it does
not. Columns are therefore found by name, never by position.

CFFEX has always counted volume on one side, and publishes turnover in 万元.
A contract with no trades carries a settlement price and a carried-forward
「今收盘」; the lake stores that session's OHLC as null, because there was no
trade to have a price.

The per-session trading-parameter file (``sj/jycs``, back to 2010) names each
live contract's listing day and last trading day. That is the scheduled end
the contract tables need for contracts that have not expired yet.
"""

from __future__ import annotations

import csv
import io
import logging
import math
import xml.etree.ElementTree as ET
from datetime import date, datetime

import polars as pl

from cnequity.adapters.futures_exchange.common import (
    ExchangeDay,
    FuturesDayUnavailable,
    FuturesPayloadError,
    decode_text,
    drop_placeholders,
    fetch_bytes,
    parse_number,
    parse_option_settle,
    parse_price,
)
from cnequity.domain.derivatives import (
    WAN_YUAN,
    parse_future_code,
    parse_option_code,
)

logger = logging.getLogger(__name__)

EXCHANGE = "CFE"
FIRST_SESSION = date(2010, 4, 16)
FIRST_OPTION_SESSION = date(2019, 12, 23)

DAILY_URL = "http://www.cffex.com.cn/sj/hqsj/rtj/{ym}/{dd}/{ymd}_1.csv"
PARAMS_URL = "http://www.cffex.com.cn/sj/jycs/{ym}/{dd}/index.xml"

_SUMMARY_CODES = frozenset({"小计", "合计", "总计"})
_COLUMNS = {
    "code": "合约代码",
    "open": "今开盘",
    "high": "最高价",
    "low": "最低价",
    "volume": "成交量",
    "amount": "成交金额",
    "open_interest": "持仓量",
    "oi_change": "持仓变化",
    "close": "今收盘",
    "settle": "今结算",
    "pre_settle": "前结算",
    "delta": "Delta",
}


def _url(template: str, trade_date: date) -> str:
    return template.format(
        ym=trade_date.strftime("%Y%m"),
        dd=trade_date.strftime("%d"),
        ymd=trade_date.strftime("%Y%m%d"),
    )


def _int(value, field: str, code: str) -> int | None:
    """A count cell; blank stays None: the file prints 0 for none, never a blank."""
    number = parse_number(value)
    if number is None:
        return None
    if not math.isfinite(number) or not number.is_integer():
        raise FuturesPayloadError(f"CFFEX {code}: {field}={value!r} is not a whole number")
    return int(number)


def _check_block(product: str, block: list[dict], subtotal: list[str], index: dict) -> None:
    volume = sum(r["volume"] or 0 for r in block)
    oi = sum(r["open_interest"] or 0 for r in block)
    amount = sum(r["_amount_wan"] or 0.0 for r in block)
    want_volume = _int(subtotal[index["volume"]], "volume", "小计") or 0
    want_oi = _int(subtotal[index["open_interest"]], "open_interest", "小计") or 0
    want_amount = parse_number(subtotal[index["amount"]]) or 0.0
    if volume != want_volume or oi != want_oi or abs(amount - want_amount) > 0.01:
        raise FuturesPayloadError(
            f"CFFEX {product}: contracts sum to volume={volume} oi={oi} amount={amount:.3f} "
            f"but 小计 says {want_volume}/{want_oi}/{want_amount:.3f}"
        )


def parse_daily_csv(body: bytes, trade_date: date) -> ExchangeDay:
    """Split one CFFEX session file into futures and option rows."""
    text = decode_text(body)
    if "合约代码" not in text[:200]:
        # CFFEX serves an HTML page, not a CSV, for a day it has no file for.
        raise FuturesDayUnavailable(f"CFFEX {trade_date.isoformat()}: no daily file")
    rows = list(csv.reader(io.StringIO(text)))
    header = [cell.strip() for cell in rows[0]]
    try:
        index = {key: header.index(name) for key, name in _COLUMNS.items() if name != "Delta"}
    except ValueError as exc:
        raise FuturesPayloadError(f"CFFEX header changed: {header}") from exc
    delta_index = header.index("Delta") if "Delta" in header else None

    futures: list[dict] = []
    options: list[dict] = []
    block: list[dict] = []
    block_product: str | None = None
    for raw in rows[1:]:
        if not raw or not raw[0].strip():
            continue
        code = raw[0].strip()
        if code in _SUMMARY_CODES:
            if code == "小计" and block and block_product is not None:
                _check_block(block_product, block, raw, index)
            block, block_product = [], None
            continue

        def cell(key: str, row: list[str] = raw) -> str | None:
            return row[index[key]] if index[key] < len(row) else None

        volume = _int(cell("volume"), "volume", code)
        traded = bool(volume)
        amount_wan = parse_number(cell("amount"))
        settle = parse_price(cell("settle"))
        row = {
            "exchange": EXCHANGE,
            "exchange_code": code,
            "trade_date": trade_date,
            "open": parse_price(cell("open")) if traded else None,
            "high": parse_price(cell("high")) if traded else None,
            "low": parse_price(cell("low")) if traded else None,
            "close": parse_price(cell("close")) if traded else None,
            "settle": settle,
            "pre_settle": parse_price(cell("pre_settle")),
            "volume": volume,
            "amount": None if amount_wan is None else round(amount_wan * WAN_YUAN, 2),
            "open_interest": _int(cell("open_interest"), "open_interest", code),
            "oi_change": _int(cell("oi_change"), "oi_change", code),
            "_amount_wan": amount_wan,
        }
        if "-" in code:
            contract = parse_option_code(code, EXCHANGE, trade_date)
            delta = (
                parse_number(raw[delta_index])
                if delta_index is not None and delta_index < len(raw)
                else None
            )
            row.update(
                {
                    "symbol": contract.symbol,
                    "product": contract.product,
                    "underlying_symbol": contract.underlying_symbol,
                    "option_type": contract.option_type,
                    "strike": contract.strike,
                    "settle": parse_option_settle(cell("settle")),
                    # CFFEX publishes no exercise count in this file.
                    "exercise_volume": None,
                    "delta": delta,
                    "implied_vol": None,
                    "series_implied_vol": None,
                }
            )
            options.append(row)
            product = contract.product
        else:
            contract = parse_future_code(code, EXCHANGE, trade_date)
            row.update({"symbol": contract.symbol, "product": contract.product})
            futures.append(row)
            product = contract.product
        if block_product not in (None, product):
            raise FuturesPayloadError(
                f"CFFEX {trade_date.isoformat()}: {code} follows {block_product} without a 小计"
            )
        block_product = product
        block.append(row)
    if block:
        raise FuturesPayloadError(f"CFFEX {trade_date.isoformat()}: last block has no 小计")
    if not futures and not options:
        raise FuturesDayUnavailable(f"CFFEX {trade_date.isoformat()}: file lists no contracts")

    def frame(items: list[dict]) -> pl.DataFrame:
        if not items:
            return pl.DataFrame()
        return pl.DataFrame(drop_placeholders(items), infer_schema_length=None).drop("_amount_wan")

    return ExchangeDay(EXCHANGE, trade_date, futures=frame(futures), options=frame(options))


def fetch_cffex_day(trade_date: date, *, config=None) -> ExchangeDay:
    """Every CFFEX contract for *trade_date*, or :class:`FuturesDayUnavailable`."""
    if trade_date < FIRST_SESSION:
        raise FuturesDayUnavailable(f"CFFEX has no sessions before {FIRST_SESSION.isoformat()}")
    body = fetch_bytes(_url(DAILY_URL, trade_date), config=config, as_of=trade_date)
    return parse_daily_csv(body, trade_date)


def _xml_date(value: str | None) -> date | None:
    text = (value or "").strip()
    if len(text) != 8 or not text.isdigit():
        return None
    return datetime.strptime(text, "%Y%m%d").date()


def parse_params_xml(body: bytes, trade_date: date) -> pl.DataFrame:
    """Listing and last trading day for each contract live on *trade_date*."""
    try:
        root = ET.fromstring(body)
    except ET.ParseError as exc:
        raise FuturesDayUnavailable(
            f"CFFEX params {trade_date.isoformat()}: not an XML file"
        ) from exc
    rows: list[dict] = []
    for node in root:
        code = (node.findtext("INSTRUMENT_ID") or "").strip()
        if not code:
            continue
        kind = "option" if "-" in code else "future"
        parse = parse_option_code if kind == "option" else parse_future_code
        try:
            contract = parse(code, EXCHANGE, trade_date)
        except ValueError:
            logger.warning("CFFEX params: unrecognised contract code %r", code)
            continue
        rows.append(
            {
                "symbol": contract.symbol,
                "kind": kind,
                "exchange_code": code,
                "list_date": _xml_date(node.findtext("OPEN_DATE")),
                "last_trade_date": _xml_date(node.findtext("END_TRADING_DAY")),
                "as_of": trade_date,
            }
        )
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fetch_cffex_params(trade_date: date, *, config=None) -> pl.DataFrame:
    body = fetch_bytes(_url(PARAMS_URL, trade_date), config=config)
    return parse_params_xml(body, trade_date)
