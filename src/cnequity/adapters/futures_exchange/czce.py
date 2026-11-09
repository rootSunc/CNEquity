"""CZCE daily quotes and contract reference data, read from the exchange.

Two archives, both plain HTTP (measured 2026-09-25):

- ``cn/exchange/{year}/datadaily/{day}.txt`` — comma separated, no header, from
  2010-01-04 through 2015-10. Earlier days answer 404, and the older HTML
  archive (``cn/exchange/jyxx/hq``) sits behind a JavaScript challenge, so CZCE
  history in this lake starts at 2010-01-04.
- ``cn/DFSStaticFiles/{Future|Option}/{year}/{day}/...DataDaily.txt`` — pipe
  separated with a header, from 2015-09 on. The header renamed its columns
  once (「品种月份/空盘量」 → 「合约代码/持仓量」), and the encoding moved from GBK
  to UTF-8, so columns are found by name and the text is decoded by trial.

Codes carry one year digit (``TA601``); ``domain.derivatives`` resolves the
decade from the trading day. Every product (futures) or expiry series
(options) ends in a 「小计」 row, options add a per-product 「合计」, and the file
ends in 「总计」. Volume and open interest in the subtotals equal the contracts
exactly. Turnover is rounded per contract to 0.01 万元 while the subtotal is
not, so it is allowed half a cent per row. Counts before 2020 are
double-sided, options included, and are halved here.

An untraded contract is printed with zero prices; the lake stores null. Option
implied volatility is published in percent and stored as a fraction.

``FutureDataReferenceData.xml`` / ``OptionDataReferenceData.xml`` list live
contracts with their first and last trading day. They exist only for recent
sessions.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import date

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
    parse_future_code,
    parse_option_code,
    percent_to_fraction,
    single_sided_amount,
    single_sided_count,
)

logger = logging.getLogger(__name__)

EXCHANGE = "CZC"
FIRST_SESSION = date(2010, 1, 4)
FIRST_OPTION_SESSION = date(2017, 4, 19)
#: First day read from DFSStaticFiles; the comma archive covers everything before.
DFS_SINCE = date(2015, 10, 1)

ARCHIVE_URL = "http://www.czce.com.cn/cn/exchange/{y}/datadaily/{ymd}.txt"
FUTURES_URL = "https://www.czce.com.cn/cn/DFSStaticFiles/Future/{y}/{ymd}/FutureDataDaily.txt"
OPTIONS_URL = "https://www.czce.com.cn/cn/DFSStaticFiles/Option/{y}/{ymd}/OptionDataDaily.txt"
FUTURES_REFERENCE_URL = (
    "https://www.czce.com.cn/cn/DFSStaticFiles/Future/{y}/{ymd}/FutureDataReferenceData.xml"
)
OPTIONS_REFERENCE_URL = (
    "https://www.czce.com.cn/cn/DFSStaticFiles/Option/{y}/{ymd}/OptionDataReferenceData.xml"
)

#: Column order of the headerless comma archive.
_ARCHIVE_COLUMNS = (
    "code",
    "pre_settle",
    "open",
    "high",
    "low",
    "close",
    "settle",
    "chg1",
    "chg2",
    "volume",
    "open_interest",
    "oi_change",
    "amount",
    "delivery_settle",
)
_HEADER = {
    "品种月份": "code",
    "合约代码": "code",
    "品种代码": "code",
    "昨结算": "pre_settle",
    "今开盘": "open",
    "最高价": "high",
    "最低价": "low",
    "今收盘": "close",
    "今结算": "settle",
    "成交量(手)": "volume",
    "空盘量": "open_interest",
    "持仓量": "open_interest",
    "增减量": "oi_change",
    "成交额(万元)": "amount",
    "DELTA": "delta",
    "隐含波动率": "implied_vol",
    "行权量": "exercise_volume",
}
_CONTRACT = re.compile(r"^[A-Z]{1,3}\d{3}")


def _urls(template: str, trade_date: date) -> str:
    return template.format(y=trade_date.year, ymd=trade_date.strftime("%Y%m%d"))


def _records(text: str, trade_date: date) -> list[dict]:
    """Rows of either archive as dicts, summary rows marked by ``code``."""
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 2 or "行情表" not in lines[0]:
        raise FuturesDayUnavailable(f"CZCE {trade_date.isoformat()}: no daily file")
    if "|" in lines[1]:
        header = [_HEADER.get(cell.strip()) for cell in lines[1].split("|")]
        if header[0] != "code" or "settle" not in header or "volume" not in header:
            raise FuturesPayloadError(f"CZCE header changed: {lines[1][:120]}")
        body = [[cell.strip() for cell in line.split("|")] for line in lines[2:]]
    else:
        header = list(_ARCHIVE_COLUMNS)
        body = [[cell.strip() for cell in line.split(",")] for line in lines[1:]]
    return [
        {key: cells[i] for i, key in enumerate(header) if key and i < len(cells)}
        for cells in body
        if cells and cells[0]
    ]


def _blocks(records: list[dict], trade_date: date):
    block: list[dict] = []
    for record in records:
        code = record["code"]
        if _CONTRACT.match(code):
            block.append(record)
            continue
        if code == "小计":
            _check(block, record, trade_date)
            yield from block
            block = []
    if block:
        raise FuturesPayloadError(f"CZCE {trade_date.isoformat()}: last block has no 小计")


def _check(block: list[dict], subtotal: dict, trade_date: date) -> None:
    label = f"CZCE {trade_date.isoformat()} {block[0]['code'][:2] if block else ''}"
    for field in ("volume", "open_interest"):
        got = sum(parse_number(r.get(field)) or 0.0 for r in block)
        want = parse_number(subtotal.get(field)) or 0.0
        if got != want:
            raise FuturesPayloadError(f"{label}: contracts sum to {field}={got}, 小计 {want}")
    got = sum(parse_number(r.get("amount")) or 0.0 for r in block)
    want = parse_number(subtotal.get("amount")) or 0.0
    if abs(got - want) > 0.005 * len(block) + 0.01:
        raise FuturesPayloadError(f"{label}: contracts sum to amount={got:.2f}, 小计 {want:.2f}")


def _bar(record: dict, trade_date: date) -> dict:
    def count(field: str) -> int | None:
        return single_sided_count(
            parse_number(record.get(field)),
            exchange=EXCHANGE,
            trade_date=trade_date,
            field=field,
        )

    volume = count("volume")
    traded = bool(volume)
    amount = single_sided_amount(
        parse_number(record.get("amount")), exchange=EXCHANGE, trade_date=trade_date
    )
    return {
        "exchange": EXCHANGE,
        "exchange_code": record["code"],
        "trade_date": trade_date,
        "open": parse_price(record.get("open")) if traded else None,
        "high": parse_price(record.get("high")) if traded else None,
        "low": parse_price(record.get("low")) if traded else None,
        "close": parse_price(record.get("close")) if traded else None,
        "settle": parse_price(record.get("settle")),
        "pre_settle": parse_price(record.get("pre_settle")),
        "volume": volume,
        "amount": None if amount is None else round(amount, 2),
        "open_interest": count("open_interest"),
        "oi_change": count("oi_change"),
    }


def parse_futures(body: bytes, trade_date: date) -> pl.DataFrame:
    rows = []
    for record in _blocks(_records(decode_text(body), trade_date), trade_date):
        contract = parse_future_code(record["code"], EXCHANGE, trade_date)
        rows.append(
            {"symbol": contract.symbol, "product": contract.product, **_bar(record, trade_date)}
        )
    if not rows:
        raise FuturesDayUnavailable(f"CZCE {trade_date.isoformat()}: file lists no contracts")
    return pl.DataFrame(drop_placeholders(rows), infer_schema_length=None)


def parse_options(body: bytes, trade_date: date) -> pl.DataFrame:
    rows = []
    for record in _blocks(_records(decode_text(body), trade_date), trade_date):
        contract = parse_option_code(record["code"], EXCHANGE, trade_date)
        exercised = parse_number(record.get("exercise_volume"))
        rows.append(
            {
                "symbol": contract.symbol,
                "product": contract.product,
                "underlying_symbol": contract.underlying_symbol,
                "option_type": contract.option_type,
                "strike": contract.strike,
                **_bar(record, trade_date),
                "settle": parse_option_settle(record.get("settle")),
                "exercise_volume": None
                if exercised is None
                else single_sided_count(
                    exercised, exchange=EXCHANGE, trade_date=trade_date, field="exercise"
                ),
                "delta": parse_number(record.get("delta")),
                "implied_vol": percent_to_fraction(parse_number(record.get("implied_vol"))),
                "series_implied_vol": None,
            }
        )
    # The option expiry file can publish a zero-settlement row with no trades
    # or remaining position. It is still an exchange observation (and can
    # carry the day's closing position change or exercises).
    return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()


def fetch_czce_day(trade_date: date, *, config=None, kind: str | None = None) -> ExchangeDay:
    if trade_date < FIRST_SESSION:
        raise FuturesDayUnavailable(f"CZCE files start {FIRST_SESSION.isoformat()}")
    template = FUTURES_URL if trade_date >= DFS_SINCE else ARCHIVE_URL
    futures = pl.DataFrame()
    if kind != "options":
        futures = parse_futures(
            fetch_bytes(
                _urls(template, trade_date), config=config, as_of=trade_date, follow_redirects=True
            ),
            trade_date,
        )
    options = pl.DataFrame()
    if kind != "futures" and trade_date >= FIRST_OPTION_SESSION:
        options = parse_options(
            fetch_bytes(
                _urls(OPTIONS_URL, trade_date),
                config=config,
                as_of=trade_date,
                follow_redirects=True,
            ),
            trade_date,
        )
    return ExchangeDay(EXCHANGE, trade_date, futures=futures, options=options)


def _xml_day(value: str | None) -> date | None:
    text = (value or "").strip()[:10]
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def parse_reference(body: bytes, trade_date: date, *, kind: str) -> pl.DataFrame:
    try:
        root = ET.fromstring(decode_text(body).lstrip("﻿"))
    except ET.ParseError as exc:
        raise FuturesDayUnavailable(f"CZCE reference {trade_date}: not an XML file") from exc
    parse = parse_option_code if kind == "option" else parse_future_code
    rows = []
    for node in root:
        code = (node.findtext("CtrCd") or "").strip()
        if not code:
            continue
        try:
            contract = parse(code, EXCHANGE, trade_date)
        except ValueError:
            logger.warning("CZCE reference: unrecognised contract code %r", code)
            continue
        last = node.findtext("ExpiryDt") if kind == "option" else None
        rows.append(
            {
                "symbol": contract.symbol,
                "kind": kind,
                "exchange_code": code,
                "list_date": _xml_day(node.findtext("FrstTrdDt")),
                "last_trade_date": _xml_day(last or node.findtext("LstTrdDt")),
                "as_of": trade_date,
            }
        )
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fetch_czce_reference(trade_date: date, *, config=None, kind: str | None = None) -> pl.DataFrame:
    frames = []
    for template, reference_kind in (
        (FUTURES_REFERENCE_URL, "future"),
        (OPTIONS_REFERENCE_URL, "option"),
    ):
        if kind is not None and reference_kind != kind:
            continue
        body = fetch_bytes(_urls(template, trade_date), config=config, follow_redirects=True)
        frame = parse_reference(body, trade_date, kind=reference_kind)
        if not frame.is_empty():
            frames.append(frame)
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
