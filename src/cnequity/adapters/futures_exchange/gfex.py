"""GFEX daily quotes and contract reference data, read from the exchange.

One POST per session and instrument kind (``trade_type`` 0 futures, 1
options) returns every live contract from the exchange's first session,
2022-12-22 (options 2022-12-23). Each product ends in a 「<品种>小计」 row and the
response in 「总计」; the contracts sum to them exactly. A closed session
answers 200 with nothing but an all-zero 「总计」, which reads here as "no file".

GFEX counts on one side and publishes turnover in 万元. Untraded contracts
carry zero prices and a settlement. Options come with a per-contract ``delta``
and ``impliedVolatility`` in percent, stored as a fraction, and an exercise
count (``matchQtySum``). For futures ``delivMonth`` is the month ("2506"); for
options it is the whole contract code ("ps2506-C-37000").

The contract-information endpoint lists live contracts with their first and
last trading day, one request per product and kind.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime

import polars as pl

from cnequity.adapters.futures_exchange.common import (
    ExchangeDay,
    FuturesDayUnavailable,
    FuturesPayloadError,
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
    percent_to_fraction,
)

logger = logging.getLogger(__name__)

EXCHANGE = "GFE"
FIRST_SESSION = date(2022, 12, 22)
FIRST_OPTION_SESSION = date(2022, 12, 23)

QUOTES_URL = "http://www.gfex.com.cn/u/interfacesWebTiDayQuotes/loadList"
CONTRACTS_URL = "http://www.gfex.com.cn/u/interfacesWebTtQueryContractInfo/loadList"


def _rows(body: bytes, trade_date: date) -> list[dict]:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FuturesPayloadError(f"GFEX {trade_date.isoformat()}: not a JSON answer") from exc
    if str(payload.get("code")) != "0":
        raise FuturesPayloadError(f"GFEX {trade_date.isoformat()}: {payload.get('msg')!r}")
    return payload.get("data") or []


def _is_summary(row: dict) -> bool:
    return "计" in str(row.get("variety") or "")


def _checked(rows: list[dict], trade_date: date) -> list[dict]:
    contracts = [r for r in rows if not _is_summary(r)]
    if not contracts:
        raise FuturesDayUnavailable(f"GFEX {trade_date.isoformat()}: no contracts listed")
    for subtotal in (r for r in rows if str(r.get("variety") or "").endswith("小计")):
        product = str(subtotal.get("variety"))[: -len("小计")]
        block = [r for r in contracts if r.get("variety") == product]
        for field in ("volumn", "openInterest"):
            got = sum(parse_number(r.get(field)) or 0.0 for r in block)
            want = parse_number(subtotal.get(field)) or 0.0
            if got != want:
                raise FuturesPayloadError(
                    f"GFEX {trade_date.isoformat()} {product}: {field} {got} vs 小计 {want}"
                )
        got = sum(parse_number(r.get("turnover")) or 0.0 for r in block)
        want = parse_number(subtotal.get("turnover")) or 0.0
        if abs(got - want) > 0.005 * len(block) + 0.01:
            raise FuturesPayloadError(
                f"GFEX {trade_date.isoformat()} {product}: turnover {got} vs 小计 {want}"
            )
    return contracts


def _whole(value) -> int | None:
    """A count cell; blank stays None: the file prints 0 for none, never a blank."""
    number = parse_number(value)
    return None if number is None else int(number)


def _bar(raw: dict, code: str, trade_date: date) -> dict:
    volume = _whole(raw.get("volumn"))
    traded = bool(volume)
    amount = parse_number(raw.get("turnover"))
    return {
        "exchange": EXCHANGE,
        "exchange_code": code,
        "trade_date": trade_date,
        "open": parse_price(raw.get("open")) if traded else None,
        "high": parse_price(raw.get("high")) if traded else None,
        "low": parse_price(raw.get("low")) if traded else None,
        "close": parse_price(raw.get("close")) if traded else None,
        "settle": parse_price(raw.get("clearPrice")),
        "pre_settle": parse_price(raw.get("lastClear")),
        "volume": volume,
        "amount": None if amount is None else round(amount * WAN_YUAN, 2),
        "open_interest": _whole(raw.get("openInterest")),
        "oi_change": _whole(raw.get("diffI")),
    }


def parse_futures(body: bytes, trade_date: date) -> pl.DataFrame:
    rows = []
    for raw in _checked(_rows(body, trade_date), trade_date):
        code = f"{str(raw.get('varietyOrder')).strip()}{str(raw.get('delivMonth')).strip()}"
        contract = parse_future_code(code, EXCHANGE, trade_date)
        rows.append(
            {
                "symbol": contract.symbol,
                "product": contract.product,
                **_bar(raw, code, trade_date),
            }
        )
    return pl.DataFrame(drop_placeholders(rows), infer_schema_length=None)


def parse_options(body: bytes, trade_date: date) -> pl.DataFrame:
    rows = []
    for raw in _checked(_rows(body, trade_date), trade_date):
        code = str(raw.get("delivMonth") or "").strip()
        contract = parse_option_code(code, EXCHANGE, trade_date)
        exercised = parse_number(raw.get("matchQtySum"))
        rows.append(
            {
                "symbol": contract.symbol,
                "product": contract.product,
                "underlying_symbol": contract.underlying_symbol,
                "option_type": contract.option_type,
                "strike": contract.strike,
                **_bar(raw, code, trade_date),
                "settle": parse_option_settle(raw.get("clearPrice")),
                "exercise_volume": None if exercised is None else int(exercised),
                "delta": parse_number(raw.get("delta")),
                "implied_vol": percent_to_fraction(parse_number(raw.get("impliedVolatility"))),
                "series_implied_vol": None,
            }
        )
    return pl.DataFrame(drop_placeholders(rows), infer_schema_length=None)


def _post(trade_date: date, trade_type: int, *, config=None) -> bytes:
    return fetch_bytes(
        QUOTES_URL,
        config=config,
        as_of=trade_date,
        method="POST",
        data={"trade_date": trade_date.strftime("%Y%m%d"), "trade_type": str(trade_type)},
    )


def fetch_gfex_day(trade_date: date, *, config=None, kind: str | None = None) -> ExchangeDay:
    if trade_date < FIRST_SESSION:
        raise FuturesDayUnavailable(f"GFEX opened {FIRST_SESSION.isoformat()}")
    futures = pl.DataFrame()
    if kind != "options":
        futures = parse_futures(_post(trade_date, 0, config=config), trade_date)
    options = pl.DataFrame()
    if kind != "futures" and trade_date >= FIRST_OPTION_SESSION:
        options = parse_options(_post(trade_date, 1, config=config), trade_date)
    return ExchangeDay(EXCHANGE, trade_date, futures=futures, options=options)


def _day(value) -> date | None:
    text = str(value or "").strip()
    return datetime.strptime(text, "%Y%m%d").date() if len(text) == 8 and text.isdigit() else None


def parse_reference(body: bytes, trade_date: date, *, kind: str) -> pl.DataFrame:
    rows = []
    parse = parse_option_code if kind == "option" else parse_future_code
    for raw in _rows(body, trade_date):
        code = str(raw.get("contractId") or "").strip()
        try:
            contract = parse(code, EXCHANGE, trade_date)
        except ValueError:
            continue
        rows.append(
            {
                "symbol": contract.symbol,
                "kind": kind,
                "exchange_code": code,
                "list_date": _day(raw.get("startTradeDate")),
                "last_trade_date": _day(raw.get("endTradeDate")),
                "as_of": trade_date,
            }
        )
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fetch_gfex_reference(
    trade_date: date, *, config=None, products: list[str] | None = None, kind: str | None = None
) -> pl.DataFrame:
    """Contract information for each product seen in that session's quotes."""
    if products is None:
        futures = parse_futures(_post(trade_date, 0, config=config), trade_date)
        products = sorted({p.lower() for p in futures["product"].to_list()})
    frames = []
    for product in products:
        for trade_type, reference_kind in ((0, "future"), (1, "option")):
            if kind is not None and reference_kind != kind:
                continue
            body = fetch_bytes(
                CONTRACTS_URL,
                config=config,
                method="POST",
                data={"variety": product, "trade_type": str(trade_type)},
            )
            # This endpoint has no date parameter: it is today's snapshot,
            # even when products were selected from historical quotes.
            from cnequity.domain.market_time import shanghai_today

            frame = parse_reference(body, shanghai_today(), kind=reference_kind)
            if not frame.is_empty():
                frames.append(frame)
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
