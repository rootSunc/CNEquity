"""Experimental DCE official route, opt-in and not verified on live payloads.

The measured endpoints returned HTTP 412 at the tested exits in September
2026. Other network conditions are unknown. A successful health response
alone is not acceptance: required fields, response dates when present, schema
and published grand totals must pass; live fixtures still need verification.
"""

from __future__ import annotations

import json
import logging
from datetime import date

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
    parse_future_code,
    parse_option_code,
    percent_to_fraction,
    single_sided_amount,
    single_sided_count,
)

logger = logging.getLogger(__name__)

EXCHANGE = "DCE"
FIRST_SESSION = date(2000, 1, 4)
QUOTES_URL = "http://www.dce.com.cn/dcereport/publicweb/dailystat/dayQuotes"
#: Option products, as the endpoint's ``varietyId`` spells them.
OPTION_PRODUCTS: tuple[str, ...] = (
    "m", "c", "i", "pg", "l", "v", "pp", "p", "a", "b", "y", "eg", "eb", "jd", "cs", "lh", "lg",
)  # fmt: skip


def _rows(body: bytes, trade_date: date) -> list[dict]:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FuturesPayloadError(f"DCE {trade_date.isoformat()}: not a JSON answer") from exc
    raw_rows = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(raw_rows, list) or any(not isinstance(r, dict) for r in raw_rows):
        raise FuturesPayloadError(f"DCE {trade_date}: expected a data array")
    rows = [r for r in raw_rows if "计" not in str(r.get("variety") or "")]
    if not rows:
        raise FuturesDayUnavailable(f"DCE {trade_date.isoformat()}: no contracts listed")
    required = ("volumn", "openInterest", "turnover", "clearPrice")
    for row in rows:
        if not row.get("contractId") or any(
            parse_number(row.get(field)) is None for field in required
        ):
            raise FuturesPayloadError(f"DCE {trade_date}: missing contract or numeric fields")
        response_day = str(row.get("tradeDate") or "").replace("-", "")[:8]
        if response_day and response_day != trade_date.strftime("%Y%m%d"):
            raise FuturesPayloadError(f"DCE {trade_date}: response date mismatch")
    # This route is experimental: require independently published totals and
    # fail closed rather than silently accepting a truncated candidate mapping.
    totals = [r for r in raw_rows if "总计" in str(r.get("variety") or "")]
    if len(totals) != 1:
        raise FuturesPayloadError(
            f"DCE {trade_date}: missing unique grand total; mapping unverified"
        )
    for field in ("volumn", "openInterest", "turnover"):
        total = parse_number(totals[0].get(field))
        observed = sum(parse_number(r[field]) for r in rows)
        tolerance = max(0.01 * len(rows), abs(observed) * 1e-8) if field == "turnover" else 0
        if total is None or abs(total - observed) > tolerance:
            raise FuturesPayloadError(f"DCE {trade_date}: {field} total mismatch")

    return rows


def _bar(raw: dict, code: str, trade_date: date) -> dict:
    def count(field: str) -> int | None:
        return single_sided_count(
            parse_number(raw.get(field)),
            exchange=EXCHANGE,
            trade_date=trade_date,
            field=field,
        )

    volume = count("volumn")
    traded = bool(volume)
    amount = single_sided_amount(
        parse_number(raw.get("turnover")), exchange=EXCHANGE, trade_date=trade_date
    )
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
        "amount": None if amount is None else round(amount, 2),
        "open_interest": count("openInterest"),
        "oi_change": count("diffI"),
    }


def parse_futures(body: bytes, trade_date: date) -> pl.DataFrame:
    rows = []
    for raw in _rows(body, trade_date):
        code = str(raw.get("contractId") or "").strip()
        contract = parse_future_code(code, EXCHANGE, trade_date)
        rows.append(
            {"symbol": contract.symbol, "product": contract.product, **_bar(raw, code, trade_date)}
        )
    return pl.DataFrame(drop_placeholders(rows), infer_schema_length=None)


def parse_options(body: bytes, trade_date: date) -> pl.DataFrame:
    rows = []
    for raw in _rows(body, trade_date):
        code = str(raw.get("contractId") or "").strip()
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
                "exercise_volume": None
                if exercised is None
                else single_sided_count(
                    exercised, exchange=EXCHANGE, trade_date=trade_date, field="exercise"
                ),
                "delta": parse_number(raw.get("delta")),
                "implied_vol": percent_to_fraction(parse_number(raw.get("impliedVolatility"))),
                "series_implied_vol": None,
            }
        )
    return pl.DataFrame(drop_placeholders(rows), infer_schema_length=None)


def _post(trade_date: date, trade_type: str, variety: str, *, config=None) -> bytes:
    return fetch_bytes(
        QUOTES_URL,
        config=config,
        as_of=trade_date,
        method="POST",
        json_body={
            "contractId": "",
            "lang": "zh",
            "optionSeries": "",
            "statisticsType": "0",
            "tradeDate": trade_date.strftime("%Y%m%d"),
            "tradeType": trade_type,
            "varietyId": variety,
        },
    )


def fetch_dce_official_day(
    trade_date: date, *, config=None, kind: str | None = None
) -> ExchangeDay:
    futures = pl.DataFrame()
    if kind != "options":
        futures = parse_futures(_post(trade_date, "1", "all", config=config), trade_date)
    frames = []
    for product in OPTION_PRODUCTS if kind != "futures" else ():
        try:
            frames.append(parse_options(_post(trade_date, "2", product, config=config), trade_date))
        except FuturesDayUnavailable:
            continue
    options = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    return ExchangeDay(EXCHANGE, trade_date, futures=futures, options=options)


__all__ = ["fetch_dce_official_day", "parse_futures", "parse_options"]
