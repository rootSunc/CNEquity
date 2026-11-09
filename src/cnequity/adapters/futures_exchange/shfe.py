"""SHFE (and INE) daily quotes and contract parameters, read from the exchange.

SHFE publishes one JSON file per session for futures (back to at least
2002-01-07) and one for options (from copper options' first session,
2018-09-21). INE's 2018 futures use its own official daily file; the SHFE
daily file does not contain the first INE session. From 2019, SHFE's file
contains INE products too. Rows are assigned to ``INE`` by product.

Each product block ends in a 「小计」 row, and the file in a 「总计」 row. Measured
across 2002, 2010, 2019, 2025 and 2026 files, futures and options alike, the
subtotals equal the contracts above them exactly, on volume, open interest and
turnover. A file that does not add up is refused. TAS rows (``sc_tas``) and
exchange-for-physical rows (``alefp``) are not contracts and are skipped.

Two things changed over the years and are handled here rather than downstream:

- **Field layout.** Early files pad codes with spaces, carry
  ``PRODUCTSORTNO``/``ORDERNO``, and have no ``TURNOVER`` (so ``amount`` is
  null for those years); later ones add ``TURNOVER`` and ``PRODUCTGROUPID``.
- **Counting basis.** Until 2019-12-31 volume, open interest and turnover were
  double-sided, options included (every 2018 option count is even). They are
  halved at this boundary.

Options carry a per-contract ``DELTA`` and an exercise count, and the file's
``o_cursigma`` section publishes one implied volatility per expiry series,
already a fraction — stored as ``series_implied_vol``.

``ContractBaseInfo`` (futures from 2015, options since listing) names each live
contract's listing day and last trading day.
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
    parse_future_code,
    parse_option_code,
    single_sided_amount,
    single_sided_count,
)

logger = logging.getLogger(__name__)

FIRST_SESSION = date(2002, 1, 7)
FIRST_OPTION_SESSION = date(2018, 9, 21)
INE_FIRST_SESSION = date(2018, 3, 26)
INE_DIRECT_LAST_SESSION = date(2018, 12, 31)

FUTURES_URL = "https://www.shfe.com.cn/data/tradedata/future/dailydata/kx{ymd}.dat"
INE_FUTURES_URL = "https://www.ine.cn/data/tradedata/future/dailydata/kx{ymd}.dat"
OPTIONS_URL = "https://www.shfe.com.cn/data/tradedata/option/dailydata/kx{ymd}.dat"
FUTURES_REFERENCE_URL = (
    "https://www.shfe.com.cn/data/busiparamdata/future/ContractBaseInfo{ymd}.dat"
)
OPTIONS_REFERENCE_URL = (
    "https://www.shfe.com.cn/data/busiparamdata/option/ContractBaseInfo{ymd}.dat"
)

#: INE products; 2018 futures come from INE's own daily file.
INE_PRODUCTS = frozenset({"sc", "lu", "nr", "bc", "ec"})


def _exchange(root: str) -> str:
    return "INE" if root.lower() in INE_PRODUCTS else "SHF"


def _load(body: bytes, trade_date: date) -> dict:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FuturesDayUnavailable(f"SHFE {trade_date.isoformat()}: not a daily file") from exc
    if not isinstance(payload, dict) or not payload.get("o_curinstrument"):
        raise FuturesDayUnavailable(f"SHFE {trade_date.isoformat()}: file lists no contracts")
    return payload


def _root(product_id) -> str:
    return str(product_id or "").strip().rsplit("_", 1)[0].strip()


def _count(value, *, exchange: str, trade_date: date, field: str) -> int | None:
    return single_sided_count(
        parse_number(value), exchange=exchange, trade_date=trade_date, field=field
    )


def _check(block: list[dict], subtotal: dict, label: str) -> None:
    for field in ("VOLUME", "OPENINTEREST", "TURNOVER"):
        want = parse_number(subtotal.get(field))
        if want is None:
            continue
        got = sum(parse_number(r.get(field)) or 0.0 for r in block)
        if abs(got - want) > 0.01:
            raise FuturesPayloadError(
                f"SHFE {label}: contracts sum to {field}={got} but 小计 says {want}"
            )


def _bar(raw: dict, *, exchange: str, trade_date: date) -> dict:
    volume = _count(raw.get("VOLUME"), exchange=exchange, trade_date=trade_date, field="volume")
    traded = bool(volume)
    amount = single_sided_amount(
        parse_number(raw.get("TURNOVER")), exchange=exchange, trade_date=trade_date
    )
    return {
        "exchange": exchange,
        "trade_date": trade_date,
        "open": parse_price(raw.get("OPENPRICE")) if traded else None,
        "high": parse_price(raw.get("HIGHESTPRICE")) if traded else None,
        "low": parse_price(raw.get("LOWESTPRICE")) if traded else None,
        "close": parse_price(raw.get("CLOSEPRICE")) if traded else None,
        "settle": parse_price(raw.get("SETTLEMENTPRICE")),
        "pre_settle": parse_price(raw.get("PRESETTLEMENTPRICE")),
        "volume": volume,
        "amount": None if amount is None else round(amount, 2),
        "open_interest": _count(
            raw.get("OPENINTEREST"), exchange=exchange, trade_date=trade_date, field="oi"
        ),
        "oi_change": _count(
            raw.get("OPENINTERESTCHG"), exchange=exchange, trade_date=trade_date, field="oi_change"
        ),
    }


def _blocks(rows: list[dict], key: str, trade_date: date):
    """Contract rows grouped by product, each block checked against its 小计."""
    block: list[dict] = []
    for raw in rows:
        product_id = str(raw.get("PRODUCTID") or "").strip()
        tag = str(raw.get(key) or "").strip()
        if product_id == "总计" or product_id.endswith("_tas") or "efp" in product_id:
            continue
        if tag == "小计":
            _check(block, raw, f"{trade_date.isoformat()} {product_id}")
            yield from block
            block = []
            continue
        if not tag:
            continue
        block.append(raw)
    if block:
        raise FuturesPayloadError(f"SHFE {trade_date.isoformat()}: last block has no 小计")


def parse_futures(body: bytes, trade_date: date) -> pl.DataFrame:
    payload = _load(body, trade_date)
    rows: list[dict] = []
    for raw in _blocks(payload["o_curinstrument"], "DELIVERYMONTH", trade_date):
        root = _root(raw.get("PRODUCTID"))
        exchange = _exchange(root)
        code = f"{root}{str(raw['DELIVERYMONTH']).strip()}"
        contract = parse_future_code(code, exchange, trade_date)
        rows.append(
            {
                "symbol": contract.symbol,
                "exchange_code": code,
                "product": contract.product,
                **_bar(raw, exchange=exchange, trade_date=trade_date),
            }
        )
    return (
        pl.DataFrame(drop_placeholders(rows), infer_schema_length=None) if rows else pl.DataFrame()
    )


def parse_options(body: bytes, trade_date: date) -> pl.DataFrame:
    payload = _load(body, trade_date)
    sigma: dict[str, float] = {}
    for raw in payload.get("o_cursigma") or []:
        series = str(raw.get("INSTRUMENTID") or "").strip()
        value = parse_number(raw.get("SIGMA"))
        if series and series != "小计" and value is not None:
            sigma[series.lower()] = value
    rows: list[dict] = []
    for raw in _blocks(payload["o_curinstrument"], "INSTRUMENTID", trade_date):
        root = _root(raw.get("PRODUCTID"))
        exchange = _exchange(root)
        code = str(raw["INSTRUMENTID"]).strip()
        contract = parse_option_code(code, exchange, trade_date)
        series = str(raw.get("UNDERLYINGINSTRID") or "").strip().lower()
        exercised = parse_number(raw.get("EXECVOLUME"))
        rows.append(
            {
                "symbol": contract.symbol,
                "exchange_code": code,
                "product": contract.product,
                "underlying_symbol": contract.underlying_symbol,
                "option_type": contract.option_type,
                "strike": contract.strike,
                **_bar(raw, exchange=exchange, trade_date=trade_date),
                "settle": parse_option_settle(raw.get("SETTLEMENTPRICE")),
                "exercise_volume": None
                if exercised is None
                else single_sided_count(
                    exercised, exchange=exchange, trade_date=trade_date, field="exercise"
                ),
                "delta": parse_number(raw.get("DELTA")),
                "implied_vol": None,
                "series_implied_vol": sigma.get(series),
            }
        )
    return (
        pl.DataFrame(drop_placeholders(rows), infer_schema_length=None) if rows else pl.DataFrame()
    )


def fetch_shfe_day(trade_date: date, *, config=None, kind: str | None = None) -> ExchangeDay:
    """SHFE and INE contracts for *trade_date*; options only once they listed."""
    if trade_date < FIRST_SESSION:
        raise FuturesDayUnavailable(f"SHFE files start {FIRST_SESSION.isoformat()}")
    ymd = trade_date.strftime("%Y%m%d")
    futures = pl.DataFrame()
    if kind != "options":
        futures = parse_futures(
            fetch_bytes(FUTURES_URL.format(ymd=ymd), config=config, as_of=trade_date), trade_date
        )
        if INE_FIRST_SESSION <= trade_date <= INE_DIRECT_LAST_SESSION:
            ine = parse_futures(
                fetch_bytes(INE_FUTURES_URL.format(ymd=ymd), config=config, as_of=trade_date),
                trade_date,
            )
            if ine.is_empty() or set(ine["exchange"]) != {"INE"}:
                raise FuturesPayloadError(
                    f"INE {trade_date}: direct daily file has no INE contracts"
                )
            futures = pl.concat(
                [futures.filter(pl.col("exchange") == "SHF"), ine], how="vertical_relaxed"
            )
    options = pl.DataFrame()
    if kind != "futures" and trade_date >= FIRST_OPTION_SESSION:
        options = parse_options(
            fetch_bytes(OPTIONS_URL.format(ymd=ymd), config=config, as_of=trade_date), trade_date
        )
    return ExchangeDay("SHF", trade_date, futures=futures, options=options)


def _day(value) -> date | None:
    text = str(value or "").strip()
    return datetime.strptime(text, "%Y%m%d").date() if len(text) == 8 and text.isdigit() else None


def parse_reference(body: bytes, trade_date: date, *, kind: str) -> pl.DataFrame:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FuturesDayUnavailable(f"SHFE reference {trade_date}: not a JSON file") from exc
    key = "OptionContractBaseInfo" if kind == "option" else "ContractBaseInfo"
    rows: list[dict] = []
    for raw in payload.get(key) or []:
        code = str(raw.get("INSTRUMENTID") or "").strip()
        root = str(raw.get("COMMODITYID") or "").strip() or code.rstrip("0123456789CP")
        exchange = _exchange(root)
        parse = parse_option_code if kind == "option" else parse_future_code
        try:
            contract = parse(code, exchange, trade_date)
        except ValueError:
            logger.warning("SHFE reference: unrecognised contract code %r", code)
            continue
        rows.append(
            {
                "symbol": contract.symbol,
                "kind": kind,
                "exchange_code": code,
                "list_date": _day(raw.get("OPENDATE")),
                "last_trade_date": _day(raw.get("EXPIREDATE")),
                "as_of": trade_date,
            }
        )
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fetch_shfe_reference(trade_date: date, *, config=None, kind: str | None = None) -> pl.DataFrame:
    ymd = trade_date.strftime("%Y%m%d")
    frames = []
    if kind != "option":
        frames.append(
            parse_reference(
                fetch_bytes(FUTURES_REFERENCE_URL.format(ymd=ymd), config=config),
                trade_date,
                kind="future",
            )
        )
    if kind != "future" and trade_date >= FIRST_OPTION_SESSION:
        frames.append(
            parse_reference(
                fetch_bytes(OPTIONS_REFERENCE_URL.format(ymd=ymd), config=config),
                trade_date,
                kind="option",
            )
        )
    frames = [f for f in frames if not f.is_empty()]
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
