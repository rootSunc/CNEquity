"""大宗交易, read from the exchanges that publish it.

Block trades are not a vendor's measurement: each exchange publishes its own
per-transaction record, so reading them directly removes a hop. This exists as
the availability route for `block_trades`, whose only other source is EastMoney
— when that is down the alternative was nothing at all.

**It is not equivalent cover.** Neither exchange publishes Beijing, and
EastMoney does: measured 2026-09-15, the lake held SH 15 / SZ 17 / BJ 4 and the
two exchanges together carried every one of the 32 SH/SZ names and none of the
four BJ ones. `DatasetSpec.backup_gaps` states that so nothing reads a degraded
day as a whole one.

**SZSE ignores ``PAGESIZE`` and paginates at 20.** Asking for 200 returns the
first 20 with ``pagecount: 2`` in the metadata, which is how a first pass at
this silently lost half of a session — 15 ChiNext names that all sat on page 2.
Pages are walked by ``PAGENO`` until the reported count is reached.
"""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.domain.rate_limit import source_request
from cnequity.domain.symbols import format_symbol, is_all_a_symbol

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 30.0
_SOURCE = "exchange"
_MAX_PAGES = 40

_SZSE_URL = (
    "https://www.szse.cn/api/report/ShowReport/data"
    "?SHOWTYPE=JSON&CATALOGID=1265&TABKEY=tab1&txtKsrq={day}&txtZzrq={day}&PAGENO={page}"
)
_SSE_URL = (
    "https://query.sse.com.cn/commonQuery.do"
    "?jsonCallBack=cb&isPagination=true&pageHelp.pageSize=500&pageHelp.pageNo=1"
    "&pageHelp.beginPage=1&pageHelp.cacheSize=1&pageHelp.endPage=1"
    "&sqlId=COMMON_SSE_XXPL_JYXXPL_DZJYXX_L_1&stockId=&startDate={day}&endDate={day}"
)
_SZSE_HEADERS = {"Referer": "https://www.szse.cn/disclosure/deal/block/equity/index.html"}
_SSE_HEADERS = {"Referer": "https://www.sse.com.cn/"}

EMPTY = pl.DataFrame(
    schema={
        "symbol": pl.Utf8,
        "trade_date": pl.Date,
        "price": pl.Float64,
        "volume": pl.Float64,
        "amount": pl.Float64,
    }
)


def _client():
    from curl_cffi import requests as curl_requests

    return curl_requests.Session()


def _number(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).replace(",", "").replace("&nbsp;", "").strip()
    if not text or text in {"-", "--"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _row(code: str, exchange: str, day: date, price, volume, amount) -> dict | None:
    code = str(code).replace("&nbsp;", "").strip().zfill(6)
    if len(code) != 6 or not code.isdigit() or not is_all_a_symbol(code, exchange):
        return None
    return {
        "symbol": format_symbol(code, exchange),
        "trade_date": day,
        "price": _number(price),
        # Left in 万股 / 万元, because that is what the dataset already holds:
        # EastMoney's 2026-09-15 row for 002422.SZ is 5747.68 against the
        # exchange's identical trade. Converting to shares and CNY here —
        # which the unit contract's wording invites — would have made the
        # backup disagree with the primary by exactly 10,000x on every row.
        "volume": _number(volume),
        "amount": _number(amount),
    }


def fetch_szse_block_trades(trade_date: date, *, config=None) -> pl.DataFrame:
    """Per-transaction SZ block trades, walked to the end of the report."""
    rows: list[dict] = []
    session = _client()
    try:
        page = 1
        while page <= _MAX_PAGES:
            with source_request(config, _SOURCE):
                resp = session.get(
                    _SZSE_URL.format(day=trade_date.isoformat(), page=page),
                    headers=_SZSE_HEADERS,
                    impersonate="chrome",
                    timeout=_TIMEOUT_SECONDS,
                )
            resp.raise_for_status()
            payload = resp.json() or []
            if not payload:
                break
            tab = payload[0] or {}
            batch = tab.get("data") or []
            for item in batch:
                row = _row(
                    item.get("zqdh"),
                    "SZ",
                    trade_date,
                    item.get("cjjg"),
                    item.get("cjgsnew"),
                    item.get("cjjenew"),
                )
                if row:
                    rows.append(row)
            meta = tab.get("metadata") or {}
            total = meta.get("recordcount")
            if not batch or (
                isinstance(total, int) and page * (meta.get("pagesize") or 20) >= total
            ):
                break
            page += 1
        else:
            logger.warning(
                "SZSE block trades for %s exceeded %d pages; reporting what was read",
                trade_date,
                _MAX_PAGES,
            )
    except Exception as exc:  # noqa: BLE001 — one exchange being down is a covered case
        logger.warning("SZSE block trades unavailable for %s: %s", trade_date, exc)
        return EMPTY.clone()
    finally:
        session.close()
    return pl.DataFrame(rows, schema=EMPTY.schema) if rows else EMPTY.clone()


def fetch_sse_block_trades(trade_date: date, *, config=None) -> pl.DataFrame:
    """Per-transaction SH block trades. One request covers the session."""
    import json

    session = _client()
    try:
        with source_request(config, _SOURCE):
            resp = session.get(
                _SSE_URL.format(day=trade_date.isoformat()),
                headers=_SSE_HEADERS,
                impersonate="chrome",
                timeout=_TIMEOUT_SECONDS,
            )
        resp.raise_for_status()
        text = resp.text
        payload = json.loads(text[text.find("(") + 1 : text.rfind(")")])
        page = payload.get("pageHelp") or {}
        data = page.get("data") or []
        total = page.get("total")
        if isinstance(total, int) and total > len(data):
            # One request is meant to cover the day; a short page means the
            # server capped it, and a partial session must not look complete.
            logger.warning(
                "SSE block trades returned %d of %d rows for %s; not writing a partial day",
                len(data),
                total,
                trade_date,
            )
            return EMPTY.clone()
    except Exception as exc:  # noqa: BLE001
        logger.warning("SSE block trades unavailable for %s: %s", trade_date, exc)
        return EMPTY.clone()
    finally:
        session.close()

    rows = [
        row
        for item in data
        if (
            row := _row(
                item.get("stockid"),
                "SH",
                trade_date,
                item.get("tradeprice"),
                item.get("tradeqty"),
                item.get("tradeamount"),
            )
        )
    ]
    return pl.DataFrame(rows, schema=EMPTY.schema) if rows else EMPTY.clone()


def fetch_block_trades_exchange(trade_date: date, *, config=None) -> pl.DataFrame:
    """Both exchanges for one session. A silent exchange contributes nothing.

    Never fills one exchange's absence from the other: they publish different
    markets, so a missing SZSE page is missing SZ rows, not a reason to trust
    the SH half as the whole day.
    """
    frames = [
        fetch_szse_block_trades(trade_date, config=config),
        fetch_sse_block_trades(trade_date, config=config),
    ]
    live = [f for f in frames if not f.is_empty()]
    if not live:
        return EMPTY.clone()
    merged = pl.concat(live, how="vertical")
    # One row per security, as the primary writes it: across 176,093 (day,
    # security) pairs the lake has never held two. The exchanges publish each
    # transaction, so 2026-09-15 came to 29 SZ rows for 17 securities and 56 SH
    # rows for 15 — a degraded day would have carried several times the rows of
    # its neighbours, at prices meaning something else, under a primary key that
    # includes `price`.
    #
    # `price` is then the volume-weighted average, which is what the vendor's
    # single row holds: over those 32 securities it agreed to 0.003%, the width
    # of its four decimals. Summing rather than deduplicating matters here —
    # 603382.SH traded twice at one price and size, and dropping the repeat made
    # the total exactly half of EastMoney's.
    return (
        merged.group_by(["symbol", "trade_date"])
        .agg(
            pl.col("volume").sum(),
            pl.col("amount").sum(),
        )
        .with_columns(
            pl.when(pl.col("volume") > 0)
            .then((pl.col("amount") / pl.col("volume")).round(4))
            .otherwise(None)
            .alias("price")
        )
        .select(EMPTY.columns)
    )
