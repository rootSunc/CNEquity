"""龙虎榜, read from the exchanges that publish it.

The availability route for `dragon_tiger`, whose only other source is
EastMoney. Each exchange publishes a daily list of the securities that tripped
a disclosure rule, and a per-security detail of the five largest buying and
selling desks — which is where `buy_amount` / `sell_amount` come from, because
the list itself carries only turnover.

**Not equivalent cover.** Neither exchange publishes Beijing, and the SSE's
series starts 2017-01-01 (its own page says so). `DatasetSpec.backup_gaps`
records both.

**`reason` is written as the exchange words it**, which is not how EastMoney
words it: for 000428.SZ on 2026-09-15 the vendor said "日跌幅偏离值达到7%的前5只
证券" (the rule) and SZSE said "日价格跌幅偏离值达到-9.18%" (the measurement).
`reason` is part of the primary key, so a degraded day carries keys that differ
in wording from its neighbours. That is the honest trade: inventing the
vendor's phrasing would stamp our own inference with `source="exchange"`.

**The detail costs one request per (security, rule).** Measured 2026-09-15:
about 60 across both exchanges. Only ever paid when EastMoney has already
failed, which is why the step asks for it rather than shadowing every session.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date

import polars as pl

from cnequity.domain.rate_limit import source_request
from cnequity.domain.symbols import format_symbol, is_all_a_symbol

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 30.0
_SOURCE = "exchange"

_SZSE_LIST = (
    "https://www.szse.cn/api/report/ShowReport/data"
    "?SHOWTYPE=JSON&CATALOGID=1842_xxpl_after&txtStart={day}&txtEnd={day}"
)
_SZSE_DETAIL = (
    "https://www.szse.cn/api/report/ShowReport/data"
    "?SHOWTYPE=JSON&CATALOGID=1842_detal&TABKEY=tab1,tab2&DQRQ={day}&ZQDM={code}&ZBDM={zbdm}"
)
_SSE_LIST = (
    "https://query.sse.com.cn/commonSoaQuery.do"
    "?jsonCallBack=cb&isPagination=true&token=QUERY&sqlId=JYGKXX_ZL"
    "&tradeDateStart={day}&tradeDateEnd={day}&secCode=&refType=&bsType=&branchName="
    "&pageHelp.pageSize=500&pageHelp.pageNo=1&pageHelp.beginPage=1"
    "&pageHelp.cacheSize=1&pageHelp.endPage=1"
)
_SSE_DETAIL = (
    "https://query.sse.com.cn/marketdata/tradedata/queryTradeOpenInfo.do"
    "?jsonCallBack=cb&orderB=desc&orderS=desc&Token=QUERY"
    "&tradeDate={day}&refType={ref}&secCode={code}"
)
_SZSE_HEADERS = {"Referer": "https://www.szse.cn/disclosure/deal/public/index.html"}
_SSE_HEADERS = {"Referer": "https://www.sse.com.cn/"}

EMPTY = pl.DataFrame(
    schema={
        "symbol": pl.Utf8,
        "trade_date": pl.Date,
        "reason": pl.Utf8,
        "buy_amount": pl.Float64,
        "sell_amount": pl.Float64,
        "net_amount": pl.Float64,
    }
)


def _client():
    from curl_cffi import requests as curl_requests

    return curl_requests.Session()


def _number(value: object) -> float:
    if value is None:
        return 0.0
    text = str(value).replace(",", "").replace("&nbsp;", "").strip()
    if not text or text in {"-", "--"}:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _clean(text: object) -> str:
    return re.sub(r"<[^>]+>", "", str(text or "")).replace("&nbsp;", "").strip()


def fetch_szse_dragon_tiger(trade_date: date, *, config=None) -> pl.DataFrame:
    """SZ list plus the desk detail behind each listed (security, rule)."""
    session = _client()
    rows: list[dict] = []
    try:
        with source_request(config, _SOURCE):
            resp = session.get(
                _SZSE_LIST.format(day=trade_date.isoformat()),
                headers=_SZSE_HEADERS,
                impersonate="chrome",
                timeout=_TIMEOUT_SECONDS,
            )
        resp.raise_for_status()
        listed = ((resp.json() or [{}])[0] or {}).get("data") or []
    except Exception as exc:  # noqa: BLE001 — one exchange down is a covered case
        logger.warning("SZSE dragon_tiger list unavailable for %s: %s", trade_date, exc)
        session.close()
        return EMPTY.clone()

    try:
        for item in listed:
            code = _clean(item.get("zqdm")).zfill(6)
            if len(code) != 6 or not code.isdigit() or not is_all_a_symbol(code, "SZ"):
                continue
            # The indicator id is only in the row's own detail link. Guessing it
            # returns an empty detail that looks like a quiet security.
            link = re.search(r"ZBDM=(\w+)", str(item.get("bz") or ""))
            if not link:
                logger.warning("SZSE dragon_tiger row for %s carries no detail link", code)
                continue
            try:
                with source_request(config, _SOURCE):
                    detail = session.get(
                        _SZSE_DETAIL.format(
                            day=trade_date.isoformat(), code=code, zbdm=link.group(1)
                        ),
                        headers=_SZSE_HEADERS,
                        impersonate="chrome",
                        timeout=_TIMEOUT_SECONDS,
                    )
                detail.raise_for_status()
                tabs = detail.json() or []
            except Exception as exc:  # noqa: BLE001
                logger.warning("SZSE dragon_tiger detail failed for %s: %s", code, exc)
                continue
            desks = (tabs[1] or {}).get("data") if len(tabs) > 1 else None
            if not desks:
                continue
            buy = sum(_number(d.get("mrje")) for d in desks)
            sell = sum(_number(d.get("mcje")) for d in desks)
            rows.append(
                {
                    "symbol": format_symbol(code, "SZ"),
                    "trade_date": trade_date,
                    "reason": _clean(item.get("plyy")),
                    "buy_amount": buy,
                    "sell_amount": sell,
                    "net_amount": buy - sell,
                }
            )
    finally:
        session.close()
    return pl.DataFrame(rows, schema=EMPTY.schema) if rows else EMPTY.clone()


def _sse_json(text: str) -> dict:
    return json.loads(text[text.find("(") + 1 : text.rfind(")")])


def fetch_sse_dragon_tiger(trade_date: date, *, config=None) -> pl.DataFrame:
    """SH list plus desk detail. The SSE series starts 2017-01-01."""
    session = _client()
    rows: list[dict] = []
    try:
        with source_request(config, _SOURCE):
            resp = session.get(
                _SSE_LIST.format(day=trade_date.isoformat()),
                headers=_SSE_HEADERS,
                impersonate="chrome",
                timeout=_TIMEOUT_SECONDS,
            )
        resp.raise_for_status()
        listed = (_sse_json(resp.text).get("pageHelp") or {}).get("data") or []
    except Exception as exc:  # noqa: BLE001
        logger.warning("SSE dragon_tiger list unavailable for %s: %s", trade_date, exc)
        session.close()
        return EMPTY.clone()

    try:
        for item in listed:
            code = str(item.get("secCode") or "").strip().zfill(6)
            ref = str(item.get("refType") or "").strip()
            if len(code) != 6 or not code.isdigit() or not is_all_a_symbol(code, "SH") or not ref:
                continue
            try:
                with source_request(config, _SOURCE):
                    detail = session.get(
                        _SSE_DETAIL.format(day=trade_date.strftime("%Y%m%d"), ref=ref, code=code),
                        headers=_SSE_HEADERS,
                        impersonate="chrome",
                        timeout=_TIMEOUT_SECONDS,
                    )
                detail.raise_for_status()
                desks = (_sse_json(detail.text).get("pageHelp") or {}).get("data") or []
            except Exception as exc:  # noqa: BLE001
                logger.warning("SSE dragon_tiger detail failed for %s: %s", code, exc)
                continue
            if not desks:
                continue
            buy = sum(_number(d.get("branchTxAmt")) for d in desks if d.get("bsType") == "B")
            sell = sum(_number(d.get("branchTxAmt")) for d in desks if d.get("bsType") == "S")
            rows.append(
                {
                    "symbol": format_symbol(code, "SH"),
                    "trade_date": trade_date,
                    # SSE gives a numeric rule code; the securities' own list
                    # carries no prose, so the code is what it actually said.
                    "reason": f"SSE refType={ref}",
                    "buy_amount": buy,
                    "sell_amount": sell,
                    "net_amount": buy - sell,
                }
            )
    finally:
        session.close()
    return pl.DataFrame(rows, schema=EMPTY.schema) if rows else EMPTY.clone()


def fetch_dragon_tiger_exchange(trade_date: date, *, config=None) -> pl.DataFrame:
    """Both exchanges for one session; a silent one contributes nothing."""
    frames = [
        fetch_szse_dragon_tiger(trade_date, config=config),
        fetch_sse_dragon_tiger(trade_date, config=config),
    ]
    live = [f for f in frames if not f.is_empty()]
    if not live:
        return EMPTY.clone()
    return pl.concat(live, how="vertical").unique(
        subset=["symbol", "trade_date", "reason"], keep="first"
    )
