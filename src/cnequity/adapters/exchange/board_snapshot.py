"""Short-lived, session-checked sharing for official exchange board responses."""

from __future__ import annotations

import io
import json
import logging
import time
import warnings
from datetime import date, datetime, timezone

from cnequity.domain.http_policy import record_cache_reuse, record_http_response
from cnequity.domain.rate_limit import source_request
from cnequity.file_lock import exclusive_lock
from cnequity.storage.atomic import write_json_atomic

logger = logging.getLogger(__name__)

_TTL_SECONDS = 300
# Seconds before each retry of a board request. A transport failure is retried;
# a ValueError is the board's own answer (wrong session, mid-session, missing
# columns) and is not. One reset from SZSE on 2026-10-27 cost every Shenzhen
# stock its status, and the same request succeeded five seconds later.
_RETRY_DELAYS = (2.0, 5.0)
_sleep = time.sleep
_SOURCE = "exchange"
_TIMEOUT_SECONDS = 60.0
_SSE_HEADERS = {"Referer": "https://www.sse.com.cn/"}
_SZSE_HEADERS = {"Referer": "https://www.szse.cn/"}
# The field order of each row follows `select`; the quote and status parsers
# read rows by position from this tuple.
SSE_SELECT = ("code", "name", "open", "high", "low", "last", "volume", "amount", "prev_close")
_SSE_BOARD_URL = (
    "http://yunhq.sse.com.cn:32041/v1/sh1/list/exchange/{board}"
    f"?select={','.join(SSE_SELECT)}&begin=0&end=6000"
)
SSE_URL = _SSE_BOARD_URL.format(board="equity")
# ETFs and LOFs sit on their own board; the equity board carries none of them.
SSE_FUND_URL = _SSE_BOARD_URL.format(board="fund")
_SZSE_URL = (
    "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx"
    "&CATALOGID={catalog}&TABKEY=tab1"
    "&txtBeginDate={day}&txtEndDate={day}&random=0.1"
)
_SZSE_STOCK_CATALOG = "1815_stock_snapshot"
_SZSE_FUND_CATALOG = "1815_stock_lf_snapshot"
_SZSE_FUND_HEADERS = {"Referer": "https://fund.szse.cn/marketdata/trade/index.html"}


def _cached(config, exchange: str, day: date, fetch):
    if config is None:
        return fetch()
    path = config.meta_root / "source_cache" / "exchange" / f"{exchange}-{day}.json"
    with exclusive_lock(path.with_suffix(".lock")):
        if path.exists():
            try:
                item = json.loads(path.read_text(encoding="utf-8"))
                age = datetime.now(timezone.utc) - datetime.fromisoformat(item["captured_at"])
                if 0 <= age.total_seconds() < _TTL_SECONDS:
                    record_cache_reuse(config, _SOURCE, f"{exchange}_board")
                    return item["payload"]
            except (OSError, ValueError, KeyError, TypeError):
                pass
        payload = _with_retry(fetch, exchange)
        write_json_atomic(
            path,
            {"captured_at": datetime.now(timezone.utc).isoformat(), "payload": payload},
        )
        return payload


def _with_retry(fetch, exchange: str):
    for delay in (*_RETRY_DELAYS, None):
        try:
            return fetch()
        except (ValueError, AssertionError):
            raise
        except Exception as exc:  # noqa: BLE001 — transport errors are retried
            if delay is None:
                raise
            logger.info("%s board request failed (%s); retrying in %.0fs", exchange, exc, delay)
            _sleep(delay)
    raise AssertionError("unreachable")


def sse_snapshot(
    trade_date: date, *, config, client_factory, url: str = SSE_URL, cache_key: str = "sse"
) -> dict:
    def fetch():
        with source_request(config, _SOURCE):
            response = client_factory().get(
                url,
                headers=_SSE_HEADERS,
                impersonate="chrome",
                timeout=_TIMEOUT_SECONDS,
            )
            record_http_response(config, _SOURCE, response)
        response.raise_for_status()
        payload = response.json()
        raw_date = str(payload.get("date") or "")
        try:
            observed = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:8]))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"SSE snapshot carried an unreadable date {raw_date!r}") from exc
        if observed != trade_date:
            raise ValueError(f"SSE snapshot serves {observed}, not {trade_date}")
        if not isinstance(payload.get("time"), int) or payload["time"] < 150000:
            raise ValueError(f"SSE snapshot is mid-session (time={payload.get('time')})")
        if not isinstance(payload.get("list"), list) or not payload["list"]:
            raise ValueError("SSE snapshot has no board rows")
        return payload

    return _cached(config, cache_key, trade_date, fetch)


def sse_fund_snapshot(trade_date: date, *, config, client_factory) -> dict:
    """The Shanghai ETF/LOF board, under the same session checks as the equity one."""
    return sse_snapshot(
        trade_date,
        config=config,
        client_factory=client_factory,
        url=SSE_FUND_URL,
        cache_key="sse_fund",
    )


_SZSE_STOCK_COLUMNS = frozenset(
    {
        "交易日期",
        "证券代码",
        "证券简称",
        "开盘",
        "最高",
        "最低",
        "今收",
        "成交量(万股)",
        "成交金额(万元)",
    }
)
# The fund report has no open/high/low: it can show a fund traded, not that it halted.
_SZSE_FUND_COLUMNS = frozenset({"交易日期", "证券代码", "证券简称", "前收", "今收", "成交金额(元)"})


def szse_report(trade_date: date, *, config, client_factory):
    return _szse_report(
        trade_date,
        config=config,
        client_factory=client_factory,
        catalog=_SZSE_STOCK_CATALOG,
        headers=_SZSE_HEADERS,
        required=_SZSE_STOCK_COLUMNS,
        cache_key="szse",
    )


def szse_fund_report(trade_date: date, *, config, client_factory):
    return _szse_report(
        trade_date,
        config=config,
        client_factory=client_factory,
        catalog=_SZSE_FUND_CATALOG,
        headers=_SZSE_FUND_HEADERS,
        required=_SZSE_FUND_COLUMNS,
        cache_key="szse_fund",
    )


def _szse_report(
    trade_date: date, *, config, client_factory, catalog, headers, required, cache_key
):
    import pandas as pd

    def fetch():
        with source_request(config, _SOURCE):
            response = client_factory().get(
                _SZSE_URL.format(catalog=catalog, day=trade_date.isoformat()),
                headers=headers,
                impersonate="chrome",
                timeout=_TIMEOUT_SECONDS,
            )
            record_http_response(config, _SOURCE, response)
        response.raise_for_status()
        if not response.content:
            raise ValueError(f"SZSE published no report for {trade_date}")
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Workbook contains no default style")
            frame = pd.read_excel(io.BytesIO(response.content), dtype=str)
        missing = required - set(frame.columns)
        if missing:
            raise ValueError(f"SZSE report is missing {sorted(missing)}")
        dates = pd.to_datetime(frame["交易日期"], errors="coerce").dt.date
        if frame.empty or dates.isna().any() or not dates.eq(trade_date).all():
            raise ValueError(f"SZSE report contains rows outside {trade_date}")
        return frame.where(pd.notna(frame), None).to_dict("records")

    return pd.DataFrame(_cached(config, cache_key, trade_date, fetch))
