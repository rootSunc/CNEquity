"""融资融券 detail, read from the exchanges that compile it.

Margin balances are not a vendor's measurement — the exchanges aggregate what
member firms report and publish the result themselves, daily, per security. A
redistributor can only copy that file, so reading it directly removes a hop
without losing anything except one field (below).

The official endpoints have different formats:

* **SSE** serves ``queryMargin.do`` with a 2,000-row page cap. Read every
  page and verify the published total before accepting the day.
* **SZSE** serves the same detail as an xlsx export of report ``1837_xxpl``
  tab 2, in raw 元 and 股 — the JSON form of the identical report paginates at
  20 rows and states 亿/万 units, so the export is both cheaper and less lossy.

**SSE does not publish 融券余额.** Its ``rqylje`` field is null for every row
(all 1,999 checked), and the published column set stops at 融券余量 in shares.
So ``short_balance`` is null on SH rows here where EastMoney carries a number.
That gap is deliberate: the value can be reconstructed as 融券余量 × close, but
stamping a locally computed figure with ``source="exchange"`` would attribute
arithmetic of ours to the exchange. Callers that need the field can keep the
EastMoney path, which the step retains as a whole-day fallback.

Publication lags differ and neither is instant: on 2026-08-30, SSE had already
published 2026-08-28 while SZSE's export for the same session was still
header-only. A missing exchange is reported through ``covered``, never filled
in from the other one.
"""

from __future__ import annotations

import io
import logging
import warnings
from dataclasses import dataclass
from datetime import date

import polars as pl

from cnequity.domain.http_policy import record_http_response
from cnequity.domain.rate_limit import source_request
from cnequity.domain.symbols import format_symbol, is_all_a_symbol, is_etf_symbol
from cnequity.orchestrator.outcomes import SourcePayloadError, SourceUnavailableError
from cnequity.orchestrator.source_gaps import record_source_gap

logger = logging.getLogger(__name__)

_TIMEOUT_SECONDS = 60.0
# Must match the `[sources.exchange]` section that gates this adapter.
_SOURCE = "exchange"

_EMPTY_MARGIN = pl.DataFrame(
    schema={
        "symbol": pl.Utf8,
        "trade_date": pl.Date,
        "margin_balance": pl.Float64,
        "margin_buy": pl.Float64,
        "short_balance": pl.Float64,
        "short_sell_volume": pl.Float64,
    }
)

# The server caps pages at 2,000 even when a larger size is requested.
SSE_PAGE_SIZE = 2000
SSE_URL = (
    "http://query.sse.com.cn/marketdata/tradedata/queryMargin.do"
    "?isPagination=true&tabType=mxtype&detailsDate={day}"
    "&pageHelp.pageSize={page_size}&pageHelp.pageNo={page_no}"
    "&pageHelp.beginPage={page_no}&pageHelp.cacheSize=1&pageHelp.endPage={page_no}"
)
_SSE_HEADERS = {"Referer": "https://www.sse.com.cn/"}

SZSE_URL = (
    "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx"
    "&CATALOGID=1837_xxpl&TABKEY=tab2&txtDate={day}&random=0.1"
)
_SZSE_HEADERS = {"Referer": "https://www.szse.cn/"}
_SZSE_COLUMNS = {
    "证券代码": "code",
    "融资余额(元)": "margin_balance",
    "融资买入额(元)": "margin_buy",
    "融券余额(元)": "short_balance",
    "融券卖出量(股/份)": "short_sell_volume",
}


@dataclass(frozen=True)
class ExchangeMarginResult:
    """Margin detail plus which exchanges actually published for the session.

    The two publish on different lags, so a day covered by one of them is
    routine and must stay distinguishable from a day covered by both — writing
    a half-market snapshot as if it were the whole market would make every
    downstream completeness check wrong.
    """

    rows: pl.DataFrame
    covered: frozenset[str]
    failures: dict[str, str]

    @property
    def missing(self) -> frozenset[str]:
        """Exchanges this fetch asks that did not publish."""
        return frozenset(EXCHANGES) - self.covered

    @property
    def is_empty(self) -> bool:
        return self.rows.is_empty()


def _client():
    from curl_cffi import requests as cr

    return cr


def _keep_symbol(code: str, exchange: str) -> bool:
    """Marginable securities include ETFs, which the dataset already carries."""
    return is_all_a_symbol(code, exchange) or is_etf_symbol(code, exchange)


def _number(value) -> float | None:
    """Parse a published figure, tolerating thousands separators and blanks."""
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text or text in {"-", "--", "nan", "None"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _finish(rows: list[dict]) -> pl.DataFrame:
    if not rows:
        return _EMPTY_MARGIN.clone()
    return (
        pl.DataFrame(rows, schema_overrides=dict(_EMPTY_MARGIN.schema))
        .unique(subset=["symbol", "trade_date"], keep="last")
        .sort("symbol")
    )


def fetch_sse_margin_trading(trade_date: date, *, config=None) -> pl.DataFrame:
    """Official SH 融资融券 detail. ``short_balance`` is null — SSE omits it."""
    data: list[dict] = []
    seen: set[str] = set()
    total: int | None = None
    page_size = SSE_PAGE_SIZE
    pagination_error: str | None = None
    try:
        for page_no in range(1, 101):
            url = SSE_URL.format(
                day=trade_date.strftime("%Y%m%d"), page_size=page_size, page_no=page_no
            )
            with source_request(config, _SOURCE):
                resp = _client().get(
                    url, headers=_SSE_HEADERS, impersonate="chrome", timeout=_TIMEOUT_SECONDS
                )
                record_http_response(config, _SOURCE, resp)
            resp.raise_for_status()
            page = (resp.json() or {}).get("pageHelp") or {}
            batch = page.get("data") or []
            reported = int(page["total"]) if page.get("total") is not None else None
            if page_no == 1:
                total = reported
                page_size = int(page.get("pageSize") or page_size)
                if page_size < 1:
                    raise ValueError("invalid page size")
            elif reported != total:
                raise ValueError("published total changed during pagination")
            codes = [str(item.get("stockCode") or "").strip() for item in batch]
            if len(set(codes)) != len(codes) or seen.intersection(codes):
                raise ValueError("duplicate securities / repeated margin page")
            if total is not None and len(data) + len(batch) > total:
                raise ValueError("margin page exceeds the published total")
            seen.update(codes)
            data.extend(batch)
            if total is None or len(data) == total:
                break
            if not batch or len(data) > total:
                raise ValueError(f"incomplete margin response: {len(data)} of {total}")
        else:
            raise ValueError("margin pagination exceeded 100 pages")
    except Exception as exc:
        logger.warning("SSE margin detail unavailable for %s: %s", trade_date, exc)
        pagination_error = str(exc)

    rows: list[dict] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        code = str(item.get("stockCode") or "").strip().zfill(6)
        if len(code) != 6 or not code.isdigit() or not _keep_symbol(code, "SH"):
            continue
        rows.append(
            {
                "symbol": format_symbol(code, "SH"),
                "trade_date": trade_date,
                "margin_balance": _number(item.get("rzye")),
                "margin_buy": _number(item.get("rzmre")),
                # `rqylje` is the 融券余额 slot and SSE leaves it null; parsed
                # rather than hardcoded so the field appears if it ever fills.
                "short_balance": _number(item.get("rqylje")),
                "short_sell_volume": _number(item.get("rqmcl")),
            }
        )
    if not rows and data:
        logger.warning("SSE margin detail returned no usable rows; format may have changed")
    frame = _finish(rows)
    if pagination_error and not frame.is_empty():
        try:
            record_source_gap(
                "margin_trading",
                f"SSE margin pagination incomplete for {trade_date}: {pagination_error}; "
                "validated preceding pages retained",
                frame=frame,
                dates=[trade_date],
            )
        except SourceUnavailableError:
            # A strict caller has no command collector to distinguish these
            # independently usable rows from a complete publisher snapshot.
            return _EMPTY_MARGIN.clone()
    return frame


def fetch_szse_margin_trading(trade_date: date, *, config=None) -> pl.DataFrame:
    """Official SZ 融资融券 detail, in the raw 元 / 股 the export states."""
    try:
        import pandas as pd

        with source_request(config, _SOURCE):
            resp = _client().get(
                SZSE_URL.format(day=trade_date.isoformat()),
                headers=_SZSE_HEADERS,
                impersonate="chrome",
                timeout=_TIMEOUT_SECONDS,
            )
            record_http_response(config, _SOURCE, resp)
        resp.raise_for_status()
        if not resp.content:
            logger.info("SZSE published no margin detail for %s", trade_date)
            return _EMPTY_MARGIN.clone()
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Workbook contains no default style")
            pdf = pd.read_excel(io.BytesIO(resp.content), dtype=str)
    except Exception as exc:
        logger.warning("SZSE margin detail unavailable for %s: %s", trade_date, exc)
        return _EMPTY_MARGIN.clone()

    missing = [column for column in _SZSE_COLUMNS if column not in pdf.columns]
    if missing:
        logger.warning("SZSE margin detail is missing %s", missing)
        return _EMPTY_MARGIN.clone()
    if pdf.empty:
        # A header-only export is how SZSE represents "not published yet";
        # observed for 2026-08-28 while SSE had already served that session.
        logger.info("SZSE margin detail for %s is header-only; not published yet", trade_date)
        return _EMPTY_MARGIN.clone()

    rows: list[dict] = []
    for record in pdf[list(_SZSE_COLUMNS)].to_dict("records"):
        code = str(record["证券代码"]).strip().zfill(6)
        if len(code) != 6 or not code.isdigit() or not _keep_symbol(code, "SZ"):
            continue
        rows.append(
            {
                "symbol": format_symbol(code, "SZ"),
                "trade_date": trade_date,
                **{
                    field: _number(record[column])
                    for column, field in _SZSE_COLUMNS.items()
                    if field != "code"
                },
            }
        )
    if not rows:
        logger.warning("SZSE margin detail returned no usable rows for %s", trade_date)
    return _finish(rows)


# What a complete result covers; callers compare against this, never a literal.
EXCHANGES = ("sse", "szse")


def fetch_exchange_margin_trading(trade_date: date, *, config=None) -> ExchangeMarginResult:
    """Both exchanges, without hiding which of them published."""
    frames: list[pl.DataFrame] = []
    covered: set[str] = set()
    failures: dict[str, str] = {}
    # Looked up per call so tests can patch one exchange's fetcher.
    fetchers = {"sse": fetch_sse_margin_trading, "szse": fetch_szse_margin_trading}
    for exchange in EXCHANGES:
        fetch = fetchers[exchange]
        try:
            fetched = fetch(trade_date, config=config)
        except (SourceUnavailableError, SourcePayloadError) as exc:
            # Provider adapters handle their wire failures. Local storage or
            # computation failures must still reach the command as failures.
            failures[exchange] = str(exc)
            logger.warning("%s margin detail unavailable for %s: %s", exchange, trade_date, exc)
            continue
        if fetched.is_empty():
            failures[exchange] = "no usable rows"
            continue
        frames.append(fetched)
        covered.add(exchange)
    rows = pl.concat(frames, how="vertical_relaxed") if frames else _EMPTY_MARGIN.clone()
    return ExchangeMarginResult(rows=rows, covered=frozenset(covered), failures=failures)
