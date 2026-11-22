"""同花顺 (data.10jqka.com.cn) money flow: per stock, per industry, per concept.

A degraded stand-in for EastMoney's push2 money flow, fetched only when push2
cannot deliver. It is **not** the same measure, so it lands in its own
datasets (``fund_flow_ths``, ``sector_fund_flow_ths``) rather than in
``fund_flow``: 同花顺 reports total inflow / outflow / net and turnover, with
no main-force figure and no super-large…small split, and prints amounts to
four significant figures ("2.99亿", "2753.86万"; boards in 亿 to 0.01).

The pages are the live "即时" ranking and carry **no date**. They describe the
last session only while the market is closed after it (15:30 → next 09:15,
Beijing), so a fetch is refused outside that window rather than stamped onto
the wrong day. Each page needs a fresh ``hexin-v`` token (see ``hexin.py``);
a sweep is all-or-nothing, since a partial market ranking is not a snapshot.

Measured 2026-09-26: 105 stock pages of 50 (5,210 SH/SZ names — the stock
list carries no Beijing names), 2 industry pages (90 boards), 8 concept
pages (387 boards); a stock sweep takes ~5 minutes at one page per 3 s. The list pages have returned 401 after ~20 rapid requests before
(``q.10jqka.com.cn``), so pages go one at a time on the ``ths_data`` lane.
"""

from __future__ import annotations

import html
import logging
import re
import time
from datetime import date, datetime, timezone

import httpx
import polars as pl

from cnequity.adapters.ths.hexin import hexin_v
from cnequity.domain.http_policy import record_business_refusal, record_http_response
from cnequity.domain.rate_limit import source_request
from cnequity.domain.symbols import format_symbol, infer_exchange_from_code, is_all_a_symbol

logger = logging.getLogger(__name__)

SOURCE = "ths"
_LANE = "ths_data"
_BASE = "http://data.10jqka.com.cn/funds"
_PATHS = {
    "stock": "ggzjl/field/zdf",
    "industry": "hyzjl/field/tradezdf",
    "concept": "gnzjl/field/tradezdf",
}
_REFERERS = {"stock": "ggzjl", "industry": "hyzjl", "concept": "gnzjl"}
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
)
_STOCK_HEADER = [
    "序号", "股票代码", "股票简称", "最新价", "涨跌幅", "换手率",
    "流入资金(元)", "流出资金(元)", "净额(元)", "成交额(元)",
]  # fmt: skip
_BOARD_HEADER = [
    "序号", "行业", "行业指数", "涨跌幅", "流入资金(亿)", "流出资金(亿)",
    "净额(亿)", "公司家数", "领涨股", "涨跌幅", "当前价(元)",
]  # fmt: skip
_MAX_PAGES = 200
_EMPTY_PAGE_RETRY_SECONDS = 10.0

FUND_FLOW_THS_COLUMNS = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "inflow": pl.Float64,
    "outflow": pl.Float64,
    "net_inflow": pl.Float64,
    "amount": pl.Float64,
    "change_pct": pl.Float64,
    "turnover_pct": pl.Float64,
}
SECTOR_FUND_FLOW_THS_COLUMNS = {
    "sector_code": pl.Utf8,
    "sector_name": pl.Utf8,
    "board_type": pl.Utf8,
    "trade_date": pl.Date,
    "sector_index": pl.Float64,
    "change_pct": pl.Float64,
    "inflow": pl.Float64,
    "outflow": pl.Float64,
    "net_inflow": pl.Float64,
    "company_count": pl.Int64,
}


class ThsFundFlowError(RuntimeError):
    """A 同花顺 money-flow sweep could not produce a whole, dated snapshot."""


class ThsEmptyPage(ThsFundFlowError):
    """同花顺 served its empty placeholder table instead of the requested page.

    Seen 2026-11-23 at stock page 83/105: no rows, no pager, and the multi-day
    ranking's default header (阶段涨跌幅 …). It is a transient answer, not a
    layout change, so the sweep asks for that page again.
    """


# ---- session guard ---------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def last_closed_session(now: datetime | None = None) -> date | None:
    """See :func:`cnequity.domain.market_time.last_closed_session`."""
    from cnequity.domain.market_time import last_closed_session as _shared

    return _shared(now or _now())


def require_closed_session(trade_date: date, now: datetime | None = None) -> None:
    """Refuse unless the live ranking can only describe *trade_date*."""
    if last_closed_session(now) != trade_date:
        raise ThsFundFlowError(
            f"同花顺 money flow is undated live data; it describes {trade_date.isoformat()} only "
            "from that session's 15:30 close until the next session opens (Beijing)"
        )


# ---- parsing ----------------------------------------------------------------------

_COMMENT = re.compile(r"<!--.*?-->", re.S)
_ROW = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
_CELL = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.S)
_TAG = re.compile(r"<[^>]+>")
_CODE_HREF = re.compile(r"/code/(\d+)/")
_PAGE_INFO = re.compile(r'class="page_info">\s*(\d+)\s*/\s*(\d+)')


def _text(cell: str) -> str:
    return html.unescape(_TAG.sub("", cell)).strip()


def _amount(text: str) -> float | None:
    """'2.99亿' → 2.99e8, '-772.1万' → -7.721e6, '5236.00' → 5236.0."""
    value = text.replace(",", "").strip()
    if value in {"", "-", "--"}:
        return None
    scale = 1.0
    if value.endswith("亿"):
        scale, value = 1e8, value[:-1]
    elif value.endswith("万"):
        scale, value = 1e4, value[:-1]
    try:
        return round(float(value) * scale, 2)
    except ValueError as exc:
        raise ThsFundFlowError(f"unparseable amount {text!r}") from exc


def _percent(text: str) -> float | None:
    value = text.replace("%", "").strip()
    if value in {"", "-", "--"}:
        return None
    try:
        return float(value)
    except ValueError as exc:
        raise ThsFundFlowError(f"unparseable percent {text!r}") from exc


def _split(page_html: str) -> tuple[list[str], list[list[str]], tuple[int, int] | None]:
    """(header texts, body rows as raw cells, (page, pages))."""
    body = _COMMENT.sub("", page_html)
    head = re.search(r"<thead>(.*?)</thead>", body, re.S)
    header = [_text(c) for c in _CELL.findall(head.group(1))] if head else []
    tbody = re.search(r"<tbody>(.*?)</tbody>", body, re.S)
    rows = [_CELL.findall(r) for r in _ROW.findall(tbody.group(1))] if tbody else []
    info = _PAGE_INFO.search(body)
    return (
        header,
        [r for r in rows if r],
        ((int(info.group(1)), int(info.group(2))) if info else None),
    )


def _reject_placeholder(rows: list, info: tuple[int, int] | None, kind: str) -> None:
    if not rows and info is None:
        raise ThsEmptyPage(f"同花顺 {kind} page came back as the empty placeholder table")


def parse_stock_page(page_html: str, trade_date: date) -> tuple[list[dict], int | None]:
    header, rows, info = _split(page_html)
    _reject_placeholder(rows, info, "stock")
    if header != _STOCK_HEADER:
        raise ThsFundFlowError(f"同花顺 stock money-flow columns changed: {header}")
    out: list[dict] = []
    for cells in rows:
        if len(cells) != len(_STOCK_HEADER):
            raise ThsFundFlowError(f"同花顺 stock row has {len(cells)} cells: {cells[:3]}")
        code = _text(cells[1]).zfill(6)
        exchange = infer_exchange_from_code(code)
        if exchange is None or not is_all_a_symbol(code, exchange):
            continue
        out.append(
            {
                "symbol": format_symbol(code, exchange),
                "trade_date": trade_date,
                "inflow": _amount(_text(cells[6])),
                "outflow": _amount(_text(cells[7])),
                "net_inflow": _amount(_text(cells[8])),
                "amount": _amount(_text(cells[9])),
                "change_pct": _percent(_text(cells[4])),
                "turnover_pct": _percent(_text(cells[5])),
            }
        )
    return out, info[1] if info else None


def parse_board_page(
    page_html: str, trade_date: date, board_type: str
) -> tuple[list[dict], int | None]:
    header, rows, info = _split(page_html)
    _reject_placeholder(rows, info, board_type)
    if header != _BOARD_HEADER:
        raise ThsFundFlowError(f"同花顺 {board_type} money-flow columns changed: {header}")
    out: list[dict] = []
    for cells in rows:
        if len(cells) != len(_BOARD_HEADER):
            raise ThsFundFlowError(f"同花顺 {board_type} row has {len(cells)} cells")
        code = _CODE_HREF.search(cells[1])
        if code is None:
            raise ThsFundFlowError(f"同花顺 {board_type} row without a board code: {cells[1]}")
        count = _text(cells[7])
        out.append(
            {
                "sector_code": code.group(1),
                "sector_name": _text(cells[1]),
                "board_type": board_type,
                "trade_date": trade_date,
                "sector_index": _amount(_text(cells[2])),
                "change_pct": _percent(_text(cells[3])),
                "inflow": _amount(_text(cells[4]) + "亿"),
                "outflow": _amount(_text(cells[5]) + "亿"),
                "net_inflow": _amount(_text(cells[6]) + "亿"),
                "company_count": int(count) if count.isdigit() else None,
            }
        )
    return out, info[1] if info else None


# ---- fetching ---------------------------------------------------------------------


def _wire_recorder(config, dataset: str, trade_date: date, run_id: str | None):
    if config is None or not config.should_archive_raw(dataset):
        return None
    from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive, begin_capture

    if not run_id:
        raise RawArchiveError(f"{dataset}: exact wire capture requires run_id")
    scope = f"daily:{trade_date.isoformat()}"
    nonce = begin_capture(config, dataset, run_id, source=SOURCE, request_scope=scope)
    archive = RawPayloadArchive(
        config.meta_root,
        enabled=True,
        datasets=[dataset],
        compression=config.raw_archive_compression,
        max_payload_bytes=config.raw_archive_max_payload_bytes,
        capture_owner=config,
        capture_run_id=run_id,
        capture_source=SOURCE,
        capture_scope=scope,
        capture_nonce=nonce,
    )

    def record(url: str, payload: bytes, kind: str, page: int) -> None:
        archive.archive(
            dataset,
            payload,
            source=SOURCE,
            run_id=run_id,
            request_params={"trade_date": trade_date.isoformat(), "kind": kind, "page": page},
            url=url,
            payload_format="bytes",
            http_metadata={"wire_exact": True, "encoding": "gbk"},
            observation_id=f"{run_id}:{scope}:{kind}:{page}",
            request_scope=scope,
        )

    return record


def _get_page(client: httpx.Client, kind: str, page: int, config, *, recorder=None) -> str:
    url = f"{_BASE}/{_PATHS[kind]}/order/desc/page/{page}/ajax/1/free/1/"
    last: Exception | None = None
    for attempt in range(2):
        token = hexin_v(config)
        headers = {
            "hexin-v": token,
            "Cookie": f"v={token}",
            "Referer": f"{_BASE}/{_REFERERS[kind]}/",
            "X-Requested-With": "XMLHttpRequest",
        }
        try:
            with source_request(config, _LANE):
                resp = client.get(url, headers=headers)
                record_http_response(config, _LANE, resp)
        except httpx.HTTPError as exc:
            last = exc
        else:
            if resp.status_code == 200:
                if recorder is not None:
                    recorder(url, resp.content, kind, page)
                return resp.content.decode("gbk", errors="replace")
            last = ThsFundFlowError(f"{url} -> HTTP {resp.status_code}")
            if resp.status_code in (401, 403):
                if resp.status_code == 401:
                    record_business_refusal(config, _LANE, kind="public_token_gate")
                raise last
            break
        # Only a transport error gets a second attempt; token/refusal statuses
        # stop this source before the next page or dataset can send again.
        if attempt == 0:
            time.sleep(5.0)
    raise ThsFundFlowError(f"同花顺 {kind} page {page} failed: {last}") from last


def _sweep(kind: str, parse, config, *, recorder=None) -> list[dict]:
    rows: list[dict] = []
    with httpx.Client(timeout=20.0, headers={"User-Agent": _UA}, trust_env=False) as client:
        page, pages = 1, None
        while pages is None or page <= pages:
            if page > _MAX_PAGES:
                raise ThsFundFlowError(f"同花顺 {kind} pagination exceeded {_MAX_PAGES} pages")
            try:
                got, total = parse(_get_page(client, kind, page, config, recorder=recorder))
            except ThsEmptyPage:
                time.sleep(_EMPTY_PAGE_RETRY_SECONDS)
                got, total = parse(_get_page(client, kind, page, config, recorder=recorder))
            if pages is None:
                if total is None:
                    raise ThsFundFlowError(f"同花顺 {kind} page 1 has no page count")
                pages = total
            if not got and page < pages:
                raise ThsFundFlowError(f"同花顺 {kind} page {page}/{pages} came back empty")
            rows.extend(got)
            page += 1
    logger.info("同花顺 %s money flow: %d rows from %d pages", kind, len(rows), pages or 0)
    return rows


def fetch_fund_flow_ths(
    trade_date: date, *, config=None, run_id: str | None = None
) -> pl.DataFrame:
    require_closed_session(trade_date)
    recorder = _wire_recorder(config, "fund_flow_ths", trade_date, run_id)
    rows = _sweep(
        "stock", lambda text: parse_stock_page(text, trade_date), config, recorder=recorder
    )
    if not rows:
        raise ThsFundFlowError("同花顺 stock money flow returned no rows")
    return pl.DataFrame(rows, schema=FUND_FLOW_THS_COLUMNS).unique(
        subset=["symbol", "trade_date"], keep="first", maintain_order=True
    )


def fetch_sector_fund_flow_ths(
    trade_date: date, *, config=None, run_id: str | None = None
) -> pl.DataFrame:
    require_closed_session(trade_date)
    recorder = _wire_recorder(config, "sector_fund_flow_ths", trade_date, run_id)
    rows: list[dict] = []
    for kind in ("industry", "concept"):
        rows.extend(
            _sweep(
                kind,
                lambda text, k=kind: parse_board_page(text, trade_date, k),
                config,
                recorder=recorder,
            )
        )
    if not rows:
        raise ThsFundFlowError("同花顺 board money flow returned no rows")
    return pl.DataFrame(rows, schema=SECTOR_FUND_FLOW_THS_COLUMNS).unique(
        subset=["sector_code", "board_type", "trade_date"], keep="first", maintain_order=True
    )
