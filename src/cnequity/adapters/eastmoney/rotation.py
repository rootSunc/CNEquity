"""EastMoney rotation datasets: hot rank, sector bars/flows, market news."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from urllib.parse import urlencode

import polars as pl

from cnequity.adapters.eastmoney.clist import fetch_clist_pages
from cnequity.adapters.eastmoney.common import _to_float, _to_int
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.domain.symbols import format_symbol, infer_exchange_from_code, is_all_a_symbol

logger = logging.getLogger(__name__)

_HOT_RANK_URL = "https://emappdata.eastmoney.com/stockrank/getAllCurrentList"
_NEWS_URL = "https://np-listapi.eastmoney.com/comm/web/getFastNewsList"
_HOT_MAX_PAGES = 100
_NEWS_MAX_PAGES = 100

_CONCEPT_FS = "m:90+t:3"
_INDUSTRY_FS = "m:90+t:2"
_BOARD_FIELDS = "f12,f14,f2,f3,f15,f16,f17,f5,f6,f8,f62"


def _object_rows(raw: object, *, source: str) -> list[dict]:
    """Validate an optional feed's row container and isolate bad members."""
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise RuntimeError(f"EastMoney {source} response rows are not a list")
    rows: list[dict] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            logger.warning("EastMoney %s: skipping non-object row %s", source, index)
            continue
        rows.append(item)
    return rows


def _hot_symbol(sc: str) -> str | None:
    text = str(sc or "").strip().upper()
    if len(text) < 8:
        return None
    mkt, code = text[:2], text[2:].zfill(6)
    if mkt not in {"SH", "SZ", "BJ"} or len(code) != 6 or not code.isdigit():
        return None
    # EastMoney can stamp a BSE (92xxxx) code with an SH/SZ market prefix.
    # Route by the numeric code so those names are not dropped as malformed.
    if mkt in {"SH", "SZ"} and infer_exchange_from_code(code) == "BJ":
        mkt = "BJ"
    if not is_all_a_symbol(code, mkt):
        return None
    return format_symbol(code, mkt)


def _rank_value(value: object) -> int | None:
    return _to_int(value)


def _news_symbols(stock_list: list | None) -> str | None:
    if not stock_list:
        return None
    out: list[str] = []
    for raw in stock_list:
        text = str(raw)
        if "." in text:
            parts = text.split(".", 1)
            code = parts[1].zfill(6)
            mkt = _news_market(code)
        else:
            code = text.zfill(6)
        mkt = _news_market(code)
        if len(code) != 6 or not code.isdigit() or not is_all_a_symbol(code, mkt):
            continue
        out.append(format_symbol(code, mkt))
    return ",".join(sorted(set(out))) if out else None


def _news_market(code: str) -> str:
    return infer_exchange_from_code(code)


def fetch_hot_rank(
    trade_date: date, *, top_n: int = 500, config=None, require_top_n: bool = False
) -> pl.DataFrame:
    if top_n < 0:
        raise ValueError("hot rank top_n must be non-negative")
    # Count unique symbols, not wire rows. The endpoint can repeat a symbol at
    # a page boundary; stopping on raw row count would return fewer than
    # ``top_n`` names while claiming the requested rank window was complete.
    rows_by_symbol: dict[str, dict] = {}
    page_size = 100
    page = 1
    seen_pages: set[str] = set()
    client_kwargs = {"config": config} if config is not None else {}
    with EastMoneyClient(**client_kwargs) as client:
        while len(rows_by_symbol) < top_n:
            if page > _HOT_MAX_PAGES:
                raise RuntimeError(
                    f"EastMoney hot rank pagination exceeded {_HOT_MAX_PAGES} pages "
                    f"with {len(rows_by_symbol)} unique A-share symbols"
                )
            body = json.dumps(
                {
                    "appId": "appId01",
                    "globalId": "786e4c21-70dc-435a-93bb-b",
                    "pageNo": page,
                    "pageSize": page_size,
                }
            ).encode()
            resp = client.post(
                _HOT_RANK_URL,
                content=body,
                headers={"Content-Type": "application/json"},
                timeout=30.0,
            )
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise RuntimeError("EastMoney hot rank response is not an object")
            batch = _object_rows(payload.get("data"), source="hot rank")
            if not batch:
                break
            fingerprint = json.dumps(batch, sort_keys=True, default=str, separators=(",", ":"))
            if fingerprint in seen_pages:
                raise RuntimeError(
                    f"EastMoney hot rank pagination repeated page {page}; "
                    "refusing a potentially incomplete result"
                )
            seen_pages.add(fingerprint)
            for item in batch:
                sym = _hot_symbol(item.get("sc", ""))
                if not sym:
                    continue
                rows_by_symbol[sym] = {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "rank": _rank_value(item.get("rk")),
                    "rank_change": _rank_value(item.get("rc")),
                    "hist_rank": _rank_value(item.get("hisRc")),
                }
                if len(rows_by_symbol) >= top_n:
                    break
            if len(batch) < page_size:
                break
            page += 1
    if require_top_n and len(rows_by_symbol) < top_n:
        raise RuntimeError(
            f"EastMoney hot rank returned only {len(rows_by_symbol)} unique A-share "
            f"symbols; expected {top_n}"
        )
    elif len(rows_by_symbol) < top_n:
        logger.warning(
            "EastMoney hot rank returned only %d unique A-share symbols; expected %d "
            "- accepting the partial snapshot",
            len(rows_by_symbol),
            top_n,
        )
    rows = list(rows_by_symbol.values())
    return (
        pl.DataFrame(rows).unique(subset=["symbol", "trade_date"], keep="last")
        if rows
        else pl.DataFrame()
    )


def _fetch_board_rows(client: EastMoneyClient, fs: str, board_type: str) -> list[dict]:
    # Smaller pages: pz=5000 often trips push2 502 mid-universe; 100 is stable.
    raw = fetch_clist_pages(client, fields=_BOARD_FIELDS, fs=fs, page_size=100)
    missing_codes = sum(1 for item in raw if not str(item.get("f12") or "").strip())
    if missing_codes:
        raise RuntimeError(
            f"EastMoney {board_type} board clist returned {missing_codes} row(s) without f12"
        )
    rows: list[dict] = []
    for item in raw:
        code = str(item["f12"]).strip()
        rows.append(
            {
                "sector_code": code,
                "sector_name": str(item.get("f14") or ""),
                "board_type": board_type,
                "item": item,
            }
        )
    return rows


def fetch_sector_fund_flow(trade_date: date, *, config=None) -> pl.DataFrame:
    rows: list[dict] = []
    client_kwargs = {"config": config} if config is not None else {}
    with EastMoneyClient(**client_kwargs) as client:
        boards = _fetch_board_rows(client, _CONCEPT_FS, "concept") + _fetch_board_rows(
            client, _INDUSTRY_FS, "industry"
        )
        for b in boards:
            item = b["item"]
            rows.append(
                {
                    "sector_code": b["sector_code"],
                    "sector_name": b["sector_name"],
                    "board_type": b["board_type"],
                    "trade_date": trade_date,
                    "main_net_inflow": _to_float(item.get("f62")),
                    "change_pct": _to_float(item.get("f3")),
                    "turnover_pct": _to_float(item.get("f8")),
                }
            )
    return (
        pl.DataFrame(rows).unique(subset=["sector_code", "trade_date"], keep="last")
        if rows
        else pl.DataFrame()
    )


def fetch_news_headlines(trade_date: date, *, page_size: int = 200, config=None) -> pl.DataFrame:
    if page_size <= 0:
        raise ValueError("news page_size must be positive")
    rows: list[dict] = []
    target = trade_date.isoformat()
    client_kwargs = {"config": config} if config is not None else {}
    with EastMoneyClient(**client_kwargs) as client:
        sort_end = ""
        seen_cursors: set[str] = set()
        for _page in range(_NEWS_MAX_PAGES):
            params_dict = {
                "client": "web",
                "biz": "web_724",
                "fastColumn": "102",
                "sortEnd": sort_end,
                "pageSize": page_size,
                "req_trace": "1",
            }
            params = urlencode(params_dict)
            resp = client.get(f"{_NEWS_URL}?{params}", timeout=30.0)
            resp.raise_for_status()
            payload = resp.json()
            if not isinstance(payload, dict):
                raise RuntimeError("EastMoney news response is not an object")
            raw_data = payload.get("data")
            if raw_data is None:
                items = []
                next_cursor = ""
            elif not isinstance(raw_data, dict):
                raise RuntimeError("EastMoney news response data is not an object")
            else:
                items = _object_rows(raw_data.get("fastNewsList"), source="news")
                next_cursor = str(raw_data.get("sortEnd") or "").strip()

            page_dates: list[date] = []
            for item in items:
                show_time = str(item.get("showTime") or "")
                try:
                    pub_dt = datetime.fromisoformat(show_time)
                except ValueError:
                    if show_time.startswith(target):
                        logger.warning(
                            "news_headlines: skipping item with invalid showTime %r",
                            show_time,
                        )
                    continue
                page_dates.append(pub_dt.date())
                if pub_dt.date() != trade_date:
                    continue
                title = str(item.get("title") or "").strip()
                if not title:
                    logger.warning("news_headlines: skipping item without a title")
                    continue
                news_id = str(item.get("code") or item.get("realSort") or "").strip()
                if not news_id:
                    logger.warning("news_headlines: skipping item without a stable news id")
                    continue
                rows.append(
                    {
                        "news_id": news_id,
                        "publish_date": trade_date,
                        "publish_time": pub_dt.strftime("%H:%M:%S"),
                        "title": title,
                        "summary": str(item.get("summary") or "").strip() or None,
                        "related_symbols": _news_symbols(item.get("stockList")),
                        "channel": "fast_news",
                    }
                )

            # Results are newest-first. Once a page crosses before the target,
            # all later pages are older and cannot contain another target row.
            if not items or (page_dates and min(page_dates) < trade_date):
                break
            if not next_cursor:
                break
            if next_cursor in seen_cursors or next_cursor == sort_end:
                raise RuntimeError("EastMoney news pagination cursor did not advance")
            seen_cursors.add(next_cursor)
            sort_end = next_cursor
        else:
            raise RuntimeError(
                f"EastMoney news pagination exceeded {_NEWS_MAX_PAGES} pages "
                f"without reaching {target}"
            )
    if not rows:
        logger.info("news_headlines: no items for %s (market may be closed)", target)
    return pl.DataFrame(rows).unique(subset=["news_id"], keep="last") if rows else pl.DataFrame()
