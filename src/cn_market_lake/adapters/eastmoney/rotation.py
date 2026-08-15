"""EastMoney rotation datasets: hot rank, sector bars/flows, market news."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from urllib.parse import urlencode

import polars as pl

from cn_market_lake.adapters.eastmoney.clist import fetch_clist_pages
from cn_market_lake.adapters.eastmoney.common import _to_float
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.domain.symbols import format_symbol

logger = logging.getLogger(__name__)

_HOT_RANK_URL = "https://emappdata.eastmoney.com/stockrank/getAllCurrentList"
_NEWS_URL = "https://np-listapi.eastmoney.com/comm/web/getFastNewsList"

_CONCEPT_FS = "m:90+t:3"
_INDUSTRY_FS = "m:90+t:2"
_BOARD_FIELDS = "f12,f14,f2,f3,f15,f16,f17,f5,f6,f8,f62"


def _hot_symbol(sc: str) -> str | None:
    text = str(sc or "").strip().upper()
    if len(text) < 8:
        return None
    mkt, code = text[:2], text[2:].zfill(6)
    if mkt not in {"SH", "SZ", "BJ"}:
        return None
    return format_symbol(code, mkt)


def _news_symbols(stock_list: list | None) -> str | None:
    if not stock_list:
        return None
    out: list[str] = []
    for raw in stock_list:
        text = str(raw)
        if "." in text:
            parts = text.split(".", 1)
            code = parts[1].zfill(6)
            mkt = "SH" if code.startswith(("60", "68")) else "SZ"
        else:
            code = text.zfill(6)
            mkt = "SH" if code.startswith(("60", "68")) else "SZ"
        out.append(format_symbol(code, mkt))
    return ",".join(sorted(set(out))) if out else None


def fetch_hot_rank(trade_date: date, *, top_n: int = 500) -> pl.DataFrame:
    rows: list[dict] = []
    page_size = 100
    page = 1
    with EastMoneyClient() as client:
        while len(rows) < top_n:
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
            batch = payload.get("data") or []
            if not batch:
                break
            for item in batch:
                sym = _hot_symbol(item.get("sc", ""))
                if not sym:
                    continue
                rows.append(
                    {
                        "symbol": sym,
                        "trade_date": trade_date,
                        "rank": int(item.get("rk") or 0),
                        "rank_change": int(item.get("rc") or 0),
                        "hist_rank": int(item.get("hisRc") or 0),
                    }
                )
                if len(rows) >= top_n:
                    break
            if len(batch) < page_size:
                break
            page += 1
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def _fetch_board_rows(client: EastMoneyClient, fs: str, board_type: str) -> list[dict]:
    # Smaller pages: pz=5000 often trips push2 502 mid-universe; 100 is stable.
    raw = fetch_clist_pages(client, fields=_BOARD_FIELDS, fs=fs, page_size=100)
    rows: list[dict] = []
    for item in raw:
        code = str(item.get("f12") or "").strip()
        if not code:
            continue
        rows.append(
            {
                "sector_code": code,
                "sector_name": str(item.get("f14") or ""),
                "board_type": board_type,
                "item": item,
            }
        )
    return rows


def fetch_sector_fund_flow(trade_date: date) -> pl.DataFrame:
    rows: list[dict] = []
    with EastMoneyClient() as client:
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
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fetch_news_headlines(trade_date: date, *, page_size: int = 200) -> pl.DataFrame:
    rows: list[dict] = []
    target = trade_date.isoformat()
    with EastMoneyClient() as client:
        params = urlencode(
            {
                "client": "web",
                "biz": "web_724",
                "fastColumn": "102",
                "sortEnd": "",
                "pageSize": page_size,
                "req_trace": "1",
            }
        )
        resp = client.get(f"{_NEWS_URL}?{params}", timeout=30.0)
        resp.raise_for_status()
        items = (resp.json().get("data") or {}).get("fastNewsList") or []
        for item in items:
            show_time = str(item.get("showTime") or "")
            if not show_time.startswith(target):
                continue
            pub_dt = datetime.fromisoformat(show_time)
            rows.append(
                {
                    "news_id": str(item.get("code") or item.get("realSort") or ""),
                    "publish_date": trade_date,
                    "publish_time": pub_dt.strftime("%H:%M:%S"),
                    "title": str(item.get("title") or "").strip(),
                    "summary": str(item.get("summary") or "").strip() or None,
                    "related_symbols": _news_symbols(item.get("stockList")),
                    "channel": "fast_news",
                }
            )
    if not rows:
        logger.info("news_headlines: no items for %s (market may be closed)", target)
    return pl.DataFrame(rows) if rows else pl.DataFrame()
