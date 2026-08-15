"""EastMoney per-symbol stock news (on-demand + batch sentiment input)."""

from __future__ import annotations

import logging
from datetime import date

from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.domain.sentiment import aggregate_scores, score_text
from cn_market_lake.domain.symbols import parse_symbol

logger = logging.getLogger(__name__)

_NEWS_URL = "https://np-anotice-stock.eastmoney.com/api/security/news"

_MARKET_CODES = {"SH": "1", "SZ": "0", "BJ": "2"}


def _parse_publish_date(raw: object) -> date | None:
    if raw is None:
        return None
    text = str(raw).strip().replace("/", "-")
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _normalize_item(item: dict, *, use_snownlp: bool) -> dict | None:
    title = str(item.get("title") or item.get("TITLE") or "").strip()
    if not title:
        return None
    publish_raw = item.get("showtime") or item.get("NOTICE_DATE") or item.get("publish_time")
    pub_date = _parse_publish_date(publish_raw)
    score, method = score_text(title, use_snownlp=use_snownlp)
    news_id = str(item.get("art_code") or item.get("uniqueUrl") or item.get("url") or title)
    return {
        "news_id": news_id,
        "title": title,
        "publish_time": str(publish_raw or ""),
        "publish_date": pub_date.isoformat() if pub_date else None,
        "url": str(item.get("url") or item.get("uniqueUrl") or ""),
        "sentiment_score": score,
        "sentiment_method": method,
    }


def fetch_stock_news(
    symbol: str,
    *,
    on_date: date | None = None,
    limit: int = 30,
    use_snownlp: bool = True,
    client: EastMoneyClient | None = None,
) -> dict:
    """Fetch recent headlines for *symbol*; optionally filter to *on_date*."""
    info = parse_symbol(symbol)
    market = _MARKET_CODES.get(info.exchange, "0")
    owns = client is None
    if client is None:
        client = EastMoneyClient()

    params = {
        "stock_list": info.code,
        "page_size": str(limit),
        "page_index": "1",
        "market_code": market,
        "client": "web",
    }
    items: list[dict] = []
    try:
        resp = client.get(_NEWS_URL, params=params)
        resp.raise_for_status()
        payload = resp.json()
        raw_list = (payload.get("data") or {}).get("list") or []
        for raw in raw_list:
            norm = _normalize_item(raw, use_snownlp=use_snownlp)
            if norm is None:
                continue
            if on_date is not None:
                pub = _parse_publish_date(norm.get("publish_time"))
                if pub is not None and pub != on_date:
                    continue
            items.append(norm)
    except Exception as exc:
        logger.warning("EastMoney stock_news failed for %s: %s", symbol, exc)
        if owns:
            client.close()
        return {
            "symbol": symbol,
            "source": "eastmoney",
            "items": [],
            "headline_count": 0,
            "aggregate_sentiment": 0.0,
            "error": str(exc),
        }

    if owns:
        client.close()

    scores = [float(i["sentiment_score"]) for i in items]
    return {
        "symbol": symbol,
        "source": "eastmoney",
        "items": items,
        "headline_count": len(items),
        "aggregate_sentiment": aggregate_scores(scores),
        "on_date": on_date.isoformat() if on_date else None,
    }
