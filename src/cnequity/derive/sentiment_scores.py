"""Batch sentiment scores from announcements + curated headlines (+ optional stock news)."""

from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path

import polars as pl

from cnequity.adapters.eastmoney.stock_news import fetch_stock_news
from cnequity.config import Config
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.sentiment import score_text

logger = logging.getLogger(__name__)

_ANNOUNCEMENT_CHANNEL = "announcement_keywords"
_HEADLINES_CHANNEL = "news_headlines"
_NEWS_CHANNEL = "stock_news_nlp"
_DEFAULT_NEWS_SYMBOL_LIMIT = 50
_HTTP_FAIL_BREAKER = 5
_A_SHARE_RE = re.compile(r"^\d{6}\.(SH|SZ)$", re.IGNORECASE)


def _read_announcements(root: Path, trade_date: date) -> pl.DataFrame:
    if not root.exists():
        return pl.DataFrame()
    from cnequity.query.parquet_scan import collect_parquet_root

    try:
        df = collect_parquet_root(
            root,
            partition_col="announce_date",
            start=trade_date,
            end=trade_date,
        )
    except FileNotFoundError:
        return pl.DataFrame()
    if "announce_date" not in df.columns:
        return pl.DataFrame()
    if "announcement_id" in df.columns:
        df = dedupe_by_primary_key(df, "announcement_index")
    return df.filter(pl.col("announce_date") == trade_date)


def _announcement_sentiment(config: Config, trade_date: date) -> pl.DataFrame:
    root = config.curated_root / "announcement_index"
    announcements = _read_announcements(root, trade_date)
    if announcements.is_empty() or "title" not in announcements.columns:
        return pl.DataFrame()

    scored = announcements.with_columns(
        pl.col("title")
        .map_elements(
            lambda t: score_text(str(t), use_snownlp=False)[0],
            return_dtype=pl.Float64,
        )
        .alias("_score")
    )
    agg = scored.group_by("symbol").agg(
        pl.col("_score").mean().alias("sentiment_score"),
        pl.len().alias("headline_count"),
    )
    return agg.with_columns(
        pl.lit(trade_date).alias("trade_date"),
        pl.lit(_ANNOUNCEMENT_CHANNEL).alias("score_channel"),
    ).select(["symbol", "trade_date", "score_channel", "sentiment_score", "headline_count"])


def _read_news_headlines(root: Path, trade_date: date) -> pl.DataFrame:
    if not root.exists():
        return pl.DataFrame()
    from cnequity.query.parquet_scan import collect_parquet_root

    try:
        df = collect_parquet_root(
            root,
            partition_col="publish_date",
            start=trade_date,
            end=trade_date,
        )
    except FileNotFoundError:
        return pl.DataFrame()
    if "publish_date" not in df.columns or "title" not in df.columns:
        return pl.DataFrame()
    if "news_id" in df.columns:
        df = dedupe_by_primary_key(df, "news_headlines")
    return df.filter(pl.col("publish_date") == trade_date)


def _parse_related_symbols(raw: object) -> list[str]:
    if raw is None:
        return []
    text = str(raw).strip()
    if not text or text.lower() in {"none", "null"}:
        return []
    out: list[str] = []
    for part in text.replace(";", ",").split(","):
        sym = part.strip().upper()
        if _A_SHARE_RE.match(sym):
            out.append(sym)
    return out


def _headlines_sentiment(config: Config, trade_date: date) -> pl.DataFrame:
    """Score curated news_headlines (no HTTP) — primary news channel for batch."""
    headlines = _read_news_headlines(config.curated_root / "news_headlines", trade_date)
    if headlines.is_empty():
        return pl.DataFrame()

    rows: list[dict] = []
    for row in headlines.iter_rows(named=True):
        symbols = _parse_related_symbols(row.get("related_symbols"))
        if not symbols:
            continue
        score, _ = score_text(str(row.get("title") or ""), use_snownlp=False)
        for sym in symbols:
            rows.append({"symbol": sym, "sentiment_score": score})
    if not rows:
        return pl.DataFrame()

    return (
        pl.DataFrame(rows)
        .group_by("symbol")
        .agg(
            pl.col("sentiment_score").mean().alias("sentiment_score"),
            pl.len().alias("headline_count"),
        )
        .with_columns(
            pl.lit(trade_date).alias("trade_date"),
            pl.lit(_HEADLINES_CHANNEL).alias("score_channel"),
        )
        .select(["symbol", "trade_date", "score_channel", "sentiment_score", "headline_count"])
    )


def _hot_rank_symbols(config: Config, trade_date: date, limit: int) -> list[str]:
    root = config.curated_root / "hot_rank"
    if not root.exists() or limit <= 0:
        return []
    from cnequity.domain.canonical import dedupe_by_primary_key
    from cnequity.query.parquet_scan import collect_parquet_root

    try:
        df = collect_parquet_root(
            root,
            partition_col="trade_date",
            start=trade_date,
            end=trade_date,
        )
        if df.is_empty():
            # Prefer the latest available day when the requested snapshot is
            # absent. This also works when hot_rank is month/year partitioned.
            df = collect_parquet_root(root, partition_col="trade_date", end=trade_date)
            if "trade_date" in df.columns and not df.is_empty():
                latest = df["trade_date"].max()
                df = df.filter(pl.col("trade_date") == latest)
    except FileNotFoundError:
        return []
    if "symbol" not in df.columns:
        return []
    df = dedupe_by_primary_key(df, "hot_rank")
    if "rank" in df.columns:
        df = df.sort("rank")
    return df["symbol"].unique(maintain_order=True).to_list()[:limit]


def _news_sentiment_symbols(config: Config, trade_date: date, limit: int) -> list[str]:
    symbols: list[str] = []
    seen: set[str] = set()

    def _extend(cands: list[str]) -> None:
        for sym in cands:
            if sym in seen:
                continue
            seen.add(sym)
            symbols.append(sym)
            if len(symbols) >= limit:
                return

    _extend(_hot_rank_symbols(config, trade_date, limit))
    if len(symbols) >= limit:
        return symbols[:limit]

    ann = _read_announcements(config.curated_root / "announcement_index", trade_date)
    if not ann.is_empty() and "symbol" in ann.columns:
        _extend(ann["symbol"].unique().to_list())
    if len(symbols) >= limit:
        return symbols[:limit]

    bars_root = config.curated_root / "daily_bars"
    if bars_root.exists():
        from cnequity.domain.canonical import dedupe_by_primary_key
        from cnequity.query.parquet_scan import collect_parquet_root

        try:
            bars = collect_parquet_root(
                bars_root,
                partition_col="trade_date",
                start=trade_date,
                end=trade_date,
            )
        except FileNotFoundError:
            bars = pl.DataFrame()
        if "symbol" in bars.columns and "amount" in bars.columns:
            bars = dedupe_by_primary_key(bars, "daily_bars")
            if "volume" in bars.columns:
                bars = bars.filter((pl.col("volume") > 0) | pl.col("volume").is_null())
            top = (
                bars.sort("amount", descending=True)
                .unique(subset=["symbol"], keep="first", maintain_order=True)
                .head(max(0, limit - len(symbols)))["symbol"]
                .to_list()
            )
            _extend(top)
    return symbols[:limit]


def _stock_news_sentiment(config: Config, trade_date: date) -> pl.DataFrame:
    """HTTP fallback — capped universe + circuit breaker so batch never hangs."""
    if not config.sources.get("eastmoney", True):
        return pl.DataFrame()

    limit = max(0, int(config.sentiment_news_symbol_limit or _DEFAULT_NEWS_SYMBOL_LIMIT))
    if limit == 0:
        return pl.DataFrame()

    symbols = _news_sentiment_symbols(config, trade_date, limit)
    if not symbols:
        return pl.DataFrame()

    rows: list[dict] = []
    # Batch path defaults to keywords; SnowNLP per-headline is too slow for daily.
    use_snownlp = False
    client = None
    consecutive_failures = 0
    try:
        from cnequity.adapters.eastmoney.em_auth import EastMoneyClient

        client = EastMoneyClient(config=config)
        for sym in symbols:
            if consecutive_failures >= _HTTP_FAIL_BREAKER:
                logger.warning(
                    "stock_news_nlp: aborting after %d consecutive failures (%d/%d symbols done)",
                    consecutive_failures,
                    len(rows),
                    len(symbols),
                )
                break
            try:
                payload = fetch_stock_news(
                    sym,
                    on_date=trade_date,
                    limit=20,
                    use_snownlp=use_snownlp,
                    client=client,
                    config=config,
                )
            except Exception as exc:  # noqa: BLE001 — fail-soft per symbol
                consecutive_failures += 1
                logger.warning("stock_news fetch failed for %s: %s", sym, exc)
                continue
            if payload.get("error"):
                consecutive_failures += 1
                logger.warning(
                    "stock_news fetch returned an error for %s: %s", sym, payload["error"]
                )
                continue
            consecutive_failures = 0
            if payload.get("headline_count", 0) == 0:
                continue
            rows.append(
                {
                    "symbol": sym,
                    "trade_date": trade_date,
                    "score_channel": _NEWS_CHANNEL,
                    "sentiment_score": payload["aggregate_sentiment"],
                    "headline_count": payload["headline_count"],
                }
            )
            _maybe_cache_news(config, payload)
    finally:
        if client is not None:
            client.close()

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def _maybe_cache_news(config: Config, payload: dict) -> None:
    """Warm on-demand cache when batch fetch pulls news."""
    if not config.on_demand_enabled:
        return
    symbol = payload["symbol"]
    cache_dir = config.meta_root / "on_demand" / "stock_news"
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{symbol.replace('.', '_')}.json"
    if path.exists():
        return
    from cnequity.storage.atomic import write_json_atomic

    write_json_atomic(path, payload, ensure_ascii=False, indent=2)


def compute_sentiment_scores(config: Config, trade_date: date) -> pl.DataFrame:
    frames: list[pl.DataFrame] = []
    ann = _announcement_sentiment(config, trade_date)
    if not ann.is_empty():
        frames.append(ann)

    # Prefer lake headlines (no HTTP). HTTP stock_news is optional fallback only
    # when headlines are empty for the day.
    try:
        headlines = _headlines_sentiment(config, trade_date)
        if not headlines.is_empty():
            frames.append(headlines)
        else:
            news = _stock_news_sentiment(config, trade_date)
            if not news.is_empty():
                frames.append(news)
    except Exception as exc:  # noqa: BLE001 — never block announcement channel
        logger.warning("news sentiment channel failed (soft): %s", exc)

    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")
