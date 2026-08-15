from datetime import date
from unittest.mock import patch

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.derive.sentiment_scores import compute_sentiment_scores


@pytest.fixture
def news_batch_lake(tmp_path):
    root = tmp_path / "data"
    ann = root / "curated" / "announcement_index" / "announce_date=2024-06-28"
    ann.mkdir(parents=True)
    pl.DataFrame(
        {
            "announcement_id": ["a1"],
            "symbol": ["600519.SH"],
            "title": ["业绩超预期增长"],
            "announce_date": [date(2024, 6, 28)],
            "category": [""],
            "url": [""],
            "source": ["cninfo"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    ).write_parquet(ann / "part-0.parquet")
    return Config(data_root=root, sources={"eastmoney": True}, sentiment_use_snownlp=False)


@pytest.fixture
def headlines_lake(tmp_path):
    root = tmp_path / "data"
    hl = root / "curated" / "news_headlines" / "publish_date=2024-06-28"
    hl.mkdir(parents=True)
    pl.DataFrame(
        {
            "news_id": ["n1", "n2"],
            "publish_date": [date(2024, 6, 28), date(2024, 6, 28)],
            "publish_time": ["10:00:00", "11:00:00"],
            "title": ["签约利好落地", "无关宏观新闻"],
            "summary": ["", ""],
            "related_symbols": ["600519.SH", None],
            "channel": ["fast_news", "fast_news"],
            "source": ["eastmoney", "eastmoney"],
            "data_version": ["v1", "v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00", "2024-06-28T00:00:00+00:00"],
        }
    ).write_parquet(hl / "part-0.parquet")
    return Config(
        data_root=root,
        sources={"eastmoney": True},
        sentiment_use_snownlp=False,
        sentiment_news_symbol_limit=50,
    )


def test_batch_sentiment_uses_headlines_without_http(headlines_lake):
    with patch(
        "cn_market_lake.derive.sentiment_scores.fetch_stock_news",
    ) as fetch_news:
        df = compute_sentiment_scores(headlines_lake, date(2024, 6, 28))
        fetch_news.assert_not_called()

    channels = set(df["score_channel"].to_list())
    assert "news_headlines" in channels
    news = df.filter(pl.col("score_channel") == "news_headlines")
    assert news["symbol"][0] == "600519.SH"
    assert news["sentiment_score"][0] > 0


def test_batch_sentiment_http_fallback_when_no_headlines(news_batch_lake):
    news_payload = {
        "symbol": "600519.SH",
        "source": "eastmoney",
        "items": [{"title": "签约利好", "sentiment_score": 0.8}],
        "headline_count": 1,
        "aggregate_sentiment": 0.8,
    }
    with patch(
        "cn_market_lake.derive.sentiment_scores.fetch_stock_news",
        return_value=news_payload,
    ):
        df = compute_sentiment_scores(news_batch_lake, date(2024, 6, 28))

    channels = set(df["score_channel"].to_list())
    assert "announcement_keywords" in channels
    assert "stock_news_nlp" in channels
    news = df.filter(pl.col("score_channel") == "stock_news_nlp")
    assert news["sentiment_score"][0] == pytest.approx(0.8)


def test_batch_sentiment_soft_fails_http_channel(news_batch_lake):
    with patch(
        "cn_market_lake.derive.sentiment_scores.fetch_stock_news",
        side_effect=RuntimeError("boom"),
    ):
        df = compute_sentiment_scores(news_batch_lake, date(2024, 6, 28))

    assert "announcement_keywords" in set(df["score_channel"].to_list())
    assert "stock_news_nlp" not in set(df["score_channel"].to_list())
