from datetime import date
from unittest.mock import patch

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.derive.sentiment_scores import (
    _hot_rank_symbols,
    _news_sentiment_symbols,
    _read_news_headlines,
    compute_sentiment_scores,
)


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
            "news_id": ["n1", "n2", "n1"],
            "publish_date": [date(2024, 6, 28)] * 3,
            "publish_time": ["10:00:00", "11:00:00", "10:01:00"],
            "title": ["签约利好落地", "无关宏观新闻", "签约利好落地修订"],
            "summary": ["", "", ""],
            "related_symbols": ["600519.SH", None, "600519.SH"],
            "channel": ["fast_news"] * 3,
            "source": ["eastmoney"] * 3,
            "data_version": ["v1"] * 3,
            "fetched_at": [
                "2024-06-28T00:00:00+00:00",
                "2024-06-28T00:00:00+00:00",
                "2024-06-28T00:00:01+00:00",
            ],
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
        "cnequity.derive.sentiment_scores.fetch_stock_news",
    ) as fetch_news:
        df = compute_sentiment_scores(headlines_lake, date(2024, 6, 28))
        fetch_news.assert_not_called()

    channels = set(df["score_channel"].to_list())
    assert "news_headlines" in channels
    news = df.filter(pl.col("score_channel") == "news_headlines")
    assert news["symbol"][0] == "600519.SH"
    assert news["headline_count"][0] == 1
    assert news["sentiment_score"][0] > 0


def test_news_headlines_reader_handles_month_partitions(tmp_path):
    root = tmp_path / "news_headlines"
    part = root / "publish_date=2024-06"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "news_id": ["n1"],
            "publish_date": [date(2024, 6, 28)],
            "title": ["签约利好落地"],
            "related_symbols": ["600519.SH"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    ).write_parquet(part / "part-0.parquet")

    result = _read_news_headlines(root, date(2024, 6, 28))

    assert result["news_id"].to_list() == ["n1"]


def test_news_sentiment_universe_reads_all_daily_bar_shards(tmp_path):
    root = tmp_path / "data"
    part = root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["000001.SZ"],
            "trade_date": [date(2024, 6, 28)],
            "amount": [100.0],
            "volume": [100],
        }
    ).write_parquet(part / "part-000.parquet")
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "600000.SH"],
            "trade_date": [date(2024, 6, 28)] * 2,
            "amount": [900.0, 800.0],
            "volume": [100, 100],
        }
    ).write_parquet(part / "part-001.parquet")

    config = Config(data_root=root)
    assert _news_sentiment_symbols(config, date(2024, 6, 28), 2) == [
        "600519.SH",
        "600000.SH",
    ]


def test_hot_rank_fallback_reads_all_latest_partition_shards(tmp_path):
    root = tmp_path / "data"
    part = root / "curated" / "hot_rank" / "trade_date=2024-06-27"
    part.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600519.SH"], "rank": [1]}).write_parquet(part / "part-000.parquet")
    pl.DataFrame({"symbol": ["000001.SZ"], "rank": [2]}).write_parquet(part / "part-001.parquet")

    assert _hot_rank_symbols(Config(data_root=root), date(2024, 6, 28), 2) == [
        "600519.SH",
        "000001.SZ",
    ]


def test_hot_rank_reader_filters_target_day_inside_month_partition(tmp_path):
    root = tmp_path / "data"
    part = root / "curated" / "hot_rank" / "trade_date=2024-06"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["000001.SZ", "600519.SH"],
            "trade_date": [date(2024, 6, 27), date(2024, 6, 28)],
            "rank": [1, 2],
        }
    ).write_parquet(part / "part-000.parquet")

    assert _hot_rank_symbols(Config(data_root=root), date(2024, 6, 28), 2) == ["600519.SH"]


def test_news_sentiment_universe_reads_daily_bars_inside_month_partition(tmp_path):
    root = tmp_path / "data"
    part = root / "curated" / "daily_bars" / "trade_date=2024-06"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["000001.SZ", "600519.SH"],
            "trade_date": [date(2024, 6, 28)] * 2,
            "amount": [100.0, 200.0],
            "volume": [100, 100],
        }
    ).write_parquet(part / "part-000.parquet")

    assert _news_sentiment_symbols(Config(data_root=root), date(2024, 6, 28), 1) == ["600519.SH"]


def test_news_sentiment_universe_ignores_zero_volume_amount_placeholder(tmp_path):
    root = tmp_path / "data"
    part = root / "curated" / "daily_bars" / "trade_date=2024-06-28"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "trade_date": [date(2024, 6, 28)] * 2,
            "amount": [100.0, 9_999_999.0],
            "volume": [100, 0],
        }
    ).write_parquet(part / "part-000.parquet")

    assert _news_sentiment_symbols(Config(data_root=root), date(2024, 6, 28), 1) == ["600519.SH"]


def test_batch_sentiment_http_fallback_when_no_headlines(news_batch_lake):
    news_payload = {
        "symbol": "600519.SH",
        "source": "eastmoney",
        "items": [{"title": "签约利好", "sentiment_score": 0.8}],
        "headline_count": 1,
        "aggregate_sentiment": 0.8,
    }
    with patch(
        "cnequity.derive.sentiment_scores.fetch_stock_news",
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
        "cnequity.derive.sentiment_scores.fetch_stock_news",
        side_effect=RuntimeError("boom"),
    ):
        df = compute_sentiment_scores(news_batch_lake, date(2024, 6, 28))

    assert "announcement_keywords" in set(df["score_channel"].to_list())
    assert "stock_news_nlp" not in set(df["score_channel"].to_list())


def test_stock_news_error_payload_counts_toward_breaker(tmp_path, monkeypatch):
    from cnequity.derive import sentiment_scores as scores

    cfg = Config(
        data_root=tmp_path / "data",
        sources={"eastmoney": True},
        sentiment_news_symbol_limit=6,
    )
    cfg.rate_limit = lambda _name: None
    symbols = [f"600{i:03d}.SH" for i in range(6)]
    monkeypatch.setattr(scores, "_news_sentiment_symbols", lambda *_args: symbols)

    class _Client:
        def close(self):
            return None

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.em_auth.EastMoneyClient",
        lambda **_kwargs: _Client(),
    )
    calls: list[str] = []

    def _error(symbol, **_kwargs):
        calls.append(symbol)
        return {"symbol": symbol, "headline_count": 0, "error": "offline"}

    monkeypatch.setattr(scores, "fetch_stock_news", _error)
    assert scores._stock_news_sentiment(cfg, date(2024, 6, 28)).is_empty()
    assert len(calls) == 5, "error payloads must trip the five-failure breaker"


def test_step_sentiment_scores_with_prior_event_run_data(tmp_path):
    from cnequity.steps.research import step_sentiment_scores
    from cnequity.storage.layout import init_data_layout

    root = tmp_path / "data"
    cfg = Config(data_root=root, sources={"eastmoney": True}, sentiment_use_snownlp=False)
    init_data_layout(cfg)

    trade_date = date(2024, 6, 28)

    # 1. Seed calendar
    cal_path = cfg.curated_root / "trading_calendar" / "part-merged.parquet"
    cal_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame([{"trade_date": trade_date, "is_trading": True}]).write_parquet(cal_path)

    # 2. Simulate prior corporate_events run having populated curated announcement_index
    ann_dir = cfg.curated_root / "announcement_index" / f"announce_date={trade_date.isoformat()}"
    ann_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "announcement_id": ["ann_101"],
            "symbol": ["600519.SH"],
            "title": ["贵州茅台净利润大增"],
            "announce_date": [trade_date],
            "category": ["年报"],
            "url": ["http://test"],
            "source": ["cninfo"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T18:00:00+00:00"],
        }
    ).write_parquet(ann_dir / "part-0.parquet")

    # 3. Simulate prior news_wire run having populated curated news_headlines
    news_dir = cfg.curated_root / "news_headlines" / f"publish_date={trade_date.isoformat()}"
    news_dir.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "news_id": ["news_201"],
            "publish_date": [trade_date],
            "publish_time": ["12:00:00"],
            "title": ["重大利好消息"],
            "summary": [""],
            "related_symbols": ["600519.SH"],
            "channel": ["fast_news"],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T18:00:00+00:00"],
        }
    ).write_parquet(news_dir / "part-0.parquet")

    # 4. Execute step_sentiment_scores during daily research pass
    run_id = "test-research-run"
    result = step_sentiment_scores(cfg, trade_date, run_id, {})

    assert result.get("status") != "failed"
    assert result.get("rows_written", 0) > 0

    # 5. Staged parquet should exist with both announcement and news score channels
    from cnequity.storage.parquet import StagingWriter

    staged = StagingWriter(cfg.staging_root).list_run_files("sentiment_scores", run_id)
    assert len(staged) > 0
    staged_df = pl.read_parquet(staged[0])
    assert "600519.SH" in staged_df["symbol"].to_list()
    channels = set(staged_df["score_channel"].to_list())
    assert "announcement_keywords" in channels
    assert "news_headlines" in channels

