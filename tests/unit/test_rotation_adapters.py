"""Unit tests for EastMoney rotation adapters."""

from datetime import date
from unittest.mock import MagicMock

from cn_market_lake.adapters.eastmoney.rotation import (
    _hot_symbol,
    fetch_hot_rank,
    fetch_news_headlines,
)


def test_hot_symbol_parsing():
    assert _hot_symbol("SZ002185") == "002185.SZ"
    assert _hot_symbol("SH600519") == "600519.SH"


def test_fetch_hot_rank_normalizes(monkeypatch):
    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "data": [{"sc": "SZ002185", "rk": 1, "rc": 2, "hisRc": 3}],
    }
    mock_client.post.return_value = mock_resp

    class CM:
        def __enter__(self):
            return mock_client

        def __exit__(self, *a):
            pass

    monkeypatch.setattr("cn_market_lake.adapters.eastmoney.rotation.EastMoneyClient", lambda: CM())
    df = fetch_hot_rank(date(2026, 7, 14), top_n=10)
    assert df.height == 1
    assert df["symbol"][0] == "002185.SZ"
    assert df["rank"][0] == 1


def test_fetch_news_headlines_filters_date(monkeypatch):
    mock_client = MagicMock()
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "data": {
            "fastNewsList": [
                {
                    "code": "n1",
                    "showTime": "2026-07-14 16:00:00",
                    "title": "测试新闻",
                    "summary": "摘要",
                    "stockList": ["0.600519"],
                },
                {
                    "code": "n2",
                    "showTime": "2026-07-13 16:00:00",
                    "title": "旧新闻",
                },
            ]
        }
    }
    mock_client.get.return_value = mock_resp

    class CM:
        def __enter__(self):
            return mock_client

        def __exit__(self, *a):
            pass

    monkeypatch.setattr("cn_market_lake.adapters.eastmoney.rotation.EastMoneyClient", lambda: CM())
    df = fetch_news_headlines(date(2026, 7, 14))
    assert df.height == 1
    assert df["news_id"][0] == "n1"
    assert "600519.SH" in df["related_symbols"][0]
