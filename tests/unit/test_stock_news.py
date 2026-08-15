from datetime import date

from cn_market_lake.adapters.eastmoney.stock_news import fetch_stock_news
from cn_market_lake.config import Config
from cn_market_lake.domain.sentiment import aggregate_scores, keyword_score, score_text
from cn_market_lake.query.on_demand import OnDemandService


class FakeNewsClient:
    def __init__(self, items: list[dict]):
        self.items = items

    def get(self, url, **kwargs):
        class Resp:
            def __init__(self, data):
                self._data = data

            def raise_for_status(self):
                return None

            def json(self):
                return {"data": {"list": self._data}}

        return Resp(self.items)

    def close(self):
        return None


def test_keyword_score_positive():
    score, method = score_text("业绩超预期增长", use_snownlp=False)
    assert score > 0
    assert method == "keyword"


def test_keyword_score_negative():
    assert keyword_score("收到行政处罚决定书") < 0


def test_aggregate_scores():
    assert aggregate_scores([1.0, -1.0]) == 0.0


def test_fetch_stock_news_parses_and_scores():
    client = FakeNewsClient(
        [
            {
                "title": "公司签订重大合同利好",
                "showtime": "2024-06-28 15:00:00",
                "art_code": "n1",
                "url": "https://example.com/1",
            },
            {
                "title": "日常经营简报",
                "showtime": "2024-06-27 10:00:00",
                "art_code": "n2",
            },
        ]
    )
    payload = fetch_stock_news(
        "600519.SH",
        on_date=date(2024, 6, 28),
        use_snownlp=False,
        client=client,  # type: ignore[arg-type]
    )
    assert payload["headline_count"] == 1
    assert payload["items"][0]["sentiment_method"] == "keyword"
    assert payload["aggregate_sentiment"] > 0


def test_on_demand_stock_news_caches(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        on_demand_datasets=["stock_news"],
        sources={"eastmoney": True},
    )

    class StubService(OnDemandService):
        def _fetch_remote(self, dataset, symbol, **kwargs):
            return {
                "symbol": symbol,
                "source": "eastmoney",
                "items": [{"news_id": "1", "title": "分红方案公布", "sentiment_score": 0.5}],
                "headline_count": 1,
                "aggregate_sentiment": 0.5,
                "data_version": "v1",
                "fetched_at": "2024-06-28T00:00:00+00:00",
            }

    svc = StubService(cfg)
    first = svc.fetch("stock_news", "600519.SH")
    second = svc.fetch("stock_news", "600519.SH")
    assert first == second
    assert (cfg.meta_root / "on_demand" / "stock_news" / "600519_SH.json").exists()
