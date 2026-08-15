from datetime import date

import polars as pl
import pytest

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.adapters.eastmoney.consensus import fetch_analyst_consensus
from cn_market_lake.adapters.eastmoney.institutional import (
    _quarter_end_dates,
    fetch_institutional_holdings,
)
from cn_market_lake.config import Config
from cn_market_lake.derive.sentiment_scores import compute_sentiment_scores
from cn_market_lake.domain.schemas import validate_dataframe
from cn_market_lake.orchestrator.registry import get_step
from cn_market_lake.query import load


class FakeDatacenterClient:
    def __init__(self, batches: dict[str, list[dict]]):
        self.batches = batches

    def get(self, url, **kwargs):
        class Resp:
            def __init__(self, data):
                self._data = data

            def raise_for_status(self):
                return None

            def json(self):
                return {"success": True, "result": {"data": self._data}}

        for key, rows in self.batches.items():
            if key in url:
                return Resp(rows)
        return Resp([])

    def close(self):
        return None


def test_p2_steps_registered():
    for name in ("institutional_holdings", "analyst_consensus", "sentiment_scores"):
        assert get_step(name).fn is not None


def test_quarter_end_dates_floor_is_2001_not_2016():
    """Measured 2026-08: RPT_MAIN_ORGHOLD returns real rows at 2001-12-31
    (1,276) — 2016 was an unverified guess, not a probed floor."""
    dates = _quarter_end_dates(date(2026, 7, 16))
    assert "2001-12-31" in dates
    assert "2000-12-31" not in dates


def test_institutional_holdings_parses():
    # Current RPT_MAIN_ORGHOLD schema: keyed by REPORT_DATE (no NOTICE_DATE),
    # HOULD_NUM / HOLD_VALUE / TOTALSHARES_RATIO, A-share via SECUCODE.
    client = FakeDatacenterClient(
        {
            "RPT_MAIN_ORGHOLD": [
                {
                    "SECURITY_CODE": "600519",
                    "SECUCODE": "600519.SH",
                    "REPORT_DATE": "2024-03-31",
                    "ORG_TYPE_NAME": "基金",
                    "HOULD_NUM": 120,
                    "TOTALSHARES_RATIO": 8.5,
                    "HOLD_VALUE": 1_000_000_000,
                },
                {  # NEEQ must be dropped
                    "SECURITY_CODE": "834948",
                    "SECUCODE": "834948.NQ",
                    "REPORT_DATE": "2024-03-31",
                    "ORG_TYPE_NAME": "基金",
                    "HOULD_NUM": 1,
                },
            ]
        }
    )
    df = fetch_institutional_holdings(date(2024, 4, 28), client=client)  # type: ignore[arg-type]
    assert df.height == 1
    assert df["symbol"][0] == "600519.SH"
    assert df["holder_type"][0] == "fund"
    assert df["report_period"][0] == "2024Q1"
    assert df["holding_ratio"][0] == 8.5


def test_analyst_consensus_parses():
    # Current RPT_WEB_RESPREDICT snapshot: EPS1/YEAR1, target = avg(min,max),
    # rating from the dominant RATING_*_NUM bucket, stamped forecast_date.
    client = FakeDatacenterClient(
        {
            "RPT_WEB_RESPREDICT": [
                {
                    "SECURITY_CODE": "600519",
                    "SECUCODE": "600519.SH",
                    "YEAR1": 2024,
                    "EPS1": 50.5,
                    "DEC_AIMPRICEMAX": 1900.0,
                    "DEC_AIMPRICEMIN": 1700.0,
                    "RATING_ORG_NUM": 12,
                    "RATING_BUY_NUM": 10,
                    "RATING_ADD_NUM": 2,
                }
            ]
        }
    )
    df = fetch_analyst_consensus(date(2024, 6, 28), client=client)  # type: ignore[arg-type]
    assert df.height == 1
    assert df["symbol"][0] == "600519.SH"
    assert df["eps_forecast"][0] == 50.5
    assert df["forecast_date"][0] == date(2024, 6, 28)
    assert df["target_price"][0] == 1800.0
    assert df["rating"][0] == "buy"
    assert df["analyst_count"][0] == 12


@pytest.fixture
def sentiment_lake(tmp_path):
    root = tmp_path / "data"
    part = root / "curated" / "announcement_index" / "announce_date=2024-06-28"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "announcement_id": ["a1", "a2", "a3"],
            "symbol": ["600519.SH", "600519.SH", "000001.SZ"],
            "title": ["业绩超预期增长", "分红方案公布", "日常经营公告"],
            "announce_date": [date(2024, 6, 28)] * 3,
            "category": ["", "", ""],
            "url": ["", "", ""],
            "source": ["cninfo"] * 3,
            "data_version": ["v1"] * 3,
            "fetched_at": ["2024-06-28T00:00:00+00:00"] * 3,
        }
    ).write_parquet(part / "part-0.parquet")
    return Config(data_root=root)


def test_sentiment_scores_from_announcements(sentiment_lake):
    df = compute_sentiment_scores(sentiment_lake, date(2024, 6, 28))
    ann = df.filter(pl.col("score_channel") == "announcement_keywords")
    assert ann.height == 2
    moutai = ann.filter(pl.col("symbol") == "600519.SH")
    assert moutai["sentiment_score"][0] > 0
    assert moutai["headline_count"][0] == 2


def test_sentiment_scores_schema(sentiment_lake):
    raw = (
        compute_sentiment_scores(sentiment_lake, date(2024, 6, 28))
        .filter(pl.col("score_channel") == "announcement_keywords")
        .with_columns(
            source=pl.lit("derived"),
            data_version=pl.lit("v1"),
            fetched_at=pl.lit("2024-06-28T00:00:00+00:00"),
        )
    )
    out = validate_dataframe(raw, "sentiment_scores")
    assert out.height == 2


def test_load_analyst_consensus(tmp_path):
    root = tmp_path / "data"
    part = root / "curated" / "analyst_consensus" / "forecast_date=2024-06-28"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "forecast_date": [date(2024, 6, 28)],
            "forecast_year": [2024],
            "eps_forecast": [50.5],
            "pe_forecast": [25.0],
            "target_price": [1800.0],
            "rating": ["买入"],
            "analyst_count": [12],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    ).write_parquet(part / "part-0.parquet")
    cfg = Config(data_root=root)
    df = load("analyst_consensus", start="2024-06-28", end="2024-06-28", config=cfg)
    assert df.height == 1


# AkShare is a retired package now; `test_tdx_decoupling.py` guards both the
# import ban and the pyproject declaration for the whole retired set.
