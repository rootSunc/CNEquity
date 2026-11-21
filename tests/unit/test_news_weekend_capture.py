"""Calendar news snapshots must still reach the source after Friday is covered."""

from datetime import date

import httpx
import polars as pl
import pytest
from tests.coverage_fakes import covered

from cnequity.config import Config
from cnequity.steps.common import fetch_incremental_daily
from cnequity.steps.rotation import step_news_headlines


@pytest.mark.parametrize("dataset", ["news_headlines", "flash_news_wire"])
@pytest.mark.parametrize("day", [date(2024, 6, 29), date(2024, 6, 30)])
def test_calendar_snapshot_fetches_weekend_without_inventing_gap_dates(
    tmp_path, dataset, day, monkeypatch
):
    config = Config(data_root=tmp_path / "lake")
    covered(monkeypatch, dataset, date(2024, 6, 28))
    fetched = []

    def fetch(requested):
        fetched.append(requested)
        return pl.DataFrame({"news_id": ["wire-1"], "publish_date": [requested]})

    frame, findings = fetch_incremental_daily(config, dataset, day, fetch, date_col="publish_date")
    assert fetched == [day]
    assert frame["publish_date"].to_list() == [day]
    assert findings == []


def test_weekend_empty_news_has_real_wire_capture(tmp_path, monkeypatch):
    config = Config(data_root=tmp_path / "lake")
    covered(monkeypatch, "news_headlines", date(2024, 6, 28))
    requests = []

    class Client:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def get(self, url, **kwargs):
            requests.append(url)
            return httpx.Response(
                200,
                request=httpx.Request("GET", url),
                json={"data": {"fastNewsList": [], "sortEnd": ""}},
            )

    monkeypatch.setattr("cnequity.adapters.eastmoney.news_feed.EastMoneyClient", Client)
    result = step_news_headlines(config, date(2024, 6, 30), "weekend-empty", {})
    assert len(requests) == 1
    assert result["rows_written"] == 0
    # Empty stages nothing, so no published row can move the watermark past Friday.
    assert not list((config.staging_root / "news_headlines").rglob("*.parquet"))


def test_trading_snapshot_still_skips_closed_day(tmp_path, monkeypatch):
    config = Config(data_root=tmp_path / "lake")
    covered(monkeypatch, "fund_flow", date(2024, 6, 28))

    def unexpected_fetch(day):
        pytest.fail("a trading-session snapshot must not fetch a closed day")

    frame, findings = fetch_incremental_daily(
        config, "fund_flow", date(2024, 6, 30), unexpected_fetch
    )
    assert frame.is_empty()
    assert findings == []
