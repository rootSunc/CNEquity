from datetime import date

import polars as pl
import pytest

from cn_market_lake.adapters.tdx_protocol.bars import (
    TdxBarsPaginationError,
    fetch_bars_paginated,
)


class FakeClient:
    def __init__(self, pages: list[list[dict]]):
        self.pages = pages
        self.calls = 0

    def bars(self, symbol, frequency, market, start, offset):
        if self.calls >= len(self.pages):
            return None
        page = self.pages[self.calls]
        self.calls += 1
        return pl.DataFrame(page)


def test_paginated_fetch_stops_when_page_older_than_start():
    page1 = [
        {
            "datetime": date(2024, 6, 27),
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
            "amount": 1,
        }
    ] + [
        {
            "datetime": date(2020, 1, 2),
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
            "amount": 1,
        }
    ] * 799
    client = FakeClient([page1, []])
    rows = fetch_bars_paginated(client, "600519.SH", date(2024, 6, 26), date(2024, 6, 27))
    assert len(rows) == 1
    assert client.calls == 1


def test_paginated_fetch_merges_pages_when_window_spans_pages():
    page1 = [
        {
            "datetime": date(2024, 6, 27),
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
            "amount": 1,
        }
    ] + [
        {
            "datetime": date(2024, 6, 26),
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
            "amount": 1,
        }
    ] * 799
    pages = [
        page1,
        [
            {
                "datetime": date(2024, 6, 25),
                "open": 2,
                "high": 2,
                "low": 2,
                "close": 2,
                "volume": 2,
                "amount": 2,
            }
        ],
        [],
    ]
    client = FakeClient(pages)
    rows = fetch_bars_paginated(client, "600519.SH", date(2024, 6, 25), date(2024, 6, 27))
    assert len(rows) == 3
    assert client.calls == 2


class FailOnSecondPageClient:
    def __init__(self, pages: list[list[dict]]):
        self.pages = pages
        self.calls = 0

    def bars(self, symbol, frequency, market, start, offset):
        self.calls += 1
        if self.calls == 2:
            raise RuntimeError("network jitter")
        if self.calls > len(self.pages):
            return None
        return pl.DataFrame(self.pages[self.calls - 1])


def test_backfill_mid_page_failure_raises():
    page1 = [
        {
            "datetime": date(2024, 6, 27),
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
            "amount": 1,
        }
    ] * 800
    page2 = [
        {
            "datetime": date(2016, 1, 4),
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
            "amount": 1,
        }
    ]
    client = FailOnSecondPageClient([page1, page2])
    with pytest.raises(TdxBarsPaginationError, match="start=800"):
        fetch_bars_paginated(
            client, "600519.SH", date(2016, 1, 1), date(2024, 6, 27), backfill=True
        )


def test_incremental_mid_page_failure_returns_partial():
    page1 = [
        {
            "datetime": date(2024, 6, 27),
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
            "amount": 1,
        },
        {
            "datetime": date(2024, 6, 26),
            "open": 1,
            "high": 1,
            "low": 1,
            "close": 1,
            "volume": 1,
            "amount": 1,
        },
    ] * 400
    page2 = [
        {
            "datetime": date(2024, 6, 25),
            "open": 2,
            "high": 2,
            "low": 2,
            "close": 2,
            "volume": 2,
            "amount": 2,
        }
    ]
    client = FailOnSecondPageClient([page1, page2])
    rows = fetch_bars_paginated(
        client, "600519.SH", date(2024, 6, 25), date(2024, 6, 27), backfill=False
    )
    assert len(rows) == 2
    assert {r["trade_date"] for r in rows} == {date(2024, 6, 26), date(2024, 6, 27)}
    assert client.calls == 2


class FakeIndexClient:
    """Records which method (bars vs index) was called."""

    def __init__(self, page: list[dict]):
        self.page = page
        self.used_index = False
        self.used_bars = False

    def bars(self, symbol, frequency, market, start, offset):
        self.used_bars = True
        return pl.DataFrame(self.page) if start == 0 else pl.DataFrame([])

    def index(self, symbol, frequency, start, offset):
        self.used_index = True
        return pl.DataFrame(self.page) if start == 0 else pl.DataFrame([])


def _bar(d):
    return {"datetime": d, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "amount": 1}


def test_index_symbol_uses_index_method_not_bars():
    client = FakeIndexClient([_bar(date(2024, 6, 27))])
    rows = fetch_bars_paginated(
        client, "399001.SZ", date(2024, 6, 1), date(2024, 6, 27), is_index=True
    )
    assert rows and rows[0]["symbol"] == "399001.SZ"
    assert client.used_index is True
    assert client.used_bars is False


def test_stock_symbol_uses_bars_method():
    client = FakeIndexClient([_bar(date(2024, 6, 27))])
    fetch_bars_paginated(client, "600519.SH", date(2024, 6, 1), date(2024, 6, 27))
    assert client.used_bars is True
    assert client.used_index is False
