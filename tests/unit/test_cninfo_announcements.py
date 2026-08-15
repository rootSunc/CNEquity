"""CNINFO announcement index — pagination, symbol filtering, dedupe."""

from __future__ import annotations

from datetime import date

import pytest

from cn_market_lake.adapters.cninfo.announcements import (
    _symbol_from_cninfo,
    fetch_announcement_index,
)


def test_symbol_from_cninfo_maps_exchange_prefixes():
    assert _symbol_from_cninfo("600519") == "600519.SH"
    assert _symbol_from_cninfo("000001") == "000001.SZ"
    assert _symbol_from_cninfo("920001") == "920001.BJ"


def test_symbol_from_cninfo_rejects_non_all_a_codes():
    assert _symbol_from_cninfo("810001") is None


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeClient:
    def __init__(self, pages: dict[str, list[dict]]):
        self.pages = pages
        self.calls: list[dict] = []
        self.closed = False

    def post(self, url, data):
        self.calls.append(data)
        column = data["column"]
        page = data["pageNum"]
        batches = self.pages.get(column, [])
        idx = page - 1
        batch = batches[idx] if idx < len(batches) else []
        has_more = idx + 1 < len(batches)
        return _FakeResponse({"announcements": batch, "hasMore": has_more})

    def close(self):
        self.closed = True


def test_fetch_announcement_index_paginates_and_dedupes(monkeypatch):
    pages = {
        "szse": [
            [
                {
                    "secCode": "000001",
                    "announcementId": "A1",
                    "announcementTitle": "半年报",
                    "announcementType": "定期报告",
                    "adjunctUrl": "/a1.pdf",
                }
            ],
            [
                {
                    "secCode": "000001",
                    "announcementId": "A1",
                    "announcementTitle": "半年报(更正)",
                    "announcementType": "定期报告",
                    "adjunctUrl": "/a1b.pdf",
                }
            ],
        ],
        "sse": [
            [
                {
                    "secCode": "600519",
                    "announcementId": "B1",
                    "announcementTitle": "分红公告",
                    "announcementType": "分红送配",
                    "adjunctUrl": "/b1.pdf",
                }
            ]
        ],
    }
    client = _FakeClient(pages)
    df = fetch_announcement_index(date(2024, 6, 28), client=client)
    assert client.closed is False  # caller owns the client, must not be closed
    assert df.height == 2  # A1 deduped keep-last, plus B1
    a1 = df.filter(df["symbol"] == "000001.SZ")
    assert a1["title"].to_list() == ["半年报(更正)"]
    assert set(df["symbol"].to_list()) == {"000001.SZ", "600519.SH"}
    assert len(client.calls) == 3  # szse page1, szse page2, sse page1


def test_fetch_announcement_index_owns_and_closes_default_client(monkeypatch):
    created: list[_FakeClient] = []

    def _factory(**kwargs):
        client = _FakeClient({"szse": [[]], "sse": [[]]})
        created.append(client)
        return client

    monkeypatch.setattr("cn_market_lake.adapters.cninfo.announcements.httpx.Client", _factory)
    df = fetch_announcement_index(date(2024, 6, 28))
    assert df.is_empty()
    assert created[0].closed is True


def test_fetch_announcement_index_skips_non_all_a_symbols():
    pages = {
        "szse": [
            [
                {
                    "secCode": "810001",
                    "announcementId": "C1",
                    "announcementTitle": "无效",
                    "adjunctUrl": "/c1.pdf",
                }
            ]
        ],
        "sse": [[]],
    }
    client = _FakeClient(pages)
    df = fetch_announcement_index(date(2024, 6, 28), client=client)
    assert df.is_empty()


def test_fetch_announcement_index_uses_rate_limiter_when_config_given():
    calls: list[str] = []

    class _Cfg:
        def rate_limit(self, source):
            calls.append(source)

    client = _FakeClient({"szse": [[]], "sse": [[]]})
    fetch_announcement_index(date(2024, 6, 28), client=client, config=_Cfg())
    assert calls == ["cninfo", "cninfo"]


class _OverrunClient:
    """Reproduces a measured live cninfo behavior: past its own reported
    ``totalpages``, the server keeps re-serving page 1's rows with
    ``hasMore`` still true, forever. Without a cap on ``totalpages`` itself,
    the pagination loop never exits — this is what actually happened in
    production (one day's szse column reached page 7727+ before an unrelated
    network blip finally killed the run)."""

    def __init__(self, total_pages: int):
        self.total_pages = total_pages
        self.calls = 0

    def post(self, url, data):
        self.calls += 1
        page = data["pageNum"]
        real_page = min(page, self.total_pages)
        item = {
            "secCode": "000001",
            "announcementId": f"P{real_page}",
            "announcementTitle": "x",
            "adjunctUrl": "/x.pdf",
        }
        if data["column"] == "sse":
            return _FakeResponse({"announcements": [], "hasMore": False, "totalpages": 0})
        return _FakeResponse(
            {"announcements": [item], "hasMore": True, "totalpages": self.total_pages}
        )

    def close(self):
        pass


def test_fetch_announcement_index_stops_at_totalpages_even_when_hasmore_lies():
    client = _OverrunClient(total_pages=3)
    df = fetch_announcement_index(date(2024, 1, 31), client=client)
    assert client.calls == 4  # szse pages 1..3, then sse's single (empty) page
    assert df.height == 3


def test_fetch_announcement_index_raises_runtime_error_on_transport_failure():
    class _BoomClient:
        def post(self, url, data):
            raise RuntimeError("network down")

        def close(self):
            pass

    with pytest.raises(RuntimeError, match="CNINFO announcement pagination failed"):
        fetch_announcement_index(date(2024, 6, 28), client=_BoomClient())
