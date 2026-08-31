"""CNINFO announcement index — pagination, symbol filtering, dedupe."""

from __future__ import annotations

from datetime import date

import pytest

from cnequity.adapters.cninfo.announcements import (
    _symbol_from_cninfo,
    fetch_announcement_index,
)
from cnequity.adapters.cninfo.regulatory import fetch_regulatory_events


def test_symbol_from_cninfo_maps_exchange_prefixes():
    assert _symbol_from_cninfo("600519") == "600519.SH"
    assert _symbol_from_cninfo("000001") == "000001.SZ"
    assert _symbol_from_cninfo("920001") == "920001.BJ"
    assert _symbol_from_cninfo("830001") == "830001.BJ"


def test_symbol_from_cninfo_rejects_non_all_a_codes():
    assert _symbol_from_cninfo("810001") is None


def test_symbol_from_cninfo_rejects_malformed_codes():
    assert _symbol_from_cninfo("abc") is None
    assert _symbol_from_cninfo("0000001") is None


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
                },
                {
                    "secCode": "600519",
                    "announcementId": "B1",
                    "announcementTitle": "分红公告",
                    "announcementType": "分红送配",
                    "adjunctUrl": "/b1.pdf",
                },
            ],
        ],
    }
    client = _FakeClient(pages)
    df = fetch_announcement_index(date(2024, 6, 28), client=client)
    assert client.closed is False  # caller owns the client, must not be closed
    assert df.height == 2  # A1 deduped keep-last, plus B1
    a1 = df.filter(df["symbol"] == "000001.SZ")
    assert a1["title"].to_list() == ["半年报(更正)"]
    assert set(df["symbol"].to_list()) == {"000001.SZ", "600519.SH"}
    # One walk, not one per exchange: `column` selects a site section, and
    # every value of it answers with the same all-market result.
    assert len(client.calls) == 2
    assert {call["column"] for call in client.calls} == {"szse"}


def test_cninfo_rejects_a_row_from_a_different_announcement_date():
    pages = {
        "szse": [
            [
                {
                    "secCode": "000001",
                    "announcementId": "A1",
                    "announcementTitle": "跨日公告",
                    "announcementTime": "2024-06-27 23:59:59",
                }
            ]
        ],
        "sse": [[]],
    }

    with pytest.raises(RuntimeError, match="does not match requested 2024-06-28"):
        fetch_announcement_index(date(2024, 6, 28), client=_FakeClient(pages))

    with pytest.raises(RuntimeError, match="does not match requested 2024-06-28"):
        fetch_regulatory_events(date(2024, 6, 28), client=_FakeClient(pages))


def test_cninfo_accepts_the_live_endpoint_epoch_millisecond_announcement_time():
    """announcementTime is a Unix millisecond epoch on the real endpoint.

    Confirmed live against https://www.cninfo.com.cn/new/hisAnnouncement/query:
    the field is an int, not an ISO date string - only hand-written fixtures
    used the string form, so this shape went untested before.
    """
    pages = {
        "szse": [
            [
                {
                    "secCode": "000001",
                    "announcementId": "A1",
                    "announcementTitle": "epoch公告",
                    "announcementTime": 1719559800000,  # 2024-06-28 15:30 CST
                }
            ]
        ],
        "sse": [[]],
    }

    df = fetch_announcement_index(date(2024, 6, 28), client=_FakeClient(pages))
    assert df.height == 1


def test_cninfo_rejects_an_epoch_millisecond_row_from_a_different_date():
    pages = {
        "szse": [
            [
                {
                    "secCode": "000001",
                    "announcementId": "A1",
                    "announcementTitle": "epoch公告",
                    "announcementTime": 1719559800000,  # 2024-06-28, not 2024-06-27
                }
            ]
        ],
        "sse": [[]],
    }

    with pytest.raises(RuntimeError, match="does not match requested 2024-06-27"):
        fetch_announcement_index(date(2024, 6, 27), client=_FakeClient(pages))


def test_fetch_announcement_index_owns_and_closes_default_client(monkeypatch):
    created: list[_FakeClient] = []

    def _factory(**kwargs):
        client = _FakeClient({"szse": [[]], "sse": [[]]})
        created.append(client)
        return client

    monkeypatch.setattr("cnequity.adapters.cninfo.announcements.httpx.Client", _factory)
    df = fetch_announcement_index(date(2024, 6, 28))
    assert df.is_empty()
    assert created[0].closed is True


def test_fetch_announcement_index_closes_owned_client_on_failure(monkeypatch):
    created: list[_FakeClient] = []

    def _factory(**kwargs):
        client = _FakeClient({"szse": [[]], "sse": [[]]})
        created.append(client)
        return client

    monkeypatch.setattr("cnequity.adapters.cninfo.announcements.httpx.Client", _factory)
    monkeypatch.setattr(
        "cnequity.adapters.cninfo.announcements.post_with_retry",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("cninfo down")),
    )
    with pytest.raises(RuntimeError, match="pagination failed"):
        fetch_announcement_index(date(2024, 6, 28))
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


def test_fetch_announcement_index_skips_rows_without_stable_identity():
    pages = {
        "szse": [
            [
                {
                    "secCode": "000001",
                    "announcementTitle": "无法定位",
                    "announcementType": "其他",
                },
                {
                    "secCode": "000001",
                    "announcementId": "A1",
                    "announcementTitle": "可定位",
                    "announcementType": "其他",
                },
            ]
        ],
        "sse": [[]],
    }
    df = fetch_announcement_index(date(2024, 6, 28), client=_FakeClient(pages))
    assert df["announcement_id"].to_list() == ["A1"]


def test_cninfo_skips_non_object_rows_and_keeps_valid_rows():
    pages = {
        "szse": [
            [
                None,
                {
                    "secCode": "000001",
                    "announcementId": "A1",
                    "announcementTitle": "有效公告",
                },
            ]
        ],
        "sse": [[]],
    }
    df = fetch_announcement_index(date(2024, 6, 28), client=_FakeClient(pages))
    assert df["announcement_id"].to_list() == ["A1"]


def test_cninfo_rejects_non_list_announcement_batch():
    class MalformedClient:
        def post(self, url, data):
            return _FakeResponse({"announcements": {"announcementId": "A1"}})

        def close(self):
            pass

    with pytest.raises(RuntimeError, match="announcements.*is not a list"):
        fetch_announcement_index(date(2024, 6, 28), client=MalformedClient())


def test_regulatory_skips_rows_without_stable_identity():
    from cnequity.adapters.cninfo.regulatory import fetch_regulatory_events

    pages = {
        "szse": [
            [
                {
                    "secCode": "000001",
                    "announcementTitle": "行政处罚公告",
                },
                {
                    "secCode": "000001",
                    "announcementId": "R1",
                    "announcementTitle": "行政处罚公告",
                },
            ]
        ],
        "sse": [[]],
    }
    df = fetch_regulatory_events(date(2024, 6, 28), client=_FakeClient(pages))
    assert df["event_id"].to_list() == ["reg-R1"]


def test_regulatory_skips_non_object_rows_and_keeps_valid_rows():
    pages = {
        "szse": [
            [
                None,
                {
                    "secCode": "000001",
                    "announcementId": "R1",
                    "announcementTitle": "行政处罚公告",
                },
            ]
        ],
        "sse": [[]],
    }
    df = fetch_regulatory_events(date(2024, 6, 28), client=_FakeClient(pages))
    assert df["event_id"].to_list() == ["reg-R1"]


def test_fetch_announcement_index_uses_rate_limiter_when_config_given():
    calls: list[str] = []

    class _Cfg:
        def rate_limit(self, source):
            calls.append(source)

    client = _FakeClient({"szse": [[]], "sse": [[]]})
    fetch_announcement_index(date(2024, 6, 28), client=client, config=_Cfg())
    assert calls == ["cninfo"]


class _OverrunClient:
    """Reproduces a measured live cninfo behavior: past its own reported
    ``totalpages``, the server keeps re-serving page 1's rows with
    ``hasMore`` still true, forever. Without a cap on ``totalpages`` itself,
    the pagination loop never exits — this is what actually happened in
    production (one day's szse column reached page 7727+ before an unrelated
    network blip finally killed the run)."""

    def __init__(self, total_pages: int, reported_total_pages=None):
        self.total_pages = total_pages
        self.reported_total_pages = reported_total_pages
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
        reported_total_pages = (
            self.total_pages if self.reported_total_pages is None else self.reported_total_pages
        )
        return _FakeResponse(
            {"announcements": [item], "hasMore": True, "totalpages": reported_total_pages}
        )

    def close(self):
        pass


def test_fetch_announcement_index_stops_at_totalpages_even_when_hasmore_lies():
    client = _OverrunClient(total_pages=3)
    df = fetch_announcement_index(date(2024, 1, 31), client=client)
    assert client.calls == 3  # pages 1..3
    assert df.height == 3


def test_fetch_announcement_index_rejects_a_repeated_page():
    class RepeatedPageClient:
        def __init__(self):
            self.calls = 0

        def post(self, url, data):
            self.calls += 1
            if data["column"] == "sse":
                return _FakeResponse({"announcements": [], "hasMore": False})
            return _FakeResponse(
                {
                    "announcements": [
                        {
                            "secCode": "000001",
                            "announcementId": "same",
                            "announcementTitle": "公告",
                        }
                    ],
                    "hasMore": True,
                }
            )

    client = RepeatedPageClient()
    with pytest.raises(RuntimeError, match="announcement.*repeated page"):
        fetch_announcement_index(date(2024, 1, 31), client=client)
    assert client.calls == 2


@pytest.mark.parametrize(
    ("fetch", "title"),
    [
        (fetch_announcement_index, "公告"),
        (fetch_regulatory_events, "行政处罚决定"),
    ],
)
def test_cninfo_fetchers_continue_after_a_full_page_without_pagination_metadata(fetch, title):
    class NoPaginationMetadataClient:
        def __init__(self):
            self.calls = 0

        def post(self, url, data):
            self.calls += 1
            if data["column"] == "sse":
                return _FakeResponse({"announcements": []})
            page = data["pageNum"]
            if page == 1:
                batch = [
                    {
                        "secCode": "000001",
                        "announcementId": f"full-{index}",
                        "announcementTitle": title,
                    }
                    for index in range(30)
                ]
            elif page == 2:
                batch = [
                    {
                        "secCode": "000001",
                        "announcementId": "tail",
                        "announcementTitle": title,
                    }
                ]
            else:
                batch = []
            return _FakeResponse({"announcements": batch})

    client = NoPaginationMetadataClient()
    df = fetch(date(2024, 1, 31), client=client)
    assert df.height == 31
    assert client.calls == 2  # a short page ends the walk without metadata


def test_fetch_announcement_index_rejects_empty_page_before_totalpages():
    class EmptyPageClient:
        def post(self, url, data):
            if data["pageNum"] == 1:
                return _FakeResponse(
                    {
                        "announcements": [
                            {
                                "secCode": "000001",
                                "announcementId": "P1",
                                "announcementTitle": "公告",
                            }
                        ],
                        "hasMore": True,
                        "totalpages": 2,
                    }
                )
            return _FakeResponse({"announcements": [], "hasMore": False, "totalpages": 2})

        def close(self):
            pass

    with pytest.raises(RuntimeError, match="empty page before the reported end"):
        fetch_announcement_index(date(2024, 1, 31), client=EmptyPageClient())


def test_fetch_announcement_index_uses_totalpages_when_hasmore_is_false():
    class StaleHasMoreClient:
        def __init__(self):
            self.calls = 0

        def post(self, url, data):
            self.calls += 1
            if data["column"] == "sse":
                return _FakeResponse({"announcements": [], "hasMore": False, "totalpages": 0})
            item = {
                "secCode": "000001",
                "announcementId": f"P{data['pageNum']}",
                "announcementTitle": "公告",
            }
            return _FakeResponse({"announcements": [item], "hasMore": False, "totalpages": 2})

    client = StaleHasMoreClient()
    df = fetch_announcement_index(date(2024, 1, 31), client=client)
    assert client.calls == 2
    assert set(df["announcement_id"].to_list()) == {"P1", "P2"}


@pytest.mark.parametrize("total_pages", ["3", " 3 "])
def test_fetch_announcement_index_accepts_string_totalpages(total_pages):
    client = _OverrunClient(total_pages=3, reported_total_pages=total_pages)
    df = fetch_announcement_index(date(2024, 1, 31), client=client)
    assert client.calls == 3
    assert df.height == 3


def test_fetch_announcement_index_normalizes_string_hasmore():
    class StringMetaClient(_FakeClient):
        def post(self, url, data):
            response = super().post(url, data)
            payload = response.json()
            payload["hasMore"] = "true" if payload["hasMore"] else "false"
            return _FakeResponse(payload)

    client = StringMetaClient(
        {
            "szse": [
                [
                    {
                        "secCode": "000001",
                        "announcementId": "A1",
                        "announcementTitle": "公告",
                    }
                ],
                [],
            ],
            "sse": [[]],
        }
    )
    df = fetch_announcement_index(date(2024, 1, 31), client=client)
    assert len(client.calls) == 2
    assert df.height == 1


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("totalpages", 1.5, "totalpages.*non-negative integer"),
        ("totalpages", True, "totalpages.*non-negative integer"),
        ("hasMore", "sometimes", "hasMore.*boolean"),
    ],
)
def test_fetch_announcement_index_rejects_malformed_pagination_metadata(field, value, message):
    class MalformedMetaClient:
        def post(self, url, data):
            return _FakeResponse(
                {
                    "announcements": [
                        {
                            "secCode": "000001",
                            "announcementId": "A1",
                            "announcementTitle": "公告",
                        }
                    ],
                    "hasMore": False,
                    "totalpages": 1,
                    field: value,
                }
            )

        def close(self):
            pass

    with pytest.raises(RuntimeError, match=message):
        fetch_announcement_index(date(2024, 1, 31), client=MalformedMetaClient())


def test_fetch_announcement_index_raises_runtime_error_on_transport_failure(monkeypatch):
    monkeypatch.setattr("cnequity.adapters.cninfo.announcements.time.sleep", lambda *_: None)

    class _BoomClient:
        def post(self, url, data):
            raise RuntimeError("network down")

        def close(self):
            pass

    with pytest.raises(RuntimeError, match="CNINFO announcement pagination failed"):
        fetch_announcement_index(date(2024, 6, 28), client=_BoomClient())


def test_totalpages_zero_is_a_short_page_not_a_contradiction():
    """CNINFO reports floor(records / 30), so a small bucket says zero pages.

    Measured on the live endpoint: a `category` bucket holding 25 rows answers
    `totalpages=0`, and 2026-01-01's 1375 rows answer `totalpages=45` with the
    last 25 on page 46. Treating the count as a page total drops the tail of
    almost every day; treating zero as impossible fails every small bucket.
    """

    class ShortPageClient:
        def __init__(self):
            self.calls = 0

        def post(self, url, data):
            self.calls += 1
            rows = [
                {
                    "secCode": "000001",
                    "announcementId": f"A{index}",
                    "announcementTitle": "公告",
                }
                for index in range(25)
            ]
            return _FakeResponse(
                {
                    "announcements": rows if data["pageNum"] == 1 else [],
                    "totalRecordNum": 25,
                    "totalpages": 0,
                    "hasMore": False,
                }
            )

        def close(self):
            pass

    client = ShortPageClient()
    df = fetch_announcement_index(date(2024, 1, 31), client=client)
    assert df.height == 25
    assert client.calls == 1


def test_a_partial_last_page_is_still_read():
    """The walk follows the record total past the reported page count."""

    class UnderReportedPagesClient:
        def __init__(self):
            self.calls = 0

        def post(self, url, data):
            self.calls += 1
            page = data["pageNum"]
            if page == 1:
                rows = [
                    {
                        "secCode": "000001",
                        "announcementId": f"A{index}",
                        "announcementTitle": "公告",
                    }
                    for index in range(30)
                ]
            elif page == 2:
                rows = [
                    {"secCode": "000001", "announcementId": "tail", "announcementTitle": "公告"}
                ]
            else:
                rows = []
            return _FakeResponse(
                {
                    "announcements": rows,
                    "totalRecordNum": 31,
                    "totalpages": 1,  # floor(31 / 30)
                    "hasMore": page == 1,
                }
            )

        def close(self):
            pass

    client = UnderReportedPagesClient()
    df = fetch_announcement_index(date(2024, 1, 31), client=client)
    assert client.calls == 2
    assert df.height == 31
    assert "tail" in df["announcement_id"].to_list()
