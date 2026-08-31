"""Paginated snapshot fetches must fail loud, never silently truncate."""

from datetime import date

import pytest

from cnequity.adapters.cninfo import announcements as cninfo_announcements
from cnequity.adapters.cninfo.regulatory import fetch_regulatory_events
from cnequity.adapters.eastmoney import clist
from cnequity.adapters.eastmoney.clist import fetch_clist_pages


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _clist_payload(codes, total):
    return {"data": {"total": total, "diff": [{"f12": c, "f13": 1} for c in codes]}}


def test_clist_raises_when_all_hosts_fail(monkeypatch):
    monkeypatch.setattr(clist.time, "sleep", lambda *_: None)

    class AllFail:
        def get(self, url, **kwargs):
            raise RuntimeError("connection reset")

    with pytest.raises(RuntimeError, match="failed on all hosts"):
        fetch_clist_pages(AllFail(), fields="f12,f13")


def test_clist_raises_on_midpagination_truncation(monkeypatch):
    monkeypatch.setattr(clist.time, "sleep", lambda *_: None)

    class FailSecondPage:
        def __init__(self):
            self.calls = 0

        def get(self, url, **kwargs):
            self.calls += 1
            # page 1 returns a full page (total forces a second page); page 2 dies.
            if "pn=2" in url or self.calls > 1:
                raise RuntimeError("read timeout on page 2")
            return _Resp(_clist_payload([f"{600000 + i}" for i in range(5000)], total=10000))

    with pytest.raises(RuntimeError, match="page 2 failed"):
        fetch_clist_pages(FailSecondPage(), fields="f12,f13", page_size=5000)


def test_clist_raises_on_empty_page_before_reported_total():
    class EmptySecondPage:
        def get(self, url, **kwargs):
            if "pn=1" in url:
                return _Resp(_clist_payload(["600000", "600001"], total=4))
            return _Resp(_clist_payload([], total=4))

    with pytest.raises(RuntimeError, match="empty before reported total"):
        fetch_clist_pages(EmptySecondPage(), fields="f12,f13", page_size=2)


def test_clist_raises_on_short_page_before_reported_total():
    class ShortFirstPage:
        def get(self, url, **kwargs):
            if "pn=1" in url:
                return _Resp(_clist_payload(["600000"], total=3))
            return _Resp(_clist_payload(["600001", "600002"], total=3))

    with pytest.raises(RuntimeError, match="potentially truncated result"):
        fetch_clist_pages(ShortFirstPage(), fields="f12,f13", page_size=2)


def test_clist_deduplicates_boundary_rows_before_counting_total():
    class RepeatedBoundary:
        def get(self, url, **kwargs):
            if "pn=1" in url:
                return _Resp(_clist_payload(["600000", "600001"], total=3))
            return _Resp(_clist_payload(["600001", "600002"], total=3))

    rows = fetch_clist_pages(RepeatedBoundary(), fields="f12,f13", page_size=2)

    assert [row["f12"] for row in rows] == ["600000", "600001", "600002"]


def test_clist_rejects_a_full_repeated_page_before_looping():
    class RepeatedFullPage:
        def get(self, url, **kwargs):
            return _Resp(_clist_payload(["600000", "600001"], total=4))

    with pytest.raises(RuntimeError, match="pagination did not advance"):
        fetch_clist_pages(RepeatedFullPage(), fields="f12,f13", page_size=2)


def test_clist_rejects_success_without_data_object():
    class Malformed:
        def get(self, url, **kwargs):
            return _Resp({"success": True})

    with pytest.raises(RuntimeError, match="no data object"):
        clist._fetch_clist_page(
            Malformed(),
            host="https://push2.eastmoney.com",
            fields="f12,f13",
            fs="m:1",
            page=1,
            page_size=10,
            max_retries=1,
            retry_backoff_seconds=0,
        )


def test_clist_rejects_non_list_diff():
    class Malformed:
        def get(self, url, **kwargs):
            return _Resp({"success": True, "data": {"total": 0, "diff": {}}})

    with pytest.raises(RuntimeError, match="diff is not a list"):
        clist._fetch_clist_page(
            Malformed(),
            host="https://push2.eastmoney.com",
            fields="f12,f13",
            fs="m:1",
            page=1,
            page_size=10,
            max_retries=1,
            retry_backoff_seconds=0,
        )


def test_clist_rejects_non_object_diff_rows():
    class Malformed:
        def get(self, url, **kwargs):
            return _Resp({"success": True, "data": {"total": 1, "diff": [None]}})

    with pytest.raises(RuntimeError, match="contains a non-object row"):
        clist._fetch_clist_page(
            Malformed(),
            host="https://push2.eastmoney.com",
            fields="f12,f13",
            fs="m:1",
            page=1,
            page_size=10,
            max_retries=1,
            retry_backoff_seconds=0,
        )


def test_clist_rejects_non_integer_total():
    class Malformed:
        def get(self, url, **kwargs):
            return _Resp({"success": True, "data": {"total": 1.5, "diff": []}})

    with pytest.raises(RuntimeError, match="total is not a non-negative integer"):
        clist._fetch_clist_page(
            Malformed(),
            host="https://push2.eastmoney.com",
            fields="f12,f13",
            fs="m:1",
            page=1,
            page_size=10,
            max_retries=1,
            retry_backoff_seconds=0,
        )


def test_clist_rejects_missing_total_when_rows_are_present():
    class MissingTotal:
        def get(self, url, **kwargs):
            return _Resp({"data": {"diff": [{"f12": "600519", "f13": 1}]}})

    with pytest.raises(RuntimeError, match="without a reported total"):
        clist._fetch_clist_page(
            MissingTotal(),
            host="https://push2.eastmoney.com",
            fields="f12,f13",
            fs="m:1",
            page=1,
            page_size=10,
            max_retries=1,
            retry_backoff_seconds=0,
        )


def test_regulatory_raises_on_page_failure(monkeypatch):
    monkeypatch.setattr(cninfo_announcements.time, "sleep", lambda *_: None)

    class FailPost:
        def post(self, url, **kwargs):
            raise RuntimeError("cninfo 503")

        def close(self):
            return None

    with pytest.raises(RuntimeError, match="regulatory pagination failed"):
        fetch_regulatory_events(date(2024, 6, 28), client=FailPost())


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _OverrunClient:
    """Measured live against production cninfo: request a page past the
    server's own reported ``totalpages`` and it keeps re-serving page 1's
    rows with ``hasMore`` still true — forever. Hit in production:
    announcement_index (same endpoint, same shape) ran 9.6h reaching page
    18977 on one day before an unrelated DNS blip finally stopped it.
    ``totalpages`` itself stays correct even on the overshot pages, so it has
    to be the actual stop condition, not ``hasMore``."""

    def __init__(self, total_pages: int):
        self.total_pages = total_pages
        self.calls = 0

    def post(self, url, **kwargs):
        self.calls += 1
        data = kwargs["data"]
        if data["category"] != cninfo_announcements._CNINFO_CATEGORIES[0]:
            return _Response({"announcements": [], "hasMore": False, "totalpages": 0})
        item = {
            "secCode": "000001",
            "announcementId": f"P{data['pageNum']}",
            "announcementTitle": "行政处罚决定",
            "adjunctUrl": "/x.pdf",
        }
        return _Response({"announcements": [item], "hasMore": True, "totalpages": self.total_pages})

    def close(self):
        return None


def test_regulatory_stops_at_totalpages_even_when_hasmore_lies():
    client = _OverrunClient(total_pages=3)
    df = fetch_regulatory_events(date(2024, 1, 31), client=client)
    assert client.calls == 28  # first bucket pages 1..3 plus 25 empty buckets
    assert df.height == 3


def test_regulatory_repeated_page_is_audited_and_keeps_partial_rows():
    class RepeatedPageClient:
        def __init__(self):
            self.calls = 0

        def post(self, url, **kwargs):
            self.calls += 1
            data = kwargs["data"]
            if data["category"] != cninfo_announcements._CNINFO_CATEGORIES[0]:
                return _Response({"announcements": [], "hasMore": False})
            return _Response(
                {
                    "announcements": [
                        {
                            "secCode": "000001",
                            "announcementId": "reg-repeat",
                            "announcementTitle": "行政处罚决定",
                        }
                    ],
                    "hasMore": True,
                }
            )

    findings: list[dict] = []
    client = RepeatedPageClient()
    df = fetch_regulatory_events(date(2024, 1, 31), client=client, findings=findings)
    assert df.height == 1
    assert findings[0]["dataset"] == "regulatory"
    assert findings[0]["check"] == "cninfo_truncation_at_100_pages"
    assert client.calls == 27


def test_regulatory_rejects_empty_page_before_totalpages():
    class EmptyPageClient:
        def post(self, url, **kwargs):
            page = kwargs["data"]["pageNum"]
            if page == 1:
                return _Response(
                    {
                        "announcements": [
                            {
                                "secCode": "000001",
                                "announcementId": "R1",
                                "announcementTitle": "行政处罚决定",
                            }
                        ],
                        "hasMore": True,
                        "totalpages": 2,
                    }
                )
            return _Response({"announcements": [], "hasMore": False, "totalpages": 2})

    with pytest.raises(RuntimeError, match="empty page before the reported end"):
        fetch_regulatory_events(date(2024, 1, 31), client=EmptyPageClient())


def test_regulatory_uses_totalpages_when_hasmore_is_false():
    class StaleHasMoreClient:
        def __init__(self):
            self.calls = 0

        def post(self, url, **kwargs):
            self.calls += 1
            category = kwargs["data"]["category"]
            if category != cninfo_announcements._CNINFO_CATEGORIES[0]:
                return _Response({"announcements": [], "hasMore": False, "totalpages": 0})
            page = kwargs["data"]["pageNum"]
            item = {
                "secCode": "000001",
                "announcementId": f"R{page}",
                "announcementTitle": "行政处罚决定",
            }
            return _Response({"announcements": [item], "hasMore": False, "totalpages": 2})

    client = StaleHasMoreClient()
    df = fetch_regulatory_events(date(2024, 1, 31), client=client)
    assert client.calls == 27
    assert set(df["event_id"].to_list()) == {"reg-R1", "reg-R2"}


def test_regulatory_rejects_malformed_pagination_metadata():
    class Malformed:
        def post(self, url, **kwargs):
            return _Response(
                {
                    "announcements": [
                        {
                            "secCode": "000001",
                            "announcementId": "R1",
                            "announcementTitle": "行政处罚决定",
                        }
                    ],
                    "totalpages": "unknown",
                    "hasMore": True,
                }
            )

        def close(self):
            return None

    with pytest.raises(RuntimeError, match="regulatory pagination failed.*totalpages"):
        fetch_regulatory_events(date(2024, 1, 31), client=Malformed())


def test_regulatory_survives_one_transient_error(monkeypatch):
    """A single 504 must not kill a multi-year backfill walk over one page.

    Hit in production: `regulatory_events` backfilled 2010-2026 died on page 8
    with a 504 from cninfo, and because `walk_day_backfill` restarts the whole
    step on any raise, that one blip meant redoing the entire walk from day 1.
    """
    monkeypatch.setattr(cninfo_announcements.time, "sleep", lambda *_: None)

    calls = {"n": 0}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"announcements": [], "hasMore": False}

    class FlakyOnceThenOk:
        def post(self, url, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("cninfo 504")
            return Response()

        def close(self):
            return None

    df = fetch_regulatory_events(date(2024, 6, 28), client=FlakyOnceThenOk())
    assert df.is_empty()  # no announcements this run, but no raise either
    assert calls["n"] >= 2, "expected a retry, not a fail on the first attempt"
