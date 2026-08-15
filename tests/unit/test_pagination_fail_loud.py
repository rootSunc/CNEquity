"""Paginated snapshot fetches must fail loud, never silently truncate."""

from datetime import date

import pytest

from cn_market_lake.adapters.cninfo import announcements as cninfo_announcements
from cn_market_lake.adapters.cninfo.regulatory import fetch_regulatory_events
from cn_market_lake.adapters.eastmoney import clist
from cn_market_lake.adapters.eastmoney.clist import fetch_clist_pages


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
        if data["column"] == "sse":
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
    assert client.calls == 4  # szse pages 1..3, then sse's single (empty) page
    assert df.height == 3


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
