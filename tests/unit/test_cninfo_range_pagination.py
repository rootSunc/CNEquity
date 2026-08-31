"""CNINFO range pagination: date splitting, no-progress recovery and resume."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from cnequity.adapters.cninfo import announcements
from cnequity.adapters.cninfo.announcements import (
    fetch_announcement_index_range,
    invalidate_cninfo_checkpoint,
)


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def _row(key: str, day: str) -> dict:
    return {
        "secCode": "000001",
        "announcementId": key,
        "announcementTitle": key,
        "announcementDate": day,
    }


def test_long_range_is_recursively_date_sliced(monkeypatch):
    monkeypatch.setattr(announcements.time, "sleep", lambda *_: None)

    class Client:
        def __init__(self):
            self.calls = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            start, end = data["seDate"].split("~")
            if data["category"] != announcements._CNINFO_CATEGORIES[0]:
                return _Response({"announcements": [], "totalpages": 0, "hasMore": False})
            if (start, end) == ("2024-01-01", "2024-01-10"):
                # The broad query is too long; its children are bounded.
                return _Response(
                    {
                        "announcements": [_row("root", "2024-01-01")],
                        "totalpages": 101,
                        "hasMore": True,
                    }
                )
            return _Response(
                {
                    "announcements": [_row(f"{start}-{end}", start)],
                    "totalpages": 1,
                    "hasMore": False,
                }
            )

    client = Client()
    metrics = {}
    frame = fetch_announcement_index_range(
        date(2024, 1, 1),
        date(2024, 1, 10),
        client=client,
        max_pages_per_slice=100,
        metrics=metrics,
    )

    assert frame.height == 2
    assert metrics["split_reasons"] == 1
    assert {call["seDate"] for call in client.calls} >= {
        "2024-01-01~2024-01-05",
        "2024-01-06~2024-01-10",
    }


def test_repeated_page_is_sliced_when_range_has_multiple_days():
    class Client:
        def __init__(self):
            self.calls = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            start, end = data["seDate"].split("~")
            if data["category"] != announcements._CNINFO_CATEGORIES[0]:
                return _Response({"announcements": [], "hasMore": False})
            if (start, end) == ("2024-02-01", "2024-02-02"):
                return _Response(
                    {
                        "announcements": [_row("same", "2024-02-01")],
                        "totalpages": 2,
                        "hasMore": True,
                    }
                )
            return _Response(
                {
                    "announcements": [_row(start, start)],
                    "totalpages": 1,
                    "hasMore": False,
                }
            )

    client = Client()
    frame = fetch_announcement_index_range(
        date(2024, 2, 1), date(2024, 2, 2), client=client, max_pages_per_slice=2
    )

    assert frame.height == 2
    assert any(call["seDate"] == "2024-02-01~2024-02-01" for call in client.calls)
    assert any(call["seDate"] == "2024-02-02~2024-02-02" for call in client.calls)


def test_range_checkpoint_resumes_at_failed_page(monkeypatch, tmp_path):
    monkeypatch.setattr(announcements, "_POST_RETRIES", 1)
    checkpoint = tmp_path / "cninfo.json"

    class FailsPageTwo:
        def __init__(self):
            self.calls = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            if data["category"] != announcements._CNINFO_CATEGORIES[0]:
                return _Response({"announcements": [], "hasMore": False})
            if data["pageNum"] == 1:
                return _Response(
                    {
                        "announcements": [_row("p1", "2024-03-01")],
                        "hasMore": True,
                    }
                )
            raise RuntimeError("page two unavailable")

    first = FailsPageTwo()
    with pytest.raises(RuntimeError, match="page 2"):
        fetch_announcement_index_range(
            date(2024, 3, 1),
            date(2024, 3, 1),
            client=first,
            checkpoint_path=checkpoint,
        )
    saved = json.loads(checkpoint.read_text())
    slice_state = saved["slices"]["category_ndbg_szsh:2024-03-01:2024-03-01"]
    assert slice_state["next_page"] == 2
    assert [call["pageNum"] for call in first.calls] == [1, 2]

    class ResumesPageTwo(FailsPageTwo):
        def post(self, _url, data):
            self.calls.append(dict(data))
            if data["category"] != announcements._CNINFO_CATEGORIES[0]:
                return _Response({"announcements": [], "hasMore": False})
            if data["pageNum"] == 2:
                return _Response(
                    {
                        "announcements": [_row("p2", "2024-03-01")],
                        "hasMore": False,
                    }
                )
            raise AssertionError("checkpoint should not request page 1 again")

    second = ResumesPageTwo()
    frame = fetch_announcement_index_range(
        date(2024, 3, 1), date(2024, 3, 1), client=second, checkpoint_path=checkpoint
    )
    assert set(frame["announcement_id"]) == {"p1", "p2"}
    assert [
        call["pageNum"]
        for call in second.calls
        if call["category"] == announcements._CNINFO_CATEGORIES[0]
    ] == [2]
    assert len(second.calls) == 26


def test_cninfo_total_is_raw_rows_while_final_frame_uses_unique_keys(tmp_path):
    checkpoint = tmp_path / "cninfo.json"

    class DuplicateClient:
        def __init__(self):
            self.calls = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            if data["category"] != announcements._CNINFO_CATEGORIES[0]:
                return _Response(
                    {"announcements": [], "total": 0, "totalpages": 0, "hasMore": False}
                )
            return _Response(
                {
                    "announcements": [_row("same", "2024-04-01"), _row("same", "2024-04-01")],
                    "total": 2,
                    "totalpages": 1,
                    "hasMore": False,
                }
            )

    metrics = {}
    client = DuplicateClient()
    frame = fetch_announcement_index_range(
        date(2024, 4, 1),
        client=client,
        checkpoint_path=checkpoint,
        metrics=metrics,
    )

    assert frame.height == 1
    assert frame["announcement_id"].to_list() == ["same"]
    assert metrics["raw_rows"] == 2
    assert metrics["unique_keys"] == 1
    assert metrics["duplicate_rows"] == 1
    saved = json.loads(checkpoint.read_text())
    szse = saved["slices"]["category_ndbg_szsh:2024-04-01:2024-04-01"]
    assert szse["raw_row_count"] == 2
    assert szse["unique_keys"] == ["same"]
    assert len(szse["page_signatures"]) == 1
    assert len(szse["page_signatures"][0]) == 64


def test_cninfo_rejects_raw_row_overrun_even_when_ids_are_distinct():
    """A source total is an exact raw-row count, not a normalized-row bound."""

    class OverrunClient:
        def post(self, _url, data):
            if data["category"] != announcements._CNINFO_CATEGORIES[0]:
                return _Response({"announcements": [], "total": 0, "totalpages": 0})
            return _Response(
                {
                    "announcements": [_row("first", "2024-04-03"), _row("second", "2024-04-03")],
                    "total": 1,
                    "totalpages": 1,
                    "hasMore": False,
                }
            )

    with pytest.raises(RuntimeError, match="does not match reported total"):
        fetch_announcement_index_range(
            date(2024, 4, 3),
            client=OverrunClient(),
        )


def test_cninfo_checkpoint_refresh_revision_and_ttl_absorb_same_date_corrections(tmp_path):
    checkpoint = tmp_path / "cninfo.json"

    class RevisingClient:
        def __init__(self, title):
            self.title = title
            self.calls = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            if data["category"] != announcements._CNINFO_CATEGORIES[0]:
                return _Response({"announcements": [], "totalpages": 0, "hasMore": False})
            return _Response(
                {
                    "announcements": [
                        {
                            **_row("same-date", "2024-04-02"),
                            "announcementTitle": self.title,
                        }
                    ],
                    "total": 1,
                    "totalpages": 1,
                    "hasMore": False,
                }
            )

    first = RevisingClient("v1")
    first_frame = fetch_announcement_index_range(
        date(2024, 4, 2),
        client=first,
        checkpoint_path=checkpoint,
        source_revision="provider-v1",
    )
    assert first_frame["title"].to_list() == ["v1"]
    assert len(first.calls) == 26

    # Refresh is the safe default: a completed same-date page is read again,
    # so a corrected announcement replaces the old payload.
    refreshed = RevisingClient("v2")
    refreshed_frame = fetch_announcement_index_range(
        date(2024, 4, 2),
        client=refreshed,
        checkpoint_path=checkpoint,
        source_revision="provider-v1",
    )
    assert refreshed_frame["title"].to_list() == ["v2"]
    assert len(refreshed.calls) == 26

    class NoRequests(RevisingClient):
        def post(self, _url, data):
            raise AssertionError("fresh checkpoint should be reusable when refresh=False")

    reused = NoRequests("unused")
    reused_frame = fetch_announcement_index_range(
        date(2024, 4, 2),
        client=reused,
        checkpoint_path=checkpoint,
        refresh=False,
        checkpoint_ttl_days=1,
        source_revision="provider-v1",
    )
    assert reused_frame["title"].to_list() == ["v2"]
    assert reused.calls == []

    # A provider revision invalidates the complete ledger even in explicit
    # resume mode, preventing a mixed-version range.
    revised_source = RevisingClient("v3")
    revised_frame = fetch_announcement_index_range(
        date(2024, 4, 2),
        client=revised_source,
        checkpoint_path=checkpoint,
        refresh=False,
        source_revision="provider-v2",
    )
    assert revised_frame["title"].to_list() == ["v3"]
    assert len(revised_source.calls) == 26

    saved = json.loads(checkpoint.read_text())
    old = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
    for record in saved["slices"].values():
        record["completed_at"] = old
    checkpoint.write_text(json.dumps(saved))
    expired = RevisingClient("v4")
    expired_frame = fetch_announcement_index_range(
        date(2024, 4, 2),
        client=expired,
        checkpoint_path=checkpoint,
        refresh=False,
        checkpoint_ttl_days=1,
        source_revision="provider-v2",
    )
    assert expired_frame["title"].to_list() == ["v4"]
    assert len(expired.calls) == 26

    invalidate_cninfo_checkpoint(checkpoint)
    assert not checkpoint.exists()
