"""CNINFO wire-payload archival and offline replay coverage."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from cnequity.adapters.cninfo import announcements
from cnequity.adapters.cninfo.announcements import (
    fetch_announcement_index_range,
    replay_announcement_index_range,
)
from cnequity.storage.raw_archive import RawArchiveError, RawPayloadArchive


class _Response:
    status_code = 200
    url = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
    headers = {"content-type": "application/json", "set-cookie": "do-not-save"}

    def __init__(self, payload: dict, wire: bytes | None = None):
        self.payload = payload
        self.content = (
            wire if wire is not None else json.dumps(payload, ensure_ascii=False).encode()
        )

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _Client:
    def __init__(self, pages):
        self.pages = pages
        self.calls: list[dict] = []

    def post(self, _url, data):
        self.calls.append(dict(data))
        key = (data["column"], data["seDate"], data["pageNum"])
        return self.pages[key]


def _config(tmp_path: Path):
    return SimpleNamespace(
        meta_root=tmp_path / "meta",
        raw_archive_enabled=True,
        raw_archive_compression="none",
        raw_archive_max_payload_bytes=None,
        should_archive_raw=lambda dataset: dataset == "announcement_index",
    )


def _row(identifier: str, title: str = "公告", day: str = "2024-01-01"):
    return {
        "secCode": "000001",
        "announcementId": identifier,
        "announcementTitle": title,
        "announcementDate": day,
    }


def test_each_cninfo_http_page_archives_exact_wire_and_transport_metadata(tmp_path):
    first = {"announcements": [_row("a", "v1")], "totalpages": 2, "hasMore": True}
    second = {"announcements": [_row("a", "v2")], "totalpages": 2, "hasMore": False}
    pages = {
        ("szse", "2024-01-01~2024-01-01", 1): _Response(first, b'{"wire":"page-1"}'),
        ("szse", "2024-01-01~2024-01-01", 2): _Response(second, b'{"wire":"page-2"}'),
    }
    config = _config(tmp_path)
    client = _Client(pages)

    frame = fetch_announcement_index_range(date(2024, 1, 1), client=client, config=config)
    records = RawPayloadArchive(config.meta_root).records("announcement_index")

    assert frame["title"].to_list() == ["v2"]
    assert len(records) == len(client.calls) == 2
    by_page = {
        record.request_params["pageNum"]: record
        for record in records
        if record.request_params["column"] == "szse"
    }
    assert hashlib.sha256(b'{"wire":"page-1"}').hexdigest() == by_page[1].payload_sha256
    assert by_page[2].pagination["reported_total_pages"] == 2
    assert by_page[2].http_metadata["wire_exact"] is True
    metadata = Path(config.meta_root / by_page[1].metadata_path).read_text(encoding="utf-8")
    assert "do-not-save" not in metadata
    assert "set-cookie" not in metadata


@pytest.mark.parametrize(("row_count", "reported_pages"), [(31, 1), (25, 0)])
def test_replay_uses_record_total_when_totalpages_omits_partial_page(
    tmp_path, row_count, reported_pages
):
    """CNINFO's totalpages counts full pages, including zero for a short bucket."""
    target = date(2024, 1, 1)
    payloads = {}
    rows = [_row(str(index)) for index in range(row_count)]
    for page, offset in enumerate(range(0, row_count, 30), start=1):
        payloads[("szse", "2024-01-01~2024-01-01", page)] = _Response(
            {
                "announcements": rows[offset : offset + 30],
                "totalRecordNum": row_count,
                "totalpages": reported_pages,
                "hasMore": offset + 30 < row_count,
            }
        )
    payloads[("sse", "2024-01-01~2024-01-01", 1)] = _Response(
        {
            "announcements": [],
            "totalRecordNum": 0,
            "totalpages": 0,
            "hasMore": False,
        }
    )
    config = _config(tmp_path)

    live = fetch_announcement_index_range(target, client=_Client(payloads), config=config)
    replayed = replay_announcement_index_range(RawPayloadArchive(config.meta_root), target)

    assert live.height == replayed.height == row_count
    assert set(replayed["announcement_id"]) == {str(index) for index in range(row_count)}
    assert str(row_count - 1) in replayed["announcement_id"]


def test_dense_day_partition_archive_replays_only_with_complete_reconciliation(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(announcements, "_CNINFO_MARKET_PLATES", ("sz", "sh"))
    monkeypatch.setattr(announcements, "_CNINFO_PLATE_BOARDS", {})
    monkeypatch.setattr(announcements, "_CNINFO_TRADES", ())
    monkeypatch.setattr(announcements, "_CNINFO_CATEGORIES", ())
    target = date(2024, 1, 3)

    class PlateClient:
        def __init__(self):
            self.calls: list[dict] = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            plate = data["plate"]
            if not plate:
                return _Response(
                    {
                        "announcements": [_row("a", day=target.isoformat())],
                        "totalRecordNum": 2,
                        "totalpages": 101,
                        "hasMore": True,
                    }
                )
            identifier = {"sz": "a", "sh": "b"}[plate]
            return _Response(
                {
                    "announcements": [_row(identifier, day=target.isoformat())],
                    "totalRecordNum": 1,
                    "totalpages": 1,
                    "hasMore": False,
                }
            )

    config = _config(tmp_path)
    live = fetch_announcement_index_range(target, client=PlateClient(), config=config)
    archive = RawPayloadArchive(config.meta_root)
    records = archive.records("announcement_index")
    replayed = replay_announcement_index_range(archive, target)

    assert set(live["announcement_id"].to_list()) == {"a", "b"}
    assert replayed.sort("announcement_id").equals(live.sort("announcement_id"))
    assert {
        record.pagination["column"] for record in records if record.request_params.get("plate")
    } == {"szse|plate=sz", "szse|plate=sh"}

    incomplete = [record for record in records if record.request_params.get("plate") != "sh"]
    with pytest.raises(RawArchiveError, match="partition children are incomplete"):
        replay_announcement_index_range(archive, target, records=incomplete)

    sh = next(record for record in records if record.request_params.get("plate") == "sh")
    sh.request_params["plate"] = "sz"
    with pytest.raises(RawArchiveError, match="request filters disagree"):
        replay_announcement_index_range(archive, target, records=records)


def test_conflicting_record_total_aliases_fail_live_and_replay(tmp_path):
    target = date(2024, 1, 2)
    payload = {
        "announcements": [_row("a", day=target.isoformat())],
        "total": 1,
        "totalRecordNum": 2,
        "totalpages": 101,
        "hasMore": True,
    }
    config = _config(tmp_path)
    client = _Client({("szse", f"{target}~{target}", 1): _Response(payload)})

    with pytest.raises(RuntimeError, match="record totals disagree"):
        fetch_announcement_index_range(target, client=client, config=config)
    with pytest.raises(RawArchiveError, match="record totals disagree"):
        replay_announcement_index_range(RawPayloadArchive(config.meta_root), target)


def test_category_replay_rejects_parent_capture_missing_page_one(monkeypatch, tmp_path):
    monkeypatch.setattr(announcements, "_CNINFO_CATEGORIES", ("cat-a", "cat-b"))
    target = date(2024, 1, 2)

    class RepeatedControlClient:
        def post(self, _url, data):
            category = data["category"]
            if not category:
                return _Response(
                    {
                        "announcements": [_row("a", day=target.isoformat())],
                        "totalRecordNum": 2,
                        "totalpages": 2,
                        "hasMore": True,
                    }
                )
            identifier = {"cat-a": "a", "cat-b": "b"}[category]
            return _Response(
                {
                    "announcements": [_row(identifier, day=target.isoformat())],
                    "totalRecordNum": 1,
                    "totalpages": 1,
                    "hasMore": False,
                }
            )

    config = _config(tmp_path)
    archive = RawPayloadArchive(config.meta_root)
    fetch_announcement_index_range(
        target,
        client=RepeatedControlClient(),
        config=config,
        max_pages_per_slice=2,
    )
    incomplete = [
        record
        for record in archive.records("announcement_index")
        if not (
            not record.request_params.get("category") and record.request_params.get("pageNum") == 1
        )
    ]

    with pytest.raises(RawArchiveError, match="missing page 1"):
        replay_announcement_index_range(archive, target, records=incomplete, max_pages_per_slice=2)


def test_broad_archive_replays_nested_dense_day_without_duplicate_exchange(monkeypatch, tmp_path):
    monkeypatch.setattr(announcements, "_CNINFO_CATEGORIES", ("cat-a", "cat-b"))
    first = date(2024, 1, 3)
    second = date(2024, 1, 4)

    class NestedCategoryClient:
        def __init__(self):
            self.calls: list[dict] = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            period = data["seDate"]
            category = data["category"]
            if period == "2024-01-03~2024-01-04":
                return _Response(
                    {
                        "announcements": [_row("broad", day=first.isoformat())],
                        "totalRecordNum": 3,
                        "totalpages": 101,
                        "hasMore": True,
                    }
                )
            if period == "2024-01-03~2024-01-03" and not category:
                return _Response(
                    {
                        "announcements": [_row("a", day=first.isoformat())],
                        "totalRecordNum": 2,
                        "totalpages": 101,
                        "hasMore": True,
                    }
                )
            if period == "2024-01-03~2024-01-03":
                identifier = {"cat-a": "a", "cat-b": "b"}[category]
                return _Response(
                    {
                        "announcements": [_row(identifier, day=first.isoformat())],
                        "totalRecordNum": 1,
                        "totalpages": 1,
                        "hasMore": False,
                    }
                )
            return _Response(
                {
                    "announcements": [_row("c", day=second.isoformat())],
                    "totalRecordNum": 1,
                    "totalpages": 1,
                    "hasMore": False,
                }
            )

    config = _config(tmp_path)
    client = NestedCategoryClient()
    live = fetch_announcement_index_range(first, second, client=client, config=config)
    replayed = replay_announcement_index_range(RawPayloadArchive(config.meta_root), first, second)

    assert set(live["announcement_id"].to_list()) == {"a", "b", "c"}
    assert replayed.sort("announcement_id").equals(live.sort("announcement_id"))
    assert {call["column"] for call in client.calls} == {"szse"}


def test_partition_marker_does_not_leak_into_next_refresh(monkeypatch, tmp_path):
    monkeypatch.setattr(announcements, "_CNINFO_MARKET_PLATES", ("sz", "sh"))
    monkeypatch.setattr(announcements, "_CNINFO_PLATE_BOARDS", {})
    monkeypatch.setattr(announcements, "_CNINFO_TRADES", ())
    monkeypatch.setattr(announcements, "_CNINFO_CATEGORIES", ())
    target = date(2024, 1, 5)
    checkpoint = tmp_path / "checkpoint.json"
    config = _config(tmp_path)

    class DenseClient:
        def post(self, _url, data):
            if not data["plate"]:
                return _Response(
                    {
                        "announcements": [_row("a", day=target.isoformat())],
                        "totalRecordNum": 2,
                        "totalpages": 101,
                        "hasMore": True,
                    }
                )
            identifier = {"sz": "a", "sh": "b"}[data["plate"]]
            return _Response(
                {
                    "announcements": [_row(identifier, day=target.isoformat())],
                    "totalRecordNum": 1,
                    "totalpages": 1,
                    "hasMore": False,
                }
            )

    fetch_announcement_index_range(
        target, client=DenseClient(), config=config, checkpoint_path=checkpoint
    )

    class OrdinaryClient:
        def __init__(self):
            self.calls: list[dict] = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            return _Response(
                {
                    "announcements": [_row("fresh", day=target.isoformat())],
                    "totalRecordNum": 1,
                    "totalpages": 1,
                    "hasMore": False,
                }
            )

    ordinary = OrdinaryClient()
    refreshed = fetch_announcement_index_range(
        target, client=ordinary, config=config, checkpoint_path=checkpoint
    )
    replayed = replay_announcement_index_range(RawPayloadArchive(config.meta_root), target)

    assert [call["column"] for call in ordinary.calls] == ["szse"]
    assert [call["plate"] for call in ordinary.calls] == [""]
    assert refreshed["announcement_id"].to_list() == ["fresh"]
    assert replayed["announcement_id"].to_list() == ["fresh"]


def test_identical_response_bytes_keep_distinct_page_observations_and_replay_revisions(tmp_path):
    # Identical bytes on pages 1 and 2 trigger the live no-progress split;
    # distinct observation sidecars preserve both actual page requests.
    repeated = _Response({"announcements": [_row("same", "v1")], "totalpages": 2, "hasMore": True})
    empty = _Response({"announcements": [], "hasMore": False})
    pages = {
        ("szse", "2024-01-01~2024-01-02", 1): repeated,
        ("szse", "2024-01-01~2024-01-02", 2): repeated,
        ("szse", "2024-01-01~2024-01-01", 1): _Response(
            {"announcements": [_row("same", "day-1")], "totalpages": 1, "hasMore": False}
        ),
        ("szse", "2024-01-02~2024-01-02", 1): _Response(
            {
                "announcements": [_row("same", "day-2", "2024-01-02")],
                "totalpages": 1,
                "hasMore": False,
            }
        ),
        ("sse", "2024-01-01~2024-01-02", 1): empty,
        ("sse", "2024-01-01~2024-01-01", 1): empty,
        ("sse", "2024-01-02~2024-01-02", 1): empty,
    }
    config = _config(tmp_path)
    client = _Client(pages)
    live = fetch_announcement_index_range(
        date(2024, 1, 1), date(2024, 1, 2), client=client, config=config, max_pages_per_slice=2
    )
    archive = RawPayloadArchive(config.meta_root)
    observations = [
        record
        for record in archive.records("announcement_index")
        if record.request_params["seDate"] == "2024-01-01~2024-01-02"
        and record.request_params["column"] == "szse"
    ]
    replayed = replay_announcement_index_range(
        archive, date(2024, 1, 1), date(2024, 1, 2), max_pages_per_slice=2
    )
    assert len(observations) == 2
    assert live["announcement_id"].to_list() == replayed["announcement_id"].to_list()
    assert live["title"].to_list() == replayed["title"].to_list()

    incomplete = [
        record
        for record in archive.records("announcement_index")
        if record.request_params.get("seDate") != "2024-01-02~2024-01-02"
    ]
    with pytest.raises(RawArchiveError, match="do not fully cover parent"):
        replay_announcement_index_range(
            archive,
            date(2024, 1, 1),
            date(2024, 1, 2),
            records=incomplete,
            max_pages_per_slice=2,
        )


def test_new_complete_broad_capture_is_not_replaced_by_historic_split_children(tmp_path):
    repeated = _Response(
        {"announcements": [_row("same", "old-root")], "totalpages": 2, "hasMore": True}
    )
    empty = _Response({"announcements": [], "hasMore": False})
    config = _config(tmp_path)
    old = _Client(
        {
            ("szse", "2024-01-01~2024-01-02", 1): repeated,
            ("szse", "2024-01-01~2024-01-02", 2): repeated,
            ("szse", "2024-01-01~2024-01-01", 1): _Response(
                {"announcements": [_row("old-1", "old-1")], "hasMore": False}
            ),
            ("szse", "2024-01-02~2024-01-02", 1): _Response(
                {
                    "announcements": [_row("old-2", "old-2", "2024-01-02")],
                    "hasMore": False,
                }
            ),
            ("sse", "2024-01-01~2024-01-02", 1): empty,
            ("sse", "2024-01-01~2024-01-01", 1): empty,
            ("sse", "2024-01-02~2024-01-02", 1): empty,
        }
    )
    fetch_announcement_index_range(
        date(2024, 1, 1),
        date(2024, 1, 2),
        client=old,
        config=config,
        run_id="old-split-run",
        max_pages_per_slice=2,
    )

    new = _Client(
        {
            ("szse", "2024-01-01~2024-01-02", 1): _Response(
                {
                    "announcements": [
                        _row("new-1", "new-1"),
                        _row("new-2", "new-2", "2024-01-02"),
                    ],
                    "totalpages": 1,
                    "hasMore": False,
                }
            ),
            ("sse", "2024-01-01~2024-01-02", 1): empty,
        }
    )
    latest = fetch_announcement_index_range(
        date(2024, 1, 1),
        date(2024, 1, 2),
        client=new,
        config=config,
        run_id="new-complete-run",
        max_pages_per_slice=2,
    )
    replayed = replay_announcement_index_range(
        RawPayloadArchive(config.meta_root),
        date(2024, 1, 1),
        date(2024, 1, 2),
        max_pages_per_slice=2,
    )
    assert set(latest["announcement_id"]) == {"new-1", "new-2"}
    assert set(replayed["announcement_id"]) == {"new-1", "new-2"}


def test_replay_descends_into_children_when_broad_row_has_no_date(tmp_path):
    config = _config(tmp_path)
    missing = _row("missing", "broad")
    missing.pop("announcementDate")
    empty = _Response({"announcements": [], "hasMore": False})
    client = _Client(
        {
            ("szse", "2024-01-01~2024-01-02", 1): _Response(
                {"announcements": [missing], "hasMore": False}
            ),
            ("szse", "2024-01-01~2024-01-01", 1): _Response(
                {"announcements": [_row("day-1", "day-1")], "hasMore": False}
            ),
            ("szse", "2024-01-02~2024-01-02", 1): _Response(
                {
                    "announcements": [_row("day-2", "day-2", "2024-01-02")],
                    "hasMore": False,
                }
            ),
            ("sse", "2024-01-01~2024-01-02", 1): empty,
        }
    )
    live = fetch_announcement_index_range(
        date(2024, 1, 1),
        date(2024, 1, 2),
        client=client,
        config=config,
        run_id="missing-date-split-run",
    )
    replayed = replay_announcement_index_range(
        RawPayloadArchive(config.meta_root), date(2024, 1, 1), date(2024, 1, 2)
    )
    assert set(live["announcement_id"]) == {"day-1", "day-2"}
    assert set(replayed["announcement_id"]) == {"day-1", "day-2"}


def test_same_run_retry_cannot_mix_new_parent_with_old_child(tmp_path, monkeypatch):
    monkeypatch.setattr(announcements, "_POST_RETRIES", 1)
    config = _config(tmp_path)
    empty = _Response({"announcements": [], "hasMore": False})
    old_missing = _row("old-root", "old-root")
    old_missing.pop("announcementDate")
    old = _Client(
        {
            ("szse", "2024-01-01~2024-01-02", 1): _Response(
                {"announcements": [old_missing], "hasMore": False}
            ),
            ("szse", "2024-01-01~2024-01-01", 1): _Response(
                {"announcements": [_row("old-1", "old-1")], "hasMore": False}
            ),
            ("szse", "2024-01-02~2024-01-02", 1): _Response(
                {
                    "announcements": [_row("old-2", "old-2", "2024-01-02")],
                    "hasMore": False,
                }
            ),
            ("sse", "2024-01-01~2024-01-02", 1): empty,
        }
    )
    fetch_announcement_index_range(
        date(2024, 1, 1),
        date(2024, 1, 2),
        client=old,
        config=config,
        run_id="same-run",
    )

    new_missing = _row("new-root", "new-root")
    new_missing.pop("announcementDate")

    class InterruptedRetry:
        def post(self, _url, data):
            window = data["seDate"]
            if data["column"] == "sse":
                return empty
            if window == "2024-01-01~2024-01-02":
                return _Response({"announcements": [new_missing], "hasMore": False})
            if window == "2024-01-01~2024-01-01":
                return _Response({"announcements": [_row("new-1", "new-1")], "hasMore": False})
            raise RuntimeError("new day two interrupted")

    with pytest.raises(RuntimeError, match="new day two interrupted"):
        fetch_announcement_index_range(
            date(2024, 1, 1),
            date(2024, 1, 2),
            client=InterruptedRetry(),
            config=config,
            run_id="same-run",
        )
    with pytest.raises(RawArchiveError, match="do not fully cover parent"):
        replay_announcement_index_range(
            RawPayloadArchive(config.meta_root), date(2024, 1, 1), date(2024, 1, 2)
        )


def test_same_run_retry_cannot_replay_a_half_written_invocation(tmp_path, monkeypatch):
    """A newer capture that stopped mid-walk is not a usable replay source.

    There is no second exchange column to lose any more — one walk answers the
    whole market — so the way an invocation goes partial now is a page that
    never came back. Replay must say so rather than quietly serving the page
    it did get, or the previous run's rows.
    """
    monkeypatch.setattr(announcements, "_POST_RETRIES", 1)
    config = _config(tmp_path)
    complete = _Client(
        {
            ("szse", "2024-01-01~2024-01-01", 1): _Response(
                {"announcements": [_row("old-1")], "totalpages": 2, "hasMore": True}
            ),
            ("szse", "2024-01-01~2024-01-01", 2): _Response(
                {"announcements": [_row("old-2")], "totalpages": 2, "hasMore": False}
            ),
        }
    )
    fetch_announcement_index_range(
        date(2024, 1, 1), client=complete, config=config, run_id="same-run"
    )

    class InterruptedSecondPage:
        def post(self, _url, data):
            if data["pageNum"] == 1:
                return _Response(
                    {"announcements": [_row("new-1")], "totalpages": 2, "hasMore": True}
                )
            raise RuntimeError("page 2 interrupted before response")

    with pytest.raises(RuntimeError, match="page 2 interrupted"):
        fetch_announcement_index_range(
            date(2024, 1, 1),
            client=InterruptedSecondPage(),
            config=config,
            run_id="same-run",
        )
    with pytest.raises(RawArchiveError):
        replay_announcement_index_range(RawPayloadArchive(config.meta_root), date(2024, 1, 1))


def test_replay_exact_daily_range_prefers_newer_invocation_over_old_broad(tmp_path):
    config = _config(tmp_path)
    empty = _Response({"announcements": [], "hasMore": False})
    broad = _Client(
        {
            ("szse", "2024-01-01~2024-01-02", 1): _Response(
                {
                    "announcements": [
                        _row("old-1", "old-1"),
                        _row("old-2", "old-2", "2024-01-02"),
                    ],
                    "hasMore": False,
                }
            ),
            ("sse", "2024-01-01~2024-01-02", 1): empty,
        }
    )
    fetch_announcement_index_range(
        date(2024, 1, 1),
        date(2024, 1, 2),
        client=broad,
        config=config,
        run_id="old-broad-invocation",
    )
    daily = _Client(
        {
            ("szse", "2024-01-02~2024-01-02", 1): _Response(
                {
                    "announcements": [_row("new-2", "new-2", "2024-01-02")],
                    "hasMore": False,
                }
            ),
            ("sse", "2024-01-02~2024-01-02", 1): empty,
        }
    )
    fetch_announcement_index_range(
        date(2024, 1, 2), client=daily, config=config, run_id="new-daily-invocation"
    )
    replayed = replay_announcement_index_range(
        RawPayloadArchive(config.meta_root), date(2024, 1, 2)
    )
    assert replayed["announcement_id"].to_list() == ["new-2"]
    end_only = replay_announcement_index_range(
        RawPayloadArchive(config.meta_root), end=date(2024, 1, 2)
    )
    assert end_only["announcement_id"].to_list() == ["new-2"]

    broad_replay = replay_announcement_index_range(
        RawPayloadArchive(config.meta_root), date(2024, 1, 1), date(2024, 1, 2)
    )
    assert set(broad_replay["announcement_id"]) == {"old-1", "old-2"}
    with pytest.raises(RawArchiveError, match="explicit replay date range"):
        replay_announcement_index_range(RawPayloadArchive(config.meta_root))


def test_replay_fails_closed_on_newest_interrupted_capture(tmp_path, monkeypatch):
    monkeypatch.setattr(announcements, "_POST_RETRIES", 1)
    config = _config(tmp_path)
    complete = _Client(
        {
            ("szse", "2024-01-01~2024-01-01", 1): _Response(
                {
                    "announcements": [_row("old", "complete")],
                    "totalpages": 1,
                    "hasMore": False,
                }
            ),
            ("sse", "2024-01-01~2024-01-01", 1): _Response(
                {"announcements": [], "totalpages": 0, "hasMore": False}
            ),
        }
    )
    fetch_announcement_index_range(
        date(2024, 1, 1), client=complete, config=config, run_id="complete-run"
    )

    class Interrupted:
        def post(self, _url, data):
            if data["pageNum"] == 1:
                return _Response(
                    {
                        "announcements": [_row("partial", "partial")],
                        "totalpages": 2,
                        "hasMore": True,
                    }
                )
            raise RuntimeError("page two interrupted")

    with pytest.raises(RuntimeError, match="page 2"):
        fetch_announcement_index_range(
            date(2024, 1, 1),
            client=Interrupted(),
            config=config,
            run_id="interrupted-run",
        )

    with pytest.raises(RawArchiveError, match="missing page 2"):
        replay_announcement_index_range(RawPayloadArchive(config.meta_root), date(2024, 1, 1))


def test_replay_rejects_page_total_drift_within_capture(tmp_path):
    config = _config(tmp_path)
    client = _Client(
        {
            ("szse", "2024-01-01~2024-01-01", 1): _Response(
                {
                    "announcements": [_row("p1")],
                    "totalpages": 2,
                    "hasMore": True,
                }
            ),
            ("szse", "2024-01-01~2024-01-01", 2): _Response(
                {
                    "announcements": [_row("p2")],
                    "totalpages": 3,
                    "hasMore": False,
                }
            ),
        }
    )

    with pytest.raises(RuntimeError, match="totalpages changed"):
        fetch_announcement_index_range(
            date(2024, 1, 1), client=client, config=config, run_id="drift-run"
        )
    with pytest.raises(RawArchiveError, match="page total changed"):
        replay_announcement_index_range(RawPayloadArchive(config.meta_root), date(2024, 1, 1))


def test_replay_uses_live_strict_pagination_value_parsing(tmp_path):
    config = _config(tmp_path)
    client = _Client(
        {
            ("szse", "2024-01-01~2024-01-01", 1): _Response(
                {
                    "announcements": [_row("x")],
                    "totalpages": True,
                    "hasMore": False,
                }
            )
        }
    )
    with pytest.raises(RuntimeError, match="not a non-negative integer"):
        fetch_announcement_index_range(
            date(2024, 1, 1), client=client, config=config, run_id="bool-pages-run"
        )
    with pytest.raises(RawArchiveError, match="not a non-negative integer"):
        replay_announcement_index_range(RawPayloadArchive(config.meta_root), date(2024, 1, 1))


def test_replay_does_not_fall_back_after_newest_json_parse_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(announcements, "_POST_RETRIES", 1)
    config = _config(tmp_path)
    complete = _Client(
        {
            ("szse", "2024-01-01~2024-01-01", 1): _Response(
                {"announcements": [_row("old")], "totalpages": 1, "hasMore": False}
            ),
            ("sse", "2024-01-01~2024-01-01", 1): _Response(
                {"announcements": [], "totalpages": 0, "hasMore": False}
            ),
        }
    )
    fetch_announcement_index_range(
        date(2024, 1, 1), client=complete, config=config, run_id="old-json-run"
    )

    class InvalidJson(_Response):
        def __init__(self):
            super().__init__({}, wire=b"not-json")

        def json(self):
            raise ValueError("invalid json")

    class InvalidClient:
        def post(self, _url, data):
            return InvalidJson()

    with pytest.raises(RuntimeError, match="page 1"):
        fetch_announcement_index_range(
            date(2024, 1, 1),
            client=InvalidClient(),
            config=config,
            run_id="new-invalid-json-run",
        )
    with pytest.raises(RawArchiveError, match="unparsed JSON"):
        replay_announcement_index_range(RawPayloadArchive(config.meta_root), date(2024, 1, 1))


def test_replay_rejects_latest_non_object_json_capture(tmp_path, monkeypatch):
    monkeypatch.setattr(announcements, "_POST_RETRIES", 1)
    config = _config(tmp_path)

    class ListResponse(_Response):
        def __init__(self):
            self.payload = []
            self.content = b"[]"

    class ListClient:
        def post(self, _url, data):
            return ListResponse()

    with pytest.raises(RuntimeError, match="not an object"):
        fetch_announcement_index_range(
            date(2024, 1, 1),
            client=ListClient(),
            config=config,
            run_id="list-json-run",
        )
    with pytest.raises(RawArchiveError, match="unparsed JSON|JSON is not an object"):
        replay_announcement_index_range(RawPayloadArchive(config.meta_root), date(2024, 1, 1))


def test_checkpoint_cannot_reuse_rows_when_required_wire_archive_is_missing(tmp_path):
    payload = {"announcements": [_row("a")], "totalpages": 1, "hasMore": False}
    pages = {
        ("szse", "2024-01-01~2024-01-01", 1): _Response(payload, b"wire-a"),
        ("sse", "2024-01-01~2024-01-01", 1): _Response(
            {"announcements": [], "totalpages": 0, "hasMore": False}, b"wire-empty"
        ),
    }
    config = _config(tmp_path)
    checkpoint = tmp_path / "cninfo.json"
    first = _Client(pages)
    fetch_announcement_index_range(
        date(2024, 1, 1), client=first, config=config, checkpoint_path=checkpoint
    )
    saved = json.loads(checkpoint.read_text(encoding="utf-8"))
    reference = saved["slices"]["szse:2024-01-01:2024-01-01"]["raw_archives"][0]
    (config.meta_root / reference["payload_path"]).unlink()

    second = _Client(pages)
    fetch_announcement_index_range(
        date(2024, 1, 1), client=second, config=config, checkpoint_path=checkpoint, refresh=False
    )
    # A checkpoint whose wire evidence is gone cannot be resumed from its
    # normalized rows: the page is fetched again so the capture is complete.
    assert [call["pageNum"] for call in second.calls] == [1]


def test_replay_rejects_distinct_raw_rows_over_source_total(tmp_path):
    payload = {
        "announcements": [_row("a"), _row("b")],
        "total": 1,
        "totalpages": 1,
        "hasMore": False,
    }
    empty = {"announcements": [], "total": 0, "totalpages": 0, "hasMore": False}
    config = _config(tmp_path)
    client = _Client(
        {
            ("szse", "2024-01-01~2024-01-01", 1): _Response(payload),
            ("sse", "2024-01-01~2024-01-01", 1): _Response(empty),
        }
    )
    with pytest.raises(RuntimeError, match="does not match reported total"):
        fetch_announcement_index_range(date(2024, 1, 1), client=client, config=config)

    with pytest.raises(RawArchiveError, match="does not match.*total"):
        replay_announcement_index_range(RawPayloadArchive(config.meta_root), date(2024, 1, 1))


def test_replay_rejects_tampered_cninfo_wire_payload(tmp_path):
    payload = {"announcements": [_row("a")], "totalpages": 1, "hasMore": False}
    config = _config(tmp_path)
    client = _Client(
        {
            ("szse", "2024-01-01~2024-01-01", 1): _Response(payload, b"wire-a"),
            ("sse", "2024-01-01~2024-01-01", 1): _Response(
                {"announcements": [], "totalpages": 0, "hasMore": False}, b"wire-empty"
            ),
        }
    )
    fetch_announcement_index_range(date(2024, 1, 1), client=client, config=config)
    archive = RawPayloadArchive(config.meta_root)
    record = next(
        item
        for item in archive.records("announcement_index")
        if item.request_params["column"] == "szse"
    )
    path = config.meta_root / record.payload_path
    path.write_bytes(b"tampered")
    with pytest.raises(RawArchiveError):
        replay_announcement_index_range(archive, date(2024, 1, 1))


def test_raw_archive_rejects_symlinked_roots_and_payload_boundaries(tmp_path):
    archive = RawPayloadArchive(tmp_path / "meta")
    record = archive.archive(
        "announcement_index",
        b"wire-a",
        source="cninfo",
        captured_at=None,
    )
    assert record is not None
    meta = tmp_path / "meta"
    raw = meta / "raw"
    dataset_root = raw / "announcement_index"
    payload = meta / record.payload_path
    sidecar = meta / record.metadata_path
    original_payload = payload.read_bytes()

    # The configured data/meta root itself must not be canonicalised through a
    # link before a read or replay.
    linked_meta = tmp_path / "linked-meta"
    linked_meta.symlink_to(meta, target_is_directory=True)
    linked_archive = RawPayloadArchive(linked_meta)
    with pytest.raises(RawArchiveError, match="symlink"):
        linked_archive.read(record)
    with pytest.raises(RawArchiveError, match="symlink"):
        linked_archive.replay(record, lambda data: data)

    external_raw = tmp_path / "external-raw"
    raw.rename(external_raw)
    raw.symlink_to(external_raw, target_is_directory=True)
    with pytest.raises(RawArchiveError, match="symlink"):
        archive.records("announcement_index")
    with pytest.raises(RawArchiveError, match="symlink"):
        archive.archive("announcement_index", b"wire-b", source="cninfo")
    raw.unlink()
    external_raw.rename(raw)

    # Replacing the raw dataset directory with an external link must block
    # both enumeration and a subsequent write; the external tree stays unchanged.
    # Do not mkdir first: POSIX rename replaces an empty dest, Windows refuses.
    external_dataset = tmp_path / "external-dataset"
    dataset_root.rename(external_dataset)
    before_external = sorted(
        path.relative_to(external_dataset) for path in external_dataset.rglob("*")
    )
    dataset_root.symlink_to(external_dataset, target_is_directory=True)
    with pytest.raises(RawArchiveError, match="symlink"):
        archive.records("announcement_index")
    with pytest.raises(RawArchiveError, match="symlink"):
        archive.archive("announcement_index", b"wire-b", source="cninfo")
    assert (
        sorted(path.relative_to(external_dataset) for path in external_dataset.rglob("*"))
        == before_external
    )

    # Restore the normal dataset directory to exercise the leaf payload and
    # sidecar checks independently.
    dataset_root.unlink()
    external_dataset.rename(dataset_root)
    external_payload = tmp_path / "external-payload"
    external_payload.write_bytes(b"not-the-archive")
    payload.unlink()
    payload.symlink_to(external_payload)
    with pytest.raises(RawArchiveError, match="symlink"):
        archive.read(record)
    payload.unlink()
    payload.write_bytes(original_payload)

    external_sidecar = tmp_path / "external-sidecar.json"
    external_sidecar.write_text("{}", encoding="utf-8")
    sidecar.unlink()
    sidecar.symlink_to(external_sidecar)
    with pytest.raises(RawArchiveError, match="symlink"):
        archive.replay(record, lambda data: data)
    with pytest.raises(RawArchiveError, match="symlink"):
        archive.archive(
            "announcement_index",
            b"wire-a",
            source="cninfo",
            captured_at=datetime.fromisoformat(record.captured_at),
        )
    assert external_sidecar.read_text(encoding="utf-8") == "{}"
