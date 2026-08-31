"""CNINFO range pagination: date splitting, no-progress recovery and resume."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest

from cnequity.adapters.cninfo import announcements
from cnequity.adapters.cninfo.announcements import (
    fetch_announcement_index_range,
    invalidate_cninfo_checkpoint,
)
from cnequity.config import Config
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps import events


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
            if data["column"] == "sse":
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
            if data["column"] == "sse":
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
            if data["column"] == "sse":
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
    slice_state = saved["slices"]["szse:2024-03-01:2024-03-01"]
    assert slice_state["next_page"] == 2
    assert [call["pageNum"] for call in first.calls] == [1, 2]

    class ResumesPageTwo(FailsPageTwo):
        def post(self, _url, data):
            self.calls.append(dict(data))
            if data["column"] == "sse":
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
    assert [call["pageNum"] for call in second.calls] == [2, 1]


def test_cninfo_total_is_raw_rows_while_final_frame_uses_unique_keys(tmp_path):
    checkpoint = tmp_path / "cninfo.json"

    class DuplicateClient:
        def __init__(self):
            self.calls = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            if data["column"] == "sse":
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
    szse = saved["slices"]["szse:2024-04-01:2024-04-01"]
    assert szse["raw_row_count"] == 2
    assert szse["unique_keys"] == ["same"]
    assert len(szse["page_signatures"]) == 1
    assert len(szse["page_signatures"][0]) == 64


def test_cninfo_rejects_raw_row_overrun_even_when_ids_are_distinct():
    """A source total is an exact raw-row count, not a normalized-row bound."""

    class OverrunClient:
        def post(self, _url, data):
            if data["column"] == "sse":
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
            if data["column"] == "sse":
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
    assert len(first.calls) == 2

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
    assert len(refreshed.calls) == 2

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
    assert len(revised_source.calls) == 2

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
    assert len(expired.calls) == 2

    invalidate_cninfo_checkpoint(checkpoint)
    assert not checkpoint.exists()


def test_single_day_repeated_page_101_is_terminal_but_resumable(tmp_path):
    """A no-progress page at the unsplittable day boundary must not truncate.

    The source has historically repeated page 1 around page 101.  A broad
    range can be split by date, but one day cannot; persist the failed page and
    let a later invocation retry that page instead of returning the first 100
    rows as a successful result.
    """
    checkpoint = tmp_path / "cninfo.json"
    target = date(2024, 5, 6)

    class RepeatsAtPage101:
        def __init__(self):
            self.calls: list[dict] = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            if data["column"] == "sse":
                return _Response({"announcements": [], "hasMore": False})
            page = data["pageNum"]
            # Page 101 is an exact replay of page 1.  Use max_pages=200 so the
            # repeated-page detector, rather than the page cap, is exercised.
            identity = 1 if page == 101 else page
            return _Response(
                {
                    "announcements": [_row(f"p{identity}", target.isoformat())],
                    "hasMore": True,
                }
            )

    first = RepeatsAtPage101()
    with pytest.raises(RuntimeError, match="page 101:.*repeated page"):
        fetch_announcement_index_range(
            target,
            client=first,
            checkpoint_path=checkpoint,
            max_pages_per_slice=200,
        )
    assert [call["pageNum"] for call in first.calls] == list(range(1, 102))
    saved = json.loads(checkpoint.read_text(encoding="utf-8"))
    state = saved["slices"][f"szse:{target}:{target}"]
    assert state["status"] == "failed"
    assert state["failure_page"] == 101
    assert state["next_page"] == 101
    assert state["pages"] == 100

    class ResumesAtPage101(RepeatsAtPage101):
        def post(self, _url, data):
            self.calls.append(dict(data))
            if data["column"] == "sse":
                return _Response({"announcements": [], "hasMore": False})
            if data["pageNum"] != 101:
                raise AssertionError("a resumable single-day failure must retry page 101")
            return _Response(
                {
                    "announcements": [_row("p101", target.isoformat())],
                    "hasMore": False,
                }
            )

    second = ResumesAtPage101()
    frame = fetch_announcement_index_range(
        target,
        client=second,
        checkpoint_path=checkpoint,
        max_pages_per_slice=200,
        refresh=False,
    )
    assert frame.height == 101
    assert [call["pageNum"] for call in second.calls] == [101, 1]


def test_cninfo_step_uses_registry_date_chunks_for_default_backfill(tmp_path, monkeypatch):
    """The step must bound a direct/default backfill, not just CLI ranges."""
    config = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    config._backfill_start = date(2024, 1, 1)
    config._backfill_end = date(2024, 2, 10)
    calls: list[tuple[date, date]] = []

    def fetch_range(start, end, **_kwargs):
        calls.append((start, end))
        return pl.DataFrame(
            [
                {
                    "announcement_id": f"{start}-{offset}",
                    "symbol": "000001.SZ",
                    "title": "公告",
                    "announce_date": start + timedelta(days=offset),
                    "category": "",
                    "url": "",
                }
                for offset in range((end - start).days + 1)
            ]
        )

    monkeypatch.setattr(events, "_record_cninfo_metrics", lambda *_args, **_kwargs: None)
    result = events._cninfo_range_backfill(
        config,
        date(2024, 2, 10),
        "run-cninfo-chunks",
        "announcement_index",
        fetch_range,
        date_col="announce_date",
        floor=date(2010, 1, 1),
    )

    assert calls == [(date(2024, 1, 1), date(2024, 1, 31)), (date(2024, 2, 1), date(2024, 2, 10))]
    assert len(result["slices"]) == 2
    assert result["rows_written"] == 41
    staged = list((config.staging_root / "announcement_index").rglob("*.parquet"))
    assert pl.concat([pl.read_parquet(path) for path in staged]).height == 41
    # The helper temporarily narrows the private bounds and must not leak its
    # final slice into a caller that reuses the Config object.
    assert config._backfill_start == date(2024, 1, 1)
    assert config._backfill_end == date(2024, 2, 10)


def test_failed_contract_checkpoint_restarts_at_page_one(tmp_path):
    checkpoint = tmp_path / "contract.json"
    target = date(2024, 6, 3)

    class BadTotal:
        def post(self, _url, data):
            if data["column"] == "sse":
                return _Response({"announcements": [], "total": 0, "totalpages": 0})
            return _Response(
                {
                    "announcements": [_row("old", target.isoformat())],
                    "total": 2,
                    "totalpages": 1,
                    "hasMore": False,
                }
            )

    with pytest.raises(RuntimeError, match="raw row count"):
        fetch_announcement_index_range(target, client=BadTotal(), checkpoint_path=checkpoint)
    failed = json.loads(checkpoint.read_text())["slices"]["szse:2024-06-03:2024-06-03"]
    assert failed["status"] == "failed"
    assert failed["next_page"] == 1
    assert failed["rows"] == []

    class Fixed:
        def __init__(self):
            self.calls = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            if data["column"] == "sse":
                return _Response({"announcements": [], "total": 0, "totalpages": 0})
            return _Response(
                {
                    "announcements": [
                        _row("new-1", target.isoformat()),
                        _row("new-2", target.isoformat()),
                    ],
                    "total": 2,
                    "totalpages": 1,
                    "hasMore": False,
                }
            )

    fixed = Fixed()
    frame = fetch_announcement_index_range(
        target, client=fixed, checkpoint_path=checkpoint, refresh=False
    )
    assert set(frame["announcement_id"]) == {"new-1", "new-2"}
    assert fixed.calls[0]["pageNum"] == 1


def test_expired_failed_checkpoint_restarts_instead_of_resuming(tmp_path):
    checkpoint = tmp_path / "expired-failed.json"
    target = date(2024, 6, 4)

    class RepeatedPage:
        def post(self, _url, data):
            if data["column"] == "sse":
                return _Response({"announcements": [], "hasMore": False})
            return _Response({"announcements": [_row("same", target.isoformat())], "hasMore": True})

    with pytest.raises(RuntimeError, match="repeated page"):
        fetch_announcement_index_range(
            target, client=RepeatedPage(), checkpoint_path=checkpoint, max_pages_per_slice=10
        )
    saved = json.loads(checkpoint.read_text())
    old = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    record = saved["slices"]["szse:2024-06-04:2024-06-04"]
    record["failed_at"] = old
    record["updated_at"] = old
    saved["updated_at"] = old
    checkpoint.write_text(json.dumps(saved), encoding="utf-8")

    class Fixed:
        def __init__(self):
            self.calls = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            if data["column"] == "sse":
                return _Response({"announcements": [], "hasMore": False})
            return _Response(
                {"announcements": [_row("fixed", target.isoformat())], "hasMore": False}
            )

    fixed = Fixed()
    frame = fetch_announcement_index_range(
        target,
        client=fixed,
        checkpoint_path=checkpoint,
        refresh=False,
        checkpoint_ttl_days=1,
    )
    assert frame["announcement_id"].to_list() == ["fixed"]
    assert fixed.calls[0]["pageNum"] == 1


def test_broad_range_missing_date_splits_to_single_days():
    class Client:
        def __init__(self):
            self.calls = []

        def post(self, _url, data):
            self.calls.append(dict(data))
            if data["column"] == "sse":
                return _Response({"announcements": [], "hasMore": False})
            start, end = data["seDate"].split("~")
            if start != end:
                row = _row("missing", start)
                row.pop("announcementDate")
                return _Response({"announcements": [row], "hasMore": False})
            return _Response({"announcements": [_row(start, start)], "hasMore": False})

    client = Client()
    frame = fetch_announcement_index_range(date(2024, 6, 5), date(2024, 6, 6), client=client)
    assert frame.height == 2
    assert {call["seDate"] for call in client.calls} >= {
        "2024-06-05~2024-06-05",
        "2024-06-06~2024-06-06",
    }


def test_range_scoped_checkpoint_paths_do_not_overwrite_other_dates(tmp_path):
    config = Config(data_root=tmp_path / "data")
    historical = events._cninfo_checkpoint_options(
        config, "announcement_index", date(2020, 1, 1), date(2020, 1, 31)
    )["checkpoint_path"]
    daily = events._cninfo_checkpoint_options(
        config, "announcement_index", date(2024, 6, 7), date(2024, 6, 7)
    )["checkpoint_path"]
    assert historical != daily
    assert historical.parent == daily.parent


def test_sparse_cninfo_range_treats_empty_dates_as_confirmed_observations(tmp_path, monkeypatch):
    config = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    config._backfill_start = date(2024, 6, 8)
    config._backfill_end = date(2024, 6, 10)

    def fetch_range(_start, _end, **_kwargs):
        return pl.DataFrame(
            [
                {
                    "announcement_id": "only-event",
                    "symbol": "000001.SZ",
                    "title": "公告",
                    "announce_date": date(2024, 6, 9),
                    "category": "",
                    "url": "",
                }
            ]
        )

    monkeypatch.setattr(events, "_record_cninfo_metrics", lambda *_args, **_kwargs: None)
    result = events._cninfo_range_backfill(
        config,
        date(2024, 6, 10),
        "run-sparse-cninfo",
        "announcement_index",
        fetch_range,
        date_col="announce_date",
        floor=date(2010, 1, 1),
    )

    assert result.get("status", "success") == "success"
    assert result["days_empty"] == 0
    assert result["days_fetched"] == 3
    staged = list((config.staging_root / "announcement_index").rglob("*.parquet"))
    assert pl.read_parquet(staged).height == 1


def test_failed_later_cninfo_slice_persists_cumulative_request_retries(tmp_path):
    config = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    config._backfill_start = date(2024, 1, 1)
    config._backfill_end = date(2024, 2, 10)
    manifest = Manifest(config.manifest_path)
    run_id = manifest.start_run("backfill:announcement_index")
    batch_id = "cninfo-range-batch"
    manifest.start_batch(
        run_id,
        batch_id,
        "announcement_index",
        "announcement_index",
        blocks_compaction=False,
    )
    calls = 0

    def fetch_range(start, end, *, metrics, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            metrics["retries"] = 2
            return pl.DataFrame(
                [
                    {
                        "announcement_id": "first-slice",
                        "symbol": "000001.SZ",
                        "title": "公告",
                        "announce_date": start,
                        "category": "",
                        "url": "",
                    }
                ]
            )
        metrics["retries"] = 3
        raise RuntimeError("second slice unavailable")

    with pytest.raises(RuntimeError, match="second slice unavailable"):
        events._cninfo_range_backfill(
            config,
            date(2024, 2, 10),
            run_id,
            "announcement_index",
            fetch_range,
            date_col="announce_date",
            floor=date(2010, 1, 1),
            batch_id=batch_id,
        )

    batch = next(row for row in manifest.get_batches_for_run(run_id) if row["batch_id"] == batch_id)
    assert batch["request_retry_count"] == 5
    performance = manifest.get_run_metadata(run_id)["performance"]["announcement_index"]
    assert performance["retries"] == 5
    assert performance["slice_count"] == 2
