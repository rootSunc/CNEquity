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
from cnequity.adapters.cninfo.regulatory import (
    fetch_regulatory_events_range,
    replay_regulatory_events_range,
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
        column = "szse" if data["category"] == announcements._CNINFO_CATEGORIES[0] else "sse"
        key = (column, data["seDate"], data["pageNum"])
        return self.pages.get(
            key,
            _Response({"announcements": [], "totalpages": 0, "hasMore": False}),
        )


def _config(tmp_path: Path):
    return SimpleNamespace(
        meta_root=tmp_path / "meta",
        raw_archive_enabled=True,
        raw_archive_compression="none",
        raw_archive_max_payload_bytes=None,
        should_archive_raw=lambda dataset: dataset in {"announcement_index", "regulatory_events"},
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
    empty = {"announcements": [], "totalpages": 0, "hasMore": False}
    pages = {
        ("szse", "2024-01-01~2024-01-01", 1): _Response(first, b'{"wire":"page-1"}'),
        ("szse", "2024-01-01~2024-01-01", 2): _Response(second, b'{"wire":"page-2"}'),
        ("sse", "2024-01-01~2024-01-01", 1): _Response(empty, b'{"wire":"empty"}'),
    }
    config = _config(tmp_path)
    client = _Client(pages)

    frame = fetch_announcement_index_range(date(2024, 1, 1), client=client, config=config)
    records = RawPayloadArchive(config.meta_root).records("announcement_index")

    assert frame["title"].to_list() == ["v2"]
    assert len(records) == len(client.calls) == 27
    by_page = {
        record.request_params["pageNum"]: record
        for record in records
        if record.request_params["category"] == announcements._CNINFO_CATEGORIES[0]
    }
    assert hashlib.sha256(b'{"wire":"page-1"}').hexdigest() == by_page[1].payload_sha256
    assert by_page[2].pagination["reported_total_pages"] == 2
    assert by_page[2].http_metadata["wire_exact"] is True
    metadata = Path(config.meta_root / by_page[1].metadata_path).read_text(encoding="utf-8")
    assert "do-not-save" not in metadata
    assert "set-cookie" not in metadata


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
        and record.request_params["category"] == announcements._CNINFO_CATEGORIES[0]
    ]
    replayed = replay_announcement_index_range(
        archive, date(2024, 1, 1), date(2024, 1, 2), max_pages_per_slice=2
    )
    assert len(observations) == 2
    assert live["announcement_id"].to_list() == replayed["announcement_id"].to_list()
    assert live["title"].to_list() == replayed["title"].to_list()


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
    reference = saved["slices"]["category_ndbg_szsh:2024-01-01:2024-01-01"]["raw_archives"][0]
    (config.meta_root / reference["payload_path"]).unlink()

    second = _Client(pages)
    fetch_announcement_index_range(
        date(2024, 1, 1), client=second, config=config, checkpoint_path=checkpoint, refresh=False
    )
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
        if item.request_params["category"] == announcements._CNINFO_CATEGORIES[0]
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


def test_regulatory_pages_use_same_archive_and_replay_path(tmp_path):
    payload = {"announcements": [_row("r", "行政处罚公告")], "totalpages": 1, "hasMore": False}
    empty = {"announcements": [], "totalpages": 0, "hasMore": False}
    config = _config(tmp_path)
    client = _Client(
        {
            ("szse", "2024-01-01~2024-01-01", 1): _Response(payload),
            ("sse", "2024-01-01~2024-01-01", 1): _Response(empty),
        }
    )
    live = fetch_regulatory_events_range(date(2024, 1, 1), client=client, config=config)
    archive = RawPayloadArchive(config.meta_root)
    replayed = replay_regulatory_events_range(archive, date(2024, 1, 1))
    records = archive.records("regulatory_events")
    assert len(records) == 26
    assert live.to_dicts() == replayed.to_dicts()
