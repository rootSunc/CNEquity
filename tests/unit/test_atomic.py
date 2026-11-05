"""Atomic parquet replace — including the Windows WinError-32 retry path."""

from __future__ import annotations

import errno
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import polars as pl
import pytest

from cnequity.storage import atomic as atomic_mod
from cnequity.storage.atomic import write_json_atomic, write_parquet_atomic


def _tiny_df() -> pl.DataFrame:
    return pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [date(2024, 6, 28)]})


def test_write_parquet_atomic_roundtrip(tmp_path):
    out = tmp_path / "part.parquet"
    write_parquet_atomic(out, _tiny_df())
    assert out.exists()
    assert pl.read_parquet(out).height == 1


def test_write_parquet_atomic_same_target_is_safe_for_concurrent_refreshes(tmp_path):
    out = tmp_path / "derived.parquet"
    frames = [pl.DataFrame({"value": [index]}) for index in range(8)]

    with ThreadPoolExecutor(max_workers=len(frames)) as pool:
        list(pool.map(lambda frame: write_parquet_atomic(out, frame), frames))

    value = pl.read_parquet(out)["value"][0]
    assert value in range(len(frames))
    assert not list(tmp_path.glob(".*.tmp"))


def test_write_parquet_atomic_retries_permission_error(tmp_path, monkeypatch):
    out = tmp_path / "part.parquet"
    calls = {"n": 0}
    real_replace = atomic_mod.os.replace

    def flaky_replace(src, dst):
        calls["n"] += 1
        if calls["n"] < 3:
            raise PermissionError(32, "file in use")
        return real_replace(src, dst)

    monkeypatch.setattr(atomic_mod.os, "replace", flaky_replace)
    monkeypatch.setattr(atomic_mod.time, "sleep", lambda _: None)

    write_parquet_atomic(out, _tiny_df())
    assert calls["n"] == 3
    assert out.exists()


def test_write_parquet_atomic_raises_after_exhausted_retries(tmp_path, monkeypatch):
    out = tmp_path / "part.parquet"
    monkeypatch.setattr(
        atomic_mod.os,
        "replace",
        lambda *_a, **_k: (_ for _ in ()).throw(PermissionError(32, "file in use")),
    )
    monkeypatch.setattr(atomic_mod.time, "sleep", lambda _: None)

    with pytest.raises(PermissionError):
        write_parquet_atomic(out, _tiny_df())
    assert not out.exists()
    assert not list(tmp_path.glob(".*.tmp"))


def test_write_json_atomic_roundtrip_and_unique_temp_cleanup(tmp_path):
    out = tmp_path / "quality" / "health.json"

    write_json_atomic(out, {"healthy": True, "rows": 3}, indent=2)

    import json

    assert json.loads(out.read_text(encoding="utf-8")) == {"healthy": True, "rows": 3}
    assert not list(out.parent.glob(".*.tmp"))


def test_write_json_atomic_retries_permission_error(tmp_path, monkeypatch):
    out = tmp_path / "quality.json"
    calls = {"n": 0}
    real_replace = atomic_mod.os.replace

    def flaky_replace(src, dst):
        calls["n"] += 1
        if calls["n"] < 3:
            raise PermissionError(32, "file in use")
        return real_replace(src, dst)

    monkeypatch.setattr(atomic_mod.os, "replace", flaky_replace)
    monkeypatch.setattr(atomic_mod.time, "sleep", lambda _: None)

    write_json_atomic(out, {"ok": True})

    assert calls["n"] == 3
    assert out.exists()


def _always_denied(*_a, **_k):
    raise PermissionError(13, "Access is denied")


def test_replace_with_retry_explains_a_persistent_windows_lock(tmp_path, monkeypatch):
    out = tmp_path / "part.parquet"
    out.write_text("old", encoding="utf-8")
    tmp = tmp_path / ".part.parquet.tmp"
    tmp.write_text("new", encoding="utf-8")
    monkeypatch.setattr(atomic_mod, "IS_WINDOWS", True)
    monkeypatch.setattr(atomic_mod.os, "replace", _always_denied)
    monkeypatch.setattr(atomic_mod.time, "sleep", lambda _: None)

    with pytest.raises(atomic_mod.FileInUseError, match="in use by another program") as caught:
        atomic_mod.replace_with_retry(tmp, out)
    # Existing ``except PermissionError`` handlers keep working.
    assert isinstance(caught.value, PermissionError)
    assert str(out) in str(caught.value)


def test_replace_with_retry_keeps_the_raw_error_off_windows(tmp_path, monkeypatch):
    monkeypatch.setattr(atomic_mod, "IS_WINDOWS", False)
    monkeypatch.setattr(atomic_mod.os, "replace", _always_denied)
    monkeypatch.setattr(atomic_mod.time, "sleep", lambda _: None)

    with pytest.raises(PermissionError) as caught:
        atomic_mod.replace_with_retry(tmp_path / "a", tmp_path / "b")
    assert not isinstance(caught.value, atomic_mod.FileInUseError)


def _tree(root, text):
    root.mkdir()
    (root / "part.parquet").write_text(text, encoding="utf-8")


def test_swap_with_backup_promotes_staged_and_keeps_old_at_backup(tmp_path):
    target, staged, backup = tmp_path / "data", tmp_path / "staged", tmp_path / ".data-old"
    _tree(target, "old")
    _tree(staged, "new")

    atomic_mod.swap_with_backup(staged, target, backup)

    assert (target / "part.parquet").read_text(encoding="utf-8") == "new"
    assert (backup / "part.parquet").read_text(encoding="utf-8") == "old"


def test_swap_with_backup_puts_the_original_back_when_promotion_fails(tmp_path, monkeypatch):
    target, staged, backup = tmp_path / "data", tmp_path / "staged", tmp_path / ".data-old"
    _tree(target, "old")
    _tree(staged, "new")
    real_replace = atomic_mod.os.replace

    def deny_promotion(src, dst):
        if src == staged:
            raise PermissionError(13, "Access is denied")
        real_replace(src, dst)

    monkeypatch.setattr(atomic_mod.os, "replace", deny_promotion)
    monkeypatch.setattr(atomic_mod.time, "sleep", lambda _: None)

    with pytest.raises(PermissionError):
        atomic_mod.swap_with_backup(staged, target, backup)
    assert (target / "part.parquet").read_text(encoding="utf-8") == "old"
    assert not backup.exists()


def test_swap_with_backup_names_the_backup_when_restore_also_fails(tmp_path, monkeypatch):
    target, staged, backup = tmp_path / "data", tmp_path / "staged", tmp_path / ".data-old"
    _tree(target, "old")
    _tree(staged, "new")
    real_replace = atomic_mod.os.replace

    def deny_after_move_aside(src, dst):
        if src == target:
            return real_replace(src, dst)
        raise PermissionError(13, "Access is denied")

    monkeypatch.setattr(atomic_mod.os, "replace", deny_after_move_aside)
    monkeypatch.setattr(atomic_mod.time, "sleep", lambda _: None)

    with pytest.raises(atomic_mod.RollbackIncompleteError, match=re.escape(str(backup))):
        atomic_mod.swap_with_backup(staged, target, backup)
    assert (backup / "part.parquet").read_text(encoding="utf-8") == "old"


def _no_hard_links(monkeypatch, err=errno.ENOTSUP):
    def refuse(src, dst):
        raise OSError(err, "Operation not supported", str(src), None, str(dst))

    monkeypatch.setattr(atomic_mod.os, "link", refuse)


def test_raw_archive_writes_payloads_where_hard_links_are_unsupported(tmp_path, monkeypatch):
    """exFAT/FAT and some network shares refuse ``link()`` with ENOTSUP."""
    from cnequity.storage.raw_archive import RawPayloadArchive

    _no_hard_links(monkeypatch)
    archive = RawPayloadArchive(tmp_path / "meta")
    first = archive.archive("daily_bars", {"k": 1}, source="tdx_protocol")
    again = archive.archive("daily_bars", {"k": 1}, source="tdx_protocol")

    assert again.payload_path == first.payload_path
    payload = tmp_path / "meta" / first.payload_path
    assert payload.is_file()
    assert [p.name for p in payload.parent.iterdir() if p.name.startswith(".")] == []


def test_publish_once_keeps_an_existing_target_without_hard_links(tmp_path, monkeypatch):
    _no_hard_links(monkeypatch)
    target = tmp_path / "payload"
    target.write_bytes(b"first")
    temporary = tmp_path / ".payload.tmp"
    temporary.write_bytes(b"second")

    assert atomic_mod.publish_once(temporary, target) is False
    assert target.read_bytes() == b"first"
    assert not temporary.exists()


def test_publish_once_raises_link_errors_that_are_not_unsupported(tmp_path, monkeypatch):
    _no_hard_links(monkeypatch, errno.EACCES)
    temporary = tmp_path / ".payload.tmp"
    temporary.write_bytes(b"x")

    with pytest.raises(PermissionError):
        atomic_mod.publish_once(temporary, tmp_path / "payload")


def test_immutable_evidence_writers_work_where_hard_links_are_unsupported(tmp_path, monkeypatch):
    from cnequity.adapters.cninfo.reviewed_holder_notices import save_holder_evidence
    from cnequity.quality.decision_gaps import save_immutable

    _no_hard_links(monkeypatch)
    gaps = save_immutable({"events": [1]}, tmp_path / "gaps")
    assert save_immutable({"events": [1]}, tmp_path / "gaps") == gaps
    entries = [{"ex_date": "2020-06-19"}]
    holder = save_holder_evidence(tmp_path / "meta", "run-1", "600025.SH", entries)
    assert save_holder_evidence(tmp_path / "meta", "run-1", "600025.SH", entries) == holder
    for path in (gaps, holder):
        assert [p.name for p in path.parent.iterdir()] == [path.name]

    # An existing file is still never replaced, and a mismatch still fails.
    gaps.write_text("corrupt")
    holder.write_text("corrupt")
    with pytest.raises(ValueError, match="different content"):
        save_immutable({"events": [1]}, tmp_path / "gaps")
    with pytest.raises(ValueError, match="hash collision"):
        save_holder_evidence(tmp_path / "meta", "run-1", "600025.SH", entries)
    assert gaps.read_text() == holder.read_text() == "corrupt"
