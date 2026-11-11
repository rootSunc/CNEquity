"""The mkdir lock shared by the shell pipelines and the operations page."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from cnequity.config import Config
from cnequity.orchestrator.scheduler_lock import (
    SchedulerLockError,
    _proc_state,
    host_identity,
    lock_directory,
    pid_alive,
    scheduler_lock,
    scheduler_lock_holder,
)

ROOT = Path(__file__).resolve().parents[2]


def test_an_unreaped_exit_is_not_a_live_process(monkeypatch):
    assert _proc_state("12 (cne job) Z 1 1") == "Z"
    assert _proc_state("12 (name) with) paren) S 1") == "S"
    if sys.platform == "win32":
        return
    monkeypatch.setattr(
        "cnequity.orchestrator.scheduler_lock._zombie",
        lambda pid: True,
    )
    assert pid_alive(os.getpid()) is False


def test_lock_directory_follows_the_data_root_unless_overridden(tmp_path, monkeypatch):
    config = Config(data_root=tmp_path / "lake")
    monkeypatch.delenv("CNE_SCHEDULER_LOCK_DIR", raising=False)
    monkeypatch.delenv("CNE_LOCK_DIR", raising=False)
    assert lock_directory(config) == (tmp_path / "lake" / "locks").resolve()
    monkeypatch.setenv("CNE_SCHEDULER_LOCK_DIR", str(tmp_path / "custom"))
    assert lock_directory(config) == (tmp_path / "custom").resolve()


def test_a_dead_owner_is_reclaimed_and_a_live_one_blocks(tmp_path):
    root = tmp_path / "locks"
    stale = root / "daily.lock"
    stale.mkdir(parents=True)
    (stale / "pid").write_text(
        json.dumps({"pid": 2**31 - 1, "host": host_identity(), "start": "old"}), encoding="utf-8"
    )
    with scheduler_lock(root, "daily"):
        assert scheduler_lock_holder(root, "daily") == os.getpid()
    assert scheduler_lock_holder(root, "daily") is None


def test_a_remote_owner_is_not_reclaimed(tmp_path):
    marker = '{"pid":99999999,"host":"other-host","start":"old"}'
    directory = tmp_path / "daily.lock"
    directory.mkdir()
    (directory / "pid").write_text(marker)
    assert scheduler_lock_holder(tmp_path, "daily") == 99999999
    with pytest.raises(SchedulerLockError) as caught:
        with scheduler_lock(tmp_path, "daily"):
            pass
    assert caught.value.code == 1
    assert (directory / "pid").read_text() == marker


def test_a_legacy_pid_marker_is_reclaimed_once_its_process_is_gone(tmp_path):
    directory = tmp_path / "daily.lock"
    directory.mkdir()
    (directory / "pid").write_text("99999999\n")
    assert scheduler_lock_holder(tmp_path, "daily") is None
    with scheduler_lock(tmp_path, "daily"):
        assert scheduler_lock_holder(tmp_path, "daily") == os.getpid()
    assert not directory.exists()


def test_a_legacy_pid_marker_of_a_live_process_blocks(tmp_path):
    directory = tmp_path / "daily.lock"
    directory.mkdir()
    (directory / "pid").write_text(f"{os.getpid()}\n")
    assert scheduler_lock_holder(tmp_path, "daily") == os.getpid()
    with pytest.raises(SchedulerLockError):
        with scheduler_lock(tmp_path, "daily"):
            pass
    assert directory.exists()


def test_an_ownerless_directory_is_reclaimed_only_after_the_grace(tmp_path):
    directory = tmp_path / "daily.lock"
    directory.mkdir()
    (directory / ".pid.abc.tmp").write_text("{")
    assert scheduler_lock_holder(tmp_path, "daily") == -1
    with pytest.raises(SchedulerLockError):
        with scheduler_lock(tmp_path, "daily"):
            pass
    old = directory.stat().st_mtime - 3600
    os.utime(directory, (old, old))
    assert scheduler_lock_holder(tmp_path, "daily") is None
    with scheduler_lock(tmp_path, "daily"):
        assert sorted(p.name for p in directory.iterdir()) == ["pid"]
    assert not directory.exists()


def test_reused_local_pid_does_not_keep_a_dead_owners_lock(tmp_path, monkeypatch):
    directory = tmp_path / "daily.lock"
    directory.mkdir()
    (directory / "pid").write_text(
        json.dumps({"pid": os.getpid(), "host": host_identity(), "start": "old"})
    )
    monkeypatch.setattr("cnequity.orchestrator.scheduler_lock.process_start", lambda pid: "new")
    with scheduler_lock(tmp_path, "daily"):
        assert json.loads((directory / "pid").read_text())["start"] == "new"
    assert not directory.exists()


def test_another_process_blocks_until_it_exits(tmp_path):
    root = tmp_path / "locks"
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import sys, time\n"
            "from pathlib import Path\n"
            "from cnequity.orchestrator.scheduler_lock import scheduler_lock\n"
            "with scheduler_lock(Path(sys.argv[1]), 'events'):\n"
            "    print('held', flush=True)\n"
            "    time.sleep(30)\n",
            str(root),
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert child.stdout.readline().strip() == "held"
    with pytest.raises(SchedulerLockError) as caught:
        with scheduler_lock(root, "events"):
            pass
    assert caught.value.code == 1
    child.kill()
    child.wait(timeout=5)
    with scheduler_lock(root, "events"):
        assert scheduler_lock_holder(root, "events") == os.getpid()


def test_a_mac_keeps_its_identity_when_the_hostname_changes(monkeypatch):
    from cnequity.orchestrator import scheduler_lock as module

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_platform_uuid", lambda: "1228759C-BF6E-562C-9599-26651563D05C")
    monkeypatch.setattr(module.socket, "gethostname", lambda: "laptop.home")
    before = host_identity()
    monkeypatch.setattr(module.socket, "gethostname", lambda: "laptop.office")
    assert host_identity() == before == "mac:1228759C-BF6E-562C-9599-26651563D05C"
