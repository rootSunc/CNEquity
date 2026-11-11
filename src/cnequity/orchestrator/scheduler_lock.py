"""The scheduler's mkdir lock, as a Python context manager.

macOS does not ship ``flock``, so a scheduled run holds a directory with an
owner marker. The marker records host, PID, process start and a release
token. The dashboard's daily and events jobs take the same directory. Two different lock files would
let a panel run and a launchd run both believe they own the session.

``CNE_SCHEDULER_LOCK_DIR`` and the older ``CNE_LOCK_DIR`` still win. Otherwise
the directory is ``{data.root}/locks``, which is where a ``cne serve`` process
can see it.
"""

from __future__ import annotations

import functools
import json
import os
import re
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from cnequity.config import Config
from cnequity.file_lock import LockUnavailable, exclusive_lock
from cnequity.storage.atomic import write_json_atomic

IS_WINDOWS = sys.platform == "win32"


class SchedulerLockError(RuntimeError):
    """``code`` matches the shell helper: 1 is busy, 2 is an I/O failure."""

    def __init__(self, message: str, *, code: int):
        super().__init__(message)
        self.code = code


def lock_directory(config: Config) -> Path:
    """Where both the shell and the panel look for ``daily.lock`` / ``events.lock``."""
    override = os.environ.get("CNE_SCHEDULER_LOCK_DIR") or os.environ.get("CNE_LOCK_DIR")
    if override:
        return Path(override).expanduser().resolve()
    return Path(config.data_root).resolve() / "locks"


def _proc_state(stat_text: str) -> str | None:
    """The state field from ``/proc/<pid>/stat``. ``comm`` may contain spaces."""
    end = stat_text.rfind(")")
    if end < 0:
        return None
    fields = stat_text[end + 1 :].split()
    return fields[0] if fields else None


def _zombie(pid: int) -> bool:
    """An exited process whose parent has not waited is not doing work.

    ``os.kill(pid, 0)`` still succeeds for that pid, which made a cancelled
    panel job look running until the parent reaped it.
    """
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except OSError:
        return False
    return _proc_state(text) == "Z"


def pid_alive(pid: int) -> bool:
    """Whether *pid* is a live process.

    ``os.kill(pid, 0)`` on Windows is not a probe: it terminates the process.
    A handle query is the equivalent of the shell's ``kill -0``.
    """
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        return False
    if IS_WINDOWS:
        from cnequity.windows_process import pid_alive as windows_pid_alive

        return windows_pid_alive(pid)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return not _zombie(pid)


@functools.cache
def _platform_uuid() -> str | None:
    """The Mac's hardware UUID, which survives network and hostname changes."""
    try:
        result = subprocess.run(
            ["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    found = re.search(r'"IOPlatformUUID"\s*=\s*"([0-9A-Fa-f-]{36})"', result.stdout)
    return found.group(1).upper() if found else None


def host_identity() -> str:
    # macOS rewrites the hostname when a laptop joins another network. A marker
    # written under the old name would then read as a remote owner and hold
    # the lock until a maintainer removed it.
    if sys.platform == "darwin":
        hardware = _platform_uuid()
        if hardware:
            return f"mac:{hardware}"
    return f"{socket.gethostname()}:{uuid.getnode():012x}"


def process_start(pid: int) -> str | None:
    if IS_WINDOWS:
        from cnequity.windows_process import creation_time

        return creation_time(pid)
    try:
        text = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
        fields = text[text.rfind(")") + 1 :].split()
        boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        return f"{boot}:{fields[19]}"
    except (OSError, IndexError):
        pass
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "lstart="],
            capture_output=True,
            text=True,
            timeout=5,
            env={**os.environ, "LC_ALL": "C"},
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.stdout.strip() or None


def _read_owner(path: Path) -> dict | None:
    try:
        text = path.read_text(encoding="utf-8").strip()
        if text.isdigit():
            return {"pid": int(text)}  # Legacy marker: host identity is unknown.
        value = json.loads(text)
        if isinstance(value, dict) and type(value.get("pid")) is int and value["pid"] > 0:
            return value
    except (OSError, ValueError):
        pass
    return None


#: An ownerless lock directory older than this is a writer that died between
#: ``mkdir`` and its marker; every current writer does both under the guard.
OWNERLESS_GRACE_SECONDS = 60


def _local_hosts() -> set[str]:
    # The second spelling is what development builds before the hardware UUID
    # wrote on a Mac; a marker it left must still be recognised as local.
    return {host_identity(), f"{socket.gethostname()}:{uuid.getnode():012x}"}


def _owner_active(owner: dict) -> bool:
    host = owner.get("host")
    if host is None:
        # A pid-only marker from 0.16 and earlier. Those scripts only ever ran
        # against a local lock directory, so a dead PID means a dead owner.
        return pid_alive(owner["pid"])
    # A remote owner's PID is not meaningful on this host.
    if host not in _local_hosts():
        return True
    if not pid_alive(owner["pid"]):
        return False
    start = process_start(owner["pid"])
    return not start or not owner.get("start") or start == owner["start"]


def _ownerless_is_young(directory: Path) -> bool:
    try:
        age = time.time() - directory.stat().st_mtime
    except OSError:
        return False
    return age < OWNERLESS_GRACE_SECONDS


def _paths(root: Path, name: str) -> tuple[Path, Path]:
    if name in {".", ".."} or not re.fullmatch(r"[A-Za-z0-9_.-]+", name):
        raise ValueError("invalid scheduler lock name")
    directory = root / f"{name}.lock"
    return directory, directory / "pid"


def scheduler_lock_holder(root: Path, name: str) -> int | None:
    """PID marker for a live or unverified owner, including remote owners."""
    directory, marker = _paths(Path(root), name)
    owner = _read_owner(marker)
    if owner and _owner_active(owner):
        return owner["pid"]
    # An ownerless directory can be a process between mkdir and marker write.
    if owner is None and directory.exists() and _ownerless_is_young(directory):
        return -1
    return None


def acquire_scheduler_lock(root: Path, name: str, *, pid: int) -> dict:
    root = Path(root)
    directory, marker = _paths(root, name)
    owner = {
        "pid": pid,
        "host": host_identity(),
        "start": process_start(pid),
        "token": uuid.uuid4().hex,
    }
    if not pid_alive(pid):
        raise SchedulerLockError("scheduler owner process is not alive", code=2)
    try:
        root.mkdir(parents=True, exist_ok=True)
        # All new shell/panel writers serialize the short marker transition.
        # Keep the guard inode: unlinking it would split waiting acquirers.
        with exclusive_lock(root / f".{name}.guard", blocking=False):
            if directory.exists():
                previous = _read_owner(marker)
                if _ownerless_is_young(directory) if previous is None else _owner_active(previous):
                    raise SchedulerLockError(
                        f"调度锁 {name} 正被占用或其主机身份尚未核验。", code=1
                    )
                marker.unlink(missing_ok=True)
                # A marker write interrupted by a crash leaves its temp file.
                for leftover in directory.glob(".pid.*.tmp"):
                    leftover.unlink()
                directory.rmdir()  # Never recursively remove an unexpected file.
            directory.mkdir()
            try:
                write_json_atomic(marker, owner)
            except OSError:
                directory.rmdir()
                raise
        return owner
    except LockUnavailable as exc:
        raise SchedulerLockError(f"调度锁 {name} 正被占用。", code=1) from exc
    except OSError as exc:
        raise SchedulerLockError(f"无法取得调度锁 {name}（{exc}）", code=2) from exc


def release_scheduler_lock(root: Path, name: str, owner: dict) -> None:
    directory, marker = _paths(Path(root), name)
    with exclusive_lock(Path(root) / f".{name}.guard", blocking=True, timeout=5):
        if _read_owner(marker) == owner:
            marker.unlink()
            directory.rmdir()


@contextmanager
def scheduler_lock(root: Path, name: str) -> Iterator[None]:
    owner = acquire_scheduler_lock(root, name, pid=os.getpid())
    try:
        yield
    finally:
        release_scheduler_lock(root, name, owner)
