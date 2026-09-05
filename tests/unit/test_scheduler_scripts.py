"""Regression tests for the macOS/cron scheduler wrappers.

These tests intentionally drive the shell entry points with a tiny fake ``cne``
binary.  They do not load launchd agents or touch the user's LaunchAgents
directory.
"""

from __future__ import annotations

import os
import plistlib
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DAILY = ROOT / "scripts" / "daily_pipeline.sh"
STALE = ROOT / "scripts" / "stale_pipeline.sh"
EVENTS = ROOT / "scripts" / "events_pipeline.sh"
DAILY_PLIST = ROOT / "scripts" / "launchd" / "com.cnequity.daily.plist.template"
STALE_PLIST = ROOT / "scripts" / "launchd" / "com.cnequity.stale.plist.template"
EVENTS_PLIST = ROOT / "scripts" / "launchd" / "com.cnequity.events.plist.template"


def _ensure_unix_shell() -> None:
    if sys.platform == "win32":
        pytest.skip("scheduler wrappers are Unix shell scripts")


def _run(
    script: Path, *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    _ensure_unix_shell()
    return subprocess.run(
        [str(script), *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def _popen(script: Path, env: dict[str, str]) -> subprocess.Popen[str]:
    _ensure_unix_shell()
    return subprocess.Popen(
        [str(script)], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )


def _stub_cne(tmp_path: Path) -> Path:
    path = tmp_path / "cne"
    path.write_text(
        """#!/bin/sh
printf 'argc=%s\\n' "$#" >> "$CNE_CALL_LOG"
for arg in "$@"; do printf '<%s>\\n' "$arg" >> "$CNE_CALL_LOG"; done
sleep "${CNE_STUB_SLEEP:-0}"
exit "${CNE_STUB_STATUS:-0}"
""",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _stale_env(tmp_path: Path, cne: Path, calls: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "CNE_BIN": str(cne),
            "CNE_CALL_LOG": str(calls),
            "CNE_CONFIG": str(tmp_path / "cnequity.toml"),
            "CNE_LOG_DIR": str(tmp_path / "logs"),
            "CNE_SCHEDULER_LOCK_DIR": str(tmp_path / "locks"),
        }
    )
    return env


def _call_args(calls: Path) -> list[str]:
    lines = calls.read_text(encoding="utf-8").splitlines()
    return [line[1:-1] for line in lines if line.startswith("<") and line.endswith(">")]


def test_daily_template_schedules_all_groups_and_disables_inline_wait():
    payload = plistlib.loads(DAILY_PLIST.read_bytes())
    assert payload["Label"] == "com.cnequity.daily"
    assert payload["EnvironmentVariables"]["CNE_GROUPS"] == (
        "core capital signals fundamentals macro_risk research"
    )
    assert payload["EnvironmentVariables"]["CNE_STALE_RETRY"] == "0"

    source = DAILY.read_text(encoding="utf-8")
    assert 'STALE_RETRY="${CNE_STALE_RETRY:-0}"' in source
    # Keep the old sleep path available only behind the explicit compatibility
    # switch; the installed plist above turns it off.
    assert 'sleep "$STALE_RETRY_DELAY_SEC"' in source


def test_stale_template_is_a_late_independent_agent():
    payload = plistlib.loads(STALE_PLIST.read_bytes())
    assert payload["Label"] == "com.cnequity.stale"
    assert payload["ProgramArguments"][-1].endswith("scripts/stale_pipeline.sh")
    assert payload["StartCalendarInterval"] == {"Hour": 20, "Minute": 5}


def test_events_template_runs_every_calendar_day_on_its_own_lock():
    """The whole point: no trading-day gate, and not behind the daily lock."""
    payload = plistlib.loads(EVENTS_PLIST.read_bytes())
    assert payload["Label"] == "com.cnequity.events"
    assert payload["ProgramArguments"][-1].endswith("scripts/events_pipeline.sh")
    # A weekday filter here would put the market calendar back in the path.
    assert "Weekday" not in payload["StartCalendarInterval"]
    assert 'scheduler_lock_acquire "$REPO_ROOT" events' in EVENTS.read_text(encoding="utf-8")


def test_events_pipeline_forwards_group_and_date_and_returns_cne_failure(tmp_path):
    cne = _stub_cne(tmp_path)
    calls = tmp_path / "calls"
    env = _stale_env(tmp_path, cne, calls)
    env["CNE_STUB_STATUS"] = "5"
    env["CNE_EVENTS_GROUP"] = "news_wire"

    result = _run(EVENTS, "2026-08-30", env=env)

    assert result.returncode == 5
    assert _call_args(calls) == [
        "run",
        "events",
        "--config",
        str(tmp_path / "cnequity.toml"),
        "--trade-date",
        "2026-08-30",
        "--group",
        "news_wire",
    ]
    assert "FAILED" in result.stdout


def test_an_event_sweep_is_not_blocked_by_the_daily_wrapper(tmp_path):
    """A sweep skipped because the evening batch is running is a sweep lost."""
    cne = _stub_cne(tmp_path)
    calls = tmp_path / "calls"
    env = _stale_env(tmp_path, cne, calls)
    env["CNE_STUB_SLEEP"] = "1"

    daily_lock = tmp_path / "locks" / "daily.lock"
    daily_lock.mkdir(parents=True)
    (daily_lock / "pid").write_text(f"{os.getpid()}\n", encoding="utf-8")

    result = _run(EVENTS, env=env)

    assert result.returncode == 0
    assert "skipping" not in result.stdout
    assert _call_args(calls)[:2] == ["run", "events"]


def test_stale_pipeline_forwards_target_date_and_returns_cne_failure(tmp_path):
    cne = _stub_cne(tmp_path)
    calls = tmp_path / "calls"
    env = _stale_env(tmp_path, cne, calls)
    env["CNE_STUB_STATUS"] = "7"

    result = _run(STALE, "2026-08-28", env=env)

    assert result.returncode == 7
    assert _call_args(calls) == [
        "run",
        "daily",
        "--stale-only",
        "--config",
        str(tmp_path / "cnequity.toml"),
        "--trade-date",
        "2026-08-28",
    ]
    assert "FAILED" in result.stdout


def test_daily_and_stale_wrappers_share_an_atomic_nonblocking_lock(tmp_path):
    cne = _stub_cne(tmp_path)
    calls = tmp_path / "calls"
    env = _stale_env(tmp_path, cne, calls)
    env["CNE_STUB_SLEEP"] = "1"

    first = _popen(STALE, env)
    lock_dir = tmp_path / "locks" / "daily.lock"
    deadline = time.monotonic() + 2
    while not lock_dir.exists() and time.monotonic() < deadline:
        time.sleep(0.02)

    second = _run(STALE, env=env)
    first_stdout, first_stderr = first.communicate(timeout=5)

    assert lock_dir.exists() is False
    assert second.returncode == 0
    assert "skipping" in second.stdout
    assert first.returncode == 0, first_stdout + first_stderr
    # The second invocation observed the first process and did not submit a
    # duplicate stale-only request.
    assert _call_args(calls).count("--stale-only") == 1


def test_stale_pipeline_recovers_lock_owned_by_dead_process(tmp_path):
    cne = _stub_cne(tmp_path)
    calls = tmp_path / "calls"
    env = _stale_env(tmp_path, cne, calls)
    lock = tmp_path / "locks" / "daily.lock"
    lock.mkdir(parents=True)
    (lock / "pid").write_text("99999999\n", encoding="utf-8")

    result = _run(STALE, env=env)

    assert result.returncode == 0, result.stdout + result.stderr
    assert _call_args(calls).count("--stale-only") == 1
    assert not lock.exists()


def test_stale_pipeline_does_not_remove_ownerless_lock(tmp_path):
    cne = _stub_cne(tmp_path)
    calls = tmp_path / "calls"
    env = _stale_env(tmp_path, cne, calls)
    lock = tmp_path / "locks" / "daily.lock"
    lock.mkdir(parents=True)

    result = _run(STALE, env=env)

    assert result.returncode == 0
    assert "skipping" in result.stdout
    assert lock.exists()
    assert not calls.exists()


def test_installer_xml_escapes_checkout_path(tmp_path):
    repo = tmp_path / "CN&Equity"
    (repo / "scripts" / "launchd").mkdir(parents=True)
    (repo / ".venv" / "bin").mkdir(parents=True)
    shutil.copy2(ROOT / "scripts" / "install_scheduler.sh", repo / "scripts")
    shutil.copy2(DAILY_PLIST, repo / "scripts" / "launchd")
    shutil.copy2(STALE_PLIST, repo / "scripts" / "launchd")
    shutil.copy2(EVENTS_PLIST, repo / "scripts" / "launchd")
    cne = repo / ".venv" / "bin" / "cne"
    cne.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    cne.chmod(0o755)

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    uname = fake_bin / "uname"
    uname.write_text("#!/bin/sh\nprintf 'Darwin\\n'\n", encoding="utf-8")
    uname.chmod(0o755)
    launchctl = fake_bin / "launchctl"
    launchctl.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    launchctl.chmod(0o755)
    home = tmp_path / "home"
    env = os.environ.copy()
    env.update(
        {
            "HOME": str(home),
            "PATH": f"{fake_bin}:{env['PATH']}",
            "CNE_LAUNCHCTL": str(launchctl),
        }
    )

    result = _run(repo / "scripts" / "install_scheduler.sh", env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    daily = plistlib.loads(
        (home / "Library" / "LaunchAgents" / "com.cnequity.daily.plist").read_bytes()
    )
    stale = plistlib.loads(
        (home / "Library" / "LaunchAgents" / "com.cnequity.stale.plist").read_bytes()
    )
    events = plistlib.loads(
        (home / "Library" / "LaunchAgents" / "com.cnequity.events.plist").read_bytes()
    )
    assert daily["ProgramArguments"][-1] == str(repo / "scripts" / "daily_pipeline.sh")
    assert stale["ProgramArguments"][-1] == str(repo / "scripts" / "stale_pipeline.sh")
    assert events["ProgramArguments"][-1] == str(repo / "scripts" / "events_pipeline.sh")


def _stub_cne_failing_groups(tmp_path: Path) -> Path:
    """A stub that fails only the groups named in ``CNE_STUB_FAIL_GROUPS``."""
    path = tmp_path / "cne"
    path.write_text(
        """#!/bin/sh
printf 'argc=%s\\n' "$#" >> "$CNE_CALL_LOG"
for arg in "$@"; do printf '<%s>\\n' "$arg" >> "$CNE_CALL_LOG"; done
group=""
prev=""
for arg in "$@"; do
  if [ "$prev" = "--group" ]; then group="$arg"; fi
  prev="$arg"
done
for bad in ${CNE_STUB_FAIL_GROUPS:-}; do
  if [ "$bad" = "$group" ]; then exit 1; fi
done
exit 0
""",
        encoding="utf-8",
    )
    path.chmod(0o755)
    return path


def _daily_env(tmp_path: Path, cne: Path, calls: Path, **extra: str) -> dict[str, str]:
    env = _stale_env(tmp_path, cne, calls)
    env.update(
        {
            "CNE_GROUPS": "core research",
            # Keep the run to the group loop: no probes, no desktop popup, and
            # a data root with no meta/ so backup_meta exits before writing.
            "CNE_SOURCE_HEALTH": "0",
            "CNE_NOTIFY": "0",
            "CNE_DATA_ROOT": str(tmp_path / "lake"),
        }
    )
    env.update(extra)
    return env


def _run_daily(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return _run(DAILY, env=env)


def test_daily_pipeline_fails_hard_when_a_gate_group_fails(tmp_path):
    """The gate decides whether anyone gets paged, and nothing exercised it."""
    cne = _stub_cne_failing_groups(tmp_path)
    env = _daily_env(tmp_path, cne, tmp_path / "calls", CNE_STUB_FAIL_GROUPS="core")

    result = _run_daily(env)

    assert result.returncode == 1
    assert "GATE FAILED: core" in result.stdout
    # A failing gate group must not stop the others from getting their turn.
    assert "group research OK" in result.stdout


def test_daily_pipeline_keeps_a_soft_group_failure_warn_only(tmp_path):
    """Overseas EastMoney lag must not paint an otherwise good day red."""
    cne = _stub_cne_failing_groups(tmp_path)
    env = _daily_env(tmp_path, cne, tmp_path / "calls", CNE_STUB_FAIL_GROUPS="research")

    result = _run_daily(env)

    assert result.returncode == 0
    assert "warn-only" in result.stdout
    assert "research" in result.stdout


def test_daily_pipeline_can_be_told_to_fail_on_a_soft_group(tmp_path):
    cne = _stub_cne_failing_groups(tmp_path)
    env = _daily_env(
        tmp_path,
        cne,
        tmp_path / "calls",
        CNE_STUB_FAIL_GROUPS="research",
        CNE_SOFT_FAIL_OK="0",
    )

    result = _run_daily(env)

    assert result.returncode == 1
    assert "EM/soft FAILED: research" in result.stdout


def test_daily_pipeline_exits_0_when_every_group_succeeds(tmp_path):
    cne = _stub_cne_failing_groups(tmp_path)
    env = _daily_env(tmp_path, cne, tmp_path / "calls")

    result = _run_daily(env)

    assert result.returncode == 0
    assert "DONE ok" in result.stdout


def test_health_notify_honours_the_same_cne_override_as_the_pipeline(tmp_path):
    """`daily_pipeline.sh` runs this script and honours CNE_BIN; this one
    hardcoded the repo venv, so an override left the health gate probing a
    different — or absent — binary while the pipeline used the real one.
    """
    cne = _stub_cne_failing_groups(tmp_path)
    calls = tmp_path / "calls"
    env = _daily_env(tmp_path, cne, calls)

    result = _run(ROOT / "scripts" / "health_notify.sh", env=env)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "OK" in result.stdout
    # It really went through the stub, not some other cne on the machine.
    assert "audit" in _call_args(calls)
    assert "--datasets" in _call_args(calls)
