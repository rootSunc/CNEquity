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
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DAILY = ROOT / "scripts" / "daily_pipeline.sh"
STALE = ROOT / "scripts" / "stale_pipeline.sh"
DAILY_PLIST = ROOT / "scripts" / "launchd" / "com.cnequity.daily.plist.template"
STALE_PLIST = ROOT / "scripts" / "launchd" / "com.cnequity.stale.plist.template"


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


def test_stale_pipeline_forwards_target_date_and_returns_cne_failure(tmp_path):
    cne = _stub_cne(tmp_path)
    calls = tmp_path / "calls"
    env = _stale_env(tmp_path, cne, calls)
    env["CNE_STUB_STATUS"] = "7"

    result = subprocess.run(
        [str(STALE), "2026-08-28"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

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

    first = subprocess.Popen(
        [str(STALE)], env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    lock_dir = tmp_path / "locks" / "daily.lock"
    deadline = time.monotonic() + 2
    while not lock_dir.exists() and time.monotonic() < deadline:
        time.sleep(0.02)

    second = subprocess.run([str(STALE)], env=env, capture_output=True, text=True, check=False)
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

    result = subprocess.run([str(STALE)], env=env, capture_output=True, text=True, check=False)

    assert result.returncode == 0, result.stdout + result.stderr
    assert _call_args(calls).count("--stale-only") == 1
    assert not lock.exists()


def test_stale_pipeline_does_not_remove_ownerless_lock(tmp_path):
    cne = _stub_cne(tmp_path)
    calls = tmp_path / "calls"
    env = _stale_env(tmp_path, cne, calls)
    lock = tmp_path / "locks" / "daily.lock"
    lock.mkdir(parents=True)

    result = subprocess.run([str(STALE)], env=env, capture_output=True, text=True, check=False)

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

    result = subprocess.run(
        [str(repo / "scripts" / "install_scheduler.sh")],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    daily = plistlib.loads(
        (home / "Library" / "LaunchAgents" / "com.cnequity.daily.plist").read_bytes()
    )
    stale = plistlib.loads(
        (home / "Library" / "LaunchAgents" / "com.cnequity.stale.plist").read_bytes()
    )
    assert daily["ProgramArguments"][-1] == str(repo / "scripts" / "daily_pipeline.sh")
    assert stale["ProgramArguments"][-1] == str(repo / "scripts" / "stale_pipeline.sh")
