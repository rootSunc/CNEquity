"""Regeneration must preserve host policy and expose drift without installing jobs."""

import importlib.util
import os
import plistlib
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/scheduler_config.py"
spec = importlib.util.spec_from_file_location("scheduler_config", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_renderer_preserves_host_groups_cadence_and_event_scope(tmp_path):
    old = {
        "StartCalendarInterval": {"Hour": 11, "Minute": 15},
        "EnvironmentVariables": {
            "CNE_GROUPS": "core",
            "CNE_SOURCE_VANTAGE": "overseas",
            "CNE_CONFIG": "/my/config.toml",
        },
    }
    (tmp_path / "com.cnequity.daily.plist").write_bytes(plistlib.dumps(old))
    event = {"StartInterval": 900, "EnvironmentVariables": {"CNE_EVENTS_GROUP": "disclosures"}}
    (tmp_path / "com.cnequity.events.plist").write_bytes(plistlib.dumps(event))
    jobs = module.render_jobs(ROOT, tmp_path, groups=None, vantage=None)
    assert jobs["com.cnequity.daily"]["EnvironmentVariables"] == {
        **old["EnvironmentVariables"],
        "CNE_STALE_RETRY": "0",
    }
    assert jobs["com.cnequity.stale"]["EnvironmentVariables"]["CNE_GROUPS"] == "core"
    assert jobs["com.cnequity.stale"]["EnvironmentVariables"]["CNE_CONFIG"] == "/my/config.toml"
    assert jobs["com.cnequity.events"]["StartInterval"] == 900
    assert "StartCalendarInterval" not in jobs["com.cnequity.events"]
    assert jobs["com.cnequity.events"]["EnvironmentVariables"]["CNE_EVENTS_GROUP"] == "disclosures"
    changed = module.render_jobs(ROOT, tmp_path, groups="core,capital", vantage="cn")
    assert changed["com.cnequity.daily"]["EnvironmentVariables"]["CNE_GROUPS"] == "core capital"


def test_check_and_dry_run_do_not_install_or_overwrite_host_files(tmp_path):
    dest = tmp_path / "installed"
    preview = tmp_path / "preview"
    env = dict(
        os.environ,
        CNE_SCHEDULER_DEST_DIR=str(dest),
        CNE_GROUPS="core",
        CNE_SOURCE_VANTAGE="overseas",
        CNE_LAUNCHCTL="/must/not/run",
    )

    def run(*args):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    assert run("--check").returncode == 1
    assert not dest.exists()
    result = run("--dry-run", str(preview))
    assert result.returncode == 0, result.stderr
    assert not dest.exists()
    preview.rename(dest)
    assert run("--check").returncode == 0
    before = (dest / "com.cnequity.daily.plist").read_bytes()
    assert run("--dry-run", str(dest)).returncode == 1
    assert (dest / "com.cnequity.daily.plist").read_bytes() == before


def test_new_install_has_six_groups_and_rejects_empty_override(tmp_path):
    import pytest

    jobs = module.render_jobs(ROOT, tmp_path, groups=None, vantage=None)
    assert jobs["com.cnequity.daily"]["EnvironmentVariables"]["CNE_GROUPS"] == module.DEFAULT_GROUPS
    with pytest.raises(ValueError, match="non-empty"):
        module.render_jobs(ROOT, tmp_path, groups="", vantage=None)


def test_stale_time_override_is_scoped_and_survives_regeneration(tmp_path):
    dest = tmp_path / "installed"
    dest.mkdir()
    old = {"EnvironmentVariables": {"CNE_GROUPS": "core", "CNE_SOURCE_VANTAGE": "overseas"}}
    daily = dest / "com.cnequity.daily.plist"
    daily.write_bytes(plistlib.dumps(old))
    before = daily.read_bytes()
    env = dict(os.environ, CNE_SCHEDULER_DEST_DIR=str(dest), CNE_LAUNCHCTL="/usr/bin/true")
    env.pop("CNE_GROUPS", None)
    env.pop("CNE_SOURCE_VANTAGE", None)

    def run(*args):
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args], env=env, capture_output=True, text=True
        )

    result = run("--stale-only", "--stale-at", "23:00")
    assert result.returncode == 0, result.stderr
    assert daily.read_bytes() == before
    assert not (dest / "com.cnequity.events.plist").exists()
    job = plistlib.loads((dest / "com.cnequity.stale.plist").read_bytes())
    assert job["StartCalendarInterval"] == {"Hour": 23, "Minute": 0}
    assert job["EnvironmentVariables"]["CNE_GROUPS"] == "core"
    assert job["RunAtLoad"] is False
    assert run("--check", "--stale-only").returncode == 0
    assert run("--check", "--stale-only", "--stale-at", "22:00").returncode == 1
    for invalid in ("24:00", "23:60", "-1:00", "23", "1:00"):
        assert run("--stale-only", "--stale-at", invalid).returncode == 2
    assert run("--daily-only", "--stale-at", "23:00").returncode == 2
