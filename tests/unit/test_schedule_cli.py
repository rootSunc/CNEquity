"""`cne schedule`, `cne run maintain` and the metadata backup they rely on."""

from __future__ import annotations

import os
import sqlite3
import tarfile
import time
from contextlib import closing
from datetime import date, datetime, timezone

import pytest
from click.testing import CliRunner
from test_serve_schedule import FakeBackend

from cnequity.cli.main import cli
from cnequity.config import load_config
from cnequity.orchestrator.run_window import mark_done, pending_session
from cnequity.serve.ops.records import atomic_write
from cnequity.serve.ops.scheduler import settings, state_dir
from cnequity.storage.meta_backup import backup_meta, rotate


def _meta(tmp_path):
    meta = tmp_path / "meta"
    (meta / "state").mkdir(parents=True)
    (meta / "state" / "x.json").write_text("{}", encoding="utf-8")
    (meta / "revisions" / "daily_bars").mkdir(parents=True)
    (meta / "revisions" / "daily_bars" / "receipt.json").write_text("{}", encoding="utf-8")
    (meta / "revisions" / "data" / "gen1").mkdir(parents=True)
    (meta / "revisions" / "data" / "gen1" / "big.parquet").write_bytes(b"x" * 10)
    with closing(sqlite3.connect(meta / "manifest.db")) as conn:
        conn.execute("CREATE TABLE t (v)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.commit()
    return meta


def test_backup_keeps_receipts_and_a_readable_manifest_but_not_generations(tmp_path):
    result = backup_meta(_meta(tmp_path), tmp_path / "backups")

    with tarfile.open(result.archive) as tar:
        names = set(tar.getnames())
        tar.extract("manifest.db", tmp_path / "out", filter="data")
    assert "revisions/daily_bars/receipt.json" in names
    assert "state/x.json" in names
    assert not any(name.startswith("revisions/data") for name in names)
    with closing(sqlite3.connect(tmp_path / "out" / "manifest.db")) as conn:
        assert conn.execute("SELECT v FROM t").fetchall() == [(1,)]
    assert not list((tmp_path / "backups").glob(".*partial"))


def test_backup_refuses_a_missing_or_empty_meta(tmp_path):
    with pytest.raises(FileNotFoundError):
        backup_meta(tmp_path / "nope", tmp_path / "b")
    (tmp_path / "empty").mkdir()
    with pytest.raises(ValueError, match="nothing to back up"):
        backup_meta(tmp_path / "empty", tmp_path / "b")


def test_rotation_applies_age_and_count(tmp_path):
    folder = tmp_path / "b"
    folder.mkdir()
    now = time.time()
    for index in range(5):
        path = folder / f"meta-2026010{index}-000000.tar.gz"
        path.write_bytes(b"")
        os.utime(path, (now - index * 86400, now - index * 86400))
    removed = rotate(folder, retention_days=3, retention_count=2)
    assert sorted(p.name for p in folder.iterdir()) == [
        "meta-20260100-000000.tar.gz",
        "meta-20260101-000000.tar.gz",
    ]
    assert len(removed) == 3


def test_maintain_is_due_only_after_the_days_run(config):
    now = datetime(2026, 9, 30, 14, tzinfo=timezone.utc)  # 22:00 Beijing, a session
    session = date(2026, 9, 30)
    assert pending_session(config, "maintain", now) is None
    mark_done(config.meta_root, "daily", session)
    assert pending_session(config, "maintain", now) == session
    mark_done(config.meta_root, "maintain", session)
    assert pending_session(config, "maintain", now) is None


@pytest.fixture
def maintain_env(config, monkeypatch):
    calls: list[list[str]] = []
    failing: set[str] = set()

    class Done:
        def __init__(self, code):
            self.returncode = code

    def run(argv, **_kwargs):
        args = argv[argv.index("cnequity") + 1 :]
        calls.append(args)
        return Done(1 if args[0] in failing or " ".join(args[:2]) in failing else 0)

    monkeypatch.setattr("cnequity.cli.schedule_cmds.subprocess.run", run)
    (config.meta_root / "state").mkdir(parents=True, exist_ok=True)
    (config.meta_root / "state" / "a.json").write_text("{}", encoding="utf-8")
    return config, calls, failing


def _maintain(config, *extra):
    return CliRunner().invoke(cli, ["run", "maintain", "--config", str(config.config_path), *extra])


def test_maintain_runs_every_check_and_backs_up(maintain_env, monkeypatch):
    config, calls, _ = maintain_env
    monkeypatch.setattr(
        "cnequity.cli.schedule_cmds.shanghai_today", lambda: date(2026, 10, 21)
    )  # a Wednesday: the run audit, not the whole lake

    result = _maintain(config, "--groups", "core")

    assert result.exit_code == 0, result.output
    assert [c[0] for c in calls[:5]] == ["audit", "status", "sources", "sources", "verify"]
    assert calls[0][:2] == ["audit", "--config"]
    assert [calls[2][1], calls[3][1]] == ["probe", "slo"]
    assert ["--groups", "core"] == calls[1][3:5]
    assert calls[-1][:2] == ["run", "clean"]
    assert "--dry-run" in calls[-1]
    assert list(config.backup_root.glob("meta-*.tar.gz"))


def test_a_stale_dataset_fails_maintenance_but_a_source_probe_does_not(maintain_env):
    config, _calls, failing = maintain_env
    failing.add("sources probe")
    assert _maintain(config).exit_code == 0
    failing.add("status")
    result = _maintain(config)
    assert result.exit_code == 1
    assert "Freshness gate" in result.output


def test_full_audit_runs_on_its_weekday(maintain_env, monkeypatch):
    config, calls, _ = maintain_env
    monkeypatch.setattr(
        "cnequity.cli.schedule_cmds.shanghai_today", lambda: date(2026, 10, 24)
    )  # a Saturday: the default full_audit_weekday is 6
    _maintain(config)
    assert calls[0][:3] == ["audit", "--full", "--quality-only"]


@pytest.fixture
def fake_timer(config, monkeypatch):
    monkeypatch.setattr("cnequity.serve.ops.scheduler.SchedulerBackend", FakeBackend)
    return config


def test_schedule_install_status_remove(fake_timer):
    config = fake_timer
    args = ["--config", str(config.config_path)]

    installed = CliRunner().invoke(cli, ["schedule", "install", "--yes", *args])
    assert installed.exit_code == 0, installed.output
    assert settings(config)["daily"] and settings(config)["stale"]

    status = CliRunner().invoke(cli, ["schedule", "status", *args])
    assert status.exit_code == 0, status.output
    assert "isolated timer" in status.output

    removed = CliRunner().invoke(cli, ["schedule", "remove", "--yes", *args])
    assert removed.exit_code == 0, removed.output
    assert not settings(config)["daily"]


def test_install_keeps_a_dashboard_event_schedule(fake_timer):
    path = fake_timer.config_path
    path.write_text(
        path.read_text(encoding="utf-8")
        + '\n[job.events.groups.disclosures]\nsteps = ["announcement_index", "compact"]\n',
        encoding="utf-8",
    )
    config = load_config(path)
    atomic_write(
        state_dir(config) / "settings.json",
        {
            "config_path": str(path),
            "daily": False,
            "stale": False,
            "events": True,
            "events_group": "disclosures",
            "events_interval_minutes": 30,
        },
    )

    result = CliRunner().invoke(cli, ["schedule", "install", "--yes", "--config", str(path)])

    assert result.exit_code == 0, result.output
    saved = settings(config)
    assert saved["daily"] and saved["events"] and saved["events_interval_minutes"] == 30


def test_install_without_yes_asks(fake_timer):
    result = CliRunner().invoke(
        cli, ["schedule", "install", "--config", str(fake_timer.config_path)], input="n\n"
    )
    assert result.exit_code == 1
    assert not settings(fake_timer)["daily"]


def test_vantage_comes_from_flag_then_env_then_config(config, monkeypatch):
    from cnequity.diagnostics.source_health import effective_vantage

    monkeypatch.delenv("CNE_SOURCE_VANTAGE", raising=False)
    assert effective_vantage(config) == "local"
    config.source_vantage = "overseas"
    assert effective_vantage(config) == "overseas"
    monkeypatch.setenv("CNE_SOURCE_VANTAGE", "cn-sh")
    assert effective_vantage(config) == "cn-sh"
    assert effective_vantage(config, "manual") == "manual"


def test_maintain_probes_with_the_configured_vantage(maintain_env, monkeypatch):
    config, calls, _ = maintain_env
    monkeypatch.delenv("CNE_SOURCE_VANTAGE", raising=False)
    path = config.config_path
    path.write_text(
        path.read_text(encoding="utf-8") + '\n[source_health]\nvantage = "overseas"\n',
        encoding="utf-8",
    )

    _maintain(config)

    probe = next(c for c in calls if c[:2] == ["sources", "probe"])
    assert probe[probe.index("--vantage") + 1] == "overseas"


def test_an_invalid_vantage_fails_validation(config):
    from cnequity.config import validate_config

    config.source_vantage = "bad label!"
    assert any("source_health.vantage" in e for e in validate_config(config))


def _runs(config, session, statuses):
    import json
    import sqlite3
    from contextlib import closing

    config.manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(config.manifest_path)) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ingestion_runs "
            "(job_name TEXT, status TEXT, metadata_json TEXT, started_at TEXT)"
        )
        for group, status in statuses.items():
            conn.execute(
                "INSERT INTO ingestion_runs VALUES (?, ?, ?, ?)",
                (f"daily:{group}", status, json.dumps({"trade_date": str(session)}), str(session)),
            )
        conn.commit()


@pytest.fixture
def grouped(config, monkeypatch):
    from cnequity.config.loader import ScheduleGroup

    config.schedule_groups = {
        name: ScheduleGroup(at="16:00", steps=["compact"]) for name in ("core", "fundamentals")
    }
    config.maintenance_gate_groups = ("core",)
    config.maintenance_soft_fail_max_days = 3
    days = iter([date(2026, 10, 21), date(2026, 10, 22), date(2026, 10, 23)])
    current = {}

    def session():
        return current["day"]

    def advance():
        current["day"] = next(days)
        return current["day"]

    monkeypatch.setattr("cnequity.domain.market_time.last_closed_session", session)
    return config, advance


def test_a_failed_gate_group_is_a_problem_at_once(grouped):
    from cnequity.cli.schedule_cmds import _check_daily_groups

    config, advance = grouped
    _runs(config, advance(), {"core": "failed", "fundamentals": "degraded"})
    problems, notes = _check_daily_groups(config)
    assert problems == ["daily group core failed"] and notes == []


def test_a_soft_group_escalates_after_consecutive_failures_and_resets(grouped):
    from cnequity.cli.schedule_cmds import _check_daily_groups

    config, advance = grouped
    for expected_days in (1, 2):
        _runs(config, advance(), {"core": "success", "fundamentals": "failed"})
        problems, notes = _check_daily_groups(config)
        assert problems == [] and str(expected_days) in notes[0]
    # Checking the same session twice does not count it twice.
    assert "2 session" in _check_daily_groups(config)[1][0]
    day = advance()
    _runs(config, day, {"core": "success", "fundamentals": "failed"})
    problems, _ = _check_daily_groups(config)
    assert problems == ["daily group fundamentals failed 3 session(s) in a row"]
    _runs(config, day, {"fundamentals": "success"})  # a later rerun the same day
    assert _check_daily_groups(config) == ([], [])


def test_the_shell_pipelines_streak_carries_over(grouped):
    from cnequity.cli.schedule_cmds import _check_daily_groups

    config, advance = grouped
    legacy = config.data_root / "state" / "soft_streak" / "fundamentals"
    legacy.parent.mkdir(parents=True)
    legacy.write_text("2026-10-20 2\n", encoding="utf-8")
    _runs(config, advance(), {"core": "success", "fundamentals": "failed"})
    problems, _ = _check_daily_groups(config)
    assert problems == ["daily group fundamentals failed 3 session(s) in a row"]


def test_maintain_retries_the_debt_ledger_without_failing_on_it(maintain_env):
    from cnequity.storage.state import StateStore

    config, calls, failing = maintain_env
    StateStore(config.meta_root).record_outstanding_keys(
        "daily_bars", [("600000.SH", date(2026, 10, 19))], run_id="r1", reason="test"
    )
    failing.add("backfill")

    result = _maintain(config)

    assert result.exit_code == 0, result.output
    assert ["backfill", "daily_bars", "--outstanding"] == calls[0][:3]
    assert calls[1][0] == "audit"  # the audit and gate see what the retry settled
    assert "Outstanding keys: daily_bars" in result.output
