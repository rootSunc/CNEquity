"""Web scheduling never registers real tasks in tests."""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cnequity.config import load_config
from cnequity.orchestrator.run_window import marker_path
from cnequity.serve.app import create_app
from cnequity.serve.ops.catalog import OpsError
from cnequity.serve.ops.records import atomic_write, read_record
from cnequity.serve.ops.scheduled import claim_session, tick
from cnequity.serve.ops.scheduler import ScheduleService, edit_times, settings, state_dir


class FakeBackend:
    kind = "isolated timer"

    def __init__(self, *_args, **kwargs):
        self.working_directory = kwargs.get("working_directory") or os.getcwd()
        self.enabled = False
        self.calls = []
        self.failure = False

    def observe(self):
        return {
            "available": True,
            "installed": self.enabled,
            "active": self.enabled,
            "matches": self.enabled,
            "fingerprint": str(self.enabled),
            "error": None,
        }

    def render(self):
        return "ISOLATED TIMER"

    def apply(self, enabled, artifact):
        self.calls.append((enabled, artifact))
        if self.failure:
            raise OpsError("permission denied")
        self.enabled = enabled


@pytest.fixture
def scheduler(config, monkeypatch):
    monkeypatch.setattr("cnequity.serve.ops.scheduler.SchedulerBackend", FakeBackend)
    return ScheduleService(config)


def preview(service, daily=True, stale=True):
    return service.preview(daily=daily, stale=stale, daily_run_at="18:10", stale_run_at="21:30")


def test_enable_pause_and_preserve_config(scheduler):
    original = scheduler.path.read_bytes()
    plan = preview(scheduler)
    assert not scheduler.backend.calls
    assert not (state_dir(scheduler.config) / "settings.json").exists()
    result = scheduler.apply(plan["token"], acknowledged=True, requested_by="test")
    assert result["enabled"] and result["daily"] and result["stale"]
    assert result["daily_run_at"] == "18:10"
    assert scheduler.path.with_name(scheduler.path.name + ".schedule.bak").read_bytes() == original
    assert (
        scheduler.path.stat().st_mode
        == scheduler.path.with_name(scheduler.path.name + ".schedule.bak").stat().st_mode
    )
    assert load_config(scheduler.path).daily_waves == scheduler.config.daily_waves
    plan = preview(scheduler, False, False)
    result = scheduler.apply(plan["token"], acknowledged=True, requested_by="test")
    assert not result["enabled"] and not result["daily"]
    assert scheduler.backend.calls[-1] == (False, "")


@pytest.mark.usefixtures("chinese_output")
def test_confirmation_expiry_replay_and_external_changes(scheduler):
    plan = preview(scheduler)
    with pytest.raises(OpsError, match="确认"):
        scheduler.apply(plan["token"], acknowledged=False, requested_by="test")
    scheduler.path.write_text(scheduler.path.read_text() + "\n# external edit\n")
    with pytest.raises(OpsError, match="变化"):
        scheduler.apply(plan["token"], acknowledged=True, requested_by="test")
    assert not scheduler.backend.calls
    plan = preview(scheduler)
    scheduler.previews[plan["token"]]["expires"] = time.monotonic() - 1
    with pytest.raises(OpsError, match="失效"):
        scheduler.apply(plan["token"], acknowledged=True, requested_by="test")
    plan = preview(scheduler)
    scheduler.apply(plan["token"], acknowledged=True, requested_by="test")
    with pytest.raises(OpsError, match="失效"):
        scheduler.apply(plan["token"], acknowledged=True, requested_by="test")


@pytest.mark.usefixtures("chinese_output")
def test_native_failure_remains_paused_and_retryable(scheduler):
    scheduler.backend.failure = True
    with pytest.raises(OpsError, match="保持暂停"):
        scheduler.apply(preview(scheduler)["token"], acknowledged=True, requested_by="test")
    assert not settings(scheduler.config)["daily"]
    scheduler.backend.failure = False
    assert scheduler.apply(preview(scheduler)["token"], acknowledged=True, requested_by="test")[
        "enabled"
    ]
    scheduler.backend.failure = True
    paused = scheduler.apply(
        preview(scheduler, False, False)["token"], acknowledged=True, requested_by="test"
    )
    assert not paused["enabled"] and paused["warning"]


@pytest.mark.usefixtures("chinese_output")
def test_changed_schedule_state_invalidates_old_preview(scheduler):
    plan = preview(scheduler)
    atomic_write(
        state_dir(scheduler.config) / "settings.json",
        {"config_path": str(scheduler.path), "daily": False},
    )
    with pytest.raises(OpsError, match="变化"):
        scheduler.apply(plan["token"], acknowledged=True, requested_by="test")
    assert not scheduler.backend.calls


def test_stale_heartbeat_is_visible_and_pause_works_with_unknown_native_state(
    scheduler, monkeypatch
):
    scheduler.apply(preview(scheduler)["token"], acknowledged=True, requested_by="test")
    atomic_write(
        state_dir(scheduler.config) / "last_tick.json",
        {"at": "2000-01-01T00:00:00+00:00", "status": "idle"},
    )
    assert scheduler.home()["timer_health"] == "stale"
    monkeypatch.setattr(
        scheduler.backend,
        "observe",
        lambda: {
            "available": True,
            "installed": None,
            "active": False,
            "matches": False,
            "fingerprint": None,
            "error": "denied",
        },
    )
    scheduler.backend.failure = True
    paused = scheduler.apply(
        preview(scheduler, False, False)["token"], acknowledged=True, requested_by="test"
    )
    assert not paused["daily"] and not paused["enabled"]


@pytest.mark.usefixtures("chinese_output")
def test_renamed_lake_or_other_config_cannot_be_taken_over(scheduler):
    atomic_write(
        state_dir(scheduler.config) / "settings.json",
        {"config_path": str(scheduler.path.with_name("other.toml")), "daily": True},
    )
    with pytest.raises(OpsError, match="另一份配置"):
        scheduler.home()


def test_timer_main_restores_shared_scheduler_lock_directory(config, monkeypatch):
    from cnequity.serve.ops.scheduled import main

    root = Path(config.data_root) / "shared-locks"
    monkeypatch.setattr(
        "sys.argv",
        ["scheduled", "--config", str(config.config_path), "--scheduler-lock-dir", str(root)],
    )
    seen = []
    monkeypatch.setattr(
        "cnequity.serve.ops.scheduled.tick",
        lambda _config: seen.append(os.environ["CNE_SCHEDULER_LOCK_DIR"]) or {"status": "idle"},
    )
    monkeypatch.setenv("CNE_SCHEDULER_LOCK_DIR", "old-value")
    assert main() == 0
    assert seen == [str(root)]


@pytest.mark.parametrize(
    "text",
    [
        "[job.daily]\nrun_at = \"17:30\" # keep this\n[job.stale]\nrun_at = '21:00'\n",
        '[job.daily.groups.core]\nsteps = ["instruments"]\n',
        '["job" . \'daily\']\n"run_at" = "17:30"\n',
    ],
)
def test_time_edits_preserve_semantics_and_comments(text):
    result = edit_times(text, "18:10", "21:30")
    assert '"18:10"' in result and '"21:30"' in result
    assert "# keep this" in result if "# keep this" in text else True


@pytest.mark.usefixtures("chinese_output")
def test_invalid_or_inline_times_refused(scheduler):
    with pytest.raises(OpsError, match="HH:MM"):
        edit_times("", "25:00", "21:00")
    with pytest.raises(OpsError, match="自动修改"):
        edit_times('job = {daily = {run_at = "17:30"}}\n', "18:00", "21:00")
    with pytest.raises(OpsError, match="同时启用日更"):
        preview(scheduler, False, True)


def test_schedule_api_security_and_live_time_refresh(scheduler, monkeypatch):
    client = TestClient(create_app(scheduler.config), base_url="http://127.0.0.1")
    client.app.state.schedule_service = scheduler
    csrf = client.get("/api/ops").json()["csrf_token"]
    headers = {"Origin": "http://127.0.0.1", "X-CNE-CSRF": csrf}
    body = {"daily": True, "stale": True, "daily_run_at": "18:10", "stale_run_at": "21:30"}
    assert client.post("/api/ops/schedule/preview", json=body).status_code == 403
    assert (
        client.post(
            "/api/ops/schedule/preview",
            headers={**headers, "Origin": "https://evil.example"},
            json=body,
        ).status_code
        == 403
    )
    assert client.get("/api/ops/schedule").status_code == 200
    assert not scheduler.backend.calls
    token = client.post("/api/ops/schedule/preview", headers=headers, json=body).json()["token"]
    result = client.post(
        "/api/ops/schedule/apply", headers=headers, json={"token": token, "acknowledged": True}
    )
    assert result.status_code == 200, result.text
    schedule = client.get("/api/ops").json()["occupancy"]["schedule"]
    assert schedule["daily_run_at"] == "18:10"
    assert schedule["today"]
    assert isinstance(schedule["today_is_session"], bool)
    assert isinstance(schedule["before_daily_run_at"], bool)
    read_only = TestClient(
        create_app(scheduler.config, read_only=True), base_url="http://127.0.0.1"
    )
    assert (
        read_only.post("/api/ops/schedule/preview", headers=headers, json=body).status_code == 404
    )
    remote = TestClient(
        create_app(scheduler.config, token="secret"), base_url="http://remote.example"
    )
    assert (
        remote.post(
            "/api/ops/schedule/preview", headers={"Authorization": "Bearer secret"}, json=body
        ).status_code
        == 403
    )
    assert not scheduler.backend.calls[1:]


def test_failed_os_registration_still_refreshes_saved_times_in_web(scheduler):
    client = TestClient(create_app(scheduler.config), base_url="http://127.0.0.1")
    client.app.state.schedule_service = scheduler
    csrf = client.get("/api/ops").json()["csrf_token"]
    headers = {"Origin": "http://127.0.0.1", "X-CNE-CSRF": csrf}
    plan = preview(scheduler)
    scheduler.backend.failure = True
    result = client.post(
        "/api/ops/schedule/apply",
        headers=headers,
        json={"token": plan["token"], "acknowledged": True},
    )
    assert result.status_code == 422
    assert client.get("/api/ops").json()["occupancy"]["schedule"]["daily_run_at"] == "18:10"
    assert not settings(scheduler.config)["daily"]


@pytest.mark.usefixtures("chinese_output")
def test_tick_claims_under_child_lock_and_rejects_duplicate_or_pause(scheduler, monkeypatch):
    service = scheduler
    config = service.config
    directory = state_dir(config)
    atomic_write(
        directory / "settings.json",
        {"config_path": str(service.path), "daily": True, "stale": True},
    )
    session = date(2026, 9, 30)
    monkeypatch.setattr(
        "cnequity.serve.ops.scheduled.pending_session", lambda _config, job: session
    )
    claim_session(config, {"job": "daily", "session": session.isoformat()})
    assert marker_path(config.meta_root, "daily", session).exists()
    monkeypatch.setattr("cnequity.serve.ops.scheduled.pending_session", lambda *_args: None)
    with pytest.raises(OpsError, match="已执行"):
        claim_session(config, {"job": "daily", "session": session.isoformat()})
    atomic_write(directory / "settings.json", {"config_path": str(service.path), "daily": False})
    with pytest.raises(OpsError, match="暂停"):
        claim_session(config, {"job": "daily", "session": session.isoformat()})
    assert tick(config)["status"] == "idle"


def _wait_until_released(config, seconds: float = 20.0) -> None:
    """Wait until the finished child has also released the slot and scheduler locks.

    A job writes its final record a moment before its process exits and the
    kernel drops its locks. A tick in that window correctly answers "waiting",
    and the real scheduler simply starts the next job one tick later; a test
    that ticks straight after the record turns terminal must wait out the same
    window instead of racing it.
    """
    from cnequity.file_lock import is_locked
    from cnequity.serve.ops.preflight import scheduler_blockers
    from cnequity.serve.ops.records import slot_path

    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and (
        is_locked(slot_path(config)) or scheduler_blockers(config, "lake-write")
    ):
        time.sleep(0.05)


def test_real_timer_child_logs_and_marks_once_without_web(scheduler, monkeypatch):
    # Freeze the exchange window in both parent and a real subprocess. No
    # production clock override or native scheduler installation is needed.
    import subprocess

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 9, 30, 14, tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr("cnequity.orchestrator.run_window.datetime", FrozenDateTime)
    real_popen = subprocess.Popen
    script = (
        "import sys; from datetime import datetime, timezone; "
        "from pathlib import Path; import cnequity.orchestrator.run_window as window; "
        "window.datetime=type('Frozen', (datetime,), "
        "{'now': classmethod(lambda cls, tz=None: datetime(2026,9,30,14,tzinfo=timezone.utc).astimezone(tz))}); "
        "from cnequity.serve.ops.job import execute; execute(Path(sys.argv[1]))"
    )

    def spawn(argv, **kwargs):
        if argv[1:3] == ["-m", "cnequity.serve.ops.job"]:
            argv = [argv[0], "-c", script, argv[-1]]
        return real_popen(argv, **kwargs)

    monkeypatch.setattr("cnequity.serve.ops.runner.subprocess.Popen", spawn)
    config = scheduler.config
    path = scheduler.path
    text = edit_times(path.read_text(), "00:00", "00:00")
    path.write_text(text)
    config = load_config(path)
    atomic_write(
        state_dir(config) / "settings.json",
        {"config_path": str(path), "daily": True, "stale": True},
    )
    monkeypatch.setenv("CNE_OPS_HANDLER", "ops_handler:main")
    monkeypatch.setenv("CNE_OPS_STUB_MODE", "unicode")
    tests = str(Path(__file__).resolve().parents[1])
    monkeypatch.setenv("PYTHONPATH", tests + os.pathsep + os.environ.get("PYTHONPATH", ""))
    from cnequity.orchestrator.run_window import pending_session
    from cnequity.serve.ops.runner import OpsService

    session = pending_session(config, "daily")
    assert session == date(2026, 9, 30)
    report = tick(config)
    assert report["status"] == "started", report
    service = OpsService(config, storage_busy=lambda: False)
    job = report["jobs"][0]
    deadline = time.monotonic() + 20
    while (
        service.get_job(job["job_id"])["state"] in {"starting", "running"}
        and time.monotonic() < deadline
    ):
        time.sleep(0.05)
    record = service.get_job(job["job_id"])
    assert record["state"] == "succeeded", record
    assert record["scheduled"] == {"job": "daily", "session": session.isoformat()}
    assert "中文" in Path(record["logs"]["err"]).read_text(encoding="utf-8")
    assert marker_path(config.meta_root, "daily", session).exists()
    # Maintenance follows the day's run before the catch-up gets its turn.
    _wait_until_released(config)
    after = tick(config)
    assert after["jobs"][0]["job"] == "maintain", after
    deadline = time.monotonic() + 20
    while (
        service.get_job(after["jobs"][0]["job_id"])["state"] in {"starting", "running"}
        and time.monotonic() < deadline
    ):
        time.sleep(0.05)
    assert service.get_job(after["jobs"][0]["job_id"])["state"] == "complete"
    assert marker_path(config.meta_root, "maintain", session).exists()
    _wait_until_released(config)
    late = tick(config)
    assert late["jobs"][0]["job"] == "stale", late
    deadline = time.monotonic() + 20
    while (
        service.get_job(late["jobs"][0]["job_id"])["state"] in {"starting", "running"}
        and time.monotonic() < deadline
    ):
        time.sleep(0.05)
    assert service.get_job(late["jobs"][0]["job_id"])["state"] == "succeeded"
    assert marker_path(config.meta_root, "stale", session).exists()
    _wait_until_released(config)
    assert tick(config)["status"] in {"idle", "waiting"}
    assert read_record(state_dir(config) / "last_tick.json")["status"] in {"idle", "waiting"}


def test_a_schedule_reports_in_the_language_it_was_set_up_in(scheduler, monkeypatch):
    """A launchd/cron/Task Scheduler job has no LANG; the installer's language rides along."""
    from cnequity import i18n
    from cnequity.serve.ops import scheduled

    with i18n.use_language("zh_CN"):
        scheduler.apply(preview(scheduler)["token"], acknowledged=True, requested_by="test")
    saved = settings(scheduler.config)
    assert saved["language"] == "zh_CN"

    seen = []
    monkeypatch.setattr(scheduled, "load_config", lambda _path: scheduler.config)
    monkeypatch.setattr(
        scheduled,
        "tick",
        lambda _config: seen.append(i18n.current_language()) or {"status": "idle"},
    )
    monkeypatch.setattr("sys.argv", ["scheduled", "--config", str(scheduler.path)])
    assert scheduled.main() == 0
    assert seen == ["zh_CN"]
    assert i18n.current_language() == "en"


def test_an_unknown_saved_language_is_an_invalid_settings_file(scheduler):
    scheduler.apply(preview(scheduler)["token"], acknowledged=True, requested_by="test")
    path = state_dir(scheduler.config) / "settings.json"
    atomic_write(path, {**read_record(path), "language": "klingon"})
    with pytest.raises(OpsError):
        settings(scheduler.config)
