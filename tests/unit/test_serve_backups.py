"""Exercise real snapshot commands against an isolated lake, never a provider."""

from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pytest
from test_serve_ops import _client, _csrf, _launch, _preview
from test_serve_schedule import FakeBackend

from cnequity.config import load_config
from cnequity.orchestrator.run_window import marker_path
from cnequity.serve.ops.backups import inventory, restore_target
from cnequity.serve.ops.catalog import OpsError, build_argv, normalize, prepared, spec_for
from cnequity.serve.ops.records import atomic_write, read_record
from cnequity.serve.ops.runner import OpsService
from cnequity.serve.ops.scheduled import backup_session, claim_session, events_due, tick
from cnequity.serve.ops.scheduler import ScheduleService, settings, state_dir
from cnequity.storage.snapshots import SnapshotStore


@pytest.fixture
def published(config):
    path = config.curated_root / "daily_bars" / "trade_date=2024-06-18"
    path.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600000.SH"],
            "trade_date": [date(2024, 6, 18)],
            "close": [10.0],
            "fetched_at": [datetime(2024, 6, 18, tzinfo=timezone.utc)],
        }
    ).write_parquet(path / "part-merged.parquet")
    return config


def test_web_backup_verify_restore_real_children(published, tmp_path, monkeypatch):
    monkeypatch.delenv("CNE_OPS_HANDLER", raising=False)
    config = published
    client = _client(config)
    headers = _csrf(client)
    root = tmp_path / "外部备份"
    params = {"name": "web-backup", "datasets": ["daily_bars"], "snapshot_root": str(root)}
    preview = _preview(client, headers, "snapshot.create", params)
    assert "daily_bars" in preview["plan"]
    assert not root.exists(), "preview must not create a backup"
    record = _launch(client, headers, preview)
    assert record["state"] == "complete", record
    rows = client.get("/api/ops/backups", params={"root": str(root)}).json()
    assert rows["snapshots"][0]["name"] == "web-backup"
    assert rows["snapshots"][0]["datasets"] == ["daily_bars"]
    assert rows["snapshots"][0]["verified"] is False
    params.pop("datasets")
    checked = _launch(client, headers, _preview(client, headers, "snapshot.verify", params))
    assert checked["state"] == "complete", checked
    target = tmp_path / "恢复的湖"
    restored = _preview(client, headers, "snapshot.restore", {**params, "target": str(target)})
    response = client.post(
        "/api/ops/jobs",
        headers=headers,
        json={
            "preview_id": restored["preview_id"],
            "launch_token": restored["launch_token"],
            "acknowledged": ["confirm"],
        },
    )
    assert response.status_code == 422 and not target.exists()
    result = _launch(client, headers, restored, ["confirm", "heavy"])
    assert result["state"] == "complete", result
    restored_file = next((target / "curated").rglob("*.parquet"))
    assert pl.read_parquet(restored_file)["close"].to_list() == [10.0]
    assert client.app.state.config.data_root == config.data_root


@pytest.mark.usefixtures("chinese_output")
def test_changed_manifest_requires_new_preview(published):
    store = SnapshotStore(published)
    manifest = store.create("baseline", ["daily_bars"])
    client = _client(published)
    headers = _csrf(client)
    preview = _preview(client, headers, "snapshot.verify", {"name": "baseline"})
    manifest.write_bytes(manifest.read_bytes() + b"\n")
    response = client.post(
        "/api/ops/jobs",
        headers=headers,
        json={
            "preview_id": preview["preview_id"],
            "launch_token": preview["launch_token"],
            "acknowledged": ["confirm"],
        },
    )
    assert response.status_code == 422 and "变化" in response.json()["detail"]
    assert not client.get("/api/ops/jobs").json()


def test_tampered_payload_fails_before_restoring(published, tmp_path, monkeypatch):
    monkeypatch.delenv("CNE_OPS_HANDLER", raising=False)
    store = SnapshotStore(published)
    store.create("tampered", ["daily_bars"])
    next((store.path("tampered") / "data").rglob("*.parquet")).write_bytes(b"damaged")
    client = _client(published)
    headers = _csrf(client)
    checked = _launch(
        client, headers, _preview(client, headers, "snapshot.verify", {"name": "tampered"})
    )
    assert checked["state"] == "findings", checked
    target = tmp_path / "restore"
    restored = _launch(
        client,
        headers,
        _preview(client, headers, "snapshot.restore", {"name": "tampered", "target": str(target)}),
        ["confirm", "heavy"],
    )
    assert restored["state"] == "error", restored
    assert not target.exists()


def test_restore_target_rejects_active_nested_parent_occupied_or_linked(published, tmp_path):
    occupied = tmp_path / "occupied"
    occupied.mkdir()
    (occupied / "keep").write_text("keep")
    linked = tmp_path / "linked"
    linked.symlink_to(occupied, target_is_directory=True)
    for target in [
        published.data_root,
        published.data_root / "nested",
        tmp_path,
        published.meta_root / "snapshots" / "child",
        occupied,
        linked,
        Path("relative"),
    ]:
        with pytest.raises((OpsError, ValueError)):
            restore_target(published, str(target))
    assert (occupied / "keep").read_text() == "keep"


@pytest.mark.usefixtures("chinese_output")
def test_backup_args_are_repeated_and_bad_inventory_is_visible(published):
    spec = spec_for("snapshot.create")
    params = normalize(spec, {"name": "backup", "datasets": ["daily_bars"]}, published)
    argv = build_argv(spec, params, published.config_path)
    assert argv[argv.index("--dataset") + 1] == "daily_bars"
    repeated = build_argv(
        spec, {**params, "datasets": ["daily_bars", "instruments"]}, published.config_path
    )
    assert repeated.count("--dataset") == 2
    with pytest.raises(OpsError, match="首次配置"):
        prepared(spec_for("snapshot.verify"), {"name": "backup"}, None)
    with pytest.raises(OpsError):
        normalize(spec, {"name": "../bad", "datasets": ["daily_bars"]}, published)
    with pytest.raises(OpsError):
        normalize(spec, {"name": "empty", "datasets": []}, published)
    with pytest.raises(OpsError, match="混入"):
        normalize(
            spec,
            {
                "name": "unsafe",
                "datasets": ["daily_bars"],
                "snapshot_root": str(published.curated_root / "daily_bars"),
            },
            published,
        )
    store = SnapshotStore(published)
    store.root.mkdir(parents=True)
    (store.root / "invalid").mkdir()
    assert inventory(published)["snapshots"][0]["error"]
    readonly = _client(published, read_only=True)
    assert readonly.get("/api/ops/backups").status_code == 200
    assert readonly.post("/api/ops/preview", json={"op": "snapshot.restore"}).status_code == 404
    assert readonly.get("/api/ops/backups", params={"root": "relative"}).status_code == 422


@pytest.fixture
def automation(published, monkeypatch):
    path = published.config_path
    path.write_text(path.read_text() + '\n[job.events.groups.news]\nsteps = ["announcements"]\n')
    config = load_config(path)
    monkeypatch.setattr("cnequity.serve.ops.scheduler.SchedulerBackend", FakeBackend)
    return ScheduleService(config)


def auto_preview(service, **options):
    return service.preview(
        daily=False, stale=False, daily_run_at="17:30", stale_run_at="21:00", **options
    )


@pytest.mark.usefixtures("chinese_output")
def test_auto_backup_and_events_can_enable_without_daily(automation, tmp_path):
    plan = auto_preview(
        automation,
        backup=True,
        backup_datasets=["daily_bars"],
        backup_root=str(tmp_path / "backups"),
        events=True,
        events_group="news",
        events_interval_minutes=5,
    )
    assert plan["action"] == "启用 / 更新" and not automation.backend.calls
    result = automation.apply(plan["token"], acknowledged=True, requested_by="test")
    assert result["enabled"] and result["backup"] and result["events"]
    assert not result["daily"]
    assert result["backup_choices"] == ["daily_bars"]
    assert inventory(automation.config)["root"] == str(tmp_path / "backups")
    automation.backend.failure = True
    with pytest.raises(OpsError):
        automation.apply(
            auto_preview(automation, events=True, events_group="news")["token"],
            acknowledged=True,
            requested_by="test",
        )
    assert not settings(automation.config)["events"] and not settings(automation.config)["backup"]


@pytest.mark.parametrize(
    "options",
    [
        {"backup": True},
        {"backup": True, "backup_datasets": ["missing"]},
        {"backup_root": "relative"},
        {"events": True, "events_group": "missing"},
        {"events_interval_minutes": 0},
        {"events_interval_minutes": 1441},
        {"events_interval_minutes": True},
        {"backup_run_at": "24:00"},
    ],
)
def test_invalid_automatic_scope_refused(automation, options):
    with pytest.raises(OpsError):
        auto_preview(automation, **options)
    assert not automation.backend.calls


def test_backup_calendar_and_event_interval_are_independent_of_exchange(automation):
    config = automation.config
    saturday = datetime(2026, 10, 3, 14, tzinfo=timezone.utc)
    saved = {"backup_run_at": "23:00", "events_group": "news", "events_interval_minutes": 5}
    assert backup_session(config, saved, saturday) is None  # Beijing 22:00
    assert backup_session(config, {**saved, "backup_run_at": "21:00"}, saturday) == date(
        2026, 10, 3
    )
    atomic_write(
        state_dir(config) / "events_last.json", {"at": saturday.isoformat(), "group": "news"}
    )
    from datetime import timedelta

    assert not events_due(config, saved, saturday + timedelta(minutes=4, seconds=59))
    assert events_due(config, saved, saturday + timedelta(minutes=5))
    assert events_due(config, {**saved, "events_group": "new"}, saturday)
    atomic_write(state_dir(config) / "events_last.json", {"at": "bad", "group": "news"})
    with pytest.raises(ValueError):
        events_due(config, saved, saturday)


def freeze_children(monkeypatch):
    import subprocess

    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 10, 3, 14, tzinfo=timezone.utc).astimezone(tz)

    monkeypatch.setattr("cnequity.serve.ops.scheduled.datetime", Frozen)
    real_popen = subprocess.Popen
    script = (
        "import sys; from datetime import datetime,timezone; from pathlib import Path; "
        "import cnequity.serve.ops.scheduled as scheduled; "
        "scheduled.datetime=type('Frozen',(datetime,),{'now':classmethod(lambda cls,tz=None:datetime(2026,10,3,14,tzinfo=timezone.utc).astimezone(tz))}); "
        "from cnequity.serve.ops.job import execute; execute(Path(sys.argv[1]))"
    )

    def spawn(argv, **kwargs):
        if argv[1:3] == ["-m", "cnequity.ops.job"]:
            argv = [argv[0], "-c", script, argv[-1]]
        return real_popen(argv, **kwargs)

    monkeypatch.setattr("cnequity.serve.ops.runner.subprocess.Popen", spawn)


def wait_timer(config, report):
    import time

    assert report["status"] == "started", report
    service = OpsService(config, storage_busy=lambda: False)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        job = service.get_job(report["jobs"][0]["job_id"])
        if job["state"] not in {"starting", "running"}:
            return job
        time.sleep(0.05)
    pytest.fail("timer child did not finish")


@pytest.mark.usefixtures("chinese_output")
def test_real_auto_backup_runs_once_on_weekend_without_web(automation, monkeypatch):
    monkeypatch.delenv("CNE_OPS_HANDLER", raising=False)
    freeze_children(monkeypatch)
    plan = auto_preview(
        automation, backup=True, backup_run_at="21:00", backup_datasets=["daily_bars"]
    )
    automation.apply(plan["token"], acknowledged=True, requested_by="test")
    config = automation.config
    report = tick(config)
    job = wait_timer(config, report)
    assert job["state"] == "complete", job
    assert marker_path(config.meta_root, "backup", date(2026, 10, 3)).exists()
    rows = inventory(config)["snapshots"]
    assert len(rows) == 1 and rows[0]["name"].startswith("web-auto-2026-10-03-")
    assert tick(config)["status"] == "idle"
    with pytest.raises(OpsError, match="已执行"):
        claim_session(config, job["scheduled"])


@pytest.mark.usefixtures("chinese_output")
def test_real_events_attempt_is_throttled_even_after_failure(automation, monkeypatch):
    from test_serve_ops import _use_stub

    freeze_children(monkeypatch)
    _use_stub(monkeypatch, "fail")
    plan = auto_preview(automation, events=True, events_group="news", events_interval_minutes=1)
    automation.apply(plan["token"], acknowledged=True, requested_by="test")
    config = automation.config
    job = wait_timer(config, tick(config))
    assert job["state"] == "failed", job
    assert tick(config)["status"] == "idle"
    assert read_record(state_dir(config) / "events_last.json")["group"] == "news"
    with pytest.raises(OpsError, match="间隔"):
        claim_session(config, job["scheduled"])
    automation.apply(auto_preview(automation)["token"], acknowledged=True, requested_by="test")
    with pytest.raises(OpsError, match="暂停"):
        claim_session(config, job["scheduled"])


@pytest.mark.usefixtures("chinese_output")
def test_busy_timer_waits_without_consuming_attempt_and_changed_scope_rejects(
    automation, monkeypatch
):
    from cnequity.file_lock import exclusive_lock
    from cnequity.serve.ops.records import slot_path

    freeze_children(monkeypatch)
    plan = auto_preview(
        automation, backup=True, backup_run_at="21:00", backup_datasets=["daily_bars"]
    )
    automation.apply(plan["token"], acknowledged=True, requested_by="test")
    config = automation.config
    with exclusive_lock(slot_path(config), blocking=False):
        assert tick(config)["status"] == "waiting"
    assert not marker_path(config.meta_root, "backup", date(2026, 10, 3)).exists()
    with pytest.raises(OpsError, match="范围"):
        claim_session(
            config,
            {
                "job": "backup",
                "session": "2026-10-03",
                "datasets": [],
                "root": settings(config)["backup_root"],
            },
        )
    assert not marker_path(config.meta_root, "backup", date(2026, 10, 3)).exists()
