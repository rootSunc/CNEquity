"""Browser approvals and deletion safety on isolated, tiny local lakes."""

import json
import time

import polars as pl
import pytest
from click.testing import CliRunner
from fastapi.testclient import TestClient

from cnequity.cli.main import cli
from cnequity.config import Config
from cnequity.serve.app import create_app
from cnequity.storage.lifecycle import LifecycleStore, digest
from cnequity.storage.revisions import RevisionStore


@pytest.fixture
def web(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    writer = RevisionStore(cfg.meta_root, cfg.curated_root)
    source = cfg.curated_root / "daily_bars/part.parquet"
    source.parent.mkdir(parents=True)
    for i in range(7):
        pl.DataFrame({"close": [float(i)]}).write_parquet(source)
        writer.commit(
            "daily_bars",
            run_id=str(i),
            changed_files=[source],
            schema_version=1,
            contract_fingerprint="test",
        )
    store = LifecycleStore(cfg.meta_root)
    store.import_manifest(
        {
            "schema_version": 1,
            "meta_root": str(store.meta),
            "holds": {},
            "reference_roots": [],
            "reference_fingerprint": digest([]),
        }
    )
    client = TestClient(create_app(cfg), base_url="http://127.0.0.1")
    return client, cfg, store, writer


def headers(client):
    return {
        "Origin": "http://127.0.0.1",
        "X-CNE-Storage-CSRF": client.app.state.storage_maintenance.csrf,
    }


def wait(client, response):
    assert response.status_code == 202, response.text
    job = response.json()
    deadline = time.monotonic() + 10
    while job["status"] in {"checking", "executing"}:
        assert time.monotonic() < deadline, job
        time.sleep(0.01)
        job = client.get(f"/api/storage/jobs/{job['job_id']}").json()
    return job


def scheduled(client) -> list[str]:
    """The history the next backfill or daily run would delete (keep 5)."""
    summary = client.get("/api/storage").json()
    return [o["object_id"] for o in summary["objects"] if o["status"] == "scheduled"]


def review(client, kind="revisions", phase="purge", **kwargs):
    if kind == "revisions" and not kwargs:
        kwargs = {"object_ids": scheduled(client)}
    return wait(
        client,
        client.post(
            "/api/storage/reviews",
            headers=headers(client),
            json={"kind": kind, "phase": phase, **kwargs},
        ),
    )


def confirm(client, job, **changes):
    body = {
        "review_id": job["job_id"],
        "confirmation_token": job["confirmation_token"],
        "confirmed": True,
        "maintenance_confirmed": True,
    }
    body.update(changes)
    return client.post("/api/storage/confirm", headers=headers(client), json=body)


def test_reading_and_review_never_delete_then_explicit_confirmation_does(web):
    client, cfg, store, writer = web
    current = writer.current_root("daily_bars")
    contents = (current / "part.parquet").read_bytes()
    original = store.registry()
    for _ in range(2):
        summary = client.get("/api/storage").json()
        assert summary["keep"] == 5 and summary["automatic_retention"] is True
        assert sum(o["status"] == "scheduled" for o in summary["objects"]) == 2
        assert sum(o["status"] == "protected" for o in summary["objects"]) == 6
        in_use = [o for o in summary["objects"] if o["current"]]
        assert len(in_use) == 1
        assert in_use[0]["kind"] == "revisions"
        assert "current_generation" in in_use[0]["reasons"]
        assert sum(o["kind"] == "revisions" and not o["current"] for o in summary["objects"]) == 7
    assert store.registry() == original
    job = review(client)
    assert len(job["objects"]) == 2
    assert len(store.inspect()["objects"]) == 8
    assert confirm(client, job, confirmed=False).status_code == 409
    assert confirm(client, job, maintenance_confirmed=False).status_code == 409
    assert confirm(client, job, confirmation_token="wrong").status_code == 409
    assert confirm(client, job, confirmed="true").status_code == 422
    result = wait(client, confirm(client, job))
    assert result["status"] == "complete", result
    assert len(store.inspect()["objects"]) == 6
    assert (current / "part.parquet").read_bytes() == contents
    assert writer.current_root("daily_bars") == current
    assert confirm(client, job).status_code == 409
    assert not client.app.state.storage_maintenance.gate.closed


def test_operator_can_delete_selected_history_and_the_result_reports_space(web):
    client, _, store, writer = web
    current = writer.current_root("daily_bars")
    contents = (current / "part.parquet").read_bytes()
    summary = client.get("/api/storage").json()
    recent = [
        o for o in summary["objects"] if not o["current"] and o["reasons"] == ["recent_generation"]
    ]
    assert len(recent) >= 2
    live = next(o for o in summary["objects"] if o["current"])
    legacy = next(o for o in summary["objects"] if "unreceipted_generation" in o["reasons"])
    assert review(client, object_ids=[live["object_id"]])["status"] == "error"
    assert review(client, object_ids=[legacy["object_id"]])["status"] == "error"
    mark = {"kind": "revisions", "phase": "mark", "object_ids": [recent[0]["object_id"]]}
    assert (
        client.post("/api/storage/reviews", headers=headers(client), json=mark).status_code == 422
    )
    store.hold(recent[0]["object_id"], "audit")
    assert review(client, object_ids=[recent[0]["object_id"]])["status"] == "error"
    assert len(store.inspect()["objects"]) == 8

    job = review(client, object_ids=[recent[1]["object_id"]])
    assert job["status"] == "ready", job
    assert [item["label"] for item in job["objects"]] == [recent[1]["label"]]
    result = wait(client, confirm(client, job))
    assert result["status"] == "complete", result
    journal = result["result"]
    assert journal["items"][recent[1]["object_id"]] == "deleted"
    assert journal["logical_bytes_deleted"] == recent[1]["logical_bytes"]
    assert isinstance(journal["filesystem_free_bytes_before"], int)
    assert isinstance(journal["filesystem_free_bytes_after"], int)
    assert len(store.inspect()["objects"]) == 7
    assert (current / "part.parquet").read_bytes() == contents
    assert writer.current_root("daily_bars") == current

    summary = client.get("/api/storage").json()
    unmarked = next(o for o in summary["objects"] if o["status"] == "scheduled" and can_pick(o))
    job = review(client, object_ids=[unmarked["object_id"]])
    assert job["status"] == "ready", job
    result = wait(client, confirm(client, job))
    assert result["status"] == "complete", result
    assert result["result"]["logical_bytes_deleted"] == unmarked["logical_bytes"]
    assert unmarked["object_id"] not in {o["object_id"] for o in store.inspect()["objects"]}
    assert (current / "part.parquet").read_bytes() == contents


def can_pick(obj):
    return not obj["current"] and set(obj["reasons"]) <= {"recent_generation"}


def staged_run(cfg, *, incomplete=False):
    from cnequity.orchestrator.manifest import Manifest

    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily", {})
    manifest.start_batch(run_id, "data", "daily_bars", "daily_bars")
    manifest.finish_batch(run_id, "data", "failed" if incomplete else "success")
    manifest.start_batch(run_id, "compact", "compact", "compact")
    manifest.finish_batch(run_id, "compact", "success")
    manifest.finish_run(run_id, "failed" if incomplete else "warning")
    path = cfg.staging_root / "daily_bars" / f"run_id={run_id}"
    path.mkdir(parents=True)
    (path / "batch.parquet").write_bytes(b"retained staging bytes")
    return run_id, path


def test_staging_browser_confirmation_is_selected_and_leaves_incomplete_batches(web):
    client, cfg, _, writer = web
    current = writer.current_root("daily_bars")
    selected, path = staged_run(cfg)
    other, other_path = staged_run(cfg)
    incomplete, unfinished_path = staged_run(cfg, incomplete=True)
    summary = client.get("/api/storage").json()
    staging = {o["object_id"]: o for o in summary["objects"] if o["kind"] == "staging"}
    assert staging[f"staging/{selected}"]["reasons"] == []
    assert staging[f"staging/{incomplete}"]["reasons"] == ["incomplete_batches"]
    staging_mark = {"kind": "staging", "phase": "mark"}
    assert (
        client.post("/api/storage/reviews", headers=headers(client), json=staging_mark).status_code
        == 422
    )
    assert review(client, kind="staging", object_ids=[f"staging/{incomplete}"])["status"] == "error"
    job = review(client, kind="staging", object_ids=[f"staging/{selected}"])
    assert job["status"] == "ready", job
    assert path.exists() and other_path.exists() and unfinished_path.exists()
    assert confirm(client, job, maintenance_confirmed=False).status_code == 409
    assert confirm(client, job, confirmation_token="wrong").status_code == 409
    result = wait(client, confirm(client, job))
    assert result["status"] == "complete", result
    assert result["result"]["logical_bytes_deleted"] == len(b"retained staging bytes")
    assert result["result"]["items"] == {f"daily_bars/run_id={selected}": "deleted"}
    assert not path.exists() and other_path.exists() and unfinished_path.exists()
    assert writer.current_root("daily_bars") == current
    assert f"staging/{other}" in {
        o["object_id"] for o in client.get("/api/storage").json()["objects"]
    }
    assert confirm(client, job).status_code == 409


@pytest.mark.parametrize(
    "change",
    [
        "content",
        "extra_path",
        "hold",
        "resume",
        "run_lock",
        "symlink",
        "reference",
        "lake_identity",
    ],
)
def test_staging_rechecks_all_selected_objects_before_deletion(web, change):
    from cnequity.file_lock import exclusive_lock
    from cnequity.orchestrator.manifest import Manifest

    client, cfg, store, _ = web
    first, first_path = staged_run(cfg)
    second, path = staged_run(cfg)
    job = review(client, kind="staging", object_ids=[f"staging/{first}", f"staging/{second}"])
    assert job["status"] == "ready", job
    lock = None
    if change == "content":
        (path / "batch.parquet").write_bytes(b"altered bytes")
    elif change == "extra_path":
        (cfg.staging_root / "adj_factors" / f"run_id={second}").mkdir(parents=True)
    elif change == "hold":
        store.hold(f"staging/{second}", "review")
    elif change == "resume":
        Manifest(cfg.manifest_path).resume_run(second)
    elif change == "symlink":
        (path / "link").symlink_to(first_path, target_is_directory=True)
    elif change == "reference":
        registered = cfg.data_root / "research"
        registered.mkdir()
        registry = store.registry()
        registry["reference_roots"] = [{"path": str(registered), "exclude": []}]
        store._save(registry)
        (registered / "new-reference.json").write_text("{}")
    elif change == "lake_identity":
        registry = store.registry()
        registry["lake_id"] = "another-lake"
        store._save(registry)
    else:
        lock = exclusive_lock(cfg.meta_root / "locks" / f"{second}.lock")
        lock.__enter__()
    try:
        result = wait(client, confirm(client, job))
        assert result["status"] == "error", result
        assert first_path.exists() and path.exists()
    finally:
        if lock:
            lock.__exit__(None, None, None)


def test_staging_interrupted_deletion_requires_fresh_browser_review_and_resumes(web, monkeypatch):
    from cnequity.storage.lifecycle.staging import StagingRetirement

    client, cfg, _, _ = web
    run_id, path = staged_run(cfg)
    (path / "other.parquet").write_bytes(b"more bytes")
    job = review(client, kind="staging", object_ids=[f"staging/{run_id}"])
    import shutil

    actual = shutil.rmtree

    def interrupted(target, *args, **kwargs):
        if "staging-trash" in str(target):
            (target / "batch.parquet").unlink()
            raise OSError("interrupted after one deletion")
        return actual(target, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr("cnequity.storage.lifecycle.staging.shutil.rmtree", interrupted)
        result = wait(client, confirm(client, job))
    assert result["status"] == "error", result
    assert not path.exists()
    summary = client.get("/api/storage").json()
    assert {"kind": "staging", "plan_id": job["plan_id"]} in summary["unfinished"]
    assert review(client, kind="revisions", resume_plan_id=job["plan_id"])["status"] == "error"
    assert review(client, kind="staging")["status"] == "error"
    resumed = review(client, kind="staging", resume_plan_id=job["plan_id"])
    assert resumed["status"] == "ready", resumed
    result = wait(client, confirm(client, resumed))
    assert result["status"] == "complete", result
    assert result["result"]["logical_bytes_deleted"] == len(b"retained staging bytesmore bytes")
    assert not client.get("/api/storage").json()["unfinished"]
    assert StagingRetirement(cfg).apply(job["plan_id"], maintenance=True) == result["result"]


def test_staging_orphans_and_linked_content_are_not_browser_deletable(web):
    client, cfg, _, _ = web
    orphan = cfg.staging_root / "daily_bars/run_id=orphan"
    orphan.mkdir(parents=True)
    (orphan / "part").write_bytes(b"keep")
    assert review(client, kind="staging", object_ids=["staging/orphan"])["status"] == "error"
    run_id, path = staged_run(cfg)
    (path / "link").symlink_to(orphan, target_is_directory=True)
    assert review(client, kind="staging", object_ids=[f"staging/{run_id}"])["status"] == "error"
    assert orphan.exists() and path.exists()


def test_staging_purge_requires_import_and_rejects_paths_outside_selected_run(web):
    import json

    from cnequity.storage.lifecycle import LifecycleError
    from cnequity.storage.lifecycle.staging import StagingRetirement

    _, cfg, store, _ = web
    _, path = staged_run(cfg)
    retirement = StagingRetirement(cfg)
    plan = retirement.plan()
    plan.pop("plan_id")
    item = plan["objects"][0]
    item["paths"] = {"../curated": next(iter(item["paths"].values()))}
    forged_id = digest(plan)
    (store.root / "staging-plans" / f"{forged_id}.json").write_text(json.dumps(plan))
    with pytest.raises(LifecycleError, match="escapes"):
        retirement.apply(forged_id, maintenance=True)
    store.registry_path.unlink()
    with pytest.raises(LifecycleError):
        retirement.plan()
    assert path.exists()


@pytest.mark.parametrize(
    "bad",
    [
        {},
        {"Origin": "http://evil.example"},
        {"Sec-Fetch-Site": "cross-site"},
        {"X-CNE-Storage-CSRF": "wrong"},
    ],
)
def test_origin_and_csrf_required(web, bad):
    client, _, store, _ = web
    supplied = {} if not bad else headers(client) | bad
    response = client.post("/api/storage/reviews", headers=supplied, json={"kind": "revisions"})
    assert response.status_code == 403
    assert not client.app.state.storage_maintenance.jobs
    assert len(store.inspect()["objects"]) == 8


def test_dns_rebinding_and_remote_auth(web):
    client, cfg, _, _ = web
    assert client.get("/api/storage", headers={"Host": "evil.example"}).status_code == 403
    remote = TestClient(create_app(cfg, token="secret"), base_url="http://remote.example")
    assert remote.get("/api/storage").status_code == 401
    assert remote.get("/api/storage", headers={"Authorization": "Bearer secret"}).status_code == 200


def test_stale_review_fails_without_deletion(web):
    client, _, store, _ = web
    job = review(client)
    store.hold(job["objects"][0]["object_id"], "new dependency")
    result = wait(client, confirm(client, job))
    assert result["status"] == "error"
    assert len(store.inspect()["objects"]) == 8
    assert not client.app.state.storage_maintenance.gate.closed


def test_expired_confirmation_and_restart_are_rejected(web):
    client, cfg, store, _ = web
    job = review(client)
    client.app.state.storage_maintenance.jobs[job["job_id"]]["_expires"] = 0
    assert confirm(client, job).status_code == 409
    restarted = TestClient(create_app(cfg), base_url="http://127.0.0.1")
    assert confirm(restarted, job).status_code == 409
    assert len(store.inspect()["objects"]) == 8


def test_execution_drains_reads_and_blocks_new_ones_but_keeps_shell_available(web):
    client, _, _, _ = web
    assert client.get("/api/storage").status_code == 200
    job = review(client)
    gate = client.app.state.storage_maintenance.gate
    assert gate.enter()
    response = confirm(client, job)
    try:
        assert response.status_code == 202
        assert gate.closed
        cached = client.get("/api/storage").json()
        assert cached["inventory_paused"] is True
        assert cached["active_jobs"][0]["status"] == "executing"
        assert client.get("/api/datasets").status_code == 503
        assert client.get("/").status_code == 200
        assert client.get(f"/api/storage/jobs/{response.json()['job_id']}").status_code == 200
        assert (
            client.post(
                "/api/storage/reviews", headers=headers(client), json={"kind": "revisions"}
            ).status_code
            == 409
        )
    finally:
        gate.leave()
    assert wait(client, response)["status"] == "complete"


def test_there_is_no_mark_phase_and_a_review_needs_a_selection(web):
    """The seven-day mark phase was retired: deletion is always of chosen versions."""
    client, _, store, _ = web
    for body in ({"kind": "revisions", "phase": "mark"}, {"kind": "revisions"}):
        response = client.post("/api/storage/reviews", headers=headers(client), json=body)
        if response.status_code == 202:
            assert wait(client, response)["status"] == "error"
        else:
            assert response.status_code == 422
    assert "pending" not in store.registry()
    assert len(store.inspect()["objects"]) == 8


def test_marks_from_the_retired_phase_are_dropped(web):
    client, _, store, _ = web
    value = store.registry()
    value["pending"] = {"revision/daily_bars/gone": {"not_before": "2026-10-05T00:00:00+00:00"}}
    store._save(value)
    assert "pending" not in store.registry()
    store.hold(scheduled(client)[0], "keep it")
    assert "pending" not in store.registry_path.read_text(encoding="utf-8")


def test_registered_experiments_from_older_versions_are_left_alone(web, tmp_path):
    """Experiment retirement is gone; a registry that still lists one must load,
    show no experiment row and offer no way to delete its directory."""
    client, _, store, _ = web
    source = tmp_path / "cne-old"
    source.mkdir()
    value = store.registry()
    value["experiments"] = [
        {
            "object_id": "experiment/test",
            "path": str(source),
            "status": "needs_classification",
            "references": [],
        }
    ]
    value["holds"]["experiment/test"] = ["kept by an older version"]
    store._save(value)

    report = store.inspect()
    assert "experiments" not in report
    assert "experiment/test" not in report["resource_holds"]
    summary = client.get("/api/storage").json()
    assert all(not o["object_id"].startswith("experiment/") for o in summary["objects"])
    response = client.post(
        "/api/storage/reviews",
        headers=headers(client),
        json={"kind": "experiments", "phase": "purge"},
    )
    assert response.status_code == 422
    assert source.exists()
    # The next registry write drops the retired entries; the hold stays.
    store.hold(scheduled(client)[0], "keep it")
    saved = json.loads(store.registry_path.read_text(encoding="utf-8"))
    assert "experiments" not in saved
    assert "experiment/test" in saved["holds"]


def test_failed_review_does_not_leave_service_busy(web, monkeypatch):
    client, _, _, _ = web
    service = client.app.state.storage_maintenance

    def fail(**kwargs):
        raise OSError("unreadable evidence")

    monkeypatch.setattr(service.store, "plan", fail)
    assert review(client)["status"] == "error"
    assert not service.busy and not service.gate.closed


def test_clean_without_dry_run_never_requests_deletion(web, monkeypatch):
    client, cfg, _, _ = web
    import cnequity.cli.maintain_cmds as commands

    monkeypatch.setattr(commands, "_cfg", lambda _: cfg)
    for name in [
        "clean_stale_lock_files",
        "clean_source_snapshots",
        "clean_run_logs",
    ]:
        original = getattr(commands, name)

        def checked(*args, _original=original, **kwargs):
            assert kwargs["dry_run"] is True
            return _original(*args, **kwargs)

        monkeypatch.setattr(commands, name, checked)
    result = CliRunner().invoke(cli, ["run", "clean", "--force"])
    assert result.exit_code == 0, result.output


def test_dashboard_cannot_be_framed_for_clickjacking(web):
    client, _, _, _ = web
    response = client.get("/")
    assert response.headers["content-security-policy"] == "frame-ancestors 'none'"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["referrer-policy"] == "no-referrer"


def test_partial_failure_needs_another_web_review_and_confirmation(web, monkeypatch):
    import cnequity.storage.lifecycle.purge as purge

    client, _, store, _ = web
    job = review(client)

    def fail(path):
        next(path.rglob("*.parquet")).unlink()
        raise OSError("injected partial deletion")

    with monkeypatch.context() as patch:
        patch.setattr(purge.shutil, "rmtree", fail)
        assert wait(client, confirm(client, job))["status"] == "error"
    summary = client.get("/api/storage").json()
    assert summary["unfinished"] == [{"kind": "revisions", "plan_id": job["plan_id"]}]
    assert review(client)["status"] == "error"
    retry = review(client, resume_plan_id=job["plan_id"])
    assert retry["status"] == "ready" and retry["resuming"]
    assert retry["objects"] == job["objects"]
    assert confirm(client, retry, confirmed=False).status_code == 409
    assert wait(client, confirm(client, retry))["status"] == "complete"
    assert len(store.inspect()["objects"]) == 6


def test_background_scan_holds_the_same_maintenance_gate(web, monkeypatch):
    from threading import Event
    from types import SimpleNamespace

    client, _, _, _ = web
    view = client.app.state.view
    gate = client.app.state.storage_maintenance.gate
    entered, release, finished = Event(), Event(), Event()

    def scan(_):
        entered.set()
        assert release.wait(5)
        finished.set()
        return None

    monkeypatch.setattr(
        "cnequity.serve.lake.stats_freshness", lambda _: SimpleNamespace(stale=True)
    )
    monkeypatch.setattr("cnequity.serve.lake.refresh_stats_if_stale", scan)
    assert view.refresh_stats_in_background()
    try:
        assert entered.wait(5)
        assert gate.readers == 1
        gate.close()
        assert not view.refresh_stats_in_background()
    finally:
        release.set()
        assert finished.wait(5)
        gate.drain(timeout=5)
        gate.reopen()
    assert gate.readers == 0
