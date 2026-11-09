"""Dashboard operations: a whitelisted command, started out of process."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cnequity.config import load_config
from cnequity.orchestrator.manifest import Manifest
from cnequity.serve.app import create_app
from cnequity.serve.ops.catalog import (
    DERIVE_NAMES,
    EXCLUDED_CHOICES,
    OPS,
    command_template,
    describe,
    flags_of,
    spec_for,
)
from cnequity.serve.ops.job import aggregate, classify, last_json
from cnequity.serve.ops.preflight import mark_due_daily_session

ROOT = Path(__file__).resolve().parents[1]


def _client(config, **kwargs):
    return TestClient(create_app(config, **kwargs), base_url="http://127.0.0.1")


def _csrf(client) -> dict:
    home = client.get("/api/ops")
    assert home.status_code == 200, home.text
    token = home.json()["csrf_token"]
    return {
        "Origin": "http://127.0.0.1",
        "X-CNE-CSRF": token,
    }


def _use_stub(monkeypatch, mode: str = "ok") -> None:
    monkeypatch.setenv("CNE_OPS_HANDLER", "ops_handler:main")
    monkeypatch.setenv("CNE_OPS_STUB_MODE", mode)
    tests = str(ROOT)
    current = os.environ.get("PYTHONPATH", "")
    if tests not in current.split(os.pathsep):
        monkeypatch.setenv("PYTHONPATH", os.pathsep.join([tests, current]) if current else tests)


def _preview(client, headers, op: str, params: dict | None = None) -> dict:
    response = client.post(
        "/api/ops/preview", headers=headers, json={"op": op, "params": params or {}}
    )
    assert response.status_code == 200, response.text
    return response.json()


def _launch(
    client, headers, preview: dict, acknowledged: list[str] | None = None, *, timeout: float = 20
) -> dict:
    acks = ["confirm"] if acknowledged is None else acknowledged
    response = client.post(
        "/api/ops/jobs",
        headers=headers,
        json={
            "preview_id": preview["preview_id"],
            "launch_token": preview["launch_token"],
            "acknowledged": acks,
        },
    )
    if response.status_code == 409:
        return {
            "state": "rejected",
            "outcome": {"message": response.json()["detail"]},
            "job_id": None,
        }
    assert response.status_code == 202, response.text
    job = response.json()
    deadline = time.monotonic() + timeout
    while job["state"] in {"starting", "running"} and time.monotonic() < deadline:
        time.sleep(0.05)
        job = client.get(f"/api/ops/jobs/{job['job_id']}").json()
    return job


def test_catalog_flags_match_click():
    from cnequity.cli.main import cli

    root = cli.make_context("cne", [], resilient_parsing=True)
    for spec in OPS:
        node = cli
        context = root
        for name in spec.command:
            command = node.get_command(context, name)
            assert command is not None, spec.id
            context = command.make_context(name, [], parent=context, resilient_parsing=True)
            node = command
        options = {option for param in node.params for option in param.opts}
        for flag in flags_of(spec):
            assert flag in options, f"{spec.id} uses {flag}, Click has {sorted(options)}"
        for param in spec.params:
            if not param.choices:
                continue
            match = next(
                (
                    item
                    for item in node.params
                    if (param.flag and param.flag in item.opts)
                    or (param.flag is None and item.name == param.name)
                ),
                None,
            )
            choices = set(getattr(getattr(match, "type", None), "choices", ()) or ())
            if not choices:
                continue
            excluded = EXCLUDED_CHOICES.get((spec.id, param.name), frozenset())
            assert set(param.choices) | set(excluded) == choices


def test_command_templates_omit_the_local_config_path():
    daily = command_template(spec_for("daily.full"))
    assert (
        daily == "cne run daily [--trade-date TRADE_DATE] [--backfill] [--no-events] [--pack PACK]"
    )
    assert command_template(spec_for("init.start")).startswith("cne init --json --profile PROFILE")
    assert command_template(spec_for("backfill.run")).startswith("cne backfill DATASET")
    assert command_template(spec_for("daily.stale")).startswith("cne run daily --stale-only")
    cards = {card["id"]: card["command"] for card in describe(None, setup=True)}
    assert cards["daily.full"] == daily
    assert "--config" not in "".join(cards.values())


def test_click_walk_is_isolated_from_the_helper_above(config):
    """The alignment walk above is the contract. This one rejects a forged flag."""
    client = _client(config)
    headers = _csrf(client)
    rejected = client.post(
        "/api/ops/preview",
        headers=headers,
        json={"op": "daily.full", "params": {"config": "other.toml"}},
    )
    assert rejected.status_code == 422
    future = (datetime.now().date() + timedelta(days=2)).isoformat()
    dated = client.post(
        "/api/ops/preview",
        headers=headers,
        json={"op": "daily.full", "params": {"trade_date": future}},
    )
    assert dated.status_code == 422
    demo = client.post(
        "/api/ops/preview",
        headers=headers,
        json={"op": "init.start", "params": {"profile": "demo"}},
    )
    assert demo.status_code == 422


def test_preview_builds_the_command_and_a_token(config):
    client = _client(config)
    body = _preview(client, _csrf(client), "check.verify")
    assert body["command"].startswith("cne verify ")
    assert "--config" in body["command"]
    assert body["launch_token"]
    assert body["blockers"] == []


def test_job_and_backfill_preview_use_utf8_despite_parent_encoding(config, monkeypatch):
    from cnequity.config.bootstrap import path_for_toml

    folder = config.config_path.parent / "中文 路径"
    folder.mkdir()
    path = folder / "配置.toml"
    path.write_text(
        config.config_path.read_text(encoding="utf-8").replace(
            path_for_toml(config.data_root), path_for_toml(folder / "数据湖")
        ),
        encoding="utf-8",
    )
    config = load_config(path)
    _use_stub(monkeypatch, "unicode")
    monkeypatch.setenv("PYTHONIOENCODING", "ascii")
    monkeypatch.setenv("PYTHONUTF8", "0")
    client = _client(config)
    headers = _csrf(client)
    preview = _preview(
        client,
        headers,
        "backfill.run",
        {
            "dataset": "daily_bars",
            "start": "2024-06-01",
            "end": "2024-06-28",
            "symbols": ["600519.SH"],
        },
    )
    assert "中文 路径" in preview["command"]
    assert preview["plan"]
    assert "\ufffd" not in preview["plan"]
    done = _launch(client, headers, preview)
    assert done["state"] == "succeeded", done
    assert done["result"]["summary"]["message"] == "取数完成：中文"
    for stream, text in (("err", "进度：中文日志"), ("out", "取数完成：中文")):
        log = Path(done["logs"][stream]).read_text(encoding="utf-8")
        assert text in log
        response = client.get(f"/api/stream/ops/jobs/{done['job_id']}?stream={stream}")
        assert response.status_code == 200
        assert text in response.text
        assert "\ufffd" not in response.text


def test_mutations_require_the_page_and_a_loopback_host(config):
    client = _client(config)
    missing = client.post("/api/ops/preview", json={"op": "check.verify", "params": {}})
    assert missing.status_code == 403
    foreign = TestClient(create_app(config), base_url="http://example.test")
    home = foreign.get("/api/ops")
    assert home.status_code == 403
    remote = TestClient(create_app(config, token="s3cret"), base_url="http://example.test")
    denied = remote.post(
        "/api/ops/preview",
        headers={"Authorization": "Bearer s3cret", "Origin": "http://example.test"},
        json={"op": "check.verify", "params": {}},
    )
    assert denied.status_code == 403
    allowed = TestClient(
        create_app(config, token="s3cret", allow_remote_ops=True), base_url="http://example.test"
    )
    page = allowed.get("/api/ops", headers={"Authorization": "Bearer s3cret"})
    assert page.status_code == 200
    assert page.json()["mode"]["ops_enabled"] is True
    started = allowed.post(
        "/api/ops/preview",
        headers={
            "Authorization": "Bearer s3cret",
            "Origin": "http://example.test",
            "X-CNE-CSRF": page.json()["csrf_token"],
        },
        json={"op": "check.verify", "params": {}},
    )
    assert started.status_code == 200


def test_read_only_registers_no_write_routes(config):
    client = _client(config, read_only=True)
    mutations = {
        (route.path, method)
        for route in client.app.routes
        for method in getattr(route, "methods", set()) - {"GET", "HEAD"}
    }
    assert mutations == set()
    assert client.get("/api/ops").status_code == 200
    assert client.get("/api/ops").json()["mode"]["read_only"] is True


def test_normal_write_routes_are_exactly_the_whitelist(config):
    client = _client(config)
    mutations = {
        (route.path, method)
        for route in client.app.routes
        for method in getattr(route, "methods", set()) - {"GET", "HEAD"}
    }
    assert mutations == {
        ("/api/storage/reviews", "POST"),
        ("/api/storage/confirm", "POST"),
        ("/api/ops/preview", "POST"),
        ("/api/ops/jobs", "POST"),
        ("/api/ops/jobs/{job_id}/cancel", "POST"),
        ("/api/ops/schedule/preview", "POST"),
        ("/api/ops/schedule/apply", "POST"),
        ("/api/ops/settings/preview", "POST"),
        ("/api/ops/settings/apply", "POST"),
        ("/api/ops/sources/{source}/check-key", "POST"),
        ("/api/setup/config", "POST"),
    }


def test_a_stub_command_reports_success_partial_failure_and_findings(config, monkeypatch):
    _use_stub(monkeypatch, "ok")
    client = _client(config)
    headers = _csrf(client)
    done = _launch(client, headers, _preview(client, headers, "check.status"))
    assert done["state"] == "complete", done
    assert done["job_id"]

    monkeypatch.setenv("CNE_OPS_STUB_MODE", "partial")
    headers = _csrf(client)
    partial = _launch(client, headers, _preview(client, headers, "daily.full"))
    assert partial["state"] == "partial", partial
    assert partial["runs"]
    assert partial["runs"][0]["status"] == "warning"
    stored = Manifest(config.manifest_path).get_run(partial["runs"][0]["run_id"])
    assert json.loads(stored["metadata_json"])["launch"]["id"] == partial["job_id"]
    listed = client.get("/api/runs?limit=10").json()
    assert listed["total"] >= 1
    assert any(row["launch_id"] == partial["job_id"] for row in listed["runs"])
    detail = client.get(f"/api/runs/{partial['runs'][0]['run_id']}")
    assert detail.status_code == 200
    assert detail.json()["launch_id"] == partial["job_id"]

    monkeypatch.setenv("CNE_OPS_STUB_MODE", "fail")
    headers = _csrf(client)
    failed = _launch(client, headers, _preview(client, headers, "check.status"))
    assert failed["state"] == "failed"

    monkeypatch.setenv("CNE_OPS_STUB_MODE", "gate")
    headers = _csrf(client)
    findings = _launch(client, headers, _preview(client, headers, "check.verify"))
    assert findings["state"] == "findings"

    monkeypatch.setenv("CNE_OPS_STUB_MODE", "click")
    headers = _csrf(client)
    broken = _launch(client, headers, _preview(client, headers, "check.status"))
    assert broken["state"] == "error"
    assert "锁被占用" in broken["outcome"]["message"]

    monkeypatch.setenv("CNE_OPS_STUB_MODE", "boom")
    headers = _csrf(client)
    crashed = _launch(client, headers, _preview(client, headers, "check.status"))
    assert crashed["state"] == "error"
    assert "RuntimeError" in crashed["outcome"]["message"]


def test_one_slot_and_cancel(config, monkeypatch):
    _use_stub(monkeypatch, "sleep")
    client = _client(config)
    headers = _csrf(client)
    preview = _preview(client, headers, "check.status")
    queued = _preview(client, headers, "check.verify")
    first = client.post(
        "/api/ops/jobs",
        headers=headers,
        json={
            "preview_id": preview["preview_id"],
            "launch_token": preview["launch_token"],
            "acknowledged": [],
        },
    )
    assert first.status_code == 202, first.text
    job = first.json()
    deadline = time.monotonic() + 20
    while job["state"] == "starting" and time.monotonic() < deadline:
        time.sleep(0.05)
        job = client.get(f"/api/ops/jobs/{job['job_id']}").json()
    assert job["state"] == "running", job
    # Issued before the slot was taken, so the refusal comes from the child
    # rather than from the preview. A preview taken now simply has no token.
    second = _launch(client, headers, queued, acknowledged=[])
    assert second["state"] == "rejected"
    blocked = _preview(client, headers, "check.status")
    assert blocked["launch_token"] is None
    assert blocked["blockers"]
    cancelled = client.post(f"/api/ops/jobs/{job['job_id']}/cancel", headers=headers)
    assert cancelled.status_code == 202, cancelled.text
    deadline = time.monotonic() + 20
    while job["state"] in {"starting", "running"} and time.monotonic() < deadline:
        time.sleep(0.1)
        job = client.get(f"/api/ops/jobs/{job['job_id']}").json()
    assert job["state"] == "interrupted", job


def test_a_dead_running_record_is_reported_lost(config):
    client = _client(config)
    from cnequity.serve.ops.records import atomic_write, jobs_dir, public_record, slot_path

    job_id = "ab" * 16
    path = jobs_dir(config) / f"{job_id}.json"
    atomic_write(
        path,
        {
            "schema_version": 1,
            "job_id": job_id,
            "op": "check.status",
            "title": "新鲜度",
            "state": "running",
            "pid": 2**31 - 1,
            "created_at": "2020-01-01T00:00:00+00:00",
            "slot_lock": str(slot_path(config)),
            "logs": {},
        },
    )
    shown = public_record(config, json.loads(path.read_text(encoding="utf-8")))
    assert shown["state"] == "lost"
    assert client.get(f"/api/ops/jobs/{job_id}", headers=_csrf(client)).json()["state"] == "lost"


def test_a_job_finishing_during_status_read_is_not_reported_lost(config):
    from cnequity.serve.ops.records import atomic_write, jobs_dir, public_record

    job_id = "cd" * 16
    stale = {"job_id": job_id, "state": "running", "pid": 2**31 - 1}
    finished = {**stale, "state": "complete", "finished_at": "2026-10-03T00:00:00+00:00"}
    atomic_write(jobs_dir(config) / f"{job_id}.json", finished)
    shown = public_record(config, stale)
    assert shown["state"] == "complete"
    assert shown["finished_at"] == finished["finished_at"]


def test_launch_metadata_and_session_marker(config, monkeypatch):
    monkeypatch.setenv("CNE_LAUNCH_ID", "from-the-panel")
    run_id = Manifest(config.manifest_path).start_run("daily:core", {"trade_date": "2026-09-24"})
    stored = json.loads(Manifest(config.manifest_path).get_run(run_id)["metadata_json"])
    assert stored["launch"] == {"id": "from-the-panel", "source": "serve"}
    # An explicit launch is not overwritten.
    monkeypatch.setenv("CNE_LAUNCH_ID", "other")
    again = Manifest(config.manifest_path).start_run(
        "daily:core", {"trade_date": "2026-09-24", "launch": {"id": "kept", "source": "serve"}}
    )
    kept = json.loads(Manifest(config.manifest_path).get_run(again)["metadata_json"])
    assert kept["launch"]["id"] == "kept"

    early = datetime.fromisoformat("2026-09-24T09:20:00+00:00")
    due = datetime.fromisoformat("2026-09-24T09:40:00+00:00")
    monday = datetime.fromisoformat("2026-09-28T00:30:00+00:00")
    marker = config.meta_root / "state" / "scheduler" / "daily-2026-09-24.done"
    assert mark_due_daily_session(config, "daily.full", {}, now=early) is False
    assert not marker.exists()
    assert mark_due_daily_session(config, "daily.full", {"backfill": True}, now=due) is False
    assert not marker.exists()
    assert mark_due_daily_session(config, "daily.group", {}, now=due) is False
    assert mark_due_daily_session(config, "daily.full", {}, now=due) is True
    assert marker.exists()
    marker.unlink()
    # Monday 08:30 Beijing still owes Thursday, but a dateless command means "today".
    assert mark_due_daily_session(config, "daily.full", {}, now=monday) is False
    assert not marker.exists()
    assert mark_due_daily_session(config, "daily.full", {"trade_date": "2026-09-24"}, now=monday)
    assert marker.exists()


def test_classify_uses_runs_before_stdout():
    assert classify("run_json", 0, [{"status": "warning"}], "")[0] == "partial"
    assert classify("run_json", 0, [], '{"status": "nothing_stale"}')[0] == "skipped"
    assert classify("gate", 1, [], "")[0] == "findings"
    assert classify("report", 0, [], "hello")[0] == "complete"


def test_a_usable_init_that_still_owes_a_step_is_partial():
    usable = '{"status": "failed", "lake_usable": true, "owed_steps": ["corporate_actions"]}'
    assert classify("run_json", 0, [{"status": "failed"}], usable)[0] == "partial"
    unusable = '{"status": "failed", "lake_usable": false}'
    assert classify("run_json", 1, [{"status": "failed"}], unusable)[0] == "failed"
    assert aggregate(["success", "skipped_non_trading_day"]) == "succeeded"
    assert last_json('noise {"a": 1} tail {"status": "failed"}')["status"] == "failed"


def test_api_docs_open_in_first_time_setup(tmp_path, monkeypatch):
    """The setup banner prints the /api/docs link; it answered 409 until a lake existed."""
    monkeypatch.delenv("CNE_CONFIG", raising=False)
    client = TestClient(
        create_app(None, setup=True, config_path=tmp_path / "cnequity.toml"),
        base_url="http://127.0.0.1",
    )
    assert client.get("/api/docs").status_code == 200
    assert client.get("/api/openapi.json").status_code == 200
    assert client.get("/api/health").status_code == 409


def test_setup_wizard_writes_config_without_starting_uvicorn(tmp_path, monkeypatch):
    monkeypatch.delenv("CNE_CONFIG", raising=False)
    target = tmp_path / "cnequity.toml"
    data = tmp_path / "lake"
    client = TestClient(
        create_app(None, setup=True, config_path=target), base_url="http://127.0.0.1"
    )
    assert client.get("/api/health").status_code == 409
    headers = _csrf(client)
    relative = client.post(
        "/api/setup/config",
        headers=headers,
        json={"data_root": "relative/lake"},
    )
    assert relative.status_code == 422
    created = client.post(
        "/api/setup/config",
        headers=headers,
        json={"data_root": str(data)},
    )
    assert created.status_code == 200, created.text
    assert target.is_file()
    assert client.get("/api/health").status_code == 200
    again = client.post(
        "/api/setup/config",
        headers=headers,
        json={"data_root": str(data)},
    )
    assert again.status_code == 409


def test_setup_creates_missing_default_data_directories(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    target = tmp_path / "configs" / "cnequity.toml"
    client = TestClient(
        create_app(None, setup=True, config_path=target), base_url="http://127.0.0.1"
    )
    home = client.get("/api/ops").json()
    data = Path(home["mode"]["suggested_data_root"])
    assert not data.parent.exists()
    response = client.post(
        "/api/setup/config", headers=_csrf(client), json={"data_root": str(data)}
    )
    assert response.status_code == 200, response.text
    assert data.is_dir()
    assert load_config(target).data_root == data
    assert client.get("/api/ops").json()["mode"]["setup"] is False


def test_setup_rejects_a_file_in_the_data_directory_ancestors(tmp_path):
    parent = tmp_path / "file"
    parent.write_text("preserve me", encoding="utf-8")
    target = tmp_path / "configs" / "cnequity.toml"
    client = TestClient(
        create_app(None, setup=True, config_path=target), base_url="http://127.0.0.1"
    )
    response = client.post(
        "/api/setup/config",
        headers=_csrf(client),
        json={"data_root": str(parent / "nested" / "lake")},
    )
    assert response.status_code == 422
    assert not target.exists()
    assert parent.read_text(encoding="utf-8") == "preserve me"
    assert client.get("/api/ops").json()["mode"]["setup"] is True


def test_mode_reports_a_config_file_that_now_names_another_lake(tmp_path):
    """The server keeps its lake; the page is told the file moved on."""
    path = tmp_path / "cnequity.toml"
    first, second = tmp_path / "lake-a", tmp_path / "lake-b"
    path.write_text(f"[data]\nroot = '{first.as_posix()}'\n", encoding="utf-8")
    app = create_app(load_config(path))
    client = TestClient(app, base_url="http://127.0.0.1")

    def mode():
        response = client.get("/api/ops")
        assert response.status_code == 200, response.text
        return response.json()["mode"]

    assert mode()["data_root"] == str(first)
    assert mode()["config_drift"] is None

    path.write_text(f"[data]\nroot = '{second.as_posix()}'\n", encoding="utf-8")
    assert mode()["config_drift"] == {"kind": "data_root", "configured_data_root": str(second)}
    assert mode()["data_root"] == str(first)
    assert app.state.view.config.data_root == first
    assert client.get("/api/health").status_code == 200

    path.write_text("[data\nroot = ", encoding="utf-8")
    broken = mode()["config_drift"]
    assert (broken["kind"], broken["line"], broken["column"]) == ("invalid", 1, 6)
    assert "(at line" not in broken["detail"]
    assert client.get("/api/health").status_code == 200

    path.unlink()
    assert mode()["config_drift"] == {"kind": "missing"}

    path.write_text(f"[data]\nroot = '{first.as_posix()}'\n", encoding="utf-8")
    assert mode()["config_drift"] is None


@pytest.mark.usefixtures("chinese_output")
def test_jobs_cannot_start_while_the_config_file_names_another_lake(tmp_path):
    """A job reads the file itself, so it would work on the lake the page is
    not showing. Preview refuses; a token taken before the edit does too."""
    path = tmp_path / "cnequity.toml"
    first, second = tmp_path / "lake-a", tmp_path / "lake-b"
    path.write_text(f"[data]\nroot = '{first.as_posix()}'\n", encoding="utf-8")
    app = create_app(load_config(path))
    client = TestClient(app, base_url="http://127.0.0.1")
    headers = _csrf(client)
    early = _preview(client, headers, "check.verify")
    assert early["launch_token"]

    path.write_text(f"[data]\nroot = '{second.as_posix()}'\n", encoding="utf-8")
    held = _preview(client, headers, "check.verify")
    assert held["launch_token"] is None
    assert any(str(second) in item and "重启" in item for item in held["blockers"])
    refused = client.post(
        "/api/ops/jobs",
        headers=headers,
        json={"preview_id": early["preview_id"], "launch_token": early["launch_token"]},
    )
    assert refused.status_code == 422
    assert "重启" in refused.json()["detail"]
    assert not app.state.ops._previews[early["preview_id"]].used

    path.write_text("[data", encoding="utf-8")
    assert any("读不出来" in item for item in _preview(client, headers, "check.verify")["blockers"])

    path.write_text(f"[data]\nroot = '{first.as_posix()}'\n", encoding="utf-8")
    assert _preview(client, headers, "check.verify")["launch_token"]


def test_failed_group_retries_inherit_the_process_environment():
    import inspect

    from cnequity.cli.run_cmds import retry

    assert "env=" not in inspect.getsource(retry.callback)


def test_a_restarted_server_still_sees_the_running_job(config, monkeypatch):
    """The job is a separate process. A new server reads the same record."""
    from cnequity.orchestrator.scheduler_lock import pid_alive

    _use_stub(monkeypatch, "sleep")
    first = _client(config)
    headers = _csrf(first)
    preview = _preview(first, headers, "check.status")
    started = first.post(
        "/api/ops/jobs",
        headers=headers,
        json={
            "preview_id": preview["preview_id"],
            "launch_token": preview["launch_token"],
            "acknowledged": [],
        },
    )
    assert started.status_code == 202, started.text
    job = started.json()
    deadline = time.monotonic() + 20
    while job["state"] == "starting" and time.monotonic() < deadline:
        time.sleep(0.05)
        job = first.get(f"/api/ops/jobs/{job['job_id']}").json()
    assert job["state"] == "running", job
    pid = int(job["pid"])
    assert pid_alive(pid)
    # Drop the first server. The child was started in its own session.
    del first
    again = _client(config)
    seen = again.get(f"/api/ops/jobs/{job['job_id']}", headers=_csrf(again))
    assert seen.status_code == 200, seen.text
    assert seen.json()["state"] == "running"
    assert pid_alive(pid)
    cancelled = again.post(
        f"/api/ops/jobs/{job['job_id']}/cancel",
        headers=_csrf(again),
    )
    assert cancelled.status_code == 202, cancelled.text
    deadline = time.monotonic() + 20
    while pid_alive(pid) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not pid_alive(pid)


def test_finished_job_log_stream_replays_and_closes(config, monkeypatch):
    _use_stub(monkeypatch, "ok")
    client = _client(config)
    headers = _csrf(client)
    job = _launch(client, headers, _preview(client, headers, "check.status"), acknowledged=[])
    assert job["state"] == "complete"
    with client.stream("GET", f"/api/stream/ops/jobs/{job['job_id']}?stream=out") as response:
        assert response.status_code == 200
        body = "".join(response.iter_text())
    assert "success" in body
    assert '"done": true' in body or '"done":true' in body


def test_a_later_success_clears_the_continue_init_prompt(config):
    from cnequity.orchestrator.init_phases import expected_steps

    manifest = Manifest(config.manifest_path)
    phases = ["phase4_finalize"]
    init_id = manifest.start_run("init", {"phases": phases, "trade_date": "2026-07-06"})
    for step in expected_steps(phases):
        if step == "derive_industry_index":
            continue
        manifest.start_batch(init_id, f"b-{step}", task_id=step, dataset=step)
        manifest.finish_batch(init_id, f"b-{step}", "success")
    manifest.finish_run(init_id, "success")

    client = _client(config)
    pending = client.get("/api/ops").json()["occupancy"]["incomplete_init"]
    assert pending["run_id"] == init_id

    daily = manifest.start_run("daily:core")
    manifest.start_batch(
        daily, "b-idx", task_id="derive_industry_index", dataset="derive_industry_index"
    )
    manifest.finish_batch(daily, "b-idx", "success")

    assert client.get("/api/ops").json()["occupancy"]["incomplete_init"] is None
    preview = _preview(client, _csrf(client), "daily.full", {})
    assert all("没跑完" not in note for note in preview["hints"])


@pytest.mark.usefixtures("chinese_output")
def test_init_resume_only_accepts_the_latest_incomplete_run(config):
    manifest = Manifest(config.manifest_path)
    older = manifest.start_run("init", {"phases": ["phase1_reference"], "trade_date": "2026-07-30"})
    newer = manifest.start_run("init", {"phases": ["phase1_reference"], "trade_date": "2026-07-31"})
    client = _client(config)
    headers = _csrf(client)
    rejected = client.post(
        "/api/ops/preview",
        headers=headers,
        json={"op": "init.resume", "params": {"run_id": older}},
    )
    assert rejected.status_code == 422
    assert "最近一次" in rejected.json()["detail"]
    accepted = _preview(client, headers, "init.resume", {"run_id": newer})
    assert accepted["launch_token"]
    assert newer in accepted["command"]


def test_backfill_preview_is_a_real_plan_and_confirm_starts_it(config, monkeypatch):
    _use_stub(monkeypatch, "ok")
    client = _client(config)
    headers = _csrf(client)
    preview = _preview(
        client,
        headers,
        "backfill.run",
        {
            "dataset": "daily_bars",
            "start": "2024-06-01",
            "end": "2024-06-28",
            "symbols": ["600519.SH"],
        },
    )
    assert preview["launch_token"], preview
    assert "cne backfill daily_bars" in preview["command"]
    assert "--plan" not in preview["command"]
    assert preview["plan"]
    assert "daily_bars" in preview["plan"]
    job = _launch(client, headers, preview)
    assert job["state"] == "succeeded", job
    assert "backfill" in job["command"]
    assert "600519.SH" in job["command"]


def test_real_checks_on_a_sample_lake(tmp_path, monkeypatch):
    """status, verify and audit run the real commands against an offline lake."""
    from datetime import date

    from cnequity.cli.demo import run_sample_demo
    from cnequity.config import load_config

    monkeypatch.delenv("CNE_OPS_HANDLER", raising=False)
    root = tmp_path / "lake"
    path = tmp_path / "sample.toml"
    run_sample_demo(
        symbols=["600519.SH"],
        days=5,
        data_root=root,
        config_out=path,
        trade_date=date(2024, 6, 28),
    )
    config = load_config(path)
    client = _client(config)
    headers = _csrf(client)
    status = _launch(client, headers, _preview(client, headers, "check.status"), timeout=60)
    assert status["state"] == "complete", status
    verify = _launch(client, headers, _preview(client, headers, "check.verify"), timeout=90)
    assert verify["state"] in {"complete", "findings"}, verify
    audit = _launch(client, headers, _preview(client, headers, "check.audit"), timeout=90)
    assert audit["state"] in {"complete", "findings"}, audit
    # These three commands only read. They must finish as a check, not as a crash.
    assert {status["state"], verify["state"], audit["state"]} <= {"complete", "findings"}


@pytest.mark.usefixtures("chinese_output")
def test_explicit_missing_config_does_not_start_setup(tmp_path, monkeypatch):
    import uvicorn
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    monkeypatch.setattr(
        uvicorn, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("started"))
    )
    missing = tmp_path / "missing.toml"
    result = CliRunner().invoke(cli, ["serve", "--config", str(missing), "--port", "8791"])
    assert result.exit_code != 0
    assert "找不到配置" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_missing_default_config_opens_setup_without_binding(tmp_path, monkeypatch):
    from click.testing import CliRunner

    from cnequity.cli import consume_cmds
    from cnequity.cli.main import cli

    monkeypatch.delenv("CNE_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)
    captured: dict = {}

    def fake_bind(port):
        captured["bound"] = port
        v4 = type(
            "Sock",
            (),
            {"getsockname": lambda self: ("127.0.0.1", port), "close": lambda self: None},
        )()
        v6 = type(
            "Sock",
            (),
            {"getsockname": lambda self: ("::1", port, 0, 0), "close": lambda self: None},
        )()
        return [v4, v6]

    def fake_run(app, host, port, sockets):
        captured["setup"] = app.state.setup
        captured["host"] = host
        captured["port"] = port
        captured["sockets"] = len(sockets)

    monkeypatch.setattr(consume_cmds, "bind_loopback", fake_bind)
    monkeypatch.setattr(consume_cmds, "_run_server", fake_run)
    result = CliRunner().invoke(cli, ["serve", "--port", "8791"])
    assert result.exit_code == 0, result.output
    assert captured["setup"] is True
    assert captured["host"] == "127.0.0.1"
    assert captured["sockets"] == 2
    assert "http://localhost:8791/" in result.output
    assert "http://127.0.0.1:8791/" not in result.output
    assert "首次配置" in result.output


def test_loopback_accepts_ipv4_and_ipv6():
    """localhost on this machine is ::1 first. Both names have to connect."""
    import asyncio
    import urllib.request

    import uvicorn
    from fastapi import FastAPI

    from cnequity.cli.consume_cmds import bind_loopback

    app = FastAPI()

    @app.get("/ping")
    def ping() -> dict[str, bool]:
        return {"ok": True}

    sockets = bind_loopback(0)
    port = sockets[0].getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="warning", access_log=False))

    def status(url: str) -> int:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status

    async def run() -> None:
        task = asyncio.create_task(server._serve(sockets=sockets))
        try:
            for _ in range(100):
                if server.started:
                    break
                await asyncio.sleep(0.02)
            assert server.started
            loop = asyncio.get_running_loop()
            for url in (
                f"http://127.0.0.1:{port}/ping",
                f"http://[::1]:{port}/ping",
                f"http://localhost:{port}/ping",
            ):
                assert await loop.run_in_executor(None, status, url) == 200
        finally:
            server.should_exit = True
            await task

    try:
        asyncio.run(run())
    finally:
        for sock in sockets:
            sock.close()


@pytest.mark.usefixtures("chinese_output")
def test_a_taken_loopback_port_is_released_with_the_error():
    import socket

    import pytest
    from click import ClickException

    from cnequity.cli.consume_cmds import bind_loopback

    holder = socket.socket()
    holder.bind(("127.0.0.1", 0))
    port = holder.getsockname()[1]
    holder.listen(1)
    try:
        with pytest.raises(ClickException, match="已被占用"):
            bind_loopback(port)
    finally:
        holder.close()
    rebound = bind_loopback(port)
    try:
        families = {sock.family for sock in rebound}
        assert socket.AF_INET in families
    finally:
        for sock in rebound:
            sock.close()


def test_derive_names_are_the_ones_without_apply():
    assert "adj_factor_source" not in DERIVE_NAMES
    assert "bse_code_migration" not in DERIVE_NAMES


def test_job_record_write_retries_transient_windows_replace_denial(tmp_path, monkeypatch):
    """The panel polls a record while the job process rewrites it."""
    import cnequity.storage.atomic as atomic
    from cnequity.serve.ops.records import atomic_write, read_record, update_record

    real_replace = atomic.os.replace
    denials = {"left": 0}

    def _flaky_replace(src, dst):
        if denials["left"]:
            denials["left"] -= 1
            raise PermissionError(5, "Access is denied")
        real_replace(src, dst)

    import cnequity.file_replace as file_replace

    monkeypatch.setattr(file_replace, "_REPLACE_BACKOFF_SEC", 0.0)
    monkeypatch.setattr(atomic.os, "replace", _flaky_replace)
    path = tmp_path / "jobs" / f"{'a' * 32}.json"
    atomic_write(path, {"job_id": "a" * 32, "state": "starting", "label": "日线"})
    denials["left"] = 2
    update_record(path, state="running")

    assert denials["left"] == 0
    assert read_record(path)["state"] == "running"
    assert "日线" in path.read_text(encoding="utf-8")
    assert not list(path.parent.glob("*.tmp"))


def test_job_list_filters_by_time_and_operation(config):
    from cnequity.serve.ops.records import atomic_write, jobs_dir

    stamps = {
        "a" * 32: ("events.run", "2026-11-08T21:00:00+00:00"),
        "b" * 32: ("daily.full", "2026-11-08T09:30:00+00:00"),
        "c" * 32: ("events.run", "2026-11-08T08:00:00+00:00"),
        "d" * 32: ("check.status", "2026-11-07T08:00:00+00:00"),
    }
    for job_id, (op, created) in stamps.items():
        atomic_write(
            jobs_dir(config) / f"{job_id}.json",
            {"job_id": job_id, "op": op, "state": "complete", "created_at": created},
        )
    client = _client(config)
    since = client.get(
        "/api/ops/jobs", params={"since": "2026-11-08T00:00:00+00:00", "limit": 500}
    ).json()
    assert [job["job_id"] for job in since] == ["a" * 32, "b" * 32, "c" * 32]
    quiet = client.get("/api/ops/jobs", params={"exclude_op": "events.run"}).json()
    assert [job["op"] for job in quiet] == ["daily.full", "check.status"]
    only = client.get("/api/ops/jobs", params={"op": "check.status,daily.full", "limit": 1}).json()
    assert [job["op"] for job in only] == ["daily.full"]


def test_job_list_carries_rows_written_and_failed_datasets(config):
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.serve.ops.records import atomic_write, jobs_dir

    manifest = Manifest(config.manifest_path)
    run_id = manifest.start_run("daily:stale")
    manifest.start_batch(run_id, "b1", "task", "hot_rank")
    manifest.finish_batch(
        run_id, "b1", "failed", error_message="403", reason_code="source_transient"
    )
    manifest.start_batch(run_id, "b2", "task", "valuation_metrics")
    manifest.finish_batch(run_id, "b2", "success", rows_written=7)
    manifest.finish_run(run_id, "failed", rows_written=7)
    job_id = "e" * 32
    atomic_write(
        jobs_dir(config) / f"{job_id}.json",
        {
            "job_id": job_id,
            "op": "daily.stale",
            "state": "failed",
            "created_at": "2026-11-08T21:00:00+00:00",
            "result": {"status": "failed", "run_ids": [run_id]},
        },
    )
    job = _client(config).get("/api/ops/jobs").json()[0]
    assert job["rows_written"] == 7
    assert job["failed_datasets"] == [{"dataset": "hot_rank", "reason_codes": ["source_transient"]}]


def test_source_status_counts_requests_keys_and_todays_failures(config, monkeypatch):
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.serve.ops.preflight import source_status

    manifest = Manifest(config.manifest_path)
    run_id = manifest.start_run("events:news_wire")
    manifest.start_batch(run_id, "b1", "task", "news_headlines")
    manifest.finish_batch(run_id, "b1", "failed", error_message="proxy down")
    manifest.finish_run(run_id, "failed")
    monkeypatch.setattr(config, "tushare_token", None, raising=False)
    shown = source_status(config)
    assert shown["keys"]["tushare"] is False
    assert shown["failures"]["eastmoney"] == {"batches": 1, "datasets": ["news_headlines"]}
    assert "push2" in shown["requests"]


def test_a_key_check_probes_once_and_reports_the_verdict(config, monkeypatch):
    import cnequity.serve.ops.keys as keys
    from cnequity.adapters.tushare.client import TushareRefused

    monkeypatch.delenv("HITHINK_FINANCE_API_KEY", raising=False)
    config.tushare_token = "test-token"
    config.ths_official_api_key = None
    client = _client(config)
    headers = _csrf(client)
    assert client.post("/api/ops/sources/eastmoney/check-key", headers=headers).status_code == 404
    assert (
        client.post("/api/ops/sources/ths_official/check-key", headers=headers).status_code == 422
    )

    calls = []

    def refuse(cfg):
        calls.append(cfg)
        return "rejected", str(TushareRefused("trade_cal", 40101, "token 不对"))

    monkeypatch.setitem(keys._PROBES, "tushare", refuse)
    response = client.post("/api/ops/sources/tushare/check-key", headers=headers)
    assert response.status_code == 200, response.text
    assert response.json()["state"] == "rejected"
    assert "test-token" not in response.text
    assert (
        client.get("/api/ops").json()["occupancy"]["sources"]["key_checks"]["tushare"]["state"]
        == "rejected"
    )
    assert len(calls) == 1
    assert client.post("/api/ops/sources/tushare/check-key").status_code in (400, 403)
    assert len(calls) == 1


def test_the_tushare_probe_sorts_refusals_and_turns_the_source_on_only_for_itself(
    config, monkeypatch
):
    import cnequity.adapters.tushare.client as client_mod
    from cnequity.serve.ops.keys import _probe_tushare

    config.tushare_token = "test-token"
    config.sources = {**config.sources, "tushare": False}
    replies = iter(
        [
            client_mod.TusharePermissionDenied("trade_cal", 40203, "积分不足"),
            client_mod.TushareRefused("trade_cal", 40101, "token 不对"),
            None,
        ]
    )

    def fake_rows(self, api, params=None, fields=""):
        reply = next(replies)
        if reply is not None:
            raise reply
        return ["cal_date"], [["20261109"]]

    monkeypatch.setattr(client_mod.TushareClient, "rows", fake_rows)
    assert _probe_tushare(config)[0] == "limited"
    assert _probe_tushare(config)[0] == "rejected"
    assert _probe_tushare(config) == ("ok", "")
    assert config.sources["tushare"] is False


def test_the_freshness_anchor_waits_for_todays_daily_update(config):
    from datetime import date, datetime
    from datetime import time as clock
    from zoneinfo import ZoneInfo

    from cnequity.orchestrator.run_window import current_anchor, mark_done

    monday = date(2026, 11, 9)
    config.daily_run_at = "17:30"

    def anchor_at(hour, minute):
        now = datetime.combine(monday, clock(hour, minute), tzinfo=ZoneInfo("Asia/Shanghai"))
        return current_anchor(config, now)

    assert anchor_at(14, 0) == date(2026, 11, 6), "the session is not final yet"
    assert anchor_at(16, 0) == date(2026, 11, 6), "before 17:30 Friday is still the anchor"
    assert anchor_at(17, 30) == monday
    mark_done(config.meta_root, "daily", monday)
    assert anchor_at(16, 0) == monday, "a finished daily update counts at once"
