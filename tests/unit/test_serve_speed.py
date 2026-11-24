"""Dashboard caching, the storage precheck, job list size and keyed sources."""

from __future__ import annotations

import os
import stat
import threading
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cnequity.config import load_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.serve.app import create_app
from cnequity.serve.lake import LakeView

#: A made-up key; low entropy so secret scanners do not take it for a real one.
_FAKE_KEY = "fake-key-for-tests"


def _client(config, **kwargs):
    return TestClient(create_app(config, **kwargs), base_url="http://127.0.0.1")


# --- version-keyed summaries ----------------------------------------------


def test_a_summary_is_kept_until_the_lake_moves_then_rebuilt_in_the_background(config, monkeypatch):
    version = ["v1"]
    monkeypatch.setattr("cnequity.serve.lake.lake_version", lambda cfg: (version[0],))
    # Not throttled here; a running backfill is what the throttle is for.
    monkeypatch.setattr("cnequity.serve.lake._REBUILD_MIN_INTERVAL", 0.0)
    view = LakeView(config)
    builds = []
    released = threading.Event()

    def build():
        builds.append(version[0])
        if len(builds) > 1:
            released.wait(5)
        return f"built at {version[0]}"

    assert view._cached("k", build) == "built at v1"
    assert view._cached("k", build) == "built at v1"
    assert builds == ["v1"], "an unchanged lake is not walked again"

    version[0] = "v2"
    view._version = None
    # The old answer at once; the rebuild runs behind it.
    assert view._cached("k", build) == "built at v1"
    released.set()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline and view._cached("k", build) != "built at v2":
        time.sleep(0.02)
    assert view._cached("k", build) == "built at v2"
    assert builds == ["v1", "v2"]


def test_invalidate_makes_the_next_read_wait_for_a_fresh_build(config, monkeypatch):
    monkeypatch.setattr("cnequity.serve.lake.lake_version", lambda cfg: ("same",))
    view = LakeView(config)
    values = iter(["first", "second"])
    assert view._cached("k", lambda: next(values)) == "first"
    view.invalidate()
    assert view._cached("k", lambda: next(values)) == "second"


def test_lake_summaries_answer_304_while_the_lake_is_unchanged(config):
    client = _client(config)
    first = client.get("/api/tiers")
    assert first.status_code == 200, first.text
    tag = first.headers["etag"]
    assert first.headers["cache-control"] == "no-cache"
    again = client.get("/api/tiers", headers={"If-None-Match": tag})
    assert again.status_code == 304
    # Another language is another answer.
    chinese = client.get("/api/tiers", headers={"If-None-Match": tag, "X-CNE-Lang": "zh-CN"})
    assert chinese.status_code == 200


def test_the_dataset_page_carries_its_anchor(config):
    client = _client(config)
    body = client.get("/api/datasets/daily_bars")
    assert body.status_code == 200, body.text
    assert "anchor" in body.json()


# --- operations ---------------------------------------------------------


def test_the_job_list_leaves_results_and_configs_to_the_job_page():
    from cnequity.serve.ops.routes import _list_item

    job = {
        "job_id": "j1",
        "op": "daily.full",
        "effective_config": {"big": "x" * 5000},
        "result": {
            "status": "degraded",
            "run_ids": ["r1"],
            "summary": {
                "groups": [
                    {
                        "group": "core",
                        "status": "degraded",
                        "details": "x" * 5000,
                        "fallback": [{"dataset": "fund_flow", "reason_codes": ["paused"], "x": 1}],
                    }
                ],
                "events": {"fallback": [], "noise": "x" * 5000},
                "fallback": [],
                "steps": ["x" * 5000],
            },
        },
    }
    item = _list_item(job)
    assert "effective_config" not in item
    assert item["result"]["run_ids"] == ["r1"]
    assert item["result"]["summary"]["groups"] == [
        {
            "group": "core",
            "status": "degraded",
            "fallback": [{"dataset": "fund_flow", "reason_codes": ["paused"]}],
        }
    ]
    assert len(repr(item)) < 1000


def test_the_status_parts_that_read_the_lake_are_kept_per_version(config, monkeypatch):
    from cnequity.ops import preflight

    calls = []
    monkeypatch.setattr(preflight, "lake_empty", lambda cfg: calls.append("empty") or True)
    monkeypatch.setattr(preflight, "incomplete_init", lambda cfg: None)
    monkeypatch.setattr(preflight, "schedule_view", lambda cfg: {})
    monkeypatch.setattr(
        "cnequity.storage.lake_version.lake_version", lambda cfg: (7, None, None, None, "d", 1)
    )
    preflight._settled.clear()
    preflight.occupancy(config, storage_busy=False)
    preflight.occupancy(config, storage_busy=False)
    assert calls == ["empty"]


def test_the_lake_version_never_opens_the_catalog_duckdb_is_reading(config, monkeypatch):
    # A second SQLite library closing the catalog truncates the WAL and
    # shared memory DuckDB's SQLite has mapped: the serve process died of SIGBUS.
    from cnequity.storage import lake_version as module
    from cnequity.storage.catalog_bridge import lake_root
    from equitylake_core.catalog import CATALOG_FILE

    catalog = lake_root(config) / CATALOG_FILE
    assert module.lake_version(config)[0] is None
    catalog.parent.mkdir(parents=True, exist_ok=True)
    catalog.write_bytes(b"x")
    opened = []
    real = module.connect_readonly
    monkeypatch.setattr(
        module, "connect_readonly", lambda path, **kw: opened.append(path) or real(path, **kw)
    )
    before = module.lake_version(config)
    assert before[0] is not None
    Path(f"{catalog}-wal").write_bytes(b"frame")
    assert module.lake_version(config)[0] != before[0]
    assert all(Path(p).name != CATALOG_FILE for p in opened)


def test_the_page_inventory_does_not_measure_generation_trees(config, monkeypatch):
    from cnequity.serve.storage import StorageMaintenance

    seen = {}

    def inspect(*, keep, measure=True):
        seen["measure"] = measure
        return {"objects": []}

    maintenance = StorageMaintenance(config)
    monkeypatch.setattr(maintenance.store, "inspect", inspect)
    monkeypatch.setattr("cnequity.serve.storage.StagingRetirement.inspect", lambda self: [])
    maintenance._summary()
    assert seen == {"measure": False}


def test_a_restarted_server_shows_its_last_inventory_while_it_rebuilds(config, monkeypatch):
    from cnequity.serve.storage import StorageMaintenance

    first = StorageMaintenance(config, persist=True)
    built = {"objects": [{"object_id": "revision/a/1"}]}
    monkeypatch.setattr(first, "_summary", lambda: dict(built))
    assert first.summary()["objects"] == built["objects"]

    later = StorageMaintenance(config, persist=True)
    rebuilds = []
    monkeypatch.setattr(later, "_rebuild_later", lambda key: rebuilds.append(key))
    answer = later.summary()
    assert answer["objects"] == built["objects"]
    assert answer["refreshing"], "a copy from disk is never taken as current"
    assert len(rebuilds) == 1

    # Only a serving process keeps one.
    assert StorageMaintenance(config)._kept is None


# --- one snapshot per summary -------------------------------------------


def test_a_pinned_block_asks_a_read_only_catalog_for_its_snapshot_once(tmp_path):
    from equitylake_core.catalog import Catalog, pinned_snapshots

    Catalog(tmp_path / "lake", create=True).close()
    catalog = Catalog(tmp_path / "lake", read_only=True)
    real = catalog.con
    asked = []

    class Counting:
        def execute(self, *args):
            asked.append(args[0])
            return real.execute(*args)

        def __getattr__(self, name):
            return getattr(real, name)

    catalog.con = Counting()
    try:
        with pinned_snapshots():
            first = catalog.snapshot()
            assert catalog.snapshot() == first
        assert len(asked) == 1
        catalog.snapshot()
        assert len(asked) == 2, "outside the block every call asks again"
    finally:
        catalog.con = real
        catalog.close()


# --- keyed sources ------------------------------------------------------


def _keyed_config(tmp_path: Path, extra: str = "") -> Path:
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f"""[data]
root = "{path_for_toml(tmp_path / "data")}"

[sources.tushare]
enabled = true
{extra}
""",
        encoding="utf-8",
    )
    return path


def test_a_key_is_stored_in_its_own_file_and_the_config_names_only_the_file(tmp_path, monkeypatch):
    from cnequity.ops.keys import remove_key, save_key

    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    path = _keyed_config(tmp_path)
    secret = _FAKE_KEY
    where = save_key(path, "tushare", secret)
    key_file = Path(where["key_file"])
    assert key_file.read_text().strip() == secret
    if os.name != "nt":  # Windows has no owner-only file mode
        assert stat.S_IMODE(key_file.stat().st_mode) == 0o600
    text = path.read_text()
    assert secret not in text
    assert "token_file" in text
    config = load_config(path)
    assert config.tushare_token == secret
    assert config.credential_origins["tushare"] == "file"

    remove_key(path, "tushare")
    assert not key_file.exists()
    assert "token_file" not in path.read_text()
    assert load_config(path).tushare_token is None


def test_a_malformed_key_is_refused_before_anything_is_written(tmp_path):
    from cnequity.ops.catalog import OpsError
    from cnequity.ops.keys import save_key

    path = _keyed_config(tmp_path)
    before = path.read_text()
    with pytest.raises(OpsError):
        save_key(path, "tushare", "has space inside")
    assert path.read_text() == before


def test_keys_are_entered_only_on_this_machine(tmp_path, monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    path = _keyed_config(tmp_path)
    app = create_app(load_config(path), token="t", allow_remote_ops=True)
    remote = TestClient(app, base_url="http://192.168.1.5", client=("192.168.1.9", 5000))
    home = remote.get("/api/ops?token=t")
    headers = {"Origin": "http://192.168.1.5", "X-CNE-CSRF": home.json()["csrf_token"]}
    answer = remote.put(
        "/api/ops/sources/tushare/key?token=t", json={"key": _FAKE_KEY}, headers=headers
    )
    assert answer.status_code == 403
    assert "token_file" not in path.read_text()


def test_the_key_route_saves_then_checks(tmp_path, monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.setattr(
        "cnequity.ops.keys.check_key",
        lambda cfg, source: {"state": "ok", "message": "", "checked_at": "2026-11-23T00:00:00"},
    )
    path = _keyed_config(tmp_path)
    client = _client(load_config(path))
    home = client.get("/api/ops").json()
    headers = {"Origin": "http://127.0.0.1", "X-CNE-CSRF": home["csrf_token"]}
    answer = client.put("/api/ops/sources/tushare/key", json={"key": _FAKE_KEY}, headers=headers)
    assert answer.status_code == 200, answer.text
    body = answer.json()
    assert body["check"]["state"] == "ok"
    assert _FAKE_KEY not in answer.text
    status = client.get("/api/ops/status").json()["occupancy"]["sources"]
    assert status["keys"]["tushare"] is True
    assert status["key_origins"]["tushare"] == "file"
    assert status["key_hints"]["tushare"] == "…" + _FAKE_KEY[-4:]


# --- Tushare in the daily update ----------------------------------------


def test_tushare_joins_the_daily_run_only_when_asked_and_while_its_key_works(tmp_path, monkeypatch):
    from cnequity.config.keyed_sources import TUSHARE_DAILY_GROUP
    from cnequity.orchestrator.cadence import group_is_runnable
    from cnequity.quality.freshness import freshness_owed
    from cnequity.storage.key_checks import save_check

    monkeypatch.setenv("TUSHARE_TOKEN", _FAKE_KEY)
    assert TUSHARE_DAILY_GROUP not in load_config(_keyed_config(tmp_path)).schedule_groups

    config = load_config(_keyed_config(tmp_path, "daily = true"))
    group = config.schedule_groups[TUSHARE_DAILY_GROUP]
    assert "fund_flow_tushare" in group.steps
    # Asked for, but the key was never confirmed: not run, not owed.
    assert not group_is_runnable(config, group)
    assert not freshness_owed(config, "fund_flow_tushare")

    save_check(config, "tushare", "ok", "", "2026-11-23T00:00:00+00:00")
    assert group_is_runnable(config, group)
    assert freshness_owed(config, "fund_flow_tushare")

    save_check(config, "tushare", "rejected", "expired", "2026-11-23T01:00:00+00:00")
    assert not group_is_runnable(config, group)
    assert not freshness_owed(config, "fund_flow_tushare")
