"""配置改动后，HTTP 页面、缓存和存储维护必须指向同一个数据湖。"""

from pathlib import Path

import polars as pl
import pytest
from fastapi.testclient import TestClient

from cnequity.config import Config, load_config
from cnequity.file_lock import exclusive_lock
from cnequity.serve.app import create_app
from cnequity.serve.ops.catalog import OpsError
from cnequity.serve.ops.records import slot_path
from cnequity.storage.stats import rebuild_stats


def write_config(path, root):
    path.write_text(f"[data]\nroot = '{root.as_posix()}'\n", encoding="utf-8")


def setup_lakes(tmp_path, **kwargs):
    path = tmp_path / "配置.toml"
    first, second = tmp_path / "原数据湖", tmp_path / "新数据湖"
    write_config(path, first)
    app = create_app(load_config(path), **kwargs)
    return path, first, second, app, TestClient(app, base_url="http://127.0.0.1")


def test_external_config_edit_rebinds_cache_maintenance_and_invalidates_previews(tmp_path):
    path, first, second, app, client = setup_lakes(tmp_path)
    assert client.get("/api/ops").json()["mode"]["data_root"] == str(first)
    original_view = app.state.view
    original_maintenance = app.state.storage_maintenance
    preview = app.state.ops.preview("diag.doctor", {})
    write_config(path, second)
    # 读取接口也能同步，不要求先打开操作页。
    response = client.get("/api/datasets")
    assert response.status_code == 200, response.text
    assert app.state.view is not original_view
    assert app.state.storage_maintenance is not original_maintenance
    assert app.state.view.config.data_root == second
    assert Path(app.state.storage_maintenance.store.meta) == second / "meta"
    assert client.get("/api/ops").json()["mode"]["data_root"] == str(second)
    assert load_config(path).data_root == second
    assert preview["preview_id"] is not None
    with pytest.raises(OpsError, match="失效"):
        app.state.ops._take(preview["preview_id"], preview["launch_token"], [])
    assert not list(first.rglob("*.parquet"))  # 重载不启动取数。


@pytest.mark.parametrize("busy", ["maintenance", "readers", "job", "request"])
def test_directory_switch_waits_for_existing_work(tmp_path, busy):
    path, first, second, app, client = setup_lakes(tmp_path)
    maintenance = app.state.storage_maintenance
    write_config(path, second)

    def assert_waiting():
        response = client.get("/api/datasets")
        assert response.status_code == 409
        assert "数据目录已修改" in response.json()["detail"]
        assert app.state.view.config.data_root == first

    if busy == "maintenance":
        maintenance.busy = True
        assert_waiting()
        maintenance.busy = False
    elif busy == "readers":
        maintenance.gate.enter()
        assert_waiting()
        maintenance.gate.leave()
    elif busy == "request":
        app.state.config_runtime.active_requests = 1
        assert_waiting()
        app.state.config_runtime.active_requests = 0
    else:
        with exclusive_lock(slot_path(app.state.config), blocking=False):
            assert_waiting()
            # 用户仍能查看运行记录，不能因重载等待而无法停止任务。
            assert client.get("/api/ops/jobs").status_code == 200
    assert client.get("/api/datasets").status_code == 200
    assert app.state.view.config.data_root == second


def test_bad_config_has_clear_error_and_recovers_without_serving_old_data(tmp_path):
    path, _, second, app, client = setup_lakes(tmp_path)
    original = app.state.view
    path.write_text("[data\ninvalid", encoding="utf-8")
    response = client.get("/api/datasets")
    assert response.status_code == 409
    assert "配置文件无法读取或格式无效" in response.json()["detail"]
    assert app.state.view is original
    write_config(path, second)
    assert client.get("/api/datasets").status_code == 200
    assert app.state.view.config.data_root == second


def test_authentication_precedes_config_reload(tmp_path):
    path, _, _, _, client = setup_lakes(tmp_path, token="test-secret")
    path.write_text("[invalid", encoding="utf-8")
    assert client.get("/api/datasets").status_code == 401


def test_cached_dataset_counts_follow_the_new_lake(tmp_path):
    path, first, second, _, client = setup_lakes(tmp_path)
    for root, count in [(first, 1), (second, 2)]:
        target = root / "curated" / "instruments"
        target.mkdir(parents=True)
        pl.DataFrame({"symbol": ["600371.SH"] * count, "name": ["万向德农"] * count}).write_parquet(
            target / "part.parquet"
        )
        rebuild_stats(Config(data_root=root), datasets=["instruments"])

    def count():
        response = client.get("/api/datasets")
        assert response.status_code == 200, response.text
        return next(row["row_count"] for row in response.json() if row["dataset"] == "instruments")

    assert count() == 1
    write_config(path, second)
    assert count() == 2
