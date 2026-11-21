import sys
from datetime import date, datetime, timezone

import duckdb
import httpx
import pytest

from cnequity.config import Config, load_config
from cnequity.config.errors import ConfigError
from cnequity.config.resources import resources_for
from cnequity.domain.http_policy import record_http_response, record_request_event
from cnequity.domain.request_meter import request_meter_scope
from cnequity.ops import scheduled
from cnequity.orchestrator.manifest import Manifest
from cnequity.orchestrator.run_window import mark_done, pending_full_audit


def test_duckdb_workloads_keep_independent_limits(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        '[data]\nroot = "lake"\n[duckdb]\nmemory_limit = "1GB"\nthreads = 2\n'
        '[duckdb.write]\nmemory_limit = "3GB"\nthreads = 1\n'
        'temp_directory = "spill"\nmax_temp_directory_size = "2GB"\n'
        "[query]\nscan_budget_rows = 5000\n"
    )
    config = load_config(path)
    assert config.query_scan_budget_rows == 5000
    assert resources_for(config, "read").threads == 2
    write = resources_for(config, "write")
    assert write.temp_directory == tmp_path / "spill"
    assert resources_for(config, "audit").memory_limit == "512MB"
    with duckdb.connect() as con:
        write.apply(con)
        assert con.execute("SELECT current_setting('threads')").fetchone() == (1,)
        assert con.execute("SELECT current_setting('temp_directory')").fetchone() == (
            str(write.temp_directory),
        )


@pytest.mark.parametrize(
    "body, message",
    [
        ("[duckdb]\nthreads = 0\n", "DuckDB read threads"),
        ("[duckdb.write]\nmemory_limit = 4\n", "DuckDB write memory_limit"),
        ("[storage.retention]\nkeep_generations = -1\n", "keep_generations"),
    ],
)
def test_resource_and_retention_errors_reach_the_cli(tmp_path, body, message):
    path = tmp_path / "config.toml"
    path.write_text('[data]\nroot = "lake"\n' + body)
    with pytest.raises(ConfigError, match=message):
        load_config(path)


def test_run_receipt_uses_only_selected_config_values(tmp_path):
    cfg = Config(data_root=tmp_path / "lake", workers=2)
    cfg.tushare_token = "fixture-private-tushare"
    cfg.eastmoney_proxy = "http://user:fixture-private-proxy@localhost"
    first = Manifest(cfg.manifest_path, config=cfg)
    run = first.start_run("daily:core", {"trade_date": "2026-11-20"})
    cfg.workers = 3
    retry = first.start_run("daily:core")
    old = first.get_run_metadata(run)["effective_config"]
    new = first.get_run_metadata(retry)["effective_config"]
    assert old["settings"]["workers"] == 2
    assert new["settings"]["workers"] == 3
    assert old["digest"] != new["digest"]
    assert "fixture-private" not in str(old)


def test_route_meter_records_the_fallback_and_the_provider_that_failed():
    with request_meter_scope() as meter:
        record_http_response(None, "eastmoney_push2", httpx.Response(200), meter=False)
        record_request_event(None, "eastmoney_dc", "fallback")
        record_http_response(None, "eastmoney_dc", httpx.Response(503), meter=False)
    assert meter.routes() == {
        "eastmoney_dc": ["attempted", "failed", "fallback"],
        "eastmoney_push2": ["attempted"],
    }


def test_timer_offers_weekly_audit_after_friday_maintenance(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    friday, saturday = date(2026, 11, 20), date(2026, 11, 21)
    for job in ("daily", "maintain"):
        mark_done(cfg.meta_root, job, friday)
    monkeypatch.setattr(scheduled, "settings", lambda config: {"daily": True})

    class Service:
        def __init__(self, *args, **kwargs):
            pass

        def preview(self, operation, params):
            assert operation == "daily.maintain"
            assert params == (
                {"full_audit_only": True, "notify": True}
                if sys.platform == "darwin"
                else {"full_audit_only": True}
            )
            return {"preview_id": "audit", "launch_token": "token", "acknowledgements": []}

        def start(self, *args, **kwargs):
            assert kwargs["scheduled"] == {"job": "full-audit", "session": str(saturday)}
            return {"job_id": "job"}

    monkeypatch.setattr(scheduled, "OpsService", Service)
    now = datetime(2026, 11, 21, 4, tzinfo=timezone.utc)
    assert scheduled.tick(cfg, now=now)["jobs"][0]["job"] == "full-audit"
    assert pending_full_audit(cfg, saturday) == saturday
    mark_done(cfg.meta_root, "full-audit", saturday)
    assert scheduled.tick(cfg, now=now)["status"] == "idle"
