"""Request-boundary and CLI regressions; all data and traffic are isolated."""

import json
from contextlib import contextmanager
from datetime import date, datetime, timezone
from email.utils import format_datetime

import httpx
import pytest
from click.testing import CliRunner

from cnequity.cli.main import cli
from cnequity.config import Config, load_config, validate_config
from cnequity.domain.http_policy import (
    SourceCoolingDown,
    record_http_response,
    record_request_event,
    retry_after_seconds,
)
from cnequity.domain.rate_limit import RateLimiter


def test_queued_wait_rechecks_new_cooldown(tmp_path, monkeypatch):
    now = [100.0]
    limiter = RateLimiter("source", 1.0, tmp_path)
    monkeypatch.setattr("cnequity.domain.rate_limit.time.time", lambda: now[0])
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        if len(sleeps) == 1:
            limiter.defer(30)
        now[0] += seconds

    monkeypatch.setattr("cnequity.domain.rate_limit.time.sleep", sleep)
    limiter.wait()
    limiter.wait()
    assert sleeps == [1.0, 29.0]
    assert now[0] == 130.0


def test_zero_interval_still_honors_explicit_cooldown(tmp_path, monkeypatch):
    now = [100.0]
    monkeypatch.setattr("cnequity.domain.rate_limit.time.time", lambda: now[0])
    monkeypatch.setattr(
        "cnequity.domain.rate_limit.time.sleep",
        lambda seconds: now.__setitem__(0, now[0] + seconds),
    )
    limiter = RateLimiter("source", 0, tmp_path)
    limiter.defer(30)
    limiter.wait()
    assert now[0] == 130


def test_pacing_happens_after_capacity_is_acquired(tmp_path, monkeypatch):
    from cnequity.adapters.throttle import SourceRateLimiters

    cfg = Config(data_root=tmp_path)
    limiters = SourceRateLimiters(cfg)
    events = []

    @contextmanager
    def slot(*args, **kwargs):
        events.append("capacity")
        yield

    monkeypatch.setattr(limiters, "slot", slot)
    monkeypatch.setattr(limiters, "wait", lambda source: events.append("paced"))
    with limiters.request("sina"):
        events.append("request")
    assert events == ["capacity", "paced", "request"]


def test_tdx_and_http_use_same_explicit_egress_root(tmp_path, monkeypatch):
    shared = tmp_path / "egress"
    monkeypatch.setenv("CNE_RATE_LIMIT_ROOT", str(shared))
    cfg = Config(data_root=tmp_path / "lake", source_intervals={"sina": 0.001})
    spec = cfg.tdx_rate_limit_spec()
    assert spec.state_dir == spec.concurrency_state_dir == str(shared)
    with cfg.source_request("sina"):
        pass
    assert (shared / "sina.json").exists()
    assert not cfg.data_root.exists()


def test_response_meter_aggregates_body_and_keeps_query_out_of_state(tmp_path):
    from cnequity.diagnostics.source_limits import build_source_limits

    cfg = Config(data_root=tmp_path / "lake")
    for status, body in ((200, b"ok"), (503, b"retry")):
        response = httpx.Response(
            status,
            content=body,
            request=httpx.Request("GET", "https://example.org/api/report?token=private-marker"),
        )
        record_http_response(cfg, "sina_bars", response)
    path = cfg.rate_limit_root / "wire-sina.json"
    text = path.read_text()
    assert "private-marker" not in text
    assert "/api/report" not in text
    report = build_source_limits(cfg)["sources"]["sina"]["wire_responses_today"]
    assert report["responses"] == 2
    assert report["body_bytes"] == 7
    assert report["statuses"] == {"200": 1, "503": 1}
    assert len(report["endpoints"]) == 1


def test_extra_request_events_are_scoped_and_reported(tmp_path):
    from cnequity.diagnostics.source_limits import build_source_limits

    cfg = Config(data_root=tmp_path / "lake")
    record_request_event(cfg, "eastmoney_push2", "fallback")
    record_request_event(cfg, "eastmoney_push2", "retry")
    assert build_source_limits(cfg)["sources"]["eastmoney_push2"]["request_events_today"] == {
        "retry": 1,
        "fallback": 1,
    }
    with pytest.raises(ValueError, match="unknown request event"):
        record_request_event(cfg, "eastmoney_push2", "ordinary")


def test_optional_response_meter_failure_does_not_hide_refusal(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")

    def fail_meter(*_args, **_kwargs):
        raise OSError("meter unavailable")

    monkeypatch.setattr("cnequity.domain.http_policy._record_wire_response", fail_meter)
    record_http_response(cfg, "sina", httpx.Response(429))
    with pytest.raises(SourceCoolingDown):
        with cfg.source_request("sina"):
            pytest.fail("refusal policy must still be enforced")


@pytest.mark.parametrize("status", [403, 412, 429, 456])
def test_refusal_blocks_new_config_and_alias_without_network(tmp_path, status):
    cfg = Config(data_root=tmp_path)
    response = httpx.Response(status, headers={"Retry-After": "900"})
    with cfg.source_request("sina_bars"):
        record_http_response(cfg, "sina_bars", response)
    fresh = Config(data_root=tmp_path)
    with pytest.raises(SourceCoolingDown):
        with fresh.source_request("sina"):
            pytest.fail("must not reach a second network call")
    assert json.loads((cfg.rate_limit_root / "circuit-sina.json").read_text())["status"] == status


def test_nested_same_family_request_rechecks_refusal(tmp_path):
    cfg = Config(data_root=tmp_path, source_intervals={"sina": 0})
    with cfg.source_request("sina"):
        record_http_response(cfg, "sina", httpx.Response(429))
        with pytest.raises(SourceCoolingDown):
            with cfg.source_request("sina_bars"):
                pytest.fail("nested call must not bypass the new cooldown")


@pytest.mark.parametrize(
    ("status", "headers"),
    [(401, {}), (403, {"WWW-Authenticate": 'Bearer realm="api"'})],
)
def test_auth_refusal_does_not_block_other_credentials(tmp_path, status, headers):
    cfg = Config(data_root=tmp_path)
    record_http_response(cfg, "ths_official", httpx.Response(status, headers=headers))
    assert not (cfg.rate_limit_root / "circuit-ths_official.json").exists()
    with cfg.source_request("ths_official"):
        pass


def test_public_ths_401_cools_other_public_ths_lanes(tmp_path, monkeypatch):
    from cnequity.adapters.ths import boards, corporate_actions, fund_flow

    cfg = Config(data_root=tmp_path, source_intervals={"ths": 0, "ths_pages": 0})
    calls = []
    monkeypatch.setattr(
        boards.httpx,
        "get",
        lambda *args, **kwargs: calls.append(1) or httpx.Response(401),
    )
    with pytest.raises(boards.ThsError, match="401"):
        boards._get("https://q.10jqka.com.cn/thshy/", config=cfg)
    assert len(calls) == 1
    with pytest.raises(SourceCoolingDown):
        boards._get("https://d.10jqka.com.cn/v6/line/bk_881121/01/last.js", config=cfg)
    with pytest.raises(SourceCoolingDown):
        corporate_actions._fetch_page("920001", config=cfg)
    monkeypatch.setattr(fund_flow, "hexin_v", lambda _cfg: "token")
    with httpx.Client(transport=httpx.MockTransport(lambda _req: pytest.fail("network"))) as client:
        with pytest.raises(SourceCoolingDown):
            fund_flow._get_page(client, "stock", 1, cfg)
    assert len(calls) == 1


def test_html_challenge_on_json_endpoint_starts_shared_cooldown(tmp_path):
    cfg = Config(data_root=tmp_path)
    response = httpx.Response(200, text="<html>captcha challenge</html>")
    record_http_response(cfg, "eastmoney_dc", response, expected_json=True)
    with pytest.raises(SourceCoolingDown):
        with cfg.source_request("eastmoney_dc"):
            pytest.fail("challenge must stop the next request")


def test_only_one_recovery_probe_runs_and_success_reopens_source(tmp_path, monkeypatch):
    from cnequity.domain.http_policy import cooldown_status

    now = [100.0]
    monkeypatch.setattr("cnequity.domain.http_policy.time.time", lambda: now[0])
    cfg = Config(data_root=tmp_path, source_intervals={"sina": 0})
    record_http_response(cfg, "sina", httpx.Response(429))
    now[0] = 401
    with cfg.source_request("sina"):
        # A slow but live probe must retain its sole slot beyond ten minutes.
        now[0] = 1_101
        with pytest.raises(SourceCoolingDown, match="recovery probe"):
            with Config(data_root=tmp_path, source_intervals={"sina": 0}).source_request("sina"):
                pytest.fail("parallel probe must not leave process")
    assert cooldown_status(cfg.rate_limit_root, "sina")["probe_required"] is False
    with cfg.source_request("sina"):
        pass


def test_failed_recovery_probe_restarts_cooldown(tmp_path, monkeypatch):
    from cnequity.domain.http_policy import cooldown_status

    now = [100.0]
    monkeypatch.setattr("cnequity.domain.http_policy.time.time", lambda: now[0])
    cfg = Config(data_root=tmp_path, source_intervals={"sina": 0})
    record_http_response(cfg, "sina", httpx.Response(429))
    now[0] = 401
    with pytest.raises(httpx.ConnectError):
        with cfg.source_request("sina"):
            raise httpx.ConnectError("isolated probe failed")
    state = cooldown_status(cfg.rate_limit_root, "sina")
    assert state["cooldown_until"] == 701
    assert state["probe_required"] is True


def test_dead_recovery_probe_owner_does_not_block_next_attempt(tmp_path, monkeypatch):
    from cnequity.domain.http_policy import cooldown_status

    now = [100.0]
    monkeypatch.setattr("cnequity.domain.http_policy.time.time", lambda: now[0])
    cfg = Config(data_root=tmp_path, source_intervals={"sina": 0})
    record_http_response(cfg, "sina", httpx.Response(429))
    path = cfg.rate_limit_root / "circuit-sina.json"
    state = json.loads(path.read_text())
    state.update(probe_token="abandoned", probe_started_at=400, probe_pid=99999999, probe_thread=1)
    path.write_text(json.dumps(state))
    now[0] = 401
    assert cooldown_status(cfg.rate_limit_root, "sina")["probe_inflight"] is False
    with cfg.source_request("sina"):
        pass
    assert cooldown_status(cfg.rate_limit_root, "sina")["probe_required"] is False


def test_recovery_probe_auth_error_does_not_recool_shared_egress(tmp_path, monkeypatch):
    from cnequity.domain.http_policy import cooldown_status

    now = [100.0]
    monkeypatch.setattr("cnequity.domain.http_policy.time.time", lambda: now[0])
    cfg = Config(data_root=tmp_path, source_intervals={"ths_official": 0})
    record_http_response(cfg, "ths_official", httpx.Response(429))
    now[0] = 401
    response = httpx.Response(401, request=httpx.Request("GET", "https://example.test/data"))
    with pytest.raises(httpx.HTTPStatusError):
        with cfg.source_request("ths_official"):
            record_http_response(cfg, "ths_official", response)
            response.raise_for_status()
    state = cooldown_status(cfg.rate_limit_root, "ths_official")
    assert state["kind"] == "credentials"
    assert state["cooldown_until"] is None
    assert state["probe_required"] is False
    with cfg.source_request("ths_official"):
        pass


def test_http_guard_expires_and_does_not_block_unrelated_source(tmp_path, monkeypatch):
    now = [100.0]
    monkeypatch.setattr("cnequity.domain.http_policy.time.time", lambda: now[0])
    cfg = Config(data_root=tmp_path, source_intervals={"sina": 0})
    record_http_response(cfg, "sina", httpx.Response(429))
    with cfg.source_request("ths_official"):
        pass
    now[0] = 401
    with cfg.source_request("sina"):
        pass


def test_ths_keyed_service_does_not_share_public_website_refusals(tmp_path):
    cfg = Config(data_root=tmp_path, source_concurrency={"ths": 1, "ths_official": 4})
    record_http_response(cfg, "ths_pages", httpx.Response(403))
    with cfg.source_request("ths_official"):
        state = json.loads((cfg.rate_limit_root / "concurrency-ths_official.json").read_text())
        assert state["limit"] == 4
    with pytest.raises(SourceCoolingDown):
        with cfg.source_request("ths_bonus"):
            pytest.fail("public website aliases must share the refusal")


def test_retry_after_http_date_and_invalid_values():
    value = format_datetime(datetime.fromtimestamp(1000, timezone.utc), usegmt=True)
    assert retry_after_seconds(value, now=400) == 600
    for value in ("invalid", "nan", "inf", "-1", ""):
        assert retry_after_seconds(value, now=400) == 0


@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), True, "fast"])
def test_invalid_intervals_fail_before_creating_any_state(tmp_path, value):
    cfg = Config(data_root=tmp_path / "lake", source_intervals={"sina": value})
    assert any("sources.sina.min_interval_seconds" in message for message in validate_config(cfg))
    with pytest.raises(ValueError, match="finite number"):
        with cfg.source_request("sina"):
            pytest.fail("invalid policy allowed a request")
    assert not cfg.data_root.exists()


@pytest.mark.parametrize("raw", ["nan", "inf", "-1", "true", '"fast"'])
def test_toml_interval_validation_preserves_invalid_types(tmp_path, raw):
    path = tmp_path / "config.toml"
    path.write_text(f"[sources.sina]\nmin_interval_seconds = {raw}\n")
    cfg = load_config(path)
    assert any("sources.sina.min_interval_seconds" in message for message in validate_config(cfg))


@pytest.mark.parametrize("scope", ["", " , , "])
def test_empty_symbol_scope_cannot_start_backfill(scope):
    result = CliRunner().invoke(cli, ["backfill", "daily_bars", "--symbols", scope])
    assert result.exit_code == 2
    assert "--symbols" in result.output


@pytest.mark.usefixtures("chinese_output")
def test_clean_dry_run_cannot_reconcile_run_state(tmp_path):
    path = tmp_path / "missing-config.toml"
    result = CliRunner().invoke(
        cli, ["run", "clean", "--dry-run", "--reconcile-runs", "--config", str(path)]
    )
    assert result.exit_code == 2
    assert "对账会修改运行状态" in result.output
    assert list(tmp_path.iterdir()) == []


def test_generic_backfill_plan_is_offline_and_does_not_create_lake(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake", sources={"tdx_protocol": False})
    monkeypatch.setattr("cnequity.cli.backfill_cmds._cfg", lambda path: cfg)
    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            "daily_bars",
            "--symbols",
            "000001.SZ,000001.SZ",
            "--start",
            "2018-01-01",
            "--end",
            "2018-01-31",
            "--plan",
        ],
    )
    assert result.exit_code == 0, result.output
    plan = json.loads(result.stdout)
    assert plan["symbols"] == ["000001.SZ"]
    assert plan["data_root"] == str(cfg.data_root)
    assert (plan["start"], plan["end"]) == ("2018-01-01", "2018-01-31")
    assert plan["registered_sources"]["primary"] == "tdx_protocol"
    assert plan["source_status"]["tdx_protocol"]["enabled"] is False
    assert set(plan["source_status"]["tdx_protocol"]["pacing_seconds"]) == {"tdx_protocol"}
    assert plan["cold_request_lower_bound"] == 1
    assert plan["broad_tip_snapshots"]["exchange"] == "skip"
    assert plan["tdx_history_window"]["strategy"] == "exponential-bracket-binary-seek-then-scan"
    # What is owed is the planner's to say once the run starts; a plan only names the watermark.
    assert plan["checkpoint"] == {"watermark": None}
    assert plan["writes"] is False
    assert not cfg.data_root.exists()


def test_source_limits_reports_shared_budget_without_creating_a_lake(tmp_path, monkeypatch):
    shared = tmp_path / "egress"
    monkeypatch.setenv("CNE_RATE_LIMIT_ROOT", str(shared))
    cfg = Config(data_root=tmp_path / "lake", eastmoney_push2_daily_budget=4)
    monkeypatch.setattr("cnequity.cli.quality_cmds._cfg", lambda _: cfg)
    report = CliRunner().invoke(cli, ["sources", "limits"])
    assert report.exit_code == 0, report.output
    payload = json.loads(report.output)
    assert payload["eastmoney"]["push2"]["budget_remaining"] == 4
    assert payload["sources"]["tdx_protocol"]["enabled"] is True
    assert payload["latest_run_metrics"] is None
    assert not cfg.data_root.exists()
    assert not shared.exists()


@pytest.mark.parametrize(
    "metadata_json", ["[]", "null", '{"metrics": []}', '{"metrics": {"source_metrics": []}}']
)
def test_source_limits_tolerates_legacy_non_object_metrics(tmp_path, monkeypatch, metadata_json):
    import sqlite3

    from cnequity.orchestrator.manifest import Manifest

    cfg = Config(data_root=tmp_path / "lake")
    run_id = Manifest(cfg.manifest_path).start_run("fixture")
    with sqlite3.connect(cfg.manifest_path) as conn:
        conn.execute(
            "UPDATE ingestion_runs SET metadata_json = ? WHERE run_id = ?",
            (metadata_json, run_id),
        )
    monkeypatch.setattr("cnequity.cli.quality_cmds._cfg", lambda _: cfg)

    result = CliRunner().invoke(cli, ["sources", "limits"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["latest_run_metrics"] == {
        "run_id": run_id,
        "requests": None,
        "request_retries": None,
        "source_metrics": {},
    }


def test_tdx_limits_use_daily_wire_width_when_global_workers_is_one(tmp_path):
    from cnequity.diagnostics.source_limits import effective_source_policy

    cfg = Config(data_root=tmp_path / "lake", workers=1, tdx_daily_workers=4)
    policy = effective_source_policy(cfg, "tdx_protocol")
    assert policy["configured_max_concurrency"] == 4

    with cfg.source_request("tdx_protocol"):
        state = json.loads((cfg.rate_limit_root / "concurrency-tdx_protocol.json").read_text())
        assert state["limit"] == 4


def test_source_limits_reports_effective_same_day_shared_policy(tmp_path, monkeypatch):
    import time

    from cnequity.domain.rate_limit import _policy_day

    shared = tmp_path / "egress"
    shared.mkdir()
    day = _policy_day(time.time())
    (shared / "concurrency-sina.json").write_text(
        json.dumps({"policy_day": day, "limit": 2, "leases": []})
    )
    (shared / "sina.json").write_text(json.dumps({"policy_day": day, "min_interval": 1.2}))
    monkeypatch.setenv("CNE_RATE_LIMIT_ROOT", str(shared))
    cfg = Config(
        data_root=tmp_path / "lake",
        source_intervals={"sina": 0.1},
        source_concurrency={"sina": 8},
    )
    monkeypatch.setattr("cnequity.cli.quality_cmds._cfg", lambda _: cfg)
    report = CliRunner().invoke(cli, ["sources", "limits"])
    assert report.exit_code == 0, report.output
    policy = json.loads(report.output)["sources"]["sina"]
    assert policy["configured_max_concurrency"] == 8
    assert policy["effective_max_concurrency"] == 2
    assert policy["pacing_seconds"]["sina"]["effective_seconds"] == 1.2
    assert not cfg.data_root.exists()


def test_source_limits_reads_recorded_run_and_repair_keys(tmp_path, monkeypatch):
    from cnequity.orchestrator.manifest import Manifest

    cfg = Config(data_root=tmp_path / "lake")
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("fixture")
    manifest.record_stage_metrics(run_id, "fixture", 0.01, {"requests": 2, "request_retries": 1})
    # The planner owes one daily_bars key (a fetch left it unanswered).
    monkeypatch.setattr(
        "cnequity.quality.planned_coverage.gap_keys",
        lambda config, dataset, day: (
            [("000001.SZ", date(2026, 9, 1))] if dataset == "daily_bars" else []
        ),
    )
    monkeypatch.setattr("cnequity.cli.quality_cmds._cfg", lambda _: cfg)
    report = CliRunner().invoke(cli, ["sources", "limits"])
    assert report.exit_code == 0, report.output
    payload = json.loads(report.output)
    assert payload["latest_run_metrics"]["run_id"] == run_id
    assert payload["latest_run_metrics"]["requests"] == 2
    assert payload["latest_run_metrics"]["request_retries"] == 1
    assert payload["outstanding"]["daily_bars"]["repair_command"] == (
        "cne backfill daily_bars --outstanding"
    )


def test_probe_listing_needs_no_config():
    result = CliRunner().invoke(cli, ["sources", "probe", "--list", "--config", "/missing.toml"])
    assert result.exit_code == 0
    assert "tdx_protocol" in result.output


@pytest.mark.parametrize("keys", [["misspelled"], ["sina", "misspelled"]])
def test_unknown_probe_names_fail_before_any_probe(tmp_path, monkeypatch, keys):
    from cnequity.diagnostics import source_health

    monkeypatch.setattr(source_health, "run_probe", lambda *args: pytest.fail("network probe"))
    with pytest.raises(ValueError, match="Unknown probe sources"):
        source_health.run_probes(Config(data_root=tmp_path), vantage="local", only=keys)


def test_invalid_vantage_cannot_write_outside_report_directory(tmp_path):
    from cnequity.diagnostics.source_health import run_probes

    with pytest.raises(ValueError, match="vantage"):
        run_probes(Config(data_root=tmp_path), vantage="../outside", only=[])


def test_tdx_probe_respects_tdx_enabled(tmp_path, monkeypatch):
    from cnequity.diagnostics import source_health

    cfg = Config(data_root=tmp_path, tdx_enabled=False)
    result = source_health.run_probe(source_health.PROBES_BY_KEY["tdx_protocol"], cfg)
    assert result.status == "skipped"


@pytest.mark.parametrize(
    ("disabled", "lane"),
    [("eastmoney", "eastmoney_dc"), ("ths", "ths_pages"), ("cni", "cni"), ("sw", "sw")],
)
def test_explicit_source_switch_is_enforced_at_request_boundary(tmp_path, disabled, lane):
    cfg = Config(data_root=tmp_path / "lake", sources={disabled: False})
    with pytest.raises(RuntimeError, match="source disabled"):
        with cfg.source_request(lane):
            pytest.fail("disabled source made a request")
    assert not cfg.data_root.exists()


def test_actual_cninfo_http_refusal_is_not_retried(tmp_path):
    from cnequity.adapters.cninfo.announcements import post_with_retry

    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(429, headers={"Retry-After": "600"})

    cfg = Config(data_root=tmp_path)
    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            post_with_retry(client, "https://example.test", data={}, config=cfg)
        with pytest.raises(SourceCoolingDown):
            post_with_retry(client, "https://example.test", data={}, config=cfg)
    assert len(calls) == 1


def test_interrupted_dump_keeps_existing_file_and_cleans_temporary(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from cnequity.adapters.ths_official.corporate_actions import download_adjustment_factor_dump

    target = tmp_path / "dump.parquet"
    target.write_bytes(b"previous complete dump")

    def chunks():
        yield b"partial new dump"
        raise httpx.ReadError("interrupted")

    @contextmanager
    def stream(*args, **kwargs):
        yield SimpleNamespace(status_code=200, iter_bytes=chunks)

    monkeypatch.setattr("httpx.stream", stream)
    client = SimpleNamespace(download_url=lambda kind: ("https://example.test/private-link", 300))
    with pytest.raises(httpx.ReadError):
        download_adjustment_factor_dump(client, target)
    assert target.read_bytes() == b"previous complete dump"
    assert list(tmp_path.iterdir()) == [target]
