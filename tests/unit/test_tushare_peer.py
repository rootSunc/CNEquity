"""Tushare peer checks: opt-in, warning-only, and judged on the right rows."""

import json
from datetime import date, datetime, timezone

import httpx
import polars as pl
import pytest

from cnequity.adapters.tushare.client import TushareClient, cached_permissions
from cnequity.config import Config
from cnequity.quality import tushare_peer
from cnequity.quality.tushare_peer import run_tushare_peer_checks

PRIOR, DAY = date(2026, 10, 30), date(2026, 11, 2)
STAMP = datetime(2026, 11, 2, tzinfo=timezone.utc)


def _cfg(tmp_path):
    return Config(
        data_root=tmp_path / "lake",
        sources={"tushare": True},
        tushare_token="tok",
        source_intervals={"tushare": 0},
    )


def _write(root, dataset, partition, frame):
    path = root / dataset / partition / "part.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.with_columns(
        pl.lit("test").alias("source"),
        pl.lit("v1").alias("data_version"),
        pl.lit(STAMP).alias("fetched_at"),
    ).write_parquet(path)


def _factors(cfg, steps):
    for day, factors in ((PRIOR, {s: 1.0 for s in steps}), (DAY, steps)):
        _write(
            cfg.derived_root,
            "adj_factors",
            f"trade_date={day}",
            pl.DataFrame(
                {
                    "trade_date": [day] * len(factors),
                    "factor": list(factors.values()),
                    "symbol": list(factors),
                    "adjust_type": ["hfq"] * len(factors),
                }
            ),
        )


def _bars(cfg, symbols):
    _write(
        cfg.curated_root,
        "daily_bars",
        f"trade_date={DAY}",
        pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [DAY] * len(symbols),
                "open": [10.0] * len(symbols),
                "high": [10.0] * len(symbols),
                "low": [10.0] * len(symbols),
                "close": [10.0] * len(symbols),
                "pre_close": [10.0] * len(symbols),
                "volume": [1000] * len(symbols),
                "amount": [10000.0] * len(symbols),
            }
        ),
    )


def _client(cfg, answers, denied=()):
    """A Tushare double answering each endpoint from *answers*."""
    calls = []

    def respond(request):
        body = json.loads(request.content)
        api = body["api_name"]
        calls.append((api, body["params"]))
        if api in denied:
            return httpx.Response(200, json={"code": 40203, "msg": "抱歉，您没有访问该接口的权限"})
        fields, rows = answers.get(api, (["ts_code"], []))
        if callable(rows):
            rows = rows(body["params"])
        if body["params"].get("limit") == 1:
            rows = rows[:1]
        return httpx.Response(200, json={"code": 0, "data": {"fields": fields, "items": rows}})

    http = httpx.Client(transport=httpx.MockTransport(respond))
    return TushareClient(cfg, client=http), calls


def _only(monkeypatch, *names):
    monkeypatch.setattr(tushare_peer, "CHECKS", {n: tushare_peer.CHECKS[n] for n in names})


def test_without_a_token_nothing_runs_and_nothing_is_sent(tmp_path):
    cfg = Config(data_root=tmp_path / "lake", sources={"tushare": True})
    assert run_tushare_peer_checks(cfg, DAY) == []
    assert not (cfg.meta_root / "quality" / "source_diffs").exists()


def test_a_factor_step_tushare_does_not_have_is_a_warning(tmp_path, monkeypatch):
    _only(monkeypatch, "adj_factors")
    cfg = _cfg(tmp_path)
    _factors(cfg, {"600981.SH": 1.35, "600036.SH": 1.0, "000001.SZ": 1.01})

    def factors(params):
        on_day = params["trade_date"] == "20261102"
        return [["600981.SH", 1.0], ["600036.SH", 1.0], ["000001.SZ", 1.01 if on_day else 1.0]]

    client, _ = _client(cfg, {"adj_factor": (["ts_code", "adj_factor"], factors)})
    findings = run_tushare_peer_checks(cfg, DAY, client=client)
    assert [f["check"] for f in findings] == ["tushare_peer_factor_step"]
    assert findings[0]["severity"] == "warning"
    assert findings[0]["symbols"] == ["600981.SH"]
    saved = json.loads(
        (cfg.meta_root / "quality/source_diffs/tushare-peer-2026-11-02.json").read_text()
    )
    assert saved["checks"] == {"adj_factors": "disagreed"}


def test_matching_factor_steps_leave_no_finding(tmp_path, monkeypatch):
    _only(monkeypatch, "adj_factors")
    cfg = _cfg(tmp_path)
    _factors(cfg, {"000001.SZ": 1.01})

    def factors(params):
        return [["000001.SZ", 1.01 if params["trade_date"] == "20261102" else 1.0]]

    client, _ = _client(cfg, {"adj_factor": (["ts_code", "adj_factor"], factors)})
    assert run_tushare_peer_checks(cfg, DAY, client=client) == []


def test_fund_flow_gaps_count_only_symbols_that_traded(tmp_path, monkeypatch):
    _only(monkeypatch, "fund_flow")
    cfg = _cfg(tmp_path)
    traded = [f"60000{i}.SH" for i in range(6)]
    _bars(cfg, traded)
    _write(
        cfg.curated_root,
        "fund_flow",
        f"trade_date={DAY}",
        pl.DataFrame(
            {
                "symbol": traded[:1],
                "trade_date": [DAY],
                "main_net_inflow": [1e6],
                "super_large_net_inflow": [None],
                "large_net_inflow": [None],
                "medium_net_inflow": [None],
                "small_net_inflow": [None],
            },
            schema_overrides={
                c: pl.Float64
                for c in (
                    "super_large_net_inflow",
                    "large_net_inflow",
                    "medium_net_inflow",
                    "small_net_inflow",
                )
            },
        ),
    )
    # The peer also lists a long-delisted code with zero flow.
    rows = [[s, 100.0] for s in traded] + [["000003.SZ", 0.0]]
    client, _ = _client(cfg, {"moneyflow_dc": (["ts_code", "net_amount"], rows)})
    findings = run_tushare_peer_checks(cfg, DAY, client=client)
    missing = [f for f in findings if f["check"] == "tushare_peer_fund_flow_missing"]
    assert missing[0]["missing"] == 5 and missing[0]["peer_rows"] == 6
    assert "000003.SZ" not in missing[0]["symbols"]
    assert missing[0]["severity"] == "warning"


def test_a_denied_endpoint_skips_its_check_and_is_cached(tmp_path, monkeypatch):
    _only(monkeypatch, "adj_factors", "fund_flow")
    cfg = _cfg(tmp_path)
    client, calls = _client(cfg, {}, denied=("adj_factor", "moneyflow_dc"))
    assert run_tushare_peer_checks(cfg, DAY, client=client) == []
    saved = json.loads(
        (cfg.meta_root / "quality/source_diffs/tushare-peer-2026-11-02.json").read_text()
    )
    assert saved["checks"] == {
        "adj_factors": "skipped_permission",
        "fund_flow": "skipped_permission",
    }
    assert cached_permissions(cfg) == {"adj_factor": "denied", "moneyflow_dc": "denied"}
    # Only the two probes went out; no check queried a denied endpoint.
    assert [api for api, _ in calls] == ["adj_factor", "moneyflow_dc"]


def test_a_failing_check_does_not_stop_the_others(tmp_path, monkeypatch):
    def broken(config, day, client):
        raise RuntimeError("peer down")

    monkeypatch.setattr(
        tushare_peer,
        "CHECKS",
        {"broken": (("daily",), broken), "adj_factors": tushare_peer.CHECKS["adj_factors"]},
    )
    cfg = _cfg(tmp_path)
    _factors(cfg, {"600981.SH": 1.35})
    client, _ = _client(
        cfg,
        {
            "daily": (["ts_code"], [["x"]]),
            "adj_factor": (["ts_code", "adj_factor"], [["600981.SH", 1.0]]),
        },
    )
    findings = run_tushare_peer_checks(cfg, DAY, client=client)
    saved = json.loads(
        (cfg.meta_root / "quality/source_diffs/tushare-peer-2026-11-02.json").read_text()
    )
    assert saved["checks"]["broken"].startswith("error:")
    assert [f["check"] for f in findings] == ["tushare_peer_factor_step"]


@pytest.mark.parametrize("enabled", [False, True])
def test_doctor_reads_permissions_from_cache_only(tmp_path, enabled):
    from cnequity.diagnostics.report import Finding, _check_tushare

    cfg = Config(
        data_root=tmp_path / "lake",
        sources={"tushare": enabled},
        tushare_token="tok",
    )
    findings: list[Finding] = []
    _check_tushare(cfg, findings)
    assert len(findings) == (1 if enabled else 0)
