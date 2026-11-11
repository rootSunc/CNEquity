"""同花顺 money-flow fallback: token sandbox, parsing, session guard, step hook."""

from __future__ import annotations

import subprocess
from datetime import date, datetime

import polars as pl
import pytest

from cnequity.adapters.ths import fund_flow as ths
from cnequity.adapters.ths import hexin
from cnequity.config import Config
from cnequity.run_scope import set_backfill

_STOCK_PAGE = """
<table><thead><tr>
<th>序号</th><th>股票代码</th><th>股票简称</th><th>最新价</th><th>涨跌幅</th><th>换手率</th>
<th>流入资金(元)</th><th>流出资金(元)</th><th>净额(元)</th><th>成交额(元)</th>
<!--<th>大单流入(元)</th>-->
</tr></thead><tbody>
<tr><td>1</td><td><a>301311</a></td><td>昆船智能</td><td>17.98</td><td>20.03%</td><td>14.10%</td>
<td>2.99亿</td><td>2.71亿</td><td>2753.86万</td><td>5.70亿</td><!--<td>1.55亿</td>--></tr>
<tr><td>2</td><td>920571</td><td>国航远洋</td><td>9.53</td><td>7.81%</td><td>3.2%</td>
<td>1234.5万</td><td>2000万</td><td>-765.5万</td><td>3.1亿</td></tr>
</tbody></table><span class="page_info">1/105</span>
"""

_BOARD_PAGE = """
<table><thead><tr>
<th>序号</th><th>行业</th><th>行业指数</th><th>涨跌幅</th><th>流入资金(亿)</th><th>流出资金(亿)</th>
<th>净额(亿)</th><th>公司家数</th><th>领涨股</th><th>涨跌幅</th><th>当前价(元)</th>
</tr></thead><tbody>
<tr><td>1</td><td><a href="http://q.10jqka.com.cn/thshy/detail/code/881280/">风电设备</a></td>
<td>4342.38</td><td>2.65%</td><td>45.43</td><td>38.37</td><td>7.06</td><td>32</td>
<td><a href="#">洛轴股份</a></td><td>19.99%</td><td>35.54</td></tr>
</tbody></table><span class="page_info">1/2</span>
"""

_D = date(2026, 9, 24)


# ---- token sandbox ------------------------------------------------------------------


def test_the_token_script_runs_with_no_permissions(monkeypatch, tmp_path):
    deno = tmp_path / "deno"
    deno.write_text("#!/bin/sh\n")
    deno.chmod(0o755)
    seen: list[list[str]] = []

    def _run(argv, **kwargs):
        seen.append(argv)
        assert "new Function(" in kwargs["input"]
        return subprocess.CompletedProcess(argv, 0, stdout="Ak3qiRXrLJPMtb-4-E15FMM2X\n", stderr="")

    monkeypatch.setattr(hexin.subprocess, "run", _run)
    cfg = Config(data_root=tmp_path / "data", ths_js_runtime=str(deno))
    assert hexin.hexin_v(cfg) == "Ak3qiRXrLJPMtb-4-E15FMM2X"
    argv = seen[0]
    assert argv[:2] == [str(deno), "run"]
    assert not [arg for arg in argv if arg.startswith("--allow") or arg == "-A"]
    assert "--no-prompt" in argv


def test_a_missing_runtime_is_a_clear_error(tmp_path):
    cfg = Config(data_root=tmp_path / "data", ths_js_runtime=str(tmp_path / "nope"))
    with pytest.raises(hexin.HexinTokenUnavailable, match="js_runtime"):
        hexin.hexin_v(cfg)


def test_garbage_output_is_not_a_token(monkeypatch, tmp_path):
    monkeypatch.setattr(hexin, "find_deno", lambda config=None: "/bin/deno")
    monkeypatch.setattr(
        hexin.subprocess,
        "run",
        lambda argv, **k: subprocess.CompletedProcess(argv, 1, stdout="", stderr="ReferenceError"),
    )
    with pytest.raises(hexin.HexinTokenUnavailable, match="ReferenceError"):
        hexin.hexin_v()


# ---- parsing --------------------------------------------------------------------------


def test_stock_page_parses_units_and_ignores_commented_columns():
    rows, pages = ths.parse_stock_page(_STOCK_PAGE, _D)
    assert pages == 105
    first, bj = rows
    assert first == {
        "symbol": "301311.SZ",
        "trade_date": _D,
        "inflow": 2.99e8,
        "outflow": 2.71e8,
        "net_inflow": 27538600.0,
        "amount": 5.70e8,
        "change_pct": 20.03,
        "turnover_pct": 14.10,
    }
    assert bj["symbol"] == "920571.BJ" and bj["net_inflow"] == -7655000.0


def test_board_page_takes_the_code_from_the_link_and_reads_yi():
    rows, pages = ths.parse_board_page(_BOARD_PAGE, _D, "industry")
    assert pages == 2
    assert rows[0]["sector_code"] == "881280"
    assert rows[0]["inflow"] == 4543000000.0 and rows[0]["net_inflow"] == 706000000.0
    assert rows[0]["company_count"] == 32


def test_a_changed_layout_fails_loudly():
    with pytest.raises(ths.ThsFundFlowError, match="columns changed"):
        ths.parse_stock_page(_STOCK_PAGE.replace("净额(元)", "净流入(元)"), _D)


# ---- session guard ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("now", "session"),
    [
        ("2026-09-26T12:00:00+00:00", date(2026, 9, 24)),  # Saturday after the 9-25 close
        ("2026-09-24T08:00:00+00:00", date(2026, 9, 24)),  # 16:00 Beijing
        ("2026-09-24T03:00:00+00:00", None),  # 11:00 Beijing, market open
        ("2026-09-28T00:30:00+00:00", date(2026, 9, 24)),  # Monday before the open
        ("2026-09-28T02:00:00+00:00", None),
    ],
)
def test_the_live_ranking_is_dated_by_the_calendar(now, session):
    assert ths.last_closed_session(datetime.fromisoformat(now)) == session


def test_a_fetch_for_another_session_is_refused():
    with pytest.raises(ths.ThsFundFlowError, match="undated live data"):
        ths.require_closed_session(
            date(2026, 9, 23), datetime.fromisoformat("2026-09-26T12:00:00+00:00")
        )


# ---- the fallback hook ---------------------------------------------------------------------


def test_fund_flow_falls_back_to_ths_and_still_reports_the_push2_failure(tmp_path, monkeypatch):
    from cnequity.adapters.eastmoney.em_auth import Push2PausedError
    from cnequity.steps import capital, ths_fallback

    def _push2(*a, **k):
        raise Push2PausedError("paused")

    monkeypatch.setattr(capital, "_run_capital_step", _push2)
    monkeypatch.setattr(
        ths, "fetch_fund_flow_ths", lambda d, config=None: pl.DataFrame({"symbol": ["600000.SH"]})
    )
    written: list[tuple] = []
    monkeypatch.setattr(
        ths_fallback,
        "write_fetched",
        lambda config, run_id, dataset, df, **k: (
            written.append((dataset, k["source"])) or {"rows_written": df.height}
        ),
    )
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"eastmoney": True, "ths": True},
        raw_archive_enabled=False,  # this hook test supplies normalized rows only
    )
    with pytest.raises(Push2PausedError):
        capital.step_fund_flow(cfg, _D, "run-1", {})
    assert written == [("fund_flow_ths", "ths")]


@pytest.mark.parametrize("dataset", ["fund_flow_ths", "sector_fund_flow_ths"])
@pytest.mark.parametrize("fail_last_page", [False, True])
def test_ths_fallback_archives_exact_pages_before_publishing(
    tmp_path, monkeypatch, dataset, fail_last_page
):
    from contextlib import nullcontext

    import httpx

    import cnequity.steps  # noqa: F401
    from cnequity.config import WaveConfig
    from cnequity.orchestrator.engine import JobEngine
    from cnequity.query import load
    from cnequity.steps import capital, rotation
    from cnequity.storage.raw_archive import RawPayloadArchive

    cfg = Config(data_root=tmp_path / "data", workers=1, max_retries=0)
    parent = dataset.removesuffix("_ths")

    def push2(*args, **kwargs):
        from cnequity.orchestrator.outcomes import SourceUnavailableError

        raise SourceUnavailableError("push2 unavailable")

    monkeypatch.setattr(capital, "_run_capital_step", push2)
    monkeypatch.setattr(rotation, "_step_sector_fund_flow", push2)
    monkeypatch.setattr(ths, "require_closed_session", lambda day: None)
    monkeypatch.setattr(ths, "hexin_v", lambda config: "test-token")
    monkeypatch.setattr(ths, "source_request", lambda *args: nullcontext())
    served = []

    def get(self, url, **kwargs):
        if dataset == "fund_flow_ths":
            last = "/page/2/" in url
            text = _STOCK_PAGE.replace("1/105", "2/2" if last else "1/2")
            if last:
                text = text.replace("301311", "301312").replace("920571", "920572")
        else:
            last = "/gnzjl/" in url
            text = _BOARD_PAGE.replace("1/2", "1/1")
        if last and fail_last_page:
            return httpx.Response(403, request=httpx.Request("GET", url))
        payload = text.encode("gbk")
        served.append(payload)
        return httpx.Response(200, content=payload, request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.Client, "get", get)
    result = JobEngine(cfg).run_job(
        "daily:capital" if parent == "fund_flow" else "daily:research",
        _D,
        waves=[WaveConfig(name="fallback", parallel=False, steps=[parent, "compact"])],
    )
    assert result["status"] == "degraded"
    assert result["fallback"]
    archive = RawPayloadArchive(cfg.meta_root)
    records = archive.records(dataset)
    assert len(records) == len(served)
    assert {archive.read(record) for record in records} == set(served)
    assert all(record.http_metadata["wire_exact"] for record in records)
    assert not list((cfg.curated_root / parent).rglob("*.parquet"))
    if fail_last_page:
        assert not list((cfg.curated_root / dataset).rglob("*.parquet"))
    else:
        frame = load(dataset, config=cfg)
        assert frame.height == (4 if dataset == "fund_flow_ths" else 2)
        assert frame["source"].unique().to_list() == ["ths"]


def test_a_failed_ths_fallback_never_masks_the_push2_error(tmp_path, monkeypatch):
    from cnequity.steps import capital

    def _push2(*a, **k):
        raise RuntimeError("push2 502")

    def _ths(*a, **k):
        raise ths.ThsFundFlowError("401")

    monkeypatch.setattr(capital, "_run_capital_step", _push2)
    monkeypatch.setattr(ths, "fetch_fund_flow_ths", _ths)
    cfg = Config(data_root=tmp_path / "data", sources={"eastmoney": True, "ths": True})
    with pytest.raises(RuntimeError, match="push2 502"):
        capital.step_fund_flow(cfg, _D, "run-1", {})


def test_no_fallback_on_a_backfill_or_when_push2_works(tmp_path, monkeypatch):
    from cnequity.steps import capital

    monkeypatch.setattr(
        ths, "fetch_fund_flow_ths", lambda *a, **k: pytest.fail("同花顺 must not be asked")
    )
    cfg = Config(data_root=tmp_path / "data", sources={"eastmoney": True, "ths": True})
    monkeypatch.setattr(capital, "_run_capital_step", lambda *a, **k: {"status": "success"})
    assert capital.step_fund_flow(cfg, _D, "run-1", {}) == {"status": "success"}

    def _push2(*a, **k):
        raise RuntimeError("push2 502")

    monkeypatch.setattr(capital, "_run_capital_step", _push2)
    set_backfill(cfg, active=True)
    with pytest.raises(RuntimeError, match="push2 502"):
        capital.step_fund_flow(cfg, _D, "run-1", {})


def test_the_ths_tables_are_never_stale_or_missing():
    from cnequity.domain.datasets import DATASETS
    from cnequity.quality.freshness import is_stale

    for name in ("fund_flow_ths", "sector_fund_flow_ths"):
        spec = DATASETS[name]
        assert spec.required is False and spec.empty_severity == "info"
        assert spec.watermark  # a live-snapshot (no watermark) spec is re-fetched when empty
        assert not is_stale(name, date(2020, 1, 2), date(2026, 9, 24))


def test_the_fallback_is_not_repeated_for_a_day_already_held(tmp_path, monkeypatch):
    from cnequity.steps import ths_fallback

    cfg = Config(data_root=tmp_path / "data", sources={"eastmoney": True, "ths": True})
    part = cfg.curated_root / "fund_flow_ths" / "trade_date=2026-09"
    part.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600000.SH"], "trade_date": [_D]}).write_parquet(
        part / "part-0.parquet"
    )
    monkeypatch.setattr(
        ths, "fetch_fund_flow_ths", lambda *a, **k: pytest.fail("already held; not refetched")
    )
    assert ths_fallback.stage_ths_fallback(cfg, _D, "run-2300", "fund_flow_ths") is None
