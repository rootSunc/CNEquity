"""Offline coverage for the explicit 同花顺 BJ corporate-action repair."""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone

import httpx
import polars as pl

import cnequity.steps  # noqa: F401
from cnequity.adapters.ths import corporate_actions as ca
from cnequity.config import Config
from cnequity.run_scope import set_backfill, set_run_flags
from cnequity.steps import events

_PAGE = """
<html><body>
<table class="m_table">
<tr><th>报告期</th><th>董事会日期</th><th>股东大会预案公告日期</th>
<th>实施公告日</th><th>分红方案说明</th><th>A股股权登记日</th>
<th>A股除权除息日</th><th>分红总额</th><th>方案进度</th></tr>
<tr><td>2021年报</td><td>2022-04-19</td><td>2022-05-27</td>
<td>2022-06-29</td><td>10送3股派0.5元(含税)</td><td>2022-07-06</td>
<td>2022-07-07</td><td>766.67万</td><td>实施方案</td></tr>
<tr><td>2020年报</td><td>2021-04-19</td><td>2021-05-27</td>
<td>2021-06-29</td><td>不分配不转增</td><td>--</td>
<td>--</td><td>--</td><td>实施方案</td></tr>
<tr><td>2019年报</td><td>2020-04-19</td><td>2020-05-27</td>
<td>2020-06-29</td><td>10配3股，配股价5.00元</td><td>2020-07-06</td>
<td>2020-07-07</td><td>--</td><td>董事会预案</td></tr>
</table>
</body></html>
"""


def test_parse_plan_uses_per_share_units_and_keeps_combined_actions():
    values = ca._parse_plan("10转4股派4.50元(含税)")
    assert values == {
        "cash_dividend": 0.45,
        "bonus_ratio": 0.0,
        "transfer_ratio": 0.4,
        "allotment_ratio": 0.0,
        "allotment_price": None,
    }


def test_parse_plan_accepts_transfer_increase_wording():
    values = ca._parse_plan("10股转增3股派0.5元")
    assert values["transfer_ratio"] == 0.3
    assert values["cash_dividend"] == 0.05


def test_rows_from_page_requires_completed_plan_and_filters_window():
    rows = ca._rows_from_page("430090.BJ", _PAGE, date(2022, 1, 1), date(2022, 12, 31))
    assert len(rows) == 2
    by_type = {row["action_type"]: row for row in rows}
    assert by_type["cash_dividend"]["cash_dividend"] == 0.05
    assert by_type["bonus"]["bonus_ratio"] == 0.3
    assert by_type["cash_dividend"]["ex_date"] == date(2022, 7, 7)


def test_fetch_corporate_actions_only_queries_bj_and_reports_page_failures():
    calls: list[str] = []

    def page(code: str) -> str:
        calls.append(code)
        if code == "430198":
            raise RuntimeError("source unavailable")
        return _PAGE

    frame, failed = ca.fetch_corporate_actions_ths(
        ["600519.SH", "430090.BJ", "430198.BJ", "430090.BJ"],
        date(2001, 1, 1),
        date(2025, 12, 31),
        page_fetcher=page,
    )
    assert calls == ["430090", "430198"]
    assert failed == ["430198.BJ"]
    assert frame["symbol"].unique().to_list() == ["430090.BJ"]
    assert frame["ex_date"].to_list() == [date(2022, 7, 7)] * 2


def test_bonus_page_reused_without_refreshing_raw_observation_time(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "lake",
        sources={"ths_bonus": True},
        source_intervals={"ths": 0, "ths_bonus": 0},
    )
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return httpx.Response(
            200, content=_PAGE.encode("gb18030"), request=httpx.Request("GET", url)
        )

    monkeypatch.setattr(ca.httpx, "get", fake_get)
    windows = [(date(2022, 1, 1), date(2022, 12, 31))] * 2
    for run_id, window in zip(("run-1", "run-2"), windows, strict=True):
        frame, failed = ca.fetch_corporate_actions_ths(
            ["430090.BJ"], *window, config=cfg, run_id=run_id, request_scope="bonus-repair"
        )
        assert len(frame) == 2
        assert failed == []
    repeated, failed = ca.fetch_corporate_actions_ths(
        ["430090.BJ"], *windows[1], config=cfg, run_id="run-2", request_scope="bonus-repair"
    )
    assert len(repeated) == 2 and failed == []
    assert len(calls) == 1

    records = [
        json.loads(path.read_text())
        for path in (cfg.meta_root / "raw" / "corporate_actions").glob("**/*.json")
    ]
    assert len(records) == 2
    assert {record["run_id"] for record in records} == {"run-1", "run-2"}
    assert len({record["captured_at"] for record in records}) == 1
    assert {record["http_metadata"]["cache_reuse"] for record in records} == {False, True}
    assert len({record["payload_sha256"] for record in records}) == 1


def test_bonus_page_single_flight_and_bad_or_expired_cache_refetches(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "lake",
        sources={"ths_bonus": True},
        source_intervals={"ths": 0, "ths_bonus": 0},
        raw_archive_enabled=False,
    )
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return httpx.Response(
            200, content=_PAGE.encode("gb18030"), request=httpx.Request("GET", url)
        )

    monkeypatch.setattr(ca.httpx, "get", fake_get)

    def read(_):
        return ca.fetch_corporate_actions_ths(
            ["430090.BJ"], date(2022, 1, 1), date(2022, 12, 31), config=cfg
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(read, range(2)))
    assert all(len(frame) == 2 and not failed for frame, failed in results)
    assert len(calls) == 1

    path = next((cfg.meta_root / "source_cache" / "ths_bonus").glob("*.json"))
    saved = json.loads(path.read_text())
    saved["sha256"] = "0" * 64
    path.write_text(json.dumps(saved))
    assert len(read(None)[0]) == 2
    assert len(calls) == 2

    saved = json.loads(path.read_text())
    saved["captured_at"] = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    path.write_text(json.dumps(saved))
    assert len(read(None)[0]) == 2
    assert len(calls) == 3


def test_bonus_page_does_not_cache_empty_or_challenge_html(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "lake",
        sources={"ths_bonus": True},
        source_intervals={"ths": 0, "ths_bonus": 0},
        raw_archive_enabled=False,
        max_retries=1,
    )
    pages = [_PAGE.replace("实施方案", "董事会预案"), "<html>challenge</html>", _PAGE]
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return httpx.Response(
            200, content=pages.pop(0).encode("gb18030"), request=httpx.Request("GET", url)
        )

    monkeypatch.setattr(ca.httpx, "get", fake_get)
    args = (["430090.BJ"], date(2022, 1, 1), date(2022, 12, 31))
    frame, failed = ca.fetch_corporate_actions_ths(*args, config=cfg)
    assert frame.is_empty() and failed == []
    frame, failed = ca.fetch_corporate_actions_ths(*args, config=cfg)
    assert frame.is_empty() and failed == ["430090.BJ"]
    frame, failed = ca.fetch_corporate_actions_ths(*args, config=cfg)
    assert len(frame) == 2 and failed == []
    assert len(calls) == 3


def test_bonus_cache_write_failure_keeps_valid_actions(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "lake",
        sources={"ths_bonus": True},
        source_intervals={"ths": 0, "ths_bonus": 0},
        raw_archive_enabled=False,
    )
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        return httpx.Response(
            200, content=_PAGE.encode("gb18030"), request=httpx.Request("GET", url)
        )

    def fail_cache(*args, **kwargs):
        raise OSError("cache read-only")

    monkeypatch.setattr(ca.httpx, "get", fake_get)
    monkeypatch.setattr(ca, "write_json_atomic", fail_cache)
    frame, failed = ca.fetch_corporate_actions_ths(
        ["430090.BJ"], date(2022, 1, 1), date(2022, 12, 31), config=cfg
    )
    assert len(frame) == 2 and failed == []
    assert len(calls) == 1
    assert not list((cfg.meta_root / "source_cache" / "ths_bonus").glob("*.json"))


def test_step_ths_repair_is_scoped_to_delisted_bj_and_keeps_provenance(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "lake",
        sources={"ths_bonus": True},
        raw_archive_enabled=False,
    )
    set_backfill(cfg, active=True, start=date(2020, 1, 1), end=date(2020, 12, 31))
    set_run_flags(cfg, ca_ths_repair=True)

    monkeypatch.setattr(events, "load_symbols", lambda _cfg: ["430090.BJ", "600000.SH"])
    monkeypatch.setattr(
        events,
        "instrument_metadata",
        lambda _cfg: pl.DataFrame(
            {
                "symbol": ["430090.BJ", "600000.SH"],
                "list_date": [date(2010, 1, 1)] * 2,
                "delist_date": [date(2025, 9, 30), None],
            }
        ),
    )
    monkeypatch.setattr(events, "fetch_corporate_actions", lambda *args, **kwargs: pl.DataFrame())

    repair_rows = pl.DataFrame(
        {
            "symbol": ["430090.BJ"],
            "ex_date": [date(2020, 7, 7)],
            "action_type": ["cash_dividend"],
            "cash_dividend": [0.05],
            "bonus_ratio": [0.0],
            "transfer_ratio": [0.0],
            "allotment_ratio": [None],
            "allotment_price": [None],
        },
        schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
    )
    seen = []

    def fake_ths(symbols, start, end, **kwargs):
        seen.append((symbols, start, end))
        return repair_rows, []

    monkeypatch.setattr(events, "fetch_corporate_actions_ths", fake_ths)
    result = events.step_corporate_actions(cfg, date(2020, 12, 31), "run-1", {})

    assert seen == [(["430090.BJ"], date(2020, 1, 1), date(2020, 12, 31))]
    assert result["rows_written"] == 1
    staged = list((cfg.staging_root / "corporate_actions").glob("**/*.parquet"))
    assert len(staged) == 1
    assert pl.read_parquet(staged[0])["source"].to_list() == ["ths"]
