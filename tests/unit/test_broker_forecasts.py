"""Offline tests for broker_forecasts (Tushare report_rc)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import httpx
import polars as pl
import pytest

from cnequity.adapters.tushare import broker_forecasts as bf
from cnequity.adapters.tushare.client import (
    TushareClient,
    TushareNotConfigured,
    TusharePermissionDenied,
)
from cnequity.config import Config, load_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.domain.http_policy import SourceCoolingDown
from cnequity.domain.schemas import BROKER_FORECASTS_SCHEMA, validate_dataframe

FIELDS = bf._FIELDS.split(",")


def _item(code="600519.SH", day="20240425", org="国信证券", quarter="2024Q4", eps=1.28, **extra):
    base = {f: None for f in FIELDS}
    base.update(
        ts_code=code,
        name="贵州茅台",
        report_date=day,
        report_title="点评",
        org_name=org,
        author_name="张三",
        quarter=quarter,
        eps=eps,
        np=308400.0,
        rating="买入",
    )
    base.update(extra)
    return [base[f] for f in FIELDS]


def _cfg(tmp_path, **kw):
    return Config(
        data_root=tmp_path,
        sources={"tushare": True},
        source_intervals={"tushare": 0},
        tushare_token="tok",
        **kw,
    )


def _fetch(cfg, respond, **kw):
    with httpx.Client(transport=httpx.MockTransport(respond)) as http:
        return bf.fetch_broker_forecasts(
            date(2024, 4, 25), config=cfg, client=TushareClient(cfg, client=http, **kw)
        )


def _ok(items, fields=FIELDS):
    return httpx.Response(
        200, json={"code": 0, "msg": "", "data": {"fields": fields, "items": items}}
    )


def test_normalize_matches_the_schema_and_derives_fiscal_year():
    frame = bf.normalize(
        FIELDS,
        [_item(), _item(quarter="2025Q4", eps=1.58)],
        fetched_at=datetime(2026, 11, 4, tzinfo=timezone.utc),
    )
    assert frame.schema == pl.Schema(BROKER_FORECASTS_SCHEMA)
    assert frame["fiscal_year"].to_list() == [2024, 2025]
    assert frame["report_date"].to_list() == [date(2024, 4, 25)] * 2
    assert set(frame["source"]) == {"tushare"}
    validate_dataframe(frame, "broker_forecasts")


def test_forecast_id_collapses_exact_repeats_but_keeps_different_numbers():
    rows = [_item(), _item(), _item(eps=1.30)]
    frame = bf.normalize(FIELDS, rows, fetched_at=datetime.now(timezone.utc))
    assert frame.height == 2
    assert frame["forecast_id"].n_unique() == 2


def test_rows_without_code_or_date_are_dropped():
    frame = bf.normalize(
        FIELDS,
        [_item(code=""), _item(day="2024-04-25"), _item()],
        fetched_at=datetime.now(timezone.utc),
    )
    assert frame.height == 1


def test_pages_until_a_short_page(tmp_path, monkeypatch):
    monkeypatch.setattr(bf, "_PAGE_SIZE", 2)
    seen = []

    def respond(request):
        body = json.loads(request.content)
        seen.append(body["params"])
        assert body["api_name"] == "report_rc" and body["token"] == "tok"
        offset = body["params"]["offset"]
        items = [_item(eps=offset + i) for i in range(2 if offset < 4 else 1)]
        return _ok(items)

    frame = _fetch(_cfg(tmp_path), respond)
    assert [p["offset"] for p in seen] == [0, 2, 4]
    assert all(p["start_date"] == p["end_date"] == "20240425" for p in seen)
    assert frame.height == 5


def test_business_error_is_raised_not_retried(tmp_path):
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(200, json={"code": 40203, "msg": "抱歉，您没有访问该接口的权限"})

    with pytest.raises(TusharePermissionDenied):
        _fetch(_cfg(tmp_path, max_retries=3), respond)
    assert len(calls) == 1


def test_rate_limit_message_cools_the_source(tmp_path):
    def respond(request):
        return httpx.Response(200, json={"code": -1, "msg": "抱歉，您每分钟最多访问该接口"})

    with pytest.raises(SourceCoolingDown):
        _fetch(_cfg(tmp_path), respond)


def test_server_error_is_retried(tmp_path):
    calls = []

    def respond(request):
        calls.append(request)
        return httpx.Response(502) if len(calls) == 1 else _ok([_item()])

    frame = _fetch(
        _cfg(tmp_path, max_retries=2, retry_backoff_seconds=0), respond, sleep=lambda _s: None
    )
    assert len(calls) == 2 and frame.height == 1


def test_without_a_token_nothing_is_requested(tmp_path):
    with pytest.raises(TushareNotConfigured):
        bf.fetch_broker_forecasts(date(2024, 4, 25), config=Config(data_root=tmp_path))
    with pytest.raises(TushareNotConfigured):
        bf.fetch_broker_forecasts(
            date(2024, 4, 25), config=Config(data_root=tmp_path, sources={"tushare": True})
        )


def test_loader_reads_api_url_and_token_file(tmp_path, monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    (tmp_path / "token").write_text("tok\n", encoding="utf-8")
    path = tmp_path / "cnequity.toml"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n'
        '[sources.tushare]\nenabled = true\napi_url = "https://relay.test/"\ntoken_file = "token"\n',
        encoding="utf-8",
    )
    cfg = load_config(path)
    assert cfg.tushare_api_url == "https://relay.test"
    assert cfg.tushare_token == "tok"
    assert cfg.sources.get("tushare") is True
    assert cfg.inline_credentials == ()


def test_api_url_defaults_to_the_official_endpoint(tmp_path, monkeypatch):
    from cnequity.adapters.tushare.client import TUSHARE_API_URL, api_url

    path = tmp_path / "cnequity.toml"
    path.write_text(f'[data]\nroot = "{path_for_toml(tmp_path / "lake")}"\n', encoding="utf-8")
    cfg = load_config(path)
    assert cfg.tushare_api_url is None
    assert api_url(cfg) == TUSHARE_API_URL
