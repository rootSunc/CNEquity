"""The shared Tushare client: endpoint, paging, refusals and opt-in gating."""

import json

import httpx
import pytest

from cnequity.adapters.tushare.client import (
    TUSHARE_API_URL,
    TushareClient,
    TushareNotConfigured,
    TusharePermissionDenied,
    TushareQuotaExhausted,
    TushareRefused,
    api_url,
    probe_permissions,
    tushare_enabled,
)
from cnequity.config import Config
from cnequity.domain.http_policy import SourceCoolingDown


def _cfg(tmp_path, **kw):
    return Config(
        data_root=tmp_path,
        sources={"tushare": True},
        tushare_token="tok",
        source_intervals={"tushare": 0},
        **kw,
    )


def _ok(fields, items):
    return httpx.Response(
        200, json={"code": 0, "msg": "", "data": {"fields": fields, "items": items}}
    )


def _refuse(code, msg):
    return httpx.Response(200, json={"code": code, "msg": msg, "data": None})


def test_disabled_or_tokenless_source_never_builds_a_client(tmp_path):
    assert not tushare_enabled(None)
    assert not tushare_enabled(Config(data_root=tmp_path, tushare_token="tok"))
    assert not tushare_enabled(Config(data_root=tmp_path, sources={"tushare": True}))
    with pytest.raises(TushareNotConfigured):
        TushareClient(Config(data_root=tmp_path, sources={"tushare": True}))
    assert probe_permissions(Config(data_root=tmp_path), {"daily": {}}) == {}


def test_requests_go_to_the_configured_endpoint_with_the_api_path(tmp_path):
    seen = []

    def respond(request):
        seen.append((str(request.url), json.loads(request.content)))
        return _ok(["ts_code", "close"], [["600036.SH", 43.55]])

    cfg = _cfg(tmp_path)
    cfg.tushare_api_url = "https://relay.example/"
    with httpx.Client(transport=httpx.MockTransport(respond)) as http:
        frame = TushareClient(cfg, client=http).query("daily", {"trade_date": "20261030"})
    assert seen[0][0] == "https://relay.example/daily"
    assert seen[0][1]["api_name"] == "daily" and seen[0][1]["token"] == "tok"
    assert frame.to_dicts() == [{"ts_code": "600036.SH", "close": 43.55}]
    assert api_url(_cfg(tmp_path)) == TUSHARE_API_URL


def test_query_all_pages_until_a_short_page(tmp_path):
    offsets = []

    def respond(request):
        params = json.loads(request.content)["params"]
        offsets.append(params["offset"])
        rows = 2 if params["offset"] < 4 else 1
        return _ok(["n"], [[params["offset"] + i] for i in range(rows)])

    with httpx.Client(transport=httpx.MockTransport(respond)) as http:
        frame = TushareClient(_cfg(tmp_path), client=http).query_all("x", page_size=2)
    assert offsets == [0, 2, 4]
    assert frame["n"].to_list() == [0, 1, 2, 3, 4]


def test_full_last_page_past_the_limit_is_an_error_not_a_truncation(tmp_path):
    with httpx.Client(transport=httpx.MockTransport(lambda r: _ok(["n"], [[1], [2]]))) as http:
        with pytest.raises(RuntimeError, match="pagination"):
            TushareClient(_cfg(tmp_path), client=http).query_all("x", page_size=2, max_pages=3)


@pytest.mark.parametrize(
    ("code", "msg", "error"),
    [
        (40101, "您的token不对，请确认。", TushareRefused),
        (40203, "抱歉，您没有访问该接口的权限", TusharePermissionDenied),
        (40203, "抱歉，您访问接口(stk_mins)频率超限(2次/天)", TushareQuotaExhausted),
    ],
)
def test_business_refusals_are_raised_once_without_retry(tmp_path, code, msg, error):
    calls = []

    def respond(request):
        calls.append(1)
        return _refuse(code, msg)

    with httpx.Client(transport=httpx.MockTransport(respond)) as http:
        with pytest.raises(error):
            TushareClient(_cfg(tmp_path, max_retries=3), client=http).query("x")
    assert len(calls) == 1


def test_per_minute_rate_refusal_cools_the_source_down(tmp_path):
    cfg = _cfg(tmp_path)
    with httpx.Client(
        transport=httpx.MockTransport(lambda r: _refuse(40203, "抱歉，您每分钟最多访问该接口500次"))
    ) as http:
        client = TushareClient(cfg, client=http)
        with pytest.raises(SourceCoolingDown):
            client.query("daily")
        with pytest.raises(SourceCoolingDown):
            client.query("daily")


def test_transient_failures_are_retried(tmp_path):
    replies = iter([httpx.Response(502), _ok(["n"], [[1]])])
    with httpx.Client(transport=httpx.MockTransport(lambda r: next(replies))) as http:
        frame = TushareClient(
            _cfg(tmp_path, max_retries=2, retry_backoff_seconds=0),
            client=http,
            sleep=lambda s: None,
        ).query("x")
    assert frame["n"].to_list() == [1]


def test_empty_result_keeps_the_source_columns(tmp_path):
    with httpx.Client(
        transport=httpx.MockTransport(lambda r: _ok(["ts_code", "close"], []))
    ) as http:
        frame = TushareClient(_cfg(tmp_path), client=http).query("daily")
    assert frame.columns == ["ts_code", "close"] and frame.is_empty()


def test_permissions_are_probed_once_and_cached_per_token(tmp_path):
    calls = []

    def respond(request):
        api = json.loads(request.content)["api_name"]
        calls.append(api)
        if api == "stk_limit":
            return _refuse(40203, "抱歉，您没有访问该接口的权限")
        return _ok(["n"], [[1]])

    cfg = _cfg(tmp_path)
    probes = {"daily": {"trade_date": "20261030"}, "stk_limit": {"trade_date": "20261030"}}
    with httpx.Client(transport=httpx.MockTransport(respond)) as http:
        client = TushareClient(cfg, client=http)
        first = probe_permissions(cfg, probes, client=client)
        second = probe_permissions(cfg, probes, client=client)
    assert first == second == {"daily": "ok", "stk_limit": "denied"}
    assert calls == ["daily", "stk_limit"]
    # A different endpoint invalidates the cache: its account may differ.
    cfg.tushare_api_url = "https://relay.example"
    with httpx.Client(transport=httpx.MockTransport(respond)) as http:
        probe_permissions(cfg, probes, client=TushareClient(cfg, client=http))
    assert len(calls) == 4
