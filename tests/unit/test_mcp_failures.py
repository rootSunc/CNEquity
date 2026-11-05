"""MCP server: failures that are not a ToolError.

``test_mcp_server.py`` covers ``ToolError`` (bad argument) and ``TypeError``
(unexpected keyword). Untested:

* the generic ``except Exception`` in ``_call_tool``: an ``OSError`` from the
  disk, or a Polars/Arrow error from a damaged Parquet file. ``tools._read``
  converts only ``ReaderError``/``UniverseCoverageError``, so a file truncated
  by an interrupted write reaches the agent through this branch;
* ``dispatch`` itself raising (``INTERNAL_ERROR``), which must keep the stdio
  loop alive and preserve the request id;
* ``arguments``/``params`` that are not JSON objects;
* the HTTP transport turning a crashing tool into a 500 instead of ``isError``.
"""

from __future__ import annotations

import io
import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.mcp_server import protocol
from cnequity.mcp_server.catalog import HANDLERS


def _prov(source: str = "test") -> dict:
    return {
        "source": source,
        "data_version": "v1",
        "fetched_at": datetime(2024, 6, 28, tzinfo=timezone.utc),
    }


@pytest.fixture
def lake(tmp_path) -> Config:
    root = tmp_path / "data"
    curated = root / "curated"
    (curated / "instruments").mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "name": ["贵州茅台"],
            "exchange": ["SH"],
            "asset_type": ["stock"],
            "list_date": [date(2001, 8, 27)],
            "delist_date": [None],
            "prev_symbol": [None],
            **_prov(),
        },
        schema_overrides={"delist_date": pl.Date, "prev_symbol": pl.Utf8},
    ).write_parquet(curated / "instruments" / "part-merged.parquet")
    bars = curated / "daily_bars" / "trade_date=2024-06-27"
    bars.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 27)],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [1000],
            "amount": [10500.0],
            **_prov("tdx_protocol"),
        }
    ).write_parquet(bars / "part-0.parquet")
    return Config(data_root=root)


def _rpc(method: str, params: dict | list | None = None, request_id: int = 1) -> dict:
    return {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}


def _call(lake, method, params=None, request_id=1):
    return protocol.handle_message(lake, _rpc(method, params, request_id))


def _body(reply: dict) -> dict:
    return json.loads(reply["result"]["content"][0]["text"])


# ------------------------------------------------------ unexpected tool errors


def test_an_os_error_inside_a_tool_is_a_readable_result_not_a_dead_session(lake, monkeypatch):
    def broken(config, **kwargs):
        raise OSError(5, "Input/output error")

    monkeypatch.setitem(HANDLERS, "describe_lake", broken)

    reply = _call(lake, "tools/call", {"name": "describe_lake", "arguments": {}})
    assert "error" not in reply, "a tool fault must not become a JSON-RPC error"
    assert reply["result"]["isError"] is True
    assert _body(reply)["error"].startswith("OSError:")
    assert _call(lake, "ping", request_id=2)["result"] == {}


def test_a_parquet_file_truncated_by_an_interrupted_write_is_reported_to_the_agent(lake):
    """A half-written partition must surface as ``isError``, never as a short
    answer the agent would present as complete, and never end the session."""
    damaged = lake.curated_root / "daily_bars" / "trade_date=2024-06-28"
    damaged.mkdir(parents=True)
    path = damaged / "part-0.parquet"
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [1000],
            "amount": [10500.0],
            **_prov("tdx_protocol"),
        }
    ).write_parquet(path)
    payload = path.read_bytes()
    path.write_bytes(payload[: len(payload) // 2])

    reply = _call(
        lake, "tools/call", {"name": "query_bars", "arguments": {"symbols": ["600519.SH"]}}
    )

    assert "error" not in reply
    assert reply["result"]["isError"] is True
    body = _body(reply)
    assert body["error"]
    assert "rows" not in body
    assert _call(lake, "tools/list", request_id=2)["result"]["tools"]


# --------------------------------------------------------- protocol-level faults


def test_a_dispatch_crash_is_an_internal_error_and_the_stdio_loop_continues(lake, monkeypatch):
    real_dispatch = protocol.dispatch

    def flaky(config, method, params):
        if method == "tools/list":
            raise RuntimeError("catalog import failed")
        return real_dispatch(config, method, params)

    monkeypatch.setattr(protocol, "dispatch", flaky)
    stdin = io.StringIO(
        json.dumps(_rpc("tools/list", request_id=1))
        + "\n"
        + json.dumps(_rpc("ping", request_id=2))
        + "\n"
    )
    stdout = io.StringIO()

    protocol.serve_stdio(lake, stdin=stdin, stdout=stdout)

    replies = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [r["id"] for r in replies] == [1, 2]
    assert replies[0]["error"]["code"] == protocol.INTERNAL_ERROR
    assert replies[0]["error"]["message"].startswith("RuntimeError:")
    assert replies[1]["result"] == {}


@pytest.mark.parametrize("arguments", ["include_empty=true", [1], 7])
def test_non_object_arguments_are_invalid_params(lake, arguments):
    reply = _call(lake, "tools/call", {"name": "describe_lake", "arguments": arguments})
    assert reply["error"]["code"] == protocol.INVALID_PARAMS
    assert "arguments must be an object" in reply["error"]["message"]


def test_non_object_params_are_ignored_rather_than_fatal(lake):
    reply = protocol.handle_message(
        lake, {"jsonrpc": "2.0", "id": 9, "method": "ping", "params": [1, 2]}
    )
    assert reply == {"jsonrpc": "2.0", "id": 9, "result": {}}
    reply = protocol.handle_message(
        lake, {"jsonrpc": "2.0", "id": 10, "method": "tools/call", "params": ["describe_lake"]}
    )
    assert reply["error"]["code"] == protocol.INVALID_PARAMS


def test_a_request_with_an_id_but_no_method_is_an_invalid_request(lake):
    reply = protocol.handle_message(lake, {"jsonrpc": "2.0", "id": 4})
    assert reply["id"] == 4
    assert reply["error"]["code"] == protocol.INVALID_REQUEST


# ------------------------------------------------------------------- HTTP


def test_a_crashing_tool_over_http_is_200_with_is_error_not_500(lake, monkeypatch):
    from starlette.testclient import TestClient

    from cnequity.mcp_server.http import create_app

    def broken(config, **kwargs):
        raise RuntimeError("Out of Memory Error: failed to allocate block")

    monkeypatch.setitem(HANDLERS, "describe_lake", broken)
    client = TestClient(create_app(lake), base_url="http://127.0.0.1:8788")

    resp = client.post("/mcp", json=_rpc("tools/call", {"name": "describe_lake", "arguments": {}}))

    assert resp.status_code == 200
    reply = resp.json()
    assert reply["result"]["isError"] is True
    assert _body(reply)["error"].startswith("RuntimeError: Out of Memory")


def test_a_type_error_inside_a_tool_is_not_blamed_on_the_arguments(lake, monkeypatch):
    def broken(config, include_empty=False):
        return None + 1  # a bug in the tool, with correct arguments

    monkeypatch.setitem(HANDLERS, "describe_lake", broken)

    reply = _call(lake, "tools/call", {"name": "describe_lake", "arguments": {}})
    assert reply["result"]["isError"] is True
    error = _body(reply)["error"]
    assert error.startswith("TypeError:")
    assert "bad arguments" not in error

    wrong = _call(lake, "tools/call", {"name": "describe_lake", "arguments": {"bogus": 1}})
    assert _body(wrong)["error"].startswith("bad arguments for describe_lake")


def test_one_damaged_state_file_does_not_take_down_the_catalog(lake):
    from cnequity.query.reader import list_datasets
    from cnequity.storage.state import StateStore

    store = StateStore(lake.meta_root)
    store.set_date("instruments", date(2024, 6, 27))
    (store.root / "daily_bars.json").write_text("{", encoding="utf-8")

    catalog = {row["dataset"]: row for row in list_datasets(config=lake).iter_rows(named=True)}
    assert catalog["daily_bars"]["state_error"].startswith("JSONDecodeError")
    assert catalog["daily_bars"]["watermark"] is None
    assert catalog["instruments"]["state_error"] is None

    reply = _call(lake, "tools/call", {"name": "describe_lake", "arguments": {}})
    assert reply["result"]["isError"] is False
    rows = {row["dataset"]: row for row in _body(reply)["datasets"]}
    assert rows["daily_bars"]["state_error"].startswith("JSONDecodeError")
    assert "state_error" not in rows["instruments"]
    # Readers that need the watermark still fail loud.
    with pytest.raises(ValueError):
        store.get_date("daily_bars")
