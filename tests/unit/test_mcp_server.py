"""MCP surface: the tool layer and the stdio protocol loop."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.mcp_server import protocol, tools
from cn_market_lake.mcp_server.catalog import DESCRIPTORS, HANDLERS


def _prov(source: str = "test") -> dict:
    return {
        "source": source,
        "data_version": "v1",
        "fetched_at": datetime(2024, 6, 28, tzinfo=timezone.utc),
    }


@pytest.fixture
def lake(tmp_path):
    root = tmp_path / "data"
    curated = root / "curated"
    derived = root / "derived"

    (curated / "instruments").mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ", "600001.SH"],
            "name": ["贵州茅台", "平安银行", "邯郸钢铁"],
            "exchange": ["SH", "SZ", "SH"],
            "asset_type": ["stock", "stock", "stock"],
            "list_date": [date(2001, 8, 27), date(1991, 4, 3), date(1997, 12, 8)],
            "delist_date": [None, None, date(2009, 12, 25)],
            "prev_symbol": [None, None, None],
            **_prov(),
        }
    ).write_parquet(curated / "instruments" / "part-merged.parquet")

    bars_dir = curated / "daily_bars" / "trade_date=2024-06-27"
    bars_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "trade_date": [date(2024, 6, 27)] * 2,
            "open": [10.0, 20.0],
            "high": [11.0, 21.0],
            "low": [9.0, 19.0],
            "close": [10.5, 20.5],
            "volume": [1000, 2000],
            "amount": [10500.0, 41000.0],
            **_prov("tdx_protocol"),
        }
    ).write_parquet(bars_dir / "part-0.parquet")

    adj_dir = derived / "adj_factors" / "trade_date=2024-06-27"
    adj_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 27)],
            "adjust_type": ["hfq"],
            "factor": [2.0],
            **_prov("sina"),
        }
    ).write_parquet(adj_dir / "part-0.parquet")

    fsi_dir = curated / "financial_statement_items" / "report_period=2024Q1"
    fsi_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "600519.SH"],
            "report_period": ["2024Q1", "2024Q1"],
            "statement_type": ["income", "income"],
            "item_code": ["roe", "revenue"],
            "item_value": [0.25, 1_000_000.0],
            "announce_date": [date(2024, 4, 28), date(2024, 5, 15)],
            **_prov(),
        }
    ).write_parquet(fsi_dir / "part-0.parquet")

    return Config(data_root=root)


# --- payload shape ---------------------------------------------------------


def test_rows_are_columnar_and_drop_provenance(lake):
    payload = tools.query_bars(lake, symbols=["600519.SH"])
    assert payload["columns"][:2] == ["symbol", "trade_date"]
    assert isinstance(payload["rows"][0], list)
    # Repeated per row it would be a third of the payload; summarised it is three words.
    assert not set(tools.PROVENANCE_COLS) & set(payload["columns"])
    assert payload["sources"] == ["tdx_protocol"]


def test_provenance_included_on_request(lake):
    payload = tools.query_bars(lake, symbols=["600519.SH"], include_provenance=True)
    assert "source" in payload["columns"]


def test_truncation_reports_the_real_total(lake):
    """A page that looks complete is how an agent averages 2 rows and calls it the market."""
    payload = tools.query_bars(lake, limit=1)
    assert payload["returned"] == 1
    assert payload["total"] == 2
    assert payload["truncated"] is True
    assert "do not treat this page as the full result" in payload["note"]


def test_limit_is_clamped_not_rejected(lake):
    payload = tools.query_bars(lake, limit=10**9)
    assert payload["returned"] <= tools.MAX_LIMIT


# --- describe_lake ---------------------------------------------------------


def test_describe_lake_lists_only_populated_datasets_by_default(lake):
    payload = tools.describe_lake(lake)
    names = {row["dataset"] for row in payload["datasets"]}
    assert {"daily_bars", "instruments", "financial_statement_items"} <= names
    assert "dragon_tiger" not in names
    assert "financial_statement_items" in payload["summary"]["point_in_time"]


def test_describe_lake_states_the_adjustment_rule(lake):
    """The contract is only useful if it is in the response, not in our docs."""
    contract = " ".join(tools.describe_lake(lake)["contract"])
    assert "UNADJUSTED" in contract
    assert "snapshot_only" in contract
    assert "as_of" in contract


def test_describe_lake_can_include_empty(lake):
    payload = tools.describe_lake(lake, include_empty=True)
    assert len(payload["datasets"]) == payload["summary"]["registered"]


# --- resolve_symbol --------------------------------------------------------


def test_resolve_symbol_by_chinese_name(lake):
    payload = tools.resolve_symbol(lake, query="茅台")
    assert payload["rows"][0][0] == "600519.SH"


def test_resolve_symbol_by_bare_code(lake):
    payload = tools.resolve_symbol(lake, query="600519")
    assert payload["rows"][0][0] == "600519.SH"


def test_resolve_symbol_keeps_delisted_but_ranks_them_last(lake):
    """Dropping them here would reintroduce the survivorship bias the lake removes."""
    payload = tools.resolve_symbol(lake, query="600")
    symbols = [row[0] for row in payload["rows"]]
    assert symbols == ["600519.SH", "600001.SH"]
    delist_col = payload["columns"].index("delist_date")
    assert payload["rows"][1][delist_col] == "2009-12-25"


def test_resolve_symbol_miss_explains_the_format(lake):
    payload = tools.resolve_symbol(lake, query="zzzz")
    assert payload["total"] == 0
    assert "920xxx" in payload["note"]


def test_resolve_symbol_rejects_empty(lake):
    with pytest.raises(tools.ToolError):
        tools.resolve_symbol(lake, query="  ")


# --- query_bars ------------------------------------------------------------


def test_unadjusted_bars_carry_a_warning(lake):
    payload = tools.query_bars(lake, symbols=["600519.SH"])
    assert "adjust='hfq'" in payload["warning"]


def test_adjusted_bars_report_inexact_rows(lake):
    """000001.SZ has no factor: silently multiplying by 1.0 is the bug worth naming."""
    payload = tools.query_bars(lake, adjust="hfq")
    assert "adj_is_exact=false" in payload["warning"]
    assert "1 of 2 rows" in payload["warning"]


def test_adjusted_bars_without_gaps_have_no_warning(lake):
    payload = tools.query_bars(lake, symbols=["600519.SH"], adjust="hfq")
    assert "warning" not in payload
    close = payload["columns"].index("adj_close")
    assert payload["rows"][0][close] == pytest.approx(21.0)


def test_query_bars_rejects_a_non_bar_dataset(lake):
    with pytest.raises(tools.ToolError, match="query_dataset"):
        tools.query_bars(lake, dataset="fund_flow")


def test_query_bars_rejects_an_unknown_adjust(lake):
    with pytest.raises(tools.ToolError, match="adjust"):
        tools.query_bars(lake, adjust="raw")


def test_symbols_accept_a_comma_string(lake):
    """Agents hand back a string about as often as a list; the intent is unambiguous."""
    payload = tools.query_bars(lake, symbols="600519.SH, 000001.SZ")
    assert payload["total"] == 2


# --- query_fundamentals ----------------------------------------------------


def test_fundamentals_require_as_of(lake):
    with pytest.raises(tools.ToolError, match="point-in-time"):
        tools.query_fundamentals(lake, symbols=["600519.SH"])


def test_fundamentals_apply_the_pit_cutoff(lake):
    """revenue was announced 2024-05-15; asking as of April must not see it."""
    payload = tools.query_fundamentals(lake, as_of="2024-04-30")
    items = {row[payload["columns"].index("item_code")] for row in payload["rows"]}
    assert items == {"roe"}


# --- query_dataset ---------------------------------------------------------


def test_query_dataset_routes_bars_to_query_bars(lake):
    with pytest.raises(tools.ToolError, match="query_bars"):
        tools.query_dataset(lake, dataset="daily_bars")


def test_query_dataset_routes_pit_to_query_fundamentals(lake):
    with pytest.raises(tools.ToolError, match="query_fundamentals"):
        tools.query_dataset(lake, dataset="financial_statement_items")


def test_query_dataset_unknown_name_points_at_describe_lake(lake):
    with pytest.raises(tools.ToolError, match="describe_lake"):
        tools.query_dataset(lake, dataset="not_a_dataset")


def test_query_dataset_flags_snapshot_only_history(lake):
    (lake.curated_root / "fund_flow" / "trade_date=2024-06-27").mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 27)],
            "main_net_inflow": [1.0],
            "super_large_net_inflow": [1.0],
            "large_net_inflow": [1.0],
            "medium_net_inflow": [1.0],
            "small_net_inflow": [1.0],
            **_prov("eastmoney"),
        }
    ).write_parquet(lake.curated_root / "fund_flow" / "trade_date=2024-06-27" / "part-0.parquet")
    payload = tools.query_dataset(lake, dataset="fund_flow")
    assert payload["history_mode"] == "snapshot_only"
    assert "no honest deeper history" in payload["warning"] or "snapshot-only" in payload["warning"]


# --- run_sql ---------------------------------------------------------------


def test_run_sql_aggregates(lake):
    payload = tools.run_sql(lake, sql="SELECT count(*) AS n FROM daily_bars")
    assert payload["rows"] == [[2]]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT 1; DROP VIEW daily_bars",
        "DROP VIEW daily_bars",
        "COPY (SELECT 1) TO '/tmp/exfiltrated.csv'",
        "CREATE TABLE t AS SELECT 1",
        "ATTACH '/tmp/other.db'",
    ],
)
def test_run_sql_rejects_everything_but_one_select(lake, sql):
    """The lake ingests vendor news, so SQL reaching this tool can be shaped by
    text nobody here wrote. A read-only connection alone would still allow COPY."""
    with pytest.raises(tools.ToolError):
        tools.run_sql(lake, sql=sql)


def test_run_sql_comment_cannot_smuggle_a_second_statement(lake):
    payload = tools.run_sql(lake, sql="SELECT 1 AS n -- ; DROP VIEW daily_bars")
    assert payload["rows"] == [[1]]


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM read_text('/etc/hosts')",
        "SELECT * FROM read_csv('https://example.com/data.csv')",
    ],
)
def test_run_sql_cannot_access_external_files_or_urls(lake, sql):
    """A read-only DuckDB connection must not become a host file oracle."""
    with pytest.raises(tools.ToolError):
        tools.run_sql(lake, sql=sql)


def test_run_sql_cannot_access_a_file_outside_the_lake(lake, tmp_path):
    secret = tmp_path / "outside-lake-secret.txt"
    secret.write_text("must not be exposed", encoding="utf-8")

    with pytest.raises(tools.ToolError):
        tools.run_sql(lake, sql=f"SELECT * FROM read_text('{secret.as_posix()}')")


def test_run_sql_cannot_access_external_parquet(lake, tmp_path):
    outside = tmp_path / "outside-lake-secret.parquet"
    pl.DataFrame({"secret": ["must not be exposed"]}).write_parquet(outside)

    with pytest.raises(tools.ToolError):
        tools.run_sql(lake, sql=f"SELECT * FROM read_parquet('{outside.as_posix()}')")


# --- protocol --------------------------------------------------------------


def _call(lake, method, params=None, request_id=1):
    return protocol.handle_message(
        lake, {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}}
    )


def test_initialize_echoes_the_clients_protocol_version(lake):
    """Every shipped revision has the same tools primitive; refusing an older
    client would lock out pinned editors and buy nothing."""
    reply = _call(lake, "initialize", {"protocolVersion": "2024-11-05"})
    assert reply["result"]["protocolVersion"] == "2024-11-05"
    assert reply["result"]["capabilities"] == {"tools": {}}


def test_initialize_falls_back_to_our_version(lake):
    reply = _call(lake, "initialize", {})
    assert reply["result"]["protocolVersion"] == protocol.PROTOCOL_VERSION


def test_tools_list_matches_the_handlers(lake):
    reply = _call(lake, "tools/list")
    listed = [tool["name"] for tool in reply["result"]["tools"]]
    assert listed == list(HANDLERS)
    assert all("inputSchema" in tool for tool in reply["result"]["tools"])
    assert all("handler" not in tool for tool in DESCRIPTORS)


def test_notifications_get_no_reply(lake):
    assert (
        protocol.handle_message(lake, {"jsonrpc": "2.0", "method": "notifications/initialized"})
        is None
    )


def test_unknown_method_is_a_protocol_error(lake):
    reply = _call(lake, "resources/list")
    assert reply["error"]["code"] == protocol.METHOD_NOT_FOUND


def test_unknown_tool_is_a_protocol_error(lake):
    reply = _call(lake, "tools/call", {"name": "nope", "arguments": {}})
    assert reply["error"]["code"] == protocol.INVALID_PARAMS


def test_tool_failure_comes_back_as_a_readable_result(lake):
    """Not a JSON-RPC error: the model has to see 'as_of is required' to retry."""
    reply = _call(lake, "tools/call", {"name": "query_fundamentals", "arguments": {}})
    assert "error" not in reply
    assert reply["result"]["isError"] is True
    body = json.loads(reply["result"]["content"][0]["text"])
    assert "as_of is required" in body["error"]


def test_unexpected_argument_does_not_kill_the_session(lake):
    reply = _call(lake, "tools/call", {"name": "describe_lake", "arguments": {"nope": 1}})
    assert reply["result"]["isError"] is True
    assert _call(lake, "tools/list")["result"]["tools"]


def test_serve_stdio_round_trip(lake, tmp_path):
    import io

    stdin = io.StringIO(
        "\n".join(
            [
                json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}),
                json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
                "",
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "tools/call",
                        "params": {"name": "resolve_symbol", "arguments": {"query": "茅台"}},
                    }
                ),
            ]
        )
        + "\n"
    )
    stdout = io.StringIO()
    protocol.serve_stdio(lake, stdin=stdin, stdout=stdout)

    replies = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert [r["id"] for r in replies] == [1, 2]
    body = json.loads(replies[1]["result"]["content"][0]["text"])
    assert body["rows"][0][0] == "600519.SH"


def test_malformed_frame_gets_a_parse_error_and_the_loop_continues(lake):
    import io

    stdout = io.StringIO()
    protocol.serve_stdio(
        lake,
        stdin=io.StringIO('{"broken\n{"jsonrpc":"2.0","id":7,"method":"ping"}\n'),
        stdout=stdout,
    )
    replies = [json.loads(line) for line in stdout.getvalue().splitlines()]
    assert replies[0]["error"]["code"] == protocol.PARSE_ERROR
    assert replies[1]["id"] == 7


# --- the CLI entry point ---------------------------------------------------


def test_mcp_refuses_a_lake_with_nothing_in_it(tmp_path):
    """An MCP client spawns this from a directory of its own choosing. A
    relative data.root then resolves somewhere else, every tool answers "no
    parquet data", and the agent reports the data as missing — true of that
    path, false of the user's lake."""
    from click.testing import CliRunner

    from cn_market_lake.cli.main import cli
    from cn_market_lake.config.bootstrap import path_for_toml

    cfg_path = tmp_path / "empty.toml"
    cfg_path.write_text(f'[data]\nroot = "{path_for_toml(tmp_path / "nowhere")}"\n')

    result = CliRunner().invoke(cli, ["mcp", "--config", str(cfg_path)])
    assert result.exit_code != 0
    assert "No curated data" in result.output
    assert "resolved against the working directory" in result.output


def test_mcp_serves_a_populated_lake(lake, tmp_path):
    from click.testing import CliRunner

    from cn_market_lake.cli.main import cli
    from cn_market_lake.config.bootstrap import path_for_toml

    cfg_path = tmp_path / "lake.toml"
    cfg_path.write_text(f'[data]\nroot = "{path_for_toml(lake.data_root)}"\n')

    result = CliRunner().invoke(
        cli,
        ["mcp", "--config", str(cfg_path)],
        input=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ping"}) + "\n",
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.splitlines()[0])["result"] == {}


# --- live mode: no lake, no writes ------------------------------------------


@pytest.fixture
def empty(tmp_path):
    return Config(data_root=tmp_path / "nothing")


def _fake_live(monkeypatch):
    from cn_market_lake.mcp_server import live

    monkeypatch.setattr(
        live,
        "instruments",
        lambda cfg: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "name": ["贵州茅台"],
                "exchange": ["SH"],
                "asset_type": ["stock"],
            }
        ),
    )
    monkeypatch.setattr(
        live,
        "daily_bars",
        lambda cfg, **k: pl.DataFrame(
            {"symbol": ["600519.SH"], "trade_date": [date(2026, 7, 31)], "close": [1350.6]}
        ),
    )


def test_live_is_off_unless_asked(empty):
    """A lake user whose lake is broken must get 'no parquet data' and go fix
    it, not a quietly different answer from a vendor."""
    with pytest.raises(tools.ToolError, match="no parquet data"):
        tools.resolve_symbol(empty, query="茅台")


def test_live_serves_symbol_lookup(empty, monkeypatch):
    _fake_live(monkeypatch)
    empty._mcp_live = True
    payload = tools.resolve_symbol(empty, query="茅台")
    assert payload["origin"] == "live"
    assert payload["rows"][0][0] == "600519.SH"
    assert "NOT stored" in payload["warning"]


def test_live_bars_are_labelled_and_warned(empty, monkeypatch):
    _fake_live(monkeypatch)
    empty._mcp_live = True
    payload = tools.query_bars(empty, symbols=["600519.SH"])
    assert payload["origin"] == "live"
    assert "No adjustment" in payload["warning"]


def test_lake_rows_are_labelled_too(lake):
    """Both origins are stated, so 'origin' is never absent-and-therefore-lake."""
    assert tools.query_bars(lake, symbols=["600519.SH"])["origin"] == "lake"
    assert tools.resolve_symbol(lake, query="茅台")["origin"] == "lake"


def test_live_refuses_adjustment(empty, monkeypatch):
    """A vendor's bar has no factor attached; serving it as adjusted would be
    wrong in a way the numbers do not show."""
    _fake_live(monkeypatch)
    empty._mcp_live = True
    with pytest.raises(tools.ToolError, match="cannot honour adjust"):
        tools.query_bars(empty, symbols=["600519.SH"], adjust="hfq")


def test_live_refuses_universe(empty, monkeypatch):
    _fake_live(monkeypatch)
    empty._mcp_live = True
    with pytest.raises(tools.ToolError, match="cannot honour universe"):
        tools.query_bars(empty, symbols=["600519.SH"], universe="all_a")


def test_live_refuses_point_in_time(empty, monkeypatch):
    """There is no honest as_of against a source that returns today's view."""
    empty._mcp_live = True
    with pytest.raises(tools.ToolError, match="look-ahead"):
        tools.query_fundamentals(empty, as_of="2018-04-30")


def test_live_refuses_sql(empty):
    empty._mcp_live = True
    with pytest.raises(tools.ToolError, match="parquet on disk"):
        tools.run_sql(empty, sql="SELECT 1")


def test_live_refuses_other_datasets_by_name(empty):
    empty._mcp_live = True
    with pytest.raises(tools.ToolError, match="cml init"):
        tools.query_dataset(empty, dataset="fund_flow")


def test_live_will_not_sweep_the_market(empty):
    """An agent looping without symbols is how a user earns a rate-limit ban
    for a question they did not ask."""
    empty._mcp_live = True
    with pytest.raises(tools.ToolError, match="explicit `symbols`"):
        tools.query_bars(empty)


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"symbols": [f"{i:06d}.SH" for i in range(80)]}, "at most"),
        ({"symbols": ["600519.SH"], "start": "2016-01-01", "end": "2026-01-01"}, "at most"),
    ],
)
def test_live_caps_each_call(empty, kwargs, match):
    empty._mcp_live = True
    with pytest.raises(tools.ToolError, match=match):
        tools.query_bars(empty, **kwargs)


def test_live_writes_nothing(empty, monkeypatch):
    _fake_live(monkeypatch)
    empty._mcp_live = True
    tools.query_bars(empty, symbols=["600519.SH"])
    tools.resolve_symbol(empty, query="茅台")
    assert not list(empty.data_root.rglob("*.parquet"))


def test_describe_lake_announces_live_mode(empty, monkeypatch):
    """It changes how every other line of the contract should be read, so it
    goes first."""
    empty._mcp_live = True
    payload = tools.describe_lake(empty)
    assert payload["live_mode"] is True
    assert "LIVE MODE IS ON" in payload["contract"][0]
    assert (
        tools.describe_lake(lake_config := Config(data_root=empty.data_root))["live_mode"] is False
    )
    assert lake_config is not None
