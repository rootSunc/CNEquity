"""MCP surface: the tool layer and the stdio protocol loop."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.mcp_server import protocol, tools
from cnequity.mcp_server.catalog import DESCRIPTORS, HANDLERS
from cnequity.run_scope import set_run_flags


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
            "source": ["test", "test"],
            "data_version": ["v1", "v1"],
            "fetched_at": [
                datetime(2024, 4, 28, tzinfo=timezone.utc),
                datetime(2024, 5, 15, tzinfo=timezone.utc),
            ],
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


def test_mcp_all_a_query_fails_closed_without_historical_st_evidence(lake):
    """MCP must not bypass the strict universe contract through its wrapper."""
    status_dir = lake.curated_root / "trading_status" / "trade_date=2024-06-27"
    status_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ"],
            "trade_date": [date(2024, 6, 27)] * 2,
            "is_trading": [True, True],
            "status": ["normal", "normal"],
            **_prov("eastmoney"),
        }
    ).write_parquet(status_dir / "part-0.parquet")

    with pytest.raises(tools.ToolError, match="historical ST evidence"):
        tools.query_bars(lake, universe="all_a")


def test_mcp_rejects_universe_for_intraday_bars(lake):
    with pytest.raises(tools.ToolError, match="universe filter applies to daily_bars only"):
        tools.query_bars(lake, dataset="minute_bars", universe="all_a")


def test_mcp_catalog_exposes_explicit_sh_sz_universe():
    query_bars = next(tool for tool in DESCRIPTORS if tool["name"] == "query_bars")
    assert query_bars["inputSchema"]["properties"]["universe"]["enum"] == [
        "all_a",
        "all_a_sh_sz",
    ]


def test_query_bars_echoes_universe_semantics(lake):
    payload = tools.query_bars(lake, symbols=["600519.SH"], universe=None)

    assert payload["universe"] is None


# --- query_fundamentals ----------------------------------------------------


def test_fundamentals_require_as_of(lake):
    with pytest.raises(tools.ToolError, match="point-in-time"):
        tools.query_fundamentals(lake, symbols=["600519.SH"])


def test_fundamentals_apply_the_pit_cutoff(lake):
    """revenue was announced 2024-05-15; asking as of April must not see it."""
    payload = tools.query_fundamentals(lake, as_of="2024-04-30")
    items = {row[payload["columns"].index("item_code")] for row in payload["rows"]}
    assert items == {"roe"}


def _backfilled_vintage(lake) -> None:
    """A 2023Q4 figure announced before as_of but fetched by a 2026 backfill."""
    part = lake.data_root / "curated" / "financial_statement_items" / "report_period=2023Q4"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "report_period": ["2023Q4"],
            "statement_type": ["income"],
            "item_code": ["net_profit"],
            "item_value": [7.0e10],
            "announce_date": [date(2024, 3, 30)],
            "source": ["test"],
            "data_version": ["v1"],
            "fetched_at": [datetime(2026, 9, 1, tzinfo=timezone.utc)],
        }
    ).write_parquet(part / "part-0.parquet")


def test_fundamentals_return_backfilled_history_flagged_inexact(lake):
    _backfilled_vintage(lake)
    payload = tools.query_fundamentals(lake, as_of="2024-04-30")
    cols = payload["columns"]
    exact = {
        row[cols.index("item_code")]: row[cols.index("pit_is_exact")] for row in payload["rows"]
    }
    assert exact == {"roe": True, "net_profit": False}
    assert payload["pit_mode"] == "best_effort"
    assert payload["exact_rows"] == 1


def test_an_empty_strict_answer_points_at_the_backfilled_rows(lake):
    _backfilled_vintage(lake)
    payload = tools.query_fundamentals(lake, as_of="2024-04-01", pit_mode="strict")
    assert payload["total"] == 0
    assert "1 row(s)" in payload["note"]
    assert "best_effort" in payload["note"]


def test_fundamentals_reject_an_unknown_pit_mode(lake):
    with pytest.raises(tools.ToolError, match="pit_mode"):
        tools.query_fundamentals(lake, as_of="2024-04-30", pit_mode="compatibility")


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


def test_run_sql_reads_a_published_generation(lake):
    """Views over a committed dataset point into meta/revisions/data; the
    sandbox has to allow that directory or every such query is refused."""
    from cnequity.storage.revisions import RevisionStore

    store = RevisionStore(lake.meta_root, lake.curated_root)
    files = sorted((lake.curated_root / "daily_bars").rglob("*.parquet"))
    assert store.commit(
        "daily_bars",
        run_id="r1",
        changed_files=files,
        schema_version=1,
        contract_fingerprint="test",
    )
    payload = tools.run_sql(lake, sql="SELECT count(*) AS n FROM daily_bars")
    assert payload["rows"][0][0] > 0
    with pytest.raises(tools.ToolError):
        tools.run_sql(
            lake, sql=f"SELECT * FROM read_text('{(lake.meta_root / 'manifest.db').as_posix()}')"
        )


def _publish_daily_bars(lake) -> None:
    from cnequity.storage.revisions import RevisionStore

    store = RevisionStore(lake.meta_root, lake.curated_root)
    files = sorted((lake.curated_root / "daily_bars").rglob("*.parquet"))
    assert store.commit(
        "daily_bars",
        run_id="r1",
        changed_files=files,
        schema_version=1,
        contract_fingerprint="test",
    )


def test_run_sql_cannot_read_an_unpublished_candidate(lake):
    """Once a dataset publishes generations, ``curated/<ds>`` is the mutable
    tree compact writes its candidate into. A raw read_parquet there would see
    rows the publication gate has not passed (or has quarantined)."""
    _publish_daily_bars(lake)
    # Same schema as the published rows, so only the sandbox can refuse it.
    published = lake.curated_root / "daily_bars" / "trade_date=2024-06-27" / "part-0.parquet"
    candidate = lake.curated_root / "daily_bars" / "trade_date=2024-06-28"
    candidate.mkdir(parents=True)
    pl.read_parquet(published).with_columns(trade_date=pl.lit(date(2024, 6, 28))).write_parquet(
        candidate / "part-0.parquet"
    )

    view = tools.run_sql(lake, sql="SELECT count(*) AS n FROM daily_bars")
    assert view["rows"] == [[2]]
    raw = (lake.curated_root / "daily_bars").as_posix()
    with pytest.raises(tools.ToolError):
        tools.run_sql(lake, sql=f"SELECT count(*) FROM read_parquet('{raw}/**/*.parquet')")


def test_run_sql_still_reads_a_legacy_dataset_directly(lake):
    """A pointer-less dataset has no generation yet; its logical root is what
    the view reads, so it stays inside the sandbox."""
    raw = (lake.curated_root / "daily_bars").as_posix()
    payload = tools.run_sql(
        lake, sql=f"SELECT count(*) AS n FROM read_parquet('{raw}/**/*.parquet')"
    )
    assert payload["rows"] == [[2]]


def test_run_sql_sandbox_is_not_a_bare_prefix(lake):
    """Allowing ``curated/daily_bars`` must not also allow ``curated/daily_bars_x``."""
    sibling = lake.curated_root / "daily_bars_x"
    sibling.mkdir(parents=True)
    pl.DataFrame({"secret": ["must not be exposed"]}).write_parquet(sibling / "part-0.parquet")

    with pytest.raises(tools.ToolError):
        tools.run_sql(lake, sql=f"SELECT * FROM read_parquet('{sibling.as_posix()}/*.parquet')")


def test_run_sql_pages_without_materialising_the_result(lake):
    payload = tools.run_sql(
        lake, sql="SELECT range AS n FROM range(5000) ORDER BY n DESC", limit=10
    )
    assert payload["returned"] == 10
    assert payload["total"] == 5000
    assert payload["truncated"] is True
    assert [row[0] for row in payload["rows"]] == list(range(4999, 4989, -1))


def test_run_sql_small_result_is_not_truncated(lake):
    payload = tools.run_sql(lake, sql="SELECT 1 AS n;")
    assert payload["rows"] == [[1]]
    assert payload["total"] == 1
    assert payload["truncated"] is False


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


@pytest.mark.parametrize("transport", ["stdio", "http"])
@pytest.mark.parametrize("invalid", [None, 7, True, "invalid", []])
def test_invalid_batch_member_does_not_end_the_session(empty, transport, invalid):
    import io

    batch = [
        invalid,
        {"jsonrpc": "2.0", "id": 1, "method": "ping"},
        {"jsonrpc": "2.0", "method": "notifications/initialized"},
    ]
    following = {"jsonrpc": "2.0", "id": 2, "method": "ping"}
    if transport == "stdio":
        stdout = io.StringIO()
        protocol.serve_stdio(
            empty,
            stdin=io.StringIO(json.dumps(batch) + "\n" + json.dumps(following) + "\n"),
            stdout=stdout,
        )
        replies, next_reply = map(json.loads, stdout.getvalue().splitlines())
    else:
        client = _http_client(empty)
        response = client.post("/mcp", json=batch)
        assert response.status_code == 200
        replies = response.json()
        next_reply = client.post("/mcp", json=following).json()

    assert len(replies) == 2
    assert replies[0]["id"] is None
    assert replies[0]["error"]["code"] == protocol.INVALID_REQUEST
    assert replies[1] == {"jsonrpc": "2.0", "id": 1, "result": {}}
    assert next_reply == {"jsonrpc": "2.0", "id": 2, "result": {}}


@pytest.mark.parametrize("transport", ["stdio", "http"])
def test_empty_batch_is_an_invalid_request(empty, transport):
    import io

    if transport == "stdio":
        stdout = io.StringIO()
        protocol.serve_stdio(empty, stdin=io.StringIO("[]\n"), stdout=stdout)
        reply = json.loads(stdout.getvalue())
    else:
        response = _http_client(empty).post("/mcp", json=[])
        assert response.status_code == 200
        reply = response.json()
    assert reply["id"] is None
    assert reply["error"]["code"] == protocol.INVALID_REQUEST


def test_process_streams_speak_utf8_under_a_gbk_console(lake, monkeypatch):
    """Chinese Windows hands the process GBK pipes with CRLF line ends; the
    client still writes and reads UTF-8 with bare LF."""
    import io

    def call(index, query):
        return json.dumps(
            {
                "jsonrpc": "2.0",
                "id": index,
                "method": "tools/call",
                "params": {"name": "resolve_symbol", "arguments": {"query": query}},
            },
            ensure_ascii=False,
        ).encode("utf-8")

    wire = b"\n".join([call(1, "茅台"), b'{"bad": "\xff"}', call(2, "600519")]) + b"\n"
    sent = io.BytesIO()
    monkeypatch.setattr(protocol.sys, "stdin", io.TextIOWrapper(io.BytesIO(wire), encoding="gbk"))
    monkeypatch.setattr(
        protocol.sys, "stdout", io.TextIOWrapper(sent, encoding="gbk", newline="\r\n")
    )
    protocol.serve_stdio(lake)

    raw = sent.getvalue()
    assert b"\r" not in raw
    replies = [json.loads(line) for line in raw.decode("utf-8").splitlines()]
    assert [reply["id"] for reply in replies] == [1, None, 2]
    assert replies[1]["error"]["code"] == protocol.PARSE_ERROR
    for reply in (replies[0], replies[2]):
        body = json.loads(reply["result"]["content"][0]["text"])
        assert body["rows"][0][:2] == ["600519.SH", "贵州茅台"]


# --- the CLI entry point ---------------------------------------------------


@pytest.mark.usefixtures("chinese_output")
def test_mcp_refuses_a_lake_with_nothing_in_it(tmp_path):
    """An MCP client spawns this from a directory of its own choosing. A
    relative data.root then resolves somewhere else, every tool answers "no
    parquet data", and the agent reports the data as missing — true of that
    path, false of the user's lake."""
    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.config.bootstrap import path_for_toml

    cfg_path = tmp_path / "empty.toml"
    cfg_path.write_text(f'[data]\nroot = "{path_for_toml(tmp_path / "nowhere")}"\n')

    result = CliRunner().invoke(cli, ["mcp", "--config", str(cfg_path)])
    assert result.exit_code != 0
    assert "没有任何 curated 数据" in result.output
    assert "相对客户端拉起这个进程时的工作目录解析" in result.output


def test_mcp_serves_a_populated_lake(lake, tmp_path):
    from click.testing import CliRunner

    from cnequity.cli.main import cli
    from cnequity.config.bootstrap import path_for_toml

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
    from cnequity.mcp_server import live

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
    set_run_flags(empty, mcp_live=True)
    payload = tools.resolve_symbol(empty, query="茅台")
    assert payload["origin"] == "live"
    assert payload["rows"][0][0] == "600519.SH"
    assert "NOT stored" in payload["warning"]


def test_live_bars_are_labelled_and_warned(empty, monkeypatch):
    _fake_live(monkeypatch)
    set_run_flags(empty, mcp_live=True)
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
    set_run_flags(empty, mcp_live=True)
    with pytest.raises(tools.ToolError, match="cannot honour adjust"):
        tools.query_bars(empty, symbols=["600519.SH"], adjust="hfq")


def test_live_refuses_universe(empty, monkeypatch):
    _fake_live(monkeypatch)
    set_run_flags(empty, mcp_live=True)
    with pytest.raises(tools.ToolError, match="cannot honour universe"):
        tools.query_bars(empty, symbols=["600519.SH"], universe="all_a")


def test_live_refuses_point_in_time(empty, monkeypatch):
    """There is no honest as_of against a source that returns today's view."""
    set_run_flags(empty, mcp_live=True)
    with pytest.raises(tools.ToolError, match="look-ahead"):
        tools.query_fundamentals(empty, as_of="2018-04-30")


def test_live_refuses_sql(empty):
    set_run_flags(empty, mcp_live=True)
    with pytest.raises(tools.ToolError, match="parquet on disk"):
        tools.run_sql(empty, sql="SELECT 1")


def test_live_refuses_other_datasets_by_name(empty):
    set_run_flags(empty, mcp_live=True)
    with pytest.raises(tools.ToolError, match="cne init"):
        tools.query_dataset(empty, dataset="fund_flow")


def test_live_will_not_sweep_the_market(empty):
    """An agent looping without symbols is how a user earns a rate-limit ban
    for a question they did not ask."""
    set_run_flags(empty, mcp_live=True)
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
    set_run_flags(empty, mcp_live=True)
    with pytest.raises(tools.ToolError, match=match):
        tools.query_bars(empty, **kwargs)


def test_live_writes_nothing(empty, monkeypatch):
    _fake_live(monkeypatch)
    set_run_flags(empty, mcp_live=True)
    tools.query_bars(empty, symbols=["600519.SH"])
    tools.resolve_symbol(empty, query="茅台")
    assert not list(empty.data_root.rglob("*.parquet"))


def test_describe_lake_announces_live_mode(empty, monkeypatch):
    """It changes how every other line of the contract should be read, so it
    goes first."""
    set_run_flags(empty, mcp_live=True)
    payload = tools.describe_lake(empty)
    assert payload["live_mode"] is True
    assert "LIVE MODE IS ON" in payload["contract"][0]
    assert (
        tools.describe_lake(lake_config := Config(data_root=empty.data_root))["live_mode"] is False
    )
    assert lake_config is not None


# --- Streamable HTTP ---------------------------------------------------------


def _http_client(lake, token=None):
    from starlette.testclient import TestClient

    from cnequity.mcp_server.http import create_app

    return TestClient(create_app(lake, token=token), base_url="http://127.0.0.1:8788")


def _rpc(method, params=None, id_=1):
    return {"jsonrpc": "2.0", "id": id_, "method": method, "params": params or {}}


def test_http_round_trip_matches_stdio(lake):
    client = _http_client(lake)
    assert client.post("/mcp", json=_rpc("initialize")).json()["result"]["serverInfo"]
    reply = client.post(
        "/mcp",
        json=_rpc("tools/call", {"name": "resolve_symbol", "arguments": {"query": "茅台"}}, 2),
    ).json()
    body = json.loads(reply["result"]["content"][0]["text"])
    assert body["rows"][0][0] == "600519.SH"


def test_http_notification_is_accepted_without_a_body(lake):
    resp = _http_client(lake).post(
        "/mcp", json={"jsonrpc": "2.0", "method": "notifications/initialized"}
    )
    assert resp.status_code == 202
    assert resp.content == b""


def test_http_batch_and_parse_error(lake):
    client = _http_client(lake)
    replies = client.post("/mcp", json=[_rpc("ping", id_=1), _rpc("ping", id_=2)]).json()
    assert [r["id"] for r in replies] == [1, 2]
    bad = client.post("/mcp", content=b"{broken", headers={"content-type": "application/json"})
    assert bad.status_code == 400
    assert bad.json()["error"]["code"] == protocol.PARSE_ERROR


def test_http_offers_no_stream(lake):
    resp = _http_client(lake).get("/mcp")
    assert resp.status_code == 405
    assert resp.headers["allow"] == "POST"


@pytest.mark.parametrize(
    "headers",
    [
        {"host": "lake.example.com"},
        {"x-forwarded-for": "203.0.113.9"},
        {"cf-connecting-ip": "203.0.113.9"},
        {"origin": "https://evil.example"},
    ],
)
def test_http_without_token_accepts_only_plain_local_requests(lake, headers):
    """A tunnel may rewrite Host to localhost, and a browser page may POST
    text/plain without a preflight — neither may reach the tools tokenless."""
    resp = _http_client(lake).post("/mcp", json=_rpc("ping"), headers=headers)
    assert resp.status_code == 403


def test_http_local_origin_is_fine_without_token(lake):
    resp = _http_client(lake).post(
        "/mcp", json=_rpc("ping"), headers={"origin": "http://localhost:6274"}
    )
    assert resp.status_code == 200


def test_http_token_is_required_even_on_a_loopback_host(lake):
    client = _http_client(lake, token="s3cret")
    assert client.post("/mcp", json=_rpc("ping")).status_code == 403
    assert client.post("/mcp/wrong", json=_rpc("ping")).status_code == 403
    assert client.post("/mcp/s3cret", json=_rpc("ping")).status_code == 200
    assert client.post("/mcp?token=s3cret", json=_rpc("ping")).status_code == 200
    ok = client.post(
        "/mcp",
        json=_rpc("ping"),
        headers={"authorization": "Bearer s3cret", "host": "abc.trycloudflare.com"},
    )
    assert ok.status_code == 200


def test_every_tool_is_marked_read_only():
    assert all(d["annotations"] == {"readOnlyHint": True} for d in DESCRIPTORS)


@pytest.mark.usefixtures("chinese_output")
def test_mcp_http_options_need_http():
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    result = CliRunner().invoke(cli, ["mcp", "--token", "x"])
    assert result.exit_code != 0
    assert "--token 只用于 --http" in result.output


def test_mcp_http_refuses_a_public_bind_without_token():
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    result = CliRunner().invoke(cli, ["mcp", "--http", "--host", "0.0.0.0"])
    assert result.exit_code != 0
    assert "--token" in result.output


def test_table_rows_send_non_finite_floats_as_null():
    df = pl.DataFrame({"pe": [float("nan"), float("inf"), float("-inf"), 12.5]})
    payload = tools._frame_payload(df, limit=10)
    assert payload["rows"] == [[None], [None], [None], [12.5]]
    json.dumps(payload, allow_nan=False)  # strict JSON parsers must accept it


# --- query_dataset read budget -----------------------------------------------
#
# load() canonicalises the whole selection before a page is cut, so an
# unwindowed call held 2.6-4.8 GB on a real lake to return 200 rows, and
# trade_ticks would not have fit at all. Oversized reads are refused up front.


def _fund_flow_days(lake, days: list[date]) -> list[int]:
    sizes = []
    for day in days:
        part = lake.curated_root / "fund_flow" / f"trade_date={day}"
        part.mkdir(parents=True)
        path = part / "part-0.parquet"
        pl.DataFrame(
            {
                "symbol": ["600519.SH", "000001.SZ"],
                "trade_date": [day, day],
                "main_net_inflow": [1.0, 2.0],
                "super_large_net_inflow": [1.0, 2.0],
                "large_net_inflow": [1.0, 2.0],
                "medium_net_inflow": [1.0, 2.0],
                "small_net_inflow": [1.0, 2.0],
                **_prov("eastmoney"),
            }
        ).write_parquet(path)
        sizes.append(pl.read_parquet(path).height)
    return sizes


_FLOW_DAYS = [date(2024, 6, 24), date(2024, 6, 25), date(2024, 6, 26), date(2024, 6, 27)]


def test_query_dataset_refuses_an_oversized_read_before_loading(lake, monkeypatch):
    sizes = _fund_flow_days(lake, _FLOW_DAYS)
    # Room for the newest two days, not all four.
    monkeypatch.setattr(tools, "QUERY_SCAN_BUDGET_ROWS", sizes[-1] + sizes[-2])

    def _must_not_load(*_args, **_kwargs):
        raise AssertionError("an oversized read must be refused before load()")

    monkeypatch.setattr(tools, "_read", _must_not_load)
    with pytest.raises(tools.ToolError) as exc:
        tools.query_dataset(lake, dataset="fund_flow")
    message = str(exc.value)
    assert "budget" in message
    assert "It covers 2024-06-24..2024-06-27" in message
    assert "start=2024-06-26 end=2024-06-27 fits" in message
    assert "run_sql" in message


def test_query_dataset_serves_the_window_it_suggested(lake, monkeypatch):
    sizes = _fund_flow_days(lake, _FLOW_DAYS)
    monkeypatch.setattr(tools, "QUERY_SCAN_BUDGET_ROWS", sizes[-1] + sizes[-2])
    payload = tools.query_dataset(lake, dataset="fund_flow", start="2024-06-26", end="2024-06-27")
    assert payload["total"] == 4
    assert {row[1] for row in payload["rows"]} == {"2024-06-26", "2024-06-27"}


def test_query_dataset_suggestion_stays_inside_the_requested_window(lake, monkeypatch):
    sizes = _fund_flow_days(lake, _FLOW_DAYS)
    monkeypatch.setattr(tools, "QUERY_SCAN_BUDGET_ROWS", sizes[0] + sizes[1])
    with pytest.raises(tools.ToolError, match="start=2024-06-25 end=2024-06-26 fits"):
        tools.query_dataset(lake, dataset="fund_flow", start="2024-06-24", end="2024-06-26")


def test_query_dataset_short_symbol_lists_are_not_budgeted(lake, monkeypatch):
    _fund_flow_days(lake, _FLOW_DAYS)
    monkeypatch.setattr(tools, "QUERY_SCAN_BUDGET_ROWS", 1)
    payload = tools.query_dataset(lake, dataset="fund_flow", symbols="600519.SH")
    assert payload["total"] == 4
    many = [f"{n:06d}.SZ" for n in range(tools._SYMBOL_SCAN_EXEMPT)] + ["600519.SH"]
    with pytest.raises(tools.ToolError, match="budget"):
        tools.query_dataset(lake, dataset="fund_flow", symbols=many)


def test_query_dataset_leaves_bad_dates_to_the_reader(lake, monkeypatch):
    _fund_flow_days(lake, _FLOW_DAYS)
    monkeypatch.setattr(tools, "QUERY_SCAN_BUDGET_ROWS", 1)
    with pytest.raises(ValueError, match="isoformat|Invalid"):
        tools.query_dataset(lake, dataset="fund_flow", start="last tuesday")


def test_read_budget_prunes_derived_datasets_by_partition(tmp_path, monkeypatch):
    """The legacy PARTITION_COLS table has no derived datasets; sizing a window
    through it read every file and refused windowed option_greeks calls."""
    from cnequity.domain.datasets import DATASETS, PARTITION_COLS

    derived = [
        name
        for name, spec in DATASETS.items()
        if spec.layer == "derived"
        and spec.partition_col
        and spec.partition_granularity == "day"
        and name not in PARTITION_COLS
    ]
    assert derived, "no derived day-partitioned dataset outside PARTITION_COLS"
    name = derived[0]
    config = Config(data_root=tmp_path / "data")
    for day in ("2024-06-26", "2024-06-27"):
        part = config.derived_root / name / f"{DATASETS[name].partition_col}={day}"
        part.mkdir(parents=True)
        pl.DataFrame({"n": range(1000)}).write_parquet(part / "part-0.parquet")
    monkeypatch.setattr(tools, "QUERY_SCAN_BUDGET_ROWS", 1500)
    tools._refuse_oversized_read(config, name, start="2024-06-27", end=None, symbols=None)
    with pytest.raises(tools.ToolError, match="budget"):
        tools._refuse_oversized_read(config, name, start=None, end=None, symbols=None)
