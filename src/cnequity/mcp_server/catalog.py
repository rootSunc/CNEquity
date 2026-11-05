"""Tool descriptors: what the agent reads when deciding which tool to call.

The descriptions are load-bearing. A tool list is the only documentation an
agent gets, and it is read *before* any data, so anything not said here is
something it will assume. Two things are therefore stated in the descriptions
rather than left to `describe_lake`: that bar prices are unadjusted unless
asked, and that `as_of` is what keeps fundamentals free of look-ahead. Both
produce confident wrong answers when missed, and an agent that skips the
describe step would otherwise never see them.

Schemas are hand-written JSON Schema rather than generated from the signatures.
The generated version would be accurate about types and silent about meaning,
and meaning is the whole reason an agent picks the right argument.
"""

from __future__ import annotations

from cnequity.mcp_server import tools

_LIMIT = {
    "type": "integer",
    "description": f"Max rows to return (default {tools.DEFAULT_LIMIT}, cap {tools.MAX_LIMIT}). "
    "Responses always report the true total and set `truncated`.",
}
_OFFSET = {"type": "integer", "description": "Rows to skip, for paging with `limit`."}
_SYMBOLS = {
    "type": "array",
    "items": {"type": "string"},
    "description": "Symbols as '<code>.<EX>', e.g. ['600519.SH', '000001.SZ']. "
    "Omit for every symbol in range. Use resolve_symbol to turn a company name into a code.",
}
_START = {"type": "string", "description": "Inclusive start date, YYYY-MM-DD."}
_END = {"type": "string", "description": "Inclusive end date, YYYY-MM-DD."}
_PROVENANCE = {
    "type": "boolean",
    "description": "Include per-row source/data_version/fetched_at. Off by default — "
    "the response lists the distinct sources instead, at a fraction of the size.",
}


TOOLS: list[dict] = [
    {
        "name": "describe_lake",
        "description": (
            "START HERE. Returns what this lake actually holds — every dataset with "
            "its coverage window, history mode and freshness — plus the query "
            "contract that makes an answer correct (adjustment, point-in-time, "
            "which series have no real history). Cheap, reads no data. Call it "
            "before answering anything factual about A-share data, and use its "
            "coverage_start/coverage_end to tell 'this lake has no such data' apart "
            "from 'this did not happen'. It also reports `live_mode` — when that is "
            "on, some answers come from the vendor rather than the lake and say so. "
            "Then pick a tool: resolve_symbol for names, query_bars for prices, "
            "query_fundamentals for financials, query_dataset for everything else, "
            "run_sql for aggregation."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "include_empty": {
                    "type": "boolean",
                    "description": "Also list registered datasets with no data yet. Use it to tell "
                    "an empty dataset from an unknown name.",
                }
            },
        },
        "handler": tools.describe_lake,
    },
    {
        "name": "resolve_symbol",
        "description": (
            "Look up a symbol by company name or partial code — '茅台', 'maotai', "
            "'600519' all resolve to 600519.SH. Do this before any query when the "
            "user named a company rather than a code: a guessed suffix returns an "
            "empty result rather than an error. Delisted names are included and "
            "flagged with delist_date. Returns the canonical symbol, name and "
            "delist_date; several rows mean the fragment is ambiguous, so pick by "
            "name before querying."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Company-name fragment, or a code prefix with or without "
                    "the exchange suffix (.SH/.SZ/.BJ). The match returns the canonical "
                    "suffixed symbol to use in later calls.",
                },
                "limit": _LIMIT,
            },
            "required": ["query"],
        },
        "handler": tools.resolve_symbol,
    },
    {
        "name": "query_bars",
        "description": (
            "Daily or intraday price bars. PRICES ARE STORED UNADJUSTED: pass "
            "adjust='hfq' for any comparison across time (returns, charts, "
            "drawdowns) or a split reads as a crash. Use adjust='qfq' when the "
            "levels must match what a quote screen shows today. Pass "
            "universe='all_a' for cross-sectional work to drop names that were "
            "not tradable that day. datasets: daily_bars, index_bars, "
            "minute_bars (1m), minute_bars_5m (5m), and minute_bars_15m / _30m / "
            "_60m when the user has derived them (each bar's `resampled_from` says "
            "whether 1m or 5m built it). Every response carries "
            "`origin`: 'lake' is stored and validated; 'live' was fetched just now "
            "and cannot be adjusted or universe-filtered — read its warning before "
            "using the numbers for anything but a current quote. Not for financial "
            "statements (query_fundamentals), other datasets such as fund flow "
            "(query_dataset), or ranking and aggregating across many symbols "
            "(run_sql)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "dataset": {
                    "type": "string",
                    "enum": list(tools.BAR_DATASETS),
                    "description": "Default daily_bars.",
                },
                "symbols": _SYMBOLS,
                "start": _START,
                "end": _END,
                "adjust": {
                    "type": "string",
                    "enum": ["qfq", "hfq", "total_return"],
                    "description": "hfq = back-adjusted, the correct choice for return series. "
                    "qfq = forward-adjusted to today's price level. total_return "
                    "(daily_bars only) = hfq with fund cash distributions reinvested; "
                    "stocks read as hfq, whose factor already reinvests dividends. Omit "
                    "only when you specifically want raw traded prices for a single day.",
                },
                "universe": {
                    "type": "string",
                    "enum": ["all_a", "all_a_sh_sz"],
                    "description": "Drop rows for names unlisted/delisted that day, and "
                    "ST/suspended ones where trading_status covers it. `all_a` includes "
                    "SH/SZ/BJ; `all_a_sh_sz` is an explicit SH/SZ-only research subset "
                    "that excludes BJ. The lake path also requires versioned historical "
                    "ST evidence; incomplete research fails instead of treating unknown "
                    "names as normal. daily_bars only.",
                },
                "limit": _LIMIT,
                "offset": _OFFSET,
                "include_provenance": _PROVENANCE,
            },
        },
        "handler": tools.query_bars,
    },
    {
        "name": "query_fundamentals",
        "description": (
            "Financial statement items, point-in-time. `as_of` is REQUIRED and is "
            "what prevents look-ahead: only figures publicly announced on or "
            "before that date come back, and a restated figure returns the "
            "vintage that was current then. For a present-day view pass today's "
            "date; for a historical study pass the date being studied, never today. "
            "Results are paged with limit/offset. For prices use query_bars; for "
            "valuation ratios read valuation_metrics through query_dataset. Set "
            "pit_mode='strict' only to audit what the lake itself held on that date."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "as_of": {
                    "type": "string",
                    "description": "YYYY-MM-DD knowledge cutoff, applied to announce_date.",
                },
                "symbols": _SYMBOLS,
                "items": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "item_code filter, e.g. ['roe', 'net_profit'].",
                },
                "all_vintages": {
                    "type": "boolean",
                    "description": "Return every vintage announced by as_of instead of only "
                    "the one current then. For studying restatements; it double-counts "
                    "a fact in any cross-sectional screen.",
                },
                "pit_mode": {
                    "type": "string",
                    "enum": ["best_effort", "strict"],
                    "description": "best_effort (default) also returns rows a later "
                    "backfill recorded, flagged pit_is_exact=false; strict keeps only "
                    "rows the lake held by as_of, which is none for a backfilled history.",
                },
                "limit": _LIMIT,
                "offset": _OFFSET,
            },
            "required": ["as_of"],
        },
        "handler": tools.query_fundamentals,
    },
    {
        "name": "query_dataset",
        "description": (
            "Read any other dataset by date window and symbol — fund_flow, "
            "margin_trading, dragon_tiger, block_trades, northbound_holdings, "
            "share_unlock_schedule, corporate_actions, valuation_metrics, "
            "index_constituents and the rest. Call describe_lake for the full "
            "list and each one's coverage. Bars go through query_bars and "
            "financial statements through query_fundamentals. Results are paged with "
            "limit/offset and the response reports the true total and a truncated "
            "flag; for cross-sectional ranking use run_sql instead of paging. "
            "A call that would read more than 64 MiB of Parquet without a short "
            "symbol list is refused with the window that fits; pass start/end or "
            "symbols on large datasets."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "dataset": {"type": "string", "description": "Dataset name from describe_lake."},
                "start": _START,
                "end": _END,
                "symbols": _SYMBOLS,
                "limit": _LIMIT,
                "offset": _OFFSET,
                "include_provenance": _PROVENANCE,
            },
            "required": ["dataset"],
        },
        "handler": tools.query_dataset,
    },
    {
        "name": "run_sql",
        "description": (
            "One read-only DuckDB SELECT over every dataset, each exposed as a "
            "view under its own name. Use this for ranking, aggregation, "
            "percentiles and joins across datasets — computing them here is both "
            "correct and far cheaper than paging thousands of rows back and "
            "adding them up. `daily_bars_adj` is a ready-made view carrying "
            "hfq_open/high/low/close and open-lake qfq_* beside the raw columns; "
            "for a bounded historical qfq window use `daily_bars_qfq(start_date, "
            "end_date)` so the anchor matches that window. Prefer these over "
            "joining adj_factors yourself. Single SELECT only: no writes, DDL or "
            "multiple statements. For one symbol's bars use query_bars, and for "
            "plain dataset reads query_dataset."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "A single DuckDB SELECT. Table names are dataset names "
                    "(see describe_lake).",
                },
                "limit": _LIMIT,
            },
            "required": ["sql"],
        },
        "handler": tools.run_sql,
    },
]

HANDLERS = {tool["name"]: tool["handler"] for tool in TOOLS}

# What goes on the wire: the same list without the Python callable. Every tool
# is read-only, and saying so is what lets a client such as ChatGPT run a call
# without asking the user to confirm each one.
DESCRIPTORS = [
    {**{k: v for k, v in tool.items() if k != "handler"}, "annotations": {"readOnlyHint": True}}
    for tool in TOOLS
]
