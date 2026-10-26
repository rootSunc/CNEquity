---
description: Connect a local China A-share data lake to Claude, ChatGPT, Cursor and other AI agents through a read-only MCP server.
---

# MCP integration: connecting the lake to AI agents

`cne serve` shows the lake to people, and its operations page can also start allowlisted fetches and daily updates. `cne mcp` gives the lake to models, and it is **read-only**: there is no entry point here that triggers fetching, retries, or cleanup.

The current implementation is standard MCP over stdio and is not tied to Claude or any particular model. The client spawns a
`cne mcp` subprocess and exchanges JSON-RPC over the stdin/stdout pipes; any
agent that supports stdio MCP can reuse the same command and configuration.

Three paths; pick by what you already have:

```bash
# ① You already have a lake — full semantics
cne mcp --config /abs/path/to/cnequity.toml

# ② No lake yet, just trying it out — cne init --profile demo fetches a small slice of real data
cne init --profile demo
cne mcp --config /abs/path/to/configs/cnequity.demo.toml

# ③ No lake at all — fetch on demand and serve it, nothing written to disk
cne mcp --config /abs/path/to/cnequity.toml --live
```

The default transport is stdio: the client spawns the process and speaks JSON-RPC over the pipes, so you never run it by hand. Clients that only accept a remote URL (ChatGPT) use `--http`; see [ChatGPT](#chatgpt) below.

**Give `--config` as an absolute path.** The directory an MCP client starts the process from is unpredictable, so a relative `--config` resolves somewhere else. A relative `[data].root` inside the configuration resolves against the config file's directory, whatever the start directory. Without `--config` the shared lookup order applies, and the per-user `~/.config/cnequity/config.toml` is found from any directory; an explicit absolute path is still the surest way to reach the right lake.

Configurations written by `cne config create` already use absolute paths. At startup the server checks whether curated contains any parquet; if not, it exits immediately and prints the resolved path instead of serving an empty lake.

## Connecting clients {#connecting-clients}

Replace `/abs/path/to/cnequity.toml` below with the absolute path of your configuration; if you only want to try it out, use the demo configuration path printed by `cne init --profile demo`.

| Client | Transport | How to connect |
|---|---|---|
| Claude Code | stdio | One command |
| Claude Desktop | stdio | `claude_desktop_config.json` |
| ChatGPT | HTTP | `cne mcp --http` + HTTPS tunnel + developer-mode connector |
| Codex (CLI / IDE extension) | stdio | One command, or `~/.codex/config.toml` |
| Gemini CLI | stdio | `~/.gemini/settings.json` |
| Cursor | stdio | `~/.cursor/mcp.json` |
| VS Code (Copilot agent mode) | stdio | `.vscode/mcp.json` |
| Windsurf, Cline, Cherry Studio, Trae, LM Studio, etc. | stdio | Enter the same command and arguments in the MCP settings |

Whether a client is supported depends on whether it supports stdio or only accepts a remote URL, not on which vendor's model is behind it.

### Claude Code

```bash
claude mcp add cnequity -- cne mcp --config /abs/path/to/cnequity.toml
```

Add `-s user` to make it available in all projects.

### Claude Desktop, Cursor, Gemini CLI, Windsurf

These use the same JSON and differ only in file location: for Claude Desktop it is `claude_desktop_config.json`, opened from Settings → Developer → Edit Config; Cursor uses `~/.cursor/mcp.json`; Gemini CLI uses `~/.gemini/settings.json`; Windsurf uses `~/.codeium/windsurf/mcp_config.json`.

```json
{
  "mcpServers": {
    "cnequity": {
      "command": "cne",
      "args": ["mcp", "--config", "/abs/path/to/cnequity.toml"]
    }
  }
}
```

**If the desktop app cannot find `cne`**, replace `command` with an absolute path (`which cne` in a terminal, `where cne` on Windows). Apps launched from the Dock or the Start menu do not inherit the shell's `PATH`, especially when `cne` is installed in a virtual environment.

### Codex

```bash
codex mcp add cnequity -- cne mcp --config /abs/path/to/cnequity.toml
```

Or put it in `~/.codex/config.toml`:

```toml
[mcp_servers.cnequity]
command = "cne"
args = ["mcp", "--config", "/abs/path/to/cnequity.toml"]
```

### VS Code

In VS Code the key is `servers`, not `mcpServers`:

```json
{
  "servers": {
    "cnequity": {
      "type": "stdio",
      "command": "cne",
      "args": ["mcp", "--config", "/abs/path/to/cnequity.toml"]
    }
  }
}
```

### ChatGPT {#chatgpt}

ChatGPT only connects to public HTTPS addresses, cannot spawn local processes, and supports only two kinds of authentication: OAuth and "No authentication". So it takes three steps: start the server in HTTP mode, expose it as HTTPS through a tunnel, then give ChatGPT a URL that carries the token. Developer mode, which requires a paid plan, is needed.

```bash
# 1. Generate a random token and start the server in HTTP mode (listens only on 127.0.0.1:8788 by default)
TOKEN=$(python -c "import secrets; print(secrets.token_urlsafe(24))")
cne mcp --config /abs/path/to/cnequity.toml --http --token "$TOKEN"

# 2. In another terminal, expose it as HTTPS through a tunnel (cloudflared or ngrok both work)
cloudflared tunnel --url http://localhost:8788
```

3. In ChatGPT settings, turn on developer mode and create a new connector: set the URL to `https://<tunnel domain>/mcp/<token>` and choose "No authentication".

The token goes in the path because ChatGPT cannot add custom request headers to a connector; the server also accepts `Authorization: Bearer <token>` and `?token=<token>`. Once `--token` is set, every request must carry the token, regardless of the Host header. Without a token, only direct local connections are accepted: any request whose Host is not a loopback address, that carries forwarding headers (tunnels and reverse proxies both add them), or whose Origin is another site gets a 403.

**This URL is the key.** Anyone who has it can run arbitrary read-only SQL against your lake. Do not paste it anywhere public; stop the tunnel when you are not using it. A cloudflared quick tunnel gets a new domain every time it starts, and after it changes you have to update the connector URL in ChatGPT.

All six tools carry `readOnlyHint`, and ChatGPT treats them as read-only operations accordingly. Custom connectors in Claude on the web also accept only remote URLs and can use the same setup.

## What the six tools do

Each turn, the agent has to pick a tool from a flat list. One tool per dataset would fill most of the context with names it will not call this time, and it still would not know which one answers the question. The tools are split by **question shape** instead: describe, find a code, read market data, read financial statements, read everything else, aggregate. The dataset becomes a parameter.

| Tool | Purpose |
|------|------|
| `describe_lake` | What is in the lake, how far coverage reaches, and the semantics that make answers correct. Call it first in every session |
| `resolve_symbol` | "茅台" (Moutai) → `600519.SH`. Includes delisted stocks, flagged with `delist_date` |
| `query_bars` | Daily bars / indexes / minute bars, with `adjust` and `universe` |
| `query_fundamentals` | Financial statement items; `as_of` is **required** (PIT) |
| `query_dataset` | Any other dataset, filtered by date and symbol |
| `run_sql` | A single read-only DuckDB SELECT for cross-dataset aggregation / ranking / quantiles |

### Semantics live in the response, not in the docs

Models do not read `docs/`. So the three rules most likely to produce "confidently wrong answers" go straight into the return values:

- The `contract` field of `describe_lake` lists what price adjustment, PIT, `snapshot_only`, `history_horizon_days`, and `universe` mean.
- `query_bars` returns a `warning` when called without `adjust`; when `adjust` is given but some rows lack factors, it reports "N/M rows `adj_is_exact=false`". The response also echoes `adjust` and `universe`, so an explicit `all_a_sh_sz` Shanghai/Shenzhen-subset scope is not lost during pagination or downstream processing.
- `query_fundamentals` without `as_of` fails with an error that explains why there is no default: defaulting to today means answering a historical question with today's information, and the agent would have no way to notice.
- `query_fundamentals` defaults to `pit_mode="best_effort"`: history that a backfill recorded later is returned with `pit_is_exact=false`, and `exact_rows` counts the exact ones. `pit_mode="strict"` returns only rows the lake held on `as_of`; when that is empty but backfilled rows exist, `note` says so and points at best_effort.

### Pagination always tells the truth

Every response carries `total` / `returned` / `truncated`. Show a model only these 200 rows and it will report the mean of 200 rows as the market-wide mean; `truncated` and `note` are the switch that makes it use `run_sql` instead.

### Provenance is summarized, not per row

Every curated row carries `source` / `data_version` / `fetched_at`, and returning them per row would make market-data payloads about three times larger just to repeat the same three values. A `sources` summary is returned by default; pass `include_provenance: true` when you need it per row.

## `--live`: connect without a lake, but know what is missing

`--live` makes queries for "not in the lake" data **fetch from the source on the spot, without writing to disk**, and hand the result straight to the agent. It is **an on-ramp, not a destination**.

**It can do only two things**: `resolve_symbol` and **unadjusted** daily bars. Everything else is explicitly refused: not an empty result, but an error explaining why, because an agent reads an empty result as "this never happened".

| Tool | Under live | Why |
|--|--|--|
| `resolve_symbol` | ✅ | But it uses current security master data, so **delisted stocks are not in it at all** |
| `query_bars` (daily_bars) | ✅ unadjusted | Passing `adjust` / `universe` raises an error; see below |
| `query_bars` (minute bars / indexes) | ❌ | This market-data protocol path only provides daily bars |
| `query_fundamentals` | ❌ | The source returns restated values "as seen today"; there is no honest `as_of` |
| `query_dataset` | ❌ | Each dataset is its own adapter + pagination + quality checks; a one-off pull is "a different series wearing the same column names" |
| `run_sql` | ❌ | It queries parquet on disk, and live writes nothing |

**Why `adjust` is refused instead of "best effort"**: adjustment factors are a dataset this project derives separately from Sina and stores in the lake, not a field the market-data source attaches to each bar. Serving unadjusted prices as adjusted ones is wrong across any ex-rights event, and **you cannot tell from the numbers**.

**Every live response carries `origin: "live"` and a warning** that lists what is missing (price adjustment, universe, PIT, pre-write validation, provenance). Responses from the lake carry `origin: "lake"`. Both sides are labeled, so "no origin field" is never assumed to mean the lake.

**Off by default, never inferred automatically.** A user with a lake whose lake is broken must get "no parquet data" and go fix it, not quietly receive a similar-looking answer from somewhere else.

**Each call is capped**: at most 50 symbols and 800 days, and **`symbols` must be given explicitly**. Agents loop, and these are exactly the hosts the daily update pipeline depends on; letting a model decide on its own to scan the whole market earns the user a ban over a question they never asked.

Apart from the rate limiter's own state file (which exists precisely to rate-limit requests), **nothing extra appears on disk**, and a test guards this.

## Limits of run_sql

It accepts only **a single SELECT**, judged by DuckDB's own parser, not a regex.

The lake contains `news_headlines` and `flash_news_wire`: vendor text, not content we wrote, and the agent reads it. That means SQL reaching this tool can be influenced by content ingested into the lake. The parser can tell `SELECT ... -- ; DROP` apart from two statements; a regex cannot. The connection itself is also read-only, and the two complement each other: read-only does not stop `COPY ... TO` (it writes to a path outside the database file).

The SQL connection is also restricted to the two lake directories `curated/` and `derived/`, with DuckDB external access and extension auto-install/auto-load turned off and these settings locked. File access pointing outside the lake, such as `read_text`, `read_csv`, `read_parquet`, or HTTP URLs, therefore fails. This is not an operating-system sandbox; when deploying untrusted agents, still use process- or container-level isolation.

Examples of what is rejected: multiple statements, `DROP`, `CREATE`, `ATTACH`, `COPY ... TO`.

`daily_bars_adj` is a ready-made view with `hfq_*` / qfq and `adj_is_exact`; its qfq anchor
is each symbol's latest bar across the whole lake. For qfq aggregation over an explicit historical window, use
`daily_bars_qfq(DATE '2020-01-01', DATE '2020-12-31')`; this macro anchors on the window
and is preferable to joining `adj_factors` yourself.

## Research questions worth handing to an agent

The difference is not the data source but whether a historical series exists:

- "Where does Moutai's PE sit now within its historical percentiles over the past five years?" Needs a multi-year daily series of `valuation_metrics`
- "What was this financial-statement factor's IC in 2018, without look-ahead data?" Needs a PIT slice via `as_of`
- "What did stocks delisted in the past three years look like in the 60 days before delisting?" Needs delisted stocks to remain in the lake

Before answering, check coverage with `describe_lake`, and check the semantics and truncation hints the query returns. When data has not been collected or the evidence is insufficient, the agent should keep the limitation in its answer and must not conclude from an empty result that an event did not happen.

## Transport and implementation

The server uses the JSON-RPC handling in `mcp_server/protocol.py`, with no extra MCP SDK dependency. The default transport is stdio; `--http` uses `mcp_server/http.py` to provide Streamable HTTP: each `POST /mcp` returns JSON directly, with no SSE stream and no session, and `GET` returns 405. The default mode reads local data; `--live` is an explicit network mode that still writes no research data, though the rate-limit ledger may be updated.

## Troubleshooting

**The client reports a parse error while the server looks fine.** stdout is the JSON-RPC wire, and any extra output corrupts it. `cne mcp` already directs logs to stderr (MCP clients collect it as server logs); if you added a `print` in a fork, that is the cause.

**A tool reports "no parquet data for dataset X".** That dataset is still empty in this lake. Call `describe_lake` with `include_empty: true` to see the list of datasets that are registered but have no data, or run the corresponding step.

**You are reading a different lake from the one you think.** Check that `--config` is an absolute path; the first item in the `describe_lake` response is `data_root`.

## Related docs

- [CLI](cli.md#cne-mcp) · [Python API](python-api.md)
- [Dataset catalog](../datasets/catalog.md) · [Query guide](../datasets/query-guide.md)
