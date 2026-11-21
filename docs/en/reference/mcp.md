---
description: Connect a local China A-share data lake to Claude, ChatGPT, Cursor, and other AI agents over read-only MCP.
---

# MCP

`cne mcp` serves the lake as read-only tools to any MCP client. It has no way to start collection, retries, or cleanup; fetching starts from the CLI or the `cne serve` operations page. The server speaks standard MCP JSON-RPC and is not tied to any model.

```bash
cne mcp --config /abs/path/to/cnequity.toml            # an existing lake
cne init --profile demo                                 # no lake yet: build a 5-symbol sample first
cne mcp --config /abs/path/to/cnequity.toml --live      # no lake: fetch on demand without writing, limited scope
```

Write `--config` as an absolute path: you cannot know which directory a client starts the process from. A relative `[data].root` in the config resolves against the config file's directory. When the lake has no published data, the command exits and prints the resolved config and data directory instead of serving an empty lake.

## Connect a client {#clients}

Every stdio client uses the same command and arguments; only the config file location differs:

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

| Client | Setup |
|---|---|
| Claude Code | `claude mcp add cnequity -- cne mcp --config /abs/path/to/cnequity.toml`; add `-s user` to enable it for every project |
| Codex | `codex mcp add cnequity -- cne mcp --config /abs/path/to/cnequity.toml`, or `[mcp_servers.cnequity]` in `~/.codex/config.toml` |
| Claude Desktop, Cursor, Gemini CLI, Windsurf | The JSON above, in `claude_desktop_config.json`, `~/.cursor/mcp.json`, `~/.gemini/settings.json`, or `~/.codeium/windsurf/mcp_config.json` respectively |
| VS Code | `.vscode/mcp.json`, with the top-level key `servers` and `"type": "stdio"` |
| Clients that only accept a remote URL (such as ChatGPT) | See HTTP mode below |

If a desktop app cannot find `cne`, set `command` to the absolute path (`which cne`, or `where cne` on Windows): apps started from the Dock or Start menu do not inherit your shell's `PATH`.

### HTTP mode {#http}

```bash
TOKEN=$(python -c "import secrets; print(secrets.token_urlsafe(24))")
cne mcp --config /abs/path/to/cnequity.toml --http --token "$TOKEN"   # listens on 127.0.0.1:8788 by default
cloudflared tunnel --url http://localhost:8788                         # in another terminal: expose it over HTTPS
```

Give the client `https://<tunnel host>/mcp/<token>`. The server also accepts `Authorization: Bearer <token>` and `?token=<token>`. Listening on a non-loopback address requires `--token`. Without a token, only direct local connections are accepted, and requests with forwarding headers or a foreign Origin get 403. The URL is a key: anyone holding it can run any read-only SQL against the lake. Stop the tunnel when you are not using it.

In HTTP mode each `POST /mcp` returns JSON directly, with no SSE stream and no session.

## Tools {#tools}

| Tool | Purpose |
|---|---|
| `describe_lake` | The datasets in the lake with their coverage, history mode, and freshness, plus the semantics that make an answer correct. Call it first in every session; `include_empty` lists registered datasets that have no data yet |
| `resolve_symbol` | Maps a company name or partial code to a standard code, such as 茅台 → `600519.SH`; includes delisted stocks |
| `query_bars` | Daily, index, and minute bars, with `adjust` and `universe` |
| `query_fundamentals` | Financial statement items; `as_of` is required and `pit_mode` defaults to `best_effort` |
| `query_dataset` | Any other dataset, filtered by date and symbol |
| `run_sql` | One read-only DuckDB `SELECT`, with every dataset as a view under its own name; for ranking, aggregation, percentiles, and joins across datasets |

The semantics are in the responses, not only in these docs:

- `query_bars` warns when `adjust` is missing and, when it is present, reports how many rows have `adj_is_exact=false`; responses echo `adjust` and `universe`.
- `query_fundamentals` refuses a call without `as_of`: defaulting to today would answer a historical question with today's information. When a strict query is empty but backfilled rows exist, `note` says so.
- Every response carries `total`, `returned`, and `truncated`. When a result is truncated, aggregate with `run_sql` instead of averaging the truncated rows.
- Provenance is summarized by source in `sources`; pass `include_provenance: true` for per-row provenance.

`query_dataset` and dashboard browsing share one row limit, `[query].scan_budget_rows` (default 4,000,000 rows selected before paging). A request over the limit that names more than 20 symbols, or none, is refused with the latest date window that fits.

## run_sql boundaries {#run-sql}

It accepts exactly one `SELECT`, as judged by DuckDB's own parser, so a second statement hidden behind a comment is still detected. The lake holds vendor text such as news, and SQL reaching this tool may be influenced by that content, so the connection is also restricted: it is read-only, it may read only the directories holding published data, external access and extension auto-install and auto-load are off, and the settings are locked. `read_csv`, `read_parquet`, and similar calls pointing outside the lake or at URLs fail. This is not an operating-system sandbox; use process or container isolation for untrusted agents.

The `daily_bars_adj` view carries adjusted columns; for forward-adjusted aggregates over an explicit window, use `daily_bars_qfq(DATE '2020-01-01', DATE '2020-12-31')`.

## --live {#live}

`--live` fetches data the lake does not have straight from the source, without writing it. It supports only `resolve_symbol` (current security master data, without delisted stocks) and unadjusted `daily_bars`. Minute bars, indexes, financial statements, other datasets, and `run_sql` all fail explicitly, because an agent reads an empty result as "this never happened". Passing `adjust` or `universe` also fails: adjustment factors are a separately derived dataset in the lake, and treating unadjusted prices as adjusted is wrong across every ex-date.

Every response carries `origin` (`live` or `lake`), and live responses describe what is missing. Each call is limited to 50 symbols and 800 days and must name `symbols` explicitly. Apart from the rate limiter's own state file, nothing is written. `--live` is off by default and never turned on automatically: a broken lake should produce an error, not a quietly different answer.

## Troubleshooting {#troubleshooting}

| Symptom | Fix |
|---|---|
| The client reports a parse error | stdout is the JSON-RPC channel and logs already go to stderr; a `print` you added corrupts it |
| A tool says a dataset has no data | The dataset is still empty in this lake; check with `describe_lake` and `include_empty: true`, or run its collection |
| The answers come from the wrong lake | The first field `describe_lake` returns is `data_root`; check that `--config` is absolute |

Before answering a research question, check coverage with `describe_lake` and read the semantics and truncation notes in each response. When data was not collected or evidence is insufficient, an empty result does not show that an event did not happen.
