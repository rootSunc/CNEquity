# Installing the CNEquity MCP server

Instructions for an AI agent setting up CNEquity for a user. CNEquity is a local China A-share data lake; `cne mcp` exposes it to MCP clients as six read-only tools. No account or API token is needed.

## 1. Install

Requires Python 3.10+. Either install the package:

```bash
pip install cnequity
```

or skip installation and let the client run it with `uvx cnequity …` (needs [uv](https://docs.astral.sh/uv/)).

## 2. Create a lake

The server refuses to start without data. Ask the user which they want:

- **Try it in about a minute** (recommended first): real data for 5 stocks over the last 30 trading days, in a separate directory.

  ```bash
  cne init --profile demo --data-root ~/cnequity-demo --config-out ~/cnequity-demo/cnequity.toml
  ```

- **The full lake**: every Shanghai, Shenzhen and Beijing A-share over the last 3 years. The first run can take hours; if it stops, running the same command again resumes it.

  ```bash
  cne init
  ```

  This writes `configs/cnequity.toml` in the current directory.

`cne init` prints the absolute path of the config it wrote. Use that path below.

## 3. Register the server

`--config` **must be an absolute path**: the client starts the server from a directory of its own choosing, and a relative path resolves to an empty lake.

Cline (`cline_mcp_settings.json`) and most other clients:

```json
{
  "mcpServers": {
    "cnequity": {
      "command": "cne",
      "args": ["mcp", "--config", "/absolute/path/to/cnequity.toml"],
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

If `cne` is not on the PATH the client sees, use the absolute path of the executable (`which cne`), or `"command": "uvx"` with `"args": ["cnequity", "mcp", "--config", "/absolute/path/to/cnequity.toml"]`.

ChatGPT and other clients that only accept a remote URL need the HTTP mode and a tunnel; see the [MCP guide](https://rootsunc.github.io/CNEquity/en/reference/mcp/#connecting-clients).

## 4. Verify

Call the `describe_lake` tool. It returns the lake's `data_root`, the datasets it holds and their coverage. If it reports no data, the `--config` path is wrong or relative.

## Tools

| Tool | Use |
|---|---|
| `describe_lake` | Coverage, freshness and the query rules; call it first |
| `resolve_symbol` | Company name or code → `600519.SH`-style symbol |
| `query_bars` | Daily, index and minute bars, optionally adjusted |
| `query_fundamentals` | Financial statements with a point-in-time `as_of` cutoff |
| `query_dataset` | Any other dataset |
| `run_sql` | One read-only DuckDB `SELECT` across all datasets |

All tools are read-only; ingestion and updates are `cne` commands (`cne run daily`).
