# Query and research

| Task | Entry point |
|---|---|
| Run SQL against published data | `cne query --sql "SELECT * FROM daily_bars LIMIT 5"` |
| Apply adjustment, historical universe, or PIT semantics | Python `cnequity.query.load()` with explicit research options; see the [Python API](../reference/python-api.md) |
| Select columns and defer execution | Python `scan()`, building and collecting inside `read_scope()`. This reads raw fields. |
| Query from an MCP client | [MCP](#mcp) |
| Give an external tool a standalone Parquet copy | [Export](#parquet) |

## Prices and historical universes {#prices-and-universe}

```python
from cnequity.query import load

bars = load(
    "daily_bars", symbols=["600519.SH"], start="2024-01-01", end="2024-12-31",
    adjust="hfq", strict_adj=True,
)
print(bars.select("symbol", "trade_date", "close", "adj_close", "adj_is_exact").tail())
```

This example requires prices and adjustment factors for the window in a real lake. `close` is the raw close and `adj_close` is adjusted. Use `adjust="qfq"` for forward or `"hfq"` for backward adjustment. For a historical universe, inspect scope and evidence with `cne profile list` and `cne profile show <name>`, then pass `profile` when reading. Historical listings, delistings, and ST status depend on dated evidence. Filtering history with today's roster drops delisted stocks.

## Point-in-time financial statements {#pit}

```python
facts = load(
    "financial_statement_items",
    symbols=["000001.SZ"],
    items=["revenue"],
    as_of="2026-05-07",
    pit_mode="strict",
)
```

Both modes check disclosure dates and known source availability and publication dates against `as_of`. Strict mode also requires the lake to have observed the vintage by the cutoff and excludes rows labelled as backfilled, reconstructed, or snapshots. `best_effort` can return reconstructed rows that pass the visibility checks with `pit_is_exact=False`. It is also the default when no mode is specified. If a strict query is empty, check disclosure, observation dates, and local coverage.

The table uses synthetic dates and assumes the disclosure and first observation are recorded.

| Date | Event | Result of a strict read |
|---|---|---|
| March 31 | Reporting period ends | The lake has no disclosed record yet |
| April 28 | First disclosure, observed that day | The original vintage is visible |
| May 6 | Correction disclosed and observed | The corrected vintage is visible |
| June 3 | First backfill of a separate fact with an old disclosure date only | A strict May 7 query still excludes that fact |

## Minute bars and derivatives {#intraday-and-derivatives}

- `cne derive minute_bars_15m` materializes 15-minute bars from retained 1m or 5m bars. Use `minute_bars_30m` and `minute_bars_60m` for the other periods. Resampling respects the lunch break and prefers 1m data per symbol and session. Check `resampled_from` and input gaps. In Python, `resample_minute_history()` computes bars on demand.
- For futures term structure, join `futures_bars` to `futures_contracts`. Use `option_bars` and `option_contracts` for option chains. Leave expiry and lifecycle fields unknown where evidence is missing. Start with the [source table](catalog.md#derivative-coverage), then check local coverage.

## Export Parquet {#parquet}

```bash
cne storage export daily_bars --out ./parquet-export
```

The export reads published data and writes files into the specified output directory. Use this for DuckDB, Polars, or other tools that read Parquet directly.

## Connect MCP {#mcp}

Use the following launch configuration for a stdio-capable client. Supply the absolute path to the lake's config.

```json
{
  "mcpServers": {
    "cnequity": {
      "command": "cne",
      "args": ["mcp", "--config", "/path/to/cnequity.toml"]
    }
  }
}
```

Call `describe_lake` first to confirm the lake and coverage. MCP tools provide read-only queries, including `query_bars`, `query_fundamentals`, and `run_sql`. For URL-only clients, use `cne mcp --http --config /path/to/cnequity.toml --token <token>` with HTTPS forwarding. Provide the token through a Bearer header or `/mcp/<token>` and share it only with clients authorized to access this lake. See [MCP](../reference/mcp.md) for client setup, the tools, and the limits of `--live`.
