---
description: Query a local China A-share Parquet data lake with DuckDB SQL and Polars LazyFrames without copying adjusted data.
---

# DuckDB and Polars

The same Parquet lake can be consumed in three ways: `load()` handles research queries with semantics, DuckDB handles cross-dataset SQL, and Polars `scan()` handles LazyFrame pipelines. Pick an entry point; do not make a copy as an "adjusted data lake".

## Preparation

The examples below use the production configuration `configs/cnequity.toml` and require `daily_bars` and `adj_factors` to be published. To try it on a small lake, first run `demo --research`, and pass the demo configuration or `data_root` explicitly in each query.

## DuckDB: cross-dataset aggregation

```bash
cne query --config configs/cnequity.toml --sql "
  SELECT symbol, max(trade_date) AS last_date, avg(adj_close) AS avg_hfq_close
  FROM daily_bars_adj
  WHERE trade_date >= DATE '2024-01-01'
    AND adj_is_exact
  GROUP BY symbol
  ORDER BY avg_hfq_close DESC
  LIMIT 20
"
```

`daily_bars_adj` is a read-only view with `adj_*` and `adj_is_exact`. `cne query` accepts only a single `SELECT`, which makes it suitable for handing SQL to scripts or an MCP agent.

## Polars: LazyFrame feature pipeline

```python
import polars as pl

from cnequity.query import load

bars = (
    load(
        "daily_bars",
        start="2024-01-01",
        end="2024-12-31",
        symbols=["600519.SH", "000001.SZ"],
        adjust="hfq",
        strict_adj=True,
    )
    .lazy()
    .sort(["symbol", "trade_date"])
    .with_columns(
        pl.col("adj_close").pct_change().over("symbol").alias("daily_return")
    )
)

features = bars.select(
    ["symbol", "trade_date", "adj_close", "daily_return"]
).collect()
```

`load()` first completes strict price adjustment and materializes the result; `.lazy()` only defers the feature computation that follows. Limit the input size by date and symbol; this is not lazy reading end to end.

If you only need raw market data, scan lazily directly:

```python
from cnequity.query import scan

raw = scan("daily_bars", start="2024-01-01", end="2024-12-31",
           symbols=["600519.SH", "000001.SZ"])
print(raw.select("symbol", "trade_date", "close").limit(10).collect())
```

`scan()` does not accept `adjust`, `strict_adj`, `universe` or `as_of`, and does not perform the corresponding research checks for you.

## Connecting to an AI agent

```bash
cne mcp --config /abs/path/to/cnequity.toml
```

The MCP server is read-only; its return values declare `origin`, truncation status, and the price adjustment and PIT semantics. Collection and maintenance are done by the `cne` CLI; the operations page of a local `cne serve` can preview and launch commands from the same allowlist. Other MCP clients can reuse the same `command` / `args` configuration; for the full tool contract, see the [MCP reference](../reference/mcp.md).
