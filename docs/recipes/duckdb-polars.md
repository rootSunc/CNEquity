# Recipe：DuckDB 与 Polars

同一份 Parquet 湖可以按三种方式消费：`load()` 负责带语义的研究查询，DuckDB 负责跨数据集 SQL，Polars `scan()` 负责 LazyFrame 管道。选择入口，不要复制一份“调整后数据湖”。

## DuckDB：跨数据集聚合

```bash
cml query --config configs/cn-market-lake.toml --sql "
  SELECT symbol, max(trade_date) AS last_date, avg(adj_close) AS avg_hfq_close
  FROM daily_bars_adj
  WHERE trade_date >= DATE '2024-01-01'
    AND adj_is_exact
  GROUP BY symbol
  ORDER BY avg_hfq_close DESC
  LIMIT 20
"
```

`daily_bars_adj` 是只读视图，带 `adj_*` 与 `adj_is_exact`。`cml query` 只接受单条 `SELECT`，适合把 SQL 交给脚本或 MCP agent。

## Polars：LazyFrame 特征管道

```python
import polars as pl

from cn_market_lake.query import scan

bars = (
    scan(
        "daily_bars",
        start="2024-01-01",
        adjust="hfq",
        strict_adj=True,
    )
    .filter(pl.col("symbol").is_in(["600519.SH", "000001.SZ"]))
    .sort(["symbol", "trade_date"])
    .with_columns(
        pl.col("adj_close").pct_change().over("symbol").alias("daily_return")
    )
)

features = bars.select(
    ["symbol", "trade_date", "adj_close", "daily_return"]
).collect()
```

大窗口优先保留 LazyFrame，利用 Hive 日期分区裁剪。`strict_adj=True` 会在因子覆盖不完整时失败，避免特征管道悄悄混入未复权价格。

## 连接到 AI agent

```bash
cml mcp --config /abs/path/to/cn-market-lake.toml
```

MCP 服务只读，返回值会声明 `origin`、截断状态、复权与 PIT 口径；采集和维护仍由 `cml` CLI 执行。其它 MCP 客户端可复用同一条 `command` / `args` 配置，完整工具契约见 [MCP 参考](../reference/mcp.md)。
