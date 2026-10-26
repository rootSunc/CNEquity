---
description: 用 DuckDB SQL 与 Polars LazyFrame 直接读取本地 A 股 Parquet 数据湖，不复制调整后数据。
---

# DuckDB 与 Polars

同一份 Parquet 湖可以按三种方式消费：`load()` 负责带语义的研究查询，DuckDB 负责跨数据集 SQL，Polars `scan()` 负责 LazyFrame 管道。选择入口，不要复制一份“调整后数据湖”。

## 准备

以下使用正式配置 `configs/cnequity.toml`，要求 `daily_bars` 和 `adj_factors` 已发布。用小湖体验时先运行 `demo --research`，并在各查询中明确传入 demo 配置或 `data_root`。

## DuckDB：跨数据集聚合

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

`daily_bars_adj` 是只读视图，带 `adj_*` 与 `adj_is_exact`。`cne query` 只接受单条 `SELECT`，适合把 SQL 交给脚本或 MCP agent。

## Polars：LazyFrame 特征管道

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

`load()` 先完成严格复权并物化结果，`.lazy()` 只延迟后面的特征计算。用日期和标的限制输入规模；它不是全程惰性读取。

如果只需原始行情，可直接惰性扫描：

```python
from cnequity.query import scan

raw = scan("daily_bars", start="2024-01-01", end="2024-12-31",
           symbols=["600519.SH", "000001.SZ"])
print(raw.select("symbol", "trade_date", "close").limit(10).collect())
```

`scan()` 不接受 `adjust`、`strict_adj`、`universe` 或 `as_of`，也不替你做相应研究校验。

## 连接到 AI agent

```bash
cne mcp --config /abs/path/to/cnequity.toml
```

MCP 服务只读，返回值会声明 `origin`、截断状态、复权与 PIT 口径。采集和维护由 `cne` CLI 执行；本机 `cne serve` 的操作页可以预览并启动同一白名单里的命令。其它 MCP 客户端可复用同一条 `command` / `args` 配置，完整工具契约见 [MCP 参考](../reference/mcp.md)。
