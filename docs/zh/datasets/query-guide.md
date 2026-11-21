# 查询与研究

| 需求 | 入口 |
|---|---|
| 对发布数据运行 SQL | `cne query --sql "SELECT * FROM daily_bars LIMIT 5"` |
| 需要复权、历史股票池或 PIT | Python `cnequity.query.load()`，显式指定研究口径；参数见 [Python API](../reference/python-api.md) |
| 按列筛选并延后计算 | Python `scan()`，在 `read_scope()` 内构建并收集；此入口读取原始字段 |
| 让 MCP 客户端查湖 | [MCP](#mcp) |
| 给外部工具一份独立 Parquet | [导出](#parquet) |

## 行情与历史股票池 {#prices-and-universe}

```python
from cnequity.query import load

bars = load(
    "daily_bars", symbols=["600519.SH"], start="2024-01-01", end="2024-12-31",
    adjust="hfq", strict_adj=True,
)
print(bars.select("symbol", "trade_date", "close", "adj_close", "adj_is_exact").tail())
```

本例要求真实湖已有此区间的行情和复权因子。`close` 是原始收盘，`adj_close` 是所选口径的复权值。前复权用 `adjust="qfq"`，后复权用 `"hfq"`。对历史股票池用 `cne profile list` 和 `cne profile show <名称>` 查看范围和证据要求，再在读取时指定 `profile`。历史上市、退市和 ST 需要相应日期的证据。按今天仍上市的名单筛历史会丢掉已经退市的股票。

## 财报的时点口径 {#pit}

```python
facts = load(
    "financial_statement_items",
    symbols=["000001.SZ"],
    items=["revenue"],
    as_of="2026-05-07",
    pit_mode="strict",
)
```

两种模式都用 `as_of` 筛选披露日期，以及已知的源端可用时间和发布时间。严格模式还要求湖最迟在截止日观察到该版本，并排除标记为回填、重建或快照来源的记录。`best_effort` 可以保留通过可见性筛选的重建值，并标记 `pit_is_exact=False`。省略 `pit_mode` 时使用 `best_effort`。严格查询为空时，先检查披露和观察时间以及本地覆盖。

下表为合成示意，假定披露时间与湖的首次观察时间均已记录。

| 日期 | 事件 | 对应时点的严格查询 |
|---|---|---|
| 3 月 31 日 | 报告期结束 | 湖尚无已披露记录 |
| 4 月 28 日 | 首次披露，当日被观察 | 可读到原始版本 |
| 5 月 6 日 | 更正披露，当日被观察 | 可读到更正版本 |
| 6 月 3 日 | 第一次回填另一条仅带旧披露日期的记录 | 5 月 7 日的严格查询不会因此多出这条记录 |

## 分钟线与衍生品 {#intraday-and-derivatives}

- `cne derive minute_bars_15m` 从已入湖的 1m／5m 数据生成 15 分钟线。30／60 分钟线分别使用 `minute_bars_30m` 和 `minute_bars_60m`。重采样不跨午休，每个标的每个交易日优先使用 1m。检查 `resampled_from` 与输入缺口。Python 可用 `resample_minute_history()` 现算。
- 期货期限结构读取 `futures_bars` 并关联 `futures_contracts`，期权链读取 `option_bars` 和 `option_contracts`。缺少合约到期或生命周期证据时保留未知。先看[来源表](catalog.md#derivative-coverage)，再核对本地覆盖。

## 导出 Parquet {#parquet}

```bash
cne storage export daily_bars --out ./parquet-export
```

该命令只读已发布数据，将结果写到指定目录。适用于 DuckDB、Polars 或其他直接读 Parquet 的程序。

## MCP 接入 {#mcp}

在支持 stdio 的客户端配置下列启动信息。使用湖配置的绝对路径。

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

接入后先调用 `describe_lake` 确认湖与覆盖。MCP 查询是只读的，常用工具包括 `query_bars`、`query_fundamentals`、`run_sql`。只接受远程 URL 的客户端需要 `cne mcp --http --config /path/to/cnequity.toml --token <token>` 和 HTTPS 转发。可通过 Bearer 请求头或 `/mcp/<token>` 传令牌，令牌只交给有权访问此湖的客户端。各客户端的接入方式、工具说明与 `--live` 的限制见 [MCP](../reference/mcp.md)。
