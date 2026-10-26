---
description: A 股 PIT 财报调仓截面：按公告日取财务数据，避免用修订值回答历史调仓问题，防止回测引入未来函数。
---

# PIT 财报调仓截面

财报研究最容易出现的错误，是用今天看到的修订值回答过去的调仓问题。`financial_statement_items` 把 `announce_date` 放进版本键，严格模式还要求发布、可用和湖内观察证据均不晚于截止日。

## 准备数据与预期结果

使用正式配置 `configs/cnequity.toml`，并已采集 `financial_statement_items`。基础 demo 不包含财报。下文日期是示例，应换成证据实际覆盖的调仓日：今天回填的 2023 年财报不会自动成为 2024 年已观察到的版本，严格读取为空是合理结果。

## 1. 读取调仓日可见事实

```python
from pathlib import Path

import polars as pl

from cnequity.config import load_config
from cnequity.query import load

cfg = load_config(Path("configs/cnequity.toml"))
rebalance_date = "2024-04-30"

facts = load(
    "financial_statement_items",
    items=["roe", "revenue"],
    as_of=rebalance_date,
    pit_mode="strict",
    config=cfg,
)

if facts.is_empty():
    raise RuntimeError("没有该日可验证的 PIT 事实；先检查覆盖与观察时间")

# 每个 (symbol, report_period, item_code) 在 as_of 时只保留当时生效的版本。
screen = (
    facts.filter(pl.col("report_period") == "2023Q4")
    .pivot(
        on="item_code",
        index=["symbol", "report_period"],
        values="item_value",
        aggregate_function="last",
    )
    .filter(pl.col("roe") > 0)
)
print(screen.select(["symbol", "roe", "revenue"]))
```

如果要研究修订本身，而不是构造调仓截面，可以显式打开 `all_vintages=True`：

```python
revisions = load(
    "financial_statement_items",
    symbols=["000001.SZ"],
    items=["revenue"],
    as_of="2026-07-21",
    pit_mode="strict",
    all_vintages=True,
    config=cfg,
)
```

## 2. 把口径写进策略输入

推荐把调仓日、报告期、公告截止日和湖覆盖范围一并写入特征快照：

```python
metadata = {
    "rebalance_date": rebalance_date,
    "report_period": "2023Q4",
    "as_of": rebalance_date,
    "pit_mode": "strict",
    "dataset": "financial_statement_items",
    "lake_coverage": "see list_datasets()",
}
```

探索性检查可显式使用 `pit_mode="best_effort"`，并检查返回的 `pit_is_exact` / `pit_quality`；不要把它的结果报告成严格 PIT。

`as_of` 不是可选的“过滤条件”，而是研究问题的一部分。若数据来自一次性回填而不是逐日累积，早期期间可能只有当前源版本；先查看 [数据集目录](../datasets/catalog.md) 的 `history_mode` 和 [查询指南](../datasets/query-guide.md) 的 PIT 限制。
