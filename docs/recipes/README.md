# Recipes

这些示例刻意保持“从一份新湖开始也能复现”的形状：先运行命令，再用少量 Python / SQL 验证结果。它们不是回测策略，也不会把网络抓取、复权或 PIT 口径藏在黑盒里。

## 推荐顺序

1. [复权基线](research-baseline.md)：证明原始价和后复权价是两条不同的研究序列。
2. [PIT 财报截面](pit-rebalance.md)：在调仓日只使用当时已经公告的事实。
3. [DuckDB 与 Polars](duckdb-polars.md)：把同一份湖接到 SQL、LazyFrame 或下游特征工程。

## 示例的共同约定

- 示例配置使用 `configs/cn-market-lake.demo.toml`；全量湖则替换为自己的绝对路径配置。
- 研究读取优先 `strict_adj=True`，缺因子就失败，不把缺失默认为 1.0。
- 数据覆盖范围以 `list_datasets()` 返回的 `coverage_start` / `coverage_end` 为准，不根据仓库默认值猜测。
- 代码只读取湖；采集、重试和 compact 仍由 `cml` 命令负责。
