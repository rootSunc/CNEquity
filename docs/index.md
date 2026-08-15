# CNMarketLake

一个免费、零注册、自托管的 A 股历史数据层：多源采集，落地 Parquet，保留溯源，并把复权、Universe 和 PIT 口径做成可复查的查询合同。

## 先跑起来

```bash
pip install cn-market-lake
cml demo
```

`cml demo` 使用真实 TDX 日线写入独立的 `data/cn-market-lake-demo/`，不会碰全量湖。想直接验证研究口径，再运行：

```bash
cml demo --research --symbols 600519.SH
```

它会额外读取 Sina 的后复权因子，输出 raw 与 hfq 收益对照。网络受限时先使用不带 `--research` 的基础 demo。

## 你会得到什么

| 需求 | 入口 |
| --- | --- |
| 一分钟看到真实数据 | [快速开始](getting-started/quickstart.md) |
| 复权收益与数据质量 | [复权基线 Recipe](recipes/research-baseline.md) |
| 避免财报未来函数 | [PIT 财报截面 Recipe](recipes/pit-rebalance.md) |
| 直接接 DuckDB / Polars | [DuckDB 与 Polars Recipe](recipes/duckdb-polars.md) |
| 接给 AI agent | [MCP 参考](reference/mcp.md) |
| 线上跑批与故障恢复 | [Runbook](operations/runbook.md) |
| 论文 / 报告引用 | [引用 cn-market-lake](citation.md) |

## 设计边界

- **数据层，不是回测框架。** 交付稳定列契约和可读文件，下游可以用 Python、DuckDB、Polars 或 AI agent。
- **历史语义显式化。** `adjust="hfq"`、`strict_adj=True`、`universe="all_a"` 和 `as_of=...` 都是调用方可见的选择。
- **失败可追踪。** 每行带 `source`、`data_version`、`fetched_at`；批次、质量审计和源健康度写入 `meta/`。

## 继续阅读

从 [安装](getting-started/installation.md) 开始；全量页面与模块地图见仓库的[完整文档索引](https://github.com/rootSunc/cn-market-lake/blob/main/docs/README.md)。

项目方向见仓库根目录的 [ROADMAP](https://github.com/rootSunc/cn-market-lake/blob/main/ROADMAP.md)，欢迎通过 [Issues](https://github.com/rootSunc/cn-market-lake/issues) 或 [Discussions](https://github.com/rootSunc/cn-market-lake/discussions) 反馈。
