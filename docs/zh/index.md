# CNEquity

面向中国市场量化研究的本地数据湖。按配置采集行情、财报、公司事件和资金流等数据，发布到本地 Parquet。数据源覆盖和研究口径可以查证，未完成的采集可续跑。

```text
初始化 / 日更 / 回填 → 校验与暂存 → 发布有效数据 → Python / SQL / MCP 查询
                           └─ 记录缺口，供后续检查与重试
```

- [快速开始](getting-started/quickstart.md) · 先在离线样例湖中完成一次查询，再选择真实采集范围。
- [整体设计](architecture/overview.md) · 湖目录、写入发布、覆盖规划、质量门禁与版本复现如何配合。
- [数据与覆盖](datasets/catalog.md) · 了解每个数据集的来源和历史限制。
- [查询与研究](datasets/query-guide.md) · 查行情、复权、历史股票池和时点财报。
- 参考 · [Python API](reference/python-api.md)、[MCP](reference/mcp.md)、[配置](getting-started/configuration.md)、[命令清单](reference/cli-surface.md)。
- [日常更新](operations/runbook.md) · 日更、验收、补数和快照；[故障排查](operations/troubleshooting.md)按症状定位。
- [安装与升级](getting-started/installation.md) · 核对目标版本的迁移与回退步骤。
- [许可、来源与引用](legal-and-data-sources.md) · 查看上游数据的使用边界。

发布历史见 [GitHub Releases](https://github.com/rootSunc/CNEquity/releases)。用 `cne --version` 确认已安装的版本。
