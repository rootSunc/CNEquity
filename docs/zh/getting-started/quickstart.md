# 快速开始

## 离线读到第一条结果 {#offline-sample}

在空的工作目录运行以下命令。系统要求见[安装](installation.md)。

```bash
python -m pip install cnequity
cne init --profile sample
cne query --config configs/cnequity.sample.toml --sql "SELECT symbol, trade_date, close, source FROM daily_bars ORDER BY trade_date DESC LIMIT 5"
```

`sample` 将合成数据写入 `data/cnequity-sample/`。结果中的 `source=mock` 表明它只适用于安装和查询验证。

## 选择真实采集范围 {#init-scope}

| 命令 | 范围 |
|---|---|
| `cne init --profile demo` | 用真实来源采集少量标的和近期日线，写入独立 demo 湖。查询时使用 `configs/cnequity.demo.toml`。 |
| `cne init` | `quick`，在配置的证券范围内建立最近约三年的主干数据。 |
| `cne init --profile full` | 同一证券范围，更深的历史。默认回填起点为 2016-01-01，实际覆盖依数据集和来源而定。 |

正式初始化前，如需指定数据目录，可先运行 `cne config create --data-root /path/to/lake`。默认会使用当前目录已有的 `configs/cnequity.toml`，或用户级配置。用 `cne config path` 确认生效路径。

真实初始化的耗时取决于历史范围、源端响应和网络。中断后重跑同一条 `cne init` 续跑。结果为 `degraded` 时，用 `cne check` 和 `cne status --datasets` 查看覆盖缺口；[日常更新](../operations/runbook.md#results)说明退出码与验收。

## 查看湖并保持更新 {#browser}

```bash
cne serve --read-only
```

打开 <http://127.0.0.1:8787> 查看样例行、覆盖与运行状态。打开样例湖或演示湖时，添加对应的 `--config`。正式湖完成初始化后，可用 `cne run daily` 更新。

接下来查看[查询与研究](../datasets/query-guide.md)、[数据覆盖](../datasets/catalog.md)、[日常更新](../operations/runbook.md)或[整体设计](../architecture/overview.md)。
