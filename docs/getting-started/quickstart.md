# 快速开始

本指南覆盖两条路径：

1. **一分钟试玩**（推荐新手）：`cne demo`，小宇宙、独立目录，几分钟出真数  
2. **全量数据湖**：`cne config init` → `cne init` → `cne run daily`（耗时长、占磁盘）

详细选项见 [CLI 参考](../reference/cli.md)。安装见 [installation](installation.md)。

## 0. 一分钟试玩（可选）

不必 clone 仓库：

```bash
pip install cnequity
cne demo
# 可选：再看一根完整 1m 会话
# cne demo --intraday
```

如果当前网络无法连接 TDX，可先用确定性的离线样例验证安装、Parquet 落盘和查询链路：

```bash
cne demo --sample
```

该模式不访问网络，生成的合成行全部标记为 `source=mock`，只能用于上手验证，不能用于研究。

会写入独立的 `data/cnequity-demo/` 与 `configs/cnequity.demo.toml`。  
**不要**把 demo 的 `data_root` 拿去跑全量 `cne init`。

接着可查：

```bash
cne query --config configs/cnequity.demo.toml --sql "
  SELECT symbol, trade_date, close, volume, source
  FROM daily_bars
  ORDER BY trade_date DESC
  LIMIT 10
"
```

只想验证复权研究口径，不必初始化全市场：

```bash
cne demo --research --symbols 600519.SH
```

research demo 会把窗口扩展到约三年，读取 Sina 的 hfq 因子，并打印 raw return 与 hfq return 的对照。
它需要额外访问 Sina；网络受限时，先使用不带 `--research` 的基础 demo。

下面从第 1 步起是全量湖路径。

## 1. 准备全量配置

```bash
pip install cnequity   # 若尚未安装
cne config init                 # → configs/cnequity.toml；macOS / Windows 自动 workers=1
# 可选：cne config init --data-root /abs/path/to/lake
cne config validate
```

按需编辑 `configs/cnequity.toml` 里的 `data.root`（生产建议绝对路径）。

> 源码开发：也可 `cp configs/cnequity.example.toml configs/cnequity.toml`，与 `cne config init` 等价。

## 2. 初始化数据湖

```bash
cne init --config configs/cnequity.toml
```

`init` 会：

1. 创建 `{data.root}` 下 staging / curated / derived / meta / duckdb 目录  
2. 初始化 `meta/manifest.db`（SQLite WAL）与 DuckDB 视图  
3. 按 `[job.init.phases]` 执行分阶段全量回填（默认最近 3 年、全市场标的）

需要从 2016 年起的完整初始化时，使用 `cne init --profile full`；也可以先用默认窗口建湖，再按需回填。

**仅建目录、不跑回填：**

```bash
cne init --layout-only --config configs/cnequity.toml
```

**中断后续跑：**

```bash
cne init --resume --config configs/cnequity.toml
# 或指定 run_id
cne retry --run-id <run_id> --config configs/cnequity.toml
```

init 耗时较长（全市场日线分页回填），建议在稳定网络下运行。阶段定义见 [数据流 — Init](../architecture/data-flow.md#init全量回填)。

**它跑到哪了？** 运行中会打这几类行，正常情况下不会连续静默超过 60 秒：

| 行 | 含义 |
|------|------|
| `Step <名字> starting` / `Step <名字> success in Ns` | 步骤进出 |
| `daily_bars: 5,283 symbol(s) over … → 53 batch(es) … on 4 lane(s)` | 这一趟扫描的规模，开跑前就打出来 |
| `daily_bars 8/53 batches · 12,400 rows · 1m24s elapsed · ~7m55s left` | 滚动进度；凑满一轮 lane 后才给剩余时间 |
| `still working: daily_bars 4m12s (no output for 1m02s)` | 心跳，静默满 60 秒时点名当前步骤 |

启动时打印的 `Logging to …/logs/cne-init-<时间戳>.log` 是本次运行的日志文件（目录可用 `CNE_LOG_DIR` 覆盖）。另开一个终端也可以查：

```bash
cne status --run latest --config configs/cnequity.toml
```

**成本按标的数算，不按天数算。** `daily_bars` 是逐标的抓取，所以 `--start D --end D` 只拉一天，付出的仍是全市场（约 5,300 个标的、50+ 个批次）的一整趟扫描，实测十分钟级别，和多年窗口的差别只在每个标的返回多少根 K 线。想快速验证请用 `--symbols` 缩小范围：

```bash
cne backfill daily_bars --symbols 600519.SH,000001.SZ --start 2026-09-15 --end 2026-09-15
```

## 3. 回填验收（推荐，需仓库脚本）

验收脚本在 GitHub 仓库的 `scripts/`，**不随 PyPI 包安装**。有 checkout 时：

```bash
git clone https://github.com/rootSunc/CNEquity.git
cd CNEquity
python scripts/accept_backfill.py snapshot --out /tmp/curated-counts.json
# 同窗口重跑 daily 后对比
python scripts/accept_backfill.py check --compare /tmp/curated-counts.json
```

验收项：幂等性、覆盖起点、消费层可读。详见 [回填完成验收](../operations/runbook.md#回填完成验收)。

纯 PyPI 用户可先用 `cne status --datasets` / `cne stats show` 做粗检。

## 4. 每日增量

日更按**调度组**执行，一天 6 个组。生产就是这么跑的：

```bash
cne run daily --group core --config configs/cnequity.toml
cne run daily --group capital --config configs/cnequity.toml
cne run daily --group signals --config configs/cnequity.toml
cne run daily --group fundamentals --config configs/cnequity.toml
cne run daily --group macro_risk --config configs/cnequity.toml
cne run daily --group research --config configs/cnequity.toml
```

每组末尾含 `compact`，数据会写入 curated。组定义见 [配置 — 调度组](configuration.md#调度组)。
挂 cron 时请**按组错开**（见 [运行手册](../operations/runbook.md)），不要让六个组同时打同一批上游。

> **不带 `--group` 的 `cne run daily` 不等于「全部组」。**
> 它只跑 `[[job.daily.waves]]` 里的核心骨架 —— 行情、日历、交易状态、公司行为、复权 ——
> 不包含估值、财报、融资融券、龙虎榜、北向、指数成分等其余数据集。只跑这一条，
> 湖会安静地停在 15/42 新鲜，且不会报错。跑完会打印一行提示，列出本次没有覆盖的数据集。

非交易日自动跳过（`skipped_non_trading_day`，退出码 0）。

有仓库 checkout 时，`scripts/daily_pipeline.sh` 会按依赖顺序跑完全部分组并做收尾
（健康检查、源探测、元数据备份），一条 cron 即可；该脚本不随 PyPI 包安装。

## 5. 查看状态

```bash
cne status --config configs/cnequity.toml              # 最近一次 run 摘要
cne status --datasets --config configs/cnequity.toml   # 各数据集新鲜度
cne stats show --config configs/cnequity.toml          # 行数统计
```

## 6. 读取数据

### Python API（推荐）

```python
from cnequity.query import load

bars = load(
    "daily_bars",
    start="2024-01-01",
    end="2024-12-31",
    adjust="hfq",
    universe="all_a",
)

roe = load(
    "financial_statement_items",
    items=["roe"],
    as_of="2024-04-30",
)
```

见 [查询指南](../datasets/query-guide.md) 与 [Python API](../reference/python-api.md)。

### DuckDB SQL

```bash
cne query --sql "
  SELECT symbol, trade_date, adj_close
  FROM daily_bars_adj
  WHERE trade_date >= '2025-01-01'
" --config configs/cnequity.toml
```

数据库文件：`{data.root}/duckdb/cnequity.duckdb`。

### 直读 Parquet

```python
import polars as pl
df = pl.scan_parquet("data/cnequity/curated/daily_bars/**/*.parquet")
df.filter(pl.col("symbol") == "600519.SH").collect()
```

## 7. 失败重试

```bash
cne status --config configs/cnequity.toml    # 找到 failed run_id
cne retry --run-id <run_id> --config configs/cnequity.toml
```

retry 只重跑失败 batch；全部成功后自动 compact → derive_adj_factors → audit。

## 8. 生产调度（可选，需仓库脚本）

```bash
# 需 clone 仓库后：
scripts/install_scheduler.sh   # macOS launchd，每天 11:15 本机时间
```

见 [运维 Runbook](../operations/runbook.md)。

## 常见陷阱

| 问题 | 说明 |
|------|------|
| `load()` 读不到新数据 | 确认 run 已 compact；分组 run 必须含 `compact` step |
| `universe="all_a"` 未剔历史 ST | `trading_status` 仅覆盖日更起点之后；2016→上线日回测需注意 |
| init 中途失败 | 勿重新 `init`，用 `--resume` 或 `retry` |
| TDX 连接失败 | `cne sources probe --only tdx_protocol`；检查 `[tdx_protocol.hosts]` 与网络 |
| 缺配置报错 | 先跑 `cne config init` |
| demo 与全量混用 | demo 用独立 `data/cnequity-demo/`，全量另配 `data.root` |

更多排障：[troubleshooting](../operations/troubleshooting.md) · [runbook](../operations/runbook.md)。
