# 运维 Runbook

面向生产日更：调度、健康门禁、备份恢复与回填验收。

> 历史路径 `docs/ops-runbook.md` 保留并重定向至本文。

---

## 组件一览

| 能力 | 脚本 | 作用 |
|------|------|------|
| 调度 | `scripts/daily_pipeline.sh` | 串行跑 6 个 schedule group + 健康检查 + 备份 |
| 调度 | `scripts/install_scheduler.sh` | 安装 macOS launchd（Helsinki 每天 11:15） |
| 调度 | `scripts/uninstall_scheduler.sh` | 卸载 launchd |
| 告警 | `scripts/health_notify.sh` | `audit --full` + `status --datasets` + macOS 通知 |
| 备份 | `scripts/backup_meta.sh` | manifest + state + quality 的 tar 轮换 |

脚本使用仓库 `.venv/bin/cne`，路径相对仓库根目录自解析。

---

## 安装调度

```bash
cd /path/to/cnequity
scripts/install_scheduler.sh
```

- 生成 `~/Library/LaunchAgents/com.cnequity.daily.plist`
- **Europe/Helsinki 每天 11:15** 触发（夏令时 16:15 CST、冬令时 17:15 CST，均在收盘后）
- 非交易日自动跳过（退出 0）
- **漏跑 / 周末补数**：`uv run cne run catchup`（门禁 core + breadth；水位已齐则
  `skipped_already_fresh`），或 `scripts/daily_pipeline.sh YYYY-MM-DD` /
  `CNE_TRADE_DATE=...`（全组定点）
- **海外 Mac**：保 `core`（+ 本地 derive breadth）即可；东财组留给
  国内机器 `catchup --all-groups` / 全组 pipeline。SOCKS 出口不够，见
  [troubleshooting](troubleshooting.md#云主机--socks-能开-ipinfo-但东财-empty-reply)。

```bash
launchctl list | grep cnequity
launchctl start com.cnequity.daily   # 手动触发
scripts/uninstall_scheduler.sh
```

**Linux cron**（建议跑在大陆出口）：

```cron
15 11 * * * /path/to/cnequity/scripts/daily_pipeline.sh
```

**Windows 任务计划程序**（原生 Win10/11；`daily_pipeline.sh` 不适用于 PowerShell）：

1. 先确认 `cne doctor` 与 `cne config validate` 通过，`data.root` 用短绝对路径（如 `D:\lake`）。
2. 打开「任务计划程序」→ 创建基本任务 → 每天 16:05（或收盘后任一时刻）。
3. 操作选「启动程序」：

| 字段 | 示例 |
|------|------|
| 程序/脚本 | `C:\path\to\.venv\Scripts\cne.exe` |
| 添加参数 | `run daily --config C:\path\to\configs\cnequity.toml` |
| 起始于 | `C:\path\to`（仓库或配置所在目录） |

多 group 时建多个任务，或写一个 `.ps1` 顺序调用：

```powershell
$cne = "C:\path\to\.venv\Scripts\cne.exe"
$cfg = "--config C:\path\to\configs\cnequity.toml"
& $cne run daily --group core $cfg
& $cne run daily --group capital $cfg
# …其余 group
& $cne audit --full $cfg
```

> 控制台中文乱码时：`chcp 65001`，或设置用户环境变量 `PYTHONUTF8=1`。

---

## 每日 Pipeline

```
core → capital → signals → fundamentals → macro_risk → research
  → health_notify.sh
  → backup_meta.sh
  → group summary（gate vs soft）
```

- 单组失败不中断后续组（尽量多采数据）
- 结尾摘要区分 **gate**（默认 `CNE_GATE_GROUPS=core`）与 **soft**（东财等）
- 默认 `CNE_SOFT_FAIL_OK=1`：gate OK 时 soft 失败 **warn-only、exit 0**（海外 Mac 预期东财滞后）；
  国内全组日更可设 `CNE_SOFT_FAIL_OK=0` 让 soft 失败仍 exit 1
- 东财超时/连接失败不重试（`[sources.eastmoney] timeout_sec`，默认 15s）
- 生产 `daily_pipeline.sh` 常设 `workers=1`（TDX 客户端与多进程兼容性）

组与 step 映射见 [配置 — 调度组](../getting-started/configuration.md#调度组)。

---

## 日志

目录：`{data.root}/logs/`

| 文件 | 内容 |
|------|------|
| `daily-YYYYMMDD.log` | 各组 cne 输出 |
| `health-YYYYMMDD.log` | audit / status 全文 |
| `launchd.out.log` / `launchd.err.log` | launchd 标准流 |

---

## 日常巡检命令

```bash
cne status --datasets          # 新鲜度；STALE 时退出 1
cne audit --full               # 湖级健康；UNHEALTHY 退出 1
cne catalog                    # 行数概览
```

---

## 失败处置

1. 查看 `daily-*.log` 定位失败组
2. 重跑单组：`cne run daily --group <name>`
3. 批级失败：`cne status` → `cne retry --run-id <id>`
4. 复核：`cne audit --full` + `cne status --datasets`

详见 [故障排查](troubleshooting.md)。

---

## 服务目标（SLO）

| 指标 | 目标 |
|------|------|
| 日更成功率 | 两周内 ≥99% 交易日 pipeline 退出 0 |
| 告警时效 | 失败当次 run 结束分钟内通知 |
| 新鲜度 | T+1 `status --datasets` 无 STALE（季频数据集按 `max_staleness_days`） |

---

## 备份与恢复

**备份**：`manifest.db` + `meta/state/` + `meta/quality/`

**不备份**：`adj_factors_cache`（可 derive 重算）、`locks`、curated parquet（可重采）

```bash
scripts/backup_meta.sh
scripts/backup_meta.sh "" /Volumes/ext/cne-bak 30
```

**恢复**：

```bash
cd data/cnequity/meta
tar -xzf ../backups/meta-YYYYMMDD-HHMMSS.tar.gz
cne status    # 确认水位恢复
cne run daily --group core   # 增量续采
```

默认备份在湖内，磁盘级容灾请将 `CNE_BACKUP_DIR` 指到湖外。

---

## 环境变量（仅 `scripts/*.sh`）

下列变量由 [scripts.md](scripts.md) 中的 shell 脚本读取；**`cne` CLI 本身不读**（请用 `--config`）。

| 变量 | 默认 | 作用 |
|------|------|------|
| `CNE_CONFIG` | `configs/cnequity.toml` | 脚本传给 `cne --config` 的路径 |
| `CNE_LOG_DIR` | `{data.root}/logs` | 日志 |
| `CNE_GROUPS` | 全部 6 组 | 覆盖 pipeline 组列表 |
| `CNE_NOTIFY` | `1` | `0` 关闭通知 |
| `CNE_BACKUP_DIR` | 湖内 backups | 备份目录 |
| `CNE_BACKUP_RETENTION_DAYS` | 14 | 保留天数 |

---

## 数据湖目录（init 后）

```
{data.root}/
  staging/
  curated/
  derived/
  meta/manifest.db
  meta/quality/
  meta/source_snapshots/
  meta/on_demand/
  duckdb/cnequity.duckdb
```

---

## 分组 cron 示例

分组模式（`--group`）各组末尾会自动 compact→audit，数据写入 curated：

```cron
# 核心参考 + 行情 + 派生（周一至周五 16:05）
5 16 * * 1-5 cd /path/to/cnequity && cne run daily --group core --config configs/cnequity.toml

# 资金面（16:35）
35 16 * * 1-5 cne run daily --group capital --config configs/cnequity.toml

# 信号类（17:05）
5 17 * * 1-5 cne run daily --group signals --config configs/cnequity.toml
```

生产更推荐用 `scripts/daily_pipeline.sh`（见上文），它会串行跑完全部组并做健康检查与备份。

---

## 收尾补抓

**`daily_pipeline.sh` 已经内建了这一步**，不用额外配 cron。跑完全部组之后它会：

1. 用 `cne status --datasets` 探一下（有 STALE 退出 1）——干净的日子到此为止，零成本
2. 有 STALE 才等 `CNE_STALE_RETRY_DELAY_SEC`（默认 1800 秒）
3. 然后 `cne run daily --stale-only`，只重抓仍然落后的

排在健康检查**之前**，所以补抓成功就不会误报。

收尾补抓完成后，`daily_pipeline.sh` 会执行一次 `cne stats rebuild`，让前端的日期/分区清单直接反映本日最晚落盘的 compact 结果。这个重建对日更是否成功本身不设门禁；失败会记到日志，但不会让已经写完的数据回滚。

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `CNE_STALE_RETRY` | `1` | 设 `0` 关闭 |
| `CNE_STALE_RETRY_DELAY_SEC` | `1800` | 补抓前等多久 |

**为什么需要它。** `snapshot` 数据集（`valuation_metrics`、`fund_flow`、`sector_members`、`analyst_consensus` 等）只抓 run 当天——源端在那一个调度窗口里中断，那天就**永久没了**，后面任何一次 run 都补不回来（重放会伪造行，这是 `fetch_semantics` 的设计）。

这不是重试不够：`clist.py` 的 per-host 重试加退避一直都在，`valuation_metrics` 在 2026-07-30 / 07-31 是把所有 host 的重试都耗尽了。缺的是**当天的第二个窗口**。

**等待本身就是重点。** 立刻重试大概率撞上同一场中断，所以先睡再抓——但只在真有 STALE 时睡。

更糟的是它很安静：默认 `CNE_SOFT_FAIL_OK=1`，gate 正常时 soft 组失败只告警、退出 0。上面那两天就是这样过了三天没人发现。补抓失败同样算 soft，会出现在分组汇总的 `stale-retry:` 一行里。

### 中断持续几小时怎么办

脚本内的等待是分钟级的。源端挂半天的话，再加一行独立 cron：

```cron
5 20 * * 1-5 cd /path/to/cnequity && cne run daily --stale-only --config configs/cnequity.toml
```

`--stale-only` 没有落后的数据集时不建 run、直接退出 0，重复挂无害。

配套的可见性：

```bash
cne status --datasets     # 有 STALE 退出 1
cne serve                 # 面板首屏就列出 STALE 数据集
```

---

## Init 与资源

```bash
cne init --config configs/cnequity.toml
```

2016 起全量 init 大约 1.5–2.5 小时（TDX 分页 + Sina 复权；compact 内存尖峰约 2 GB）。
macOS 上必须 `[orchestrator].workers = 1`（TDX 客户端与 `ProcessPoolExecutor` 不兼容；
`cne config validate` 在 Darwin 上会拒绝 `workers>1`）。
Windows 上 `cne config init` 同样默认 `workers = 1`（spawn 可用，但首次建议单进程）；
需要时可自行提高，validate 不会拦截。
单实例、收盘后运行。

---

## 日内数据（minute_bars / minute_bars_5m）

**默认关闭，不在任何 cron 上。** 开启前先看清成本——下面全部是实测值，不是估算。

### 磁盘

实测 1m 26.9 B/行、5m 23.9 B/行（zstd，按日分区）。

| 范围 | 1m | 5m |
|------|-----|-----|
| 全市场（约 5,400 只） | 35 MB/日、**8.4 GB/年** | 6 MB/日、**1.5 GB/年** |
| 沪深300（默认 scope） | 2 MB/日、0.5 GB/年 | 0.4 MB/日、0.1 GB/年 |

参照：现有全部日频数据（2001–2026）共 468 MB。**全市场 1m 一年 ≈ 现有整个湖的 18 倍。**

### 耗时

TDX 一次请求返回 800 根 bar，所以 1m 每股每天只要 1 次请求（240<800），5m 一次覆盖 16 个交易日。实测吞吐（`min_interval_ms = 100`）：

| `fetch_workers` | 实测吞吐 |
|-----------------|---------|
| 1（默认） | 4.5 req/s |
| 4 | 10.1 req/s |

10 req/s 就是 100ms 限速器允许的上限——**并发不提高请求速率**（限速器跨进程，无论几个连接都在计数），只是让单条通道不再空等网络往返。

| 任务 | 请求数 | @1 worker | @4 workers |
|------|--------|-----------|-----------|
| 1m 全市场**日更** | 5,400 | 约 20 分钟 | 约 9 分钟 |
| 5m 全市场日更 | 5,400 | 约 20 分钟 | 约 9 分钟 |
| 1m 全市场种子（95 天） | 156,600 | 约 9.7 小时 | 约 4.3 小时 |
| 5m 全市场种子（491 天） | 162,000 | 约 10 小时 | 约 4.5 小时 |
| 1m CSI300 种子（95 天） | ~9,300 | 约 50 分钟 | 约 15 分钟 |

**日更完全可以挂 cron**；贵的是一次性种子。TDX 分钟线是**从今天往回翻页**的，所以种子按**标的**分片（`backfill_chunk_symbols=200`），不是按日期——按日期切会让每一片都重新走过 tip→start。失败时打印 `resume_from_symbol`。

`fetch_workers` 默认 4：限速器仍把全局限在 ~10 req/s，多开的连接只消网络空转。与 `[orchestrator].workers` 无关——日内用线程而非进程池，macOS 上同样可用。

**这个顾虑后来被坐实了。** 真跑一次全市场种子（7,747 只标的，1m 与 5m 各一次）：1m 在约 44 分钟、约 600 次重连后撞上一次连接超时，异常直接炸穿整个 step，之前已抓到但未 compact 的批次全部作废；5m 那次没有报错，但一小时后全部 7,747 只标的返回零行——是 TDX 主机在持续高频重连下开始软性拒绝，不是真的没数据。根因是 `fetch_minute_bars` **按每 50 个标的一批就重开连接**，全市场一次种子就是约 155 次重连。

已修复两处：单次连接失败会**重试一次**（换一台服务器，不会对着刚超时的那台再撞一次）；**一整批失败不再炸穿整个 step**，该批标的记入 `failed_symbols`，扫描继续（和单个标的失败早就有的容错是同一个契约，只是粒度粗一级）。批大小顺带从 50 提到 200，全市场重连次数降到约 1/4。仍然建议**先跑小范围验证一轮，再上全市场**，尤其是打算多开 `fetch_workers` 的时候。

### 挂上去

```toml
[minute_bars]
enabled = true
scope = "index:000300.SH"   # 或 "watchlist" + symbols，或 "all"
frequencies = ["5m"]        # 5m 是唯一有真历史的频率
fetch_workers = 4
```

```bash
# 一次性种子（分片、可续跑）
cne backfill minute_bars_5m --start 2024-08-01 --end 2026-07-31

# 只拉几只，不改配置（--symbols 会临时覆盖 scope 并开启本次抓取）
cne backfill minute_bars_5m --start 2026-05-01 --end 2026-07-31 \
  --symbols 600519.SH,000001.SZ

# 日更：单独一个 group，不要塞进 core
cne run daily --group intraday
```

越过源端视野的 `--start` 会被直接拒绝并给出可用起点——见 [catalog.md 历史视野](../datasets/catalog.md)。

---

## 回填完成验收

Init 或首次全量回填 compact + derive 成功且 `cne status` 为 success 后，在同一维护窗口内做下列检查，再挂 cron / 接下游。

### 前置

```bash
cne status --config configs/cnequity.toml          # success，failed batch = 0
cne audit  --config configs/cnequity.toml          # 无 mock_source / pk_duplicate error
ls data/cnequity/curated/daily_bars/       # 应有 trade_date=YYYY-MM-DD 分区
```

若配置里 `[adj_factors].adjust_types` 只有 `qfq` 而你要用后复权，先追加 `"hfq"` 并重跑
`cne derive adj_factors`。

### 幂等

```bash
.venv/bin/python scripts/accept_backfill.py snapshot \
  --config configs/cnequity.toml --out /tmp/curated-counts.json

cne run daily --config configs/cnequity.toml

.venv/bin/python scripts/accept_backfill.py check \
  --config configs/cnequity.toml --compare /tmp/curated-counts.json
```

核心数据集（`daily_bars`、`instruments`、`adj_factors` 等）行数应与重跑前一致。

### 口径抽查

```bash
.venv/bin/python scripts/accept_backfill.py check \
  --config configs/cnequity.toml \
  --symbol 600519.SH --start 2024-01-01 --end 2024-12-31
```

对照行情软件的未复权 close 与后复权 adj_close（除权日前后各抽一天）。

### 按年覆盖

```bash
.venv/bin/python scripts/accept_backfill.py check --config configs/cnequity.toml
# 看 === daily_bars by year ===
```

正常形态：2016→近年 symbols 缓增，每年 `rows ≈ symbols × ~240` 交易日，无单年腰斩。
若某年明显低于中位数 70%，对该年窗口做 `cne backfill daily_bars` 或 targeted retry。

### 消费层冒烟

```python
from cnequity.query import load

raw = load("daily_bars", start="2024-06-01", end="2024-06-30")
tradable = load(
    "daily_bars",
    start="2024-06-01",
    end="2024-06-30",
    adjust="hfq",
    universe="all_a",
)
assert tradable.height < raw.height
assert "adj_close" in tradable.columns
```

### 验收 checklist

| # | 项 | 通过标准 |
|---|-----|----------|
| 1 | 幂等 | 同窗口重跑后核心数据集 row count 不变 |
| 2 | 口径 | 标杆股 close/adj_close 与行情软件一致（人工） |
| 3 | 覆盖 | 按年行数无异常断崖；2016 起分区连续 |
| 4 | 消费 | `load(..., universe="all_a")` 剔除 ST/停牌；`adj_close` 可算 |
| 5 | 审计 | 最新 run audit 无 error；`source=mock` 行数 = 0 |

---

## 备源策略

1. 主源失败 → batch 退避重试（最多 3 次）
2. 仍失败 → 标记 batch failed；可选备源写入 `meta/source_snapshots`
3. `cne audit` 对比主源与 snapshot，由人决定是否切源
4. 不要静默用备源覆盖 curated canonical 行

---

## 相关文档

- [脚本说明](scripts.md)
- [故障排查](troubleshooting.md)
- [Schema 契约](../datasets/schema.md)
- [逐源限制](../datasets/sources.md)
