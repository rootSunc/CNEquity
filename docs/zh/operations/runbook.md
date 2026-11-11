# 运行手册

面向已建立正式湖的使用者：调度、健康门禁、备份恢复与回填验收。首次建湖见[初始化指南](../getting-started/initialization.md)。

| 任务 | 入口 |
|---|---|
| 定时更新 | `cne schedule install`：每个交易日日更，然后维护（审计、门禁、元数据备份），见[安装调度](#install-schedule) |
| 临时失败 | [排障](troubleshooting.md)，先确定失败 run / 批次，再按范围重试 |
| 扩大历史 | [初始化与续跑](../getting-started/initialization.md)，回填先审阅 `--plan` |


## 安装调度 {#install-schedule}

```bash
cne schedule install        # 确认后为当前用户注册定时任务
cne schedule status         # 是否已安装、是否在运行、最近一次检查
cne schedule remove         # 停用并注销；正在运行的任务继续
```

定时任务属于当前用户：macOS 用 launchd，Linux 用 crontab，Windows 用任务计划程序。它每分钟检查一次；每个交易日过了 `[job.daily] run_at`（北京时间）跑 `cne run daily`，跑完再跑 `cne run maintain`；过了 `[job.stale] run_at` 只重试当天缺的快照数据。一个交易日只算一次：过了 `run_at` 就跑，直到下一个交易日 09:15 开盘前都算这一天；本机时区、夏令时都不影响，电脑睡着时不跑，醒来后的下一次检查会补上。

`--pack` 只更新所选研究包的调度组（不含事件流）；`--events` 按配置的 [`[[job.events.schedules]]`](../getting-started/configuration.md#event-schedules) 跑事件流（可写多条，每条有自己的间隔、随机延迟和静默时段）；配置没写计划时，`--events-group` 另按 `--events-every` 分钟间隔跑一个事件组；`--no-stale` 不装收尾补抓。没有给出的选项保留现有设置，所以在命令行安装不会撤掉面板里设置的备份或事件流。这和 `cne serve` 操作页管理的是同一个定时任务，两边都能查看和修改。检测到旧的 `scripts/scheduler` launchd 任务时会提示先卸载，否则同一个湖会更新两遍。

### 日更之后的维护（`cne run maintain`） {#maintain}

定时任务在每个交易日的日更之后跑一次 `cne run maintain`，手工也可以运行：

1. 最近收盘交易日各日更组的运行结果：`[job.maintenance] gate_groups`（默认 `core`）失败立即判失败；其他组失败只报告，连续失败 `soft_fail_max_days`（默认 3）个交易日后判失败，计数在 `meta/state/soft_streak/`，旧脚本的计数会被沿用；
2. 运行审计；`[job.maintenance] full_audit_weekday`（默认周六）改为全湖审计 `audit --full --quality-only`，它读全部 Parquet，大湖可能要几个小时；
3. 新鲜度门禁 `status --datasets --gate`，只对定时日更实际跑的调度组判失败（`--groups` 可改）；
4. 源探针 `sources probe --stale-only`（带上 `[source_health] vantage` 出口标签）与 `sources slo`；
5. 最近 20 个交易日的运行证据 `verify --runs --days 20`；
6. 元数据备份：manifest 数据库（用 SQLite 备份接口取一致副本）、增量状态、修订与来源收据、质量结论，写成 `meta-<时间>.tar.gz`，放在 `[job.maintenance] backup_dir`（默认 `{data.root}/backups`），按 `backup_retention_days` / `backup_retention_count` 轮换；
7. `cne run clean` 的过期文件预览。

核心组失败、其他组连续失败达到上限、审计、新鲜度门禁或备份失败时退出 1；源探针和运行证据的问题只报告，不判失败。macOS 上定时任务加 `--notify`，有问题时弹出桌面通知。

## 旧版仓库脚本（已移除） {#legacy-scripts}

`scripts/scheduler/`（`daily_pipeline.sh`、`install_scheduler.sh`、`health_notify.sh`、`backup_meta.sh` 等）已移除，它做的事由打包定时任务承担：日更、收尾补抓、事件流，以及 `cne run maintain` 的审计、门禁、源探针和元数据备份。机器上还装着旧 launchd 任务时，先卸载再装新任务：

```bash
for job in com.cnequity.daily com.cnequity.stale com.cnequity.events com.cnequity.events-news; do
  launchctl bootout "gui/$(id -u)/$job" 2>/dev/null
  rm -f ~/Library/LaunchAgents/$job.plist
done
cne schedule install
```

cron 里调用这些脚本的条目手工删掉。`cne schedule status` 检测到旧 launchd 任务时会提示。旧脚本的环境变量改写成配置：`CNE_GATE_GROUPS` → `[job.maintenance] gate_groups`，`CNE_BACKUP_DIR`、`CNE_BACKUP_RETENTION_DAYS`、`CNE_BACKUP_RETENTION_COUNT` → `[job.maintenance] backup_dir`、`backup_retention_days`、`backup_retention_count`。要按日重跑全部调度组，用 `cne run daily --trade-date YYYY-MM-DD`。

<a id="web-schedule"></a>

### Web 管理跨平台定时任务

安装包用户可在 `cne serve` 的操作页设置定时任务，无需复制仓库脚本：选择自动日更、收尾补抓、每日数据备份或独立事件流，设置时间和范围，先预览再确认。日更与补抓每个交易日各尝试一次；备份按北京时间的自然日到点后尝试一次，选择数据集及备份目录；事件流选择配置中的事件组，按 1–1440 分钟间隔运行，包含周末和节假日。系统每分钟检查一次，结果和日志显示在最近任务中，关闭 serve 后仍会运行。所有 Web 任务共享单任务槽，占用时等待后续检查；事件流不累积补跑，失败也从尝试开始时计算间隔。

macOS 使用当前用户的 launchd，Windows 使用当前登录用户的任务计划程序，两者均须用户保持登录；Linux 使用当前用户的 crontab，并须 cron 服务运行。系统睡眠时不执行，恢复后的下一次检查会在交易日窗口内补跑日更与补抓；备份只补当天，事件流按当前间隔判断。页面同时显示系统任务状态和最近检查；超过 20 分钟未检查时，排查用户登录、机器休眠和 cron 服务。系统注册成功表示任务定义已安装，最近检查记录才说明系统实际触发过。

日更与补抓时间保存在原配置的 `[job.daily] run_at` / `[job.stale] run_at`，首次改动前保存 `.schedule.bak`，其他设置和注释保留。备份和事件流设置随湖保存；备份不自动删除旧副本，需关注存储空间。暂停阻止后续自动执行，已经启动的任务在任务详情里另行取消。即使系统任务因权限变化无法删除，暂停设置仍能阻止随包入口继续取数；页面会提示手动核对系统任务。

Web 只管理自己创建的系统任务，不接管已有 launchd 脚本或其他 cron / Windows 任务。迁移前核对并停用重复入口。Web 日更执行 `cne run daily`，之后执行 `cne run maintain`（审计、门禁、元数据备份），每日备份执行所选数据集的 `cne snapshot create`，独立事件流执行所选组的 `cne run events`。命令行用 `cne schedule install|status|remove` 管理的是同一个任务。系统任务固定使用创建时的 Python 环境、工作目录及配置路径；移动环境后重新设置，数据源所需配置也须在无人值守环境中可用。

**Windows 任务计划程序**（原生 Win10/11，手工配置方案）：

1. 先确认 `cne doctor` 与 `cne config validate` 通过，`data.root` 用短绝对路径（如 `D:\lake`）。
2. 打开「任务计划程序」→ 创建基本任务 → 每天 16:05（或收盘后任一时刻，周末也触发）。
3. 操作选「启动程序」：

| 字段 | 示例 |
|------|------|
| 程序/脚本 | `C:\path\to\.venv\Scripts\cne.exe` |
| 添加参数 | `run daily --config C:\path\to\configs\cnequity.toml` |
| 起始于 | `C:\path\to`（仓库或配置所在目录） |

这一条已包含事件流（公告、监管事件、资讯），不需要再为 `run events` 另建任务；非交易日日更组自动跳过，事件流照常运行。手工配置的任务计划程序和 cron 用本机时间；只有打包定时任务（`cne schedule install`）按配置的北京时间判断。想让事件流更频繁时，可以另外调度 `cne run events --group news_wire`；它与日更同时运行时，日更里的事件流段记为 `skipped_locked`，不算失败。

> 控制台中文乱码时：`chcp 65001`，或设置用户环境变量 `PYTHONUTF8=1`。

## 每日 Pipeline

```
core → capital → signals → fundamentals → macro_risk → research
  → cne run maintain（审计、门禁、源探针、元数据备份）
```

- 单组失败不中断后续组（尽量多采数据）
- `[job.maintenance] gate_groups`（默认 `core`）失败判当天失败；其他组失败只告警，连续失败 `soft_fail_max_days` 个交易日后判失败，见[日更之后的维护](#maintain)
- 东财超时/连接失败不重试（`[sources.eastmoney] timeout_sec`，默认 15s）
- macOS 上 `workers` 须为 1（TDX 客户端与多进程兼容性）

组与 step 映射见 [配置 — 调度组](../getting-started/configuration.md#调度组)。

## 日志

目录：`{data.root}/logs/`

| 文件 | 内容 |
|------|------|
| `daily-YYYYMMDD.log` | 各组 cne 输出 |
| `health-YYYYMMDD.log` | audit / status 全文 |
| `launchd.out.log` / `launchd.err.log` | launchd 标准流 |

## 日常巡检命令

```bash
cne status --datasets --gate                 # 新鲜度；STALE 时退出 1
cne audit --full                      # 湖级健康；UNHEALTHY 退出 1
cne stats show                        # 行数概览
cne sources slo --enforce             # 30 日关键源可用性（缺历史也 fail-closed）
cne sources resilience --enforce      # 核心数据集独立备援门禁与单源爆炸半径
cne verify --runs --days 20 --enforce # 连续交易日运行证据
```

`cne serve` 的操作页可以启动其中的新鲜度、全湖审计和 `cne verify`，以及下面的重跑、重试和补抓。页面同一时间只跑一个任务，并且会占用调度脚本的同一把锁，定时任务碰到它会跳过并在下一小时再判断。



## 失败处置

1. 查看 `daily-*.log` 定位失败组
2. 重跑单组：`cne run daily --group <name>`（操作页的「跑一个调度组」是同一条命令）
3. 批级失败：`cne status` → `cne run retry --run-id <id>`（跑批详情里的「重试此 run」）
4. 复核：`cne audit --full` + `cne status --datasets --gate`

从操作页启动的日更和定时任务用同一把调度锁，目录是 `{data.root}/locks`（`CNE_SCHEDULER_LOCK_DIR` 或 `CNE_LOCK_DIR` 仍可改到别处）。只跑一个调度组、补跑或 stale-only 不会把这一天记成“定时日更已跑过”；跑完全部调度组、而且已经过了当天的 `run_at`，才会写这个标记。

`degraded` 表示覆盖受限；写命令返回 0，显式质量门禁可返回 2，仍需要查看缺口步骤：成功发布的表仍可读取，但不代表组内全部数据到齐。
例如暂停 push2 后，资金流会尝试把同花顺口径写入 `fund_flow_ths` / `sector_fund_flow_ths`；
原东财表仍报告缺失，不能把备援表当作同口径替换。指定 `--run-id` 可重试该降级 run 的失败批次。
跨日补跑应使用原 run 的重试入口；实时快照仍受可观测时间窗限制，不能补造过去的快照。

详见 [故障排查](troubleshooting.md)。

## 服务目标（SLO）

| 指标 | 目标 |
|------|------|
| 日更成功率 | 两周内 ≥99% 交易日 pipeline 退出 0 |
| 告警时效 | 失败当次 run 结束分钟内通知 |
| 新鲜度 | T+1 `status --datasets --gate` 无 STALE（季频数据集按 `max_staleness_days`） |

## 备份与恢复

操作页提供“创建数据备份”、备份目录与列表、“校验”及“恢复到新目录”。创建时明确选择已发布的数据集；可选择湖外磁盘目录，也可在定时设置中启用每日备份。湖内新备份仅允许放在 `meta/snapshots` 或 `backups`，避免混入业务数据目录。列表只读取清单，不代表文件已经通过完整校验。校验和恢复均作为独立任务运行，进度与结果可在任务详情查看。

Web 备份复用可移植湖快照，包含所选数据及对应状态、契约、修订与血缘信息，不包含原 TOML 配置、凭据或完整运行数据库；它不等于整个系统的容灾镜像。恢复先展示备份范围和目标，确认后完整校验文件摘要，再复制到当前数据湖和备份目录之外的新目录或空目录。清单在预览后变化须重新预览，校验失败不恢复。恢复后 serve 仍使用原数据湖；用独立配置验收恢复目标，确认后再切换。备份不会自动删除旧副本；磁盘级容灾需选用外部存储并另行保存配置和凭据。

`cne run maintain` 的元数据备份只包含 manifest、state、quality、revision 收据、来源快照和运行证据；
**不包含** `curated/`、`derived/` 或 `meta/revisions/data/` 中的数据版本文件。
有些历史可重采，有些快照型数据错过窗口后无法从原源补回，因此不能把元数据归档当作
完整湖备份。需要恢复研究数据时，另建并校验可移植快照，或对整个湖做一致性备份。

归档默认在湖内 `{data.root}/backups`；磁盘级容灾把 `[job.maintenance] backup_dir` 设为湖外目录。
归档按 `backup_retention_days` 和 `backup_retention_count` 中更严格的限制轮换。

恢复前先停采集，解包到隔离目录检查内容，并确认有与收据匹配的数据文件；
不要将旧元数据直接覆盖到仍在运行或数据版本不匹配的湖：

```bash
mkdir -p /tmp/cnequity-meta-review
tar -xzf /path/to/offsite/cne-meta/meta-YYYYMMDD-HHMMSS.tar.gz \
  -C /tmp/cnequity-meta-review
```

核对后按实际恢复方案处理；仅恢复元数据不能重建缺失的 Parquet 或 revision generation。

需要冻结可复现实验所依赖的 Parquet 时，使用带校验和、revision receipt、契约指纹和
运行 lineage 的可移植快照：

```bash
cne snapshot create research-20260828 \
  --dataset daily_bars --dataset instruments --dataset trading_status
cne snapshot verify research-20260828
cne snapshot restore research-20260828 /new/empty/cnequity-restore
cne config create --config configs/cnequity.restore.toml \
  --data-root /new/empty/cnequity-restore
cne status --datasets --gate --config configs/cnequity.restore.toml
```

恢复命令只接受新目录或空目录，拒绝活动湖根目录，也不会覆盖已有文件。验收时
使用**指向恢复目标**的独立配置；再执行研究消费者契约测试，确认后才切换。

## 20 个交易日验收

`cne verify --runs` 从权威 `trading_calendar` 取最近窗口，每个交易日汇总所有 `daily:*` 运行，
按数据集和 stage 取最新的 receipt，只判 12 个核心档数据集（见[支持档位](../datasets/catalog.md#支持档位)），
不管它们由哪个调度组采集。缺 run、核心数据集失败或被阻塞、或只有旧 `warning` 且没有 dataset receipt，
都会失败；非核心数据集降级或失败不影响通过。档位按当前注册表判定，历史日期也按同一标准重算。报告写入
`meta/stability/latest.json` 和不可变历史目录。不得手工补写或把日历日当交易日。

## 环境变量

这些变量由 `cne` 本身读取。

| 变量 | 默认 | 作用 |
|------|------|------|
| `CNE_CONFIG` | `configs/cnequity.toml` | 所有命令 `--config` 的默认值（显式 `--config` 优先）；调度器中请设绝对路径，不能仅靠相对路径免除 `cd` |
| `CNE_LOG_DIR` | `{data.root}/logs` | 日志；长跑的 `cne` 命令也会在这里留一份 |

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

## 分组 cron 示例

分组模式（`--group`）各组末尾会自动 compact→audit，数据写入 curated：

公共配置的常规日更组如下；自定义配置以 `[job.daily.groups]` 为准。

| `--group` | 更新内容 |
|---|---|
| `core` | 股票池、交易日历与状态、公司行动、股票及指数日线、复权与行业指数派生 |
| `capital` | 资金流、北向持仓与流量、融资融券、估值、板块成员 |
| `signals` | 龙虎榜、大宗交易 |
| `fundamentals` | 财务报表、预约披露日程、指数与行业成员、股本、股东户数 |
| `macro_risk` | 宏观指标、市场宽度、解禁日程、商品行情 |
| `research` | ETF 档案、机构持仓、分析师预期、热度、板块行情与资金流、情绪评分 |

`cne run daily` 顺序运行这些组，单组失败后仍继续后续组，最后跑事件流。
失败优先返回 1；没有失败但有降级时返回 2；全部成功或正常跳过时返回 0。
配置为周更的组按交易日历运行历史数据步骤，快照步骤仍每日运行。
北向等数据遵循上游实际披露频率，组每天运行不表示每张表都有新日期。
情绪评分依赖已落盘的资讯；公告、监管事件与新闻由同一条命令末尾的事件流更新（也可单独 `cne run events`）。
前十大股东不在默认日更组中，按需使用 `cne backfill top_holders`。

```cron
# 核心参考 + 行情 + 派生（周一至周五 16:05）
5 16 * * 1-5 cd /path/to/cnequity && cne run daily --group core --config configs/cnequity.toml

# 资金面（16:35）
35 16 * * 1-5 cd /path/to/cnequity && cne run daily --group capital --config configs/cnequity.toml

# 信号类（17:05）
5 17 * * 1-5 cd /path/to/cnequity && cne run daily --group signals --config configs/cnequity.toml
```

生产更推荐用 `cne schedule install`（见上文）：它每个交易日串行跑完全部组，再用 `cne run maintain` 做审计、门禁与备份。

## 收尾补抓

打包定时任务在 `[job.stale] run_at`（默认北京时间 21:00）之后跑一次 `cne run daily --stale-only`，只处理仍落后的 snapshot 数据集，避免主日更任务内等待造成调度超时；与主任务共用调度锁，重叠时跳过。`cne schedule install --no-stale` 不装这一步。

补抓遵循与主任务相同的 push2 配置，不额外关闭该来源。被封出口可在个人配置中设置 `[sources.eastmoney] push2_paused=true`；已触发的共享熔断和预算仍会阻止请求。`CNE_SOURCE_VANTAGE` 是当前出口的稳定标签（默认 `local`），不要把海外样本标成 `cn`。

**为什么需要它。** 快照型日更只抓运行当天；同日第二个窗口可以补当天失败的采集。
错过后不能重放旧 `--trade-date` 来伪造当时观察。部分数据集有独立历史回补源，
但其来源、覆盖和 PIT 语义可能与当日快照不同；先查 `history_mode` 和
`backfill_source`，不要把所有快照型数据一概判为可补或永久不可补。

**错开窗口是重点。** 立刻重试大概率撞上同一场中断，因此补抓由独立任务在较晚时间发起。

非门禁组当日失败只告警；连续失败会按配置升级。补抓有自己的退出码与日志。

### 中断持续几小时怎么办

把 `[job.stale] run_at` 设得更晚，或在恢复后手工运行一次：

```bash
cne run daily --stale-only
```

`--stale-only` 没有落后的数据集时不建 run、直接退出 0，重复挂无害。

配套的可见性：

```bash
cne status --datasets --gate # 有 STALE 退出 1
cne serve             # 面板首屏就列出 STALE 数据集
```

## Init 与资源

```bash
cne init --config configs/cnequity.toml
```

全市场初始化会按标的和源端历史范围分页，耗时及磁盘用量取决于范围、缓存、可达性和失败续跑。先检查配置和空闲空间；单数据集回填另有 `backfill --plan`；正式任务保持单实例运行。macOS/Windows 上 `cne config create` 默认采用保守的 worker 数，修改前先用 `cne config validate` 核对。

## 日内数据（minute_bars / minute_bars_5m）

**默认关闭，不在默认调度里。** 分钟数据的请求数与证券数、源端可用窗口和分页深度一起增长；即使只选一天，全市场也要扫很多证券。落盘量也会随保留频率与日期增长，运行中还需为 staging 和修订留空间。先用少量 `--symbols` 做真实范围验证，并用 `backfill --plan` 核对源开关、窗口与切片。

TDX 分钟线从当前 tip 向过去翻页，因此种子按**标的**分片（`backfill_chunk_symbols=200`）；按日期分片会重复读取较新的页。请求间隔和最大在途数跨进程共享；提高 `fetch_workers` 只可能减少空等，不能提高配置的请求速率，更不等于源方允许该速率。连接或一批证券失败会记入失败范围供续跑，不应通过无限重试或切换出口继续冲击同一源。分钟线建议先做小范围验收，再扩到大范围；实际批次进度和缺口报告比静态耗时表可靠。

### 挂上去

```toml
[minute_bars]
enabled = true
scope = "index:000300.SH" # 或 "watchlist" + symbols，或 "all"
frequencies = ["5m"]      # 5m 是唯一有真历史的频率
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

分钟线验收不能只看最后日期。审计会在已启用的频率和配置证券范围内，用正成交量日线
检查整日缺失，报 `minute_bars_missing_session`；无成交日不作为缺失证据。现有分钟记录
的盘中完整性、时段和量额对照仍单独检查。默认回看 7 个自然日，长窗口恢复应另做全窗口验收。

每次取数的完整失败名单和空返回名单保存在运行 manifest 的
`metrics.stages.<dataset>.source_metrics.tdx_protocol`，字段为 `failed_symbols`、
`empty_symbols`，并附带 `frequency/start/end`。失败会沿用引擎的 warning/degraded 门禁；
空返回只能说明未取到记录，必须与日线或来源停牌证据核实，不能直接认定正常停牌。
整批全空仍判失败，并清除进程缓存的 TDX 主机，使后续重试重新检查可访问性；不在失败点
无限重试。量额对照除市场中位数外，也以原有 0.95..1.05 容差检查个别证券日，报
`minute_bars_daily_outliers`。差异不直接证明分钟源错误，应保留日线与分钟原始证据后裁定。

## 回填完成验收

Init 或首次全量回填 compact + derive 成功且 `cne status` 为 success 后，在同一维护窗口内做下列检查，再挂 cron / 接下游。

### 前置

```bash
cne status --config configs/cnequity.toml # success，failed batch = 0
cne audit  --config configs/cnequity.toml # 无 mock_source / pk_duplicate error
ls data/cnequity/curated/daily_bars/      # 应有 trade_date=YYYY-MM 分区
```

若配置里 `[adj_factors].adjust_types` 只有 `qfq` 而你要用后复权，先追加 `"hfq"` 并重跑
`cne derive adj_factors`。

### 幂等

```bash
.venv/bin/python scripts/accept_backfill.py snapshot \
  --config configs/cnequity.toml --out /tmp/curated-counts.json

cne run daily --config configs/cnequity.toml   # 骨架一趟即可，验收看的是幂等性

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

tradable = load(
    "daily_bars",
    start="2024-06-01",
    end="2024-06-30",
    adjust="hfq",
    profile="cn_a_sh_sz_research_v1",
    strict_adj=True,
)
assert "adj_close" in tradable.columns
```

### 验收 checklist

| # | 项 | 通过标准 |
|---|-----|----------|
| 1 | 幂等 | 同窗口重跑后核心数据集 row count 不变 |
| 2 | 口径 | 标杆股 close/adj_close 与行情软件一致（人工） |
| 3 | 覆盖 | 按年行数无异常断崖；2016 起分区连续 |
| 4 | 消费 | 显式研究 profile 的证据门禁通过；`adj_close` 可算且因子精确 |
| 5 | 审计 | 最新 run audit 无 error；`source=mock` 行数 = 0 |

## 备源策略

1. 主源失败 → batch 退避重试（最多 3 次）
2. 仍失败 → 标记 batch failed；可选备源写入 `meta/source_snapshots`
3. `cne audit` 对比主源与 snapshot，由人决定是否切源
4. 不要静默用备源覆盖 curated canonical 行

## 逐证券日线覆盖

```bash
cne verify --bars --config configs/cnequity.toml --start 2026-09-07 --end 2026-09-15
```

该命令以配置的 ingest universe 检查证券×交易日，包括整个窗口没有任何行情的证券。
上市前、退市后不要求行情；有明确非交易状态的日期可豁免。供应商返回空数据不构成停牌证明。
JSON 中 `unresolved_keys` 是缺行情/非交易证据的键数，`unknown_listing_symbols` 是无法确定
期望区间的代码；任一未解决时退出 1。它不会补抓或修改行情，也不能由普通水位检查替代。
完整历史检查按 64 个交易日分块。

## 相关文档

- [脚本说明](scripts.md)
- [故障排查](troubleshooting.md)
- [Schema 契约](../datasets/schema.md)
- [逐源限制](../datasets/sources.md)

## 衍生品回填与修复

先运行 `backfill futures_bars/option_bars --plan --exchange ... --start ... --end ...`，核对范围后去掉 `--plan`。缺行使用 `--refresh`；回填会自动更新合约表及派生。遇到拒绝保留持久欠账并停源冷却，不提高并发。部署新版本后重建旧的推断日期及连续合约/Greeks；详细步骤与证据目录见 [衍生品指南](../recipes/derivatives.md)。
