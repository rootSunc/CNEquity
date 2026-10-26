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

## 旧版仓库脚本（已弃用） {#legacy-scripts}

`scripts/scheduler/` 只在源码 checkout 里有，已弃用；打包定时任务覆盖它的全部功能并在真实湖上验证之后才会移除，移除前在更新日志中预告。迁移：先运行 `scripts/scheduler/uninstall_scheduler.sh` 卸载 launchd 任务（cron 里的条目手工删掉），再运行 `cne schedule install`。`backup_meta.sh` 已改为调用与 `cne run maintain` 相同的备份代码。


| 能力 | 脚本 | 作用 |
|------|------|------|
| 调度 | `cne serve` → 操作 → 定时任务 | 跨平台启用、暂停或改时间；日更和收尾补抓的结果统一在 Web 查看 |
| 调度 | `scripts/scheduler/daily_pipeline.sh` | 按配置顺序串行跑已启用日更组 + 健康检查 + 备份 |
| 调度 | `scripts/scheduler/install_scheduler.sh` | 安装 macOS launchd（每个交易日北京时间 `[job.daily] run_at` 之后跑一次，与本机时区无关） |
| 调度 | `scripts/scheduler/uninstall_scheduler.sh` | 卸载 launchd |
| 告警 | `scripts/scheduler/health_notify.sh` | 日常审计、每周全湖质量检查 + 分组 freshness + macOS 通知 |
| 备份 | `scripts/scheduler/backup_meta.sh` | 元数据与修订收据的 tar 轮换；完整数据另用快照 |

脚本使用仓库 `.venv/bin/cne`，路径相对仓库根目录自解析。

### 安装 launchd 任务（旧版）

```bash
cd /path/to/cnequity
scripts/scheduler/install_scheduler.sh
```

安装器将主机选择与模板分开：新安装按配置选择已启用组；重装保留已安装 daily 的
`CNE_GROUPS`、`CNE_SOURCE_VANTAGE`、自定义配置和各任务的执行时间。
明确覆盖组别和出口时使用环境变量；海外主机首次安装例如：

```bash
CNE_GROUPS=core CNE_SOURCE_VANTAGE=overseas scripts/scheduler/install_scheduler.sh
scripts/scheduler/install_scheduler.sh --check
scripts/scheduler/install_scheduler.sh --dry-run /tmp/cnequity-scheduler-review
scripts/scheduler/install_scheduler.sh --daily-only                  # 只同步现有日任务，不新增晚间任务
scripts/scheduler/install_scheduler.sh --stale-only                  # 只同步补抓任务
```

**运行时间按北京时间配置，与本机时区无关。** 日更和补抓都每小时唤醒一次（日更每小时第 7 分钟、
补抓第 37 分钟），由 `scripts/scheduler/scheduler_gate.py` 判断这次是不是「正式那一次」：

```toml
[job.daily]
run_at = "17:30"   # 北京时间；每个交易日过了这个点跑一次（默认 17:30）
[job.stale]
run_at = "21:00"   # 北京时间；当天日更跑过之后，只重试失败的快照数据（默认 21:00）
```

- 每个交易日各跑一次：过了 `run_at` 就跑，直到下一个交易日 09:15 开盘前都算这一天的；
  过了开盘还没跑（比如电脑一直关着），这一天就不补了——历史类数据下次日更按日期补齐，快照类缺这一天。
- 本机是哪个时区、有没有夏令时都不用管，也不用重装；电脑在 `run_at` 时睡着，醒来后下一次唤醒就会跑。
- 手工执行 `scripts/scheduler/daily_pipeline.sh` 不受这个判断限制，照常立即跑。
- 已跑过的交易日记在 `meta/state/scheduler/`；manifest 里已有当天日更记录的也算跑过，
  所以切换到这套机制或手工跑过之后，不会再重复跑一整遍。
- 旧的 `--stale-at` 已停用，改用 `[job.stale] run_at`。

`--check` 比较已安装文件与按主机选择生成的模板，存在差异或缺少任务时退出 1；
它不启动任务，也不检查 launchd 是否已加载。`--dry-run` 只写预览目录。
常规安装包含 daily、stale、events 三个任务；stale 继承 daily 组别并通过
`cne run daily --stale-only --groups ...` 限定补抓范围。现有事件任务的间隔和组别保留；
额外手工安装的事件任务不由此安装器删除。

补抓只重试按快照调度的数据集（资金流、估值、人气榜等），历史类留给下一次日更按日期补；
补抓与主任务遵循同一 push2 配置、预算与熔断策略。是否另有历史来源，以数据集契约为准。

日任务的质量错误和实际调度的 `CNE_GATE_GROUPS`（默认 core）滞后会让任务退出 1。
soft 组仍按失败次数升级，但同一日期重试不会重复计为多天。
每周使用 `audit --full --quality-only` 检查质量，freshness 由分组 `status` 单独门禁，
因此未调度的分钟线不会仅因滞后而触发「数据异常」。全湖健康报告仍显示这些滞后。

- 生成 `~/Library/LaunchAgents/com.cnequity.daily.plist`
- 每小时唤醒，**每个交易日北京时间 `[job.daily] run_at`（默认 17:30）之后跑一次**；
  与本机时区、夏令时无关，重装不保留旧的本机时间。
- 非交易日自动跳过（退出 0）
- **漏跑 / 周末补数**：`python scripts/run_catchup.py` 补最近一个交易日的 core 与 breadth；水位已到目标日的部分记为 `skipped_already_fresh`。要按日重跑全部调度组，用 `scripts/scheduler/daily_pipeline.sh YYYY-MM-DD` 或 `CNE_TRADE_DATE=...`。
- **按当前出口选择调度组**：先检查源健康，再启用实际能够维护的组。一个出口的失败不能推出某个地域都不可用；出现拒绝先冷却，不通过换出口继续同一轮抓取。见[取数与源保护](fetch-policy.md)。

```bash
launchctl list | grep cnequity
launchctl start com.cnequity.daily   # 手动触发
scripts/scheduler/uninstall_scheduler.sh
```

<a id="web-schedule"></a>

### Web 管理跨平台定时任务

安装包用户可在 `cne serve` 的操作页设置定时任务，无需复制仓库脚本：选择自动日更、收尾补抓、每日数据备份或独立事件流，设置时间和范围，先预览再确认。日更与补抓每个交易日各尝试一次；备份按北京时间的自然日到点后尝试一次，选择数据集及备份目录；事件流选择配置中的事件组，按 1–1440 分钟间隔运行，包含周末和节假日。系统每分钟检查一次，结果和日志显示在最近任务中，关闭 serve 后仍会运行。所有 Web 任务共享单任务槽，占用时等待后续检查；事件流不累积补跑，失败也从尝试开始时计算间隔。

macOS 使用当前用户的 launchd，Windows 使用当前登录用户的任务计划程序，两者均须用户保持登录；Linux 使用当前用户的 crontab，并须 cron 服务运行。系统睡眠时不执行，恢复后的下一次检查会在交易日窗口内补跑日更与补抓；备份只补当天，事件流按当前间隔判断。页面同时显示系统任务状态和最近检查；超过 20 分钟未检查时，排查用户登录、机器休眠和 cron 服务。系统注册成功表示任务定义已安装，最近检查记录才说明系统实际触发过。

日更与补抓时间保存在原配置的 `[job.daily] run_at` / `[job.stale] run_at`，首次改动前保存 `.schedule.bak`，其他设置和注释保留。备份和事件流设置随湖保存；备份不自动删除旧副本，需关注存储空间。暂停阻止后续自动执行，已经启动的任务在任务详情里另行取消。即使系统任务因权限变化无法删除，暂停设置仍能阻止随包入口继续取数；页面会提示手动核对系统任务。

Web 只管理自己创建的系统任务，不接管已有 launchd 脚本或其他 cron / Windows 任务。迁移前核对并停用重复入口。Web 日更执行 `cne run daily`，之后执行 `cne run maintain`（审计、门禁、元数据备份），每日备份执行所选数据集的 `cne snapshot create`，独立事件流执行所选组的 `cne run events`。命令行用 `cne schedule install|status|remove` 管理的是同一个任务。系统任务固定使用创建时的 Python 环境、工作目录及配置路径；移动环境后重新设置，数据源所需配置也须在无人值守环境中可用。

**Linux cron（手动配置方案）**：

```cron
# 每小时唤醒；CNE_SCHEDULED=1 让脚本自己按北京时间 run_at 判断每个交易日只跑一次
7 * * * * CNE_SCHEDULED=1 /path/to/cnequity/scripts/scheduler/daily_pipeline.sh
37 * * * * CNE_SCHEDULED=1 /path/to/cnequity/scripts/scheduler/stale_pipeline.sh
# 事件流独立运行，包含周末；按所需频率调整
20 20 * * * /path/to/cnequity/scripts/scheduler/events_pipeline.sh
```

**Windows 任务计划程序**（原生 Win10/11；`daily_pipeline.sh` 不适用于 PowerShell）：

1. 先确认 `cne doctor` 与 `cne config validate` 通过，`data.root` 用短绝对路径（如 `D:\lake`）。
2. 打开「任务计划程序」→ 创建基本任务 → 每天 16:05（或收盘后任一时刻，周末也触发）。
3. 操作选「启动程序」：

| 字段 | 示例 |
|------|------|
| 程序/脚本 | `C:\path\to\.venv\Scripts\cne.exe` |
| 添加参数 | `run daily --config C:\path\to\configs\cnequity.toml` |
| 起始于 | `C:\path\to`（仓库或配置所在目录） |

这一条已包含事件流（公告、监管事件、资讯），不需要再为 `run events` 另建任务；非交易日日更组自动跳过，事件流照常运行。Windows 任务计划程序和普通 cron 的时间是本机时间；只有使用仓库 scheduler gate 的任务才按配置的北京时间判断。想让事件流更频繁时，可以另外调度 `cne run events --group news_wire`；它与日更同时运行时，日更里的事件流段记为 `skipped_locked`，不算失败。

> 控制台中文乱码时：`chcp 65001`，或设置用户环境变量 `PYTHONUTF8=1`。

## 每日 Pipeline

```
core → capital → signals → fundamentals → macro_risk → research
  → health_notify.sh
  → backup_meta.sh
  → group summary（gate vs soft）
```

- 单组失败不中断后续组（尽量多采数据）
- 结尾摘要区分 **gate**（默认 `CNE_GATE_GROUPS=core`）与 **soft**（东财等）
- 默认 `CNE_SOFT_FAIL_OK=1`：gate OK 时 soft 失败 **warn-only、exit 0**（只宽容短暂失败，持续失败会升级）；
  需要任一组失败都阻断时可设 `CNE_SOFT_FAIL_OK=0` 让 soft 失败仍 exit 1
- 东财超时/连接失败不重试（`[sources.eastmoney] timeout_sec`，默认 15s）
- 生产 `daily_pipeline.sh` 常设 `workers=1`（TDX 客户端与多进程兼容性）

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

`backup_meta.sh` 只备份 manifest、state、quality、revision 收据、来源快照和运行证据；
**不包含** `curated/`、`derived/` 或 `meta/revisions/data/` 中的数据版本文件。
有些历史可重采，有些快照型数据错过窗口后无法从原源补回，因此不能把元数据归档当作
完整湖备份。需要恢复研究数据时，另建并校验可移植快照，或对整个湖做一致性备份。

```bash
scripts/scheduler/backup_meta.sh /abs/path/to/lake /path/to/offsite/cne-meta 30 30
```

`DATA_ROOT` 是湖目录，不是 TOML 配置路径；省略时脚本使用 `CNE_DATA_ROOT`，
否则使用仓库的 `data/cnequity`。默认归档保存在湖内；磁盘级容灾要指定湖外目录。
归档按天数和份数中更严格的限制轮换，见[脚本参数](scripts.md#backup_metash)。

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

`CNE_CONFIG` 与 `CNE_LOG_DIR` 由 `cne` 本身读取，其余由 [scripts.md](scripts.md) 中的 shell 脚本读取。

| 变量 | 默认 | 作用 |
|------|------|------|
| `CNE_CONFIG` | `configs/cnequity.toml` | 所有命令 `--config` 的默认值（显式 `--config` 优先）；调度器中请设绝对路径，不能仅靠相对路径免除 `cd` |
| `CNE_DATA_ROOT` | 仓库的 `data/cnequity` | `backup_meta.sh` 的默认湖目录；自定义 `data.root` 时需显式设置或传第一个参数 |
| `CNE_LOG_DIR` | `{data.root}/logs` | 日志；长跑的 `cne` 命令也会在这里留一份 |
| `CNE_GROUPS` | 按配置选择已启用组 | 覆盖 pipeline 组列表；公共配置默认运行 6 个常规组，可选组随开关加入 |
| `CNE_PYTHON` | 仓库 `.venv/bin/python`，没有时用 PATH 上的 `python3` | 调度脚本的门控与调度锁使用的解释器，须能导入 cnequity |
| `CNE_NOTIFY` | `1` | `0` 关闭通知 |
| `CNE_BACKUP_DIR` | 湖内 backups | 备份目录 |
| `CNE_BACKUP_RETENTION_DAYS` | 14 | 保留天数 |
| `CNE_BACKUP_RETENTION_COUNT` | 30 | 最多保留份数；`0` 关闭份数上限 |

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

生产更推荐用 `scripts/scheduler/daily_pipeline.sh`（见上文），它会串行跑完全部组并做健康检查与备份。

## 收尾补抓

正常补抓使用独立调度的 `scripts/scheduler/stale_pipeline.sh`。它在较晚的窗口只处理仍落后的 snapshot 数据集，避免主日更任务内等待造成调度超时；与主任务共享锁，重叠时跳过。`daily_pipeline.sh` 保留 `CNE_STALE_RETRY=1` 兼容开关，但默认关闭。

补抓遵循与主任务相同的 push2 配置，不额外关闭该来源。被封出口可在个人配置中设置 `[sources.eastmoney] push2_paused=true`，或显式传 `CNE_PUSH2_PAUSED=1`；已触发的共享熔断和预算仍会阻止请求。

| 环境变量 | 默认 | 说明 |
|---|---|---|
| `CNE_STALE_RETRY` | `0` | 旧的进程内等待补抓，仅兼容需要时设 `1` |
| `CNE_STALE_RETRY_DELAY_SEC` | `1800` | 仅旧兼容路径生效 |
| `CNE_SOURCE_HEALTH` | `1` | 每日日更后复用真实采集证据，仅主动探测过期或未触达端点；设 `0` 关闭 |
| `CNE_SOURCE_VANTAGE` | `local` | 当前出口的稳定标签；不要把海外样本标成 `cn` |

**为什么需要它。** 快照型日更只抓运行当天；同日第二个窗口可以补当天失败的采集。
错过后不能重放旧 `--trade-date` 来伪造当时观察。部分数据集有独立历史回补源，
但其来源、覆盖和 PIT 语义可能与当日快照不同；先查 `history_mode` 和
`backfill_source`，不要把所有快照型数据一概判为可补或永久不可补。

**错开窗口是重点。** 立刻重试大概率撞上同一场中断，因此补抓由独立任务在较晚时间发起。

默认 `CNE_SOFT_FAIL_OK=1` 时 soft 组当日失败只告警；连续失败会按配置升级。独立补抓有自己的退出码与日志。旧兼容补抓失败会出现在主任务的 `stale-retry:` 摘要中。

### 中断持续几小时怎么办

独立任务可设置更晚的调度窗口：

```cron
5 20 * * 1-5 cd /path/to/cnequity && scripts/scheduler/stale_pipeline.sh
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
ls data/cnequity/curated/daily_bars/      # 应有 trade_date=YYYY-MM-DD 分区
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
