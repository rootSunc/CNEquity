# 配置参考

配置是 TOML 文件。完整的键和默认值以随包模板为准，模板里每段都有注释：仓库副本见 [`configs/cnequity.example.toml`](https://github.com/rootSunc/CNEquity/blob/main/configs/cnequity.example.toml)，或运行 `cne config show --defaults`。本页说明配置如何生效，以及各段管什么。

```bash
cne config create      # 写出用户级配置，只含 extends 和 [data].root
cne config path        # 当前生效的配置、选中原因和指向的湖
cne config show        # 生效的完整配置，标出文件里写的值，凭证打码
cne config validate    # 校验
cne config diff        # 报告完整配置相对内置默认值缺少的步骤和设置
```

## 查找顺序 {#lookup}

CLI、`load()`、MCP 和定时任务用同一套规则：`--config`（Python 为 `config=`）→ 环境变量 `CNE_CONFIG` → 当前目录的 `configs/cnequity.toml` → 用户级 `~/.config/cnequity/config.toml`（遵循 `XDG_CONFIG_HOME`；Windows 为 `%APPDATA%\cnequity\config.toml`）。所以在任何目录运行，裸命令都指向同一个湖。

配置里的相对路径（`[data].root`、`[duckdb].path`、凭证文件）按配置文件所在目录解析。`cne config create` 和 `cne init` 写出的是绝对路径。

## 继承内置默认值 {#extends-builtin}

```toml
extends = "builtin"

[data]
root = "/abs/path/to/lake"
```

`extends = "builtin"` 的配置只写和默认值不同的部分，其余取当前安装版本的随包模板（已按本机平台调整，例如 macOS 和 Windows 上 `workers = 1`）。升级后新版本加入的设置和调度步骤直接生效。

- 表按键逐层合并；标量、数组和表数组（如 `[[failover.datasets]]`）整体替换默认值。
- `[data].root` 从不取默认值，必须写在文件里。
- 调度组用 `steps_add`、`steps_remove`、`enabled = false` 修改，换掉一个步骤不会冻结组内其他步骤。写 `steps = [...]` 表示整组替换，遮住新版本加入的步骤时 `cne config diff` 会报出来。

```toml
[job.daily.groups.capital]
steps_remove = ["fund_flow"]
steps_add = ["fund_flow_ths"]

[job.daily.groups.research]
enabled = false
```

没有 `extends` 的完整配置照旧工作，缺少的键回退到内置值；升级时用 `cne config upgrade` 补上新步骤，或用 `cne config upgrade --minimize` 一次改写成继承形式（`--dry-run` 预览，原文件自动备份）。

## 湖与资源 {#lake-and-resources}

| 段 | 说明 |
|---|---|
| `[data]` | `root`：湖根目录。不同的湖（正式湖、演示湖）用不同路径 |
| `[duckdb]` | `path`（支持 `{data.root}` 占位符）、`memory_limit`、`threads`：查询连接的资源 |
| `[duckdb.<工作负载>]` | 可选，分别覆盖 `write`、`audit`、`export`、`maintenance` 连接的 `memory_limit`、`threads`、`temp_directory`、`max_temp_directory_size`。内置值：写入和导出 4GB，审计 512MB 单线程，维护 8GB |
| `[query]` | `scan_budget_rows`（默认 4000000）：控制台数据浏览和 MCP `query_dataset` 在分页前可选中的行数上限 |
| `[logging]` | `console_level`、`file_level`（默认 `INFO`）；`heartbeat_seconds`（默认 30，安静这么久后报告进度，0 关闭）；`retention_days`（默认 30，`cne run clean` 清理更早的命令日志，0 不清理） |

## 采集范围 {#ingest-scope}

| 段 | 说明 |
|---|---|
| `[universe]` | `ingest`：日更覆盖的证券类别，`all_a`（沪深北 A 股，默认）、`all_a_sh_sz`、`all_instruments`。它只约束取数范围，与研究用的股票池无关；ST、停牌、CDR 和退市股始终保留。`ingest_listed_funds`、`ingest_eligible_etfs` 追加 ETF/LOF |
| `[datasets]` | `best_effort`：尽力而为档的数据集默认不调度，列在这里才启用（`["all"]` 全部启用）。未启用的数据集新鲜度显示为 `removed` 而不是落后 |
| `[datasets.history_from]` | 按数据集写这个湖的历史从哪天开始，如 `fund_flow = 2023-09-11`；规划器不再把更早的日子算作缺口，改早即可往前补 |
| `[datasets.history_until]` | 唯一来源已经停止的数据集，写历史到哪天为止；之后的日子不再算欠着，接上新来源后删掉这一行 |
| `[incremental]` | `negative_evidence_ttl_days`（默认 7）：「来源此处为空」的有效期；`deep_reconciliation_dow`（默认 6）：每周做一次深度对账的星期，0 每次都做 |
| `[minute_bars]`、`[trade_ticks]`、`[futures]` | 分钟线、分笔、期货与期权，默认关闭。请求量和磁盘量随范围增长，先在小范围核对用量 |
| `[orchestrator]` | 并发与重试：`workers`、`tdx_daily_workers`、`source_concurrency`（每个来源的在途请求上限）、`batch_size`、`max_retries`、`batch_stale_seconds`、`batch_stall_seconds` |
| `[on_demand]` | 按需抓取的数据集（默认 `stock_news`、`research_reports`），由 `cne query --dataset X --symbol Y` 触发 |
| `[raw_archive]` | 来源原始响应压缩存到 `meta/raw`。请求凭证、代理设置、Cookie 和 authorization 头一律不存档。大原文可用 `cne storage raw-external` 登记到外置目录 |

## 来源 {#sources}

`[sources.<名称>]` 每段管一个来源或请求通道：`enabled` 和 `min_interval_seconds`（跨进程的请求间隔）是共同的键，东方财富另有分主机的限速、每日预算、熔断和代理设置，Baostock 有批次冷却。模板注释说明了每个键。这些是客户端的保守设置，不是来源承诺的安全配额；不用的来源写 `enabled = false`，不要靠删配置段表示关闭。

`[tdx_protocol]` 管通达信行情协议（主机、限速、超时）；`[failover]` 配置主备来源快照与比对，不会自动切换主来源；`[exchange_audit]` 把日线与交易所发布的收盘价对照，结果只作提示，不让运行失败。

同一网络出口上有多个湖或多个进程时，设同一个 `CNE_RATE_LIMIT_ROOT`，让它们共用限速、冷却和预算账本。

### 凭证 {#credentials}

可选的付费来源（`[sources.tushare]` 的令牌、`[sources.ths_official]` 的密钥）按以下顺序读取：环境变量 `TUSHARE_TOKEN` / `HITHINK_FINANCE_API_KEY` → 文件 `token_file` / `api_key_file`（只读第一行）→ 配置里直接写的值。定时任务读不到登录 shell 里 export 的变量，所以定时运行的湖建议用权限为 600 的文件。本机访问 `cne serve` 时，也可在设置页直接填写、更换或移除密钥：密钥写入本机权限为 600 的独立文件，配置只记录 `token_file` / `api_key_file` 路径，保存后立即检测一次。凭证写在配置里，或配置文件其他用户可读时，`cne doctor` 会提示。没有凭证时这些来源不发请求。

Tushare 默认只用于按需回填（`[sources.tushare].daily = false`）。设为 `true`（或在 `cne serve` 设置页打开「Tushare 进入日更」）后，只有在密钥检测通过时，日更才会运行 `tushare` 分组；以 Tushare 为主来源的数据集也只在此时计入新鲜度。

## 调度 {#schedule}

| 段 | 说明 |
|---|---|
| `[[job.daily.waves]]` | 日更的骨干依赖顺序 |
| `[job.daily.groups.<名称>]` | 日更调度组：`steps`、`at`（参考时间）、`cadence`（`daily` 或 `weekly`）、`weekday`。每周组只让能按日期补的数据集休息；快照类数据集照常每天跑，因为漏掉的一天补不回来 |
| `[job.daily]`、`[job.stale]` | `run_at`：北京时间，日更默认 17:30，补抓快照类默认 21:00。系统定时任务每个交易日过了这个时间各跑一次，与本机时区无关 |
| `[job.events.groups.<名称>]` | 事件流组（公告、监管事件、新闻）：不看交易日历，用另一把运行锁，可以和日更并行 |
| `[[job.events.schedules]]` | `cne schedule install --events` 安装的定时任务按这里运行：`group`、`every_minutes`、`jitter_seconds`、`quiet_hours`、`quiet_every_minutes` |
| `[job.maintenance]` | `cne run maintain`：`full_audit_weekday`（全湖审计的星期，0 不跑）、`backup_dir`、`backup_retention_days`、`backup_retention_count`、`gate_groups`（这些组失败时维护立即判失败）、`soft_fail_max_days` |
| `[job.init.phases]` | `cne init` 的阶段顺序 |
| `[source_health]` | `vantage`：本机网络出口的标签，来源探测结果按标签分开解读 |

不带参数的 `cne run daily` 按配置顺序跑完全部日更组，再跑事件流组；`--group <名称>` 只跑一个组。

## 质量与存储 {#quality-and-storage}

| 段 | 说明 |
|---|---|
| `[quality]` | `publication_gate`：发布前审计候选数据，`off`（默认）、`shadow`、`block`。`audit_gate`：发布后审计报 `error` 时运行怎么办，`off`、`shadow`（默认，只记录到 `meta/quality/audit_gate.jsonl`）、`block`（运行失败，但不撤回已发布数据）。`run_failure_window_hours`（默认 24）和 `run_failure_streak`（默认 3，0 关闭）：窗口内连续这么多次运行不健康时报告 |
| `[storage.retention]` | `keep_generations`（默认 5）：每个数据集保留的最近修订数，0 不自动清理。当前修订和 `cne storage hold` 登记过的修订始终保留 |
| `[research]` | `holdout_start`：`cne decision-data` 拒绝触及这一天的窗口，让留作样本外检验的数据不被证据盘点看到 |

两种门禁的分工见[整体设计](../architecture/overview.md#quality)。切换到 `block` 之前，先读 shadow 模式的记录，了解它会多频繁触发。

## 环境变量 {#environment}

| 变量 | 作用 |
|---|---|
| `CNE_CONFIG` | 配置路径，优先级低于 `--config` |
| `CNE_LOG_DIR` | 命令日志目录，默认 `{data.root}/logs`。日志文件名为 `cne-<命令>-<UTC 时间>-<进程号>.log` |
| `CNE_RATE_LIMIT_ROOT` | 共享限速、冷却和预算账本的目录 |
| `CNE_PUSH2_PAUSED=1` | 在本地暂停东方财富 push2 系请求，与 `[sources.eastmoney] push2_paused` 相同 |
| `CNE_SOURCE_VANTAGE` | 单次运行覆盖 `[source_health].vantage` |
| `CNE_SCHEDULER_LOCK_DIR`、`CNE_LOCK_DIR` | 调度锁目录，默认 `{data.root}/locks` |
| `EQUITYLAKE_EXTENSION_DIR`、`EQUITYLAKE_EXTENSION_MIRROR` | 湖目录需要的 DuckDB 扩展的本地目录或镜像，官方扩展站点连不上时使用 |

## 已不起作用的设置 {#retired}

| 设置 | 说明 |
|---|---|
| `[storage].publish` | 0.25.0 起所有写入都发布进湖目录；保留它不会报错 |
| `[datasets].planned`、`[datasets].planner_only` | 试验期的设置，0.25.0 起覆盖规划总是生效；保留只会告警 |
