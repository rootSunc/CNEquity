# 配置参考

配置文件格式：TOML。模板随包装在 `cnequity.config.templates`；仓库内副本为 `configs/cnequity.example.toml`。

```bash
cne config create                              # 推荐：写出用户级配置 ~/.config/cnequity/config.toml
 # 首次创建时也可选：cne config create --data-root /data/cnequity
cne config path                                # 显示当前生效的配置、选中原因和数据目录
cne config show                                # 打印生效的完整配置，标出文件里写的设置
cne config validate
```

加载与校验：`cnequity.config.loader`。个人文件不参与开源默认配置；参见[升级与反馈](installation.md#升级与兼容性)。配置查找顺序：显式 `--config` → `CNE_CONFIG` → 当前目录的 `configs/cnequity.toml` → 用户级 `~/.config/cnequity/config.toml`（遵循 `XDG_CONFIG_HOME`；Windows 为 `%APPDATA%\cnequity\config.toml`）。CLI、`load()`、MCP 和定时任务用同一套规则，所以在任何目录运行都指向同一个湖。`cne config create` 默认写绝对 `data.root`；手写的相对路径（`data.root`、`duckdb.path`）按**配置文件所在目录**解析。旧版按工作目录解析的相对路径，在新位置不存在而旧位置存在时继续沿用并给出警告，运行 `cne config upgrade` 会把它固定为绝对路径。

本页按配置段查阅。首次使用通常只需确认 `[data].root`，再按需求启用分钟线、分笔或[衍生品](../recipes/derivatives.md)。表中默认值指随包模板，也就是 `extends = "builtin"` 配置继承的默认值；直接构造 `Config()` 的结果可能不同，以 `cne config show` 为准。

同一出口上多个进程/数据湖用 `CNE_RATE_LIMIT_ROOT` 统一 HTTP/TDX 限流与共享冷却。源保护、CLI 联网边界和使用顺序见[取数指南](../operations/fetch-policy.md)。

## 继承内置默认值（`extends = "builtin"`） {#extends-builtin}

`cne config create` 和首次 `cne init` 写出的配置只有几行：

```toml
extends = "builtin"

[data]
root = "/abs/path/to/lake"
```

其余设置都来自当前安装版本的内置默认值（即随包模板，按本机平台调整过，例如 macOS / Windows 的 `workers = 1`）。文件里只写想改的设置；升级后新版本加入的设置和调度 step 直接生效，不需要 `cne config upgrade`。

- 表按键逐层合并（内联表也一样）；其他值（标量、数组、`[[failover.datasets]]` 这样的表数组）整体替换默认值。
- `[data].root` 从不取默认值，必须写在文件里。
- 调度组（`[job.daily.groups.*]`、`[job.events.groups.*]`）支持 `steps_add`、`steps_remove` 和 `enabled = false`，换掉一个 step 不会冻结组内其他 step：

```toml
[job.daily.groups.capital]
steps_remove = ["fund_flow"]
steps_add = ["fund_flow_ths"]

[job.daily.groups.research]
enabled = false
```

组内 step 的顺序不影响执行：引擎按依赖、收尾顺序和名称排定。写 `steps = [...]` 仍表示整组替换；它遮住了新版本默认加入的 step 时，`cne config diff` 会报出来。

`cne config show` 打印生效的完整配置，标出哪些值来自文件，凭证打码；`cne config show --defaults` 打印全部设置及说明。

没有 `extends` 的完整配置照旧工作：缺少的键回退到代码里的内置值，升级时需要 `cne config upgrade` 补调度 step。`cne config upgrade --minimize` 把它一次性改写成继承默认值的形式：等于默认值的键删掉，调度组差异写成 `steps_add` / `steps_remove`，配置里没有的默认调度组写成 `enabled = false`，`data.root` 写成当前解析到的绝对路径。改写前会列出加载后行为会变化的设置（旧配置缺键、现在由默认值提供的部分），`--dry-run` 只预览；原文件自动备份。

## `[data]`

| 键 | 类型 | 默认 | 说明 |
|----|------|------|------|
| `root` | string | `./data/cnequity` | 数据湖根目录；相对路径按配置文件所在目录解析，**生产建议绝对路径** |

派生路径（代码内自动计算，无需配置）：

- `{root}/staging` — 本次 run 原始落地
- `{root}/curated` — canonical 数据集
- `{root}/derived` — 派生数据集（如 adj_factors）
- `{root}/meta` — manifest、水位、质量 findings
- `{root}/duckdb/cnequity.duckdb` — DuckDB 视图库

## `[orchestrator]`

| 键 | 默认 | 说明 |
|----|------|------|
| `workers` | 8 | `daily_bars` 多进程 worker 数 |
| `batch_size` | 100 | 每 batch 股票数量 |
| `max_retries` | 3 | batch 级重试次数 |
| `retry_backoff_seconds` | 5 | 重试退避 |
| `batch_stale_seconds` | 3600 | running batch 无心跳超时 → stale → failed；compact 门禁会跳过未完成数据集。**崩溃的 run 不受这个窗口约束**：run 全程持锁，进程一死 60 秒内即被回收 |

## `[tdx_protocol]`

| 键 | 默认 | 说明 |
|----|------|------|
| `enabled` | true | 禁用后 TDX 相关 step 失败 |
| `min_interval_ms` | 100 | 跨进程限速间隔（建议 ≥100，防多 job 打爆） |
| `lock_timeout_sec` | 15.0 | 申请 TDX 限速锁的最大等待时间；超时显式失败，不绕过限速 |
| `servers` | `"auto"` | `"auto"` 或 `"host:port"` 固定单服 |
| `connect_timeout_sec` | 10 | 连接超时 |
| `allow_mock` | false | **仅测试**：源不可用时返回 `source="mock"` 数据；生产必须 false |

### `[tdx_protocol.hosts]`

| 键 | 说明 |
|----|------|
| `standard` | `servers="auto"` 时优先并行探测的 A 股标准行情主机列表；为空则用内置兜底列表（`adapters/tdx_protocol/hosts.py`） |

## `[sources.<name>]`

模板里的主要来源与请求通道：

| name | 说明 |
|------|------|
| `eastmoney` | 日更主源：公告、财务、资金流 |
| `cninfo` | 公告 / 监管分页 POST |
| `pboc` | 社融月度序列 |
| `sina` | 新闻、复权因子 |
| `sina_bars` | Sina 日线兜底；**自带更慢的限速器**——因子端点和按标的 kline 端点的上游限速行为不同 |
| `baostock` | 历史行情兜底；带全市场回填批次冷却 |
| `nbs` | 仅 audit：PMI 发布稿对照 |
| `exchange` | 上交所 / 深交所自有板块（融资融券明细、交易状态、`[exchange_audit]` 价格对照） |
| `bse` | 北交所官方当前行情快照；**BJ 当日 tip bar 与成交额的主源**。它不是历史源，BJ 历史窗口仍走 Sina |
| `ths` | 同花顺公开页（行业、估值） |
| `ths_bonus` | 同花顺分红送配页，限速更保守（默认 3.0s） |
| `ths_pages` | `d.10jqka.com.cn` 的 kline 页 |
| `ths_official` | **同花顺官方 API（keyed）**，见 [`cne ths-official`](../reference/cli.md#cne-ths-official) |
| `sw` / `cni` | 申万历史行业、国证历史成分文件，默认间隔 1 秒、单请求在途 |
| `futures_exchange` | 期货交易所文件，逐主机限速与缓存 |
| `tushare` | 可选 Tushare Pro——BJ 历史 ST 证据（`stock_st`）。需 token：环境变量 `TUSHARE_TOKEN` 或 `token_file`，别把凭证写进配置 |


| 键 | 说明 |
|----|------|
| `enabled` | 显式 false 会在 HTTP 请求边界拦截该源及子通道。省略整段时各旧路径的默认行为不完全相同；对不用的源写 `enabled = false`（继承默认值的配置也一样），不要通过删配置段表达关闭 |
| `min_interval_seconds` | 跨进程请求间隔；必须是非负有限数，0 表示关闭固定间隔但不关闭拒绝冷却。锁外等待，醒来重查共享时间；实际请求前同时取得并发名额 |
| `js_runtime`（ths） | 可选。生成同花顺 `hexin-v` 令牌用的 Deno 路径；不填就在 `PATH` 和常见安装位置里找。令牌脚本在 Deno 里**不给任何权限**运行（不能读写文件、联网、读环境变量）。没有 Deno 时同花顺资金流兜底会失败并记日志，不影响其他数据 |
| `ths_data`（限速通道） | 同花顺数据中心页面（资金流兜底）默认 3 秒一页，与同花顺其他页面共用 1 个并发；可用 `[sources.ths_data] min_interval_seconds` 调整 |
| `proxy`（eastmoney） | 可选 HTTP(S) 代理 URL，对所有东财主机生效。按自身网络条件设置；未设时仍可用 `HTTPS_PROXY`。不能用轮换代理代替冷却 |
| `push2_paused`（eastmoney） | 默认关，即允许请求。开启后 push2 / push2his / push2delay 请求在本地直接失败、不发出，用于出口 IP 被封时停请求冷却；datacenter 等其他东财主机不受影响。环境变量 `CNE_PUSH2_PAUSED=1` 效果相同；日更和晚间补跑均遵循本机配置及该环境变量，补跑不会自行关闭 push2 |
| `push2_breaker`（eastmoney） | 默认开。push2 系第一次拒绝（403 / 429 / 5xx、连接被断、超时）后，所有 push2 主机当天（本地时间，到午夜）一律不再请求，也不切备用主机 |
| `push2_daily_budget`（eastmoney） | 默认 150。push2 系每天（本地时间）请求上限，跨进程累计，用完即停；0 表示不限。上限按本机账本调整，不借用别的湖的次数 |
| `push2_shared_snapshot`（eastmoney） | 默认开。instruments、valuation_metrics、fund_flow 和 clist 行情兜底共用一次全市场翻页（取字段并集），收盘后到次日开盘前重复使用 |
| `push2_min_interval_seconds` / `push2_max_concurrency`（eastmoney） | 默认 4.0 秒 / 1。push2 单独的限速通道，不和 datacenter 共用 |
| `datacenter_breaker` / `datacenter_breaker_strikes`（eastmoney） | 默认开 / 3。datacenter 连续 3 次被拒（403 / 429 / 5xx、连接被拒/重置/断开）当天停用；读超时不算；「请求过于频繁」退避后仍在则立即停用 |
| `news_breaker` / `news_breaker_strikes`（eastmoney） | 默认开 / 3。快讯通道（握手 + 列表主机）连续 3 次扫描被拒（403 / 429 / 共享冷却）后当天停用，次日恢复；代理不通、读超时不算，成功一次清零 |
| `datacenter_daily_budget`（eastmoney） | 默认 0：只计数、不设上限（计数在 `CNE_RATE_LIMIT_ROOT/eastmoney_guard.json`，默认本湖 `meta/rate_limits/`）。按实测用量设上限 |
| `daily_budget`（eastmoney） | 默认 0：只计数；设置后对同一出口的 push2 与 datacenter 请求实施共享日上限，避免分别未超额但厂商总请求过多。共享账本位于 `CNE_RATE_LIMIT_ROOT`。 |
| `datacenter_min_interval_seconds` / `datacenter_max_concurrency`（eastmoney） | 默认 1.0 秒 / 2。datacenter 单独的限速通道；其他东财主机仍按 `min_interval_seconds` 和 `source_concurrency.eastmoney` |
| `batch_size` / `batch_rest_seconds`（baostock） | 全市场回填的额外批次冷却。日请求上限 5 万次、单连接、黑名单冻结时长写在代码里，不能用配置调高 |
| `verify`（ths_official） | 默认 **开**。只允许写 `meta/source_snapshots` 与 findings，从不碰 curated 行，所以有 key 就可以安全开着 |
| `backfill`（ths_official） | 默认 **关**。它会改变湖里的内容，所以必须显式打开。持有凭证、启用源、允许它改数据是三个决定 |
| `api_key` / `api_key_file`（ths_official） | 建议用环境变量 `HITHINK_FINANCE_API_KEY` 或 `api_key_file`，而非写进配置 |

`cne serve` 的取数设置只改通达信、东财、新浪日线、Baostock、北交所官网、同花顺页面和巨潮的 `enabled`，以及东财的 `push2_paused`。分钟线、分笔、期货开关和 `[universe] ingest` / `ingest_listed_funds` / `ingest_eligible_etfs` 也在那一页。确认前先列出差异，写入前备份；间隔、预算、熔断和凭据不动。环境变量 `CNE_PUSH2_PAUSED=1` 仍优先于配置里的暂停开关。

`cne serve` 启动后一直使用启动时的数据湖。直接编辑配置文件改了 `data.root`，要重启 serve 才会切换；在那之前，控制台每页顶部会对照显示配置文件里的新目录和当前仍在使用的目录，并给出重启命令。配置文件读不出来时会指出出错的行和列。这两种情况下页面照常浏览原来的湖，但不能从控制台启动任务、修改取数设置或定时任务，因为任务会按配置文件读到另一个湖。

模板请求间隔（客户端保守设置，不是源方安全配额保证）：

| source | `min_interval_seconds` | 备注 |
|--------|------------------------|------|
| eastmoney | 0.5 | 模板中一般 HTTP 通道；push2 为 4.0 / 单在途，datacenter 为 1.0 / 两在途。裸客户端不代表 CLI 的共享策略 |
| cninfo | 1.0 | 公告/监管分页 POST |
| pboc | 1.0 | 社融月度序列，索引一次 + 每年一个工作簿 |
| nbs | 1.0 | 仅 audit：PMI 发布稿对照，每次两个请求 |
| exchange | 1.0 | 交易所融资融券、状态和对照等批量接口 |
| sina | 0.3 | 复权因子；与 Sina 日线共享在途上限和拒绝冷却 |
| sina_bars | 1.0 | BJ/退市日线 fallback；独立于复权因子限速，配合 HTTP 456 有限重试 |
| baostock | 1.0 + batch 20/120s | 历史市值/ST。另有写死的上海自然日 5 万次上限和单连接；黑名单冻结为本年次数 × 6 小时 |

凭证（`[sources.tushare]` 的 token、`[sources.ths_official]` 的 api_key）按以下顺序读取：环境变量（`TUSHARE_TOKEN` / `HITHINK_FINANCE_API_KEY`）→ 文件（`token_file` / `api_key_file`，相对路径按配置文件所在目录解析，只读第一行）→ 配置里直接写的值。launchd / cron 启动的定时任务读不到登录 shell 里 export 的变量，所以定时运行的湖建议用权限为 600 的文件。凭证直接写在配置里时，`cne doctor` 会提示，配置文件其他用户可读时也会提示。

## THS 官方接口
`ths_official` 是需要账号凭证的可选接口，与 `ths` 公共页面分开配置、限流与记录来源。它不能成为免费核心数据链的强依赖。源登记和数据使用条件见 [来源矩阵](../legal-and-data-sources.md#来源合规矩阵)；客户端代码许可证不等于数据再分发许可。

### 配置与入口

通过环境变量 `HITHINK_FINANCE_API_KEY` 或 `api_key_file` 提供 Key；不要在公共模板、日志或问题报告中写入 Key。个人配置中显式启用 `[sources.ths_official] enabled = true`。`verify` 控制对照证据，`backfill` 控制内容补入，默认后者关闭。

| 命令 | 获取与写入边界 |
|---|---|
| `cne ths-official capture` | 按 `--what` 抓对手源快照；只写快照与运行证据，不替换 canonical |
| `cne ths-official backfill` | 按窗口补财报空缺；支持 `--symbols` 缩小范围，需要内容开关 |
| `cne ths-official repair-bars` | 历史行情核对；默认报告，`--apply` 才写修复结果 |
| `cne ths-official resource-sectors` | 显式板块换源；默认报告，`--apply` 写入并自动发布 |

后两项的预演仍会联网并消耗源配额。无 Key、未启用源或未允许相应能力时，命令可能返回 `status=skipped`；这不证明抓到了数据。参数以各命令 `--help` 为准。

### 数据契约

- 财务 `net_profit` 使用归母口径，映射 `parent_holder_net_profit`，不能直接同名拼接包含少数股东的总净利润。
- 披露日要有可追溯的对应证据；当前重述值不能仅因补了日期就升级成原始 PIT。
- 复权事件优先下载全量 dump，再在本地切片；事件流和因子序列结构不同，通过专用仲裁检查比较。
- 送股、转增与配股须遵守 [产品边界](../architecture/overview.md)，不能跨来源叠加同一稀释事实。
- ETF、个股、板块端点的窗口与覆盖不同；适配器会限制已知范围。空响应不能证明退市证券不存在，不能自动删掉原有行。
- 插入缺失主键与替换已有来源是不同操作；修复前核对[产品边界](../architecture/overview.md)和[数据源限制](../datasets/sources.md)。

### 失败处理

鉴权失败先检查凭证和开关；遇到限流或 HTTP 拒绝时等待共享冷却，再做一次小范围诊断。空结果与失败、跳过是不同状态，不能用作覆盖完整的证明。见[取数与源保护](../operations/fetch-policy.md)。

## `[adj_factors]`

| 键 | 默认 | 说明 |
|----|------|------|
| `source` | `"sina"` | 复权因子来源 |
| `adjust_types` | `["hfq"]` | 仅存后复权因子；qfq 查询期派生 |

## `[sentiment]`

| 键 | 默认 | 说明 |
|----|------|------|
| `use_snownlp` | false | on-demand `stock_news` 可选 SnowNLP（包已随安装提供）；日更 batch 用关键词 |
| `news_symbol_limit` | 50 | HTTP `stock_news` 回退抓取 symbol 上限（主通道为 curated `news_headlines`） |

## `[failover]`

多源快照与 diff；不会自动切换 canonical。

| 键 | 说明 |
|----|------|
| `enabled` | 总开关 |
| `backfill_snapshots` | `false`；是否在历史回填关键路径抓取备用源快照。默认关闭，避免慢备用源阻塞 canonical 回填；需要跨源历史 diff 时显式开启。corporate_actions 的 EastMoney 快照默认回溯至 2015-09-29，主回填仍以研究底 2001-01-01 为准 |

### `[[failover.datasets]]`

| 键 | 说明 |
|----|------|
| `name` | 数据集名 |
| `primary` | 主源 adapter 名 |
| `backup` | 备源（主源 batch 失败时写 snapshot） |
| `compare_fields` | audit diff 比对字段 |
| `price_tolerance_bps` | 价格容差（基点） |

默认配置：`daily_bars`（TDX 主 / EM 备）、`corporate_actions`（EM 主 / TDX 备）。

## `[universe]`

| 键 | 默认 | 说明 |
|----|------|------|
| `default` | `"all_a"` | 配置中的默认 universe；`load()` 不会因省略 `universe` 就自动过滤 |
| `ingest` | `"all_a"` | 日更抓取覆盖的标的类别 |

`ingest` 只约束**取数范围**，与研究选股口径（`domain/universe_profiles.py`）无关：

| 值 | 含义 |
|----|------|
| `all_a` | 沪/深/北 A 股（默认） |
| `all_a_sh_sz` | 再排除北交所 |
| `all_instruments` | `instruments` 列出的全部代码，含 ETF/LOF 行情代码 |

ST、停牌、CDR 和已退市的名字在任何取值下都会保留 —— 丢掉它们正是这个湖要避免的幸存者偏差。

显式开启 `[universe].ingest_listed_funds = true` 后，日更按证券目录追加已上市 ETF/LOF，研究资格只用于筛选。已知上市前和退市后的日期不抓；未知上市日期仍尝试取数，并在逐证券覆盖报告中单独列出，不能据此声称完整。目录读取失败会报错，来源空响应保留为缺口。这个开关优先于旧的 `ingest_eligible_etfs`。旧开关仍只采集研究资格合格的 ETF，但资格目录失败、缺失或超过 14 天时现在会明确报错。

没开基金采集时，全量公司行为扫描跳过 ETF/LOF。之后开启任一开关，日更的公司行为步骤会补扫从没扫过除权历史的基金，每次最多 500 只（全市场约 2200 只，分几天扫完）；新上市基金同样补扫一次。扫完前每次运行告警 `fund_history_unswept` 并列出部分代码，这期间这些基金的复权因子缺少较早的份额拆分。

## `[datasets]` {#datasets}

尽力而为档（`best_effort`，见[支持档位](../datasets/catalog.md#支持档位)）默认不调度：只在启用后才开始积累的实时快照、已停更的来源，以及经常失败的接口。要启用，列在这里：

```toml
[datasets]
best_effort = ["news_headlines", "flash_news_wire", "hot_rank"]   # 或 ["all"]
```

配置文件自己的调度组 `steps` / `steps_add` 里写到的尽力而为数据集也视为启用；只算这份文件本身写的，不算继承的内置默认值。所以完整展开的旧配置照旧运行它列出的步骤，`extends = "builtin"` 的新配置默认全部关闭。未启用的数据集从各调度组移除，新鲜度显示为 `removed` 而不是 `stale`；`cne backfill` 仍可显式运行。`cne config minimize` 会把完整配置启用的集合写进 `best_effort`，行为不变。

## `[job.daily.waves]`

Wave DAG：每个 wave 含 `name`、`parallel`（wave 内 step 是否并行）、`steps`（step 名列表）。

默认四波：

1. `reference` — instruments, trading_calendar（并行），trading_status 等 instruments 完成后再跑（要拿到当天新上市的证券）
2. `corp_actions_to_bars` — corporate_actions → daily_bars（串行）
3. `parallel_core` — index_bars
4. `finalize` — compact, derive_adj_factors, audit

`validate_config` 要求至少一个 wave，且所有 step 名必须在 `STEP_REGISTRY` 中。

## 调度组

`[job.daily.groups.<name>]`：`at`（文档/调度参考时间）、`steps`（含末尾 `compact`）；继承默认值时用 `steps_add` / `steps_remove` / `enabled = false` 修改，见[继承内置默认值](#extends-builtin)。

日更与补跑入口使用北京时间 `[job.daily].run_at`（默认 17:30）、`[job.stale].run_at`（默认 21:00）。组内字段 `at` 是运行参考，不会仅凭配置自动安装系统调度器。

`core`、`capital`、`signals`、`fundamentals`、`macro_risk`、`research` 是日常数据组；分钟、分笔和衍生品按各自开关启用。以随包模板为准：继承默认值的配置升级后自动获得新增的 step，完整配置用 `cne config upgrade` 补上；不复制个人主机的耗时来设定所有人的任务间隔。

各 daily 任务共用非阻塞写入锁。优先由一个调度入口顺序运行所需组；多个独立定时任务可能撞锁而跳过，必须检查退出码与 run 状态。

不带参数的 `cne run daily` 按配置顺序跑完全部组，再跑事件流；`cne run daily --group <name>` 只跑该组 steps。

### 运行时间（`[job.daily]` / `[job.stale]` 的 `run_at`）

```toml
[job.daily]
run_at = "17:30"   # 北京时间（默认 17:30）
[job.stale]
run_at = "21:00"   # 北京时间（默认 21:00），只补快照类
```

定时任务每小时唤醒，每个交易日过了这个北京时间点各跑一次，与本机时区和夏令时无关。
详见 [runbook](../operations/runbook.md)。

### 取数频率（`cadence` / `weekday`）

每个组可以单独设频率，默认每天：

```toml
[job.daily.groups.fundamentals]
cadence = "weekly"   # "daily"（默认）或 "weekly"
weekday = 5          # 每周组在哪天之前的最后一个交易日跑，ISO 1=周一…5=周五（默认 5）
```

- **每周组只让「能按日期补」的数据集休息**：下次跑时从水位线之后逐日补齐，所以不丢数据，
  总请求量基本不变，只是集中到一次。
- **快照类数据集照常每天跑**，即使它所在的组是每周（资金流、热榜、ST/停牌状态等只有「今天」，
  漏一天就永久缺一天）；它们依赖的同组步骤（如 `trading_status` 需要当天的 `instruments`）也一起跑。
  哪些是快照类看[数据源说明](../datasets/sources.md)或 `DatasetSpec.fetch_semantics`。
- 每周组在当周最后一个交易日跑（不晚于 `weekday`）：周五休市就提前到周四，不会整周跳过。
- 休息日整组没有要跑的步骤时，`cne run daily --group` 输出 `skipped_not_scheduled`，日更脚本记为 SKIPPED。
- 新鲜度跟着频率走：只由每周组负责的历史数据集，按该组上次应跑的交易日判断是否过期，
  所以 `cne status`、`[job.stale].run_at`（默认北京时间 21:00）的补跑和健康门禁不会在休息日把它当成落后、再去抓一遍。
- 一个数据集同时在每天和每周的组里时，按每天算。

## `[source_health]`

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `vantage` | string | `local` | 本机网络出口的标签（`cn`、`overseas` 或自定义，如 `cn-sh`），源探针和源健康记录都带上它；有些来源拒绝非大陆出口，结果按标签分开解读。`CNE_SOURCE_VANTAGE` 和 `--vantage` 可在单次运行时覆盖 |

## `[job.maintenance]`

`cne run maintain` 的设置；`cne schedule install` 安装的定时任务在每个交易日日更后运行它，见[运行手册](../operations/runbook.md#maintain)。

| 键 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `full_audit_weekday` | int | `6` | 全湖审计的 ISO 星期（1 = 周一 … 7 = 周日）；它读全部 Parquet，大湖可能要几个小时。0 表示从不 |
| `backup_dir` | string | `{data.root}/backups` | 元数据备份目录；相对路径按配置文件所在目录解析，最好放在另一块盘 |
| `backup_retention_days` | int | `14` | 删除早于这么多天的备份；0 不按天数删 |
| `backup_retention_count` | int | `30` | 只保留最新的这么多份；0 不按份数删 |
| `gate_groups` | list | `["core"]` | 这些日更组的运行失败时，维护立即判失败 |
| `soft_fail_max_days` | int | `3` | 其他日更组失败只告警，连续失败这么多个交易日后判失败；0 表示从不升级 |

## 事件流调度组（7x24）

`[job.events.groups.<name>]`：字段与调度组相同（`at`、`steps`、`parallel`），但属于
另一个任务族——`cne run events`。区别只有两点，都是必需的：

- **不看交易日历。** 上市公司周六也发公告，资讯源全天候更新；`daily*` 任务在非交易日
  直接 `skipped_non_trading_day`，事件流不会。
- **另一把锁。** 事件流拿 `events_ingestion`，不是 `daily_ingestion`，所以晚间批处理
  跑到一半时事件流照样能跑，反之亦然。

| 组名 | 典型时间 | 内容 | 代价 |
|------|----------|------|------|
| `disclosures` | 20:00 | `announcement_index` | 每次重读 30 天对账尾窗，不宜高频 |
| `regulatory` | 20:20 | `regulatory_events` | 由**已提交**公告投影而来，必须排在 `disclosures` 之后 |
| `news_wire` | 21:00 | `news_headlines`、`flash_news_wire` | 单张实时页，想要日内新鲜度就单独高频跑这一组 |

`cne run events` 按配置文件里的先后顺序依次跑每个组（各自 `compact` 发布），
`--group <name>` 只跑一个。不带参数的 `cne run daily` 在日更组之后也会跑一遍全部事件流组。定时器见
[`scripts/scheduler/events_pipeline.sh`](../operations/scripts.md) 与 `com.cnequity.events` agent。

`validate_config` 在这里守两条：组里只能放**自然日**数据集
（`DatasetSpec.session_scope = "calendar"`），且同一个 step 不能同时出现在 `[job.daily]`
和 `[job.events]`——两个任务持不同的锁，同时抓同一个数据集就是并发写同一份 staging。

`sentiment_scores` 仍留在 `research` 组：它读的是**湖里已提交的**公告和资讯，
从来不是同一次 run 里现抓的，所以拆开之后行为不变。

### 事件流计划（`[[job.events.schedules]]`） {#event-schedules}

打包定时任务（`cne schedule install --events`）按这里的计划运行事件组；没有写计划时，用控制台设置的单个事件组和间隔。可以写多条，每条一个事件组：

| 键 | 默认 | 说明 |
|---|---|---|
| `group` | 不写 = 全部 | `[job.events.groups]` 中的组名；不写时按配置顺序跑全部组 |
| `every_minutes` | `60` | 两次尝试开始之间的分钟数（1–1440） |
| `jitter_seconds` | `0` | 每次运行后把下一次随机延后 0–N 秒（0–3600），避免整点齐发请求 |
| `quiet_hours` | 不写 | 北京时间的静默时段，如 `"1-7"`（01:00–06:59），`"23-6"` 跨午夜 |
| `quiet_every_minutes` | `60` | 静默时段内的间隔，留 5 分钟余量 |

```toml
[[job.events.schedules]]
group = "news_wire"
every_minutes = 15
jitter_seconds = 120
quiet_hours = "1-7"
quiet_every_minutes = 60

[[job.events.schedules]]
every_minutes = 360
jitter_seconds = 300
```

同一个组不能写两条；组名必须存在。控制台的定时任务页只读展示这些计划，修改要改配置。

## `[minute_bars]`

可选日内线。默认关闭，且**不在** `[job.daily.waves]` 上；其请求量和磁盘量随标的范围、回溯窗口与交易活跃度增长，不会自动成为 `cne init` 的成本。开启后用 `cne run daily --group intraday` 或 `cne backfill`，先在小范围核对实际用量。

| 键 | 默认 | 说明 |
|----|------|------|
| `enabled` | `false` | 总开关 |
| `scope` | `"index:000300.SH"` | `index:<symbol>` / `watchlist` / `all` |
| `symbols` | `[]` | `scope = "watchlist"` 时的显式列表 |
| `frequencies` | `["1m"]` | `"1m"` → `minute_bars`；`"5m"` → `minute_bars_5m` |
| `fetch_workers` | `4` | 并发 TDX 连接数；共享间隔仍限制请求起点，增加连接不代表源方允许更高频率 |

**源端视野**是滚动窗口，当前适配器按[数据集目录](../datasets/catalog.md)中的可用起点拒绝过早的 `--start`，避免无效扫描；实际上游保留期可能变化。请求与磁盘成本见[日内数据运行手册](../operations/runbook.md#日内数据minute_bars--minute_bars_5m)。

## `[futures]`

逐合约期货/期权默认 `enabled=false`，不进入 `init`。启用后由 `derivatives` 日更组执行；分钟行情还需 `minute_enabled` 与 watchlist。交易所选择、历史下限、合约参考与 Greeks 参数集中见[衍生品指南](../recipes/derivatives.md)，完整键以随包模板为准。

## `[trade_ticks]`

分笔成交记录。**自成一段**，不是 `[minute_bars]` 里的一个开关——两者的量级差一个数量级，开启分钟线不该悄悄把它一起带上。

| 键 | 默认 | 说明 |
|----|------|------|
| `enabled` | `false` | 总开关 |
| `scope` | `"watchlist"` | `index:<symbol>` / `watchlist` / `all` |
| `symbols` | `[]` | `scope = "watchlist"` 时的显式列表 |
| `max_symbols` | `200` | 单次抓取的标的数上限 |
| `fetch_workers` | `4` | 并发 TDX 连接数；共享请求间隔仍限制起点，增加连接不会提高配置的请求速率 |

**这不是逐笔成交。** A 股 Level-1 是 3 秒快照，一行聚合的是那个时间片里落下的全部真实成交。时间戳只到分钟——协议从来没带过秒——所以行用 `tick_seq`（会话内位置）标识。`direction` 是 TDX 自己按 tick rule 猜的主动方，不是交易所字段。

**历史视野**：TDX 对每个标的都回溯到 2024-01-02，是**固定底**而非滚动窗口，与分钟线的每标的 bar 数上限无关。`cne backfill trade_ticks --start` 早于此会直接拒绝。

用 `cne run daily --group ticks` 或 `cne backfill trade_ticks` 采集。

## `[quality]`

| 键 | 默认 | 说明 |
|----|------|------|
| `audit_gate` | `"shadow"` | 湖审计报 `error` 时这次 run 怎么办 |

三档：

- `off` — 什么都不记，永不失败（0.8.2 之前的行为）
- `shadow` — 把"本该被拦下"的记下来，但让 run 成功
- `block` — 让 run 失败，`cne status` 与 pipeline 退出码都能看见

audit step 依赖 `compact`，所以它在**行已经进 curated 之后**才跑：`block` 是让 run 失败，不是阻止写入。shadow 模式每个受影响的 run 往 `meta/quality/audit_gate.jsonl` 追加一行——切到 `block` 之前应该先读它。一个不知道会多频繁触发就打开的门禁，很快会被关掉。

只有 `error` 触发门禁；`warning` 和 `info` 不触发。见 [产品边界](../architecture/overview.md)。

## `[incremental]`

| 键 | 默认 | 说明 |
|----|------|------|
| `negative_evidence_ttl_days` | `7` | 「源端此处为空」这类否定证据的有效期；设 `0` 则每次都重试缺失键。标的目录发生 revision 时，仍然会让在有效期内的证据失效 |
| `deep_reconciliation_dow` | `6` | 每周做一次深度对账的星期，1=周一 … 7=周日；`0` 关闭分层、每次都做深度对账。`announcement_index` 平时回看 7 天、深度日 30 天；`daily_bars` 平时只取新交易日（交易所整版行情和 TDX 批量报价），深度日在该星期当天或之后的第一个交易日逐只复核最近 5 个交易日 |

公告可能延迟索引。每周深度对账用于补查较早窗口，减少日常重复请求，但也可能延后发现晚到的公告。严格 PIT 还检查湖内观察时间；延后采到的公告不能在更早的 `as_of` 中冒充已知。

## `[raw_archive]`

| 键 | 默认 | 说明 |
|----|------|------|
| `enabled` | `true` | 是否把源端原始响应压缩存到 `meta/raw` |
| `compression` | `"gzip"` | 压缩方式 |
| `max_payload_bytes` | `33554432` | 单个响应的存档上限（32MB） |
| `datasets` | 内置关键/快照集合 | 留空使用默认集合；财报及股东回填的分页原始响应会逐页归档以保留中途成功的证据。显式列表可按磁盘预算收窄。 |

**请求凭证、代理设置、Cookie 和 authorization 头一律不存档。**

财报 PDF 等大原文可以放到独立磁盘或 iCloud Drive，本地仍保留来源、观察时间及哈希记录。先创建目标目录，再登记：

```bash
cne storage raw-external --dataset financial_statement_items --source cninfo \
  --root "$HOME/Library/Mobile Documents/com~apple~CloudDocs/CNEquity/archives/my-lake"
```

默认仅把新 PDF 原文写到外置目录，其他响应继续留在本地。登记保存在该湖的 `meta/raw_storage.json`，所有归档读者自动识别；不使用软链接，不改写历史 `payload_path`。命令只登记，不自动迁移旧文件。迁移时按原 `payload_path` 在目标根下复制、核对字节和哈希；确认副本完整及云端上传完成后才移除本地原文，保留本地 `.json` 来源记录。

原文重放需要外置目录可用；云端文件可能需要先下载。目录不可用时会明确报错，不会重建一个空目录或重新请求供应商。可移植快照会包含外置原文且不携带本机路径，生成快照可能触发云文件下载。iCloud 同步和“移除下载”由 macOS 管理，登记命令不代表已经上传成功。

## `[storage.retention]` {#storage-retention}

回补和日更每次发布都会留下一代不可变副本。`cne backfill` 和 `cne run daily` 结束时，每个数据集只留下最近 5 代，并删掉更早的代目录。这个数量和存储运维页上“最近 5 代”是同一个口径。当前正在读取的一代、`cne storage hold` 登记过的代、收据对不上或没有收据的代，都不会删。收据仍留在 `meta/revisions/`，用来说明当时发布了什么；再按被删掉的代去读，会明确失败，不会改读更新的一代。`cne run clean` 仍然只预览；要删得比这更激进，还是在 `cne serve` 的存储运维页确认。

| 键 | 默认 | 说明 |
|----|------|------|
| `keep_generations` | `5` | 每个数据集保留的最近代数。`0` 不自动删除 |

和上一代硬链接共享的文件不会因为删掉旧目录而消失，报告里的 `freed_bytes` 只计算删除后没有任何保留代还指向的字节。没有导入引用清单时，按“没有外部引用”处理。要钉住某一次发布给研究复现，先 `cne storage import` 再 `cne storage hold`，或者把 `keep_generations` 设为 `0`。小磁盘可以把这个数改小。

清理失败（引用清单已过期，或还有未完成的删除）不会让已经发布成功的回补失败；原因打在 stderr，下一次回补或日更会再试。

## `[exchange_audit]`

| 键 | 默认 | 说明 |
|----|------|------|
| `price_tolerance_bps` | `10` | 收盘价偏差容忍（基点） |
| `turnover_tolerance_bps` | `100` | 成交额偏差容忍（基点） |
| `turnover_max_fraction` | `0.15` | 触发 finding 所需的全域占比 |

把 `daily_bars` 与上交所、深交所自己发布的收盘价对照——**全湖唯一一个能触达发布方而非第二个转售方的价格检查**。受 `[sources.exchange]` 控制；findings 是建议性的，永不让 run 失败。

上交所只提供当前正在发布的那个会话，所以 SH 是当日仲裁；深交所可查任意历史日期。

交易所日总额与日线数据可能存在统计范围差异。成交额检查使用独立容差，并按偏差标的占比触发 finding；出现告警时先核对口径与来源。

## `[margin_trading]`

| 键 | 默认 | 说明 |
|----|------|------|
| `source` | `"exchange"` | 融资融券明细的来源 |

`"exchange"` 直接读上交所与深交所的融资融券明细，它们由会员单位报送汇总——中间没有转售方。

## `[job.init.phases]`

| 键 | 说明 |
|----|------|
| `names` | init 阶段顺序列表 |

默认：

```toml
names = [
  "phase1_reference",
  "phase2c_daily_bars_backfill",
  "phase2a_corporate_actions",
  "phase3_index_and_status",
  "phase4_finalize",
  "phase5_derive_and_publish",
]
```

阶段 → step 映射见 `orchestrator/init_phases.py`。0.17.0 起日线阶段排在公司行为之前；原样保留旧默认顺序（公司行为在前）的配置自动按新顺序执行，改过的列表照写的顺序执行，已开始的 init 按它记录的顺序续跑。

## `[on_demand]`

| 键 | 说明 |
|----|------|
| `enabled` | OnDemandService 开关 |
| `datasets` | 按需抓取的数据集名列表。默认仅 `stock_news`、`research_reports`；`announcement_body` / `financial_reports` 尚未实现 |

缓存路径：默认请求为 `meta/on_demand/{dataset}/{symbol}.json`；带有会改变结果的参数时，会使用同目录下带请求摘要的变体文件，避免不同日期、条数或情感模型查询互相复用。通过 `cne query --dataset X --symbol Y` 访问；需要强制更新时追加 `--refresh`。失败或未实现的结果不会写入缓存。

## `[duckdb]`

| 键 | 默认 | 说明 |
|----|------|------|
| `path` | `{data.root}/duckdb/cnequity.duckdb` | 支持 `{data.root}` 占位符 |
| `memory_limit` | `2GB` | DuckDB 内存上限 |
| `threads` | 4 | 查询线程数 |

## `[research]`

可选。只影响 `cne decision-data` 的证据盘点命令，不影响采集与查询。

| 键 | 默认 | 说明 |
|----|------|------|
| `holdout_start` | 未设置 | 留出期起始日（如 `2025-01-01`）。窗口触及该日即在读湖前拒绝，`--end` 默认取其前一天，让研究用的证据盘点看不到留作样本外检验的数据 |

## 环境变量

下列变量用于 [运维脚本](../operations/scripts.md)。CLI 也读取 `CNE_CONFIG`（优先级低于显式 `--config`）和 `CNE_LOG_DIR`；`CNE_RATE_LIMIT_ROOT` 由共享源保护使用。其余脚本变量不应当作通用 CLI 参数。

| 变量 | 默认 | 作用 |
|------|------|------|
| `CNE_CONFIG` | `configs/cnequity.toml` | 脚本传入 `cne --config` 的路径 |
| `CNE_LOG_DIR` | `{data.root}/logs` | 日志目录。`cne init` / `cne backfill` / `cne run` 也读它，并把本次运行的日志写成 `cne-<命令>-<时间戳>.log`，启动时打印路径 |
| `CNE_GROUPS` | 按配置顺序选择已启用调度组 | 覆盖 pipeline 要跑的组 |
| `CNE_NOTIFY` | `1` | `0` 关闭 macOS 通知 |
| `CNE_BACKUP_DIR` | 湖内 backups | 元数据备份目录 |
| `CNE_BACKUP_RETENTION_DAYS` | 14 | 备份保留天数 |

## 配置与代码关系

```
cnequity.toml
    → load_config() → Config dataclass
    → validate_config() → 引用 step/group 合法性
    → JobEngine(cfg) / load(..., config=cfg)
```

`Config` 还提供：`staging_root`、`curated_root`、`derived_root`、`meta_root`、`manifest_path`、`rate_limit(source)`。

## 发布前与发布后的审计

`[quality].publication_gate` 支持 `off`（默认）、`shadow`、`block`。它在普通 compact 发布前对整批候选作离线全量审计，比较已提交基线；block 模式拒绝新增或恶化的 error，审计异常也拒绝。问题按“检查项 + 数据集 + 范围（分区、字段、来源、日期等）”识别，行数、键数等度量变多才算恶化；文案或样本变化、问题减少都不算新增。报告的 `issue_changes` 分列新增、恶化、改善、未变和已消除。结构检查只读候选数据集的变更分区；跨数据集检查只在其输入含候选数据集时运行，其余检查结果前后相同，不重复计算。派生发布使用相同门禁。

`cne repair` 子命令加 `--apply` 时始终执行阻断式结构审计。历史因子/公司行为矛盾单独写入非阻断诊断，不拒绝发布；无效因子数值、损坏文件和主键错误仍可阻断。拒绝发布时保留旧版本，并把候选移入 `_quarantine`。

`[quality].audit_gate` 仍决定发布后运行状态，两者互不替代。全历史扫描有 I/O 成本，可先 shadow 后 block。详见 [产品边界](../architecture/overview.md)。
