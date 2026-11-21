# 初始化、范围与续跑

适用：建立正式湖、扩大历史窗口或处理中断。第一次使用可先读[快速开始](quickstart.md)。

## 初始化前

```bash
cne init
```

找不到配置时（当前目录没有 `configs/cnequity.toml`，也没有用户级 `~/.config/cnequity/config.toml`），`cne init` 会先按随包示例生成用户级配置（与 `cne config create` 相同：写入绝对 `data.root`，macOS / Windows 默认 `workers=1`），再开始初始化。想先改数据目录等设置，可以先运行 `cne config create --data-root /path/to/lake`，修改后再 `cne init`。显式传入的 `--config` 或 `CNE_CONFIG` 指向不存在的文件时不会自动生成，避免拼写错误建出新湖。已有配置升级后运行 `cne config upgrade` 补上新的调度 step。

连不上数据源时先运行 `cne doctor`（离线体检），再看[排障指南](../operations/troubleshooting.md)。

`cne init`（quick / full）开跑前先花最多 8 秒做网络预检，探测和 `cne sources probe` 相同：通达信连不上时立即停下并说明原因，不再在第一个步骤里重试几分钟；交易所和东财只报告，不可用时相关步骤会降级，不挡住建湖。离线或确定网络没问题时加 `--no-preflight`，或设 `CNE_SKIP_PREFLIGHT=1` 跳过。配置里 `[tdx_protocol] allow_mock = true` 时不做预检。

也可以不先开终端做这件事：默认配置还不存在时，`cne serve` 会进入首次配置，在本机页面上填写数据目录、看体检结果，再启动和上面相同的 `cne init`。缺少的目录会自动建立。体检还在跑时不能开始初始化。页面会先显示将执行的命令，核对后再点“开始初始化”。目录里已经有数据湖时，页面会说明将接管该目录且不会清空，确认后才给出这条命令。如果配置已生成，但初始化参数有误或任务暂时被占用，可以修正参数或等占用结束后重新预览。改了深度、起点或“失败后继续”之后，也要重新预览。已经在跑的初始化关掉页面也不会停。没跑完的初始化在操作页上可以直接续跑。

`init` 按 `[job.init.phases]` 建目录、manifest、视图并回填证券、日历、公司行为、日线、指数、交易状态及派生数据。只建布局可用 `cne init --layout-only`；它不产生研究数据。

<a id="init-scope"></a>

## Init：范围、磁盘与续跑

默认 `cne init`（`--profile quick`）是沪深京全市场、近 3 年的主干。`--profile full` 仍是全市场，只是把主干加深到各数据集自己的历史起点，其中日线从 2016-01-01 起。

| 命令 | 实际范围 | 请求成本特征 |
|---|---|---|
| `cne init`（`--profile quick`） | 沪深京全市场 × 最近 3 年；证券、日历、日线、交易状态等主干 | 全市场，按证券与缺口分页 |
| `cne init --profile full` | 还是全市场；主干按各数据集默认起点拉取，日线从 2016-01-01 起 | 深历史，页数和落盘量更多 |
| `cne init --since 2018-01-01` | 全市场，从指定日期起 | 介于两者之间 |
| `cne backfill trading_status` | 补齐全市场历史 ST 证据 | 逐证券、按源节奏执行，宜安排长窗口 |

TDX / Baostock 的可达性、出口、上游限流、重试和机器配置都会改变耗时；`init` 启动时会打印范围；单数据集回填另有 `cne backfill DATASET --plan` 可离线审阅。`init` 没有 `--plan` 参数。

磁盘量随证券范围、历史起点、频率与修订增长，staging 还需工作空间。分钟线、5 分钟线和分笔默认关闭；全市场分钟数据通常远大于日频主干。扩范围前先小样测量，见[运行手册 · 日内数据](../operations/runbook.md#日内数据minute_bars--minute_bars_5m)。

默认 init 获取当前交易状态，并在日线发布后派生窗口内的历史停牌。**全市场历史 ST 扫描不属于 init，也不是初始化完成条件**；需要历史 ST 研究时显式运行 `cne backfill trading_status`。该命令不设每轮 400 只的上限，会按来源节奏持续扫描并保存 checkpoint；覆盖能力仍取决于市场、来源权限与证据收据，Baostock 不提供北交所历史 ST。

初始化会自动恢复配置市场中、历史窗口内的已知退市股票日线，并验证发布后的覆盖，再继续日线取数。来源缺失时保留有效恢复结果和待补范围，并继续能执行的后续阶段；程序、存储或完整性错误仍会失败。已发布的历史在后续补数中复用。此流程使用已知退市身份和目录，不进行全代码空间发现。

```bash
cne init --profile full --config configs/cnequity.toml

# 可选：需要历史 ST 研究时，独立补数并明确窗口
cne backfill trading_status --start 2016-01-01 --end YYYY-MM-DD --config configs/cnequity.toml

# 之后每天执行一次：全部日更组 + 事件流
cne run daily --config configs/cnequity.toml

# 只保持行情和基本面时：初始化下载不变，日更只跑对应调度组
cne init --pack market --pack fundamentals --schedule
cne check --pack market --pack fundamentals
```

`--pack` 不改变这次初始化下载的内容。`market` 是行情主干，`fundamentals` 是财报、股本和披露日程（日更组还会带上指数成分、行业成分和股东人数；估值历史另用 `cne backfill valuation_metrics`），`universe` 只标记需要历史 ST，不增加日更请求。不写 `--pack` 时沿用已保存的选择，第一次是 `market`。`--schedule` 在初始化成功结束后，为这些研究包安装当前用户的定时日更和收尾补抓，不含公告和资讯；安装失败不会把已经建好的湖判成失败。交易状态这类快照漏掉当天，下一次按日期的日更补不回这一天。

日线某一批封存之后，`load("daily_bars", symbols=[...])` 可以读到已经完成的未复权行情。不写 `symbols` 的查询仍只看已发布数据，所以半程扫描不会被当成全市场。复权因子在初始化收尾、`adj_factors` 发布之后才可用。

历史 ST checkpoint 按起止日与 universe 区分；跨天续跑请保持这三个范围不变。当前交易状态和日线派生停牌不等于完整历史 ST 证据。

升级前因 400 只 ST 上限或退市日线委派而未完成的 init，升级后直接重跑同一条 `cne init`，它会自动续跑原 run。续跑改为当前状态快照，自动恢复退市日线，保留成功批次和已抓取的 ST 暂存行；已有历史 ST checkpoint 不会被标记为完整。后续历史 ST 需求仍使用独立补数命令。

这里说的「完整」不是「全部数据集都有无限历史」。项目没有一条命令能把所有数据集的全部历史一次拉完。`init` 负责证券、日历、公司行为、个股/指数日线、交易状态和派生因子这些主干；分钟线、5 分钟线和分笔默认关闭，快照型数据也无法回补源端没有提供的历史。某个可回补的数据集需要更早的历史时，使用 `cne backfill <dataset> --start ... --end ...`；各数据集能拉到哪一年，见[数据集目录](../datasets/catalog.md)。

公司行为、指数日线或交易状态阶段失败（多为数据源暂时不可用）不会挡住建湖：日线照常入湖，结束时的摘要列出没完成的步骤。日线入湖之后的复权因子、行业指数、质量审计和停牌派生出错同样不会让已发布的湖变成不可用，也不会跳过后面的阶段；重跑 `cne init` 只补它们。证券目录、交易日历、日线和入湖发布失败仍会停下。

在 macOS 和 Windows 上，`cne init` 运行期间会阻止系统空闲睡眠，避免无人值守时睡过去、连接断开；合盖或手动睡眠仍会生效，进程结束即解除。不需要时设 `CNE_ALLOW_SLEEP=1`。

日线排在公司行为之前抓取。公司行为要向 TDX 逐只拉取全部除权除息记录，与历史窗口长短无关；初始化里只有收尾的复权因子派生用到它，日线不必等它。这次扫描按 `[source_concurrency].tdx_protocol` 同时开多条 TDX 连接（最多 4 条），请求仍受同一套 TDX 限速约束，全市场约十几分钟。没开基金采集（`[universe].ingest_listed_funds` / `ingest_eligible_etfs`）时不扫 ETF/LOF，约少四分之一请求；以后开启基金采集，日更的公司行为步骤会自动补扫还没扫过的基金（每次最多 500 只），扫完前告警列出缺历史的基金。

复权因子要向新浪逐只请求，受新浪限速约束，全市场约半小时。初始化在抓日线和扫公司行为期间于后台预取这些因子到本地缓存，收尾派生直接读缓存，到收尾时还没取到的由派生接着请求；预取失败的证券由派生时再请求，不影响建湖。

如果中途按了 Ctrl-C、进程被杀，结果是 `degraded`，或摘要列出了没完成的步骤，不要删除 `data/`，也不需要从头开始：重跑同一条 `cne init`，它会自动找到未完成的 run，从失败批次和缺失阶段继续。完成后用 `cne check` 验收。需要指定某个旧 run 时才用 `cne init --run-id RUN_ID`。

Ctrl-C 之后，命令会先停住正在跑的 worker，再把没跑完的批次记成可以马上重试的 `failed`；已经成功的批次不会重拉。进程被直接杀掉也没关系：下一次命令会根据运行锁认出这个孤儿 run。活动批次和未经封存校验的旧暂存数据仍受发布门禁保护；已经封存校验的独立事实可以发布。缺输入的派生步骤会解释跳过原因。执行结束不代表窗口内每只证券的证据都已齐全。

`status --datasets` 里的 `fresh` 只表示**已经落盘的那些日期**够新。正式数据湖还会用最新日线截面，对照配置的 `[universe].ingest`、当天 active 证券，以及明确的停牌证据，做一次轻量核对：范围内每只 active 证券都得有日线，或者有明确停牌证据。如果缺了整个配置市场（例如 `all_a` 没有 BJ），instruments 阶段会报告部分覆盖，已取得的目录仍可发布。init 没跑完、证券缺证据或截面核对不上都会另行报告。普通 `status` 查询成功返回 0；调度验收使用 `cne status --datasets --gate`，按缺口或失败返回非零。

「全市场」含北交所：上市状态、停复牌与 ST 取自北交所自己的板块页，日线历史优先走 TDX 的 BJ 专用路径，Sina 补未覆盖标的；历史 Sina 行的缺失成交额可经 TDX 逐行比对补齐。默认 universe `all_a` 覆盖沪、深、京三市的 A 股。每个数据集真正拉到哪一天，会记在 `coverage_start`。

需要更长历史时，可以一次拉满，也可以以后再补深：

```bash
cne init --profile full

# 或对单个数据集补历史
cne backfill daily_bars --start 2016-01-01 --end COVERAGE_START
```

**它跑到哪了？** 运行中会打这几类行；下表数值只是输出格式示例，
实际证券数、批次和耗时取决于自己的湖与来源：

| 行 | 含义 |
|------|------|
| `Step <名字> starting` / `Step <名字> success in Ns` | 步骤进出 |
| `daily_bars: 120 symbol(s) over … → 2 batch(es) … on 1 lane(s)` | 这一趟扫描的规模，开跑前就打出来 |
| `daily_bars 1/2 batches · 240 rows · 1m24s elapsed · ~1m24s left` | 滚动进度；凑满一轮 lane 后才给剩余时间 |
| `instruments 2/5 Beijing board · 41s elapsed · 7412 so far` | 证券目录逐段进度：每开始取一个来源（通达信、北交所、上市日期等）打一行 |
| `still working: daily_bars 4m12s (no output for 1m02s)` | 心跳，静默满 60 秒时点名当前步骤 |

启动时打印的 `Logging to …/logs/cne-init-<时间戳>.log` 是本次运行的日志文件（目录可用 `CNE_LOG_DIR` 覆盖）。另开一个终端也可以查：

```bash
cne status --run latest --config configs/cnequity.toml
```

**成本按标的数算，不按天数算。** `daily_bars` 的历史主路径逐标的抓取，所以 `--start D --end D` 只拉一天，默认仍会扫描整个配置范围；多年窗口的差别主要在每个标的返回多少根 K 线。想快速验证请用 `--symbols` 缩小范围，并先跑 `--plan`：

```bash
cne backfill daily_bars --symbols 600519.SH,000001.SZ --start 2026-09-15 --end 2026-09-15 --plan
```


## 执行结束与覆盖不足

`init`、日更、事件流、回填和重试共用执行、覆盖与发布三个状态。经过校验的部分结果可发布，命令以 `degraded` 结束并返回 0；缺口、来源冷却和未扫描范围继续保留。来源全部不可用、甚至本次取得 0 行时，命令也以覆盖不足正常结束并返回 0，保留已有湖，不制造完整覆盖。结果中的 `fallback` 列出可用数据、注册来源能力与原范围重试命令；程序错误、存储故障或完整性错误仍返回失败。

初始化职责内的来源限制不会留下必须手工运行另一命令才能关闭的执行计划。来源受限但阶段已经尝试或解释跳过的 init 不会被自动当成未完成 run；`--resume` 用于中断或真实执行错误。需要改善覆盖时，显式补数或重试具体范围。历史 ST 始终是可选的独立任务。

任务仍然可能因来源节奏、锁等待和计算耗时而运行较久；项目不承诺固定完成时间，也不把百分之百覆盖作为所有命令的成功条件。结果字段与旧调度脚本迁移见[CLI 结果契约](../reference/cli.md#命令结果与退出码)。

## 验收，而不只看完成提示

```bash
cne check
```

一条命令依次给出新鲜度与覆盖（同 `cne status --datasets --gate`）、数据质量（最近一次 run 的审计和全湖审计快照）和规模，最后给出结论；退出码 0 可用、1 有缺口或质量 error、2 证明不了。全湖审计要读每个历史分区，默认读最近一次的结果，需要当场重跑时加 `--full`。`--pack` 另按研究包给出窗口、缺口和下一步，缺数据或历史 ST 未覆盖会使退出码变差；不写 `--pack` 时结论与以前相同。

`fresh` 是新鲜度；审计健康也不等于任意历史窗口都可研究。全量审计会写报告，且按源开关执行外部核验，详见[命令副作用](../reference/cli-surface.md)。需要历史股票池时，再按真实研究窗口执行 `audit --full --research-start ... --research-end ...`，或使用严格 profile 查询。

有源码 checkout 时可用 `scripts/accept_backfill.py` 验收幂等性、覆盖起点和消费层，见[回填完成验收](../operations/runbook.md#回填完成验收)。这些脚本不随 PyPI 包安装。

## 接下来

- 持续运行：[运行手册](../operations/runbook.md)；日更与事件流分别调度。
- 扩范围前：[请求成本与源保护](../operations/fetch-policy.md)。
- 查询研究数据：[查询指南](../datasets/query-guide.md)与[研究示例](../recipes/README.md)。
