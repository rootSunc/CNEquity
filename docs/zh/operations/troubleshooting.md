# 故障排查

按症状定位：安装/路径先看 `doctor` 与配置；采集失败先看 run 和 batch；被限流先看 `sources limits`，冷却结束后再续跑。修复后按受影响范围验证。

首次使用见[快速开始](../getting-started/quickstart.md)，请求成本和保护机制见[取数策略](fetch-policy.md)。

## 症状：国证历史调样文件不存在

如果来源返回“样本历史调样文件不存在”，客户端会报 `CNI publisher does not serve adjustment history`。这是来源未提供历史文件，不是有效的空名单。保留已有历史和失败记录；恢复时需要完整的官方历史名单或可核验的调整公告链。当前成分及当前权重不能替代过去的成分与权重，也不能据此推进历史覆盖水位。

## 症状：EastMoney datacenter `code=9501` / `列不存在`

| 现象 | 原因 | 处理 |
|------|------|------|
| `EastMoney datacenter RPT_… rejected schema: XXX列不存在 (code=9501)` | 来源返回的列与当前客户端契约不一致 | 先停受影响的组，保留报表名与错误码；检查是否已有修复版本，或提交脱敏 Issue，不要把失败当空数据 |
| 日更整组失败、错误带 report 名 | 同一列契约错误会阻止该组发布 | 更新修复版本后按失败 run 重试，并复核水位与实际列；不要靠反复请求或跳过校验推进水位 |

维护者需在隔离环境核对新旧列语义后更新适配器与契约测试；用户无需手工修改安装包。

## 症状：baostock / 免费源「黑名单」或频繁失败

| 现象 | 原因 | 处理 |
|------|------|------|
| `baostock` 黑名单（`10001011`）或「今日请求已达 50000 次上限」 | 每日 API 请求上限 5 万次，且不能并发连接。本自然年每次进入黑名单冻结 `次数 × 6` 小时；响应里没有待释放时间时，至少 5 分钟内不再刷新 | 客户端超限时停在本地，并按上述时长冷却；已有连接时最多排队 10 分钟，仍未释放就不登录。已经取到的标的会留下。冷却结束前不要换 IP、不要重跑；结束后用默认限速重跑同一条命令，从尚未完成的标的继续 |
| 东财 429 / Empty reply / 连接被掐 | 429 表示限流；断连还可能来自代理、网络或服务故障 | 暂停受影响源，遵守共享冷却与主机熔断；需要停用 push2 时设置 `push2_paused = true`，见下文「东财 502」 |
| cninfo / pboc 间歇失败 | 同源风控 / 站点抖动 | 已按页/按调用 `rate_limit`；社融主写入按年取全量，任一年失败即阻止本次宏观写入，待下次完整重试 |

原则：**时间可以等，封禁成本远高于多等一天。** 勿为加速关掉 `min_interval` 或开多进程打同一免费源。

估值历史回填另有 **单飞锁**（`RunLock("baostock")`）：并发
`valuation_2001` / float_mv 扫盘会直接跳过并留下 `baostock_single_flight` warning。
除此之外，Baostock 的连接租约会在第二次 `login()` 之前失败，不会再开一条连接。

## 症状：valuation_metrics 水位「新鲜」但覆盖率断崖 / STALE

| 现象 | 原因 | 处理 |
|------|------|------|
| 最近几天只有少量标的、`valuation_bars_low_coverage` | 当日估值快照可能不完整 | 查看 `capital` 组和源状态；水位门禁会拒绝把稀疏分区当作完整当日覆盖 |
| `cne status --datasets` valuation STALE + 覆盖不足 | 可能是历史窗口或 tip 抓取不完整 | 检查缺口日期与来源。历史部分使用可回填源；当前快照只能修当日，不能通过 `--trade-date` 将今天的数据标成历史 |

```bash
# 先查看历史修复范围、登记源和成本；日期按实际缺口填写
cne backfill valuation_metrics --start 2026-09-01 --end 2026-09-04 --plan
# 当日资本与估值快照由日常组采集
cne run daily --group capital
cne status --datasets
```

审计 finding `valuation_watermark_coverage_gate` 用于指出水位与当日截面不一致；
按实际缺口修复后再检查，不能只看最大日期。

## 症状：manifest 里大量 status=running 的僵尸 run

| 现象 | 原因 | 处理 |
|------|------|------|
| `cne status` 显示 `orphaned_running_runs > 0` | 进程被杀或 OOM，run 留在 `running` | 确认原进程已退出；下一次 `cne run daily` / `cne run retry` 会核对并恢复孤儿 run |
| 需要立刻清理 | — | `cne run clean --reconcile-runs`（跳过仍持锁的 live run） |

长任务（baostock 回填）靠 **batch heartbeat** 保活，不会仅因 `started_at` 超过 1h 被误杀。

### BrokenProcessPool / worker 池被毒死

| 现象 | 原因 | 处理 |
|------|------|------|
| 日志 `worker pool broke (likely OOM under load); retrying … serially` | 一个子进程退出导致 worker 池不可用 | 未完成批次会串行重试；保留日志检查 OOM 和仍失败的范围，不要手工删除成功批次 |
| macOS 上频繁 OOM / 池崩溃 | TDX 客户端非 fork-safe + `workers>1` | `cne config validate` **拒绝** Darwin 上 `workers>1`；生产用 `workers = 1`（见 runbook / `daily_pipeline.sh`） |
| Windows 上 `import fcntl` / 文件锁失败 | 旧版用 Unix-only `fcntl.flock` | 升级到含 `cnequity.file_lock` 的版本；锁在 Win/POSIX 上语义一致 |
| Windows 上 DuckDB 视图空 / 路径错 | 反斜杠进了 `read_parquet` SQL | 新版本用 `as_posix()`；确认 `data.root` 可读写后重跑 `cne init` / 刷新视图 |
| Windows 上 PowerShell `&&` 语法错误 | PS 5.1 不支持 `&&` | 分行执行，或用 PowerShell 7+ / cmd |

## 症状：load() 读不到新数据

| 可能原因 | 检查 | 处理 |
|----------|------|------|
| 数据仍在 staging | `ls staging/*/run_id=*` | `cne run compact` 发布所有已结束未发布的 run，或 `cne run retry`；**先 compact 再** `cne run clean`（勿 `--force`，否则 demote 后只能重抓） |
| 分组 run 未 compact | 组 steps 是否含 `compact` | 配置修正后重跑组 |
| compact 被 gate 跳过 | `cne status` 看 failed batch | `cne run retry --run-id <id>` |
| 路径错误 | `config.data_root` | `cne config path` 查看实际生效的配置和数据目录 |
| 初始化还在跑，不带 `symbols` 的 `load("daily_bars")` 为空 | 未发布的全市场扫描不会被当成已发布数据 | 指定已经封存的代码，例如 `load("daily_bars", symbols=["600519.SH"])`；复权因子要等 `adj_factors` 发布 |

## 症状：daily_bars「interior symbol×session key(s) remain absent; refusing to checkpoint」

core 组每个交易日都失败，`daily_bars` 水位不前进，当天抓回的行留在 staging 没有发布。

| 现象 | 原因 | 处理 |
|------|------|------|
| `RuntimeError: daily_bars <start>..<end>: N interior symbol×session key(s) remain absent` | 窗口内有标的缺了中间某个交易日，且主源与所有备源都补不上 | 先看缺的是哪些标的（见下），再决定是收口范围还是修源 |
| 缺的几乎都是 158/159/160/51x/52x/56x 开头的代码 | 这些可能是 ETF/LOF，而不是默认 A 股范围 | 核对 `[universe].ingest` 与实际研究范围；默认 `all_a` 不抓 ETF/LOF，显式扩范围后要单独验收来源覆盖 |
| 缺的是真 A 股，且只缺当天 | 备源额度被别的标的耗尽（日志里 `circuit opened … leaving N symbol(s) unresolved` / `sina bars HTTP 456`） | 等下一轮，或 `cne run retry --run-id <id>` 复用已抓到的批次 |
| `expected key(s) remain unknown after failover`，且日志里有 `EastMoney kline circuit opened` | 东财历史主机（`push2his`）对当前出口不可达；仅有一个源返回空不足以证明该日无需行情 | 跑 `cne sources limits` 看冷却，再用 `cne sources substitutes` 核对独立备源；保留未解决键并在来源恢复后定向补数，不把空结果当作停牌证据 |

缺失明细写在 `meta/quality/findings/<run_id>.json`，带 `missing_symbols` 与 `sample_keys`：

```bash
cne status                      # 取失败的 run_id
python - <<'EOF'
import json, sys
run = "RUN_ID"
d = json.load(open(f"data/cnequity/meta/quality/findings/{run}.json"))
for f in d["findings"]:
    if f["check"] == "daily_bars_interior_gap":
        print(f["missing_keys"], "keys /", len(f["missing_symbols"]), "symbols")
        print(f["sample_keys"])
EOF
```

`daily_bars` 有配置的对账回看窗口，但复用已有覆盖、显式范围和缺口裁剪会影响实际请求。单日与多日路径都存在；以本次计划、批次窗口和 findings 判断生效门禁，不从命令名称推定。
认证在门禁**之前**执行：被证实无数据的标的会写入负面证据（TTL 见 `[incremental]`），
之后 TTL 内不再请求；门禁只对认证之后仍然缺失的键报错。

> 不要为了绕过它去调 `min_interval_*` 或放宽熔断阈值 —— 这个报错说明缺口是真的，
> 提速要靠缩小取数范围（`[universe].ingest`），不是靠加快请求。

## 症状：cne run daily 失败

1. `cne status` 查看 `run_summary` 与 failed batches
2. 查看日志 `error_message`（TDX 断连、HTTP 429、schema 校验失败等）
3. `cne run retry --run-id <id>`
4. 若 TDX 问题：`cne sources probe --only tdx_protocol`；换 `[tdx_protocol.hosts].standard`
5. 若单数据集持续失败：`cne backfill <dataset>`（需支持 backfill）

### daily_bars：TDX 批次失败但 tip 仍有数据

- **现象**：日志 `daily_bars_clist_gapfill` / `routed … through EastMoney clist`；部分行 `source=eastmoney`。
- **原因**：TDX 主源部分/全部失败时，tip 日对**缺失 key**走东财 push2 clist（全市场分页），多日窗口另走 per-symbol kline。这是 缺失键补数，不是静默换主源。
- **处理**：可接受则继续；要纯 TDX tip 时修好 TDX 后对当日 `cne run daily --group core --trade-date …` 重跑（普通行情 canonical 合并按抓取时间与来源平局规则选择，并为业务变更发布 revision）。
- **多日回填**失败仍走 kline gap-fill（慢）；clist **不能**伪造历史。

### 周末 / 漏跑后水位落后

- **现象**：今天非交易日时 `cne run daily` → `skipped_non_trading_day`；`daily_bars`
  水位停在上上个交易日；下游 freshness 门禁不过。
- **处理**（补 core + `market_breadth`）：

  ```bash
  uv run python scripts/run_catchup.py                      # 默认：最近一个交易日
  uv run python scripts/run_catchup.py --trade-date 2026-07-17
  # 国内出口再补 capital/research（东财失败不挡门禁 exit 0）：
  uv run python scripts/run_catchup.py --trade-date 2026-07-17 --all-groups
  # 或分步：
  uv run cne run daily --group core --trade-date 2026-07-17
  ```

  全组补跑：`scripts/scheduler/daily_pipeline.sh 2026-07-17`（或 `CNE_TRADE_DATE=...`）。
  **不要**对漏跑日随便加 `--backfill`：东财 CA 全量扫描在海外常直接失败。
  实际调度组应依据当前出口的可达性选择；未成功采集的数据保留缺口，不能仅凭地域将滞后标为正常。

### 东财 502 / 连接被重置（出口 IP 被封）

- **现象**：`cne sources probe` 里 `eastmoney_push2` 报 `HTTP 502`、`eastmoney_push2his`
  报 `Connection closed abruptly` / Empty reply；日更 core 正常（行情走 TDX），资金面 / 板块组失败。
- **判断**：502、连接重置可能来自路由、代理、源故障或访问控制；单个出口的结果不能证明整个地域被封，也不能单凭状态码证明是 IP 黑名单。
- **处理**：先停止重复请求。`[sources.eastmoney] push2_paused = true` 或 `CNE_PUSH2_PAUSED=1` 会在本地暂停 push2 系请求；其它源可按已有配置继续。
- **恢复**：先用 `cne sources limits` 读冷却截止与预算；等共享冷却和熔断到期后，解除显式暂停，再执行一次 `cne sources probe --only eastmoney_push2`。探测遵守采集保护；不要绕过预算或删除共享账本。
- **防止复发**：保留 push2 快照复用、4 秒间隔、单请求在途、每日预算和熔断；失败时缩小回填范围。`Retry-After` 更长时继续等待。默认值不是服务端承诺，实际请求数取决于当天范围和分页。
- **多个湖**：同一出口共享 `CNE_RATE_LIMIT_ROOT` 并统一调度；EM 的日预算与熔断也由该目录协调。详见[取数与源保护](fetch-policy.md)。

代理是网络配置，不是解除封禁的方法。不要轮换出口继续已被拒绝的抓取；独立数据源的可用性与口径应单独核对。

### sector_bars backfill 大量失败

- **现象**：日志 `THS sweep: BKxxxx (板块名) failed: ...`；`failed_sectors` 数量偏高。
- **原因**：**这是同花顺的源,不是东财**——`[sources.eastmoney] proxy` 对它不生效。
  `d.10jqka.com.cn` 对密集请求会限速甚至封禁。
- **处理**：冷却后提高 `[sources.ths] min_interval_seconds`（默认 1.0，不是安全阈值保证），再 `cne backfill sector_bars`；
  默认从断点续跑；换源后全量重拉用 `--refetch`。Checkpoint：`meta/state/sector_bars_backfill.json`。
  失败率超过 50% 时 step 状态为 `warning`，已成功的板块仍会写入。

### 在另一台机器部署

先按安装指南配置独立环境和个人配置，再通过 `sources probe --only <key>` 小范围确认可用性。HTTP 代理不一定能承载 Baostock/TDX 的 TCP 协议；不要根据地域猜测源是否可用。

需要历史回填时直接使用 `cne backfill sector_bars` 或 `cne backfill trading_status --symbols <代码>`，保留原 checkpoint，不自动强制整湖重抓。数据转移使用经过审查的快照流程，不把带个人凭证、日志和数据的整个工作目录当安装包复制。

## 症状：audit --full UNHEALTHY

| Finding 类型 | 含义 | 处理 |
|--------------|------|------|
| `pk_unique` | PK 重复；`--full` 对全历史执行检查 | 查最近 compact；必要时 backfill 重跑该分区 |
| `mixed_partition_granularity` | 盘上仍有细粒度分区叠在年/月分区上，同一 PK 跨粒度重复 | 优先 `cne repair repartition <dataset> --apply`（或 `--all`）按 `DatasetSpec` 原子重写；仅在工具无法跑时再把细粒度目录移到 `_quarantine/`。`trading_status` 历史派生须走 `partition_for`（月分区），勿再写日目录 |
| `mock_source` | 生产环境 mock 数据 | 关闭 `allow_mock`；清 mock 分区重采 |
| `adj_close_discontinuity` | 复权收益异常 | `cne derive adj_factors`；查 Sina 源 |
| `missing_corporate_action` | 除权日无 corp action | `cne backfill corporate_actions` |
| `trading_status_coverage_start` | ST 覆盖起点晚 | 研究窗口证据不足；按市场回补并核对 ST 收据，不能仅忽略 warning |
| `partition_row_count_mutation` | 行数突变 | 查是否误 compact 或源口径变化 |
| `unregistered_curated_dir` | `curated/` 下有未注册目录（如 `*.bak*`） | `mv curated/<stray> {data_root}/backups/`；勿删前确认非误移的活数据 |

Findings 文件：`meta/quality/findings/{run_id}.json`

因子与公司行为的矛盾检查按每只证券的因子覆盖区间比较：平坦因子仍检查是否漏掉已记录的事件，公司行为表为空也仍检查因子跳变。首个因子观测日缺少前值，不据此判定漏步；基金现金分红未引起因子跳变则单独计入口径提示。全量审计与局部修复检查采用相同规则，因此升级后新增的矛盾项可能是补出了原有漏报，应先核对具体证券和事件日期。

对手源公司行为也按证券的有效交易日匹配，精确日期回补提示仍使用对手源记载的原始除权日。快照中没有某个事件且采集日期早于该事件时，报告将其计入未仲裁（`peer_snapshot_uncovered`），不能据此判定事件不存在；快照明确包含的事件仍可确认。

财报对照按证券、报告期、报表类型和科目匹配，双方都按披露日期及观测时间选择最新版本。零值也是可比较的数值：双方均为零视为一致，零与非零视为差异。

## 症状：status --datasets STALE

1. 确认最近交易日是否跑过 pipeline
2. 查该数据集最近成功 run：`cne status`
3. 重跑对应 group：`cne run daily --group <name>`
4. 季频数据集（`northbound_holdings`）容忍 100 天 — 非故障

`is_stale()` 逻辑：`domain/datasets.py`

## 症状：init 中断

保留数据和配置，重跑相同的 `cne init`：它会查找未完成 run 并续跑，已成功的批次不会重拉。结果里的 `warning` / `degraded` 也一样处理，不需要拆成多条补数命令。只有要指定某个旧 run 时才用 `cne init --run-id INIT_RUN_ID`。

`--keep-going`：单 phase 失败后继续后续 phase（用于尽量多回填）。

## 症状：RunLockError

先用 `cne status` 和进程信息确认持锁者。活动的 `retry` / `compact` 需要等待或正常停止；进程退出后操作系统会释放锁，锁文件留在 `meta/locks/` 不表示仍持锁。不要删除锁文件绕过活跃写入。

调度脚本和操作页共用的是另一把锁，目录是 `{data.root}/locks`（`CNE_SCHEDULER_LOCK_DIR` 或 `CNE_LOCK_DIR` 可改到别处）。面板上的任务还在跑时，定时日更会等到下一轮再判断。

## 症状：浏览器打不开面板

| 现象 | 原因 | 处理 |
|------|------|------|
| `localhost:8787` 无法连接，`127.0.0.1:8787` 可以 | 只监听了 IPv4，macOS 会先把 `localhost` 解析到 `::1` | 使用当前版本的 `cne serve`：回环地址同时接受 `127.0.0.1` 和 `localhost`，启动信息只打印 localhost |
| 页面停在「正在打开面板」 | 首屏接口还没返回 | 等概览出现；若一直停着，看 serve 终端里的报错，并确认打开的是打印出来的那个地址 |

## 症状：磁盘不足 / staging 膨胀

1. `cne run compact`：自动找出并发布所有已结束、有 staging、还没有成功 compact 的 run
2. `cne run clean` 先列出候选。交互终端里会分两次询问：第一次删超出保留数的历史版本，第二次删已经合并且不在使用中的 staging。正在使用的一代和任何仍在使用的 staging 不会自动删除。来源快照、日志、orphan 和未完成的 run 仍只列出。回补和日更默认只留每个数据集最近 5 代（[`[storage.retention].keep_generations`](../getting-started/configuration.md#storage-retention)），不碰 staging，也不删当前正在读的一代。小磁盘把这个数改小；要全部留下则设为 `0`。最近 5 代里不是当前指针的版本，到 serve 存储运维页确认删除。`cne storage inspect` 的 `exclusive_bytes` 是删掉这一代实际能腾出的字节。磁盘不够写这次发布时，命令会直接说明还差多少，不会只退出 1。
3. incomplete / 未 compact 的失败 run 默认保留供 `cne run retry`；只有确认可丢弃时才 `--force`
4. 压缩或归档旧 `meta/source_snapshots/`（长期会膨胀）
5. curated 勿删；用 backfill 重采而非部分删除

## 症状：复权因子大量缺失

```bash
cne derive adj_factors
cne audit --full
```

`strict_adj=True` 时缺因子会报错 — 检查 `derived/adj_factors` 覆盖与 `adj_factors_cache`。

## 诊断命令速查

```bash
cne config validate
cne sources probe --only tdx_protocol
cne status
cne status --datasets
cne stats show --json
cne audit --full
cne run retry --run-id RUN_ID
cne run clean --dry-run
```

## 相关文档

- [运维 Runbook](runbook.md)
- [数据流](../architecture/data-flow.md)
- [初始化与续跑](../getting-started/initialization.md)
