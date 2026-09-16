# 配置参考

配置文件格式：TOML。模板随包装在 `cnequity.config.templates`；仓库内副本为 `configs/cnequity.example.toml`。

```bash
cne config create                              # 推荐：写出 configs/cnequity.toml
cne config create --data-root /data/cnequity
cne config validate --config configs/cnequity.toml
```

加载与校验：`cnequity.config.loader`。

---

## `[data]`

| 键 | 类型 | 默认 | 说明 |
|----|------|------|------|
| `root` | string | `./data/cnequity` | 数据湖根目录；**生产建议绝对路径** |

派生路径（代码内自动计算，无需配置）：

- `{root}/staging` — 本次 run 原始落地
- `{root}/curated` — canonical 数据集
- `{root}/derived` — 派生数据集（如 adj_factors）
- `{root}/meta` — manifest、水位、质量 findings
- `{root}/duckdb/cnequity.duckdb` — DuckDB 视图库

---

## `[orchestrator]`

| 键 | 默认 | 说明 |
|----|------|------|
| `workers` | 8 | `daily_bars` 多进程 worker 数 |
| `batch_size` | 100 | 每 batch 股票数量 |
| `max_retries` | 3 | batch 级重试次数 |
| `retry_backoff_seconds` | 5 | 重试退避 |
| `batch_stale_seconds` | 3600 | running batch 无心跳超时 → stale → failed；compact 门禁会跳过未完成数据集。**崩溃的 run 不受这个窗口约束**：run 全程持锁，进程一死 60 秒内即被回收 |

---

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

---

## `[sources.<name>]`

模板里的 `name`（14 个）：

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
| `tushare` | 可选 Tushare Pro——BJ 历史 ST 证据（`stock_st`）。需 token，**优先用环境变量 `TUSHARE_TOKEN`**，别把凭证写进配置 |


| 键 | 说明 |
|----|------|
| `enabled` | 是否启用该源；缺省（配置中没有该 `[sources.<name>]` 段落）时按**关闭**处理 |
| `min_interval_seconds` | 跨进程文件槽位限速（见 `domain/rate_limit.py`）；锁内只预订时隙，等待发生在释放锁后 |
| `proxy`（eastmoney） | 可选 HTTP(S) 代理 URL，对所有东财主机生效；**大陆网络不需要**，海外出口才配。未设时仍可用环境变量 `HTTPS_PROXY` |
| `batch_size` / `batch_rest_seconds`（baostock） | 全市场回填批次冷却，防 IP 黑名单 |
| `verify`（ths_official） | 默认 **开**。只允许写 `meta/source_snapshots` 与 findings，从不碰 curated 行，所以有 key 就可以安全开着 |
| `backfill`（ths_official） | 默认 **关**。它会改变湖里的内容，所以必须显式打开。持有凭证、启用源、允许它改数据是三个决定 |
| `api_key`（ths_official） | 建议用环境变量 `HITHINK_FINANCE_API_KEY` 而非写进配置 |

推荐默认（时间宁可慢，勿被封）：

| source | `min_interval_seconds` | 备注 |
|--------|------------------------|------|
| eastmoney | 1.0 | 日更主源；裸 `EastMoneyClient()` 也默认 1.0s 进程内节流 |
| cninfo | 1.0 | 公告/监管分页 POST |
| pboc | 1.0 | 社融月度序列，索引一次 + 每年一个工作簿 |
| nbs | 1.0 | 仅 audit：PMI 发布稿对照，每次两个请求 |
| exchange | 1.0 | 仅 audit：交易所上市列表，每所一个请求 |
| sina | 0.3 | 复权因子；经 `adj_factors` 的 `wait_source` |
| sina_bars | 1.0 | BJ/退市日线 fallback；独立于复权因子限速，配合 HTTP 456 有限重试 |
| baostock | 1.0 + batch 20/120s | 历史市值/ST；禁止多进程并行扫 |

---

## `[adj_factors]`

| 键 | 默认 | 说明 |
|----|------|------|
| `source` | `"sina"` | 复权因子来源 |
| `adjust_types` | `["hfq"]` | 仅存后复权因子（ADR-0004）；qfq 查询期派生 |

---

## `[sentiment]`

| 键 | 默认 | 说明 |
|----|------|------|
| `use_snownlp` | false | on-demand `stock_news` 可选 SnowNLP（包已随安装提供）；日更 batch 用关键词 |
| `news_symbol_limit` | 50 | HTTP `stock_news` 回退抓取 symbol 上限（主通道为 curated `news_headlines`） |

---

## `[failover]`

多源快照与 diff；不会自动切换 canonical（ADR-0003）。

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

---

## `[universe]`

| 键 | 默认 | 说明 |
|----|------|------|
| `default` | `"all_a"` | `load(..., universe=)` 默认 universe 类型 |
| `ingest` | `"all_a"` | 日更抓取覆盖的标的类别 |

`ingest` 只约束**取数范围**，与研究选股口径（`domain/universe_profiles.py`）无关：

| 值 | 含义 |
|----|------|
| `all_a` | 沪/深/北 A 股（默认） |
| `all_a_sh_sz` | 再排除北交所 |
| `all_instruments` | `instruments` 列出的全部代码，含 ETF/LOF 行情代码 |

ST、停牌、CDR 和已退市的名字在任何取值下都会保留 —— 丢掉它们正是这个湖要避免的幸存者偏差。

`instruments` 会返回 TDX 列出的全部代码，其中约四分之一是 ETF/LOF/基金行情代码：
没有任何研究口径会选中它们，也没有哪个已配置的源能稳定提供它们。把它们放进日更
会占掉四分之一的抓取量，用没人需要的代码触发东财和新浪的熔断，并把填不上的
`symbol×session` 键留给覆盖门禁 —— 门禁随后拒绝为当天落盘。

---

## `[job.daily.waves]`

Wave DAG：每个 wave 含 `name`、`parallel`（wave 内 step 是否并行）、`steps`（step 名列表）。

默认四波：

1. `reference` — instruments, trading_calendar, trading_status（并行）
2. `corp_actions_to_bars` — corporate_actions → daily_bars（串行）
3. `parallel_core` — index_bars
4. `finalize` — compact, derive_adj_factors, audit

`validate_config` 要求至少一个 wave，且所有 step 名必须在 `STEP_REGISTRY` 中。

---

## 调度组

`[job.daily.groups.<name>]`：`at`（文档/调度参考时间）、`steps`（含末尾 `compact`）。

| 组名 | 典型时间 | 实测耗时 | 内容摘要 |
|------|----------|----------|----------|
| `core` | 16:00 | **~50 min** | L0 + L1 核心 + derive_adj_factors |
| `capital` | 17:00 | 10.3 min | 资金面 + 估值 + 板块 |
| `signals` | 17:20 | 5 s | 龙虎榜、大宗交易 |
| `fundamentals` | 17:35 | 2.6 min | 财报、指数成分、行业 |
| `macro_risk` | 17:55 | 2.4 min | 宏观、市场宽度、解禁 |
| `research` | 18:15 | 11.4 min | 机构持仓、一致预期、情绪 |
| `intraday` | 18:45 | — | `minute_bars` / `minute_bars_5m`（**不在默认调度**；需先开 `[minute_bars]`） |

「实测耗时」测于 2026-08:macOS(因此 `workers=1`)+ 海外出口,即最慢的一端。
大陆 Linux + `workers=8` 会快一个数量级,这个间隔会显得很宽松——**这是刻意的**。

> **间隔必须容得下最慢的一次运行,不是典型的一次。** 所有 `daily*` 任务共用一把
> **非阻塞**的 `daily_ingestion` 锁:上一组还没跑完时,下一组不会排队,而是直接
> 中止——那一组当天就没有数据。`core` 的全市场 `daily_bars` 实测 543ms/只、
> ~5400 只约 50 分钟,曾经超出到 `capital` 的 30 分钟间隔,导致资金面组每天被跳过。
> 撞锁时报错会明确说明是被跳过,以及去哪里调间隔。

`cne run daily --group <name>` 只跑该组 steps。

---

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
`--group <name>` 只跑一个。定时器见
[`scripts/events_pipeline.sh`](../operations/scripts.md) 与 `com.cnequity.events` agent。

`validate_config` 在这里守两条：组里只能放**自然日**数据集
（`DatasetSpec.session_scope = "calendar"`），且同一个 step 不能同时出现在 `[job.daily]`
和 `[job.events]`——两个任务持不同的锁，同时抓同一个数据集就是并发写同一份 staging。

`sentiment_scores` 仍留在 `research` 组：它读的是**湖里已提交的**公告和资讯，
从来不是同一次 run 里现抓的，所以拆开之后行为不变。

---

## `[minute_bars]`

可选日内线。默认关闭，且**不在** `[job.daily.waves]` 上——全市场 1m 约 35MB/日、8.4GB/年，不能变成没人要时 `cne init` 的成本。开启后用 `cne run daily --group intraday` 或 `cne backfill`。

| 键 | 默认 | 说明 |
|----|------|------|
| `enabled` | `false` | 总开关 |
| `scope` | `"index:000300.SH"` | `index:<symbol>` / `watchlist` / `all` |
| `symbols` | `[]` | `scope = "watchlist"` 时的显式列表 |
| `frequencies` | `["1m"]` | `"1m"` → `minute_bars`；`"5m"` → `minute_bars_5m` |
| `fetch_workers` | `4` | 并发 TDX 连接数（不提高请求速率，只消网络空转；上限仍约 10 req/s） |

**源端视野**（实测 2026-08-01）：1m ≈ 95 个交易日，5m ≈ 491 个交易日。更早窗口返回空；`cne backfill … --start` 早于视野会直接拒绝。磁盘与耗时见 [runbook — 日内数据](../operations/runbook.md#日内数据minute_bars--minute_bars_5m)。

---

## `[trade_ticks]`

分笔成交记录。**自成一段**，不是 `[minute_bars]` 里的一个开关——两者的量级差一个数量级，开启分钟线不该悄悄把它一起带上。

| 键 | 默认 | 说明 |
|----|------|------|
| `enabled` | `false` | 总开关 |
| `scope` | `"watchlist"` | `index:<symbol>` / `watchlist` / `all` |
| `symbols` | `[]` | `scope = "watchlist"` 时的显式列表 |
| `max_symbols` | `200` | 单次抓取的标的数上限 |
| `fetch_workers` | `4` | 并发 TDX 连接数（实测 40 标的 × 5 会话：1 个 4.50 req/s，4 个 10.13 req/s，已到限速器天花板） |

**这不是逐笔成交。** A 股 Level-1 是 3 秒快照，一行聚合的是那个时间片里落下的全部真实成交（实测平均 6–33 笔）。时间戳只到分钟——协议从来没带过秒——所以行用 `tick_seq`（会话内位置）标识。`direction` 是 TDX 自己按 tick rule 猜的主动方，不是交易所字段。

**历史视野**：TDX 对每个标的都回溯到 2024-01-02，是**固定底**而非滚动窗口，与分钟线的每标的 bar 数上限无关。`cne backfill trade_ticks --start` 早于此会直接拒绝。

用 `cne run daily --group ticks` 或 `cne backfill trade_ticks` 采集。

---

## `[quality]`

| 键 | 默认 | 说明 |
|----|------|------|
| `audit_gate` | `"shadow"` | 湖审计报 `error` 时这次 run 怎么办 |

三档：

- `off` — 什么都不记，永不失败（0.8.2 之前的行为）
- `shadow` — 把"本该被拦下"的记下来，但让 run 成功
- `block` — 让 run 失败，`cne status` 与 pipeline 退出码都能看见

audit step 依赖 `compact`，所以它在**行已经进 curated 之后**才跑：`block` 是让 run 失败，不是阻止写入。shadow 模式每个受影响的 run 往 `meta/quality/audit_gate.jsonl` 追加一行——切到 `block` 之前应该先读它。一个不知道会多频繁触发就打开的门禁，很快会被关掉。

只有 `error` 触发门禁；发 `warning` 的那 32 个检查从不触发。见 [ADR-0012](../adr/0012-the-audit-gates-in-shadow-first.md)。

---

## `[incremental]`

| 键 | 默认 | 说明 |
|----|------|------|
| `negative_evidence_ttl_days` | `7` | 「源端此处为空」这类否定证据的有效期；设 `0` 则每次都重试缺失键。标的目录发生 revision 时，仍然会让在有效期内的证据失效 |
| `deep_reconciliation_dow` | `6` | 每周做一次深度对账的星期（0=周一）。目前只有 `announcement_index` 声明它 |

为什么需要它：CNINFO 把分页固定在 30 行且忽略 `pageSize`，它的 30 天窗口每次扫描约 1,350 个请求——全 pipeline 单项开销最大的一处——而绝大多数是在重读湖里已有的记录。实测对照源端：3 天、7 天、14 天前的日期返回的内容与已存完全一致，而 21 天前的日期在 5,932 行里多出 21 行。深尾是真的存在，所以改成每周扫一次而不是直接砍掉：一条晚索引的公告现在会在一周内落地，而不是一天内。它自己的 `announce_date` 两种做法都不变，所以 PIT 正确性不依赖它何时到达——滞后的只是湖的完整度。

---

## `[raw_archive]`

| 键 | 默认 | 说明 |
|----|------|------|
| `enabled` | `true` | 是否把源端原始响应压缩存到 `meta/raw` |
| `compression` | `"gzip"` | 压缩方式 |
| `max_payload_bytes` | `33554432` | 单个响应的存档上限（32MB） |

**请求凭证、代理设置、Cookie 和 authorization 头一律不存档。**

---

## `[exchange_audit]`

| 键 | 默认 | 说明 |
|----|------|------|
| `price_tolerance_bps` | `10` | 收盘价偏差容忍（基点） |
| `turnover_tolerance_bps` | `100` | 成交额偏差容忍（基点） |
| `turnover_max_fraction` | `0.15` | 触发 finding 所需的全域占比 |

把 `daily_bars` 与上交所、深交所自己发布的收盘价对照——**全湖唯一一个能触达发布方而非第二个转售方的价格检查**。受 `[sources.exchange]` 控制；findings 是建议性的，永不让 run 失败。

上交所只提供当前正在发布的那个会话，所以 SH 是当日仲裁；深交所可查任意历史日期。

成交额的容忍度**故意放宽**：交易所的日总额包含了连续竞价 bar 不含的交易，curated 合理地会略低。实测 2026-08-28，5,212 个共有标的中 305 个（5.9%）存在偏差，全部是 SZ，且方向一致。finding 按**全域占比**触发，所以差距扩大会被抓到，而那个长期存在的定义性差异保持安静。

---

## `[margin_trading]`

| 键 | 默认 | 说明 |
|----|------|------|
| `source` | `"exchange"` | 融资融券明细的来源 |

`"exchange"` 直接读上交所与深交所的融资融券明细，它们由会员单位报送汇总——中间没有转售方。

---

## `[job.init.phases]`

| 键 | 说明 |
|----|------|
| `names` | init 阶段顺序列表 |

默认：

```toml
names = [
  "phase1_reference",
  "phase2a_corporate_actions",
  "phase2c_daily_bars_backfill",
  "phase3_index_and_status",
  "phase4_finalize",
  "phase5_derive_and_publish",
]
```

阶段 → step 映射见 `orchestrator/init_phases.py`。

---

## `[on_demand]`

| 键 | 说明 |
|----|------|
| `enabled` | OnDemandService 开关 |
| `datasets` | 按需抓取的数据集名列表。默认仅 `stock_news`、`research_reports`；`announcement_body` / `financial_reports` 尚未实现 |

缓存路径：默认请求为 `meta/on_demand/{dataset}/{symbol}.json`；带有会改变结果的参数时，会使用同目录下带请求摘要的变体文件，避免不同日期、条数或情感模型查询互相复用。通过 `cne query --dataset X --symbol Y` 访问；需要强制更新时追加 `--refresh`。失败或未实现的结果不会写入缓存。

---

## `[duckdb]`

| 键 | 默认 | 说明 |
|----|------|------|
| `path` | `{data.root}/duckdb/cnequity.duckdb` | 支持 `{data.root}` 占位符 |
| `memory_limit` | `2GB` | DuckDB 内存上限 |
| `threads` | 4 | 查询线程数 |

---

## 环境变量（仅 `scripts/*.sh`）

下列变量由 [运维脚本](../operations/scripts.md) 读取；除 `CNE_LOG_DIR` 外 **`cne` CLI 不读**（配置路径仍用 `--config` 或默认 `configs/cnequity.toml`）。

| 变量 | 默认 | 作用 |
|------|------|------|
| `CNE_CONFIG` | `configs/cnequity.toml` | 脚本传入 `cne --config` 的路径 |
| `CNE_LOG_DIR` | `{data.root}/logs` | 日志目录。`cne init` / `cne backfill` / `cne run` 也读它，并把本次运行的日志写成 `cne-<命令>-<时间戳>.log`，启动时打印路径 |
| `CNE_GROUPS` | 全部调度组（不含需显式开启的 `intraday`） | 覆盖 pipeline 要跑的组 |
| `CNE_NOTIFY` | `1` | `0` 关闭 macOS 通知 |
| `CNE_BACKUP_DIR` | 湖内 backups | 元数据备份目录 |
| `CNE_BACKUP_RETENTION_DAYS` | 14 | 备份保留天数 |

---

## 配置与代码关系

```
cnequity.toml
    → load_config() → Config dataclass
    → validate_config() → 引用 step/group 合法性
    → JobEngine(cfg) / load(..., config=cfg)
```

`Config` 还提供：`staging_root`、`curated_root`、`derived_root`、`meta_root`、`manifest_path`、`rate_limit(source)`。
