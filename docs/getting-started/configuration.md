# 配置参考

配置文件格式：TOML。模板随包装在 `cnequity.config.templates`；仓库内副本为 `configs/cnequity.example.toml`。

```bash
cne config init                              # 推荐：写出 configs/cnequity.toml
cne config init --data-root /data/cnequity
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
| `batch_stale_seconds` | 3600 | running batch 无心跳超时 → stale → failed；compact 门禁会跳过未完成数据集 |

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

支持的 name：`eastmoney`、`cninfo`、`pboc`、`sina`、`baostock`、`nbs`、`exchange`。

| 键 | 说明 |
|----|------|
| `enabled` | 是否启用该源；缺省（配置中没有该 `[sources.<name>]` 段落）时按**关闭**处理 |
| `min_interval_seconds` | 跨进程文件槽位限速（见 `domain/rate_limit.py`）；锁内只预订时隙，等待发生在释放锁后 |
| `proxy`（eastmoney） | 可选 HTTP(S) 代理 URL，对所有东财主机生效；**大陆网络不需要**，海外出口才配。未设时仍可用环境变量 `HTTPS_PROXY` |
| `batch_size` / `batch_rest_seconds`（baostock） | 全市场回填批次冷却，防 IP 黑名单 |

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

下列变量由 [运维脚本](../operations/scripts.md) 读取；**`cne` CLI 不读**（配置路径仍用 `--config` 或默认 `configs/cnequity.toml`）。

| 变量 | 默认 | 作用 |
|------|------|------|
| `CNE_CONFIG` | `configs/cnequity.toml` | 脚本传入 `cne --config` 的路径 |
| `CNE_LOG_DIR` | `{data.root}/logs` | 日志目录 |
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
