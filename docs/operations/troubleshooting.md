# 故障排查

按症状分类的处置指南。原则：**先定位 run_id 与失败 batch，再 retry，最后 audit 复核**。

---

## 症状：EastMoney datacenter `code=9501` / `列不存在`

| 现象 | 原因 | 处理 |
|------|------|------|
| `EastMoney datacenter RPT_… rejected schema: XXX列不存在 (code=9501)` | 东财改了报表列名；旧列整报拒绝 | 在对应 adapter 的 `_COLUMNS` 换成新名；契约清单自动跟随 |
| 日更整组失败、错误带 report 名 | 同上（fail-loud，不静默空表） | 修列后重跑；用直播探针确认 |

```bash
# 离线：契约清单完整 + 9501 文案
uv run pytest tests/unit/test_datacenter_contracts.py -q
# 外网：每个 required 报表 pageSize=1，键 ⊇ 契约列
uv run pytest -m network tests/unit/test_datacenter_live_contracts.py -q
```

清单入口：`src/cn_market_lake/adapters/eastmoney/datacenter_contracts.py`。
已退役报表（如 `RPT_ECONOMICCALENDAR`）标 `required=False`，不进直播探针。

---

## 症状：baostock / 免费源「黑名单」或频繁失败

| 现象 | 原因 | 处理 |
|------|------|------|
| `baostock login failed: 黑名单用户`（`10001011`） | IP 被免费 API 封禁：日请求 >5 万、或并发连接、或扫太快 | **停扫**；换出口或去 QQ 群求助解封。解封后用 `[sources.baostock]` 默认限速 resume，**勿并发** |
| 东财 429 / Empty reply / 连接被掐 | 请求过密或海外出口 | 保持 `min_interval_seconds ≥ 1.0`；大陆出口或 `proxy`；见下文 sector_bars |
| cninfo / pboc 间歇失败 | 同源风控 / 站点抖动 | 已按页/按调用 `rate_limit`；社融按年取全量，单年失败仅告警且不影响其他年，下次运行补回 |

原则：**时间可以等，封禁成本远高于多等一天。** 勿为加速关掉 `min_interval` 或开多进程打同一免费源。

估值历史回填已对 `baostock` 做 **单飞锁**（`RunLock("baostock")`）：并发
`valuation_2001` / float_mv 扫盘会直接跳过并留下 `baostock_single_flight` warning。
rate-limit alone 不能阻止 N 个会话同时 `login()`。

---

## 症状：valuation_metrics 水位「新鲜」但覆盖率断崖 / STALE

| 现象 | 原因 | 处理 |
|------|------|------|
| 最近几天只有几百只、`valuation_bars_low_coverage` | 日更 EastMoney `capital` 未跑通，baostock 历史曾以 `end=today` 只写完部分标的 → 稀疏 tip；旧逻辑把 watermark 推到 max partition | **已修**：baostock `end` 封顶在最近「完整」东财 tip（覆盖 ≥70% 当日 bars）；watermark 拒绝推进到稀疏 tip |
| `cml status --datasets` valuation STALE + 覆盖 ~20% | 同上 | 对缺口日补东财快照（PK `keep=last` 覆盖稀疏 baostock）： |

```bash
# 例：2026-07-17 … 最近交易日（按实际缺口改）
for d in 2026-07-17 2026-07-18 2026-07-21 2026-07-22 2026-07-23 2026-07-24 2026-07-25; do
  uv run cml run daily --group capital --trade-date "$d"
done
uv run cml status --datasets   # valuation 不应再停在稀疏 tip
```

审计 finding `valuation_watermark_coverage_gate`：水位曾越过完整日，已被 compact/reconcile 拉回。

---

## 症状：manifest 里大量 status=running 的僵尸 run

| 现象 | 原因 | 处理 |
|------|------|------|
| `cml status` 显示 `orphaned_running_runs > 0` | 进程被杀 / OOM，旧代码未在 `finally` 里 `finish_run`；status 从不自动 reconcile | **已修**：每次 `cml run` / `cml retry` 入口心跳感知 reconcile；retry 全绿也会 `finish_run` |
| 需要立刻清理 | — | `cml clean --reconcile-runs`（跳过仍持锁的 live run） |

长任务（baostock 回填）靠 **batch heartbeat** 保活，不会仅因 `started_at` 超过 1h 被误杀。

### BrokenProcessPool / worker 池被毒死

| 现象 | 原因 | 处理 |
|------|------|------|
| 日志 `worker pool broke (likely OOM under load); retrying … serially` | `ProcessPoolExecutor` 一子进程死后整池毒化 | **已修**：未完成 batch 串行重试；若子进程已 `finish_batch(success)` 则**跳过重拉**（避免 INSERT OR REPLACE 降级成功行） |
| macOS 上频繁 OOM / 池崩溃 | TDX 客户端非 fork-safe + `workers>1` | `cml config validate` **拒绝** Darwin 上 `workers>1`；生产用 `workers = 1`（见 runbook / `daily_pipeline.sh`） |
| Windows 上 `import fcntl` / 文件锁失败 | 旧版用 Unix-only `fcntl.flock` | 升级到含 `cn_market_lake.file_lock` 的版本；锁在 Win/POSIX 上语义一致 |
| Windows 上 DuckDB 视图空 / 路径错 | 反斜杠进了 `read_parquet` SQL | 新版本用 `as_posix()`；确认 `data.root` 可读写后重跑 `cml init` / 刷新视图 |
| Windows 上 PowerShell `&&` 语法错误 | PS 5.1 不支持 `&&` | 分行执行，或用 PowerShell 7+ / cmd |

---

## 症状：load() 读不到新数据

| 可能原因 | 检查 | 处理 |
|----------|------|------|
| 数据仍在 staging | `ls staging/*/run_id=*` | `cml compact --run-id <id>` 或 `cml retry`；success 但无 compact batch 时**先 compact 再** `cml clean`（勿 `--force`，否则 demote 后只能重抓） |
| 分组 run 未 compact | 组 steps 是否含 `compact` | 配置修正后重跑组 |
| compact 被 gate 跳过 | `cml status` 看 failed batch | `cml retry --run-id <id>` |
| 路径错误 | `config.data_root` | 核对 `configs/cn-market-lake.toml` |

---

## 症状：cml run daily 失败

1. `cml status` 查看 `run_summary` 与 failed batches
2. 查看日志 `error_message`（TDX 断连、HTTP 429、schema 校验失败等）
3. `cml retry --run-id <id>`
4. 若 TDX 问题：`cml servers test`；换 `[tdx_protocol.hosts].standard`
5. 若单数据集持续失败：`cml backfill <dataset>`（需支持 backfill）

### daily_bars：TDX 批次失败但 tip 仍有数据

- **现象**：日志 `daily_bars_clist_gapfill` / `routed … through EastMoney clist`；部分行 `source=eastmoney`。
- **原因**：TDX 主源部分/全部失败时，tip 日对**缺失 key**走东财 push2 clist（~54 页，分钟级），不是 per-symbol kline（小时级）。这是 ADR-0005 **routing**，不是静默换主源。
- **处理**：可接受则继续；要纯 TDX tip 时修好 TDX 后对当日 `cml run daily --group core --trade-date …` 重跑（compact `keep=last` 会用更新的主源行覆盖同 PK）。
- **多日回填**失败仍走 kline gap-fill（慢）；clist **不能**伪造历史。

### 周末 / 漏跑后水位落后

- **现象**：今天非交易日时 `cml run daily` → `skipped_non_trading_day`；`daily_bars`
  水位停在上上个交易日；下游 freshness 门禁不过。
- **处理**（补 core + `market_breadth`）：

  ```bash
  uv run cml run catchup                      # 默认：最近一个交易日
  uv run cml run catchup --trade-date 2026-07-17
  # 国内出口再补 capital/research（东财失败不挡门禁 exit 0）：
  uv run cml run catchup --trade-date 2026-07-17 --all-groups
  # 或分步：
  uv run cml run daily --group core --trade-date 2026-07-17
  ```

  全组补跑：`scripts/daily_pipeline.sh 2026-07-17`（或 `CML_TRADE_DATE=...`）。
  **不要**对漏跑日随便加 `--backfill`：东财 CA 全量扫描在海外常直接失败。
  **海外机器**：catchup（TDX core + 本地 derive breadth）通常够用；
  `fund_flow` / `hot_rank` / `sector_bars` 日更落后属预期，等国内出口再 `--all-groups`。

### 东财 502 / 连接被重置（海外出口）

- **现象**：`cml sources` 里 `eastmoney_push2` 报 `HTTP 502`、`eastmoney_push2his`
  报 `Connection closed abruptly`；日更 core 正常（行情走 TDX），资金面 / 板块组失败。
- **原因**：东财对非大陆出口做风控。**大陆网络下这一整类问题不存在**，无需任何配置。
- **处理**：给 `[sources.eastmoney]` 配一个有大陆出口的 HTTP(S) 代理——Clash、
  `ssh -D` 转 HTTP、或一台国内 VPS 都行：

  ```toml
  [sources.eastmoney]
  proxy = "http://127.0.0.1:7897"
  ```

  代理对**所有**东财主机生效（push2 / push2his / datacenter / reportapi）。
  改完用 `cml sources --only eastmoney_push2,eastmoney_push2his` 验证。
  不想改配置也可以走 `HTTPS_PROXY` / `HTTP_PROXY` 环境变量。

  **代理挂了不会重试**：`ProxyError` 归入 fail-fast，一次失败即放弃该请求，
  不会拿着退避把整批时间耗光。先确认代理本身通。

### sector_bars backfill 大量失败

- **现象**：日志 `THS sweep: BKxxxx (板块名) failed: ...`；`failed_sectors` 数量偏高。
- **原因**：**这是同花顺的源,不是东财**——`[sources.eastmoney] proxy` 对它不生效。
  `d.10jqka.com.cn` 对密集请求会限速甚至封禁。
- **处理**：提高 `[sources.ths] min_interval_seconds`（默认 1.0，实测这个速率下
  ~1300 次连续请求零失败），再 `cml backfill sector_bars --retry-failed`；
  全量换源后用 `--force`。Checkpoint：`meta/state/sector_bars_backfill.json`。
  失败率超过 50% 时 step 状态为 `warning`，已成功的板块仍会写入。

### 海外机器 + 国内阿里云 VPS（推荐跑在 VPS 上）

东财 HTTPS 可用本机 `proxy` / `HTTPS_PROXY`；**baostock ST 回填是自有 TCP，普通 HTTP 代理无效**。
最稳做法：把引擎（或至少 `data.root`）同步到阿里云，在 VPS 上跑一键脚本：

```bash
# 本机 → VPS（示例）
rsync -avz --progress ~/code/cn-market-lake/ user@VPS:~/cn-market-lake/

# VPS 上
cd ~/cn-market-lake && uv sync
./scripts/china_egress_backfill.sh          # sector_bars --force + trading_status ST
# ./scripts/china_egress_backfill.sh --sector-only
# ./scripts/china_egress_backfill.sh --st-only   # ST 可断点续跑

# 跑完把湖同步回本机
rsync -avz --progress user@VPS:~/cn-market-lake/data/cn-market-lake/ \
  ~/code/cn-market-lake/data/cn-market-lake/
```

安全组只开 **你自己的 IP → 22**；不要把代理端口对公网开放。
本机隧道备选：`ssh -D 7890`（仅东财）或 `sshuttle` / `proxychains`（才可能带上 baostock）。

---

## 症状：audit --full UNHEALTHY

| Finding 类型 | 含义 | 处理 |
|--------------|------|------|
| `pk_unique` | curated PK 重复（当前分区抽样） | 查最近 compact；必要时 backfill 重跑该分区 |
| `mixed_partition_granularity` | 盘上仍有细粒度分区叠在年/月分区上，同一 PK 跨粒度重复 | 优先 `cml repartition <dataset>`（或 `--all`）按 `DatasetSpec` 原子重写；仅在工具无法跑时再把细粒度目录移到 `_quarantine/`。`trading_status` 历史派生须走 `partition_for`（月分区），勿再写日目录 |
| `mock_source` | 生产环境 mock 数据 | 关闭 `allow_mock`；清 mock 分区重采 |
| `adj_close_discontinuity` | 复权收益异常 | `cml derive adj_factors`；查 Sina 源 |
| `missing_corporate_action` | 除权日无 corp action | `cml backfill corporate_actions` |
| `trading_status_coverage_start` | ST 覆盖起点晚 | 预期警告；跑 baostock ST 回填 |
| `partition_row_count_mutation` | 行数突变 | 查是否误 compact 或源口径变化 |
| `unregistered_curated_dir` | `curated/` 下有未注册目录（如 `*.bak*`） | `mv curated/<stray> {data_root}/backups/`；勿删前确认非误移的活数据 |

Findings 文件：`meta/quality/findings/{run_id}.json`

---

## 症状：status --datasets STALE

1. 确认最近交易日是否跑过 pipeline
2. 查该数据集最近成功 run：`cml status`
3. 重跑对应 group：`cml run daily --group <name>`
4. 季频数据集（`northbound_holdings`）容忍 100 天 — 非故障

`is_stale()` 逻辑：`domain/datasets.py`

---

## 症状：init 中断

**禁止**直接重新 `cml init`（会拒绝或产生冲突）。

```bash
cml init --resume
# 或
cml retry --run-id <init_run_id>
```

`--keep-going`：单 phase 失败后继续后续 phase（用于尽量多回填）。

---

## 症状：RunLockError

另一 `retry`/`compact` 正在持有锁。等待或删除陈旧锁：

```
meta/locks/
```

仅确认无活跃 cml 进程后手动清理。

---

## 症状：磁盘不足 / staging 膨胀

1. 找出 stranded success（有 staging、incomplete=0、无 compact batch）→ 逐个 `cml compact --run-id <id>`
2. `cml clean --dry-run` → `cml clean`（终态 + 已 compact 即可删，含 failed/warning）
3. incomplete / 未 compact 的失败 run 默认保留供 `cml retry`；只有确认可丢弃时才 `--force`
4. 压缩或归档旧 `meta/source_snapshots/`（长期会膨胀）
5. curated 勿删；用 backfill 重采而非部分删除

---

## 症状：复权因子大量缺失

```bash
cml derive adj_factors
cml audit --full
```

`strict_adj=True` 时缺因子会报错 — 检查 `derived/adj_factors` 覆盖与 `adj_factors_cache`。

---

## 诊断命令速查

```bash
cml config validate
cml servers test
cml status
cml status --datasets
cml catalog
cml audit --full
cml retry --run-id <id>
cml clean --dry-run
```

---

## 相关文档

- [运维 Runbook](runbook.md)
- [数据流](../architecture/data-flow.md)
- [运维 Runbook](runbook.md)
