# CLI 参考

命令：`cne`（`cnequity.cli.main:cli`）

全局默认：`--config configs/cnequity.toml`

---

## cne demo

一分钟试玩，提供两条路径：默认拉少量流动性股票的真源近期日线；`--sample` 在完全离线时生成明确标记为 `source=mock` 的合成小湖。两者都**不是**全市场 `cne init`。

| 选项 | 说明 |
|------|------|
| `--symbols` | 逗号分隔标的（默认茅台/平安银行/五粮液/宁德/中国平安） |
| `--days` | 约多少个交易日的 `daily_bars`（默认 30） |
| `--intraday` | 额外抓同一批标的的 1m 线（最多约 5 个交易日），打印一根完整会话 |
| `--research` | 额外从 Sina 派生 hfq 因子，并打印 raw / hfq 收益对照；会把窗口扩展到约 3 年 |
| `--sample` | 不访问网络，生成可用于验证安装、查询和 DuckDB 视图的合成样例；不可与 `--research` / `--intraday` 合用 |
| `--data-root` | 独立湖根目录（默认 `data/cnequity-demo`） |
| `--trade-date` | 截至日 YYYY-MM-DD（默认今天 / 最近交易日） |
| `--config-out` | 写出供后续 `cne query` 使用的小配置（默认 `configs/cnequity.demo.toml`） |

流程：建目录 → 探测 TDX → 拉 instruments 并裁成 demo 宇宙 → 交易日历 → `daily_bars` + compact → 打印样例表；加 `--research` 时再派生 Sina hfq 并校验 exact 覆盖，加 `--intraday` 时再跑 `minute_bars`。终端有分阶段进度与 INFO 日志。需要能访问 TDX；`allow_mock` 不会打开。

只想验证研究口径，不必初始化全市场：

```bash
cne demo --research --symbols 600519.SH
```

`--research` 需要额外访问 Sina；网络受限时先运行不带该选项的基础 demo。

完全无法访问 TDX 时，可先验证本地读写和查询链路：

```bash
cne demo --sample
```

合成行会醒目标记为 `source=mock`，质量审计不会把它们视为真实数据；请勿复用该 demo 的 `data_root` 做研究或生产。

---

## cne init

初始化数据湖并执行 init phases。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径 |
| `--layout-only` | 仅建目录、manifest、DuckDB 视图 |
| `--trade-date YYYY-MM-DD` | init 截至交易日（默认今天） |
| `--resume` | 续跑最近未完成 init |
| `--run-id` | 续跑指定 init run（隐含 resume） |
| `--keep-going` | phase 失败后继续后续 phase |
| `--profile full\|quick` | 回填多少历史。`quick` = 最近 3 年，`full`（默认）= 各 step 自己的起点（`daily_bars` 为 2016-01-01） |
| `--since YYYY-MM-DD` | 显式指定历史起点，覆盖 `--profile` |
| `--quiet` | 只留 warning 及以上，不打逐批进度 |

**默认会打进度。** 全市场回填是几十个批次、可能跑几小时；之前它一声不吭直到最后吐 JSON，和卡死没法区分——而看起来卡死的进程会被 kill 掉，白扔已经跑完的几小时。现在每个批次一行：

```
14:22:07 INFO ...worker_pool: daily_bars 12/54 batches · 1,043,882 rows · 18m04s elapsed · ~1h03m left
```

**`quick` 是更浅，不是更窄。** 全市场标的一个不少，只是每只少几年。按标的裁剪会把这个湖本来要修掉的幸存者偏差直接建进去，而且一个缺席的标的看起来和「这只票从没交易过」一模一样；少几年的历史则由 `coverage_start` 如实记录。

窗口会写进 run metadata，`--resume` 自动沿用——否则几天后从新进程续跑会默认回到全深度，去抓你当初特意跳过的年份。

之后加深不必重跑 init：

```bash
cne backfill daily_bars --start 2016-01-01 --end <你的 coverage_start>
```

退出：result `status != success` 时退出 1。

---

## cne config init

从包内模板写出用户配置（PyPI 安装后无需 clone 仓库）。

| 选项 | 说明 |
|------|------|
| `--config` | 输出路径（默认 `configs/cnequity.toml`） |
| `--data-root` | 写入 `[data].root` |
| `--force` | 覆盖已存在文件 |

macOS 上会把 `orchestrator.workers` 写成 `1`（与 `validate` 规则一致）。模板源：`cnequity.config.templates`（与仓库 `configs/cnequity.example.toml` 保持同步）。

---

## cne config validate

校验 TOML 与 step 引用。有错退出 1。

---

## cne contract

查看和维护 42 个注册数据集的机器可读 JSON 契约。

| 子命令 | 说明 |
|--------|------|
| `show [DATASET]` | 输出一个数据集或完整 registry 契约；`--out PATH` 写文件而非打印 |
| `validate [PATH]` | 校验文件；省略 PATH 时校验当前 registry。文件加 `--against-registry` 做精确同步检查 |
| `diff OLD [NEW]` | 比较两个契约；省略 NEW 时比较当前 registry。默认发现 breaking 时退出 1，检查报告可加 `--allow-breaking` |

diff 会把删列、改类型、改主键、单位/PIT/历史语义变化识别为 breaking；新增
列和新增数据集为 compatible。

> `cne contract export` 已并入 `cne contract show --out`——两者本来就是同一份文档，
> 只差写不写文件。

---

## cne profile

查看版本化的研究 universe 画像（`cnequity.domain.universe_profiles` 注册表）。

| 子命令 | 说明 |
|--------|------|
| `list` | 输出注册表记录；`--official-only` 排除 legacy 兼容画像 |
| `show NAME` | 输出单个画像及其 `scope_hash`；`--symbol` 可重复，绑定具体标的并附 `concrete_scope_hash` |

画像绑定交易所/板块、CDR/ETF、ST/停牌、退市与 PIT 证据规则。研究读取用
`load(..., profile="cn_a_sh_sz_research_v1")`，并把 `name` / `version` / `scope_hash`
一起记进产出。详见 [universe 画像](universe-profiles.md)。

---

## cne doctor

环境与配置体检：`data.root` 是否绝对路径 / 可写、声明的依赖能否 import。不访问网络；无配置也能跑（新鲜安装）。有实质性风险时退出 1。

| 选项 | 说明 |
|------|------|
| `--json` | 机器可读输出 |

`cne doctor --fix` 已移除（只服务于已卸掉的 mini-racer 冲突修复）。

---

## cne run daily

| 选项 | 说明 |
|------|------|
| `--group` | `core` \| `capital` \| `signals` \| `fundamentals` \| `macro_risk` \| `research` \| `intraday` |
| `--backfill` | 强制 backfill 语义（慎用） |
| `--stale-only` | 只重抓仍落后于最后交易日的数据集（与 `--group` 互斥） |
| `--quiet` | 只留 warning 及以上，不打逐步进度 |

### --stale-only：当天的第二次机会

`snapshot` 数据集只抓 run 当天。**一次源端中断吃掉那个窗口，那天就永久没了**——`valuation_metrics` 就这样丢了 2026-07-30 和 07-31：per-host 重试和退避本来就有，只是全部耗尽了，而 snapshot 语义决定了后面任何一次 run 都补不回来。

缺的不是重试，是**当天的第二个窗口**。挂在主 pipeline 几小时之后：

```cron
# 主 pipeline
5 16 * * 1-5 /path/to/cnequity/scripts/daily_pipeline.sh

# 收尾补抓：只跑仍然落后的，没有就空转
5 20 * * 1-5 cd /path/to/cnequity && cne run daily --stale-only
```

新鲜度判据与 `cne status --datasets` 完全一致（含每数据集的 `max_staleness_days` 容忍），所以两者不会各说各话。没有落后的数据集时不建 run、直接退出 0，可以安全挂在定时器上。

派生数据集不在其中：它们由 curated 重算，该跑的是 `cne derive`，不是重抓。

无 `--group` 时跑完整 `[job.daily.waves]` DAG。`intraday` 组不在默认调度里：需先开 `[minute_bars].enabled`，再 `cne run daily --group intraday`。

成功或 `skipped_non_trading_day` 退出 0。

---

## cne run events

7x24 事件流：公告、监管事件、资讯。**不看交易日历**（这些源周末和节假日照发），
并且拿自己的 `events_ingestion` 锁，不与晚间批处理抢 `daily_ingestion`。

| 选项 | 说明 |
|------|------|
| `--group` | `[job.events.groups]` 中的一个组；默认按配置顺序跑完所有组 |
| `--trade-date` | 自然日 `YYYY-MM-DD`（默认今天），周末/节假日同样有效 |
| `--quiet` | 只留 warning 及以上 |

```cron
# 每天（含周末）一次全量事件流
0 14 * * *  /path/to/cnequity/scripts/events_pipeline.sh

# 只要资讯的日内新鲜度：单张实时页，代价很低
*/30 9-22 * * *  CNE_EVENTS_GROUP=news_wire /path/to/cnequity/scripts/events_pipeline.sh
```

`disclosures` 组每次都会重读 30 天对账尾窗，高频跑它是在重复付这份代价；
`news_wire` 没有尾窗，适合高频。组按配置顺序执行，所以 `regulatory` 能读到
`disclosures` 刚发布的公告。

---

## cne backfill \<dataset\>

单数据集 backfill。snapshot 且无 `backfill_source` 时拒绝。

成功时自动 compact 当前 run。

| 选项 | 说明 |
|------|------|
| `--start` / `--end` | 窗口（日内数据集拒绝早于源端视野的 `--start`） |
| `--symbols` | 日内、`daily_bars`、`trading_status`、`corporate_actions` 的临时标的范围；其他数据集仍使用配置中的范围 |
| `--baostock-repair` | 仅 `corporate_actions`：显式补抓已退市 SH/SZ 标的的 Baostock 分红除权数据；建议与 `--symbols` 配合 |
| `--ths-repair` | 仅 `corporate_actions`：显式补抓已退市 BJ 标的的同花顺历史分红除权数据；建议与 `--symbols` 配合 |
| `--eastmoney-bj-repair` | 仅 `corporate_actions`：按北交所旧码→920 新码映射向 EastMoney 定向补抓历史分红除权数据；建议与 `--symbols` 配合 |
| `--bse-tip-repair` | 仅 `daily_bars`：读取已有 session 的 OHLCV，仅向 BSE 请求成交额并严格核对；必须同时指定相同的 `--start/--end` 与 `--symbols` |

```bash
cne backfill minute_bars_5m --start 2026-05-01 --end 2026-07-31 \
  --symbols 600519.SH,000001.SZ

# 已有 BJ 日线只补当前分区成交额，不重抓 Sina 历史
cne backfill daily_bars --start 2026-08-21 --end 2026-08-21 \
  --symbols 920000.BJ,920001.BJ --bse-tip-repair
```

### sector_bars

| 选项 | 说明 |
|------|------|
| `--retry-failed` | 跳过 checkpoint 中已完成的板块，只重试失败项 |
| `--force` | 清空 checkpoint 后全量重拉（与 `--retry-failed` 互斥） |

Checkpoint：`meta/state/sector_bars_backfill.json`。失败超过 50% 时 step 状态为 `warning` 但仍写入已成功部分。

**网络**：走同花顺 `d.10jqka.com.cn`（日更与历史同源），限速在 `[sources.ths]`。
与东财无关，`[sources.eastmoney] proxy` 对它不生效。

```bash
# 首次或换源后全量
cne backfill sector_bars --config configs/cnequity.toml --force

# 续跑失败板
cne backfill sector_bars --config configs/cnequity.toml --retry-failed
```

---

## cne compact

| 选项 | 说明 |
|------|------|
| `--run-id` | 指定 run（默认最近 run） |

将 staging 合并入 curated。

---

## cne delisted

读退市目录，并拉它点名的那些行情。**重建**目录（扫码空间、核对终点、修 instruments、
覆盖门禁）是一次性工程，在 [`scripts/delisted_ops.py`](../operations/scripts.md#delisted_opspy)。

| 子命令 | 说明 |
|--------|------|
| `status [--since]` | 目录摘要：数量、年份、尚未 ingest |
| `backfill [--since]` | 对目录中尚未有行情的退市股拉 Sina 历史并 compact |

推荐顺序（跨 CLI 和脚本）：

```bash
cne delisted status                                   # 已知多少
python scripts/delisted_ops.py discover --limit 500   # 扫码空间，可续跑
cne delisted backfill --since 2016-01-01              # 拉扫到的行情
python scripts/delisted_ops.py repair                 # bars 已在湖里时写 delist_date
python scripts/delisted_ops.py reconcile              # 先 dry-run
python scripts/delisted_ops.py reconcile --apply      # 仅在没有 active ingestion run 时
python scripts/delisted_ops.py coverage --start 2016-01-01 --universe all_a_sh_sz
```

`coverage` 的通过声明刻意很窄：它证明退市目录已扫完，且已知与窗口重叠的退市标的具备一致的末根有效成交和证券主数据；它不证明两端之间每个交易日都连续。数据源在停牌或正式摘牌前可能保留零成交占位行，门禁不会把它们误当成末次交易。目录末日晚于窗口、但窗口内又没有行情可证明已经上市的标的会进入 `unknown_overlap`，不会被静默排除。

`reconcile --apply` 不以单一供应商返回的“最后一条记录”为真相：必须有 curated
正成交量终点，且该终点不晚于 `instruments.delist_date`，同时旧目录日期还必须落在
正式退市日之后或非交易日，才允许自动修改。命令检测到任何 active ingestion run
都会拒绝执行；修改前的目录保存在 `meta/state/history/`，质量回执写入
`meta/quality/`，并记录修改前备份和修改后目录的 SHA-256。

---

## cne derive [name]

| name | 说明 |
|------|------|
| `adj_factors`（默认） | 计算 Sina hfq 因子 |
| `trading_status` | 派生历史停牌记录（`--start` / `--end` 按年分块重建） |
| `sector_routing` | 可选：EM 板块 × TDX 88xxxx 名称映射表（**不驱动** sector_bars 采集） |
| `sector_code_map` | BK* ↔ BOARD_CODE 身份映射（lake-only；推荐成分 join） |

```bash
cne derive trading_status --start 2001-01-01 --end 2001-12-31
```

---

## cne audit

| 选项 | 说明 |
|------|------|
| `--run-id` | 指定 run 的 findings（默认最近 run） |
| `--full` | 湖级健康快照（非 per-run 文件） |
| `--research-start YYYY-MM-DD` | 与 `--full` 合用；严格验证所选历史宇宙，未通过时退出 1 |
| `--research-end YYYY-MM-DD` | 研究窗口末日；默认取 `daily_bars` 最新分区 |
| `--research-universe all_a\|all_a_sh_sz` | 历史研究口径；默认 `all_a`，`all_a_sh_sz` 排除 BJ 的来源能力缺口 |

`--full` 且 UNHEALTHY 退出 1。显式传 `--research-start` 后，研究宇宙未通过也退出 1；此时末行会显示 `HEALTHY (operational; research BLOCKED)`，表示湖的运营健康与研究可用性是两个独立门禁。未显式传 `--research-start` 时，历史宇宙状态仍写入 health 与 `historical-validity-latest.json`，但不会改变运维健康的退出码。快照同时记录 `historical_universe`，避免把 scoped 结果误读成全 A。

---

## cne verify

| 选项 | 说明 |
|------|------|
| `--dataset` | 只查这些数据集（逗号分隔）；默认全部已注册数据集 |
| `--repair` | 对可修复的缺口跑回填，按数据集从新到旧 |
| `--kind` | 只看这些缺口类型：`empty,stale,interior,shallow` |

**和 `cne audit`问的不是同一件事。** `audit` 问「落下来的数据对不对」，`verify` 问
「该落的有没有落」——后者是一个 step 一碰就抛异常时产生的故障。没有它，一个数据集可以
连续数周每次 run 都失败，而每次 run 只记录一个 failed batch，湖级看不出来。

**缺口按「能不能补」分开，而不是按大小。** `by_date` 数据集缺一个交易日是故障；
`snapshot` 数据集缺一个交易日是它本来的形状，任何回填都不可能诚实地补上它
（补了就是伪造行）。`--repair` 只跑前者。

```bash
cne verify                                  # 全表体检
cne verify --dataset daily_bars,adj_factors
cne verify --kind interior --repair         # 只补内部空洞
```

---

## cne status

| 选项 | 说明 |
|------|------|
| `--datasets` | 逐数据集新鲜度表（dataset / layer / freshness / 覆盖区间 / watermark）；有 STALE 退出 1 |
| `--all-columns` | 配合 `--datasets`：打印 `list_datasets` 的全部列（契约指纹、revision、PIT 存储列等），而非仅新鲜度 |
| `--run <id\|latest>` | 指定 run（默认 `latest`）；摘要含每个数据集 stage 的 `dataset_results` 与聚合 `dataset_status` |
| `--run-id <id>` | `--run` 的显式 id 别名；两者不能同时给 |

无选项：输出最近 run 的 JSON 摘要。run 为 `degraded`（核心正常、研究/建议层降级）退出 2，
核心失败退出 1。

---

## cne retry

重试失败 batch / 补 init 缺失 step。init run 走 `resume_init`。

| 选项 | 说明 |
|------|------|
| `--run-id <id>` | 重试指定 run |
| `--failed-groups` | 逐个独立进程重试每个 `daily:*` 分组最新的失败 run；若该分组已有更新的成功 run，则跳过旧失败 |

两项必须且只能选择一项。

成功退出 0；`RunLockError` 报错退出。

---

## cne clean

删除已 compact 的终态 run staging，以及超龄 orphan。终态含 `success` / `warning` / `failed`（需 incomplete=0 且有成功 compact batch）。

| 选项 | 说明 |
|------|------|
| `--dry-run` | 仅报告可删 staging |
| `--orphan-retention-days` | 无 manifest 的 orphan 保留天数（默认 7） |
| `--force` | 也删尚未 cleanup-ready 的 staging（incomplete / 未 compact）；成功 fetch batch 会被 demote，`cne retry` 全量重抓。**不要**对 success-without-compact 用 force——先 `cne compact --run-id` |

---

## cne serve

只读湖面板：分层总览、逐数据集覆盖与新鲜度、溯源分布、覆盖热力图。

| 选项 | 默认 | 说明 |
|------|------|------|
| `--host` | `127.0.0.1` | 非回环地址**必须**配 `--token` |
| `--port` | `8787` | |
| `--token` | 无 | 要求 `Authorization: Bearer <token>` 或 `?token=` |

```bash
cne serve
```

页面在 `/`，单数据集在 `#/dataset/<name>`（状态 / 元数据 / 数据 三个 tab），跑批在 `#/runs`（含实时甘特），质量在 `#/quality`，OpenAPI 在 `/api/docs`（由 handler 生成，不会与实现漂移）。

**面板不写湖。** 没有端点会跑批、重试或清理——那些留给 CLI。唯一的例外是 `meta/stats` 会在后台按需重建，因为它是湖的缓存而不是湖的一部分。

数值全部来自已落盘的产物（注册表、目录布局、`meta/stats`、`meta/quality/health-latest.json`、manifest），**请求路径上不扫 curated**。所以：先 `cne stats rebuild` 才有行数与体积；findings 显示的是上次 `cne audit --full` 的快照，页面上标了日期。

端点与热力图语义见 [serve 模块](../modules/serve.md)。

---

## cne stats

湖的自我度量表，写到 `meta/stats/`。`list_datasets()` 只看目录名，答不了「这个分区有多少行、多大、谁写的」——那些在这里。

产物：

| 文件 | 粒度 | 列 |
|------|------|-----|
| `partition_stats.parquet` | dataset + partition | `granularity`、`period_start/end`、`row_count`、`file_count`、`bytes` |
| `provenance_stats.parquet` | dataset + partition + source + data_version | `row_count`、`fetched_at_min/max` |
| `stats-latest.json` | — | `generated_at`、`latest_run_id`、汇总数 |

两张表而不是一张：`bytes` / `file_count` 是目录的属性，`row_count` 按源拆分，把文件级数字挂到细粒度上会让它看起来可加，而加起来是重复计数。

不含 `tier` / `layer` / `history_mode`：那些在 `domain/datasets.py`，写进数据文件的副本只会过期。

用 parquet 而非 duckdb 文件：写入是「临时文件 + 原子 rename」，读端零阻塞；duckdb 文件要独占写锁，会让 `cne serve` 和夜间跑批互相挡路。

### cne stats rebuild

| 选项 | 说明 |
|------|------|
| `--dataset` | 只重建这些数据集（可重复）；**其余数据集保留原有行**，不会被删 |
| `--if-stale` | 只在「湖动过了」时才重建，否则空转返回。放定时器上用这个 |
| `--json` | 结果输出 JSON |

全量重建：参考湖（1.5GB / 6600 万行 / 21k 分区）约 6 秒——只读 `source`、`data_version`、`fetched_at` 三列。增量刷新是可行的（跑批动过的分区可以从 `ingestion_batches.window_start/window_end` 反推），但没到需要的规模。

**`--if-stale` 的判据是 run id，不是时钟。** 改变湖的是采集，所以建于最后一个 run 之后的表无论多旧都是当前的，建于之前的无论多新都是过期的——`stats-latest.json` 的 `latest_run_id` 和 manifest 的最新 run 比对即可，只读一个小 JSON 加一行 SQLite。

并发用非阻塞锁收敛：面板请求、cron、夜间跑批同时想重建时只有一个真做，抢不到锁的直接返回而不是排队——把 web 请求堵在一次全扫后面比多看一个 run 的旧数字更糟。

`--if-stale` 判的是全湖水位，所以不能和 `--dataset` 同用，命令会直接报错而不是二选一地猜。

刷新策略（`meta/stats` 不会自己刷新）：

```bash
# 兜底：定时器上跑，没变化就是空转
cne stats rebuild --if-stale
```

面板（M2）走 `stats_freshness()` 判过期 + 后台线程调 `refresh_stats_if_stale()`；线程策略留在调用方，模块本身是同步的。

> `cne stats refresh` 已并入 `cne stats rebuild --if-stale`；原 `--force` 就是不加 `--if-stale` 的默认行为。

### cne stats show

| 选项 | 说明 |
|------|------|
| `--dataset` | 单个数据集的逐分区明细 |
| `--by-source` | 改看 source / data_version 分布 |
| `--json` | 机器可读输出 |

**无 stats 表时直扫 curated 回退**（原 `cne catalog`）：只给 dataset / files / rows，没有字节数、
源分布和逐分区明细，但一个从没跑过 `stats rebuild` 的湖不该先做一次构建才能回答「里面有什么」。
`--dataset` / `--by-source` 是 stats 表独有的视图，回退时直接报错而不是降级回答另一个问题。

> `cne catalog` 已并入本命令的回退路径；`--json` 就是它原来的输出。

---

## cne query

使用 `--dataset X --symbol CODE` 时按需抓取并读取本地缓存；追加 `--refresh` 可强制重新抓取并覆盖对应的缓存变体。`--dataset`、`--symbol` 必须成对出现，否则命令会明确报错；未指定二者时才执行 DuckDB SQL 查询。

**DuckDB 模式**（默认）：

| 选项 | 默认 |
|------|------|
| `--sql` | `SELECT COUNT(*) AS n FROM daily_bars` |

**On-demand 模式**：

| 选项 | 说明 |
|------|------|
| `--dataset` | on-demand 数据集名 |
| `--symbol` | 如 `600519.SH` |

---

## cne mcp

把这个湖接给 AI agent（MCP over stdio）。只读，和 `cne serve` 同样的边界。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径，**建议绝对路径**（客户端从哪个目录拉起进程不确定） |
| `--live` | 湖里没有的，现拉现给、不落盘。只支持 `resolve_symbol` 与未复权日线，其余工具明确拒绝 |

不用手敲：由 MCP 客户端拉起并在管道上讲 JSON-RPC。三条路按手上有什么选：

```bash
cne demo                                   # 没湖想先试试：30 秒真数据
cne mcp --config /abs/path/cnequity.toml
cne mcp --config /abs/path/cnequity.toml --live
```

上面的 `cne mcp ...` 是标准 MCP stdio server 命令，Claude 只是其中一种
客户端。Codex、Cline、Cursor、Windsurf、Gemini CLI 或其它兼容客户端，均
使用相同的 `command` / `args`；客户端的注册入口不同，但不需要改 server。

`--live` **默认关，永不自动推断**：湖坏了的用户必须拿到 `no parquet data` 去修，而不是悄悄拿到一份来自别处、看起来差不多的答案。每次调用最多 50 个标的 / 800 天，且必须显式给 `symbols`。每条响应带 `origin: "lake" | "live"`。

6 个工具（`describe_lake` / `resolve_symbol` / `query_bars` / `query_fundamentals` / `query_dataset` / `run_sql`）、口径随响应返回、`run_sql` 只收单条 SELECT：见 [MCP 参考](mcp.md)。

---

## cne sources

数据源这一面的全部：`probe` 实时探测，其余三个从已存证据算派生结论、**不联网**。

| 子命令 | 说明 |
|--------|------|
| `probe` | 探测公开数据源，报告写进湖里 |
| `slo` | 把 `meta/source_health` 历史样本按 probe/vantage 聚成可用性 SLO，并写去重事故载荷。`--window-days`（默认 30）、`--minimum-observations`（默认 10）、`--enforce`（关键源不达标退出 1） |
| `resilience` | 从注册表算源集中度、failure-domain 爆炸半径和核心数据集独立备源门禁。`--out PATH` 落 JSON，`--enforce`（有核心表缺独立备源则退出 1） |
| `policy [SOURCE]` | 查 `sources/SOURCES.yml` 的来源使用策略。省略 SOURCE 输出全部；给 SOURCE 加 `--profile personal\|commercial\|cache\|redistribution` 做保守判断，未知权限一律 fail-closed（退出 1） |

> 原为 `cne sources`（探测）+ `cne source <sub>`（派生结论）——两个顶层条目差一个字母，
> 且 `cne source --help` 不得不用一句话把自己和邻居区分开。现在收敛成一个名词。

### cne sources probe

探测本湖依赖的公开数据源。每个源发**一个**请求，断言响应体（不是状态行），串行且尊重各源限速。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径（探测不读湖，但要用里面的限速与超时） |
| `--vantage` | 这次探测从哪个出口发出：`cn` / `overseas` / 任意标签（默认 `local`） |
| `--only` | 逗号分隔的 probe key，默认全部；传空串则一个都不测 |
| `--out` | JSON 报告路径。默认写进湖里的 `meta/source_health/<vantage>.json`，也就是 `cne serve` 读的位置 |

```bash
cne sources probe --vantage cn
cne serve                    # → http://127.0.0.1:8787/source-health
```

**探测在 CLI，展示在 serve。** 面板只读，不会替你去请求十几个第三方主机——和它不触发采集是同一个理由。多次探测（不同 `--vantage`）会并排显示，不合并。

**`--vantage` 要认真填。** 好几个源在 WAF 层拒绝非大陆出口，同一主机同一秒可以大陆绿、海外红，两个都是真的。没有这个标签的报告无法解读。

**源挂了不影响退出码。** 源变红是这条命令的输出而不是它的失败。

状态五档：`ok` 可用 · `empty` 空响应 · `blocked` 被拒 · `down` 不可达 · `skipped` 未探测。`empty` 单独一档是因为它看起来比失败健康、实际更危险（回填静默截断）。

口径、加新源的方法见 [数据源健康度](../operations/source-health.md)。

---

## cne snapshot

把选定数据集复制成不可变、带校验和的可移植快照，用于冻结可复现实验依赖的 Parquet。

| 子命令 | 说明 |
|--------|------|
| `create NAME --dataset D [--dataset D ...]` | 建快照；manifest 固化每个 Parquet 的大小/SHA-256、数据集 state、契约指纹和运行 lineage |
| `verify NAME` | 逐文件校验大小与哈希；不通过退出 1 |
| `restore NAME TARGET` | 恢复到新目录或空目录（拒绝活动湖根、不覆盖已有文件）。恢复后对 TARGET 跑 `cne status --datasets` 再切换 |
| `export NAME [DEST]` | 打成一个可移植 tar 归档。`--compression auto` 有 zstd 就用 `tar.zst`，否则退到 `tar.gz`；先写同目录 `.part`，压缩器正常收尾后才原子改名 |
| `import ARCHIVE` | 先校验后落地：逐个 tar member 拒绝绝对路径、`..`、重名、软硬链接和设备节点，解出的目录先按 manifest 全量校验，再原子改目录名发布。`--name` 覆盖快照名（默认取归档文件名），`--overwrite` 只在校验通过后才替换同名快照 |

`--config` / `--snapshot-root` 各子命令通用；默认根为 `meta/snapshots`。

### 增量包 `cne snapshot delta`

整湖快照适合冻结实验依赖；**日常同步一个已有的湖用增量包**——只搬动变化的文件，
且带足够的前置条件让"应用到错误的基线上"变成一次失败而不是一次静默污染。

| 子命令 | 说明 |
|--------|------|
| `delta create NAME --from A --to B` | 把两个**数据根**（不是 `curated` 根）逐字节比对成不可变的 add/replace/delete 包。`--to` 默认当前配置的活动湖；`--dataset` 可重复，省略时取两根共有的数据集 |
| `delta create NAME --from-revision N` | 以已提交的 revision 号作前置条件。revision receipt 记的是变更文件、不是旧湖副本，所以这一模式发的是带 `allow_missing` 的 `replace`；需要严格的旧文件哈希前置就用上面的双根模式 |
| `delta verify NAME` | 校验每个 add/replace 载荷哈希与每条变更路径的语义 |
| `delta apply NAME TARGET` | 应用到**非空**的 TARGET 湖根。`--dry-run` 只验前置条件不落盘 |

`delta-create` 是 `delta create` 的兼容别名。

apply 的安全边界值得单独说：add/replace/delete 逐条对基线指纹（revision 增量则对
各数据集 revision）核验，写入走同目录临时文件，每个被覆盖的文件在整包变更与
应用后的目标指纹都通过之前一直留着备份；中途抛异常会把已做的逐条回滚——调用方
不会观察到一个已知的半成品状态。可写路径也被收窄到 `curated/`、`derived/` 和
`meta/` 下的白名单，增量包无法借此在目标根里写任意文件。

---

## cne stability

从权威 `trading_calendar` 取最近窗口，按 logical trade date 选最新 `daily:core` attempt，验证连续交易日运行证据。

| 选项 | 说明 |
|------|------|
| `--days` | 需连续通过的交易日数（默认 20） |
| `--as-of` | 含当日的 `YYYY-MM-DD` 截止 |
| `--enforce` | 门禁未过则退出 1 |

缺 run、核心 stage 失败、或只有旧 `warning` 且无 dataset receipt 都算失败。报告写
`meta/stability/latest.json` 与不可变历史目录。日更脚本每天跑但不 `--enforce`；
release 治理在第 20 天 enforce。

---

## cne --version

包版本号。

---

## 退出码汇总

| 码 | 场景 |
|----|------|
| 0 | 成功、非交易日跳过、健康检查通过 |
| 1 | 核心运行失败、UNHEALTHY、STALE、校验失败、门禁未过 |
| 2 | run 可用但降级（`degraded`）——核心 spine 正常，研究/建议层有失败（`run daily` / `run daily --stale-only` / `init` / `retry` / `status`） |

---

## 相关文档

- [快速开始](../getting-started/quickstart.md)
- [cli 模块](../modules/cli.md)
