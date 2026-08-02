# CLI 参考

命令：`asl`（`ashare_lake.cli.main:cli`）

全局默认：`--config configs/ashare-lake.toml`

---

## asl demo

一分钟真源小样（涨星 / 上手用）。拉少量流动性股票的近期日线，**不是**全市场 `asl init`。

| 选项 | 说明 |
|------|------|
| `--symbols` | 逗号分隔标的（默认茅台/平安银行/五粮液/宁德/中国平安） |
| `--days` | 约多少个交易日的 `daily_bars`（默认 30） |
| `--intraday` | 额外抓同一批标的的 1m 线（最多约 5 个交易日），打印一根完整会话 |
| `--data-root` | 独立湖根目录（默认 `data/ashare-lake-demo`） |
| `--trade-date` | 截至日 YYYY-MM-DD（默认今天 / 最近交易日） |
| `--config-out` | 写出供后续 `asl query` 使用的小配置（默认 `configs/ashare-lake.demo.toml`） |

流程：建目录 → 探测 TDX → 拉 instruments 并裁成 demo 宇宙 → 交易日历 → `daily_bars` + compact → 打印样例表；加 `--intraday` 时再跑 `minute_bars`。终端有分阶段进度与 INFO 日志。需要能访问 TDX；`allow_mock` 不会打开。

---

## asl init

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
asl backfill daily_bars --start 2016-01-01 --end <你的 coverage_start>
```

退出：result `status != success` 时退出 1。

---

## asl config init

从包内模板写出用户配置（PyPI 安装后无需 clone 仓库）。

| 选项 | 说明 |
|------|------|
| `--config` | 输出路径（默认 `configs/ashare-lake.toml`） |
| `--data-root` | 写入 `[data].root` |
| `--force` | 覆盖已存在文件 |

macOS 上会把 `orchestrator.workers` 写成 `1`（与 `validate` 规则一致）。模板源：`ashare_lake.config.templates`（与仓库 `configs/ashare-lake.example.toml` 保持同步）。

---

## asl config validate

校验 TOML 与 step 引用。有错退出 1。

---

## asl doctor

环境与配置体检：`data.root` 是否绝对路径 / 可写、声明的依赖能否 import。不访问网络；无配置也能跑（新鲜安装）。有实质性风险时退出 1。

| 选项 | 说明 |
|------|------|
| `--json` | 机器可读输出 |

`asl doctor --fix` 已移除（只服务于已卸掉的 mini-racer 冲突修复）。

---

## asl run daily

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
5 16 * * 1-5 /path/to/ashare-lake/scripts/daily_pipeline.sh

# 收尾补抓：只跑仍然落后的，没有就空转
5 20 * * 1-5 cd /path/to/ashare-lake && asl run daily --stale-only
```

新鲜度判据与 `asl status --datasets` 完全一致（含每数据集的 `max_staleness_days` 容忍），所以两者不会各说各话。没有落后的数据集时不建 run、直接退出 0，可以安全挂在定时器上。

派生数据集不在其中：它们由 curated 重算，该跑的是 `asl derive`，不是重抓。

无 `--group` 时跑完整 `[job.daily.waves]` DAG。`intraday` 组不在默认调度里：需先开 `[minute_bars].enabled`，再 `asl run daily --group intraday`。

成功或 `skipped_non_trading_day` 退出 0。

---

## asl backfill \<dataset\>

单数据集 backfill。snapshot 且无 `backfill_source` 时拒绝。

成功时自动 compact 当前 run。

| 选项 | 说明 |
|------|------|
| `--start` / `--end` | 窗口（日内数据集拒绝早于源端视野的 `--start`） |
| `--symbols` | 仅日内数据集：临时覆盖 `[minute_bars].scope`，并隐式开启抓取 |

```bash
asl backfill minute_bars_5m --start 2026-05-01 --end 2026-07-31 \
  --symbols 600519.SH,000001.SZ
```

### sector_bars

| 选项 | 说明 |
|------|------|
| `--retry-failed` | 跳过 checkpoint 中已完成的板块，只重试失败项 |
| `--force` | 清空 checkpoint 后全量重拉（与 `--retry-failed` 互斥） |

Checkpoint：`meta/state/sector_bars_backfill.json`。失败超过 50% 时 step 状态为 `warning` 但仍写入已成功部分。

**网络**：历史 kline 走 `push2his.eastmoney.com`，需国内或大陆出口代理；日更 clist 在海外通常可用。

```bash
# 首次或换源后全量（建议在国内机器）
asl backfill sector_bars --config configs/ashare-lake.toml --force

# 续跑失败板
asl backfill sector_bars --config configs/ashare-lake.toml --retry-failed
```

---

## asl compact

| 选项 | 说明 |
|------|------|
| `--run-id` | 指定 run（默认最近 run） |

将 staging 合并入 curated。

---

## asl delisted

重建退市宇宙（幸存者偏差修复）。

| 子命令 | 说明 |
|--------|------|
| `discover [--limit N]` | 扫 issued code space，分类为曾上市 / 从未发行（可续跑） |
| `status [--since]` | 目录摘要：数量、年份、尚未 ingest |
| `repair [--since]` | **不重新拉行情**：用已有 `daily_bars` 跨度写 `instruments.delist_date`，并清掉 `认购款` 占位 |
| `backfill [--since]` | 对目录中尚未有行情的退市股拉 Sina 历史并 compact |

推荐顺序：`discover` → `repair`（bars 已在湖里时）→ `backfill`（补缺口）。

```bash
asl delisted status
asl delisted repair
asl delisted backfill --since 2016-01-01
asl delisted discover --limit 500   # 扩大 band 后的续扫
```

---

## asl repartition [dataset]

| 选项 | 说明 |
|------|------|
| `--all` | 改写所有布局与配置不一致的数据集 |
| `--dry-run` | 只报告效果，不落盘 |

把历史分区改写成 `DatasetSpec.partition_granularity` 配的周期
（见 [分区粒度](../architecture/lake-layout.md#分区粒度)）。不带参数则列出待改写的数据集。

读路径按目录形状自解析，改粒度本身**不需要**迁移；这条命令只是把碎文件收回来。
写入是先建临时目录、逐分区写完并核对总行数，再一次 rename 换上去，中途挂掉不动原数据；
重复执行是幂等的。

```bash
asl repartition --all --dry-run   # 先看影响
asl repartition trading_calendar  # 单个数据集
```

---

## asl derive [name]

| name | 说明 |
|------|------|
| `adj_factors`（默认） | 计算 Sina hfq 因子 |
| `trading_status` | 派生历史停牌记录（`--start` / `--end` 按年分块重建） |
| `sector_routing` | 可选：EM 板块 × TDX 88xxxx 名称映射表（**不驱动** sector_bars 采集） |
| `sector_code_map` | BK* ↔ BOARD_CODE 身份映射（lake-only；推荐成分 join） |

```bash
asl derive trading_status --start 2001-01-01 --end 2001-12-31
```

---

## asl audit

| 选项 | 说明 |
|------|------|
| `--run-id` | 指定 run 的 findings（默认最近 run） |
| `--full` | 湖级健康快照（非 per-run 文件） |

`--full` 且 UNHEALTHY 退出 1。

---

## asl status

| 选项 | 说明 |
|------|------|
| `--datasets` | 逐数据集新鲜度表；有 STALE 退出 1 |

无选项：输出最近 run 的 JSON 摘要。

---

## asl retry --run-id \<id\>

重试失败 batch / 补 init 缺失 step。init run 走 `resume_init`。

成功退出 0；`RunLockError` 报错退出。

---

## asl clean

删除已 compact 的终态 run staging，以及超龄 orphan。终态含 `success` / `warning` / `failed`（需 incomplete=0 且有成功 compact batch）。

| 选项 | 说明 |
|------|------|
| `--dry-run` | 仅报告可删 staging |
| `--orphan-retention-days` | 无 manifest 的 orphan 保留天数（默认 7） |
| `--force` | 也删尚未 cleanup-ready 的 staging（incomplete / 未 compact）；成功 fetch batch 会被 demote，`asl retry` 全量重抓。**不要**对 success-without-compact 用 force——先 `asl compact --run-id` |

---

## asl catalog

JSON 列出 curated 各数据集文件数与行数。每次都全扫；固定的度量走 `asl stats`。

---

## asl serve

只读湖面板：分层总览、逐数据集覆盖与新鲜度、溯源分布、覆盖热力图。

| 选项 | 默认 | 说明 |
|------|------|------|
| `--host` | `127.0.0.1` | 非回环地址**必须**配 `--token` |
| `--port` | `8787` | |
| `--token` | 无 | 要求 `Authorization: Bearer <token>` 或 `?token=` |

```bash
asl serve
```

页面在 `/`，单数据集在 `#/dataset/<name>`（状态 / 元数据 / 数据 三个 tab），跑批在 `#/runs`（含实时甘特），质量在 `#/quality`，OpenAPI 在 `/api/docs`（由 handler 生成，不会与实现漂移）。

**面板不写湖。** 没有端点会跑批、重试或清理——那些留给 CLI。唯一的例外是 `meta/stats` 会在后台按需重建，因为它是湖的缓存而不是湖的一部分。

数值全部来自已落盘的产物（注册表、目录布局、`meta/stats`、`meta/quality/health-latest.json`、manifest），**请求路径上不扫 curated**。所以：先 `asl stats rebuild` 才有行数与体积；findings 显示的是上次 `asl audit --full` 的快照，页面上标了日期。

端点与热力图语义见 [serve 模块](../modules/serve.md)。

---

## asl stats

湖的自我度量表，写到 `meta/stats/`。`list_datasets()` 只看目录名，答不了「这个分区有多少行、多大、谁写的」——那些在这里。

产物：

| 文件 | 粒度 | 列 |
|------|------|-----|
| `partition_stats.parquet` | dataset + partition | `granularity`、`period_start/end`、`row_count`、`file_count`、`bytes` |
| `provenance_stats.parquet` | dataset + partition + source + data_version | `row_count`、`fetched_at_min/max` |
| `stats-latest.json` | — | `generated_at`、`latest_run_id`、汇总数 |

两张表而不是一张：`bytes` / `file_count` 是目录的属性，`row_count` 按源拆分，把文件级数字挂到细粒度上会让它看起来可加，而加起来是重复计数。

不含 `tier` / `layer` / `history_mode`：那些在 `domain/datasets.py`，写进数据文件的副本只会过期。

用 parquet 而非 duckdb 文件：写入是「临时文件 + 原子 rename」，读端零阻塞；duckdb 文件要独占写锁，会让 `asl serve` 和夜间跑批互相挡路。

### asl stats rebuild

| 选项 | 说明 |
|------|------|
| `--dataset` | 只重建这些数据集（可重复）；**其余数据集保留原有行**，不会被删 |
| `--json` | 结果输出 JSON |

全量重建：参考湖（1.5GB / 6600 万行 / 21k 分区）约 6 秒——只读 `source`、`data_version`、`fetched_at` 三列。增量刷新是可行的（跑批动过的分区可以从 `ingestion_batches.window_start/window_end` 反推），但没到需要的规模。

### asl stats refresh

只在「湖动过了」时才重建，否则空转返回。

| 选项 | 说明 |
|------|------|
| `--force` | 即使是最新的也重建 |

**判据是 run id，不是时钟。** 改变湖的是采集，所以建于最后一个 run 之后的表无论多旧都是当前的，建于之前的无论多新都是过期的——`stats-latest.json` 的 `latest_run_id` 和 manifest 的最新 run 比对即可，只读一个小 JSON 加一行 SQLite。

并发用非阻塞锁收敛：面板请求、cron、夜间跑批同时想重建时只有一个真做，抢不到锁的直接返回而不是排队——把 web 请求堵在一次全扫后面比多看一个 run 的旧数字更糟。

刷新策略（`meta/stats` 不会自己刷新）：

```bash
# 兜底：定时器上跑，没变化就是空转
asl stats refresh
```

面板（M2）走 `stats_freshness()` 判过期 + 后台线程调 `refresh_stats_if_stale()`；线程策略留在调用方，模块本身是同步的。`asl run daily && asl stats rebuild` 也可以，但 `refresh` 更省。

### asl stats show

| 选项 | 说明 |
|------|------|
| `--dataset` | 单个数据集的逐分区明细 |
| `--by-source` | 改看 source / data_version 分布 |

无 stats 时报错并提示先 `asl stats rebuild`。

---

## asl query

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

## asl mcp

把这个湖接给 AI agent（MCP over stdio）。只读，和 `asl serve` 同样的边界。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径，**建议绝对路径**（客户端从哪个目录拉起进程不确定） |

不用手敲：由 MCP 客户端拉起并在管道上讲 JSON-RPC。注册一次即可：

```bash
claude mcp add ashare-lake -- asl mcp --config /abs/path/to/ashare-lake.toml
```

6 个工具（`describe_lake` / `resolve_symbol` / `query_bars` / `query_fundamentals` / `query_dataset` / `run_sql`）、口径随响应返回、`run_sql` 只收单条 SELECT：见 [MCP 参考](mcp.md)。

---

## asl sources

探测本湖依赖的公开数据源。每个源发**一个**请求，断言响应体（不是状态行），串行且尊重各源限速。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径（探测不读湖，但要用里面的限速与超时） |
| `--vantage` | 这次探测从哪个出口发出：`cn` / `overseas` / 任意标签（默认 `local`） |
| `--only` | 逗号分隔的 probe key，默认全部；传空串则一个都不测 |
| `--out` | JSON 报告路径。默认写进湖里的 `meta/source_health/<vantage>.json`，也就是 `asl serve` 读的位置 |

```bash
asl sources --vantage cn
asl serve                    # → http://127.0.0.1:8787/source-health
```

**探测在 CLI，展示在 serve。** 面板只读，不会替你去请求十几个第三方主机——和它不触发采集是同一个理由。多次探测（不同 `--vantage`）会并排显示，不合并。

**`--vantage` 要认真填。** 好几个源在 WAF 层拒绝非大陆出口，同一主机同一秒可以大陆绿、海外红，两个都是真的。没有这个标签的报告无法解读。

**源挂了不影响退出码。** 源变红是这条命令的输出而不是它的失败。

状态五档：`ok` 可用 · `empty` 空响应 · `blocked` 被拒 · `down` 不可达 · `skipped` 未探测。`empty` 单独一档是因为它看起来比失败健康、实际更危险（回填静默截断）。

口径、加新源的方法见 [数据源健康度](../operations/source-health.md)。

---

## asl servers test

测试 TDX 连接（并行探测主机池，返回首个能出数的服务器）。

---

## asl --version

包版本号。

---

## 退出码汇总

| 码 | 场景 |
|----|------|
| 0 | 成功、非交易日跳过、健康检查通过 |
| 1 | 运行失败、UNHEALTHY、STALE、校验失败 |

---

## 相关文档

- [快速开始](../getting-started/quickstart.md)
- [cli 模块](../modules/cli.md)
