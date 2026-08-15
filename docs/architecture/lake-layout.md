# 数据湖布局

数据湖根目录：`{data.root}`（配置项 `[data].root`）。

---

## 顶层结构

```
{data_root}/
├── staging/          # 本次 run 原始落地（compact 后可清理）
├── curated/          # 下游只读的 canonical 数据
├── derived/          # 可重算的派生数据
├── raw/              # 可选：原始 HTTP 响应留存
├── meta/             # 编排元数据、水位、质量、缓存
├── duckdb/           # DuckDB 视图数据库
├── logs/             # 运维脚本日志（gitignored）
└── backups/          # 元数据 tarball + 从 curated 挪出的手术残留（*.bak* 等）
```

`backups/` 不是 curated 的一部分。手工改湖时把旧数据集目录挪到这里（例如 `corporate_actions.bak.<ts>`），**不要**留在 `curated/` 旁——整层 `curated/**` 扫描会读到陈旧副本；audit 的 `unregistered_curated_dir` 会报警。

---

## staging/

```
staging/{dataset}/run_id={run_id}/part-{batch_id}.parquet
```

- 每次 run 独立 `run_id`（UUID）
- worker step 每 batch 一个 part 文件
- 非 worker step 通常单 part
- **不保证** PK 唯一；去重在 compact 阶段完成

清理：`cml clean`（终态 + 已 compact 的 run；`--force` 可清 incomplete/未 compact，retry 将全量重抓）

---

## curated/

```
curated/{dataset}/{partition_col}={value}/part-merged.parquet
```

**核心契约**：

- 每个主键（PK）**恰好一行** canonical 记录
- 每行必含溯源列：`source`、`data_version`、`fetched_at`（UTC）
- 多源差异**不**在 curated 内共存；备源见 `meta/source_snapshots/`

**特殊：instruments**

- 无 Hive 分区，单文件 merge 语义
- compact 时**合并**而非覆盖，保留已退市 symbol（防幸存者偏差）

**分区键**：见 [数据集目录](../datasets/catalog.md)。常见：

- `trade_date` — 日线类
- `ex_date` — 除权除息
- `report_period` — 财报
- `as_of_date` — 快照类成员关系
- `announce_date` — 公告（PIT）

### 分区粒度

分区值的**周期**按数据集配置，不是一律按天：

| 目录 | 粒度 | 例子 |
|------|------|------|
| `trade_date=2024-06-03` | `day` | `daily_bars`（约 4900 行/天） |
| `trade_date=2024-06` | `month` | `trading_status`、`sector_bars`（50–1000 行/天） |
| `trade_date=2024` | `year` | `trading_calendar`、`index_bars`（< 50 行/天） |

原因是 Parquet 的 footer / 列元数据大约固定 1KB 一个文件，与内容多少无关。
一天一行的数据集按天分区，会用 4220 个文件、16MB 存下本来 50KB 的东西，
而且每次扫描都要打开 4220 个 footer。粒度在 `DatasetSpec.partition_granularity`
里配，大致按行/天分档：≥1000 → `day`，50–1000 → `month`，<50 → `year`。
audit 的 `partition_fragmentation` 会在某个数据集明显配细了时告警。

**目录值是自描述的**：`2024` / `2024-06` / `2024-06-03` 三种形状读的时候直接按
形状解析，不看配置。所以改粒度不需要迁移——老的按天目录照常能读，只是比新写入的碎。
`cml repartition <dataset>` 把历史改写成配置的粒度（只是省空间和文件句柄，不影响正确性）；
不带参数则列出当前布局和配置不一致的数据集。

粗粒度目录关闭 Hive 解析：polars 会按同名列的类型去解析目录值，
`trade_date=2024` 撞上 Date 列会直接报 `could not find a 'date/datetime' pattern for '2024'`。
真实日期列本来就在文件里，所以只是改成按目录周期做裁剪，语义不变。

---

## derived/

```
derived/adj_factors/trade_date=YYYY-MM-DD/part-*.parquet
```

当前派生数据集：

| 数据集 | 来源 | 说明 |
|--------|------|------|
| `adj_factors` | Sina hfq | 查询期与 daily_bars 组合复权 |
| `market_breadth` | daily_bars 计算 | 也可写在 curated（当前在 curated 注册） |
| `sentiment_scores` | 公告 + 新闻 | 写在 curated |

`adj_factors` 另有缓存：`meta/adj_factors_cache/{symbol}.parquet`

---

## meta/

```
meta/
├── manifest.db                    # SQLite：ingestion_runs, ingestion_batches
├── state/
│   └── {dataset}.json             # 增量水位（last_success_date 等）
├── source_snapshots/
│   └── {dataset}/source={src}/data_version={ver}/...
├── quality/
│   ├── findings/{run_id}.json
│   └── source_diffs/{run_id}.json
├── adj_factors_cache/
├── on_demand/
│   └── {dataset}/{symbol}.json
└── locks/                         # run_lock 文件锁
```

### manifest.db

表：

- `ingestion_runs` — job_name, status, started_at, finished_at
- `ingestion_batches` — dataset, batch_id, status, symbol_range, error_message

Batch 状态机：`pending` → `running` → `success` | `failed` | `stale`

### state/{dataset}.json

compact 成功后更新。用于：

- 增量抓取窗口（`incremental_window`）
- 下游缓存失效键
- `cml status --datasets` 新鲜度判断

---

## duckdb/

```
duckdb/cn-market-lake.duckdb
```

- 启动时 / `cml query` 前由 `query/views.py` 确保视图存在
- 每个 curated/derived 数据集对应视图
- 额外视图：`daily_bars_hfq`、`daily_bars_qfq`、`daily_bars_adj`（带复权列）

路径配置：`[duckdb].path`，支持 `{data.root}` 占位符。

---

## 读写权限约定

| 路径 | 写入方 | 读取方 |
|------|--------|--------|
| staging | steps（采集） | compact |
| curated | compact | load() / DuckDB / 外部 Polars |
| derived | derive steps | load() |
| meta/* | orchestrator, quality, derive | CLI, audit, load（水位） |

**下游禁止写入 curated/derived**，保证可重放与审计。

---

## 相关文档

- [ADR-0002：Parquet 湖](../adr/0002-parquet-lake-over-database.md)
- [ADR-0003：Canonical + Snapshots](../adr/0003-canonical-curated-with-source-snapshots.md)
- [storage 模块](../modules/storage.md)
