# 数据流

本文描述数据从外部源到下游可读的完整路径，以及失败与恢复行为。

---

## Init（全量回填）


触发：`cml init`

```
init_data_layout()
  ├── 创建目录树
  ├── manifest.db（空库 + WAL）
  └── DuckDB 视图初始化

run_init_phases() — 按 [job.init.phases] 顺序
  ├── phase1_reference
  │     instruments, trading_calendar（backfill 至 2016+）
  ├── phase2a_corporate_actions
  │     corporate_actions（backfill，TDX xdxr）
  ├── phase2c_daily_bars_backfill
  │     daily_bars（分页回填 2016+，symbol-batch 并行）
  ├── phase3_index_and_status
  │     index_bars（backfill）, trading_status（当日快照）
  └── phase4_finalize
        compact → derive_adj_factors → audit
```

**阶段语义**（`orchestrator/init_phases.py`）：

- `INIT_BACKFILL_PHASES` 内的阶段对 step 设置 `backfill=True`
- `trading_status` 在 init 中不做历史回填（仅当日）
- 存在未完成 init run 时，禁止新开全量 init（须 `--resume` 或 `retry`）

---

## 日更（Daily Run）

触发：`cml run daily` 或 `--group <name>`

```
检查是否交易日
  └── 否 → skipped_non_trading_day（退出 0）

start_run("daily" | "daily:<group>")
  └── 对每个 wave / step level：
        ├── 非 worker step：adapter 拉取 → validate_dataframe → staging
        ├── daily_bars：ProcessPoolExecutor × workers
        │     每 batch：manifest 记录 → 拉取 → staging
        └── 主备切换：主源失败时写 source_snapshots（不进 curated）

finalize（波次末尾或分组内显式 steps）：
  compact
    ├── compact_gate：本 run 有 incomplete batch 的数据集跳过
    ├── 主键去重：sort(fetched_at).unique(pk, keep="last")
    ├── instruments：特殊 merge，保留退市股
    └── 成功 compact 的数据集 → 更新 meta/state 水位

  derive_adj_factors
    └── Sina hfq 因子 → derived/adj_factors（带 per-symbol 缓存）

  audit
    ├── dataset_checks（PK、mock、空集、行数突变）
    ├── cross_checks（bars×calendar、adj 对账等）
    └── source_diff（主备 snapshot 比对）
```

---

## Compact 详解

**输入**：`staging/{dataset}/run_id={run_id}/part-*.parquet`

**输出**：`curated/{dataset}/{partition}={value}/part-merged.parquet`

**规则**：

1. 同一 run 内同一 PK 多行 → 保留 `fetched_at` 最新
2. 与已有 curated 分区 merge → 再次 PK dedupe
3. failed / running / stale batch 涉及的数据集 → 整个数据集本 run 不 compact
4. 原子写：先写临时文件再 rename（`storage/atomic.py`）

手动触发：`cml compact --run-id <id>`

---

## 审计（Audit）

**单次 run**：`cml audit --run-id <id>` → `meta/quality/findings/{run_id}.json`

**湖级健康**：`cml audit --full` → 汇总新鲜度、STALE、error/warning findings；UNHEALTHY 时退出码 1。

关键检查：

- `adj_factor_reconciliation`：复权收益极值 + 缺 corporate_actions 警告
- `trading_status_coverage_start`：ST 覆盖起点提示
- mock 行检测（`source="mock"` 且非测试环境）

---

## 重试（Retry）

触发：`cml retry --run-id <id>`

```
run_lock 获取
  ├── stale batch 晋升 failed
  ├── 重跑 failed worker batches / failed step batches
  ├── init：补跑缺失 phase steps
  └── 全部 batch 成功 → 自动 compact → derive → audit
```

水位在 compact 成功前**不会**前移，避免永久数据空洞。

---

## 单数据集回填（Backfill）

触发：`cml backfill <dataset>`

- `fetch_semantics="snapshot"` 且无 `backfill_source` 的数据集**禁止** backfill
- 有 `backfill_source` 的（如 `valuation_metrics` → baostock）允许历史回放
- 成功后自动 compact 当前 run

---

## 按需路径（On-Demand）

不经过 staging/curated 主路径：

```
cml query --dataset stock_news --symbol 600519.SH
  → OnDemandService.fetch()
  → meta/on_demand/{dataset}/{symbol}.json 缓存
```

适用于公告正文、新闻全文等大体积、按 symbol 稀疏访问的数据。

---

## 失败路径示意

```
batch failed
  → 水位不动
  → staging 保留（可 retry）
  → audit findings 记录
  → 人工：cml status → cml retry
  → compact 成功后下游 load() 可见新数据
```

---

## 相关文档

- [数据湖布局](lake-layout.md)
- [orchestrator 模块](../modules/orchestrator.md)
- [steps 模块](../modules/steps.md)
