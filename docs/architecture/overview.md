# 架构总览

cn-market-lake 是 A 股数据的**采集编排层**：在多个外部数据源之上，通过自研 Wave 引擎并行拉取、校验、落湖，并以稳定 schema 交付给下游选股/因子项目。

引擎本身不产生 alpha。它主要挡住三件事：回测用的数据是否干净（PIT、universe、复权）、日更是否按时到、数字对不上时能不能追到源。

运维见 [runbook](../operations/runbook.md)；字段契约见 [schema](../datasets/schema.md)；关键决策见 [ADR](../adr/)。

---

## 六层设计

现有代码覆盖 1–5 层；第 6 层（运行保障）已有 `scripts/` 落地（launchd/cron、健康通知、meta 备份）。

```
┌─────────────────────────────────────────────────────────────┐
│ 下游：选股/因子项目 / DuckDB / Polars 直读                       │
└──────────────────────────┬──────────────────────────────────┘
                           │ load() / SQL / Parquet
┌──────────────────────────▼──────────────────────────────────┐
│ 5. 消费契约层  query/          load()、DuckDB 视图、PIT、复权    │
├─────────────────────────────────────────────────────────────┤
│ 4. 质量保障层  quality/        audit、cross_checks、source_diff│
├─────────────────────────────────────────────────────────────┤
│ 3. 湖存储层    storage/ derive/  staging→curated→derived→meta │
├─────────────────────────────────────────────────────────────┤
│ 2. 采集编排层  orchestrator/ steps/  Wave DAG、manifest、worker │
├─────────────────────────────────────────────────────────────┤
│ 1. 数据源适配层 adapters/      薄 I/O，不含业务编排            │
├─────────────────────────────────────────────────────────────┤
│ 6. 运行保障层  scripts/        launchd、cron、备份、告警          │
└─────────────────────────────────────────────────────────────┘
```

| 层 | 目录 | 职责 | 关键实现 |
|----|------|------|----------|
| 1 | `adapters/` | 协议封装、分页、源侧格式转换 | `tdx_protocol/`、`eastmoney/`、`sina/`、`cninfo/`、`baostock/`、`pboc/`、`nbs/`、`exchange/`、`ths/`、`sw/`、`cni/`、`macro/`、`calendar/`；限速 `domain/rate_limit.py` |
| 2 | `orchestrator/` + `steps/` | Job/Wave/Step、批级 manifest、增量水位 | `JobEngine`、`manifest.py`、`compact_gate.py`、`storage/state.py` |
| 3 | `storage/` + `derive/` | Parquet 四层湖、compact、派生 | `parquet.py`；[ADR-0002](../adr/0002-parquet-lake-over-database.md)/[ADR-0003](../adr/0003-canonical-curated-with-source-snapshots.md) |
| 4 | `quality/` | run 级 findings、湖级 health、跨源 diff | `audit.py::lake_health()`、`cross_checks.py`、`source_diff.py` |
| 5 | `query/` | `load()`、DuckDB 视图 | `reader.py`、`views.py`；[ADR-0004](../adr/0004-store-hfq-derive-qfq-at-query.md) |
| 6 | `scripts/` | 调度、备份、通知 | 见 [运维 Runbook](../operations/runbook.md) |

### 一次日更路径

```
收盘后
  → 调度触发（launchd/cron/Task Scheduler 或手动）
  → Wave: reference → corp_actions → daily_bars → index_bars …
      每 step：adapter → validate_dataframe → staging
  → finalize: compact（manifest 门禁）→ derive_adj_factors → audit
  → curated/derived 就绪，meta/state 水位前移
```

失败路径：batch failed → 水位不动 → `cml retry --run-id` 只重跑失败 batch → 成功后自动 compact→derive→audit。

---

## 编排模型

```
Job (daily / init / backfill / retry)
  └── Wave(s) — 配置中的并行/串行边界
        └── Step level(s) — 拓扑排序后的依赖层
              └── Task / Batch — worker step 的 symbol-batch 粒度
                    └── Manifest (SQLite) — runs + batches 生命周期
```

- **Step**：`@register_step` 注册的可执行单元（40 个：37 采集 + 3 finalize）
- **Batch**：`daily_bars` 等多进程 step 的最小重试单位
- **水位**：`meta/state/{dataset}.json`，compact 成功后前移；有 failed batch 的数据集不推水位

---

## 与下游的契约边界

下游应通过 `cn_market_lake.query.load()` 读数。核心条款：

| 条款 | 说明 |
|------|------|
| hfq 存储 | 湖内只存后复权因子；qfq 查询期按窗口 anchor 派生（[ADR-0004](../adr/0004-store-hfq-derive-qfq-at-query.md)） |
| fail-loud 复权 | `strict_adj=True` 时缺因子报错，不静默 `factor=1.0`；研究路径偏好 `adjust="hfq"` |
| PIT | `financial_statement_items`、`announcement_index` 按 `announce_date <= as_of` |
| universe | `all_a` = 上市/退市过滤 + trading_status 覆盖日内的 ST/停牌过滤 |
| 水位即缓存键 | 下游缓存键宜含 `meta/state/{dataset}.json`；水位前移即失效 |
| 交易日主轴 | 窗口按 `trading_calendar` 计，不用自然日 |
| schema 演进 | curated 列只增不改；破坏性变更 bump `dataset_schema_version` |

影响上述条款的改动视为 breaking change。用法细节见 [query-guide](../datasets/query-guide.md) 与 [python-api](../reference/python-api.md)。

---

## 已知限制与差距

用之前心里有数（不少是踩过坑之后的现状，不是待办清单）：

- **幸存者偏差（当前最大的正确性缺口）**：历史若按当前上市名单回填，退市股会缺失；`universe="all_a"` 无法补救数据本身不在的问题。audit 的 `universe_survivorship_absent` 会以 error 报出——补齐退市股前别拿收益序列做结论。补齐路径：`cml delisted backfill` + `repair`。
- **复权因子**：老股 hfq 曾大面积断裂；现在是 append-only merge，加上 `adj_factor_reconciliation` audit。残余多为 `corporate_actions` 缺事件。
- **估值历史**：以前不少是当日快照；`valuation_metrics` 已可用 baostock 回填。
- **ST / 停牌**：日更只抓当天，更早窗口 `universe="all_a"` 不会按历史 ST 剔除（audit 会报覆盖起点）。
- **北向**：2024-08 后多为季频，别当逐日序列用。
- **调度 / 网络**：`daily_pipeline.sh` + cron/launchd/Task Scheduler + `health_notify.sh` 已有；海外网络下东财组仍可能 soft-fail。
- **运维**：meta 备份有了；snapshot 目录增长和部分 lazy scan 还能再收。

已经比较稳的：日历 / 指数 / 日线可回填到较深历史；财报带 `announce_date` PIT；分组跑完会 compact→audit；CDR（689）和场内 ETF 不进 `all_a`。

原则摘要见 [design-principles](design-principles.md)：源失败就暴露、口径可重算、audit 放引擎侧、组件保持单人可运维。

---

## 技术栈

| 组件 | 选型 |
|------|------|
| Python | ≥ 3.10 |
| DataFrame | Polars |
| 存储 | Parquet (PyArrow, zstd) |
| 查询 | DuckDB（视图层） |
| 编排元数据 | SQLite WAL (`manifest.db`) |
| CLI | Click |
| TDX | 内置线协议客户端（`adapters/tdx_protocol/_wire`） |

---

## 关键架构决策（ADR）

| ADR | 决策 |
|-----|------|
| [0002](../adr/0002-parquet-lake-over-database.md) | Parquet 湖优于自建数据库 |
| [0003](../adr/0003-canonical-curated-with-source-snapshots.md) | curated 每 PK 一行；备源进 snapshot，不自动切源 |
| [0004](../adr/0004-store-hfq-derive-qfq-at-query.md) | 只存 hfq 因子，qfq 查询期计算 |

---

## 相关文档

- [数据流](data-flow.md)
- [数据湖布局](lake-layout.md)
- [设计原则](design-principles.md)
- [模块索引](../modules/README.md)
