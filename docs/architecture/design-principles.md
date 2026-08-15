# 设计原则

取舍大致围绕下游三件事：回测别被脏数据带偏、日更别悄悄漏、出问题能追到源。更细的分层见 [overview](overview.md)。

## 源失败就暴露

数据源挂了 → batch 标 `failed`，不静默返回空表或假数。测试唯一例外是 `[tdx_protocol].allow_mock=true`，且行必须标 `source="mock"`（audit 会拦）。分页截断、限速失败、schema 不匹配也一样，显式失败。

## 可溯源

curated 行带 `source`（adapter 名）、`data_version`、`fetched_at`（UTC）。

## 口径可重算

- 复权：存未复权价 + 独立 `adj_factors`；qfq / hfq 查询期组合（[ADR-0004](../adr/0004-store-hfq-derive-qfq-at-query.md)）
- 多源：curated 每 PK 一行；备源进 snapshot，diff 由 audit 产出（[ADR-0003](../adr/0003-canonical-curated-with-source-snapshots.md)）
- PIT：低频数据双时间轴 `report_period` + `announce_date`
- 派生：`derived/` 或 DuckDB 视图可从 curated 重算

## 正确性优先于覆盖面

新数据集口径没验证清楚，不如先不交付。已知正确性缺陷优先于新功能。眼下更在意的几块：compact 门禁与水位、instruments 合并保留退市股、adj_factors 断裂与 append-only。

## 能查的问题放在引擎侧

`cml audit --full` 能抓的，别只靠下游自检。例如复权收益极值、adj_factors × corporate_actions 对账、PK 重复、mock 行、分区行数突变。

## 单人可运维

组件尽量本地能读懂、能重建：自研编排（非 Airflow）、Parquet 湖（非 PostgreSQL）、launchd / cron（非 K8s）、SQLite manifest。目标是一个人能读完代码并排障。

## Schema 只增不改

curated 列语义尽量稳定；破坏性变更要版本 bump + 迁移说明。写前校验在 `domain/schemas.py`，注册表在 `domain/datasets.py`。

## 无前视、universe 诚实

`load(..., as_of=)` 对 PIT 数据集过滤 `announce_date <= as_of`，别用 `report_period` 代替公告日。`universe="all_a"` 的 ST / 停牌过滤只覆盖 `trading_status` 有数据的日期；更早窗口只做上市 / 退市过滤，并在 audit 里报覆盖起点。

## ADR

- [0001](../adr/0001-record-architecture-decisions.md) — 用 Markdown 记架构决策
- [0002](../adr/0002-parquet-lake-over-database.md) — Parquet 湖优于数据库
- [0003](../adr/0003-canonical-curated-with-source-snapshots.md) — Canonical + 备源快照
- [0004](../adr/0004-store-hfq-derive-qfq-at-query.md) — 存 hfq、查询派生 qfq

相关：[架构总览](overview.md) · [Schema](../datasets/schema.md)
