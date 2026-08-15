# query 模块

路径：`src/cn_market_lake/query/`

消费层：`load()` / DuckDB 视图 / universe / on-demand。

**用法与契约**见 [查询指南](../datasets/query-guide.md) 与 [Python API](../reference/python-api.md)。

---

## 源码地图

| 文件 | 职责 |
|------|------|
| `reader.py` | `load()`, `scan()`, `list_datasets()`, `dataset_schema()` |
| `views.py` | DuckDB 视图；`daily_bars_*` 复权视图 |
| `universe.py` | `apply_universe_filter()` — `all_a` |
| `parquet_scan.py` | Hive 分区裁剪、lazy scan |
| `on_demand.py` | `OnDemandService` — 按需抓取 + JSON 缓存 |
| `__init__.py` | 导出 `load`, `scan`, `list_datasets` |

复权存储类型为 hfq（`STORED_ADJUST_TYPE`）；qfq 在查询期按窗口 anchor 派生（[ADR-0004](../adr/0004-store-hfq-derive-qfq-at-query.md)）。
