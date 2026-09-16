# query 模块

路径：`src/cnequity/query/`

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

### 成交口径的分钟重采样

TDX 的零成交分钟可能沿用昨收或最近报价。直接对这些报价取首值/最高/最低值，会把
未发生的成交价格混进 OHLC；原始 5m 数据也可能包含这种聚合痕迹。需要成交口径时，
从完整的 1m 数据重采样：

```python
from cnequity.query import load, resample_trade_bars

minute = load("minute_bars", start="2026-09-15", end="2026-09-15",
              symbols=["603869.SH"])
five_minute = resample_trade_bars(minute, "5m")
```

支持 5m、15m、30m、60m，按 09:30 和 13:00 分别对齐，不跨午休。
OHLC 只计入 `volume > 0` 或 `amount > 0` 的分钟；后者避免漏掉股数被供应商取整到零的
小额成交。整个区间无成交时保留区间末报价，量额为零，它仍然不是成交价格。

重复时间戳、午休记录、日期错配和缺少组成分钟的区间会报错；整个区间都没有记录时保持
缺失，不凭空补齐。返回值是计算结果，不覆写原始湖，也不继承原始记录的来源标签。
研究应同时保留输入数据的 revision；该函数不能恢复供应商已经丢失的成交细节。
