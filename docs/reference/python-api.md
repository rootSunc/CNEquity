# Python API 参考

模块：`cn_market_lake.query`

```python
from cn_market_lake.query import load, scan, list_datasets, dataset_schema
```

---

## load()

```python
def load(
    dataset: str,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    adjust: Literal["qfq", "hfq"] | None = None,
    universe: Literal["all_a"] | None = None,
    as_of: str | date | None = None,
    items: list[str] | None = None,
    symbols: list[str] | None = None,
    strict_adj: bool = False,
    all_vintages: bool = False,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> pl.DataFrame
```

### 参数

| 参数 | 说明 |
|------|------|
| `dataset` | 注册数据集名 |
| `start`, `end` | 含边界日期窗口（数据集主日期列） |
| `adjust` | `hfq` / `qfq`；适用于 `daily_bars`、`minute_bars`、`minute_bars_5m` 等价量数据集 |
| `universe` | `"all_a"` 可交易过滤 |
| `as_of` | PIT 截止日：过滤 `announce_date <= as_of`，并对同一科目取当时生效的那一版 |
| `items` | 财报科目 code 列表 |
| `symbols` | symbol 白名单 |
| `strict_adj` | True 时缺复权因子抛 `ReaderError` |
| `all_vintages` | True 时返回 `as_of` 前的**全部**版本（研究财报修订用）；截面选股勿开，会重复计同一事实 |
| `config` / `data_root` | 湖位置；默认读 `configs/cn-market-lake.toml` |

### 返回

- 未复权数据集：原始列
- `adjust` 非空：附加 `adj_open`, `adj_high`, `adj_low`, `adj_close`, `adj_is_exact`

### 异常

`ReaderError`（`ValueError` 子类）：未知数据集、无数据、strict_adj 失败等。

---

## scan()

与 `load()` 参数相同，返回 `pl.LazyFrame`。大窗口推荐 lazy 管道。

```python
lf = scan("daily_bars", start="2020-01-01", adjust="hfq")
df = lf.filter(pl.col("symbol") == "600519.SH").collect()
```

---

## list_datasets()

```python
def list_datasets(
    *,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> pl.DataFrame
```

列：`dataset`, `layer`, `date_col`, `fetch_semantics`, `history_mode`, `backfill_source`, `pit`, `has_data`, `coverage_start`, `coverage_end`, `watermarked`, `watermark`

`history_mode` ∈ `by_date` / `snapshot_with_backfill` / `snapshot_only`；与 `coverage_*` 一起构成可用起点合同。

---

## dataset_schema()

```python
def dataset_schema(dataset: str) -> dict[str, pl.DataType]
```

返回 `domain/schemas.py` 中注册的 Polars 类型映射。

---

## 配置解析

```python
from cn_market_lake.query.reader import resolve_config

cfg = resolve_config(config=my_cfg)
cfg = resolve_config(data_root="/path/to/lake")
```

优先级：`config` > `data_root` > 默认 toml 路径。

---

## 示例

### 后复权全市场

```python
bars = load(
    "daily_bars",
    start="2024-01-01",
    end="2024-12-31",
    adjust="hfq",
    universe="all_a",
    strict_adj=True,
)
```

### PIT 财报

```python
roe = load(
    "financial_statement_items",
    items=["roe"],
    as_of="2024-04-30",
)
```

### 指数行情

```python
idx = load("index_bars", start="2024-01-01", symbols=["000300.SH"])
```

### 显式 data_root（无需 toml）

```python
bars = load("daily_bars", start="2024-06-01", data_root="/data/cn-market-lake")
```

---

## DuckDB 等价

视图由 `query/views.py` 维护。SQL 用户可用 `cml query` 或直连 duckdb 文件，语义应与 `load()` 对齐（复权视图见 `daily_bars_adj`）。

---

## 相关文档

- [查询指南](../datasets/query-guide.md)
- [query 模块](../modules/query.md)
