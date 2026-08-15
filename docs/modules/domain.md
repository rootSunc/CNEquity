# domain 模块

路径：`src/cn_market_lake/domain/`

**数据契约层**：schema 类型、主键、数据集元数据、符号规则、跨进程限速、情绪打分工具。不含 I/O 与编排。

---

## 文件一览

| 文件 | 职责 |
|------|------|
| `schemas.py` | Polars schema、`PRIMARY_KEYS`、`validate_dataframe()`、`with_provenance()` |
| `datasets.py` | `DatasetSpec` 注册表 `DATASETS` |
| `symbols.py` | `parse_symbol()`, `is_all_a_symbol()`, CDR/ETF 分类 |
| `rate_limit.py` | 跨平台文件锁 + JSON 时间戳的跨进程 `RateLimiter` |
| `sentiment.py` | 公告关键词 + 可选 SnowNLP 打分 |

---

## schemas.py

### 核心常量

- `DATASET_SCHEMAS: dict[str, dict[str, pl.DataType]]` — 每数据集列类型
- `PRIMARY_KEYS: dict[str, list[str]]` — 主键列
- `MOCK_SOURCE = "mock"` — 测试源标识

### 溯源列

每个 curated schema 末尾包含：

```python
"source": pl.Utf8,
"data_version": pl.Utf8,
"fetched_at": pl.Datetime("us", "UTC"),
```

### validate_dataframe(df, dataset)

写 staging/curated 前调用：

- 列齐全且类型匹配
- 不允许未知列（strict）
- PK 非空

### with_provenance(df, source, data_version)

为 adapter 输出批量添加溯源列。

---

## datasets.py

### DatasetSpec 字段

| 字段 | 含义 |
|------|------|
| `name` | 数据集名 |
| `tier` | L0–L8 研究分层；无默认值，必须显式声明 |
| `layer` | `curated` / `derived`（**存储位置**，与 `tier` 正交） |
| `partition_col` | Hive 分区列；`None` = merge 文件 |
| `partition_granularity` | `day` / `month` / `quarter` / `year`；按每日行数选，不按习惯 |
| `date_col` | 查询日期列；默认等于 `partition_col` |
| `fetch_semantics` | `by_date` / `snapshot` |
| `watermark` | 是否维护 `meta/state` 水位 |
| `pit` | 是否 PIT 数据集 |
| `backfill_source` | snapshot 数据集的历史回填源名 |
| `max_staleness_days` | `status --datasets` 容忍滞后天数 |
| `required` | `False` 时空 curated 只算 warning，不拉低 `lake_health` |
| `history_horizon_days` | 源端还提供多少个**交易日**（滚动，随今天前移） |
| `history_floor_date` | 源端的**固定日历底**（不随今天移动）；与上一项二选一，同时设时它优先 |
| `backfill_chunk_days` | 单次回填子跑覆盖的日历天数（by-date 源用） |
| `backfill_chunk_symbols` | 单次回填子跑的标的数（**tip-paged 源用**，与上一项互斥） |
| `intraday_frequency` | bar 频率（`1m` / `5m`）。**行为字段**：设了就会被 audit 的会话检查、reader 的复权集合、`cml backfill --symbols` 认领 |
| `row_grain` | 一行覆盖多久（`1m` / `5m` / `tick`）。**纯描述**，不驱动任何行为 |

**两组容易混的字段：**

`history_horizon_days` vs `history_floor_date` —— 前者是「每标的固定根数」（分钟线：源端存 22,800 根 1m，除以一个完整交易日得 95 天），窗口每天往前滑；后者是服务端按日历切的保留底（分笔：所有标的都回溯到 2024-01-02），**不随今天移动，所以视野逐日变长**。用错会让 `earliest_available()` 每天漂，几个月后把源端还愿意给的数据挡在门外。

`intraday_frequency` vs `row_grain` —— 前者是行为的，它的消费者都假定存在 `bar_time` 列和「每交易日 N 根」；`trade_ticks` 故意不设它，否则会继承一批在错误列上静默通过的检查。但目录和面板仍需知道它是日内数据，这是 `row_grain` 的唯一职责。两者同时存在时必须一致（注册表测试强制）。

### 辅助函数

```python
get_dataset(name) -> DatasetSpec
curated_dataset_names() -> frozenset[str]
derived_dataset_names() -> frozenset[str]
pit_dataset_names() -> frozenset[str]
fetch_semantics(dataset) -> Literal["by_date", "snapshot"]
is_stale(dataset, mark, anchor) -> bool
```

**新增数据集必须**：在此添加 `DatasetSpec` + 在 `schemas.py` 添加 schema/PK。`tests/unit/test_dataset_registry.py` 强制同步。

---

## symbols.py

- 格式：`600519.SH`、`000001.SZ`、`920001.BJ`
- `is_all_a_symbol()`：沪深 A 股前缀白名单（`60/68/00/30/92`）；不含 ETF 前缀
- `is_cdr_symbol()`：SH `689` 段存托凭证
- `is_etf_symbol()`：场内 ETF/LOF（SH `51/52/56/58`，SZ `15/16`）
- instruments 抓取含股票 + CDR + ETF；`parse_symbol()` → `(code, exchange)`

Universe 过滤在 `query/universe.py` 使用本模块规则：`all_a` 排除 CDR 与 ETF。

---

## rate_limit.py

跨进程限速：锁文件 + 上次请求时间 JSON。供 `adapters/throttle.py` 与 HTTP adapter 使用，防止多 worker 打爆源站。锁通过包根 `file_lock.exclusive_lock`（POSIX `flock` / Windows `msvcrt.locking`）取得。

---

## sentiment.py

`research` step 使用：

- 公告标题/正文关键词情绪
- `use_snownlp=true` 时调用 SnowNLP（可选依赖）

---

## 相关文档

- [数据集目录](../datasets/catalog.md)
- [新增数据集](../development/adding-dataset.md)
