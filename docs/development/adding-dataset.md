# 新增数据集

检查清单与大致实施顺序。

---

## 检查清单

- [ ] `domain/schemas.py`：列类型 + `PRIMARY_KEYS` 条目
- [ ] `domain/datasets.py`：`DatasetSpec`（分区、语义、水位、PIT、staleness）
- [ ] `adapters/<source>/`：拉取与字段映射（或 `derive/` 若派生）
- [ ] `steps/<layer>.py`：`@register_step` 实现
- [ ] `steps/__init__.py`：import 新模块
- [ ] `configs/cn-market-lake.example.toml`：加入 wave 或 group（若日更）
- [ ] 单元测试：归一化 + 至少一个边界 case
- [ ] `docs/datasets/catalog.md` + `schema.md` / `sources.md` 更新
- [ ] `query/views.py` 自动发现（curated 目录存在即可）；特殊视图再改 views

---

## 实施顺序

### 1. 定义契约

```python
# domain/schemas.py
DATASET_SCHEMAS["my_dataset"] = { ... provenance ... }
PRIMARY_KEYS["my_dataset"] = ["symbol", "trade_date"]

# domain/datasets.py
DatasetSpec("my_dataset", partition_col="trade_date", fetch_semantics="by_date"),
```

### 2. 实现 adapter

```python
def fetch_my_dataset(cfg, trade_date) -> pl.DataFrame:
    # 返回符合 schema 的 DataFrame
    return with_provenance(df, source="eastmoney", data_version="...")
```

### 3. 注册 step

```python
@register_step("my_dataset", depends_on=["instruments"], group="signals")
def step_my_dataset(cfg, trade_date, run_id, ctx):
    df = fetch_my_dataset(cfg, trade_date)
    rows = write_simple(cfg, "my_dataset", run_id, df)
    return {"rows": rows}
```

### 4. 配置

将 `my_dataset` 加入合适的 `[job.daily.groups.*].steps`，**末尾组内已有 compact 则不必重复添加 compact 到每组**（每组独立 run 需各自 compact）。

### 5. 测试

```python
def test_my_dataset_normalizes(monkeypatch):
    monkeypatch.setattr(..., "fetch_raw", lambda: ...)
    # 调用 step 或 adapter，断言 schema/PK
```

运行 `pytest tests/unit/test_dataset_registry.py`。

---

## fetch_semantics 选择

| 源行为 | 选型 |
|--------|------|
| 可按历史日期查询 | `by_date` |
| 仅当前页面快照 | `snapshot` |
| 快照但有独立历史 API | `snapshot` + `backfill_source="baostock"` 等 |

---

## Worker Step

仅当需要按 symbol 并行（如全市场日线）时设 `requires_workers=True`。大多数 HTTP 全市场接口用单 step + 内部分页即可。

---

## PIT 数据集

- schema 含 `announce_date`
- `DatasetSpec(pit=True)`
- 文档注明 `load(..., as_of=)` 语义

---

## 相关文档

- [domain 模块](../modules/domain.md)
- [steps 模块](../modules/steps.md)
- [CONTRIBUTING.md](../../CONTRIBUTING.md)
