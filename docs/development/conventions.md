# 开发约定

与 [CONTRIBUTING.md](../../CONTRIBUTING.md) 互补；本文更完整地描述包结构与分层规则。

---

## 包布局

所有代码在 `src/cn_market_lake/`：

| 子包 | 职责 |
|------|------|
| `domain` | 契约（schema、数据集元数据） |
| `adapters` | 源 I/O，薄 |
| `steps` | 采集编排单元 |
| `orchestrator` | 引擎与 manifest |
| `storage` | 湖读写 |
| `derive` | 可重算派生 |
| `quality` | 审计 |
| `query` | 只读消费 |
| `config` / `cli` | 配置与入口 |

---

## Steps 按数据层分文件

| 文件 | 数据层 |
|------|--------|
| `reference.py` | L0 |
| `bars.py` | L1 |
| `events.py` | L2 |
| `fundamentals.py` | L3 |
| `capital.py` | L4 |
| `structure.py` | L5 |
| `macro_risk.py` | L6/L8 |
| `research.py` | L4/L7 |
| `finalize.py` | 收尾 |

新数据集 step 放入对应层文件；新层则新建文件并在 `steps/__init__.py` import。

---

## 数据契约优先

1. `domain/schemas.py` — 列类型 + PK
2. `domain/datasets.py` — `DatasetSpec`
3. `tests/unit/test_dataset_registry.py` 保持同步

写 staging 前必须 `validate_dataframe()`。

---

## Adapters 要薄

- 协议、分页、源字段映射 → `adapters/`
- 增量窗口、写 staging、manifest → `steps/`
- 不含 DuckDB / compact 逻辑

---

## 测试原则

- **单元测试离线**：mock/monkeypatch 网络
- **网络测试**：`@pytest.mark.network`，单独运行
- **集成测试**：`@pytest.mark.integration`，`tests/integration/`

```bash
ruff format .
ruff check .
pytest tests/unit
pytest tests/integration  # 可选
```

全局超时 120s（`pyproject.toml`）。

---

## 架构决策

非平凡设计选择写 ADR：`docs/adr/`，复制 `0000-template.md`。

---

## 代码风格

- Ruff：line-length 100，py310
- 类型注解：新代码推荐完整标注
- 注释：仅解释非显而易见的业务/协议细节

---

## 相关文档

- [新增数据集](adding-dataset.md)
- [测试](testing.md)
- [模块索引](../modules/README.md)
