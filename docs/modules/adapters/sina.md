# sina 适配器

路径：`src/cn_market_lake/adapters/sina/`

新浪行情接口，当前主要用于 **后复权（hfq）累积因子**。

---

## 文件

| 文件 | 职责 |
|------|------|
| `adj_factors.py` | 拉取每股累积复权序列 → 转为逐日 hfq factor |
| `__init__.py` | 导出 |

---

## 与 derive 的关系

`derive/adj_factors.py` 调用本 adapter：

1. 按 symbol 拉取 Sina 复权序列
2. 转换为 `adj_factors` schema（`adjust_type="hfq"`）
3. 缓存至 `meta/adj_factors_cache/`

配置：

```toml
[adj_factors]
source = "sina"
adjust_types = ["hfq"]

[sources.sina]
enabled = true
min_interval_seconds = 0.3
```

---

## 已知限制

- 历史断裂：部分老股因子序列不完整；audit 有 reconciliation 检查
- 仅 hfq；qfq 不在此拉取（ADR-0004）

---

## 相关文档

- [derive 模块](../derive.md)
- [ADR-0004](../../adr/0004-store-hfq-derive-qfq-at-query.md)
