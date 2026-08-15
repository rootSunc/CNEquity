# cninfo 适配器

路径：`src/cn_market_lake/adapters/cninfo/`

巨潮资讯（CNINFO）公告与监管数据。

---

## 文件

| 文件 | 数据集 |
|------|--------|
| `announcements.py` | announcement_index |
| `regulatory.py` | regulatory_events |
| `__init__.py` | 导出 |

---

## announcement_index

- 公告元数据：标题、类型、`announce_date`、`report_period` 等
- PIT 数据集：`load(..., as_of=)` 按公告日过滤
- 正文：`announcement_body` on-demand **尚未实现**（勿写入默认 `[on_demand].datasets`）

---

## regulatory_events

- 监管处罚、立案调查等
- 分区：`event_date`
- 主键：`event_id`

---

## 配置

```toml
[sources.cninfo]
enabled = true
min_interval_seconds = 1.0
```

---

## 相关文档

- [events step](../steps.md)
- [macro_risk step](../steps.md)
