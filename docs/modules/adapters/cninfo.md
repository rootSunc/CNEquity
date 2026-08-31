# cninfo 适配器

路径：`src/cnequity/adapters/cninfo/`

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

### 分页：100 页硬顶与分桶

`hisAnnouncement/query` 单次查询最多返回 **100 页 / 3000 条**：`pageNum>100`
会静默重放第 1 页，而 `totalpages` 仍按真实条数上报（实测 2026-08-28：6537 条、
`totalpages=217`、第 101 页 == 第 1 页）。中报/年报密集日必然触顶。

适配器的处理是**换一个能把当天真正切开的过滤条件**，并且只有当各桶去重后的
公告数**精确等于**源自己给出的 `totalRecordNum` 时才认为这一天读完了：

| 顺序 | 参数 | 取值 | 说明 |
|------|------|------|------|
| 1 | `plate` | `sz` / `sh` / `bj` | 市场，实测各日精确求和 |
| 2 | `plate` | `szmb`/`szzx`/`szcy`、`shmb`/`shkcp` | 市场自身仍超顶时再切板块 |
| 3 | `trade` | CSRC 行业门类 19 类 | 实测精确求和 |
| 4 | `category` | 26 个公告类别 | 兜底：一条公告可属多类，也可能一类都不属 |

对不上就换下一个轴；全部轴都无法自证覆盖时**整天失败**，不会写入"读到多少算多少"。

`column` 不在其中：该参数选的是站点栏目而非市场，`szse`/`sse`/`bj` 返回完全
相同的全市场结果，所以每天只走一遍。

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
