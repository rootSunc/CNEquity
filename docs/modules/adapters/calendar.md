# calendar 适配器

路径：`src/cn_market_lake/adapters/calendar/`

A 股**交易日历** — 节假日排除、回测窗口对齐的地基。

---

## 文件

| 文件 | 职责 |
|------|------|
| `exchange_calendar.py` | 加载种子 CSV、推导交易日、与指数 bar 交叉验证 |
| `holidays_cn.py` | 中国法定节假日规则 |
| `seeds/trading_calendar.csv` |  bundled 种子（2016–2027） |
| `__init__.py` | 导出 |

包数据：`pyproject.toml` `package-data` 包含 `seeds/*.csv`。

---

## 数据源优先级

1. **种子 CSV**（交易所公布日历为主）
2. **指数 bars 推导**（种子缺失日的兜底）
3. 禁止仅按周一到周五启发式（init backfill 会拉全量 calendar）

---

## trading_calendar 数据集

- 分区：`trade_date`
- 主键：`trade_date`
- 列含 `is_trading_day`、交易所标识等（见 [schema.md](../../datasets/schema.md)）

---

## 使用方

- `steps/reference.py` — `trading_calendar` step
- `steps/common.is_trading_day()` — 日更是否跳过
- `query/reader` — 下游应按交易日而非自然日计窗口
- `derive/trading_status_history` — 停牌推断

---

## 相关文档

- [reference step](../steps.md)
- [设计原则 — 交易日主轴](../../architecture/design-principles.md)
