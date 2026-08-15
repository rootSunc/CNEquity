# derive 模块

路径：`src/cn_market_lake/derive/`

由 curated 数据**可重算**的派生数据集。与 `steps/finalize.py` 中的 `derive_adj_factors` step 集成。

---

## 文件一览

| 文件 | 输出 | 触发 |
|------|------|------|
| `adj_factors.py` | `derived/adj_factors` | `cml derive adj_factors` / daily finalize |
| `trading_status_history.py` | 写入 `curated/trading_status` | `cml derive trading_status` |
| `market_breadth.py` | `market_breadth` | macro_risk step 内调用 |
| `sentiment_scores.py` | `sentiment_scores` | research step 内调用 |

---

## adj_factors.py

### 设计（ADR-0004）

- 只拉取、只存储 **hfq**（后复权）因子
- 来源：Sina（`[adj_factors].source`）
- qfq 在 `query/reader.py` 查询期派生

### 流程

1. 从 instruments 取 symbol 列表
2. 检查 `meta/adj_factors_cache/{symbol}.parquet`
3. 缓存未命中：调用 `adapters/sina/adj_factors.py`
4. 除权日 / 新股 / 缓存失效时重抓
5. 写入 `derived/adj_factors/trade_date=.../`

### 安全阈值

- 单日 factor 步长 > 20x → tripwire 警告
- 无缓存失败率 > 5% → fail-loud（整次 derive 失败）
- 输出列含 `adjust_type="hfq"`

### compute_adj_factors(cfg) → AdjFactorResult

返回 `rows`, `failed`, `fail_ratio` 等；CLI 打印 warnings。

---

## trading_status_history.py

`derive_suspension_history(cfg, *, start=None, end=None)` / `cml derive trading_status [--start] [--end]`：

- 对比 `daily_bars` 有成交的日期与 `trading_calendar`
- 推断历史停牌区间
- 按 `DATASETS["trading_status"].partition_for`（月分区 `trade_date=YYYY-MM`）合并写入 `trading_status`（`status="suspended"`）
- `start` / `end` 限制日历窗口（按年分块重建，避免全历史 cross-join OOM）

覆盖可与 `daily_bars` 同起点（约 2001）。与 baostock ST 历史回填互补（ST 标签仍约从 2016 起）；不替代 EastMoney 当日 ST 列表。

---

## market_breadth.py

从全市场 `daily_bars` 计算：

- 上涨/下跌家数
- 涨跌停家数（按板块涨跌幅限制规则）

由 `steps/macro_risk.py` 在抓取宏观数据时调用。

---

## sentiment_scores.py

多通道情绪分：

- `announcement_keywords` — 公告标题关键词
- `news_headlines` — curated 快讯（无 HTTP，日更主通道）
- `stock_news_nlp` — 个股新闻 HTTP 回退（有上限 + 连续失败熔断）

由 `steps/research.py` 调用；配置见 `[sentiment]`。

---

## 相关文档

- [ADR-0004](../adr/0004-store-hfq-derive-qfq-at-query.md)
- [查询指南 — 复权](../datasets/query-guide.md)
