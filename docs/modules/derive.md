# derive 模块

路径：`src/cnequity/derive/`

由 curated 数据**可重算**的派生数据集。与 `steps/finalize.py` 中的 `derive_adj_factors` step 集成。

---

## 文件一览

| 文件 | 输出 | 触发 |
|------|------|------|
| `adj_factors.py` | `derived/adj_factors` | `cne derive adj_factors` / daily finalize |
| `trading_status_history.py` | staging `trading_status` | `trading_status_derive` step / `cne derive trading_status` |
| `regulatory_events.py` | staging `regulatory_events` | `regulatory_events` step |
| `market_breadth.py` | `market_breadth` | macro_risk step 内调用 |
| `sentiment_scores.py` | `sentiment_scores` | research step 内调用 |

---

## adj_factors.py

### 设计（ADR-0004）

- 只拉取、只存储 **hfq**（后复权）因子
- 来源：Sina（`[adj_factors].source`）
- 股票 payload 使用 `f`；ETF/LOF payload 的 `f` 是占位值，真实累计因子在 `s`
- ETF/LOF 的 hfq 直接使用 `s`，qfq 在查询期按 `1/s` 转换
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

`derive_suspension_history(cfg, run_id, *, start=None, end=None)`、`trading_status_derive` step
（日更 core wave，`daily_bars` 之后、`compact` 之前，默认回看 90 天）、
`cne derive trading_status [--start] [--end]`（自建一次 run 走 derive + compact）：

- 对比 `daily_bars` 有成交（`volume > 0`）的日期与 `trading_calendar`；停牌 OHLC 占位行不算成交
- 推断历史停牌区间，**写入 staging**，由 compact 合并并发布 committed 修订
- 只报**区间内**缺口：每个标的的窗口被自身首末 bar 夹住，所以尚未入湖的最新交易日不会被误判为全市场停牌
- `start` / `end` 限制日历窗口（按年分块重建，避免全历史 cross-join OOM）

行冲突不在这里解决：`domain/trading_status.py` 的**证据等级**决定谁胜出——
交易所记录、以及当日收盘后读到的板快照（point-in-time）> 派生停牌 > 事后
盖到旧交易日上的当前态快照。日更 EastMoney 快照因此不会再抹掉派生停牌行，
而 Baostock / 退市 / 当日收盘后的快照仍能纠正它。

覆盖可与 `daily_bars` 同起点（约 2001）。与 Baostock ST 历史回填互补；显式配置 Tushare Pro 后，可用 `bak_basic` 覆盖 2016、`stock_st` 覆盖 2017-01-01 起的 BJ。实际 ST 证据起止范围以 `historical_st_evidence` 收据和 `cne audit --full` 为准，两者都不替代 EastMoney 当日 ST 列表。

---

## regulatory_events.py

`regulatory_events_from_announcements(frame)` / `derive_regulatory_events(cfg, start=, end=)`，
由 `regulatory_events` step 调用：

- 从**已提交的** `announcement_index` 行里按标题关键词筛出监管事件
  （行政处罚 / 处罚决定 / 立案 / 调查 / 监管函 / 警示函 / 处分），首个命中的
  关键词决定 `event_type`
- `event_id = "reg-" + announcement_id`，事件与它来自的公告始终可 join
- 不发任何请求。CNINFO 该端点没有服务端过滤，以前这个数据集用与
  `announcement_index` 完全相同的请求把全天公告重抓一遍（实测 2026-01-01：
  46 页 1375 条换 6 条事件），两次抓取还相隔一小时，同一天可能对不上
- 窗口取不到 `announcement_index` 的水位之后：多出来的日期记 `pending_source_coverage`
  finding 并把这次 step 标为 `degraded`，等公告入湖后的下一次运行补上
- 窗口内 `announcement_index` 一行都没有时直接报错，而不是写一个看起来干净的空结果

## market_breadth.py

从全市场 `daily_bars` 计算：

- 上涨/下跌家数
- 涨跌停家数（按板块涨跌幅限制规则）

仅统计当日有成交的股票（`volume > 0`）。停牌占位行虽然保留 OHLC，但
按数据契约是 `volume=0`，不应被当作平盘股计入市场宽度分母。

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
