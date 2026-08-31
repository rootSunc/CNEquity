# 数据集目录

cnequity 交付 **42 个注册数据集**（39 curated + 3 derived：`adj_factors`、`industry_index`、`delisting_events`），按选股用途分为 L0–L8 九类。另有 **on-demand** 数据集不进 curated 主路径。其中日内数据集 `minute_bars` / `minute_bars_5m` 默认关闭，需在 `[minute_bars]` 显式开启；分笔 `trade_ticks` 同样默认关闭，开关在**独立的** `[trade_ticks]`。

权威字段定义：[schema.md](schema.md)。逐源限制：[sources.md](sources.md)。

程序化可用起点与历史模式：`list_datasets()` → `coverage_start` / `coverage_end` / `history_mode` / `backfill_source`。

**图例**（下表）：语义 `by_date` / `snapshot`；水位 ✓ = 维护 `meta/state` 水位。

---

## 数据分层

| 层次 | 说明 | 代表数据集 |
|------|------|------------|
| **L0** 基础参考 | Universe、日历、交易状态 | instruments, trading_calendar, trading_status |
| **L1** 行情 | 未复权价量 + 复权因子 + 可选分钟/分笔/商品 + 退市形态 | daily_bars, index_bars, minute_bars*, minute_bars_5m*, trade_ticks*, commodity_bars*, adj_factors, delisting_events |
| **L2** 公司事件 | 除权除息、公告、预约披露 | corporate_actions, announcement_index, earnings_disclosure_schedule |
| **L3** 基本面 | 财报、估值、一致预期 | financial_statement_items, valuation_metrics, analyst_consensus |
| **L4** 资金面 | 北向、融资、主力 | fund_flow, northbound_*, margin_trading, dragon_tiger, block_trades, institutional_holdings |
| **L5** 结构行业 | 板块、指数成分、行业 | sector_members, index_constituents, industry_members, industry_index |
| **L6** 宏观 | 利率、景气、货币 | macro_indicators, market_breadth |
| **L7** 舆情 / 轮动 | 新闻、情绪、板块、人气、事件流 | sentiment_scores, hot_rank, sector_bars, sector_fund_flow, news_headlines, flash_news_wire, economic_calendar*（stock_news 为 on-demand） |
| **L8** 风险合规 | 解禁、监管 | share_unlock_schedule, regulatory_events |

\*可选或 `required=false`：分钟线默认关；商品期货需显式回填；`economic_calendar` 东财源已下线，仅占位。

分层是**研究用途**，与存储 `layer` 正交：`adj_factors` / `delisting_events`（L1）和 `industry_index`（L5）落在 `derived/` 而非 `curated/`，但按用途归入各自层，所以没有单独的「派生」层。权威来源是 `DatasetSpec.tier`；`test_docs_catalog.py` 断言本文档与注册表逐层一致。

---

## 采集模式

| 模式 | 含义 | 示例 |
|------|------|------|
| **batch** | 日更/周更，走 staging → compact → curated | daily_bars, fund_flow |
| **derived** | 由 curated 计算，可 `cne derive` 重算 | adj_factors |
| **on-demand** | 按 symbol 抓取，缓存于 meta | stock_news, research_reports |

### 拉取语义（fetch_semantics）

| 值 | 行为 | 数据集示例 |
|----|------|------------|
| `by_date` | 可按日期回补缺口 | daily_bars, margin_trading |
| `snapshot` | 仅抓 run 当日快照，禁止伪造历史 | valuation_metrics, sector_members |

`snapshot` 数据集若配置了 `backfill_source`（如 `valuation_metrics` → baostock、`sector_bars` → ths），允许 `cne backfill` 走专用历史源。

### 历史可用性（history_mode）

由 `fetch_semantics` + `backfill_source` 推导（见 `list_datasets()`）：

| history_mode | 含义 | 数据集 |
|--------------|------|--------|
| `by_date` | 可按日回补 / 缺口填补 | 绝大多数行情与事件表 |
| `snapshot_with_backfill` | 日更是快照，但有专用历史源 | `valuation_metrics`→baostock；`index_constituents`→cni；`industry_members`→sw；`sector_bars`→ths |
| `snapshot_only` | **永远没有诚实历史序列**（只有 tip） | `analyst_consensus`、`fund_flow`、`sector_members`、`hot_rank`、`sector_fund_flow`、`news_headlines`、`flash_news_wire`、`economic_calendar` |

`trading_status` 的停牌覆盖可从 `daily_bars` 起点派生；ST 覆盖必须以完整的 `historical_st_evidence` 收据为准。没有覆盖请求窗口的收据时，**不要**假定 2001 起 `universe="all_a"` 已剔除历史 ST。BJ 可选用 Tushare Pro：2016 年通过 `bak_basic` 的历史简称、2017-01-01 起通过 `stock_st`；2016 年以前仍需独立的更深历史源。

### `trade_ticks` 是什么，不是什么

**不是逐笔成交。** A 股 Level-1 是**每 3 秒一帧的快照**，通达信的「分笔」是这个快照的聚合结果。
实测：当日接口带的「本帧成交笔数」字段，`600519.SH` 均值 **6.3 笔**、`000001.SZ` 均值 **33.4 笔**（最大 1217）。
一个交易日因此最多约 4,800 条（14,400 交易秒 ÷ 3），实测全市场随机 40 只均值 **2,721 条**。

由此带来三条必须知道的口径：

| 项 | 实情 |
|----|------|
| 时间戳 | **只到分钟**，秒位恒为 `00`。不是被截断的，是协议从来没带过秒 |
| 主键 | 因此是 `(symbol, trade_date, tick_seq)`——同一分钟可以有 20 条记录时间戳完全相同 |
| `direction` | 通达信按 tick rule **推断**的方向，不是交易所字段；四个取值 `buy` / `sell` / `neutral` / `after_hours` |

`after_hours` 是 15:05–15:30 的盘后固定价格成交，价格恒等于当日最后成交价，且**不计入交易所当日成交量**——
与日频对账前必须先剔除它（含它 1.000363，剔除后 1.000000）。

**没有 `amount` 列。** 源端不提供；`price × volume` 可以自己算，但要知道它是近似：
一帧里多笔不同价成交被合并成一个代表价。实测这个失真在 ±0.03% 以内。

合规边界见 [legal-and-data-sources](../legal-and-data-sources.md)：这**不是**交易所 Level-2，没有逐笔委托，没有十档。

### 历史视野：两种机制，不要混

`history_mode` 说的是**能不能**回补，这一节说的是**能回补多远**。源端的限制有两种，形状完全不同：

**（一）每标的固定根数**（`history_horizon_days`）——分钟线是这种。取值为「源端还提供多少个交易日」，随今天滚动。

| 数据集 | history_horizon_days | 实测（2026-08-01） |
|--------|---------------------|-------------------|
| minute_bars（1m） | **95** | 22,800 根/标的，最早 2026-03-16 |
| minute_bars_5m（5m） | **491** | 23,568 根/标的，最早 2024-07-23（约 2 年） |

**（二）固定日期底**（`history_floor_date`）——分笔和北向资金流是这种。**不随今天滚动**，所以视野是逐日**变长**的。

| 数据集 | history_floor_date | 实测（2026-08-02） |
|--------|-------------------|-------------------|
| trade_ticks | **2024-01-02** | 所测每一只标的都是同一天；2023-12-28 为空 |
| northbound_flows | **2014-11-17** | 沪股通开通日；更早没有该资金流 feed |

两者的区别不是学术问题：把固定底写成滚动天数，`earliest_available()` 会每天往前漂，几个月后就把源端还愿意提供的数据挡在门外。

`trade_ticks` 的底落在自然年边界上，所以保留策略**可能是按自然年**而非固定日期——
`scripts/probe_trade_ticks.py` 留着就是为了**每年 1 月复测一次**。

| 其余全部 | 两者皆 `None` | 源端不设上限 |

这是**源的属性，不是本湖的待办**。更早的窗口返回的不是更少数据，而是没有数据，且没有回填源能补深——`by_date` 单独看会让人以为能回补十年。

**分钟线的机制是「每标的固定根数」，不是固定日期。** 1m 约 22,800 根、5m 约 23,568 根；除以一个完整交易日（240 / 48 根）就得到上表的天数。所以这个数字对**每个交易日都有报价的标的**成立——也就是所有正常 A 股，本数据集的服务对象。反过来，只在零星日子有 bar 的标的会按比例伸得更远：`162107.SZ`（几乎不成交的 LOF）只有 3,216 根 5m，散在 67 个交易日上，因而回溯到 2012 年。**把它当作正常个股的保证，而不是所有标的的硬上限。**

活跃标的间高度一致：1m 实测 `600519.SH` / `000001.SZ` / `300750.SZ` / `688981.SH` / `603005.SH` 均为 95±1 天，跨交易所、跨流动性。

`cne backfill minute_bars --start` 早于视野会直接报错而不是扫一整天返回空。要拉冷门标的的深历史，先把 `[minute_bars].scope` 收窄成 watchlist。程序化读法：`list_datasets()` 的 `history_horizon_days` 列。

`cne backfill trade_ticks --start` 同样会拦，但文案不同：分笔的底对所有标的一致，**没有哪个更窄的范围能拉到更早的数据**。

### `trade_ticks` 的容量（实测）

按 symbol-session 计：约 **1.85 次请求**、约 **2,721 行**；落盘约 **8.4 字节/行**（含溯源列，zstd）。

| 范围 | 请求/日 | 行数/日 | 落盘/日 | 一年（242 日） |
|------|--------|--------|--------|--------------|
| watchlist 200 只 | ~500 | ~54 万 | **~4.5 MB** | ~1.1 GB |
| 全市场 5,197 只 | ~9,600 | ~1,410 万 | **~119 MB** | ~29 GB |

串行实测 1.70 req/s（受网络延迟限制，非限速器）；4 线程可到约 8.7 req/s，则 watchlist 约 1 分钟/日、全市场约 20 分钟/日。
**首次把 200 只拉满全部历史约 312,000 次请求、10–11 小时。**

配置里 `[trade_ticks].max_symbols` 默认 200 就是为此：`index:000300.SH` 解析出约 300 只会直接报错，
要跑得自己把上限调高——这一步摩擦是故意的。`scope = "all"` 不支持，配置校验期就会拒绝。

### 为什么没有 15m / 30m / 60m 数据集

源端在同一个 491 天窗口内也提供它们，但**它们能从 5m 精确聚合**：48 根 5m 分别被 3 / 6 / 12 整除，且收盘分钟边界完全对齐（实测聚合得到 16 / 8 / 4 根，标签为 09:45…15:00 / 10:00…15:00 / 10:30…15:00）。存它们等于用三个数据集装一个 `group_by_dynamic` 就能得到的东西。

```python
from cnequity.query import load
import polars as pl

bars = load("minute_bars_5m", start="2026-07-01", symbols=["600519.SH"])
bars_15m = (
    bars.sort("bar_time")
    .group_by_dynamic("bar_time", every="15m", closed="right", group_by="symbol")
    .agg(pl.col("open").first(), pl.col("high").max(),
         pl.col("low").min(), pl.col("close").last(),
         pl.col("volume").sum(), pl.col("amount").sum())
)
```

---

## 溯源列（所有 curated 行）

| 列 | 类型 | 说明 |
|----|------|------|
| `source` | string | 数据源标识 |
| `data_version` | string | 源版本/批次 |
| `fetched_at` | timestamp[us, UTC] | 抓取时间 |

带 `announce_date` 的 PIT 数据集（`load(..., as_of=)`）：`financial_statement_items`、`announcement_index`。

按需数据集（`[on_demand].datasets`）：默认 `stock_news`、`research_reports`（已实现）。`announcement_body` / `financial_reports` 尚未实现。访问：`cne query --dataset <name> --symbol <code>.SH`。

注册表源码：`domain/datasets.py`（`DatasetSpec`）、`domain/schemas.py`（Polars dtype / `PRIMARY_KEYS`）；`test_dataset_registry.py` 断言同步。

---

## L0 基础参考

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| instruments | —（单文件 merge） | symbol | by_date | — | tdx_protocol | EM 分别从 A 股与 ETF/LOF clist 补 list_date；baostock 回填退市股（`cne backfill instruments`）；merge 保留退市 |
| trading_calendar | trade_date | trade_date | by_date | ✓ | exchange_calendar | 种子 CSV 2016–2027 |
| trading_status | trade_date（按月） | symbol, trade_date | by_date | ✓ | eastmoney | baostock ST 回填；派生停牌写月分区。`status`（normal/suspended/**delisted**）与 `risk_warning`（ST/*ST）是两列——旧版单列会让停牌冲掉 ST 标记；退市行由 `instruments` 判定并标 `derived_delisted`。旧湖读取自动兼容，物理迁移见 [schema](schema.md#trading_status) |

---

## L1 行情

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| daily_bars | trade_date | symbol, trade_date | by_date | ✓ | tdx_protocol | tip 缺口东财 clist 路由进 curated；多日 kline；BJ→sina；snapshot 仍留 audit |
| index_bars | trade_date | symbol, trade_date, frequency | by_date | ✓ | tdx_protocol | |
| minute_bars | trade_date | symbol, trade_date, bar_time, frequency | by_date | ✓ | tdx_protocol | 1m。**可选**，默认关；`[minute_bars]` 配置范围；**源端只有 95 个交易日**（见下「历史视野」）；全市场约 35MB/日；required=false |
| minute_bars_5m | trade_date | symbol, trade_date, bar_time, frequency | by_date | ✓ | tdx_protocol | 5m。同上可选；**491 个交易日（约 2 年），是唯一有真历史的日内频率**；全市场约 6MB/日；required=false |

两个日内数据集共用一组质量检查：主键重复（通用 `pk_unique`）、时段外 bar、`trade_date` 与 `bar_time` 不一致、会话缺口，以及**与日频的成交量+成交额双向对账**。
| trade_ticks | trade_date | symbol, trade_date, tick_seq | by_date | ✓ | tdx_protocol | 分笔。**可选**，默认关；`[trade_ticks]` 独立配置；**不是逐笔成交**（见下）；源端回溯至 **2024-01-02**；watchlist 200 只约 7MB/日；required=false |
| commodity_bars | trade_date | symbol, trade_date | by_date | ✓ | eastmoney+sina | 国内主连 + COMEX金 `GC0.CMX`；`cne backfill commodity_bars`；required=false |
| adj_factors | trade_date | symbol, trade_date, adjust_type | derived | ✓ | sina | 仅 hfq；股票读 `f`、ETF/LOF 读 `s`；`cne derive adj_factors` |
| delisting_events | —（单文件 merge） | symbol | derived | — | sina | 每只退市股的结尾形态；`cne delisted backfill` 产出 |

---

## L2 公司事件

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| corporate_actions | ex_date（按年） | symbol, ex_date, action_type | by_date | ✓ | eastmoney（日更） | 回填：tdx_protocol；混粒度用 `scripts/repartition.py` |
| announcement_index | announce_date | announcement_id | by_date PIT | ✓ | cninfo | `as_of` 过滤 |
| earnings_disclosure_schedule | report_period | symbol, report_period | by_date | — | eastmoney | 预约披露时间表（RPT_PUBLIC_BS_APPOIN）；现值语义非 PIT：变更覆盖 scheduled_date（first_scheduled_date 保留首约，actual_date 披露后回填）；`cne backfill` 走 2016 起全报告期 |

---

## L3 基本面

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| financial_statement_items | report_period | symbol, report_period, statement_type, item_code | by_date PIT | — | eastmoney | 按报告期分区；`cne backfill` 默认自 2001 起（`--start`/`--end` 分块）；PIT 同时受 `announce_date` 与 `fetched_at` 截止；baostock 不用于 FSI |
| valuation_metrics | trade_date | symbol, trade_date | snapshot | ✓ | eastmoney | 回填：baostock |
| analyst_consensus | forecast_date | symbol, forecast_date | snapshot | ✓ | eastmoney | |
| share_structure | change_date | symbol, change_date, announce_date | by_date PIT | — | eastmoney | 总股本/流通/限售/自由流通。**按变动日期扫，不是按报告期**：END_DATE 是股本变动日，2025Q3 有 88 个不同日期 |
| shareholder_counts | count_date | symbol, count_date, announce_date | by_date PIT | — | eastmoney | 股东户数与户均持股，筹码集中度输入。**旬末/月末也披露**：2025Q3 区间 13,356 行 / 71 个日期，只扫季末仅 5,635 行 |
| top_holders | record_date | symbol, record_date, holder_scope, holder_rank, holder_name, announce_date | by_date PIT | — | eastmoney | 一张表两个口径：`holder_scope=total`（前十大股东）/ `float`（前十大流通股东）。2025Q3 有 10,749 行全口径不落在季末 |

---

## L4 资金面

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | staleness |
|--------|--------|------|------|------|------|-----------|
| fund_flow | trade_date | symbol, trade_date | snapshot | ✓ | eastmoney | 1d |
| margin_trading | trade_date | symbol, trade_date | by_date | ✓ | exchange | 沪深交易所自行编制的融资融券明细；SH 无融券余额（`short_balance` 为 null），深交所晚一个交易日发布、两边齐了才写；`[margin_trading] source` 可切回 eastmoney |
| northbound_holdings | trade_date | symbol, trade_date, channel | by_date | ✓ | eastmoney | 100d（季频） |
| northbound_flows | trade_date | trade_date, channel | by_date | ✓ | eastmoney | 2d |
| dragon_tiger | trade_date | symbol, trade_date, reason | by_date | ✓ | eastmoney | 1d；见下「为什么这两个还在东财」 |
| block_trades | trade_date | symbol, trade_date, price, volume | by_date | ✓ | eastmoney | 1d；同上 |

**为什么这两个还在东财。** `margin_trading` 已按 [ADR-0006](../adr/0006-publishers-over-vendors.md)
换成交易所自行发布的数据，龙虎榜与大宗交易本应同样处理——两者也都由交易所公开披露。但在这次改动中没有找到
稳定的官方接口：深交所 `ShowReport` 的 `CATALOGID`（已确认 `1110` 股票列表、`1815_stock_snapshot`
个股日行情、`1837_xxpl` 融资融券）没有对应这两项的条目，数字段 1800–1845 与 `main_*` 前缀的枚举也没有命中；
上交所 `commonQuery.do` 的若干 `sqlId` 猜测全部返回 `total=0`。**没有换**，因为拿一个猜出来的解析器去替换
一个能用的源，是把可用变成不可用。补上官方接口后再迁移。
| institutional_holdings | report_period | symbol, holder_type, report_period | by_date | — | eastmoney | |

---

## L5 结构行业

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 |
|--------|--------|------|------|------|------|
| sector_members | as_of_date | symbol, sector_code, as_of_date | snapshot | ✓ | eastmoney |
| index_constituents | as_of_date | index_symbol, symbol, as_of_date | snapshot | ✓ | eastmoney |
| industry_members | as_of_date | symbol, classification_system, as_of_date | snapshot | ✓ | eastmoney |
| industry_index | trade_date（按年） | trade_date, industry_code, level, weighting | derived | ✓ | derived (industry_members × hfq daily_bars) |

`industry_index` 归 L5 而非 L1：观测单位是行业而不是标的，且由本层的成员关系算出，指数与成分不会打架。`cne derive industry_index` 重算。

快照类仅积累「每日一份成员关系」，历史分位数需多日分区累积。

历史回填（C2）：`cne backfill industry_members` = 申万 SwClass2021 月度（`classification_system=sw`，2020 起）；
`cne backfill index_constituents` = 国证调样史（399001/399006，约 2021-12 起）。中证 000300/000905 仍仅日更 EM 快照。

---

## L6 宏观

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| macro_indicators | obs_date | indicator_id, obs_date | by_date | ✓ | eastmoney / pboc（社融） | |
| market_breadth | trade_date | trade_date, metric_id | by_date | ✓ | derived (daily_bars) | |

---

## L7 舆情 / 轮动

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| sentiment_scores | trade_date | symbol, trade_date, score_channel | by_date | ✓ | derived | |
| hot_rank | trade_date | symbol, trade_date | snapshot | ✓ | eastmoney | 人气榜 top100（公开接口上限） |
| sector_bars | trade_date | sector_code, trade_date | snapshot | ✓ | eastmoney | 回填：ths（同花顺 board-kline） |
| sector_fund_flow | trade_date | sector_code, trade_date | snapshot | ✓ | eastmoney | 板块主力净流入 |
| news_headlines | publish_date | news_id | snapshot | ✓ | eastmoney | 新闻标题 |
| flash_news_wire | publish_date | wire_id, wire_source | snapshot | ✓ | eastmoney | 7×24 快讯线 |
| economic_calendar | event_date（按年） | event_id | snapshot | ✓ | —（源已下线） | EM `RPT_ECONOMICCALENDAR` 已退役（code 9501），保留 schema 等替代源；`required=false`，空表不判 UNHEALTHY |

`sector_bars` 日更只有当日 OHLC；历史由 `cne backfill sector_bars` 一次性写入（国内网络或代理）。
海外一键脚本见引擎 `scripts/china_egress_backfill.sh`（含 `trading_status` ST 回填）。

---

## L8 风险合规

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 |
|--------|--------|------|------|------|------|
| share_unlock_schedule | unlock_date | symbol, unlock_date | by_date | ✓ | eastmoney |
| regulatory_events | event_date | event_id | by_date | ✓ | cninfo（派生自 announcement_index） |

---

## 主备配置（Failover → meta/source_snapshots）

| 数据集 | 主源 | 备源 |
|--------|------|------|
| daily_bars | tdx_protocol | eastmoney |
| corporate_actions | eastmoney | tdx_protocol |

## 对发布方的核对（authority checks）

主备比对的是两个转发方：它们一致只能说明两者不冲突，不能说明谁对。以下检查越过转发方，直接对上游发布机构，
写入 `meta/quality/source_diffs/authority-<date>.json`，只报不拦（见 [ADR-0006](../adr/0006-publishers-over-vendors.md)）。

| 检查 | 数据集 | 对照方 |
|------|--------|--------|
| `macro_pmi_vs_nbs` | macro_indicators | 国家统计局 PMI 发布稿 |
| `st_labels_vs_exchange` | trading_status | 沪深交易所证券列表简称 |
| `daily_bars_vs_exchange` | daily_bars | 沪深交易所自身发布的收盘行情 |
| `adj_factor_corporate_action_divergence` | adj_factors | 由 `corporate_actions` 独立重算的复权因子步长 |

三点实测结论（2026-08-28 / 08-26），也是各自容差的由来：

- **OHLC 完全一致**：5,212 个共同标的、四个价格字段全部 0 bps。所以价格容差可以收紧（默认 10 bps），
  超阈即 error。
- **成交量/成交额单向偏低**：305 个 SZ 标的 curated 低于交易所公布值，方向全部一致——交易所日合计包含
  连续竞价 bar 不含的成交。这是口径差异不是错误，因此按「偏离标的占比」判定（默认 15%），而不是逐只报。
- **停牌占位不算缺口**：深交所会为停牌证券发布一行零成交（OHLC 等于前收），行情源则不出 bar。
  `daily_bars_missing_vs_exchange` 只统计交易所侧有成交的标的。

`adj_factors` 的重算基于除权除息日的连续性恒等式：

```
f_ex / f_prev = (1 + 送股 + 转股 + 配股) × 前收 / (前收 − 税前现金分红 + 配股比例 × 配股价)
```

没有除权日时右侧恒为 1，所以「因子在不该动的日子动了」和「该动的日子没动」都会被抓到。
容差是重要性阈值不是等式检验（默认 50 bps，≥200 bps 升为 error），因为两个来源的取整方式不同。

---

## Step → 数据集映射

| Step 模块 | 数据集 |
|-----------|--------|
| reference.py | instruments, trading_calendar, trading_status |
| bars.py | daily_bars, index_bars |
| intraday.py | minute_bars, minute_bars_5m（可选；不在默认 wave 上，`cne run daily --group intraday`） |
| events.py | corporate_actions, announcement_index, earnings_disclosure_schedule |
| fundamentals.py | valuation_metrics, financial_statement_items |
| capital.py | fund_flow, northbound_*, margin_trading, dragon_tiger, block_trades |
| structure.py | sector_members, index_constituents, industry_members |
| macro_risk.py | macro_indicators, market_breadth, share_unlock_schedule, regulatory_events |
| commodity.py | commodity_bars |
| research.py | institutional_holdings, analyst_consensus, sentiment_scores |
| rotation.py | hot_rank, sector_bars, sector_fund_flow, news_headlines |
| newsboard.py | flash_news_wire, economic_calendar |
| delisted.py | 退市股发现 / repair（已有 bars → instruments） / 回填 → daily_bars, instruments, delisting_events |
| derive / finalize | adj_factors, industry_index, compact, audit |

---

## 相关文档

- [Schema](schema.md)
- [查询指南](query-guide.md)
- [逐源限制](sources.md)
- [steps 模块](../modules/steps.md)
