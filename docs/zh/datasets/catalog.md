---
description: CNEquity 数据集目录：A 股行情、财务、期货、资金流、公告事件、指数行业与宏观数据的来源、历史起点与采集语义。
---

# 数据集目录

本页回答两件事：湖里登记了哪些数据集，以及每个数据集从哪来、能补多远。先按下方 L0–L9 分层找到数据集，再看它的采集语义和历史限制；要复制查询代码，去[查询指南](query-guide.md)。

| 想查 | 去哪里 |
|------|--------|
| 字段、类型和单位 | [字段与单位](schema.md) |
| 每个来源的限制和更新频率 | [数据源与历史限制](sources.md) |
| 程序里查起点和历史模式 | `list_datasets()` 的 `coverage_start` / `coverage_end` / `history_mode` / `backfill_source` |

!!! note "登记不等于默认采集"
    - 默认关闭、需要显式开启：分钟线 `minute_bars` / `minute_bars_5m`（`[minute_bars]`）、分笔 `trade_ticks`（独立的 `[trade_ticks]`）、期货/期权四张主表 `futures_contracts`、`option_contracts`、`futures_bars`、`option_bars`（`[futures]`，不进 `cne init`，见[产品边界](../architecture/overview.md)）。
    - derived 数据由已入湖数据派生：`adj_factors`、`industry_index`、`futures_continuous`、`option_greeks`、`delisting_events`；`minute_bars_15m` / `30m` / `60m` 默认不计算，按需派生。
    - **on-demand** 数据集不进 curated 主路径。`flash_news_wire` 是新闻兼容读取，`economic_calendar` 是停用源占位。
    - 所以登记数既不是物理表数，也不是默认采集完成数。

**图例**（下表）：语义 `by_date` / `snapshot`；水位 ✓ = 维护 `meta/state` 水位。

## 数据分层

| 层次 | 说明 | 代表数据集 |
|------|------|------------|
| **L0** 基础参考 | Universe、ETF 目录、日历、交易状态 | instruments, etf_profiles, trading_calendar, trading_status |
| **L1** 行情 | 未复权价量 + 复权因子 + 可选分钟/分笔 + 退市形态 | daily_bars, index_bars, minute_bars*, minute_bars_5m*, minute_bars_15m* / 30m* / 60m*, trade_ticks*, adj_factors, delisting_events |
| **L2** 公司事件 | 除权除息、公告、预约披露 | corporate_actions, announcement_index, earnings_disclosure_schedule |
| **L3** 基本面 | 财报、估值、一致预期 | financial_statement_items, valuation_metrics, analyst_consensus |
| **L4** 资金面 | 北向、融资、主力 | fund_flow, fund_flow_ths, northbound_*, margin_trading, dragon_tiger, block_trades, institutional_holdings |
| **L5** 结构行业 | 板块、指数成分、行业 | sector_members, index_constituents, industry_members, industry_index |
| **L6** 宏观 | 利率、景气、货币 | macro_indicators, market_breadth |
| **L7** 舆情 / 轮动 | 新闻、情绪、板块、人气、事件流 | sentiment_scores, hot_rank, sector_bars, sector_fund_flow, sector_fund_flow_ths, news_headlines, flash_news_wire, economic_calendar*（stock_news 为 on-demand） |
| **L8** 风险合规 | 解禁、监管 | share_unlock_schedule, regulatory_events |
| **L9** 衍生品 | 期货/期权合约、逐合约行情、连续合约、希腊字母、分钟线，以及商品期货主连 | commodity_bars*, futures_contracts*, option_contracts*, futures_bars*, option_bars*, futures_continuous*, option_greeks*, futures_minute_bars* |

\*可选或 `required=false`：分钟线默认关；商品期货需显式回填；期货/期权四张表需 `[futures] enabled = true`；`economic_calendar` 东财源已下线，仅占位。

分层是**研究用途**，与存储 `layer` 正交：`adj_factors` / `delisting_events`（L1）和 `industry_index`（L5）落在 `derived/` 而非 `curated/`，但按用途归入各自层，所以没有单独的「派生」层。期货/期权按 [产品边界](../architecture/overview.md) 放在股票体系旁边，合约、行情与派生统一归 L9，面板和目录里作为一个整体查看。权威来源是 `DatasetSpec.tier`；`test_docs_catalog.py` 断言本文档与注册表逐层一致。

## 支持档位

档位说明项目对一个数据集承诺到什么程度，与上面的研究分层正交。权威来源是 `DatasetSpec.support`，`list_datasets()` 的 `support` / `enabled` 两列给出每个数据集的档位和本湖是否启用；`test_docs_catalog.py` 断言本表与注册表一致。

| 档位 | 默认 | 含义 | 数据集 |
|------|------|------|--------|
| `core` 核心 | 开 | 历史可重建、日更稳定、有备源或权威对照；`cne check` 和稳定性门槛（`cne verify --runs`）只看这一档，不论由哪个调度组采集 | `instruments`, `trading_calendar`, `trading_status`, `daily_bars`, `index_bars`, `corporate_actions`, `financial_statement_items`, `share_structure`, `valuation_metrics`, `index_constituents`, `adj_factors`, `delisting_events` |
| `extended` 扩展 | 开 | 历史可重建、日更基本稳定，但非必需或只有单一来源 | `etf_profiles`, `commodity_bars`, `announcement_index`, `earnings_disclosure_schedule`, `shareholder_counts`, `top_holders`, `margin_trading`, `dragon_tiger`, `block_trades`, `institutional_holdings`, `industry_members`, `macro_indicators`, `market_breadth`, `share_unlock_schedule`, `regulatory_events`, `industry_index` |
| `best_effort` 尽力而为 | **关** | 只有启用后才开始积累的实时快照、已停更的来源，或经常失败的接口（东财 push2 资金流、新闻、人气榜）；在 `[datasets] best_effort` 中列出才启用 | `analyst_consensus`, `fund_flow`, `northbound_holdings`, `northbound_flows`, `sector_members`, `sentiment_scores`, `hot_rank`, `sector_bars`, `sector_fund_flow`, `fund_flow_ths`, `sector_fund_flow_ths`, `news_headlines`, `flash_news_wire`, `economic_calendar` |
| `optional` 可选 | 关 | 由各自开关启用（`[minute_bars]`、`[trade_ticks]`、`[futures]`） | `minute_bars`, `minute_bars_5m`, `trade_ticks`, `futures_contracts`, `option_contracts`, `futures_bars`, `futures_minute_bars`, `option_bars`, `futures_continuous`, `option_greeks`, `minute_bars_15m`, `minute_bars_30m`, `minute_bars_60m` |

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
| `snapshot_only` | 只返回当前快照；可从启用后逐日积累，不能回补未采到的过去 | `analyst_consensus`、`fund_flow`、`sector_members`、`hot_rank`、`sector_fund_flow`、`news_headlines`、`flash_news_wire`、`economic_calendar` |

`trading_status` 的停牌覆盖可从 `daily_bars` 起点派生；ST 覆盖必须以完整的 `historical_st_evidence` 收据为准。没有覆盖请求窗口的收据时，**不要**假定 2001 起 `universe="all_a"` 已剔除历史 ST。BJ 可选用 Tushare Pro：2016 年通过 `bak_basic` 的历史简称、2017-01-01 起通过 `stock_st`；2016 年以前仍需独立的更深历史源。

### `trade_ticks` 是什么，不是什么

**不是逐笔成交。** A 股 Level-1 是**每 3 秒一帧的快照**，通达信的「分笔」是这个快照的聚合结果。

由此带来三条必须知道的口径：

| 项 | 实情 |
|----|------|
| 时间戳 | **只到分钟**，秒位恒为 `00`。不是被截断的，是协议从来没带过秒 |
| 主键 | 因此是 `(symbol, trade_date, tick_seq)`——同一分钟可以有 20 条记录时间戳完全相同 |
| `direction` | 通达信按 tick rule **推断**的方向，不是交易所字段；四个取值 `buy` / `sell` / `neutral` / `after_hours` |

`after_hours` 是 15:05–15:30 的盘后固定价格成交，价格恒等于当日最后成交价，且**不计入交易所当日成交量**——
与日频对账前必须先剔除它。

**没有 `amount` 列。** 源端不提供；`price × volume` 可以自己算，但要知道它是近似：
一帧里多笔不同价成交被合并成一个代表价。不保证这个近似值等于真实成交额。

合规边界见 [legal-and-data-sources](../legal-and-data-sources.md)：这**不是**交易所 Level-2，没有逐笔委托，没有十档。

### 历史视野：两种机制，不要混

`history_mode` 说的是**能不能**回补，这一节说的是**能回补多远**。源端的限制有两种，形状完全不同：

**（一）每标的固定根数**（`history_horizon_days`）——分钟线是这种。取值为「源端还提供多少个交易日」，随今天滚动。

| 数据集 | history_horizon_days | 窗口含义 |
|--------|---------------------|-------------------|
| minute_bars（1m） | **95** | 客户端默认回填窗口；实际日期受交易活跃度影响 |
| minute_bars_5m（5m） | **491** | 客户端默认回填窗口，约 2 年 |

**（二）固定日期底**（`history_floor_date`）——分笔和北向资金流是这种。**不随今天滚动**，所以视野是逐日**变长**的。

| 数据集 | history_floor_date | 边界含义 |
|--------|-------------------|-------------------|
| trade_ticks | **2024-01-02** | 客户端允许的最早请求日期，不代表每个标的完整覆盖 |
| northbound_flows | **2014-11-17** | 沪股通开通日；更早没有该资金流 feed |
| futures_bars | **2002-01-07** | 上期所文件最早可取日；郑商所 2010-01-04、中金所 2010-04-16、广期所 2022-12-22，各所起点由适配器截断 |
| option_bars | **2017-04-19** | 郑商所白糖期权首日；上期所 2018-09-21、中金所 2019-12-23、广期所 2022-12-23 |

两者的区别不是学术问题：把固定底写成滚动天数，`earliest_available()` 会每天往前漂，几个月后就把源端还愿意提供的数据挡在门外。

上游保留范围可能变化；出现范围错误时先检查版本与数据源限制。

`history_horizon_days` 与 `history_floor_date` 都为空，只表示注册表未声明统一历史边界，不能据此推断源端有无限历史。

这是**源的属性，不是本湖的待办**。更早的窗口返回的不是更少数据，而是没有数据，且没有回填源能补深——`by_date` 单独看会让人以为能回补十年。

**分钟线按每标的根数保留。** 活跃度较低的标的可能覆盖更长的日历时间；不要把默认窗口理解为所有标的的连续历史保证。

`cne backfill minute_bars --start` 早于视野会直接报错而不是扫一整天返回空。要拉冷门标的的深历史，先把 `[minute_bars].scope` 收窄成 watchlist。程序化读法：`list_datasets()` 的 `history_horizon_days` 列。

`cne backfill trade_ticks --start` 同样会拦，但文案不同：分笔的底对所有标的一致，**没有哪个更窄的范围能拉到更早的数据**。

### `trade_ticks` 的容量

分笔按标的和交易日分页；请求数、行数及落盘量随标的活跃度和历史范围变化。并发连接可以减少网络空等，但不会提高共享限速器允许的请求起点速率。先用小范围观察批次进度与实际分区大小，再决定 watchlist 和回溯窗口；不要用单个出口的吞吐估计全市场完成时间。

配置里 `[trade_ticks].max_symbols` 默认 200 就是为此：`index:000300.SH` 解析出约 300 只会直接报错，
要跑得自己把上限调高——这一步摩擦是故意的。`scope = "all"` 不支持，配置校验期就会拒绝。

### 15m / 30m / 60m：默认不计算，可按需入湖

15m / 30m / 60m 不从源端抓取，而是由湖里的 1m 和 5m 重采样得到。默认不计算，也不进日更；查询时可以直接现算：

```python
from cnequity.query import load, resample_minute_history

window = dict(start="2026-07-01", symbols=["600519.SH"])
bars_15m = resample_minute_history(load("minute_bars", **window),
                                   load("minute_bars_5m", **window), "15m")
```

需要用 SQL、HTTP 接口或 MCP 直接读取时，把它算进湖里：

```bash
cne derive minute_bars_15m                                    # 只算还没算过或输入已更新的交易日
cne derive minute_bars_60m --start 2025-01-02 --end 2025-12-31
```

入湖后与其他数据集一样用 `load("minute_bars_15m", adjust="hfq")` 读取。计算规则、网页面板入口和读取途径见 [15 / 30 / 60 分钟线](../recipes/minute-bars-15-30-60.md)。上午和下午分别从 09:30、13:00 对齐；从 5m 算出的 K 线与从 1m 算出的口径差别见[查询指南](query-guide.md#成交口径的分钟重采样)。

## 需要 API Key 的覆盖区间

同花顺官方 API（`ths_official`）是**可选源**：没有 Key 的湖保持原有来源，日更不受影响
（见 [THS 接入](../getting-started/configuration.md#ths-官方接口)）。启用后可按许可与实际覆盖补
财报空缺和历史日线，`source` 列标明来源。服务端历史下界、字段完整性与 PIT
证据仍须按自己的查询窗口验收，不能把另一湖的行数或区间当作本湖承诺。

以下能力只写 `meta/source_snapshots`，**不进 curated**，仅供 `cne audit` 的仲裁检查使用：
`adj_factor_arbitration`、`daily_bars_arbitration`、`financial_statement_peer`。
无快照时它们静默，不影响其余检查。

THS 官方估值快照只能从启用后按日积累，不能用旧日期重放伪造观察。
这不等于 `valuation_metrics` 完全没有其他历史来源；不同来源的覆盖与口径须分开核验。

完整背景见 [THS 接入](../getting-started/configuration.md#ths-官方接口)。

## 溯源列（所有 curated 行）

| 列 | 类型 | 说明 |
|----|------|------|
| `source` | string | 数据源标识 |
| `data_version` | string | 源版本/批次 |
| `fetched_at` | timestamp[us, UTC] | 抓取时间 |

带 `announce_date` 的 PIT 数据集（`load(..., as_of=)`）：`financial_statement_items`、`announcement_index`。

按需数据集（`[on_demand].datasets`）：默认 `stock_news`、`research_reports`（已实现）。`announcement_body` / `financial_reports` 尚未实现。访问：`cne query --dataset <name> --symbol <code>.SH`。

注册表源码：`domain/datasets.py`（`DatasetSpec`）、`domain/schemas.py`（Polars dtype / `PRIMARY_KEYS`）；`test_dataset_registry.py` 断言同步。

## L0 基础参考

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| instruments | —（单文件 merge） | symbol | by_date | — | tdx_protocol | EM 分别从 A 股与 ETF/LOF clist 补 list_date；已发布交易所 ETF 目录补基金缺失上市日；baostock 回填退市股（`cne backfill instruments`）；merge 保留退市 |
| etf_profiles | as_of_date（按年） | symbol, as_of_date | snapshot | — | exchange | 上交所 ETF 细分类、深交所 ETF/基金目录及逐代码核验的官方指数方案；仅有充分境内股票指数证据的记录进入研究池。未知类别保留 unverified，不能回填未观测的历史快照 |
| trading_calendar | trade_date | trade_date | by_date | ✓ | tdx_protocol | 备源交易所 CSV；种子 2016–2027 |
| trading_status | trade_date（按月） | symbol, trade_date | by_date | ✓ | eastmoney | baostock ST 回填；派生停牌写月分区。`status`（normal/suspended/**delisted**）与 `risk_warning`（ST/*ST）是两列——旧版单列会让停牌冲掉 ST 标记；退市行由 `instruments` 判定并标 `derived_delisted`。旧湖读取自动兼容，物理迁移见 [schema](schema.md#trading_status) |

北交所日常证券目录会尝试用巨潮当日 IPO 日历的明确上市事件补上市日期；申购、招股或预告日期不能代替上市日期。来源不可用时保留目录和未知日期，并记录警告；已有上市日期在合并时保留。

证券目录每天只为湖里还没有上市日期的证券向 Baostock、东财和交易所查询；已有的日期（包括人工修复的）直接沿用。各来源都给不出日期的证券，7 天内不再重复查询，新上市证券仍每天查询直到有日期为止。

## L1 行情

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| daily_bars | trade_date | symbol, trade_date | by_date | ✓ | tdx_protocol | tip 缺口东财 clist 路由进 curated；多日 kline；BJ→sina；显式开启后只追加正式目录已核验 ETF，其他基金不进入默认日更；snapshot 仍留 audit |
| index_bars | trade_date | symbol, trade_date, frequency | by_date | ✓ | tdx_protocol | |
| minute_bars | trade_date | symbol, trade_date, bar_time, frequency | by_date | ✓ | tdx_protocol | 1m。**可选**，默认关；`[minute_bars]` 配置范围；**源端只有 95 个交易日**（见下「历史视野」）；落盘量随标的数与窗口增长；required=false |
| minute_bars_5m | trade_date | symbol, trade_date, bar_time, frequency | by_date | ✓ | tdx_protocol | 5m。同上可选；**491 个交易日（约 2 年），是唯一有真历史的日内频率**；落盘量随标的数与窗口增长；required=false |
| minute_bars_15m | trade_date | symbol, trade_date, bar_time, frequency | derived | ✓ | derived | 15m。**默认不计算**，`cne derive minute_bars_15m` 手动入湖；某只股票某天有 1m 用 1m，否则用 5m，`resampled_from` 标明来源；required=false |
| minute_bars_30m | trade_date | symbol, trade_date, bar_time, frequency | derived | ✓ | derived | 30m。同上 |
| minute_bars_60m | trade_date | symbol, trade_date, bar_time, frequency | derived | ✓ | derived | 60m。同上 |
| trade_ticks | trade_date | symbol, trade_date, tick_seq | by_date | ✓ | tdx_protocol | 分笔。**可选**，默认关；`[trade_ticks]` 独立配置；**不是逐笔成交**（见下）；源端回溯至 **2024-01-02**；落盘量随 watchlist 与窗口增长；required=false |
| adj_factors | trade_date | symbol, trade_date, adjust_type | derived | ✓ | sina | 仅 hfq；股票读 `f`、ETF/LOF 读 `s`；`cne derive adj_factors` |
| delisting_events | —（单文件 merge） | symbol | derived | — | derived | 每只退市股的结尾形态；补到的 bars 来自 sina；`cne backfill daily_bars --profile delisted` 产出 |

两个日内数据集共用一组质量检查：主键重复（通用 `pk_unique`）、时段外 bar、`trade_date` 与 `bar_time` 不一致、会话缺口，以及**与日频的成交量+成交额双向对账**。

## L2 公司事件

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| corporate_actions | ex_date（按年） | symbol, ex_date, action_type | by_date | ✓ | eastmoney（日更） | 回填：tdx_protocol；混粒度用 `cne repair repartition` |
| announcement_index | announce_date | announcement_id | by_date PIT | ✓ | cninfo | `as_of` 过滤 |
| earnings_disclosure_schedule | report_period | symbol, report_period | by_date | — | eastmoney | 预约披露时间表（RPT_PUBLIC_BS_APPOIN）；现值语义非 PIT：变更覆盖 scheduled_date（first_scheduled_date 保留首约，actual_date 披露后回填）；`cne backfill` 走 2016 起全报告期 |

## L3 基本面

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| financial_statement_items | report_period | symbol, report_period, statement_type, item_code | by_date PIT | — | eastmoney | 按报告期分区；`cne backfill` 默认自 2001 起（`--start`/`--end` 分块）；日更保留实际公告版本并补明确标记的原始披露日重建版本；baostock 不用于 FSI；备源 tdx_fin（东财失败时的日更路由与缺口回填） |
| valuation_metrics | trade_date | symbol, trade_date | snapshot | ✓ | eastmoney | 回填：baostock |
| analyst_consensus | forecast_date | symbol, forecast_date | snapshot | ✓ | eastmoney | |
| share_structure | change_date | symbol, change_date, announce_date | by_date PIT | — | eastmoney | 总股本/流通/限售/自由流通。**按变动日期扫，不是按报告期**：END_DATE 是股本变动日，不能只请求季末日期 |
| shareholder_counts | count_date | symbol, count_date, announce_date | by_date PIT | — | eastmoney | 股东户数与户均持股，筹码集中度输入。**旬末/月末也披露**：不能只按季末日期筛选 |
| top_holders | record_date | symbol, record_date, holder_scope, holder_rank, holder_name, announce_date | by_date PIT | — | eastmoney | 一张表两个口径：`holder_scope=total`（前十大股东）/ `float`（前十大流通股东）。披露日期不一定落在季末 |

财报 DMSK 的 `NOTICE_DATE` 可能是比较期数据的再公告日。原始披露日从 LICO 解析；缺少对应 LICO 记录时保留较晚日期，不猜测原始日期。日更保留再公告版本，并仅为缺少原始日期版本的科目追加 `source=eastmoney_backfill` 重建值。重建数值可能经过重述：默认 `load(..., as_of=...)` 的 `best_effort` 可读，但返回 `pit_is_exact=false`；`pit_mode="strict"` 排除重建值且要求实际观察时间不晚于查询日期。

当前版本采集时已按原始公告日入库；0.17 及更早版本写入的数据由 `cne migrate` 修正（迁移 `financial-announcements`）。它只修复有原始公告日期证据的已有科目；缺少的历史科目仍需 `cne backfill financial_statement_items --start <date> --end <date>` 按报告期补采。来源、科目及报告期的覆盖分别检查，不能由某一个报表齐全推断所有明细齐全。

估值源以零表示缺失的 PE、PB、PS 和总/流通市值统一写空值；负估值比率仍保留。0.17 及更早版本写入的旧数据由 `cne migrate` 修正。

`cne repair financial-originals --symbols <股票代码> --start-year 2016 --end-year 2024 --refresh-sources` 从巨潮按证券和披露日期完整枚举公告，定位最早的原始年报，归档 PDF 并提取明确可识别的合并报表当期科目；默认预览，加 `--apply` 追加 `source=cninfo_original` 版本。`--max-reports` 限制批量大小，重复运行跳过已含原文科目的报告；解析器升级后可用 `--reparse` 离线重放旧归档，追加新识别科目，内容及披露时间未变的行保留首次观测时间。`--all-stocks` 可替代 `--symbols`，按湖中股票目录分批；用输出的 `next_cursor` 传入 `--after` 接续，避免失败报告阻塞后续股票。不带游标重试仍会检查未恢复的失败项。比较期、母公司报表、修订版、扫描件及无法确定列头或单位的数值不用于补行。`--quarter` 选择 Q1、Q2（半年报）、Q3 或默认 Q4（年报），分别定位和归档原始报告。利润及现金流只恢复年初至报告期末累计数；第三季四列表只在列序明确且四个单元格齐全时取累计列，不用单季数填累计数。在线采证不依赖标题关键词或公告分类来判断最早披露；分页以公告记录总数为准，必须包含尾页并核对唯一公告数。无法完整枚举的窗口保留失败项。合并与公司混排的年报四列表，还须确认“合并、合并、公司、公司”的角色顺序和两组当年/上年日期；只取当年合并列，费用符号约定尚不明确的科目继续留缺口。拆成资产/负债两段的主表及期末、重述期初、上年末三列，也必须按明确日期顺序读取。表头只写“元”时，只有原文同时明确本位币及外币报表统一折成人民币的呈列政策，才可确定人民币单位；不能仅凭本位币猜测。当前支持有限科目和明确布局，不等于全市场全部季度或明细科目已恢复。原文披露时间和真实采集时间分别保存，今天取得的历史原文不会成为历史严格 PIT 的已观察证据；同一原始公告日的原文数值优先于后采集的重建值。

## L4 资金面

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | staleness |
|--------|--------|------|------|------|------|-----------|
| fund_flow | trade_date | symbol, trade_date | snapshot | ✓ | eastmoney | 1d |
| fund_flow_ths | trade_date | symbol, trade_date | snapshot | ✓ | ths | 只在 push2 取不到 `fund_flow` 时由同花顺补：流入/流出/净额/成交额，**没有**主力与超大单…小单拆分，金额 4 位有效数字；源覆盖以响应为准，不承诺北交所；为空不报。默认不判新鲜度；配置的日更调度组里排了它（例如 push2 不可用时用 `steps_add` 换入），就要求最近收盘交易日有数据 |
| margin_trading | trade_date | symbol, trade_date | by_date | ✓ | exchange | 沪深交易所自行编制的融资融券明细；SH 无融券余额（`short_balance` 为 null），深交所晚一个交易日发布、两边齐了才写；`[margin_trading] source` 可切回 eastmoney |
| northbound_holdings | trade_date | symbol, trade_date, channel | by_date | ✓ | eastmoney | 100d（季频） |
| northbound_flows | trade_date | trade_date, channel | by_date | ✓ | eastmoney | 2d |
| dragon_tiger | trade_date | symbol, trade_date, reason | by_date | ✓ | eastmoney | 1d；备源见下 |
| block_trades | trade_date | symbol, trade_date, price, volume | by_date | ✓ | eastmoney | 1d；备源见下 |
| institutional_holdings | report_period | symbol, holder_type, report_period | by_date | — | eastmoney | — |

`dragon_tiger` 与 `block_trades` 有交易所备源：深交所 `ShowReport` 的
`CATALOGID=1265`（龙虎榜）与 `1842_xxpl_after`（大宗交易），上交所对应 `1902`。

**是备源，不是换源。** 按 [产品边界](../architecture/overview.md)，东财仍是 `primary_source`，
交易所是 `backup_source`；**东财能答的时候备源根本不会被问**。failover 落在 fetch 层，所以 provenance、
水位和 compact 都不需要知道这次走的是哪条路。

**两个交易所都不发布北交所**，这一点记在 `backup_gaps` 里，而不是让它看起来像是被覆盖了。
`share_unlock_schedule` 没有给备源：深交所登记的是**已发生**的解禁，而这个数据集是前瞻日历，两者不是同一件事。

## L5 结构行业

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 |
|--------|--------|------|------|------|------|
| sector_members | as_of_date | symbol, sector_code, as_of_date | snapshot | ✓ | eastmoney |
| index_constituents | as_of_date | index_symbol, symbol, as_of_date | snapshot | ✓ | exchange（中证文件）/ cni；备源 eastmoney |
| industry_members | as_of_date | symbol, classification_system, as_of_date | snapshot | ✓ | eastmoney |
| industry_index | trade_date（按年） | trade_date, industry_code, level, weighting | derived | ✓ | derived (industry_members × hfq daily_bars) |

`industry_index` 归 L5 而非 L1：观测单位是行业而不是标的，且由本层的成员关系算出，指数与成分不会打架。`cne derive industry_index` 重算。

快照类仅积累「每日一份成员关系」，历史分位数需多日分区累积。

历史回填（C2）：`cne backfill industry_members` = 申万 SwClass2021 月度（`classification_system=sw`，2020 起）；
`cne backfill index_constituents` = 国证调样史（399001/399006，约 2021-12 起）。沪深 300、科创 50 等其他指数依赖采集后积累的 EM 快照及下面的官方带日期文件；更早历史仍可能缺失。当前名单不能回填到更早日期，否则会产生幸存者偏差；EM 适配器只接受当前已收盘交易日。历史读取只使用当时已存的快照，早于最早快照的成员关系未知。

日更（自本版起）：上证指数、沪深 300、科创 50 取中证每日成分文件（来源记为 `exchange`），深证成指、创业板指按国证调样区间推出当日成分（`cni`）；中证月末权重文件与国证权重文件按其自身日期另存。某指数的官方文件当日取不到时，只为该指数用东财补当天快照，并在审计里报 `index_constituents_official_unavailable`。东财成分每天另存为对照快照，审计 `index_constituents_peer` 列出两边各自独有的成员（东财不含 B 股）。同一指数、同一天只读一个来源：有官方成分时忽略东财的行；7 月以来已存的东财快照原样保留，只在当天没有官方成分时被读取。

EM 和国证调样史都不提供成分权重，`weight=null`，不能把它当成真实零权重或自动推断为等权。每个指数、来源的快照起止与数量可直接查询：`cne query --sql "SELECT index_symbol, source, min(as_of_date), max(as_of_date), count(DISTINCT as_of_date) FROM index_constituents GROUP BY ALL ORDER BY ALL"`。0.17 及更早版本留下的权重占位零由 `cne migrate` 改为空值，不制造缺失的历史成分或权重。

`cne repair index-snapshots --refresh-sources` 取得沪深 300、科创 50、深证成指、创业板指可公开下载的官方文件；默认预览，`--apply` 按文件实际日期替换该指数的完整快照并保留旧版本。权重文件的百分数转换为比例（总和约为 1），来源为 `exchange` 或 `cni_weights`。成分文件和权重文件可能日期不同，不跨日期填权重；更晚的无权重观察也不能抹去同日已验证权重。这条路径只能恢复官方当前仍提供的带日期文件及已归档快照，不提供任意历史日期的完整权重，也不会把最新样本倒推到历史。

`cne repair index-history --refresh-sources` 补采已核验公告目录中尚未归档的官方附件；之后默认离线回放，`--apply` 更新所选指数的成分历史。默认深证成指（399001.SZ）覆盖 2015-05-20 至 2026-09-30；`--index-symbol 000688.SH` 选择科创50，覆盖正式发布日 2020-07-23 至 2026-09-30，不将回溯基日当成实时成分起点。科创50收盘后调样从下一个已知交易日生效，明确无变动公告也须核验。每次实际生效日写完整名单，替换区间内旧的无权重观察，避免旧月末快照继续遮住临时调整。固定日期的官方终点校验名单随包保存，并标明原始文件地址及摘要；它不是新抓取的来源响应。公告及日期证据绑定原字节摘要，附件缺失、内容变化、转换矛盾或与既有官方权重名单冲突时停止更新。历史权重未知时为空，同日已验证的完整官方权重保留。此目录不覆盖其他指数或区间之外的历史；不得将当前名单、当前权重或基金持仓倒推成历史。

`load("index_constituents", as_of="YYYY-MM-DD")` 对每个指数返回截止该日最近的一整份已存快照；较早快照中已被剔除的成员不会逐证券沿用，最早快照之前返回空表。用 `start`/`end` 读取多日快照序列。

EM 响应若已包含尚未生效的调样行，会拒绝整份快照：删除未来新增行无法恢复已经从当前名单中消失的旧成员。调样生效后重试采集。

## L6 宏观

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| macro_indicators | obs_date | indicator_id, obs_date | by_date | ✓ | eastmoney / pboc（社融） | |
| market_breadth | trade_date | trade_date, metric_id | by_date | ✓ | derived (daily_bars) | |

## L7 舆情 / 轮动

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| sentiment_scores | trade_date | symbol, trade_date, score_channel | by_date | ✓ | derived | |
| hot_rank | trade_date | symbol, trade_date | snapshot | ✓ | eastmoney | 人气榜 top100（公开接口上限） |
| sector_bars | trade_date | sector_code, trade_date | snapshot | ✓ | ths | 日更与回填都走同花顺 board-kline；无第二源 |
| sector_fund_flow | trade_date | sector_code, trade_date | snapshot | ✓ | eastmoney | 板块主力净流入 |
| sector_fund_flow_ths | trade_date | board_type, sector_code, trade_date | snapshot | ✓ | ths | 只在 push2 取不到 `sector_fund_flow` 时由同花顺补：行业（881xxx）+ 概念板块的流入/流出/净额，金额单位亿、精确到 0.01；同花顺板块分类，和东财板块代码不对应。新鲜度规则同 `fund_flow_ths` |
| news_headlines | publish_date | news_id | snapshot | ✓ | eastmoney | 新闻标题 |
| flash_news_wire | publish_date | wire_id, wire_source | 兼容读取 | ✓ | eastmoney | 由 `news_headlines` 事实表投影，兼容旧 revision 的实体文件；不再重复写入 |
| economic_calendar | event_date（按年） | event_id | snapshot | ✓ | —（源已下线） | EM `RPT_ECONOMICCALENDAR` 已退役（code 9501），保留 schema 等替代源；`required=false`，空表不判 UNHEALTHY |

`sector_bars` 使用同花顺板块行情；历史通过 `cne backfill sector_bars` 分窗口补入。先用 `--plan` 检查范围；失败后重跑同一命令即从断点续跑，避免无故 `--refetch` 全量重抓。网络可达性以当前出口的小范围探测为准。

## L8 风险合规

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 |
|--------|--------|------|------|------|------|
| share_unlock_schedule | unlock_date | symbol, unlock_date | by_date | ✓ | eastmoney |
| regulatory_events | event_date | event_id | by_date | ✓ | cninfo（派生自 announcement_index） |

## L9 衍生品

期货与期权的合约、逐合约行情和派生序列，以及早先接入的商品期货主连。全部 `required=false`；除 `commodity_bars` 外由 `[futures]` 开启，不进 `cne init`。

| 数据集 | 分区键 | 主键 | 语义 | 水位 | 主源 | 备注 |
|--------|--------|------|------|------|------|------|
| commodity_bars | trade_date | symbol, trade_date | by_date | ✓ | sina | 备源 eastmoney；国内主连 + COMEX金 `GC0.CMX`；`cne backfill commodity_bars`；required=false |
| futures_contracts | —（单文件 merge） | symbol | by_date | — | futures_exchange | 期货合约表：上市/最后交易日只取权威参考；首末观测日独立保存，缺行情不等于到期；`dates_basis` 标注来源；由 futures_bars 重建；required=false |
| option_contracts | —（单文件 merge） | symbol | by_date | — | futures_exchange | 期权合约表：标的、行权价、看涨/看跌、行权方式、到期日；中金所期权标的为指数（IO→000300.SH）；required=false |
| futures_bars | trade_date（按月） | symbol, trade_date | by_date | ✓ | futures_exchange | 逐合约日线，含结算价、持仓；**单边口径**（2020 年前上期/能源/大商/郑商的双边值已折半）；成交额为元；未成交行 OHLC 为空；多交易所各自为故障域，缺一家照写其余、三日回看加持久欠账重试；读上期所（含能源，2002 起）、郑商所（2010 起）、广期所（2022-12 起）、中金所（2010-04-16 起），大商所见 sources.md；required=false |
| option_bars | trade_date | symbol, trade_date | by_date | ✓ | futures_exchange | 逐合约期权日线，含结算价、持仓、行权量与交易所发布的 Delta/IV；到期当天虚值合约结算价按 0 入湖；上期/能源 2018-09 起、郑商所 2017-04-19 起、广期所 2022-12-23 起、中金所 IO/MO/HO 2019-12-23 起；required=false |
| futures_continuous | trade_date（按月） | symbol, series, trade_date | derived | ✓ | derived | 主力/次主力连续合约：T 日用哪个合约只看 T-1 收盘持仓，只向后换月，临近最后交易日 5 天（或进入交割月）即让位；`adj_ratio`/`adj_diff` 累计换月因子；`roll_yield` 为主力对次主力年化展期收益；`cne derive futures_continuous`；required=false |
| option_greeks | trade_date | symbol, trade_date | derived | ✓ | derived | 本湖自己的期权 IV 与希腊字母：结算价反解，欧式 Black-76、美式 BAW；商品期权标的取同日期货结算价，中金所指数期权用平价倒推远期；利率 shibor_3m，缺失用 2% 并在 `rate_source` 标注；`status` 说明无解原因（到期当天为 `expiry_day`）；`cne derive option_greeks` 检测行情/合约/利率/模型内容依赖，自动重算失效日期；required=false |
| futures_minute_bars | trade_date | symbol, bar_time | by_date | ✓ | sina | 期货 1 分钟线，**只对 watchlist**（`[futures] minute_*`）；新浪每合约只留最近 1023 根（夜盘品种约 2 个交易日），只能从开启日往后积累，必须每个交易日跑；夜盘 bar 归下一交易日；required=false |

## 主备配置（Failover → meta/source_snapshots）

| 数据集 | 主源 | 备源 |
|--------|------|------|
| daily_bars | tdx_protocol | eastmoney |
| corporate_actions | eastmoney | tdx_protocol |

## 对发布方的核对（authority checks）

主备比对的是两个转发方：它们一致只能说明两者不冲突，不能说明谁对。以下检查越过转发方，直接对上游发布机构，
写入 `meta/quality/source_diffs/authority-<date>.json`，只报不拦（见 [产品边界](../architecture/overview.md)）。

| 检查 | 数据集 | 对照方 |
|------|--------|--------|
| `macro_pmi_vs_nbs` | macro_indicators | 国家统计局 PMI 发布稿 |
| `st_labels_vs_exchange` | trading_status | 沪深交易所证券列表简称 |
| `daily_bars_vs_exchange` | daily_bars | 沪深交易所自身发布的收盘行情 |
| `adj_factor_corporate_action_divergence` | adj_factors | 由 `corporate_actions` 独立重算的复权因子步长 |
| `adj_factor_pre_close_divergence` | adj_factors | 交易所公布的前收盘（`daily_bars.pre_close`）给出的步长 `前一交易日收盘 ÷ pre_close` |

价格与成交额使用各自的容差。成交额还按偏差标的占比判定，避免统计范围差异造成逐只误报。交易所停牌零成交记录不作为行情缺口；`daily_bars_missing_vs_exchange` 只统计有成交的标的。

`adj_factors` 的重算基于除权除息日的连续性恒等式：

```
f_ex / f_prev = (1 + 送股 + 转股 + 配股) × 前收 / (前收 − 税前现金分红 + 配股比例 × 配股价)
```

没有除权日时右侧恒为 1，所以「因子在不该动的日子动了」和「该动的日子没动」都会被抓到。
容差是重要性阈值不是等式检验（默认 50 bps，≥200 bps 升为 error），因为两个来源的取整方式不同。
