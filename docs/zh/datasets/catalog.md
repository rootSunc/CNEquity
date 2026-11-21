# 数据与覆盖

数据按研究用途分为 L0–L9。下表列出注册表的支持档位、主来源和历史能力。实际采集由你的配置、权限和来源可用性决定。

## 先核对覆盖 {#coverage}

| 历史类型 | 含义 |
|---|---|
| `by_date` | 支持按历史日期获取，实际日期范围仍受源端与客户端限制。派生数据还依赖输入数据。 |
| `snapshot_with_backfill` | 日常采集当前快照，历史使用独立回填来源。两条路径可能有不同覆盖与口径。 |
| `snapshot_only` | 只能积累已经观测的快照。若表中另列带凭证的回填源，启用对应来源并配置有效凭证后才有该历史路径。 |

在自己的湖运行 `cne status --datasets --all-columns`，或在 Python 使用 `list_datasets()`，确认启用状态、`history_mode` 和覆盖范围。最早与最晚日期之间可能仍有缺口。表中的起点和交易日数是注册表请求边界。

`core` 是核心验收范围，`extended` 是扩展数据，`best_effort` 按配置启用，`optional` 由使用场景决定。查本机启用情况时以配置和状态为准。源的使用条件见[许可与来源](../legal-and-data-sources.md)。

## 数据契约 {#contract}

```bash
cne contract show daily_bars
cne contract show --out dataset-contract.json
cne contract diff old-contract.json dataset-contract.json
```

契约记录主键、列类型、单位、版本和历史语义，随版本保存在 [contracts](https://github.com/rootSunc/CNEquity/tree/main/contracts)。字段解释见[字段与单位](schema.md)。数据行的溯源列包括 `source`、`data_version`、`fetched_at`，时点读取和证据要求见[查询与研究](query-guide.md#pit)。

## 分组目录 {#数据分层}

<!-- dataset-catalog:start -->

## L0 基础参考

| 数据集 | 支持 | 主来源 | 历史类型 | 回填来源 | 目录请求边界 |
|---|---|---|---|---|---|
| instruments | core | tdx_protocol | by_date | baostock | — |
| name_history | extended | tushare | by_date | — | — |
| convertible_bonds | best_effort | tushare | by_date | — | — |
| convertible_bond_coupons | best_effort | tushare | by_date | — | — |
| executives | best_effort | tushare | by_date | — | 1990-01-01 |
| etf_profiles | extended | exchange | snapshot_only | — | — |
| trading_calendar | core | tdx_protocol | by_date | — | — |
| trading_status | core | exchange | snapshot_with_backfill | baostock | — |

## L1 行情

| 数据集 | 支持 | 主来源 | 历史类型 | 回填来源 | 目录请求边界 |
|---|---|---|---|---|---|
| convertible_bond_bars | best_effort | tushare | by_date | — | 2004-01-02 |
| limit_prices | best_effort | tushare | by_date | — | 2007-01-04 |
| daily_bars | core | tdx_protocol | by_date | — | — |
| index_bars | core | tdx_protocol | by_date | — | — |
| minute_bars | optional | tdx_protocol | by_date | — | 95 个交易日 |
| minute_bars_5m | optional | tdx_protocol | by_date | — | 491 个交易日 |
| trade_ticks | optional | tdx_protocol | by_date | — | 2024-01-02 |
| adj_factors | core | sina | by_date | — | — |
| minute_bars_15m | optional | derived | by_date | — | — |
| minute_bars_30m | optional | derived | by_date | — | — |
| minute_bars_60m | optional | derived | by_date | — | — |
| delisting_events | core | derived | by_date | — | — |

## L2 公司事件

| 数据集 | 支持 | 主来源 | 历史类型 | 回填来源 | 目录请求边界 |
|---|---|---|---|---|---|
| holder_trades | best_effort | tushare | by_date | — | 2006-01-01 |
| share_repurchases | best_effort | tushare | by_date | — | 2005-01-01 |
| earnings_forecasts | best_effort | tushare | by_date | — | 2004-01-01 |
| investor_visits | best_effort | tushare | by_date | — | 2021-08-01 |
| convertible_bond_calls | best_effort | tushare | by_date | — | — |
| convertible_bond_conversions | best_effort | tushare | by_date | — | 2000-01-01 |
| corporate_actions | core | tdx_protocol | by_date | tdx_protocol | — |
| announcement_index | extended | cninfo | by_date | — | — |
| earnings_disclosure_schedule | extended | eastmoney | by_date | — | — |

## L3 基本面

| 数据集 | 支持 | 主来源 | 历史类型 | 回填来源 | 目录请求边界 |
|---|---|---|---|---|---|
| business_segments | best_effort | tushare | by_date | — | 2004-03-31 |
| executive_compensation | best_effort | tushare | by_date | — | 1990-01-01 |
| financial_statement_items | core | eastmoney | by_date | — | — |
| share_structure | core | eastmoney | by_date | — | 1990-01-01 |
| shareholder_counts | extended | eastmoney | by_date | — | 1992-01-01 |
| top_holders | extended | eastmoney | by_date | — | 2003-01-01 |
| valuation_metrics | core | eastmoney | snapshot_with_backfill | baostock | — |
| analyst_consensus | best_effort | eastmoney | snapshot_only | — | — |
| broker_forecasts | best_effort | tushare | by_date | — | 2016-01-01 |

## L4 资金面

| 数据集 | 支持 | 主来源 | 历史类型 | 回填来源 | 目录请求边界 |
|---|---|---|---|---|---|
| fund_flow_tushare | best_effort | tushare | by_date | — | 2010-01-04 |
| chip_distribution | best_effort | tushare | by_date | — | 2018-01-02 |
| northbound_top10 | best_effort | tushare | by_date | — | 2014-11-17 |
| limit_events | best_effort | tushare | by_date | — | 2020-01-02 |
| hot_money_traders | best_effort | tushare | by_date | — | — |
| hot_money_trades | best_effort | tushare | by_date | — | 2022-08-01 |
| limit_ladder | best_effort | tushare | by_date | — | 2023-10-01 |
| kpl_limit_ups | best_effort | tushare | by_date | — | 2017-10-01 |
| fund_flow_ths_tushare | best_effort | tushare | by_date | — | 2024-10-01 |
| fund_flow | best_effort | eastmoney | snapshot_only | tushare (API key) | 2023-09-11 |
| margin_trading | extended | exchange | by_date | — | 2010-03-31 |
| northbound_holdings | best_effort | eastmoney | by_date | — | — |
| northbound_flows | best_effort | eastmoney | by_date | — | 2014-11-17 |
| dragon_tiger | extended | eastmoney | by_date | — | — |
| block_trades | extended | eastmoney | by_date | — | — |
| institutional_holdings | extended | eastmoney | by_date | — | — |
| fund_flow_ths | best_effort | ths | snapshot_only | — | — |

## L5 结构行业

| 数据集 | 支持 | 主来源 | 历史类型 | 回填来源 | 目录请求边界 |
|---|---|---|---|---|---|
| sector_members | best_effort | eastmoney | snapshot_only | — | — |
| index_constituents | core | exchange | snapshot_with_backfill | cni | — |
| industry_members | extended | eastmoney | snapshot_with_backfill | sw | — |
| industry_index | extended | derived | by_date | — | — |

## L6 宏观

| 数据集 | 支持 | 主来源 | 历史类型 | 回填来源 | 目录请求边界 |
|---|---|---|---|---|---|
| macro_indicators | extended | eastmoney | by_date | — | — |
| market_breadth | extended | derived | by_date | — | — |

## L7 舆情 / 轮动

| 数据集 | 支持 | 主来源 | 历史类型 | 回填来源 | 目录请求边界 |
|---|---|---|---|---|---|
| popularity_ranks | best_effort | tushare | by_date | — | 2023-10-01 |
| strongest_sectors | best_effort | tushare | by_date | — | 2023-10-01 |
| investor_qa | best_effort | tushare | by_date | — | 2022-01-01 |
| sentiment_scores | best_effort | derived | by_date | — | — |
| hot_rank | best_effort | eastmoney | snapshot_only | — | — |
| sector_bars | best_effort | ths | snapshot_with_backfill | ths | — |
| sector_fund_flow | best_effort | eastmoney | snapshot_only | tushare (API key) | 2023-09-12 |
| sector_fund_flow_ths | best_effort | ths | snapshot_only | — | — |
| news_headlines | best_effort | eastmoney | snapshot_only | — | — |
| flash_news_wire | best_effort | eastmoney | snapshot_only | — | — |
| economic_calendar | best_effort | jin10 | snapshot_with_backfill | jin10 | — |
| economic_events | best_effort | jin10 | snapshot_only | — | — |
| market_holidays | best_effort | jin10 | snapshot_only | — | — |

## L8 风险合规

| 数据集 | 支持 | 主来源 | 历史类型 | 回填来源 | 目录请求边界 |
|---|---|---|---|---|---|
| pledge_stats | best_effort | tushare | by_date | — | 2014-03-07 |
| share_unlock_schedule | extended | eastmoney | snapshot_with_backfill | eastmoney | — |
| regulatory_events | extended | cninfo | by_date | — | — |

## L9 衍生品

| 数据集 | 支持 | 主来源 | 历史类型 | 回填来源 | 目录请求边界 |
|---|---|---|---|---|---|
| futures_member_positions | best_effort | tushare | by_date | — | 2010-01-04 |
| commodity_bars | extended | sina | by_date | eastmoney_kline+sina_global | — |
| futures_contracts | optional | futures_exchange | by_date | — | — |
| option_contracts | optional | futures_exchange | by_date | — | — |
| futures_bars | optional | futures_exchange | by_date | — | 2002-01-07 |
| futures_minute_bars | optional | sina | by_date | — | 2 个交易日 |
| option_bars | optional | futures_exchange | by_date | — | 2017-04-19 |
| futures_continuous | optional | derived | by_date | — | — |
| option_greeks | optional | derived | by_date | — | — |

<!-- dataset-catalog:end -->

## 期货与期权日线来源 {#derivative-coverage}

<!-- derivative-capabilities:start -->

| 发布者 | 路由 | 期货起点 | 期权起点 | 生命周期参考 | 历史参考 | 状态 |
|---|---|---|---|---|---|---|
| SHF | official | 2002-01-07 | 2018-09-21 | 有 | 有 | supported |
| CZC | official | 2010-01-04 | 2017-04-19 | 有 | 有 | supported |
| GFE | official | 2022-12-22 | 2022-12-23 | 有 | 无 | supported |
| DCE | sina | 2018-06-01 | 不支持 | 无 | 无 | supported |
| CFE | official | 2010-04-16 | 2019-12-23 | 有 | 有 | supported |
| DCE | official | 2000-01-04 | 2017-03-31 | 无 | 无 | experimental |

此表由 `scripts/dev/sync_docs.py` 从读取器注册表生成。起点是适配器路由边界，不证明源端或本湖连续完整；`experimental` 尚未通过真实载荷验收。INE 2018 年期货使用能源中心独立日文件，2019 年起随 SHF 路由合并发布；其独立起点不能套用 SHF 日期。

<!-- derivative-capabilities:end -->
