# Data and coverage

Datasets are grouped by research purpose, from L0 to L9. The tables show the registry's support levels, primary sources, and history modes. Actual collection depends on your configuration, permissions, and provider availability.

## Check coverage first {#coverage}

| History mode | Meaning |
|---|---|
| `by_date` | Dated history is supported within provider and client limits. Derived datasets also depend on input coverage. |
| `snapshot_with_backfill` | Daily collection takes the current snapshot. A separate source provides historical backfill and may have different coverage or semantics. |
| `snapshot_only` | Only observed snapshots can accumulate. Where the table shows a credentialed backfill source, that history path requires enabling the source and configuring valid credentials. |

Run `cne status --datasets --all-columns` on your lake, or use `list_datasets()` in Python, to check enabled datasets, `history_mode`, and coverage. Gaps can exist between the first and last dates. The floor dates and trading-session counts in the tables are request limits from the registry.

`core` is the core acceptance scope. `extended` covers additional research data, `best_effort` is enabled by configuration, and `optional` covers opt-in use cases. Check the config and status for what is enabled locally. See [license and sources](../legal-and-data-sources.md) for upstream conditions.

## Data contract {#contract}

```bash
cne contract show daily_bars
cne contract show --out dataset-contract.json
cne contract diff old-contract.json dataset-contract.json
```

Contracts record keys, column types, units, versions, and history semantics. Versioned copies live in [contracts](https://github.com/rootSunc/CNEquity/tree/main/contracts). See [fields and units](schema.md) for column explanations. Data provenance includes `source`, `data_version`, and `fetched_at`. See [query and research](query-guide.md#pit) for point-in-time evidence requirements.

## Dataset groups {#data-tiers}

<!-- dataset-catalog:start -->

## L0 Reference

| Dataset | Support | Primary source | History mode | Backfill source | Registry request limit |
|---|---|---|---|---|---|
| instruments | core | tdx_protocol | by_date | baostock | — |
| name_history | extended | tushare | by_date | — | — |
| convertible_bonds | best_effort | tushare | by_date | — | — |
| convertible_bond_coupons | best_effort | tushare | by_date | — | — |
| executives | best_effort | tushare | by_date | — | 1990-01-01 |
| etf_profiles | extended | exchange | snapshot_only | — | — |
| trading_calendar | core | tdx_protocol | by_date | — | — |
| trading_status | core | exchange | snapshot_with_backfill | baostock | — |

## L1 Prices

| Dataset | Support | Primary source | History mode | Backfill source | Registry request limit |
|---|---|---|---|---|---|
| convertible_bond_bars | best_effort | tushare | by_date | — | 2004-01-02 |
| limit_prices | best_effort | tushare | by_date | — | 2007-01-04 |
| daily_bars | core | tdx_protocol | by_date | — | — |
| index_bars | core | tdx_protocol | by_date | — | — |
| minute_bars | optional | tdx_protocol | by_date | — | 95 trading sessions |
| minute_bars_5m | optional | tdx_protocol | by_date | — | 491 trading sessions |
| trade_ticks | optional | tdx_protocol | by_date | — | 2024-01-02 |
| adj_factors | core | sina | by_date | — | — |
| minute_bars_15m | optional | derived | by_date | — | — |
| minute_bars_30m | optional | derived | by_date | — | — |
| minute_bars_60m | optional | derived | by_date | — | — |
| delisting_events | core | derived | by_date | — | — |

## L2 Corporate events

| Dataset | Support | Primary source | History mode | Backfill source | Registry request limit |
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

## L3 Fundamentals

| Dataset | Support | Primary source | History mode | Backfill source | Registry request limit |
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

## L4 Capital flows

| Dataset | Support | Primary source | History mode | Backfill source | Registry request limit |
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

## L5 Industry structure

| Dataset | Support | Primary source | History mode | Backfill source | Registry request limit |
|---|---|---|---|---|---|
| sector_members | best_effort | eastmoney | snapshot_only | — | — |
| index_constituents | core | exchange | snapshot_with_backfill | cni | — |
| industry_members | extended | eastmoney | snapshot_with_backfill | sw | — |
| industry_index | extended | derived | by_date | — | — |

## L6 Macro

| Dataset | Support | Primary source | History mode | Backfill source | Registry request limit |
|---|---|---|---|---|---|
| macro_indicators | extended | eastmoney | by_date | — | — |
| market_breadth | extended | derived | by_date | — | — |

## L7 Sentiment / rotation

| Dataset | Support | Primary source | History mode | Backfill source | Registry request limit |
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

## L8 Risk and compliance

| Dataset | Support | Primary source | History mode | Backfill source | Registry request limit |
|---|---|---|---|---|---|
| pledge_stats | best_effort | tushare | by_date | — | 2014-03-07 |
| share_unlock_schedule | extended | eastmoney | snapshot_with_backfill | eastmoney | — |
| regulatory_events | extended | cninfo | by_date | — | — |

## L9 Derivatives

| Dataset | Support | Primary source | History mode | Backfill source | Registry request limit |
|---|---|---|---|---|---|
| futures_member_positions | best_effort | tushare | by_date | — | 2010-01-04 |
| commodity_bars | extended | sina | by_date | eastmoney_kline+sina_global | — |
| futures_contracts | optional | futures_exchange | by_date | — | — |
| option_contracts | optional | futures_exchange | by_date | — | — |
| futures_bars | optional | futures_exchange | by_date | — | 2002-01-07 |
| futures_minute_bars | optional | sina | by_date | — | 2 trading sessions |
| option_bars | optional | futures_exchange | by_date | — | 2017-04-19 |
| futures_continuous | optional | derived | by_date | — | — |
| option_greeks | optional | derived | by_date | — | — |

<!-- dataset-catalog:end -->

## Daily futures and options sources {#derivative-coverage}

<!-- derivative-capabilities:start -->

| Publisher | Route | Futures since | Options since | Lifecycle reference | Historical reference | Status |
|---|---|---|---|---|---|---|
| SHF | official | 2002-01-07 | 2018-09-21 | yes | yes | supported |
| CZC | official | 2010-01-04 | 2017-04-19 | yes | yes | supported |
| GFE | official | 2022-12-22 | 2022-12-23 | yes | no | supported |
| DCE | sina | 2018-06-01 | not supported | no | no | supported |
| CFE | official | 2010-04-16 | 2019-12-23 | yes | yes | supported |
| DCE | official | 2000-01-04 | 2017-03-31 | no | no | experimental |

Generated by `scripts/dev/sync_docs.py` from the reader registry. Start dates are adapter route boundaries; they do not prove that the source or this lake is continuous and complete. `experimental` routes have not yet passed acceptance on real payloads. INE futures for 2018 come from the energy center's own daily files; from 2019 they are published together with the SHF route, so the SHF dates do not apply to INE's separate start.

<!-- derivative-capabilities:end -->
