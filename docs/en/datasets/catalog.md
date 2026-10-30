---
description: CNEquity dataset catalog: sources, history start and collection semantics for China A-share prices, financials, futures, fund flow, events, indices and macro data.
---

# Dataset catalog

This page answers two questions: which datasets the lake registers, and where each one comes from and how far back it goes. Find a dataset by the L0–L9 tiers below, then check its collection semantics and history limits; for query code to copy, go to the [Query guide](query-guide.md).

| To look up | Go to |
|------|--------|
| Columns, types and units | [Fields and units](schema.md) |
| Each source's limits and update frequency | [Sources and history limits](sources.md) |
| Start dates and history mode from code | `list_datasets()` → `coverage_start` / `coverage_end` / `history_mode` / `backfill_source` |

!!! note "Registered does not mean collected by default"
    - Off by default and enabled explicitly: minute bars `minute_bars` / `minute_bars_5m` (`[minute_bars]`), trade ticks `trade_ticks` (a separate `[trade_ticks]` section), and the four futures/options main tables `futures_contracts`, `option_contracts`, `futures_bars`, `option_bars` (`[futures]`, not part of `cne init`; see [Product boundaries](../architecture/overview.md)).
    - Derived datasets are computed from ingested data: `adj_factors`, `industry_index`, `futures_continuous`, `option_greeks`, `delisting_events`; `minute_bars_15m` / `30m` / `60m` are not computed by default and are derived on request.
    - **On-demand** datasets stay off the curated main path. `flash_news_wire` is a compatibility read for news.
    - So the registered count is neither the number of physical tables nor the number collected by default.

**Legend** (tables below): semantics `by_date` / `snapshot`; watermark ✓ = maintains a `meta/state` watermark.

## Data tiers

| Tier | Description | Representative datasets |
|------|------|------------|
| **L0** Reference | Universe, ETF catalog, calendar, trading status | instruments, etf_profiles, trading_calendar, trading_status |
| **L1** Market data | Unadjusted prices and volumes + adjustment factors + optional minute bars/trade ticks + delisting patterns | daily_bars, index_bars, minute_bars*, minute_bars_5m*, minute_bars_15m* / 30m* / 60m*, trade_ticks*, adj_factors, delisting_events |
| **L2** Corporate events | Ex-rights/ex-dividend, announcements, scheduled disclosures | corporate_actions, announcement_index, earnings_disclosure_schedule |
| **L3** Fundamentals | Financial statements, valuation, analyst consensus | financial_statement_items, valuation_metrics, analyst_consensus |
| **L4** Capital flows | Northbound, margin financing, main-force flows | fund_flow, fund_flow_ths, northbound_*, margin_trading, dragon_tiger, block_trades, institutional_holdings |
| **L5** Structure and industry | Sectors, index constituents, industries | sector_members, index_constituents, industry_members, industry_index |
| **L6** Macro | Interest rates, business climate, money | macro_indicators, market_breadth |
| **L7** Sentiment / rotation | News, sentiment, sectors, popularity, event streams | sentiment_scores, hot_rank, sector_bars, sector_fund_flow, sector_fund_flow_ths, news_headlines, flash_news_wire, economic_calendar*, economic_events*, market_holidays* (stock_news is on-demand) |
| **L8** Risk and compliance | Share unlocks, regulation | share_unlock_schedule, regulatory_events |
| **L9** Derivatives | Futures/options contracts, per-contract bars, continuous contracts, Greeks, minute bars, and commodity futures main continuous contracts | commodity_bars*, futures_contracts*, option_contracts*, futures_bars*, option_bars*, futures_continuous*, option_greeks*, futures_minute_bars* |

\*Optional or `required=false`: minute bars are off by default; commodity futures need an explicit backfill; the four futures/options tables need `[futures] enabled = true`; `economic_calendar`, `economic_events` and `market_holidays` come from Jin10; `cne backfill economic_calendar` fills the calendar back to 2008, while the other two accumulate only from the day they are turned on.

Tiers describe **research use** and are orthogonal to the storage `layer`: `adj_factors` / `delisting_events` (L1) and `industry_index` (L5) live in `derived/` rather than `curated/`, but are filed under their tiers by use, so there is no separate "derived" tier. Per [Product boundaries](../architecture/overview.md), futures/options sit alongside the equity system; contracts, market data and derived series all belong to L9 and are viewed as one group in the dashboard and the catalog. The authoritative source is `DatasetSpec.tier`; `test_docs_catalog.py` asserts that this document matches the registry tier by tier.

## Support levels

A support level says how far the project stands behind a dataset, independent of the research tiers above. The source of truth is `DatasetSpec.support`; the `support` and `enabled` columns of `list_datasets()` give each dataset's level and whether this lake runs it, and `test_docs_catalog.py` asserts this table matches the registry.

| Level | Default | Meaning | Datasets |
|-------|---------|---------|----------|
| `core` | on | Rebuildable history, a steady daily path, and a backup source or authority check; `cne check` and the stability gate (`cne verify --runs`) judge only this level, in whichever schedule group runs it | `instruments`, `trading_calendar`, `trading_status`, `daily_bars`, `index_bars`, `corporate_actions`, `financial_statement_items`, `share_structure`, `valuation_metrics`, `index_constituents`, `adj_factors`, `delisting_events` |
| `extended` | on | Rebuildable and mostly steady, but not essential or single-sourced | `etf_profiles`, `commodity_bars`, `announcement_index`, `earnings_disclosure_schedule`, `shareholder_counts`, `top_holders`, `margin_trading`, `dragon_tiger`, `block_trades`, `institutional_holdings`, `industry_members`, `macro_indicators`, `market_breadth`, `share_unlock_schedule`, `regulatory_events`, `industry_index` |
| `best_effort` | **off** | Live snapshots whose history starts the day a lake turns them on, a retired source, or feeds that fail often (EastMoney push2 money flow, news, popularity ranks); enabled only when listed in `[datasets] best_effort` | `analyst_consensus`, `fund_flow`, `northbound_holdings`, `northbound_flows`, `sector_members`, `sentiment_scores`, `hot_rank`, `sector_bars`, `sector_fund_flow`, `fund_flow_ths`, `sector_fund_flow_ths`, `news_headlines`, `flash_news_wire`, `economic_calendar`, `economic_events`, `market_holidays` |
| `optional` | off | Enabled by their own switches (`[minute_bars]`, `[trade_ticks]`, `[futures]`) | `minute_bars`, `minute_bars_5m`, `trade_ticks`, `futures_contracts`, `option_contracts`, `futures_bars`, `futures_minute_bars`, `option_bars`, `futures_continuous`, `option_greeks`, `minute_bars_15m`, `minute_bars_30m`, `minute_bars_60m` |

## Collection modes

| Mode | Meaning | Examples |
|------|------|------|
| **batch** | Daily/weekly updates, via staging → compact → curated | daily_bars, fund_flow |
| **derived** | Computed from curated; can be recomputed with `cne derive` | adj_factors |
| **on-demand** | Fetched per symbol, cached in meta | stock_news, research_reports |

### Fetch semantics (fetch_semantics)

| Value | Behavior | Example datasets |
|----|------|------------|
| `by_date` | Gaps can be backfilled by date | daily_bars, margin_trading |
| `snapshot` | Captures only the snapshot on the run date; fabricating history is forbidden | valuation_metrics, sector_members |

If a `snapshot` dataset has a `backfill_source` configured (e.g. `valuation_metrics` → baostock, `sector_bars` → ths), `cne backfill` may use that dedicated history source.

### History availability (history_mode) {#history-availability-history_mode}

Derived from `fetch_semantics` + `backfill_source` (see `list_datasets()`):

| history_mode | Meaning | Datasets |
|--------------|------|--------|
| `by_date` | Can be backfilled by date / gaps can be filled | Most market data and event tables |
| `snapshot_with_backfill` | Daily updates are snapshots, but a dedicated history source exists | `valuation_metrics`→baostock; `index_constituents`→cni; `industry_members`→sw; `sector_bars`→ths; `economic_calendar`→jin10 (from 2008) |
| `snapshot_only` | Returns only the current snapshot; accumulates day by day once enabled, but past dates that were not collected cannot be backfilled | `analyst_consensus`, `fund_flow`, `sector_members`, `hot_rank`, `sector_fund_flow`, `news_headlines`, `flash_news_wire`, `economic_events`, `market_holidays` |

Suspension coverage in `trading_status` can be derived back to the start of `daily_bars`; ST coverage must be backed by a complete `historical_st_evidence` receipt. Without a receipt covering the requested window, do **not** assume that `universe="all_a"` has excluded historical ST since 2001. For BJ, Tushare Pro is optional: historical short names via `bak_basic` for 2016, and `stock_st` from 2017-01-01; before 2016 a separate, deeper history source is still required.

### What `trade_ticks` is, and what it is not

**It is not tick-by-tick trades.** A-share Level-1 data is **a snapshot every 3 seconds**, and TDX "trade ticks" are aggregates of that snapshot.

This leads to three conventions you must know:

| Item | Reality |
|----|------|
| Timestamp | **Minute precision only**; the seconds field is always `00`. It is not truncated — the protocol never carried seconds |
| Primary key | Hence `(symbol, trade_date, tick_seq)` — the same minute can have 20 records with identical timestamps |
| `direction` | Direction **inferred** by TDX with the tick rule, not an exchange field; four values `buy` / `sell` / `neutral` / `after_hours` |

`after_hours` is post-close fixed-price trading from 15:05–15:30; its price always equals the day's last trade price, and it is **not counted in the exchange's daily volume** —
you must remove it before reconciling with daily data.

**There is no `amount` column.** The source does not provide it; you can compute `price × volume` yourself, but be aware that it is an approximation:
several trades at different prices within one frame are merged into a single representative price. This approximation is not guaranteed to equal the true turnover.

For compliance boundaries see [legal-and-data-sources](../legal-and-data-sources.md): this is **not** exchange Level-2 — there is no order-by-order data and no 10-level order book.

### History horizon: two mechanisms, do not mix them up

`history_mode` tells you **whether** you can backfill; this section tells you **how far back**. Source-side limits come in two kinds with completely different shapes:

**(1) A fixed number of bars per symbol** (`history_horizon_days`) — minute bars work this way. The value is "how many trading days the source still provides", and it rolls forward with today.

| Dataset | history_horizon_days | Window meaning |
|--------|---------------------|-------------------|
| minute_bars (1m) | **95** | Client default backfill window; actual dates depend on trading activity |
| minute_bars_5m (5m) | **491** | Client default backfill window, about 2 years |

**(2) A fixed date floor** (`history_floor_date`) — trade ticks and northbound flows work this way. It **does not roll with today**, so the horizon gets **longer** day by day.

| Dataset | history_floor_date | Boundary meaning |
|--------|-------------------|-------------------|
| trade_ticks | **2024-01-02** | Earliest request date the client allows; does not imply full coverage for every symbol |
| northbound_flows | **2014-11-17** | Launch date of Shanghai-Hong Kong Stock Connect; no such flow feed exists before it |
| futures_bars | **2002-01-07** | Earliest date for which SHFE files are available; ZCE 2010-01-04, CFFEX 2010-04-16, GFEX 2022-12-22, with each exchange's start clipped by the adapter |
| option_bars | **2017-04-19** | First day of ZCE white sugar options; SHFE 2018-09-21, CFFEX 2019-12-23, GFEX 2022-12-23 |

The difference is not academic: if you write a fixed floor as a rolling day count, `earliest_available()` moves forward every day, and within a few months it shuts out data the source is still willing to provide.

Upstream retention may change; when you hit a range error, check the version and the data source limits first.

If both `history_horizon_days` and `history_floor_date` are empty, it only means the registry declares no uniform history boundary; you cannot infer from it that the source has unlimited history.

This is **a property of the source, not a to-do item for this lake**. An earlier window does not return less data; it returns no data, and no backfill source can deepen it — looking at `by_date` alone would make you think you could backfill ten years.

**Minute bars are retained as a number of bars per symbol.** Less actively traded symbols may cover a longer calendar span; do not read the default window as a guarantee of continuous history for every symbol.

`cne backfill minute_bars --start` earlier than the horizon fails with an error instead of scanning a whole day and returning nothing. To pull deep history for thinly traded symbols, first narrow `[minute_bars].scope` to a watchlist. Programmatic read: the `history_horizon_days` column of `list_datasets()`.

`cne backfill trade_ticks --start` is blocked the same way, but with a different message: the trade-tick floor is the same for every symbol, so **no narrower scope can reach earlier data**.

### `trade_ticks` capacity

Trade ticks are paged by symbol and trading day; request count, row count and on-disk size vary with symbol activity and history range. Concurrent connections can cut idle network waiting, but they do not raise the request-start rate the shared rate limiter allows. Observe batch progress and actual partition sizes on a small scope first, then decide on the watchlist and lookback window; do not estimate full-market completion time from the throughput of a single egress.

That is why `[trade_ticks].max_symbols` defaults to 200 in the config: `index:000300.SH` resolves to about 300 symbols and fails immediately;
to run it you have to raise the limit yourself — this friction is intentional. `scope = "all"` is not supported and is rejected at config validation.

### 15m / 30m / 60m: not computed by default, can be stored on request

15m / 30m / 60m are not fetched from the source; they are resampled from the 1m and 5m data in the lake. They are not computed by default and are not part of the daily update; you can compute them on the fly at query time:

```python
from cnequity.query import load, resample_minute_history

window = dict(start="2026-07-01", symbols=["600519.SH"])
bars_15m = resample_minute_history(load("minute_bars", **window),
                                   load("minute_bars_5m", **window), "15m")
```

To read them directly through SQL, the HTTP API or MCP, compute them into the lake:

```bash
cne derive minute_bars_15m                                    # only trading days not yet computed or whose inputs changed
cne derive minute_bars_60m --start 2025-01-02 --end 2025-12-31
```

Once stored, read them like any other dataset with `load("minute_bars_15m", adjust="hfq")`. For the computation rules, the web dashboard entry point and the ways to read them, see [15 / 30 / 60-minute bars](../recipes/minute-bars-15-30-60.md). The morning and afternoon sessions are aligned from 09:30 and 13:00 respectively; for how bars computed from 5m differ from those computed from 1m, see the [Query guide](query-guide.md#trade-based-minute-resampling).

## Coverage that requires an API key

The THS official API (`ths_official`) is an **optional source**: a lake without a key keeps its existing sources, and daily updates are unaffected
(see [THS integration](../getting-started/configuration.md#ths-official-api)). Once enabled, it can fill gaps in
financial statements and historical daily bars, subject to the license and actual coverage; the `source` column identifies the source. The server-side lower bound of history, field completeness and PIT
evidence must still be verified against your own query window; another lake's row counts or ranges are not a commitment for this lake.

The following capabilities write only to `meta/source_snapshots`, **do not enter curated**, and are used only by the arbitration checks in `cne audit`:
`adj_factor_arbitration`, `daily_bars_arbitration`, `financial_statement_peer`.
Without snapshots they stay silent and do not affect the other checks.

THS official valuation snapshots can only accumulate day by day after enabling; old dates cannot be replayed to fabricate observations.
This does not mean `valuation_metrics` has no other history source at all; the coverage and definitions of different sources must be verified separately.

For full background see [THS integration](../getting-started/configuration.md#ths-official-api).

## Provenance columns (all curated rows)

| Column | Type | Description |
|----|------|------|
| `source` | string | Data source identifier |
| `data_version` | string | Source version/batch |
| `fetched_at` | timestamp[us, UTC] | Fetch time |

PIT datasets with `announce_date` (`load(..., as_of=)`): `financial_statement_items`, `announcement_index`.

On-demand datasets (`[on_demand].datasets`): by default `stock_news`, `research_reports` (implemented). `announcement_body` / `financial_reports` are not implemented yet. Access: `cne query --dataset <name> --symbol <code>.SH`.

Registry source code: `domain/datasets.py` (`DatasetSpec`), `domain/schemas.py` (Polars dtype / `PRIMARY_KEYS`); `test_dataset_registry.py` asserts they stay in sync.

## L0 Reference

| Dataset | Partition key | Primary key | Semantics | Watermark | Primary source | Notes |
|--------|--------|------|------|------|------|------|
| instruments | — (single-file merge) | symbol | by_date | — | tdx_protocol | EM fills list_date from the A-share and ETF/LOF clists separately; the published exchange ETF catalog fills missing fund listing dates; baostock backfills delisted stocks (`cne backfill instruments`); merge keeps delisted entries |
| etf_profiles | as_of_date (yearly) | symbol, as_of_date | snapshot | — | exchange | SSE ETF subcategories, the SZSE ETF/fund catalog, and official index methodologies verified code by code; only records with sufficient evidence of a domestic equity index enter the research pool. Unknown categories stay unverified, and unobserved historical snapshots cannot be backfilled |
| trading_calendar | trade_date | trade_date | by_date | ✓ | tdx_protocol | Fallback source: exchange CSV; seeded for 2016–2027 |
| trading_status | trade_date (monthly) | symbol, trade_date | by_date | ✓ | exchange | baostock ST backfill; derived suspensions are written to monthly partitions. `status` (normal/suspended/**delisted**) and `risk_warning` (ST/*ST) are two columns — the old single column let a suspension overwrite the ST flag; delisted rows are determined from `instruments` and marked `derived_delisted`. Old lakes are read compatibly; for the physical migration see [schema](schema.md#trading_status) |

The daily Beijing instrument catalogue attempts to enrich listing dates from confirmed same-day CNINFO IPO events. Subscription, prospectus and forecast dates do not establish listing dates. If that source is unavailable, the roster and unknown dates remain with a warning; compaction preserves dates already established.

Each day the instrument catalogue asks Baostock, EastMoney and the exchanges only about securities the lake has no listing date for; dates it already holds, including repaired ones, are carried forward. A security no source can date is not asked about again for 7 days, while a new listing is asked about daily until it has a date.

## L1 Market data

| Dataset | Partition key | Primary key | Semantics | Watermark | Primary source | Notes |
|--------|--------|------|------|------|------|------|
| daily_bars | trade_date | symbol, trade_date | by_date | ✓ | tdx_protocol | Tip gaps are routed from the EastMoney clist into curated; multi-day kline; BJ→sina; once explicitly enabled, only ETFs verified in the official catalog are appended, and other funds do not enter the default daily update; snapshot still stays in audit |
| index_bars | trade_date | symbol, trade_date, frequency | by_date | ✓ | tdx_protocol | |
| minute_bars | trade_date | symbol, trade_date, bar_time, frequency | by_date | ✓ | tdx_protocol | 1m. **Optional**, off by default; scope configured in `[minute_bars]`; **the source has only 95 trading days** (see "History horizon" below); on-disk size grows with symbol count and window; required=false |
| minute_bars_5m | trade_date | symbol, trade_date, bar_time, frequency | by_date | ✓ | tdx_protocol | 5m. Optional, as above; **491 trading days (about 2 years), the only intraday frequency with real history**; on-disk size grows with symbol count and window; required=false |
| minute_bars_15m | trade_date | symbol, trade_date, bar_time, frequency | derived | ✓ | derived | 15m. **Not computed by default**; store it manually with `cne derive minute_bars_15m`; for a given stock on a given day, 1m is used if present, otherwise 5m, and `resampled_from` records which; required=false |
| minute_bars_30m | trade_date | symbol, trade_date, bar_time, frequency | derived | ✓ | derived | 30m. Same as above |
| minute_bars_60m | trade_date | symbol, trade_date, bar_time, frequency | derived | ✓ | derived | 60m. Same as above |
| trade_ticks | trade_date | symbol, trade_date, tick_seq | by_date | ✓ | tdx_protocol | Trade ticks. **Optional**, off by default; configured separately in `[trade_ticks]`; **not tick-by-tick trades** (see below); the source goes back to **2024-01-02**; on-disk size grows with the watchlist and window; required=false |
| adj_factors | trade_date | symbol, trade_date, adjust_type | derived | ✓ | sina | hfq only; stocks read `f`, ETF/LOF read `s`; `cne derive adj_factors` |
| delisting_events | — (single-file merge) | symbol | derived | — | derived | End-of-life pattern of each delisted stock; recovered bars come from sina; produced by `cne backfill daily_bars --profile delisted` |

The two intraday datasets share one set of quality checks: duplicate primary keys (generic `pk_unique`), bars outside trading sessions, mismatched `trade_date` and `bar_time`, session gaps, and **two-way volume + turnover reconciliation against daily data**.

## L2 Corporate events

| Dataset | Partition key | Primary key | Semantics | Watermark | Primary source | Notes |
|--------|--------|------|------|------|------|------|
| corporate_actions | ex_date (yearly) | symbol, ex_date, action_type | by_date | ✓ | tdx_protocol (daily: the exchange's adjusted previous close names the session's ex-events, then TDX ex-rights records; EastMoney is the backup, the same-day peer and the source of the bonus/transfer split; the `actions_sweep` group re-reads every symbol's last 30 days weekly) | Backfill: tdx_protocol; for mixed granularity use `cne repair repartition` |
| announcement_index | announce_date | announcement_id | by_date PIT | ✓ | cninfo | `as_of` filtering |
| earnings_disclosure_schedule | report_period | symbol, report_period | by_date | — | eastmoney | Scheduled disclosure calendar (RPT_PUBLIC_BS_APPOIN); current-value semantics, not PIT: changes overwrite scheduled_date (first_scheduled_date keeps the first scheduled date, actual_date is filled in after disclosure); `cne backfill` covers all report periods from 2016 |

## L3 Fundamentals

| Dataset | Partition key | Primary key | Semantics | Watermark | Primary source | Notes |
|--------|--------|------|------|------|------|------|
| financial_statement_items | report_period | symbol, report_period, statement_type, item_code | by_date PIT | — | eastmoney | Partitioned by report period; `cne backfill` starts from 2001 by default (chunked with `--start`/`--end`); daily ingestion retains observed announcements and adds explicitly marked original-disclosure reconstructions; baostock is not used for FSI; backup tdx_fin (daily route when EastMoney fails, and gap backfill) |
| valuation_metrics | trade_date | symbol, trade_date | snapshot | ✓ | eastmoney | Backfill: baostock |
| analyst_consensus | forecast_date | symbol, forecast_date | snapshot | ✓ | eastmoney | |
| share_structure | change_date | symbol, change_date, announce_date | by_date PIT | — | eastmoney | Total shares / float / restricted / listed A-share float / free float. For an A+H company `float_shares` includes the H shares; an A-share float market cap uses `float_a_shares` (new in v2, null on older rows). **Scanned by change date, not by report period**: END_DATE is the share-change date, so requesting only quarter-end dates is not enough |
| shareholder_counts | count_date | symbol, count_date, announce_date | by_date PIT | — | eastmoney | Number of shareholders and average holding per account, an input for ownership concentration. **Also disclosed at ten-day-period and month ends**: do not filter by quarter-end dates only |
| top_holders | record_date | symbol, record_date, holder_scope, holder_rank, holder_name, announce_date | by_date PIT | — | eastmoney | One table, two scopes: `holder_scope=total` (top 10 shareholders) / `float` (top 10 float shareholders). Disclosure dates do not necessarily fall at quarter end |

DMSK `NOTICE_DATE` may describe a later republication of comparative figures. LICO supplies the original disclosure date; without a matching LICO record the later date is retained. Daily ingestion preserves the republication and adds `source=eastmoney_backfill` only for items lacking an original-date vintage. Reconstructed values may be restated: default `load(..., as_of=...)` best-effort reads expose them with `pit_is_exact=false`; `pit_mode="strict"` excludes reconstructions and requires observation no later than the query date.

Current versions store original disclosure dates during ingestion; data written by 0.17 or earlier is corrected by `cne migrate` (the `financial-announcements` migration). This only reconstructs existing items with original-date evidence. Missing historical items still require report-period collection with `cne backfill financial_statement_items --start <date> --end <date>`. Check coverage separately by source, item and report period; one complete statement does not establish complete detail coverage.

Vendor zero placeholders in PE, PB, PS and total/float market cap are stored as null; negative valuation ratios remain valid. Data written by 0.17 or earlier is corrected by `cne migrate`.

`cne repair financial-originals --symbols <stock symbols> --start-year 2016 --end-year 2024 --refresh-sources` enumerates the issuer’s complete CNINFO disclosure-date window to identify the earliest original annual report, archives its PDF and extracts identifiable current-year consolidated items. Preview is the default; `--apply` appends `source=cninfo_original` vintages. `--max-reports` bounds each batch, and repeated runs skip reports already containing original items. After a parser upgrade, `--reparse` can replay old archives offline to add newly identified cells; unchanged facts and disclosure times retain their first observation time. Use `--all-stocks` instead of `--symbols` to batch through the lake catalogue; pass the returned `next_cursor` to `--after` so failed reports do not block later issuers. Retrying without a cursor still selects unrecovered failures. Comparative figures, parent-only statements, revised editions, scans and ambiguous columns or units are excluded. `--quarter` selects Q1, Q2 (half-year), Q3 or the default Q4 (annual), locating and archiving each original report separately. Income and cash flow use year-to-date figures only. Four-column Q3 income tables require explicit column order and four populated cells before selecting the cumulative column; quarter-only figures never fill cumulative items. Online evidence collection does not rely on title keywords or category filters to establish first disclosure. The record count determines pagination, including a partial final page, and unique announcements must reconcile; incomplete windows remain failures. Combined group/company annual books additionally require explicit group/group/company/company roles and two current/prior-year date pairs. Only the current consolidated column is used; expense cells with unresolved sign conventions remain missing. Split asset/liability sections and three-column current/restated-opening/prior-end balances also require explicit date order. A bare “yuan” unit is accepted only when the report explicitly establishes both RMB functional currency and translation of foreign statements into RMB presentation; functional currency alone is insufficient. Support remains limited to identifiable items and layouts, not every issuer, quarter or detail. Publication and actual observation times stay separate: an old report retrieved today does not become historically observed strict PIT evidence. Original figures take precedence over later reconstructions for the same announcement date.

## L4 Capital flows

| Dataset | Partition key | Primary key | Semantics | Watermark | Primary source | staleness |
|--------|--------|------|------|------|------|-----------|
| fund_flow | trade_date | symbol, trade_date | snapshot | ✓ | eastmoney | 1d |
| fund_flow_ths | trade_date | symbol, trade_date | snapshot | ✓ | ths | Filled from THS only when push2 cannot provide `fund_flow`: inflow/outflow/net/turnover, with **no** main-force and extra-large…small order breakdown, amounts to 4 significant digits; source coverage is whatever the response returns, BSE is not promised; no report when empty. No freshness check by default; when a configured daily schedule group runs it (for example swapped in with `steps_add` while push2 is unavailable), the last closed session must have rows |
| margin_trading | trade_date | symbol, trade_date | by_date | ✓ | exchange | Margin trading details compiled by the SSE and SZSE themselves; SH has no securities-lending balance (`short_balance` is null), SZSE publishes one trading day later, and rows are written only once both are in; `[margin_trading] source` can switch back to eastmoney |
| northbound_holdings | trade_date | symbol, trade_date, channel | by_date | ✓ | eastmoney | 100d (quarterly) |
| northbound_flows | trade_date | trade_date, channel | by_date | ✓ | eastmoney | 2d |
| dragon_tiger | trade_date | symbol, trade_date, reason | by_date | ✓ | eastmoney | 1d; fallback source below |
| block_trades | trade_date | symbol, trade_date, price, volume | by_date | ✓ | eastmoney | 1d; fallback source below |
| institutional_holdings | report_period | symbol, holder_type, report_period | by_date | — | eastmoney | — |

`dragon_tiger` and `block_trades` have exchange fallback sources: SZSE `ShowReport` with
`CATALOGID=1265` (dragon-tiger list) and `1842_xxpl_after` (block trades); the SSE counterpart is `1902`.

**A fallback, not a replacement.** Per [Product boundaries](../architecture/overview.md), EastMoney is still the `primary_source`,
and the exchanges are the `backup_source`; **when EastMoney can answer, the fallback is never asked**. Failover sits in the fetch layer, so provenance,
watermarks and compact never need to know which path was taken.

**Neither exchange publishes BSE data**; this is recorded in `backup_gaps` rather than made to look covered.
`share_unlock_schedule` has no fallback source: SZSE records unlocks that **have already happened**, while this dataset is a forward-looking calendar; they are not the same thing.

## L5 Structure and industry

| Dataset | Partition key | Primary key | Semantics | Watermark | Primary source |
|--------|--------|------|------|------|------|
| sector_members | as_of_date | symbol, sector_code, as_of_date | snapshot | ✓ | eastmoney |
| index_constituents | as_of_date | index_symbol, symbol, as_of_date | snapshot | ✓ | exchange (CSI files) / cni; backup eastmoney |
| industry_members | as_of_date | symbol, classification_system, as_of_date | snapshot | ✓ | eastmoney |
| industry_index | trade_date (yearly) | trade_date, industry_code, level, weighting | derived | ✓ | derived (industry_members × hfq daily_bars) |

`industry_index` belongs to L5 rather than L1: the unit of observation is an industry, not a security, and it is computed from this tier's membership, so the index and its constituents never disagree. Recompute it with `cne derive industry_index`.

Snapshot datasets accumulate only "one membership record per day"; historical percentiles need partitions accumulated over many days.

Historical backfill (C2): `cne backfill industry_members` = Shenwan SwClass2021 monthly (`classification_system=sw`, from 2020);
`cne backfill index_constituents` = CNI sample-adjustment history (399001/399006, from about 2021-12). Other indices, including CSI 300 and STAR 50, rely on accumulated EM snapshots and the dated official files described below; earlier history can remain missing. Current membership cannot be backdated without survivorship bias; the EM adapter accepts only the current closed session. Historical reads use retained snapshots, and membership before the earliest snapshot is unknown.

Daily (from this release): the SSE Composite, CSI 300 and STAR 50 come from CSI's daily constituent files (source `exchange`), and Shenzhen Component and ChiNext from CNI's adjustment spells expanded to the session (`cni`); CSI month-end weight files and CNI weight books are kept on their own dates. When an index's publisher cannot be read, EastMoney fills that index alone for the session and the audit reports `index_constituents_official_unavailable`. EastMoney's membership is also kept as a peer snapshot, and the audit's `index_constituents_peer` lists the members each side alone holds (EastMoney carries no B shares). One index-date reads from one source: an official book hides EastMoney's rows for that index and day; EastMoney snapshots stored since July stay as they are and are read only on days without an official book.

Neither EM nor CNI adjustment history supplies weights: `weight=null` means unavailable and must not be treated as a real zero or inferred equal weight. Query each index/source's snapshot range and count directly: `cne query --sql "SELECT index_symbol, source, min(as_of_date), max(as_of_date), count(DISTINCT as_of_date) FROM index_constituents GROUP BY ALL ORDER BY ALL"`. Zero weight placeholders left by 0.17 or earlier are turned into nulls by `cne migrate`, without inventing historical membership or weights.

`cne repair index-snapshots --refresh-sources` fetches publicly available official files for CSI 300, STAR 50, Shenzhen Component and ChiNext. Preview is the default; `--apply` replaces the complete index snapshot at the file's actual date and retains old revisions. Percentage weights become ratios summing to approximately 1, with source `exchange` or `cni_weights`. Membership and weight files can have different dates; weights are never filled across dates, and later weightless observations cannot erase verified same-date weights. This recovers dated files still offered by the publisher and archived snapshots, not full weights for arbitrary historical dates or backdated current members.

`cne repair index-history --refresh-sources` archives missing official attachments from the reviewed announcement catalog. Subsequent runs replay offline by default; `--apply` updates the selected index. SZSE Component (399001.SZ) is the default, covering 2015-05-20 through 2026-09-30. `--index-symbol 000688.SH` selects STAR 50, covering its live launch on 2020-07-23 through 2026-09-30; the retrospective base date is not treated as a live membership anchor. After-close STAR 50 changes start on the next known trading session, and explicit no-change notices are also verified. It writes whole books on actual effective dates and replaces older membership-only observations within the range, so stale month-end books cannot hide temporary changes. The package includes a fixed-date publisher closing reference with its original URL and payload digest; this reference is not a freshly fetched response. Announcements and date evidence are bound to original-byte digests. Missing or changed attachments, inconsistent transitions, or disagreement with retained official weighted cohorts prevent updates. Unknown historical weights remain null, and complete verified official dated weights are retained. Other indices and dates outside the catalog range remain uncovered; current membership, current weights and fund holdings must not be backdated.

`load("index_constituents", as_of="YYYY-MM-DD")` returns each index's latest whole retained snapshot on or before that date. Removed members are not carried forward individually, and dates before the earliest snapshot return an empty table. Use `start`/`end` to read a multi-date snapshot series.

An EM response containing a future adjustment rejects the whole snapshot: removing future additions cannot recover old members already absent from the current roster. Retry collection once the adjustment takes effect.

## L6 Macro

| Dataset | Partition key | Primary key | Semantics | Watermark | Primary source | Notes |
|--------|--------|------|------|------|------|------|
| macro_indicators | obs_date | indicator_id, obs_date | by_date | ✓ | eastmoney / pboc (total social financing) | |
| market_breadth | trade_date | trade_date, metric_id | by_date | ✓ | derived (daily_bars) | |

## L7 Sentiment / rotation

| Dataset | Partition key | Primary key | Semantics | Watermark | Primary source | Notes |
|--------|--------|------|------|------|------|------|
| sentiment_scores | trade_date | symbol, trade_date, score_channel | by_date | ✓ | derived | |
| hot_rank | trade_date | symbol, trade_date | snapshot | ✓ | eastmoney | Popularity ranking top 100 (public API limit) |
| sector_bars | trade_date | sector_code, trade_date | snapshot | ✓ | ths | Both daily updates and backfill use THS board-kline; no second source |
| sector_fund_flow | trade_date | sector_code, trade_date | snapshot | ✓ | eastmoney | Sector main-force net inflow |
| sector_fund_flow_ths | trade_date | board_type, sector_code, trade_date | snapshot | ✓ | ths | Filled from THS only when push2 cannot provide `sector_fund_flow`: inflow/outflow/net for industries (881xxx) + concept sectors, amounts in units of 100 million yuan, precise to 0.01; THS sector classification, which does not map to EastMoney sector codes. Freshness follows the same rule as `fund_flow_ths` |
| news_headlines | publish_date | news_id | snapshot | ✓ | eastmoney | News headlines |
| flash_news_wire | publish_date | wire_id, wire_source | compatibility read | ✓ | eastmoney | Projected from the `news_headlines` fact table, compatible with entity files from old revisions; no longer written twice |
| economic_calendar | event_date (yearly) | event_id | snapshot | ✓ | jin10 | Jin10 economic calendar, fetched daily for two days back through two weeks ahead; one event is keyed by Jin10's `data_id`, so a released actual updates the same row; `indicator_code` (Jin10's `indicator_id`) names one indicator series across years and is null on rows written before 2026-10-30; an event Jin10 stops listing inside the window (a rescheduled release gets a new id) is flagged `withdrawn = true`, not deleted; `cne backfill economic_calendar` walks month by month and fetches only the days the lake lacks; `required=false`, an empty table is not judged UNHEALTHY |
| economic_events | event_date (yearly) | event_id | snapshot | ✓ | jin10 | Jin10 economic events (central bank decisions, official speeches, major meetings) over the calendar's window; an empty `event_time` with `time_status=待定` means the time is not fixed; an event that drops out of the window is flagged `withdrawn = true`; `required=false` |
| market_holidays | holiday_date (yearly) | holiday_id | snapshot | ✓ | jin10 | Jin10 exchange closures worldwide, fetched daily as the next 30 from the run day (about two months); `note` says whether Stock Connect southbound/northbound is closed; `required=false` |

`sector_bars` uses THS sector market data; history is filled in window by window with `cne backfill sector_bars`. Check the range with `--plan` first; after failures, rerunning the same command resumes from the checkpoint, so avoid needless `--refetch` full refetches. Network reachability is determined by a small-scope probe from the current egress.

## L8 Risk and compliance

| Dataset | Partition key | Primary key | Semantics | Watermark | Primary source |
|--------|--------|------|------|------|------|
| share_unlock_schedule | unlock_date | symbol, unlock_date | by_date | ✓ | eastmoney |
| regulatory_events | event_date | event_id | by_date | ✓ | cninfo (derived from announcement_index) |

## L9 Derivatives

Futures and options contracts, per-contract market data and derived series, plus the commodity futures main continuous contracts integrated earlier. All are `required=false`; except for `commodity_bars`, they are enabled through `[futures]` and are not part of `cne init`.

| Dataset | Partition key | Primary key | Semantics | Watermark | Primary source | Notes |
|--------|--------|------|------|------|------|------|
| commodity_bars | trade_date | symbol, trade_date | by_date | ✓ | sina | Fallback source eastmoney; domestic main continuous contracts + COMEX gold `GC0.CMX`; `cne backfill commodity_bars`; required=false |
| futures_contracts | — (single-file merge) | symbol | by_date | — | futures_exchange | Futures contract table: listing and last trading dates are taken only from authoritative references; first and last observation dates are stored separately, and missing bars do not mean expiry; `dates_basis` records the source; rebuilt from futures_bars; required=false |
| option_contracts | — (single-file merge) | symbol | by_date | — | futures_exchange | Options contract table: underlying, strike, call/put, exercise style, expiry date; CFFEX options have an index as the underlying (IO→000300.SH); required=false |
| futures_bars | trade_date (monthly) | symbol, trade_date | by_date | ✓ | futures_exchange | Per-contract daily bars, including settlement price and open interest; **single-sided convention** (pre-2020 double-sided values from SHFE/INE/DCE/ZCE have been halved); turnover in yuan; OHLC is null on rows with no trades; each exchange is its own failure domain — if one is missing the others are still written, with a three-day lookback plus retries of a persistent backlog; reads SHFE (including INE, from 2002), ZCE (from 2010), GFEX (from 2022-12) and CFFEX (from 2010-04-16); for DCE see sources.md; required=false |
| option_bars | trade_date | symbol, trade_date | by_date | ✓ | futures_exchange | Per-contract option daily bars, including settlement price, open interest, exercise volume and exchange-published Delta/IV; on expiry day, out-of-the-money contracts are stored with a settlement price of 0; SHFE/INE from 2018-09, ZCE from 2017-04-19, GFEX from 2022-12-23, CFFEX IO/MO/HO from 2019-12-23; required=false |
| futures_continuous | trade_date (monthly) | symbol, series, trade_date | derived | ✓ | derived | Dominant / second-dominant continuous contracts: the contract used on day T depends only on T-1 closing open interest, rolls only forward, and gives way 5 days before the last trading day (or on entering the delivery month); `adj_ratio`/`adj_diff` are cumulative roll factors; `roll_yield` is the annualized roll yield of the dominant over the second-dominant contract; `cne derive futures_continuous`; required=false |
| option_greeks | trade_date | symbol, trade_date | derived | ✓ | derived | This lake's own option IV and Greeks: implied from settlement prices, Black-76 for European and BAW for American options; commodity options use the same-day futures settlement price as the underlying, and CFFEX index options back out the forward via put-call parity; the rate is shibor_3m, falling back to 2% when missing and noted in `rate_source`; `status` explains why there is no solution (`expiry_day` on expiry day); `cne derive option_greeks` tracks content dependencies on bars/contracts/rates/model and automatically recomputes invalidated dates; required=false |
| futures_minute_bars | trade_date | symbol, bar_time | by_date | ✓ | sina | Futures 1-minute bars, **watchlist only** (`[futures] minute_*`); Sina keeps only the latest 1023 bars per contract (about 2 trading days for products with a night session), so data can only accumulate from the day it is enabled and must be run every trading day; night-session bars belong to the next trading day; required=false |

## Primary/fallback configuration (Failover → meta/source_snapshots)

| Dataset | Primary source | Fallback source |
|--------|------|------|
| daily_bars | tdx_protocol | eastmoney |
| corporate_actions | tdx_protocol | eastmoney |

## Checks against the publisher (authority checks)

Primary/fallback comparison compares two redistributors: agreement only shows that they do not conflict, not which one is right. The following checks go past the redistributors directly to the upstream publishing institution,
write to `meta/quality/source_diffs/authority-<date>.json`, and report without blocking (see [Product boundaries](../architecture/overview.md)).

| Check | Dataset | Reference |
|--------|--------|--------|
| `macro_pmi_vs_nbs` | macro_indicators | National Bureau of Statistics PMI releases |
| `st_labels_vs_exchange` | trading_status | Short names in the SSE/SZSE securities lists |
| `daily_bars_vs_exchange` | daily_bars | Closing quotes published by the SSE/SZSE themselves |
| `adj_factor_corporate_action_divergence` | adj_factors | Adjustment factor steps recomputed independently from `corporate_actions` |
| `adj_factor_pre_close_divergence` | adj_factors | The step `previous trading day's close ÷ pre_close` implied by the exchange-published previous close (`daily_bars.pre_close`) |

Price and turnover use separate tolerances. Turnover is also judged by the share of deviating symbols, to avoid per-symbol false positives caused by differences in statistical scope. Exchange zero-volume records for suspended securities are not treated as market-data gaps; `daily_bars_missing_vs_exchange` counts only symbols that traded.

The `adj_factors` recomputation is based on the continuity identity on the ex-rights/ex-dividend date:

```
f_ex / f_prev = (1 + bonus ratio + conversion ratio + rights ratio) × prev_close / (prev_close − pre-tax cash dividend + rights ratio × rights price)
```

With no ex-date, the right-hand side is always 1, so both "the factor moved on a day it should not have" and "it did not move on a day it should have" are caught.
The tolerance is a materiality threshold, not an equality test (default 50 bps, escalated to error at ≥200 bps), because the two sources round differently.
