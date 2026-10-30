# Fields and units

This page is for looking up columns, types and units. For what each dataset is for, see the [catalog](catalog.md); for how to read data, see the [query guide](query-guide.md); for compatibility and versioning, see the [data contract](contract.md). Column names and types are synced by `scripts/dev/sync_docs.py`; the explanatory text is maintained by hand.

### Find by layer

Ordered by [research layer](catalog.md#data-tiers); click a name to jump to its column definitions.

| Layer | Datasets |
|------|--------|
| **L0** Reference | [instruments](#instruments), [etf_profiles](#etf_profiles), [trading_calendar](#trading_calendar), [trading_status](#trading_status) |
| **L1** Prices | [daily_bars](#daily_bars), [index_bars](#index_bars), [minute_bars](#minute_bars--minute_bars_5m), [minute_bars_5m](#minute_bars--minute_bars_5m), [trade_ticks](#trade_ticks), [adj_factors](#adj_factors), [minute_bars_15m](#minute_bars_15m--minute_bars_30m--minute_bars_60m), [minute_bars_30m](#minute_bars_15m--minute_bars_30m--minute_bars_60m), [minute_bars_60m](#minute_bars_15m--minute_bars_30m--minute_bars_60m), [delisting_events](#delisting_events) |
| **L2** Corporate events | [corporate_actions](#corporate_actions), [announcement_index](#announcement_index), [earnings_disclosure_schedule](#earnings_disclosure_schedule) |
| **L3** Fundamentals | [financial_statement_items](#financial_statement_items), [share_structure](#share_structure), [shareholder_counts](#shareholder_counts), [top_holders](#top_holders), [valuation_metrics](#valuation_metrics), [analyst_consensus](#analyst_consensus) |
| **L4** Capital flows | [fund_flow](#fund_flow), [margin_trading](#margin_trading), [northbound_holdings](#northbound_holdings), [northbound_flows](#northbound_flows), [dragon_tiger](#dragon_tiger), [block_trades](#block_trades), [institutional_holdings](#institutional_holdings), [fund_flow_ths](#fund_flow_ths) |
| **L5** Industry structure | [sector_members](#sector_members), [index_constituents](#index_constituents), [industry_members](#industry_members), [industry_index](#industry_index) |
| **L6** Macro | [macro_indicators](#macro_indicators), [market_breadth](#market_breadth) |
| **L7** Sentiment / rotation | [sentiment_scores](#sentiment_scores), [hot_rank](#hot_rank), [sector_bars](#sector_bars), [sector_fund_flow](#sector_fund_flow), [sector_fund_flow_ths](#sector_fund_flow_ths), [news_headlines](#news_headlines), [flash_news_wire](#flash_news_wire), [economic_calendar](#economic_calendar) |
| **L8** Risk and compliance | [share_unlock_schedule](#share_unlock_schedule), [regulatory_events](#regulatory_events) |
| **L9** Derivatives | [commodity_bars](#commodity_bars), [futures_contracts](#futures_contracts), [option_contracts](#option_contracts), [futures_bars](#futures_bars), [futures_minute_bars](#futures_minute_bars), [option_bars](#option_bars), [futures_continuous](#futures_continuous), [option_greeks](#option_greeks) |

### Global conventions

| Rule | Value |
|------|-------|
| Time zone | All `trade_date` values and business timestamps use `Asia/Shanghai` |
| Stock symbol | `{code}.{SH\|SZ\|BJ}`, e.g. `600519.SH`; derivatives use their own contract codes and exchange suffixes |
| Stock exchange column | `SH` / `SZ` / `BJ`; for derivatives, see the corresponding schema |
| Provenance columns | Every row has `source`, `data_version` and `fetched_at` (UTC timestamp) |
| Null semantics | On suspension days OHLCV still has values, with `volume=0` and `amount=0` |
| Volume unit | A-share single-stock volume is always in **shares**; for vendors that report lots (TDX daily bars, EastMoney), the adapter multiplies by 100 at the boundary |
| Schema evolution | Compatible new columns may be added directly; breaking changes must bump `schema_version` and come with migration notes |
| `data_version` | Bumped only for semantic changes (not for added columns); see "Volume unit" below |

### PIT bitemporal extension columns

Apply only to the PIT datasets: `announcement_index`, `financial_statement_items`, `share_structure`, `shareholder_counts`, `top_holders`. **They do not apply to any other dataset.**

The four columns are optional storage columns and are not part of the required shape in `DATASET_SCHEMAS`: old Parquet files without them are still readable,
and the read side fills them in. Compact writes them to disk; old partitions without these columns get them written once at their next compact.

| Column | Type | Description |
|--------|------|-------|
| available_at | timestamp, nullable | When the fact became available at the source; must be null when unknown, and must not be replaced by the report period during backfill |
| source_published_at | timestamp, nullable | When the source actually published it; usually unknown for the current EastMoney historical backfill |
| observed_at | timestamp, nullable | When the lake actually observed the row; for old files it is derived from `fetched_at` for compatibility |
| revision_id | string, nullable | Stable fact/version identity; a SHA-256 truncated to 96 bits (24 hex characters), derived deterministically from the business fields and provenance, **without** the observation timestamp |

`observed_at` and `fetched_at` are two names for the same thing, so neither takes part in compact's
business digest comparison. Otherwise every reconciliation refetch would be judged a business change and mint a new revision,
and each revision copies the whole dataset.

### Partition keys (curated)

| Dataset | Partition |
|---------|-----------|
| daily_bars | `trade_date` (daily) |
| index_bars | `trade_date` (yearly) |
| minute_bars / minute_bars_5m | `trade_date` (daily) |
| minute_bars_15m / minute_bars_30m / minute_bars_60m | `trade_date` (daily) |
| trade_ticks | `trade_date` (daily) |
| trading_status | `trade_date` (monthly) |
| corporate_actions | `ex_date` (yearly) |
| adj_factors | `trade_date` (daily) |
| financial_statement_items | `report_period` |
| industry_members | `as_of_date` |
| northbound_flows | `trade_date` |

Multi-source snapshot path: `meta/source_snapshots/{dataset}/source={source}/data_version={ver}/`

### Primary keys

| Dataset | Primary key |
|---------|-------------|
| instruments | `(symbol)` |
| etf_profiles | `(symbol, as_of_date)` |
| trading_calendar | `(trade_date)` |
| trading_status | `(symbol, trade_date)` |
| daily_bars | `(symbol, trade_date)` |
| index_bars | `(symbol, trade_date, frequency)` |
| minute_bars / minute_bars_5m | `(symbol, trade_date, bar_time, frequency)` |
| minute_bars_15m / minute_bars_30m / minute_bars_60m | `(symbol, trade_date, bar_time, frequency)` |
| trade_ticks | `(symbol, trade_date, tick_seq)` |
| corporate_actions | `(symbol, ex_date, action_type)` |
| adj_factors | `(symbol, trade_date, adjust_type)` |
| fund_flow | `(symbol, trade_date)` |
| northbound_holdings | `(symbol, trade_date, channel)` |
| northbound_flows | `(trade_date, channel)` |
| margin_trading | `(symbol, trade_date)` |
| sector_members | `(symbol, sector_code, as_of_date)` |
| valuation_metrics | `(symbol, trade_date)` |
| announcement_index | `(announcement_id)` |
| financial_statement_items | `(symbol, report_period, statement_type, item_code, announce_date)` |
| industry_members | `(symbol, classification_system, as_of_date)` |

### MVP-P0 column definitions

#### instruments

| Column | Type | Description |
|--------|------|-------|
| symbol | string | Primary key |
| name | string |  |
| exchange | string | SH/SZ/BJ |
| asset_type | string | stock/etf/index; the legacy `etf` value also covers some LOFs, so it cannot be used to decide ETF research eligibility |
| list_date | date | Nullable |
| delist_date | date | Nullable; the last listed session (inclusive, halt placeholders count), i.e. the session before the removal date |
| prev_symbol | string | Nullable |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### etf_profiles

A snapshot of the exchanges' **current directories**; past classifications are not backfilled. The SSE directory gives the ETF subcategory and tracked index;
only its single-market stock, Shanghai/Shenzhen/Beijing cross-market stock and STAR Market stock categories are marked `eligible`. Categories that are clearly out of scope, such as cross-border, bond and
commodity, are `excluded`. The SZSE ETF list provides the fund and its tracked index, and
a separate official fund list gives the investment category: bond, money-market and other non-equity funds can be excluded; equity funds additionally need to be linked by
tracked index code to the official index methodology. Verified code by code so far:
[399006 ChiNext Index](https://www.cnindex.com.cn/docs/gz_399006_e.pdf),
[399330 SZSE 100](https://www.cnindex.com.cn/docs/gz_399330_e.pdf);
the sample space of [399673 ChiNext 50](https://www.cnindex.com.cn/docs/gz_399673_e.pdf) is the ChiNext Index constituents,
so the A-share sample space of 399006 must also be archived and verified before it can be marked `eligible`. All other indices remain `unverified`.
The two SZSE lists must agree code by code.
A missing directory record does not allow eligibility to be inferred from the code prefix either. Data comes from the [SSE ETF list](https://www.sse.com.cn/assortment/fund/etf/list/),
the [SZSE ETF list](https://fund.szse.cn/marketdata/etf/) and the [SZSE fund list](https://fund.szse.cn/marketdata/fundslist/).

| Column | Type | Description |
|--------|------|-------|
| symbol | string | Security code; together with as_of_date forms the snapshot primary key |
| as_of_date | date | Date observed by this lake; together with symbol forms the snapshot primary key |
| exchange | string | Exchange |
| name | string | Fund short name |
| list_date | date | Listing date as given by the source; nullable |
| tracking_index_code | string | Index code from the source; nullable |
| tracking_index_name | string | Index name from the source; nullable |
| fund_category | string | Official fund category |
| investment_category | string | Official investment category; on its own it cannot prove the market scope of an equity index |
| eligibility_status | string | `eligible` / `excluded` / `unverified` |
| classification_basis | string | Basis of the classification; verified Shenzhen rows include the index code and the SHA-256 and URL of the official methodology PDF; name or code prefix alone is not enough |
| source_url | string | Official directory page |
| source | string | Data source |
| data_version | string | Data version |
| fetched_at | timestamp | Time this lake collected it |

#### trading_calendar

| Column | Type | Description |
|--------|------|-------|
| trade_date | date | Primary key |
| is_trading | bool |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### trading_status

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| is_trading | bool |  |
| status | string | **Trading status**: `normal` / `suspended` / `delisted` |
| risk_warning | bool | **Risk warning (ST/*ST)**, orthogonal to status; may be null (no evidence) |
| source | string | Delisted rows are determined from `instruments` and marked `derived_delisted` |
| data_version | string |  |
| fetched_at | timestamp |  |

`status` is the trading status and `risk_warning` is the risk warning; the two are independent. A suspension cannot clear the ST flag, and a security known to be delisted must not be treated as trading normally.

`derived_traded` means committed daily bars prove a print on that date (positive volume),
rather than a first-hand status snapshot. It fills only stock/ETF keys with a known
listed life and no existing status row, setting `normal`, `is_trading=true`, and
`risk_warning=null` for stocks or `risk_warning=false` for ETFs/funds (ST does not
apply to funds, so it is not unknown). On a stock row `normal` does not mean
confirmed non-ST. Authoritative session
records take precedence; these derived rows are excluded from the EastMoney outage
carry and do not establish historical ST coverage. Zero or unknown volume, unknown
listing dates, and bars outside the listed life cannot generate these rows. Existing
contradictory statuses need a separate repair.

Historical snapshots cover dates collected by this lake; a first status row does
not establish coverage of earlier history. `cne check --full` reports
`trading_state_historical_coverage` by asset class and month, including dates before
each security's first status. Missing status, derived prints, unknown ST, and
exclusion reasons are counted separately. This report covers printed keys only;
it does not prove complete suspension history or historical ST coverage.

Preview offline with `cne repair status-traded --start YYYY-MM-DD --end YYYY-MM-DD`,
then add `--apply` after reviewing the plan. Apply recomputes against current committed
inputs, publishes a new version and preserves the old one. Use annual or monthly
windows. Strict historical universe reads still require independent ST evidence.

**Reading an old lake does not fail.** Old rows encode ST as `status="st"`, and `validate_dataframe` upgrades them
automatically on read (see `cnequity/domain/trading_status.py`); the physical files need no rewrite.

The migration does **not** add `delisted` rows to history: a day's status is the fact observed at the time, and back-filling it with today's delisting date
would invent point-in-time facts that did not exist then. To fix a stretch of history, rerun the daily update for that stretch.

#### daily_bars

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| open | float64 | Unadjusted |
| high | float64 |  |
| low | float64 |  |
| close | float64 |  |
| pre_close | float64 | Nullable. The previous close published by the exchange; on an ex-date this is the ex-rights reference price (see "Previous close" below) |
| volume | int64 | **Shares** (see "Volume unit" below); guaranteed only for `data_version=v2` |
| amount | float64 | CNY |
| source | string |  |
| data_version | string | `v2` = volume in shares; `v1` = varies by source, deprecated |
| fetched_at | timestamp |  |

##### Volume unit (`daily_bars.volume`) {#volume-unit}

Vendors do not agree on the native unit, and nothing in the payload declares it, so mixing them in one column makes values off by **exactly a factor of 100**: enough to ruin every turnover or liquidity factor, yet small enough that row counts, primary keys and OHLC checks never notice.

**Contract: always store shares.** This is also the only choice under which `amount ≈ close × volume` holds, and that identity is exactly what quality checks rely on to detect unit errors from the data itself. Each adapter converts at its own boundary.

Source data is converted uniformly at the collection boundary. TDX daily bars and minute bars have different native units, so a conversion rule must not be reused across frequencies. Volume in `minute_bars` / `minute_bars_5m` is also in shares.

Quality checks reconcile volume against price per source; data without turnover cannot be checked this way. Single-stock volume/price relationships do not apply to the index points in `index_bars` / `sector_bars`.

**Migration (v1 → v2).** A lake written by 0.12 or earlier needs its v1 rows rewritten to shares; that one-off script has been removed, so migrate such a lake with 0.19 or earlier before upgrading.

##### Previous close (`pre_close`) {#previous-close}

The day's previous close as published in exchange quotes, taken from SSE quote snapshots, the BSE quote board and TDX real-time quotes (Shenzhen via TDX), written by the daily update since 0.13.0. Bar history (TDX history, Sina, Baostock, THS) does not provide it, so those rows and existing history are empty. On an ex-dividend/ex-rights date the previous close is the ex-rights reference price computed by the exchange, so `previous trading day's close ÷ pre_close` is the adjustment-factor step that day should have, and 1 on ordinary days. Derived adjustment factors are checked against it; a deviation beyond tolerance (plus half a fen of rounding on the previous close) reports `adj_factor_pre_close_divergence`. When the same trading day is later backfilled from bars, an existing previous close is kept. `index_bars` shares this column, but nothing currently writes it.

##### BSE volume and turnover include block trades {#bse-volume-and-turnover-include-block-trades}

BSE daily `volume` / `amount` include that day's block trades. This holds for TDX, the BSE website and THS alike; Shanghai and Shenzhen daily bars do not include them. Block trades often execute at a discount, so a BSE day's `amount / volume` can fall below that day's low (or above the high when at a premium). To compute BSE average price or turnover rate on the Shanghai/Shenzhen basis, first subtract that day's block trades (`block_trades` is in units of 10,000 shares / 10,000 CNY). For the Select Tier period before the market opened on 2021-11-15, `block_trades` has no BSE records (EastMoney does not provide them), so daily bars from that period cannot be adjusted this way.

#### index_bars

Same structure as daily_bars, plus `frequency` (default `1d`).

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| open | float64 |  |
| high | float64 |  |
| low | float64 |  |
| close | float64 |  |
| pre_close | float64 | Nullable previous-close column shared with daily bars; current index sources do not write it |
| volume | int64 | **Not shares**: raw value from TDX `index()`, unit unconfirmed (see below) |
| amount | float64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |
| frequency | string | Default `1d` |

**Exception: `volume` is not in shares.** `index_bars` / `sector_bars` keep the raw value from the upstream index endpoint; the unit is unconfirmed, so it must not be used directly for single-stock turnover or summed with constituent volumes. Both datasets are still at `data_version=v1`.

#### minute_bars / minute_bars_5m

Intraday bars; the two datasets share one schema. **Optional**, off by default (`[minute_bars].enabled = false`), and not on the default daily wave.

| Dataset | frequency | Bars per trading day | Source horizon |
|--------|-----------|-----------------|---------|
| `minute_bars` | `1m` | 240 | 95 trading days |
| `minute_bars_5m` | `5m` | 48 | 491 trading days (about 2 years) |

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date | Partition column; A-shares have no night session, so it always equals the date of `bar_time` |
| bar_time | timestamp (naive) | **The bar's closing minute**, `Asia/Shanghai` wall clock; see "Bar semantics" below |
| frequency | string | `1m` / `5m` / `15m` / `30m` / `60m`; each dataset holds only one frequency |
| open | float64 | **Unadjusted**; use `load(..., adjust="hfq")` to join the day's factor on `(symbol, trade_date)` at query time |
| high | float64 | **Unadjusted**; use `load(..., adjust="hfq")` to join the day's factor on `(symbol, trade_date)` at query time |
| low | float64 | **Unadjusted**; use `load(..., adjust="hfq")` to join the day's factor on `(symbol, trade_date)` at query time |
| close | float64 | **Unadjusted**; use `load(..., adjust="hfq")` to join the day's factor on `(symbol, trade_date)` at query time |
| volume | int64 | **Shares**. TDX daily bars are in lots while intraday bars are natively in shares, so the intraday path must **not** reuse the daily ×100 conversion |
| amount | float64 | CNY |
| source | string | Provenance column |
| data_version | string | Provenance column |
| fetched_at | timestamp | Provenance column |

**Bar semantics.** The label is the bar's **closing time** (right label): a 1m `09:31` bar covers 09:30–09:31, and a 5m `09:35` bar covers 09:30–09:35; `15:00` includes the closing call auction. The trading session is `09:31–11:30` + `13:01–15:00`, with no bars during the lunch break.

Bars outside the session are filtered out, and the audit check `minute_bars_off_session` reports offending data.

**Minute boundaries can shift.** When the same window is collected again, some trades may be reassigned between adjacent minutes. Take this into account when using absolute per-minute quantities as factors, and reconcile against daily bars after aggregating by day; repeated collection does not guarantee byte-identical bars.

**Minutes with no trades.** TDX's packed-float volume decoding maps a raw 0 to `2**-127` (≈5.88e-39) rather than 0.0 (see `_wire/helper.get_volume`). The intraday path explicitly zeroes these to `volume=0` and `amount=0`, consistent with the lake-wide suspension convention. An illiquid stock has dozens of such minutes a day; a suspended stock has a whole trading day of them.

**History horizon.** The current client limits the backfill window to 95 trading days for 1m and 491 trading days for 5m. Upstream retains data by bar count, so the actual earliest date depends on how actively the instrument trades; the window length is not a guarantee of continuous coverage. For the full mechanism and exceptions, see [catalog.md history horizon](catalog.md); `cne backfill` rejects out-of-range windows outright, and `history_horizon_days` in `list_datasets()` is the programmatic contract.

**Why each dataset holds only one frequency.** The 1m horizon is 95 days and the 5m horizon is 491 days, but a dataset has only one watermark, one `coverage_start` and one `history_horizon_days`. Mixed together, all three would be wrong for both frequencies. `frequency` is still in the schema and the primary key, so the two share the same column definition and the same quality checks.

#### minute_bars_15m / minute_bars_30m / minute_bars_60m

Derived datasets resampled from the 1m and 5m data in the lake (`derive/minute_resample.py`). **Not computed by default**: neither the daily update nor `cne init` produces them; to put them in the lake, run `cne derive minute_bars_15m` manually (likewise for 30m and 60m). Without putting them in the lake, you can also compute them at query time with `resample_minute_history` using the same rules.

For a given stock and day, 1m is used if present, otherwise 5m; if that day has 1m but some interval is missing a component bar, the whole day is skipped rather than falling back to 5m. Suspended stocks often have one 1m bar missing: days with no trades at all are counted in `skipped_halted_1m`, intraday halts and resumptions (bars unbroken, missing only at the start or end) in `skipped_partial_session_1m`; days with trades but a gap in the middle of the bars are counted in `skipped_incomplete_1m`, and the run is recorded as degraded. Without a window, only trading days not yet computed, or whose 1m / 5m content or computation rules have changed, are recomputed (judged by content, so rollbacks and restores also trigger it); this means 1m data filled in later makes the corresponding trading days get recomputed from 1m. `--start` / `--end` limit the window, and `--full` recomputes everything.

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date | Partition column |
| bar_time | timestamp (naive) | The interval's closing minute; aligned from 09:30 in the morning and 13:00 in the afternoon, never spanning the lunch break |
| frequency | string | `15m` / `30m` / `60m`, matching the dataset |
| open | float64 | **Unadjusted**; OHLC counts only input bars with trades; when the interval has no trades at all, the last quote is carried forward |
| high | float64 | **Unadjusted** |
| low | float64 | **Unadjusted** |
| close | float64 | **Unadjusted** |
| volume | int64 | Shares |
| amount | float64 | CNY |
| resampled_from | string | `1m` or `5m`: which input this bar was computed from. 5m open/high/low prices already include quotes carried forward over minutes with no trades, so a few intervals differ from the 1m result |
| source | string | Provenance column, always `derived` |
| data_version | string | Provenance column |
| fetched_at | timestamp | Provenance column; time of derivation |

#### trade_ticks

Trade tick records. **Optional**, off by default (`[trade_ticks].enabled = false`), with its **own** config section and step group (`ticks`); it does not ride along with `[minute_bars]`.

**First, what it is not: it is not trade-by-trade data.** A-share Level-1 is **a snapshot every 3 seconds**, and one record aggregates all real trades within that frame.
There are no per-order records and no 10-level order book.

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date | Partition column; A-shares have no night session, so it always equals the date of `trade_time` |
| tick_seq | int32 | **0-based dense sequence number in ascending time order within the day**; this is the row's identity (see below) |
| trade_time | timestamp (naive) | **Minute precision**, seconds always `00`. Not truncated: the protocol never carried seconds |
| price | float64 | **Unadjusted**; `load(..., adjust="hfq")` provides `adj_price` |
| volume | int64 | **Shares**. The source is in lots; the adapter multiplies by 100, which is confirmed by reconciliation with daily data rather than assumed |
| direction | string | `buy` / `sell` / `neutral` / `after_hours` (see below) |
| source | string | Provenance column |
| data_version | string | Provenance column |
| fetched_at | timestamp | Provenance column |

**Why the primary key is `tick_seq` rather than `trade_time`.** Timestamps have no seconds, so up to 20 records within one minute share an identical timestamp.
Using `(symbol, trade_date, trade_time)` would drop most rows, and **the order within a minute is exactly what makes tick data valuable**.

`tick_seq` is the sequence number within a complete symbol-day. When paging fails, half a day of data must not be published as a complete result; when reading, do not treat the minute-precision timestamp as a unique key.

**`direction` is inferred, not an exchange field.** TDX uses the tick rule to judge which side initiated the trade.

`after_hours` marks **after-hours fixed-price trades** from 15:05–15:30: the price always equals the day's last trade price, and they are **not included in the exchange's daily volume**.
Reconciling with `daily_bars` requires removing after-hours trades first.

**The trading session has four segments**, unlike the two of minute bars: `09:25` (opening call auction, exactly 1 record per symbol-day), `09:30–11:30`, `13:00–15:00`, `15:05–15:30`.
Note that 09:25 and 13:00 are both **real trades**; in minute bars they are not valid bar labels, because bars are labeled by their closing minute.
Ticks outside these segments are rejected by the adapter, and the audit check `trade_ticks_off_session` reports an error.

**There is no `amount` column.** The source does not provide it. You can compute `price × volume` yourself, but be aware that it is an approximation:
several trades at different prices within one frame are merged into one representative price. An estimate must not be treated as the real turnover published upstream.
Putting an approximation that looks like a fact into the lake is worse than letting users compute it themselves.

**Price scaling depends on instrument type.** Stocks ÷100, funds ÷1000 (`SECURITY_COEFFICIENT`).
When the adapter meets an unrecognized prefix it **raises an error rather than falling back to the stock coefficient**: a wrong scale is invisible, since the numbers all still look like prices.

**The history floor is a fixed date, not a rolling window.** The current client uses **2024-01-02** as the earliest request date; check the collected data for actual coverage.
This is `history_floor_date`, a different mechanism from the minute bars' `history_horizon_days`; see [catalog.md history horizon](catalog.md) for details.

**No data for BSE.** TDX has no tick route for `.BJ` and returns empty instead of an error; the adapter raises explicitly, otherwise it would be indistinguishable from "suspended all day".


**Capacity.** Row counts grow with the instrument scope, trading activity and frequency; compression ratio and final disk usage also depend on raw archives, staging and version retention. The default `scope = "index:000300.SH"` takes only one index's constituents; before expanding to the whole market, estimate from the actual partition sizes of a small sample.

#### futures_bars

Per-**contract** daily bars for futures and options, taken from the daily market data files the exchanges publish themselves ([product boundary](../architecture/overview.md)). Each file lists every contract listed that day, **with a settlement price even when there was no trade**. Futures and options are split into two datasets, `futures_bars` / `option_bars`, whose shared columns have the same definitions.

| Column | Type | Description |
|--------|------|-------|
| symbol | string | Canonical symbol: futures `CU2511.SHF` / `TA2601.CZC` / `IF2512.CFE`; options `IO2512C4000.CFE` (underlying + month + C/P + strike) |
| exchange | string | `SHF` / `INE` / `DCE` / `CZC` / `GFE` / `CFE` |
| exchange_code | string | The exchange's raw code (`cu2511`, `TA601`, `IO2512-C-4000`) |
| product | string | Product code, uppercase |
| trade_date | date | Trading day; the night session belongs to the next trading day (exchange convention) |
| open | float64 | **Null when there was no trade**: no trade, no price |
| high | float64 | **Null when there was no trade**: no trade, no price |
| low | float64 | **Null when there was no trade**: no trade, no price |
| close | float64 | **Null when there was no trade**: no trade, no price |
| settle | float64 | Settlement price. Positive for futures; on expiry day, out-of-the-money options are stored as **0** per the exchange file (a real price, not missing); null in the rare case where an expiring contract has no settlement price in the file |
| pre_settle | float64 | Previous settlement price, same definition as `settle` |
| volume | int64 | Lots, **single-sided**. SHFE/INE/DCE/CZCE counted double-sided before 2020-01-01; those values have been halved (a double-sided value must be even; an odd value is rejected) |
| amount | float64 | CNY (the exchange publishes in 10,000 CNY, ×10⁴; also single-sided) |
| open_interest | int64 | Open interest, single-sided |
| oi_change | int64 | Change in open interest, single-sided |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### option_bars

Per-contract daily bars for options, from the same source and with the same definitions as `futures_bars`, plus option-specific fields.

| Column | Type | Description |
|--------|------|-------|
| symbol | string | Canonical symbol: futures `CU2511.SHF` / `TA2601.CZC` / `IF2512.CFE`; options `IO2512C4000.CFE` (underlying + month + C/P + strike) |
| exchange | string | `SHF` / `INE` / `DCE` / `CZC` / `GFE` / `CFE` |
| exchange_code | string | The exchange's raw code (`cu2511`, `TA601`, `IO2512-C-4000`) |
| product | string | Product code, uppercase |
| underlying_symbol | string | Underlying futures contract; for CFFEX options it is an index (IO→`000300.SH`, MO→`000852.SH`, HO→`000016.SH`) |
| option_type | string | `C` / `P` |
| strike | float64 | Strike price (in quote units) |
| trade_date | date | Trading day; the night session belongs to the next trading day (exchange convention) |
| open | float64 | **Null when there was no trade**: no trade, no price |
| high | float64 | **Null when there was no trade**: no trade, no price |
| low | float64 | **Null when there was no trade**: no trade, no price |
| close | float64 | **Null when there was no trade**: no trade, no price |
| settle | float64 | Settlement price. Positive for futures; on expiry day, out-of-the-money options are stored as **0** per the exchange file (a real price, not missing); null in the rare case where an expiring contract has no settlement price in the file |
| pre_settle | float64 | Previous settlement price, same definition as `settle` |
| volume | int64 | Lots, **single-sided**. SHFE/INE/DCE/CZCE counted double-sided before 2020-01-01; those values have been halved (a double-sided value must be even; an odd value is rejected) |
| amount | float64 | CNY (the exchange publishes in 10,000 CNY, ×10⁴; also single-sided) |
| open_interest | int64 | Open interest, single-sided |
| oi_change | int64 | Change in open interest, single-sided |
| exercise_volume | int64 | Exercise volume (not provided in CFFEX files; null) |
| delta | float64 | **Value published by the exchange**; for this lake's own recomputation, see `option_greeks` |
| implied_vol | float64 | IV **published by the exchange**, as a decimal |
| series_implied_vol | float64 | IV published by SHFE/INE per expiry series (not per strike), as a decimal |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

The primary key of both datasets is `(symbol, trade_date)`. Partitioning: `trade_date` (monthly for futures_bars, daily for option_bars).
Validation: futures settlement price > 0, option settlement price ≥ 0; trade prices > 0; a traded row may have only a close (delivery month, exchange for physicals), otherwise OHLC must satisfy the high/low envelope; option |delta| ≤ 1.0001 (SHFE prints deep in-the-money puts as −1.000001). Violating rows are quarantined without bringing down the whole trading day.
`required=false`; enabled by `[futures]`.

#### futures_contracts

Futures contract table, merged into a single file, primary key `symbol`, rebuilt from `futures_bars`. For option contracts, see `option_contracts`; the shared columns of the two tables have the same definitions.

| Column | Type | Description |
|--------|------|-------|
| symbol | string | Canonical symbol: futures `CU2511.SHF` / `TA2601.CZC` / `IF2512.CFE`; options `IO2512C4000.CFE` (underlying + month + C/P + strike) |
| exchange | string | `SHF` / `INE` / `DCE` / `CZC` / `GFE` / `CFE` |
| exchange_code | string | The exchange's raw code (`cu2511`, `TA601`, `IO2512-C-4000`) |
| product | string | Product code, uppercase |
| product_name | string | Chinese product name, from `domain/futures_products.py` (with effective dates); null for unknown products, which also produce an audit finding |
| delivery_month | date | Delivery month (1st of the month) |
| list_date | date | Listing date; see `dates_basis` |
| last_trade_date | date | Last trading day; see `dates_basis` |
| multiplier | float64 | Contract multiplier, from `domain/futures_products.py` (with effective dates); null for unknown products, which also produce an audit finding |
| tick_size | float64 | Minimum price increment, from `domain/futures_products.py` (with effective dates); null for unknown products, which also produce an audit finding |
| quote_unit | string | Quote unit, from `domain/futures_products.py` (with effective dates); null for unknown products, which also produce an audit finding |
| dates_basis | string | `exchange`: given by exchange reference files; `observed` / `observed_truncated` / `vendor_observed` / `open`: describe observational evidence only; listing/last trading dates without an authoritative reference are null, and observed bounds are stored separately in first_seen_date/last_seen_date |
| first_seen_date | date | First trading day it appears in the lake |
| last_seen_date | date | Last trading day it appears in the lake |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### option_contracts

Option contract table, merged into a single file, primary key `symbol`, rebuilt from `option_bars`.

| Column | Type | Description |
|--------|------|-------|
| symbol | string | Canonical symbol: futures `CU2511.SHF` / `TA2601.CZC` / `IF2512.CFE`; options `IO2512C4000.CFE` (underlying + month + C/P + strike) |
| exchange | string | `SHF` / `INE` / `DCE` / `CZC` / `GFE` / `CFE` |
| exchange_code | string | The exchange's raw code (`cu2511`, `TA601`, `IO2512-C-4000`) |
| product | string | Product code, uppercase |
| product_name | string | Chinese product name, from `domain/futures_products.py` (with effective dates); null for unknown products, which also produce an audit finding |
| underlying_symbol | string | Underlying futures contract; for CFFEX options it is an index (IO→`000300.SH`, MO→`000852.SH`, HO→`000016.SH`) |
| underlying_kind | string | `future` / `index` (CFFEX index options) |
| option_type | string | `C` / `P` |
| strike | float64 | Strike price (in quote units) |
| exercise_style | string | `american` / `european`, from `domain/futures_products.py` (with effective dates); null for unknown products, which also produce an audit finding |
| expiry_month | date | Kept for compatibility: the underlying contract's month (1st of the month), not the actual expiry month; use expiry_date for the real expiry |
| list_date | date | Listing date; see `dates_basis` |
| expiry_date | date | Expiry date; see `dates_basis` |
| multiplier | float64 | Contract multiplier, from `domain/futures_products.py` (with effective dates); null for unknown products, which also produce an audit finding |
| tick_size | float64 | Minimum price increment, from `domain/futures_products.py` (with effective dates); null for unknown products, which also produce an audit finding |
| dates_basis | string | `exchange`: given by exchange reference files; `observed` / `observed_truncated` / `vendor_observed` / `open`: describe observational evidence only; listing/expiry dates without an authoritative reference are null, and expiry must not be inferred from the last bar |
| first_seen_date | date | First trading day it appears in the lake |
| last_seen_date | date | Last trading day it appears in the lake |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### futures_continuous

Main / second-main continuous contracts (derived, `derive/futures_continuous.py`, rule `oi_t-1_v2`).

| Column | Type | Description |
|--------|------|-------|
| symbol | string | Product, e.g. `CU.SHF`, `IF.CFE` |
| series | string | `main` / `second` |
| trade_date | date | Trading day |
| contract_symbol | string | The specific contract used that day |
| open | float64 | That contract's raw market data for the day |
| high | float64 | That contract's raw market data for the day |
| low | float64 | That contract's raw market data for the day |
| close | float64 | That contract's raw market data for the day |
| settle | float64 | That contract's raw market data for the day |
| volume | int64 | That contract's raw market data for the day |
| open_interest | int64 | That contract's raw market data for the day |
| rolled | bool | Whether a roll happened that day |
| adj_ratio | float64 | Cumulative roll ratio factor (null after a roll price or the previous trading day is missing): raw price × `adj_ratio` gives the continuous series (the earliest prices stay as they are); dividing by the latest factor gives the back-adjusted view |
| adj_diff | float64 | Cumulative roll difference factor (null once invalidated): raw price + `adj_diff` gives the continuous series; subtracting the latest factor gives the back-adjusted view |
| roll_yield | float64 | main only: ln(main/second) ÷ the gap between their delivery months (in years); positive means backwardation |
| rule | string | `oi_t-1_v2` |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

Rolls are decided only by open interest at the T-1 close; data from day T itself plays no part. Rolls only move to further-out months; a contract with a known last trading day gives way 5 calendar days before it, and one with an unknown last trading day gives way on entering its delivery month. If there is no valid further-out month, that series is missing rather than falling back; a missing previous trading day is not replaced by an earlier observation date. Primary key `(symbol, series, trade_date)`, partitioned monthly, fully rebuilt from futures_bars each time.

#### option_greeks

This lake's own option implied volatility and Greeks (derived, `derive/option_greeks.py`). The exchange-published Delta/IV stay as they are in `option_bars`; this is a recomputation on a single consistent basis.

| Column | Type | Description |
|--------|------|-------|
| symbol | string | Canonical option symbol, same as `option_bars` |
| trade_date | date | Trading day |
| underlying_symbol | string | Same as `option_bars` |
| underlying_price | float64 | For commodity options, the same-day settlement price of the underlying future; for CFFEX index options, the forward implied by put-call parity |
| forward_source | string | `future_settle` / `parity` (median over the 3 strikes closest to at-the-money) |
| time_to_expiry | float64 | Calendar days ÷ 365; expiry day counts as 1 day (IV is not solved on that day; see `status`) |
| rate | float64 | `shibor_3m` (decimal); 2% when the lake does not have it |
| rate_source | string | `shibor_3m` / `fallback_constant` |
| model | string | `black76` (European) / `baw` (American, Barone-Adesi–Whaley, zero cost of carry) / `unknown` (exercise style missing; not computed) |
| iv | float64 | Solved from the settlement price, as a decimal |
| delta | float64 | Per unit change in the underlying price |
| gamma | float64 | Per unit change in the underlying price |
| vega | float64 | For a volatility change of 1.00 |
| theta | float64 | Per year |
| rho | float64 | For a rate change of 1.00 |
| status | string | `ok`; otherwise the reason: `expiry_day` (expiry day: the settlement price is the exercise value, with no time value to solve for), `below_intrinsic` (settlement price below the model's lower bound, common deep in the money: GFEX rounds down to the tick grid and is off by 1 tick; CFFEX deep in-the-money puts are inconsistent with the parity forward), `above_bound`, `no_underlying`, `no_expiry`, `no_exercise_style`, `no_price` |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

Primary key `(symbol, trade_date)`, partitioned daily. Content fingerprints of market data, contracts, rates, model code and output are stored; a change in any dependency, a missing receipt or a stale result triggers recomputation. `--start/--end` specify a window and `--full` forces a full recompute; a partial recompute does not wrongly mark other dates as updated and does not move the watermark back. See the [derivatives research guide](../recipes/derivatives.md) for details.

#### futures_minute_bars

Futures 1-minute bars, covering only the contracts specified by `[futures] minute_products` / `minute_contracts`.

| Column | Type | Description |
|--------|------|-------|
| symbol | string | Canonical contract symbol, same as `futures_bars` |
| exchange | string | Same as `futures_bars` |
| trade_date | date | Trading day; **night-session bars belong to the next trading day** (Friday 21:05 and Saturday 00:30 both belong to the following Monday), so here `trade_date` does not equal `bar_time.date()` |
| bar_time | timestamp (naive) | The bar's **closing** minute (the first bar after the 09:00 open is labeled 09:01), Beijing time |
| frequency | string | `1m` |
| open | float64 | Null for minutes with no trades |
| high | float64 | Null for minutes with no trades |
| low | float64 | Null for minutes with no trades |
| close | float64 |  |
| volume | int64 |  |
| open_interest | int64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

Primary key `(symbol, bar_time)`, partitioned daily. Sina keeps only the most recent 1023 bars per contract, so data can only accumulate forward from the day it is enabled, and it must run every trading day.

#### commodity_bars

Daily bars of domestic commodity futures **main continuous** contracts (EastMoney main continuous) + a narrow set of overseas markets (Sina COMEX gold ``GC0.CMX``).

| Column | Type | Description |
|--------|------|-------|
| symbol | string | Domestic `{root}0.{exchange}` (e.g. `AU0.SHF`); overseas `GC0.CMX` (COMEX gold continuous) |
| name | string | Chinese contract name |
| exchange | string | `SHF` / `DCE` / `CZC` / `INE` / `GFE` / `CMX` |
| trade_date | date | Session date of the source exchange (COMEX calendar for overseas; aligning with A-shares is an as-of join on the research side) |
| open | float64 |  |
| high | float64 |  |
| low | float64 |  |
| close | float64 |  |
| volume | int64 | Lots (EastMoney basis; often 0 for Sina overseas data) |
| amount | float64 | Turnover (nullable for overseas) |
| open_interest | float64 | Nullable |
| source | string | Provenance (`eastmoney` / `sina`) |
| data_version | string | Provenance (`eastmoney` / `sina`) |
| fetched_at | timestamp | Provenance (`eastmoney` / `sina`) |

Primary key: `(symbol, trade_date)`. Partition: `trade_date`.

Daily update: `macro_risk` group. History: `cne backfill commodity_bars [--start 2020-01-01 --end …]`.

`required=false`. Overseas v1 covers **gold only**; not fed into the A-share backtest engine.

#### corporate_actions

| Column | Type | Description |
|--------|------|-------|
| payment_date | date | Nullable; the cash payment date reported by the source, not earlier than `ex_date`; when unknown it is not replaced by the ex-date |
| payment_source | string | Nullable; evidence for the payment date: `issuer_notice:…` (issuer announcement) or `baostock:dividPayDate`; when merging, evidence grade takes precedence over fetch recency |
| symbol | string |  |
| ex_date | date |  |
| action_type | string | cash_dividend/bonus/transfer/allotment/unit_split/reorg_transfer |
| cash_dividend | float64 | **Per share** (CNY, pre-tax) |
| bonus_ratio | float64 | **Per share** (bonus shares: shares issued per 1 share held) |
| transfer_ratio | float64 | **Per share** (capitalization shares: shares issued per 1 share held) |
| split_factor | float64 | Unit split/consolidation: new units ÷ original units; neutral value 1 |
| allotment_ratio | float64 | **Per share** (rights issue: shares offered per 1 share held); nullable |
| allotment_price | float64 | Rights issue price (CNY/share), **not** a ratio; nullable |
| reference_price | float64 | `reorg_transfer` only: the ex-rights (ex-dividend) reference price published by the issuer (CNY/share); must be null for other types |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

> **Unit contract (per share).** All ratios and amounts are relative to "1 share held",
> not the "per 10 shares" basis common in TDX (`xdxr`) and EastMoney. Before staging, the adapter
> divides source-side "per 10 shares" values by 10 (e.g. "CNY 8.5 per 10 shares" → 0.85, "8 bonus shares per 10" → 0.8,
> "4 capitalization shares per 10" → 0.4, "3 rights shares per 10" → 0.3). Downstream accounting works on actual holdings with no further division by 10:
> `shares_after = shares × (1 + bonus_ratio + transfer_ratio) × split_factor`,
> `cash = shares × cash_dividend`. `allotment_price` is a per-share price, not a ratio, and is not divided by 10.
> `cash_dividend` is the declared per-share amount. When a company holds repurchased shares, the exchange computes the ex-rights reference price from a virtual per-share amount spread over total shares, which is smaller, so the adjustment factor steps slightly less (up to about 0.5% observed).
> Note: TDX `xdxr` does not split bonus and capitalization shares; it writes their sum to `bonus_ratio` (`transfer_ratio=0`);
> the total multiplier is correct, but the bonus/capitalization split is distinguishable only on the EastMoney daily path. An EastMoney plan that
> includes a cash dividend, bonus shares and capitalization shares together is split into several `(symbol, ex_date, action_type)` records, so that a single `action_type`
> does not zero out the other distribution components.

`unit_split` separately represents a fund unit split or consolidation: 1 unit becoming 3 is recorded as `split_factor=3`,
10 units consolidated into 1 as `0.1`, without pretending to be bonus or capitalization shares. A missing/null `split_factor` in old records is interpreted as 1;
new split records must give an explicit, finite, positive ratio not equal to 1. Adjustment verification includes the split multiplier in the ex-rights
reference price calculation; conflicting split ratios cannot be resolved automatically by taking the maximum. The split date must be the effective ex-date for trading,
not the record date or announcement date. This field does not represent an amount, and the "divide per-10-share values by 10" conversion does not apply.

`reorg_transfer` represents capital-reserve share conversion in a bankruptcy reorganization: `transfer_ratio` is still the number of shares issued per 1 share held,
and holdings are computed with the formula above; but most converted shares go to creditors and reorganization investors, so the ex-rights price is not computed as `1 + transfer_ratio`;
it is determined by the ex-rights (ex-dividend) reference price the issuer publishes under exchange rules. For such records, adjustment verification uses
`previous close ÷ reference_price` as the day's step instead of the bonus/capitalization formula. A missing or non-positive `reference_price`,
or one present on another type, is rejected at write time.

#### adj_factors

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| adjust_type | string | qfq/hfq |
| factor | float64 | Cumulative factor; qfq: `1/sina_qfq_factor`, hfq: `sina_hfq_factor` |
| source | string | sina (default) |
| data_version | string |  |
| fetched_at | timestamp |  |

#### financial_statement_items

Point-in-time (PIT) queries **must** filter `announce_date <= as_of` on the read side
(`load(..., as_of=)`); never align fundamentals by `report_period` alone.

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| report_period | string | e.g. ``2024Q1`` |
| statement_type | string | income / balance / cashflow / indicator |
| item_code | string | See the table below |
| item_value | float64 | Amounts in CNY; ratios as percentages; per-share items in CNY/share |
| announce_date | date | **PIT axis**: first disclosure date (from the earnings report `RPT_LICO_FN_CPD`) |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

**item_code overview** (by `statement_type`):

| statement_type | item_code |
|----------------|-----------|
| income | `revenue` `operating_cost` `operating_profit` `total_profit` `net_profit` `net_profit_deducted` `income_tax` `sale_expense` `manage_expense` `finance_expense` |
| balance | `total_assets` `total_equity` `total_liabilities` `inventory` `accounts_receivable` `monetary_funds` `fixed_assets` |
| cashflow | `net_cash_operate` `net_cash_invest` `net_cash_finance` `capex` `end_cash` |
| indicator | `roe` `eps` `eps_deducted` `bps` `gross_margin` `ocf_per_share` `revenue_yoy` `net_profit_yoy` |

Definition notes:

- `total_equity` is **total shareholders' equity** (including minority interests), not equity attributable to the parent; mind the numerator when computing B/P,
  or use `bps` (book value per share) × share count instead.
- `capex` is "cash paid to acquire and construct fixed assets, intangible assets and other long-term assets", a proxy rather than strict capital expenditure.
- **Backfilled values are restated**: EastMoney only provides the *current* version of a period's financial data. Backfill gets restated values
  but pairs them with the first disclosure date, so the registry marks `financial_statement_items` as
  `pit_quality="reconstructed"` (legacy alias `pit_grade="partial"`) rather than strict PIT.
  `load(..., pit_mode="strict")` rejects these rows; `pit_mode="best_effort"` can read them,
  but you must check the returned `pit_is_exact` / `pit_quality`. Only versions accumulated day by day that also store the real
  `available_at`/`observed_at` can be called strict PIT.
- **History depth**: `cne backfill financial_statement_items` by default uses EastMoney report periods from **2001** onward
  (chunk with `--start` / `--end`); it does not use baostock. For the actual start on disk, see `list_datasets().coverage_start`.

#### fund_flow

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| main_net_inflow | float64 | CNY |
| super_large_net_inflow | float64 |  |
| large_net_inflow | float64 |  |
| medium_net_inflow | float64 |  |
| small_net_inflow | float64 |  |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### fund_flow_ths

THS per-stock fund flow, written only when push2 cannot provide `fund_flow` (`steps/ths_fallback.py`).

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date | The THS page carries no date; it is stamped with the trading day it describes, from after the close until before the next trading day's open |
| inflow | float64 | Inflow, CNY (4 significant digits) |
| outflow | float64 | Outflow, CNY |
| net_inflow | float64 | Net = inflow − outflow, CNY; **not** EastMoney's main net inflow |
| amount | float64 | Turnover, CNY |
| change_pct | float64 | Price change, % |
| turnover_pct | float64 | Turnover rate, % |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### margin_trading

The exchange sources collect Shanghai and Shenzhen separately. When one exchange has not yet provided data or later paging fails, already validated data can still be published, but that date is marked as partially covered and a backfill gap is kept; the gap is cleared once a complete retry is published. The EastMoney source also checks coverage of both markets, and a single-market response is not treated as a complete date. `short_balance`, which SSE does not provide, stays null.

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| margin_balance | float64 |  |
| margin_buy | float64 |  |
| short_balance | float64 |  |
| short_sell_volume | float64 |  |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### northbound_holdings

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| channel | string | Shanghai/Shenzhen Connect |
| holding_shares | float64 |  |
| holding_mv | float64 |  |
| holding_ratio | float64 |  |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### northbound_flows

| Column | Type | Description |
|--------|------|-------|
| trade_date | date |  |
| channel | string | SH / SZ |
| net_buy | float64 |  |
| buy_amount | float64 |  |
| sell_amount | float64 |  |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### valuation_metrics

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| pe_ttm | float64 | Trailing-twelve-month P/E (TTM); null for EastMoney push2 rows |
| pb | float64 |  |
| ps_ttm | float64 |  |
| total_mv | float64 | CNY; for the basis, see `total_mv_basis` |
| float_mv | float64 | CNY; for the basis, see `float_mv_basis` |
| pe_dynamic | float64 | Dynamic P/E (latest period annualized, push2 `f9`); a different basis from `pe_ttm`, do not mix them |
| total_mv_basis | string | `vendor_reported`, `close_x_share_structure` (close × total shares in effect that day) or `close_x_year_end_shares_estimate` (close × total shares at the previous year end, an estimate); nullable in old rows |
| float_mv_basis | string | `vendor_reported`, `close_x_turn_implied_shares` (close × float shares implied by turnover rate), `close_x_turn_implied_shares_repaired` (converted from the old average-trade-price basis by close/average price) or `vwap_x_turn_implied_shares` (old average-trade-price basis, with no consistent quotes to convert); nullable in old rows |
| shares_as_of | date | Effective or statistics date of the share count used to compute total market value; null for vendor-reported values |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### sector_members

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| sector_code | string |  |
| sector_name | string |  |
| as_of_date | date | Snapshot date |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### announcement_index

PIT queries filter `announce_date <= as_of`.

| Column | Type | Description |
|--------|------|-------|
| announcement_id | string | Primary key |
| symbol | string |  |
| title | string |  |
| announce_date | date | **PIT axis** |
| category | string |  |
| url | string |  |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### earnings_disclosure_schedule

Scheduled disclosure calendar (EM datacenter `RPT_PUBLIC_BS_APPOIN`, mirroring the SSE/SZSE disclosure calendars).
Current-value semantics, not PIT: a schedule change overwrites `scheduled_date`, `first_scheduled_date` keeps the first scheduled date,
and `actual_date` is filled in after actual disclosure (null before then).

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| report_period | string | e.g. ``2026Q2`` (partition key) |
| scheduled_date | date | Currently effective scheduled disclosure date |
| first_scheduled_date | date | First scheduled disclosure date |
| actual_date | date | Actual disclosure date; null if not yet disclosed |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### dragon_tiger

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| reason | string |  |
| buy_amount | float64 |  |
| sell_amount | float64 |  |
| net_amount | float64 |  |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### block_trades

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| price | float64 | CNY/share |
| volume | float64 | **10,000 shares** (the basis of EastMoney's original table, also kept by the exchange fallback source) |
| amount | float64 | **10,000 CNY** |
| premium_ratio | float64 | Discount/premium relative to the close |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### index_constituents

| Column | Type | Description |
|--------|------|-------|
| index_symbol | string | e.g. ``000300.SH`` |
| symbol | string | Constituent |
| as_of_date | date | Snapshot / rebalance date |
| weight | float64 | Weight ratio, nullable; EastMoney/CNI adjustment history writes null; official file percentages become ratios summing to approximately 1 |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### industry_members

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| classification_system | string | e.g. ``sw``, ``eastmoney`` |
| industry_code | string | Level-3 (leaf) industry code; see below for EastMoney rows with `data_version=v1` |
| industry_name | string | Level-3 industry name; equals the code on Shenwan rows |
| l1_code | string | Level-1 industry code, nullable |
| l1_name | string | Level-1 industry name, nullable |
| l2_code | string | Level-2 industry code, nullable |
| l2_name | string | Level-2 industry name, nullable |
| l3_code | string | Level-3 industry code, nullable |
| l3_name | string | Level-3 industry name, nullable |
| as_of_date | date | Classification snapshot date |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

One row per security, classification system and snapshot date. The EastMoney datacenter lists every stock under its level 1, 2 and 3 industry boards without saying which is which:
a bigger board is a higher level, and where a level has a single child the two have the same members and the name suffix (`Ⅱ`/`Ⅲ`) tells them apart.
Shenwan rows carry only the six-digit level-3 code; the level-1 and level-2 codes are that code zero-filled (`480101` → `480100` → `480000`). The Shenwan file publishes no names, so the name columns are null.

EastMoney rows with `data_version=v1` (written before 0.20) kept one of the three boards, chosen by the source's page order, so the level varies from day to day.
The `industry-levels` step of `cne migrate` fills the level columns for the stored board and the levels above it; the levels below stay null and `industry_code` is unchanged.
For counts by level, read `l1_*`..`l3_*`, not `industry_code` on those rows.

An industry code is not an identity on its own: Shenwan reused its old code strings when it replaced its taxonomy on 2021-07-30.
`cne derive industry_taxonomy` writes each industry as `industry_id = system:version:code` (e.g. `sw:2014:480101`, `sw:2021:480101`, `eastmoney:board:1279`)
to `meta/industry_taxonomy.parquet`, with its level, name, `parent_id`, first and last snapshot date and largest member count; the Shenwan version follows the snapshot date.
`cne derive industry_crosswalk` compares the members of Shenwan and EastMoney industries at the same level on a day both have a full snapshot, and adds each pair's overlap evidence and relation
(`equivalent`, `left_within_right`, `right_within_left`, `partial`) to `meta/industry_crosswalk.parquet`; it is evidence only and merges no identities.

#### macro_indicators

| Column | Type | Description |
|--------|------|-------|
| indicator_id | string | e.g. ``shibor_3m``, ``cnbond_yield_10y``, ``lpr_1y`` |
| obs_date | date | Observation / release date |
| value | float64 |  |
| frequency | string | ``daily`` / ``monthly`` |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### market_breadth

Computed from curated ``daily_bars`` relative to the previous trading day.

| Column | Type | Description |
|--------|------|-------|
| trade_date | date |  |
| metric_id | string | ``advance_count``, ``decline_count``, ``limit_up_count``, etc. |
| value | float64 |  |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### share_unlock_schedule

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| unlock_date | date | Scheduled unlock date |
| unlock_shares | float64 |  |
| unlock_ratio | float64 | Share of float/total shares (depending on source) |
| unlock_type | string | e.g. IPO lock-up, private placement |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### regulatory_events

| Column | Type | Description |
|--------|------|-------|
| event_id | string | Primary key |
| symbol | string |  |
| event_date | date | Announcement date |
| event_type | string | ``penalty``, ``investigation``, ``regulatory_letter``, etc. |
| title | string |  |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### institutional_holdings

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| holder_type | string | ``fund``, ``qfii``, ``social_security``, etc. |
| report_period | string | e.g. ``2024Q1`` |
| holding_shares | float64 | Number of shares held or number of holders (depending on source) |
| holding_ratio | float64 | Percentage of float/total shares |
| holding_mv | float64 | Market value |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### analyst_consensus

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| forecast_date | date | Publication / update date |
| forecast_year | int64 | Target fiscal year |
| eps_forecast | float64 | Consensus EPS |
| pe_forecast | float64 | Implied P/E |
| target_price | float64 | Average target price |
| rating | string | e.g. buy/overweight |
| analyst_count | int64 | Number of covering institutions |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### sentiment_scores

Two channels: ``announcement_keywords`` (announcement titles) and ``stock_news_nlp`` (EastMoney per-stock news + keywords/SnowNLP).

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| score_channel | string | Primary-key dimension; ``announcement_keywords`` / ``stock_news_nlp`` |
| sentiment_score | float64 | [-1, 1] |
| headline_count | int64 | Number of headlines counted in the score |
| source | string | Provenance |
| data_version | string | Provenance |
| fetched_at | timestamp | Provenance |

#### stock_news (on-demand cache) {#stock_news按需缓存}

Cached JSON: ``meta/on_demand/stock_news/{symbol}.json``; fetched via ``cne query --dataset stock_news --symbol``.

| Field | Type | Description |
|-------|------|-------|
| symbol | string | |
| items[].news_id | string | |
| items[].title | string | |
| items[].publish_time | string | |
| items[].publish_date | string | ISO date when parseable |
| items[].sentiment_score | float64 | Per-item NLP score |
| items[].sentiment_method | string | ``keyword`` / ``snownlp`` / ``keyword+snownlp`` |
| aggregate_sentiment | float64 | Mean of item scores |
| headline_count | int64 | |
| source / data_version / fetched_at | | Provenance |

#### share_structure

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| change_date | date |  |
| total_shares | float64 |  |
| float_shares | float64 |  |
| restricted_shares | float64 |  |
| float_a_shares | float64 |  |
| free_float_shares | float64 |  |
| change_reason | string |  |
| announce_date | date |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### shareholder_counts

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| count_date | date |  |
| holder_count | float64 |  |
| holder_count_change_pct | float64 |  |
| avg_float_shares | float64 |  |
| avg_holding_value | float64 |  |
| announce_date | date |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### top_holders

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| record_date | date |  |
| holder_scope | string |  |
| holder_rank | int32 |  |
| holder_name | string |  |
| holding_shares | float64 |  |
| holding_pct | float64 |  |
| is_institution | bool |  |
| holder_type | string |  |
| announce_date | date |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### industry_index

| Column | Type | Description |
|--------|------|-------|
| trade_date | date |  |
| industry_code | string |  |
| level | string |  |
| weighting | string |  |
| ret | float64 |  |
| n_members | int64 |  |
| n_priced | int64 |  |
| n_excluded | int64 |  |
| amount | float64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### hot_rank

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| rank | int64 |  |
| rank_change | int64 |  |
| hist_rank | int64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### sector_bars

| Column | Type | Description |
|--------|------|-------|
| sector_code | string |  |
| sector_name | string |  |
| board_type | string |  |
| trade_date | date |  |
| open | float64 |  |
| high | float64 |  |
| low | float64 |  |
| close | float64 |  |
| volume | int64 |  |
| amount | float64 |  |
| change_pct | float64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### sector_fund_flow

| Column | Type | Description |
|--------|------|-------|
| sector_code | string |  |
| sector_name | string |  |
| board_type | string |  |
| trade_date | date |  |
| main_net_inflow | float64 |  |
| change_pct | float64 |  |
| turnover_pct | float64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### sector_fund_flow_ths

THS industry and concept sector fund flow, written only when push2 cannot provide `sector_fund_flow`; the sector classification is THS's own and does not map to EastMoney sector codes.

| Column | Type | Description |
|--------|------|-------|
| sector_code | string | THS sector code (industries 881xxx; concepts taken from the detail-page link) |
| sector_name | string |  |
| board_type | string | industry / concept |
| trade_date | date | Same as fund_flow_ths |
| sector_index | float64 | Sector index points |
| change_pct | float64 | Price change, % |
| inflow | float64 | Inflow, CNY (the page unit is 100 million, precise to 0.01) |
| outflow | float64 | Outflow, CNY |
| net_inflow | float64 | Net, CNY |
| company_count | int64 | Number of companies |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### news_headlines

| Column | Type | Description |
|--------|------|-------|
| news_id | string |  |
| publish_date | date |  |
| publish_time | string |  |
| title | string |  |
| summary | string |  |
| related_symbols | string |  |
| channel | string |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### flash_news_wire

| Column | Type | Description |
|--------|------|-------|
| wire_id | string |  |
| wire_source | string |  |
| item_hash | string |  |
| publish_date | date |  |
| publish_time | string |  |
| title | string |  |
| summary | string |  |
| related_symbols | string |  |
| importance | int8 |  |
| channel | string |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### economic_calendar

| Column | Type | Description |
|--------|------|-------|
| event_id | string |  |
| indicator_code | string |  |
| event_date | date |  |
| event_time | string |  |
| country | string |  |
| indicator | string |  |
| importance | int8 |  |
| forecast | float64 |  |
| previous | float64 |  |
| actual | float64 |  |
| unit | string |  |
| withdrawn | bool |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### delisting_events

| Column | Type | Description |
|--------|------|-------|
| symbol | string |  |
| first_trade_date | date |  |
| last_trade_date | date |  |
| ending_pattern | string |  |
| final_close | float64 |  |
| halt_gap_days | int64 |  |
| worst_final_return | float64 |  |
| final_window_return | float64 |  |
| bars | int64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### economic_events

| Column | Type | Description |
|--------|------|-------|
| event_id | string |  |
| event_date | date |  |
| event_time | string |  |
| time_status | string |  |
| country | string |  |
| importance | int8 |  |
| people | string |  |
| content | string |  |
| withdrawn | bool |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### market_holidays

| Column | Type | Description |
|--------|------|-------|
| holiday_id | string |  |
| holiday_date | date |  |
| country | string |  |
| exchange | string |  |
| name | string |  |
| note | string |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

### Compact deduplication

During compact, rows are grouped by primary key and the row with the largest `fetched_at` is kept.

### DuckDB views

Prefer the views in `{data_root}/duckdb/cnequity.duckdb` generated by `cne init` / compact
over hand-written globs over a whole layer. `hive_partitioning=true` applies **only** to datasets partitioned by day
(directory values `YYYY-MM-DD`); yearly/monthly partitions must use `hive_partitioning=false` (the real date is in a file column).
See [lake-layout](../architecture/lake-layout.md).

```sql
-- daily_bars / adj_factors are partitioned by day, so hive=true is safe
CREATE VIEW daily_bars_view AS
SELECT * FROM read_parquet('{root}/curated/daily_bars/**/*.parquet', hive_partitioning=true);

CREATE VIEW daily_bars_adj AS
SELECT b.*, b.close * a.factor AS adj_close
FROM daily_bars_view b
LEFT JOIN read_parquet('{root}/derived/adj_factors/**/*.parquet', hive_partitioning=true) a
  ON b.symbol = a.symbol AND b.trade_date = a.trade_date AND a.adjust_type = 'qfq';

-- Counterexample: for yearly-partitioned datasets such as index_bars, hive must be off, or the directory "1993" pollutes the DATE column
-- SELECT * FROM read_parquet('.../index_bars/**/*.parquet', hive_partitioning=false);
```
