---
description: load() query guide: China A-share price adjustment, universes, point-in-time financials and common pitfalls.
---

# Query guide

The recommended path for downstream reads is `cnequity.query.load()`. This page covers price adjustment, universes, PIT and common pitfalls.

For API signatures see the [Python API reference](../reference/python-api.md).

## Choosing a read entry point

| Entry point | What it provides | What you handle yourself |
|---|---|---|
| `load()` | Materialized data, with price adjustment, PIT, universe and strict validation applied according to the parameters | Choosing the mode and evidence window explicitly |
| `load_with_receipt()` | Pins the available revisions and returns a receipt covering row provenance, PIT and observation coverage | Saving the receipt alongside research outputs; verifying completeness separately |
| `scan()` | Raw LazyFrame, date and symbol filtering, version selection | No price adjustment, PIT or universe |
| `cne query --sql` | Read-only SELECT over local views | Validating the SQL's definitions and coverage |
| Reading Parquet directly | Reads the open files without the runtime | Versions, deduplication, PIT, price adjustment and universe |

While initialization is in progress, only `load("daily_bars", ...)` with `symbols` can read unadjusted daily bars that are sealed but not yet published. Reads that do not specify symbols still see only published data.

## Basic usage

```python
from cnequity.query import load, scan, list_datasets

# Materialized DataFrame
df = load("daily_bars", start="2024-01-01", end="2024-12-31")

# Lazy scan (large windows)
lf = scan("daily_bars", start="2020-01-01", end="2024-12-31")

# Overview of datasets in the lake (includes history_mode / backfill_source / coverage_*)
meta = list_datasets()  # or list_datasets(config=cfg)
# snapshot_only → no honest history; coverage_start is the start of the on-disk partitions
```

Config resolution order: `config=` → `data_root=` → `CNE_CONFIG` → `configs/cnequity.toml` in the current directory → the per-user `~/.config/cnequity/config.toml`

Domestic equity index ETFs use a separate current snapshot, `etf_profiles`. Filter out snapshots later than the research date,
then pick each fund's latest record as of that date; only `eligibility_status="eligible"` may enter this scope.
The SZSE catalog itself does not provide fields sufficient to verify an index's asset class; eligibility can be upgraded only after the official
index methodology, with an explicitly domestic A-share sample space, has been matched by index code and archived; all other rows stay `unverified`;
dates with no historical snapshot cannot be back-filled with today's classification either. `instruments.asset_type="etf"` is the old quote
convention, still mixed with LOFs, and is not proof of eligibility.

`list_datasets()` is the research side's **data discovery entry point**; its boundaries do not prove there are no gaps in between: `history_mode` distinguishes `by_date` / `snapshot_with_backfill` / `snapshot_only`; `coverage_start` is resolved from published data and partition boundaries (including `report_period=YYYYQn`). See [Dataset catalog — History availability](catalog.md#history-availability-history_mode).

## Price adjustment (adjust)

Applies only to datasets with price/volume columns (mainly `daily_bars`).

| Parameter | Behavior |
|------|------|
| `adjust=None` | Raw unadjusted OHLC |
| `adjust="hfq"` | Multiplies prices by the hfq factors; adds columns `adj_open`…`adj_close`, `adj_is_exact` |
| `adjust="qfq"` | Normalizes the hfq factors to the latest bar in the query window as the anchor |

```python
bars = load(
    "daily_bars",
    start="2020-01-01",
    end="2024-12-31",
    adjust="hfq",
)
```

### strict_adj

```python
bars = load("daily_bars", start="2024-01-01", adjust="hfq", strict_adj=True)
```

- `True`: rows with missing factors raise `ReaderError`; no 1.0 fill
- `False` (default): when factors are missing, `adj_is_exact=False` and prices degrade to factor=1.0

**Research recommendation**: use `hfq` + `strict_adj=True` for quantitative backtests; the qfq window anchor drifts, which makes it unsuitable for reproducible long-term research.

When you need fixed results, use `revision_map` to pin prices, factors and universe dependencies together, and keep the required quality evidence, configuration and software version. For the exact guarantees and snapshot boundaries see [Python API](../reference/python-api.md#pinned-data-versions).

`index_bars` are index levels, not security prices, and do not support `adjust=`; use the raw index levels directly.

### Total-return adjustment (total_return) {#total-return-adjustment-total_return}

```python
bars = load("daily_bars", start="2024-01-01", adjust="total_return", symbols=["510300.SH"])
```

The meaning of `hfq` / `qfq` is unchanged: for stocks, the hfq factors already include dividend reinvestment; for funds (ETF/LOF), the hfq factors come from Sina's fund factors, change only with unit splits and consolidations, **exclude cash distributions**, and therefore measure price return.

`adjust="total_return"` applies only to `daily_bars`: on top of hfq, each fund cash distribution is reinvested at the close of the trading day before the ex-date (the factor is multiplied by `prev_close / (prev_close − distribution per unit)`); stocks are the same as `hfq`. Each fund's price level on the first day of the query range equals the hfq level, so when comparing queries with different start dates, compare returns rather than price levels. The required dependencies (`corporate_actions`, `instruments`) are recorded in the read receipt as well.

Choose by purpose: unadjusted prices for price action; `hfq` for split-comparable price comparisons; `total_return` for investors' actual returns or for backtests.

### Storage conventions

- `daily_bars` in the lake stores **unadjusted** prices
- `adj_factors` lives in `derived/` and stores only `hfq` (`adjust_type="hfq"`)
- Recorded ETF/LOF splits and share transfers are corrected during daily factor derivation; omissions left by 0.17 or earlier are rebuilt by `cne migrate`, using source `derived_fund_actions`. Other vendor steps are preserved, while cash distributions remain separately included by total-return reads.
- The DuckDB views `daily_bars_adj` / `daily_bars_hfq` / `daily_bars_qfq` follow the semantics above; the table macro `daily_bars_total_return(start, end)` corresponds to `adjust="total_return"` and outputs `tr_*` and `adj_close`

## Universe filtering

```python
bars = load(
    "daily_bars",
    start="2024-01-01",
    profile="cn_a_sh_sz_research_v1",
)
```

`cn_a_sh_sz_research_v1` is explicitly limited to the Shanghai and Shenzhen markets, and at read time it strictly verifies security identity,
daily trading status, historical ST and delisting evidence; it refuses to return data when receipts are missing. It does not hide the BSE gap
under an "all A" label. To include BSE you can explicitly choose
`cn_a_all_experimental_v1`, but that profile is still marked experimental and should not be treated as an accepted research definition.
For how to use profile versions and hashes see [Universe profiles](../reference/universe-profiles.md).

The old `universe="all_a"` / `"all_a_sh_sz"` parameters remain available for compatibility; the former emits a
`DeprecationWarning`, and neither automatically becomes a strict profile. Base rules of the compatibility filter:

1. **instruments**: `list_date <= trade_date`, and either not delisted or `delist_date >= trade_date` (`delist_date` is the last listed session)
2. Exclude CDRs (689xxx.SH)
3. **trading_status** (only on dates with data): drop ST/*ST and suspended securities

### Historical ST limits

The daily update fetches only the current day's `trading_status` (the exchange board records same-day suspensions). After `init` and `cne backfill daily_bars`, suspensions are rebuilt automatically over the daily-bar window; earlier history can be rebuilt year by year with `cne derive trading_status --start/--end`, with coverage able to start at the same point as `daily_bars` (about 2001). **ST labels** come from Baostock's per-symbol `isST` history together with the optional Tushare BJ history source; a window can be used for research only once a complete, versioned `historical_st_evidence` receipt has been generated for it. A partial backfill (for example, only from 2016) does not prove that historical ST has been excluded since 2001.

Receipts can be merged from an overlapping deep-history range and a newer tail range, but newly added symbols must have first-trading-day evidence. BSE (BJ) securities can be backfilled through an explicitly configured Tushare Pro: `bak_basic` historical short names for 2016, and `stock_st` from 2017-01-01; the API requires a token, years before 2016 still block as a source capability limit, and an empty API result must not be taken as normal. Without Tushare configured, BJ still blocks explicitly. The audit item `trading_status_coverage_start` distinguishes overall coverage from `st_coverage_start`; historical research should re-check with `cne audit --full --research-start ...`.

Explicit research profiles enable strict validation automatically. If you still use the compatibility parameters, add
`strict_universe=True` to validate daily status and historical ST evidence for the requested range; missing receipts, or
BJ symbols with no historical ST source, raise `UniverseCoverageError`.
The default compatibility read is fine for exploration but should not be used directly as input to long-history backtests.

### Exchange coverage

`all_a` covers Shanghai, Shenzhen and Beijing. The BSE securities list and short names are filled from the BSE quote board during the daily update; do not conflate TDX list enumeration with the market capabilities of the TDX daily-bar protocol.

- BJ daily-bar history goes through the dedicated TDX path first (market id 2); the current tip can use the BSE board snapshot, with Sina filling the parts not covered. Explicit small-scope history requests skip the irrelevant full-market snapshot.
- Sina history rows may lack `amount`. `cne repair bar-amounts` fills turnover only when OHLC agrees and the volume difference is less than one lot; rows TDX does not serve, such as old codes, keep the missing value and the finding.
- `cne repair bse-tip-turnover` only checks stored current-day bars against the BSE snapshot and fills volume and turnover; it does not re-request Sina history. It cannot recover past BSE snapshots.

These routes fill market data; BJ historical ST still needs separate evidence. For the full sources and repair entry points see [Data sources](sources.md#daily_bars).

### NEEQ quotes before BSE listing

Over-the-counter quotes for BSE securities before their exchange listing (NEEQ market making and call auctions) follow different trading rules and have no verifiable price limits. `daily_bars` and `adj_factors` do not return this part by default: each BSE security is read from its **exchange listing start**, which is the later of `instruments.list_date` and 2020-07-27, the opening date of the Select tier. Shanghai and Shenzhen securities are unaffected.

```python
bars = load("daily_bars", symbols=["920826.BJ"])                    # from 2021-01-12
otc = load("daily_bars", symbols=["920826.BJ"], include_otc=True)   # includes NEEQ quotes
```

`universe="all_a"` likewise uses the exchange listing start to decide whether a BSE security is listed. The data is not deleted; it remains in the lake.

## PIT (Point-in-Time)

PIT datasets: `financial_statement_items`, `announcement_index`,
`share_structure`, `shareholder_counts`, `top_holders`.

```python
items = load(
    "financial_statement_items",
    items=["roe", "net_profit"],
    as_of="2024-04-30",
    pit_mode="strict",
)
```

- `pit_mode="strict"` keeps only rows whose `announce_date`, known `available_at`/
  `source_published_at`, and `fetched_at`/`observed_at` are all no later than `as_of`, and excludes
  `reconstructed` backfill rows; `pit_mode="best_effort"` can keep them, but returns
  `pit_is_exact=False`.
- For the same `(symbol, report_period, item)`, the row with the latest `announce_date` is taken
- Do not use `end=` in place of `as_of` to align financial statements

PIT reads can still use `start` / `end` to bound the data's own date column: announcement and shareholder data are filtered by
`announce_date` / `change_date` / `count_date` / `record_date`; the financial-statement
`report_period` is a quarter string and is filtered by the quarters that intersect the date bounds. `as_of` is still
the time an announcement becomes visible and cannot be replaced by a date window.

`announce_date` is part of the primary key, so a financial-statement revision **adds a new version** instead of overwriting the original value: the same item can have both
the first-published value and the revised value. By default only the version in effect at `as_of` is returned; to see the revisions themselves (the size and direction
of a revision are a signal in their own right), add `all_vintages=True`:

```python
# How many times 000001.SZ's 2024Q1 revenue was revised, and by how much each time
load(
    "financial_statement_items",
    symbols=["000001.SZ"], items=["revenue"],
    as_of="2026-07-21", pit_mode="strict", all_vintages=True,
).select("report_period", "announce_date", "item_value")
```

Note: backfill rows are marked `source=eastmoney_backfill` and treated as
`pit_quality="reconstructed"`. Omitting `pit_mode` or passing `None` defaults to
`best_effort`: historical backfills collected after `as_of` remain readable with
`pit_is_exact=False`; announcement and known availability/publication dates still obey the cutoff.
Research that needs evidence the lake held on that date should pass `pit_mode="strict"`
explicitly and record that choice. When upgrading calls that relied on the omitted
mode to filter by collection time, switch them to explicit strict mode as well.
Only versions accumulated day by day by the daily update, with real bitemporal columns stored, satisfy
strict mode (see [schema](schema.md#financial_statement_items)).
Backfill starts from 2001 by default (EastMoney); `coverage_start` in `list_datasets()` is the actual start on disk.

## Symbol and column filtering

```python
load("daily_bars", symbols=["600519.SH", "000001.SZ"], start="2024-01-01")
load("financial_statement_items", items=["roe"], as_of="2024-06-30", pit_mode="strict")
```

Symbol format: `{code}.{SH|SZ|BJ}`, consistent with `domain/symbols.py`.

## DuckDB SQL

```bash
cne query --sql "SELECT * FROM instruments LIMIT 5"
```

Common views:

| View | Description |
|------|------|
| `daily_bars` | Unadjusted; excludes pre-listing NEEQ quotes for BSE securities, matching the `load()` default |
| `daily_bars_including_otc` | Unadjusted, includes BSE NEEQ quotes (likewise `adj_factors_including_otc`) |
| `daily_bars_hfq` | hfq price columns |
| `daily_bars_qfq` | qfq price columns |
| `daily_bars_adj` | Includes adj_* and adj_is_exact |
| `daily_bars_total_return(start, end)` | Table macro: total-return-adjusted prices `tr_*` (fund cash distributions reinvested; stocks same as hfq) |
| `central_bank_rates` | Central bank rate decisions, taken from `economic_calendar` by Jin10 series id: `bank`, `rate`, `policy` (the bank's main policy rate), forecast / previous / actual, `change` (actual − previous) and `surprise` (actual − forecast); withdrawn events are left out |
| `{dataset}` | Each curated/derived dataset |

Database: `{data.root}/duckdb/cnequity.duckdb` (read-only connection).

## Reading Parquet directly

Without depending on this project's runtime:

```python
import polars as pl
pl.scan_parquet("data/cnequity/curated/daily_bars/**/*.parquet")
```

You must implement price adjustment and universe logic yourself; for production, `load()` is recommended.

## Partition pruning

`query/parquet_scan.py` prunes Hive partition directories by `partition_col` and date range. For large-window queries, prefer `scan()` + a lazy operator chain.

### Trade-based minute resampling {#trade-based-minute-resampling}

TDX zero-volume minutes may carry over the previous close or the latest quote. When you need trade-based bars, resample from complete 1m data so that untraded quotes do not leak into OHLC. For earlier history that 1m does not cover, you can resample 15m / 30m / 60m from 5m:

```python
from cnequity.query import load, resample_trade_bars

minute = load("minute_bars", start="2026-09-15", end="2026-09-15",
              symbols=["603869.SH"])
five_minute = resample_trade_bars(minute, "5m")

history = load("minute_bars_5m", start="2025-01-02", end="2025-12-31",
               symbols=["603869.SH"])
hourly = resample_trade_bars(history, "60m")
```

1m input supports 5m, 15m, 30m and 60m; 5m input supports 15m, 30m and 60m; the input must contain a single frequency. Both are aligned at 09:30 and 13:00 separately and never span the lunch break. OHLC counts only input bars with `volume > 0` or `amount > 0`; the latter keeps small trades whose share count the vendor rounded to zero. An interval with no trades keeps the last quote with zero volume and turnover; that price is still not a trade price. Duplicate timestamps, lunch-break records, 5m times off the 5-minute grid, date mismatches and missing component bars all raise errors, and intervals with no records at all are not filled in out of thin air. The result does not overwrite the raw lake and does not inherit the input rows' source labels; research should also record the input revisions, and resampling cannot recover trade detail the vendor has lost.

5m input can be trade-based only at 5-minute granularity. The vendor's 5m bars are aggregated directly from all 1m bars, so quotes carried over in untraded minutes also count toward open, high and low. As long as a 5m bar has trades, its OHLC is used as is, so in a few intervals the open, high and low differ from the results resampled from 1m; close, volume and turnover are essentially the same. For dates that 1m covers, 1m takes precedence.

To read them through SQL, the HTTP API or MCP, you can use `cne derive minute_bars_15m` (likewise for 30m and 60m) to compute them into the lake with the same rules; see [15 / 30 / 60-minute bars](../recipes/minute-bars-15-30-60.md).

`resample_minute_history` stitches the two into one series: for a given stock on a given day it uses 1m if present and 5m otherwise, and the result gains a `resampled_from` column recording whether each bar came from `1m` or `5m`. The 1m start differs from stock to stock, so the switch does not happen on a single uniform date. Whenever 1m exists for a day, that day uses only 1m; incomplete 1m raises an error as usual and does not silently fall back to 5m.

```python
from cnequity.query import load, resample_minute_history

window = dict(start="2025-01-02", end="2026-09-30", symbols=["603869.SH"])
bars_30m = resample_minute_history(load("minute_bars", **window),
                                   load("minute_bars_5m", **window), "30m")
```

## Error handling

| Exception | Common cause |
|------|----------|
| `ReaderError: unknown dataset` | Misspelled name or dataset not registered |
| `ReaderError: no parquet data` | Not yet init/compacted, or wrong path |
| `ReaderError` (strict_adj) | Missing adj_factors coverage |
| PIT without `as_of` | May include future announcements (not recommended) |

## Related docs

- [Python API reference](../reference/python-api.md)
- [Product boundaries](../architecture/overview.md)

## Derivatives research

Per-contract futures, option chains, continuous series and Greeks do not use the equity universe or price-adjustment assumptions. For complete query and quality-acceptance examples see [Commodity futures and options](../recipes/derivatives.md).
