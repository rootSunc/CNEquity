---
description: Commodity futures and options daily data: collection, configuration and research verification, including source coverage and caveats.
---

# Commodity futures and options: from collection to research verification

This pipeline suits end-of-day, daily-frequency research. The [source capability table](../datasets/sources.md) is generated from the reader registry and checked by CI. Exchange daily files are the primary source; Sina supplements DCE futures and futures minute bars for a small scope. Daily settlement prices do not represent synchronously tradable prices; there is currently no options order book, so this data cannot verify whether multi-leg arbitrage is executable. Real-time market data integration is separate work.

## Configuration and first verification

First create a standalone research lake configuration; there is no need to copy the source template:

```bash
cne config create --config configs/cnequity.futures.toml --data-root data/cnequity-futures
```

Edit the existing `[futures]` and `[sources.futures_exchange]` sections in the generated file; do not append duplicate sections with the same name. Everything below uses this configuration; an existing production lake can also merge just the relevant keys without replacing other settings:

```toml
[futures]
enabled = true
exchanges = ["SHF", "CZC", "GFE", "CFE"]
options = true
dce_route = "sina"
minute_enabled = false

[sources.futures_exchange]
enabled = true
min_interval_seconds = 1.0
```

The rate-limit keys follow `[sources.futures_exchange]` in the project example. The interval is not a safe quota promised by the exchange; narrow the scope and use the cache first, and do not try to fix failures by raising concurrency.

After saving, run `cne config validate --config configs/cnequity.futures.toml`. The explicit exchange list above does not include DCE; if you need DCE futures via Sina, add `"DCE"` to `exchanges`. Setting `dce_route="sina"` alone does not bypass the exchange scope filter, nor does it enable DCE options.

Review the plan for a small window first, then execute. The dates below are only examples; replace them with your own research window:

```bash
cne backfill futures_bars --config configs/cnequity.futures.toml --exchange SHF --start 2026-09-21 --end 2026-09-24 --plan
cne backfill futures_bars --config configs/cnequity.futures.toml --exchange SHF --start 2026-09-21 --end 2026-09-24
cne backfill option_bars --config configs/cnequity.futures.toml --exchange SHF --start 2026-09-21 --end 2026-09-24
cne status --config configs/cnequity.futures.toml --datasets --groups derivatives --all-columns
```

A standalone futures lake uses `--groups derivatives` to scope the status gate, so A-shares do not need to be collected first. The latest session's cross-section is proven per exchange: an exchange with its official contract file for that day (under `meta/derivatives/references/`, separate for futures and options), whose contracts live that day all have bars, is proven complete, and the result reads `OK` when every exchange that can be proven is. An exchange on a vendor route (DCE with the default `dce_route = "sina"`, which has no rows on days without trades) cannot be proven; it is stated beside the result as a limitation and does not fail the gate, and an official route can be proven. Without the day's official file the status is `UNVERIFIED` with exit code 2, meaning the evidence is insufficient; this differs from exit code 1 for gaps that were found. Use the window verification described below to see the exact scope.

`--plan` does not go online or create a lake; it reports the exchanges, the source's earliest available history, dates, estimated cold-cache requests, rate limits and next steps. The market data estimate excludes retries; reference requests are listed separately as an upper bound, not a promise of duration. DCE via Sina requests "a contract's full history" per request, so even a single date may enumerate about 299 candidate codes; windows spanning months or longer involve more codes. `--exchange` can be repeated; INE is routed under SHF, and futures from March 26, 2018 onward need both the SHFE and INE daily files, which the cold-cache estimate counts as two requests. Explicitly backfilling a derivatives dataset turns on the switches this invocation needs, without changing the configuration file.

After the daily bar backfill completes, the corresponding contract tables are rebuilt and compacted automatically, then continuous futures/Greeks are updated. The `followup` in the output is the status of those follow-up runs; successful collection and derivation still do not mean the research window is complete. To repair existing history, you can run explicitly:

```bash
cne backfill futures_bars --config configs/cnequity.futures.toml --exchange SHF --start 2026-09-21 --end 2026-09-24 --refetch
cne backfill futures_contracts --config configs/cnequity.futures.toml
cne backfill option_contracts --config configs/cnequity.futures.toml
cne derive futures_continuous --config configs/cnequity.futures.toml
cne derive option_greeks --config configs/cnequity.futures.toml
```

When a contract backfill is given `--start/--end`, it reads the historical reference files for dates in that window that already have market data, archives them and rebuilds; without dates, it reads only each exchange's latest reference. This recovers dates for historical contracts that have already dropped off the current list. Historical reference revisions are ordered by `as_of`, so older files do not overwrite newer archived evidence. A rejection/challenge stops the reference scan for that source's current batch. Check `reference_requests_upper_bound` with `--plan` first; routes without a reference adapter still cannot fill in lifecycles. The GFE reference API provides only a current snapshot and does not support historical replay; an explicit historical window skips it and reports a warning, and ordinary runs archive it under the actual read date, which cannot serve as evidence for a past point in time.

So GFE historical market data may have been written successfully while the automatic contract step returns `degraded`, and the whole `backfill` still exits 1. First check the `slices` and `followup` in the output and `cne status --run RUN_ID --config configs/cnequity.futures.toml` to tell missing market data apart from insufficient historical reference; do not re-download the same daily file repeatedly just because the exit code is nonzero. Rebuilding contracts without dates can fill in the current reference but cannot fabricate historical evidence.

```bash
cne backfill option_contracts --config configs/cnequity.futures.toml --exchange CFE --start 2026-09-18 --end 2026-09-18 --plan
cne backfill option_contracts --config configs/cnequity.futures.toml --exchange CFE --start 2026-09-18 --end 2026-09-18
```

`--refetch` ignores completion receipts and the old response cache, but does not bypass cooldowns/circuit breakers. Old data without a receipt is not skipped merely because "each exchange has a row". `--refetch` applies only to `futures_bars`, `option_bars` and `sector_bars`; other datasets raise an error. Continuous futures must be recomputed in full and cannot take `--start/--end`; Greeks support a window and `--full`, but automatic dependency detection is usually enough.

## Daily update integration

After the small-window backfill is done, update derivatives on their own with:

```bash
cne run daily --group derivatives --config configs/cnequity.futures.toml
```

A normal daily update fills the recent lookback window and a limited amount of older backlog; it does not replace the initial historical backfill. Within the group, the default order is guaranteed by dependencies: collect daily bars and fill contract metadata first, then compact and derive continuous futures and Greeks. A failure in any source keeps a failed or degraded status; existing derived rows do not mean this run is complete.

Once `[futures]` is enabled in a production lake, `cne run daily` and `daily_pipeline.sh` (when groups are not restricted with `CNE_GROUPS`) automatically include `derivatives`. If an older launchd install kept an explicit `CNE_GROUPS`, add `derivatives` to that list; if the configuration has no `derivatives` group, run `cne config upgrade` to add it; `cne run daily --core-only` runs only the core skeleton and does not collect futures. A standalone futures lake uses the single-group command above, to avoid also collecting the other A-share groups.

## Evidence and failure recovery

`meta/derivatives/` stores the following inspectable evidence:

| Directory | Purpose |
|---|---|
| `futures_bars/YYYY-MM-DD/`, `option_bars/YYYY-MM-DD/` | Per-publisher receipts: parser version fingerprint, normalized/valid/rejected row counts before quarantine, contract set, content fingerprint and status |
| `quarantine/` | Quarantined anomalous rows; "valid after filtering" must not be taken as "complete for the day" |
| `references/` | Successfully read exchange reference snapshots; authoritative dates for expired contracts do not disappear because a reference read failed |
| `http_cache/` | Raw response cache with content hashes; every reuse is still parsed and validated |
| `greeks_dependencies/` | Input content, model code and result fingerprints per date |

Receipt status distinguishes a successful download `captured`, a post-publish check `committed` and a backlog item `owed`. Completion skips also recheck the content actually on disk. Failed, rejected and uncommitted data stays owed; each normal daily update retries at most 3 old backlog dates, and they do not disappear even beyond the three-day lookback window. Handle a large backlog with explicit date backfills.

2018 INE futures must be read from INE's [official daily files](https://www.ine.cn/data/tradedata/future/dailydata/kx20180326.dat); the SHFE file for that year's first day does not contain INE rows. From 2019, the SHFE daily file publishes both SHF and INE rows. The regular `SHF` receipt validates the merged content; when importing only INE historical rows, a separate `INE` receipt can be recorded and validated independently against INE rows, and INE-only rows cannot be used to confirm the original `SHF` receipt.

Official historical files are cached for 30 days by default, and files from the last 7 days for 1 hour; Sina history for active/not-yet-listed contracts is cached for 1 hour, history for contracts whose delivery month passed more than 62 days ago for 30 days, quotes for 60 seconds and minute windows for 30 seconds. DCE historical days that have already been checked are reused by the regular lookback; source-side historical revisions are checked with `--refetch`. The cache is not a promise that data is never revised, nor a guarantee of unlimited retention space; clearing `http_cache` as needed increases the next request volume.

HTTP 403/412/429/456 and transient server-side rejections trigger a cooldown of at least 300 seconds, honor a longer `Retry-After`, and stop that source's current batch. The daily update does not let success on other exchanges mask a failure. A network disconnect is retried at most once more; there is no stress probing and no challenge scripts are executed.

Multiple lakes/CLIs behind the same egress can set the same `CNE_RATE_LIMIT_ROOT=/absolute/path/to/shared-budget` to share throttling, concurrency and rejection state; all involved processes must use the same path. When unset, state is shared only within a single lake. Different hosts additionally need a common rate-limiting service/coordination; a single-machine directory cannot guarantee a total budget across hosts, let alone guarantee that the IP is never banned.

## Research verification and queries

You cannot infer that the middle is complete from the earliest/latest dates. First pin down the research window, products and frequency, check each exchange for missing days, missing rows for known contracts, metadata coverage, quarantine and backlog, and then see whether derived data has been invalidated. `status --datasets` distinguishes gaps from `unverified`: the previous trading day's contract coverage is not complete proof of a newly listed contract list; DCE via Sina in particular lacks zero-volume days. There is currently no complete historical list of listed contracts, so full-market completeness cannot be vouched for.

CZCE's old-format official daily files return 404 for 2010-06-21, 2010-07-01 and 2010-08-13; other verified dates in the same year can be used, but these three days stay recorded as `owed`. Continuous adjustment chains that span these dates keep nulls; 2010 history cannot be marked gap-free.

In INE's 2021-07-28 official futures daily file, LU2108's close is above that day's high; the row is quarantined and the INE receipt for that day remains `owed`, while the other contract rows that pass validation can be used on their own. Some SHFE official files from before certain 2004 roll dates lack the overlapping contract prices needed, and the last days of the 2018 fuel oil and wire rod reporting suspension periods also lack the corresponding rows; the continuous adjustment factor stays null and must not be interpolated or set to 1.

You can check a given window directly with the CLI (read-only, offline):

```bash
cne verify --derivatives --dataset futures_bars --start 2026-09-01 --end 2026-09-24 --config configs/cnequity.futures.toml
cne verify --derivatives --dataset option_bars --start 2026-09-01 --end 2026-09-24 --config configs/cnequity.futures.toml
```

The JSON includes missing days, missing rows for known contracts, missing metadata, backlog and product coverage. Exit code 1 means gaps were found, 2 means insufficient evidence; a healthy latest cross-section does not mask missing rows inside the window. Without a complete historical list of listed contracts, it does not report full-market completeness. This command does not support automatic repair; after reviewing the specific gaps, use the scoped backfill commands above. It cannot yet prove that historical trading parameters and derived series are valid across the whole research window.

The following reads the published version; for reproducibility, pin `revision_map` as described in the [query guide](../datasets/query-guide.md). Do not apply the stock `universe` or stock price adjustment parameters to derivatives.

```python
import polars as pl
from cnequity.config import load_config
from cnequity.query import load
from cnequity.quality.derivative_checks import exchange_session_gaps
from datetime import date

cfg = load_config("configs/cnequity.futures.toml")
print(exchange_session_gaps(cfg, "futures_bars",
                           start=date(2026, 9, 21), end=date(2026, 9, 24)))

# Copper futures term structure for the day, sorted by actual delivery month.
futures = load("futures_bars", config=cfg, start="2026-09-24", end="2026-09-24")
contracts = load("futures_contracts", config=cfg)
curve = (futures.filter(pl.col("product") == "CU")
         .join(contracts.select("symbol", "delivery_month", "last_trade_date", "dates_basis"), on="symbol")
         .select("symbol", "delivery_month", "settle", "open_interest", "last_trade_date", "dates_basis")
         .sort("delivery_month"))
print(curve)

# T-shaped option chain for one underlying; empty expiry/status must not be filled in as usable pricing.
options = load("option_bars", config=cfg, start="2026-09-24", end="2026-09-24")
meta = load("option_contracts", config=cfg)
chain = (options.filter(pl.col("underlying_symbol") == "CU2611.SHF")
         .join(meta.select("symbol", "expiry_date", "exercise_style", "dates_basis"), on="symbol", how="left"))
t_shape = chain.pivot(on="option_type", index=["strike", "expiry_date"], values="settle").sort("strike")
print(t_shape)
```

`first_seen_date/last_seen_date` are observation bounds; only authoritative references fill `list_date/last_trade_date/expiry_date`. Old `observed*` dates must be rebuilt. The compatibility field `expiry_month` is actually the underlying contract month and cannot stand in for the real expiry date. The specification table is also not a complete library of historical trading parameters; historical periods such as FB's, where parameters changed mid-trading and have not been verified, stay unknown.

Continuous futures `oi_t-1_v2` selects the contract by the previous trading day's open interest; when there is no valid further-dated month, the series is not output and does not fall back to a nearer month. It supplements the shared calendar with each exchange's own observed futures sessions: in early years a few dates in SHFE official daily files are marked as holidays by the shared stock calendar, and that must not cause the next day's continuous row to be dropped. A missing previous trading day or roll price invalidates the adjustment factor, which stays null; null must not be filled with 1. The adjusted series can be used as a signal; P&L should be computed from the real contracts and roll trades.

Greeks are computed from settlement prices and a model; changes to contracts, interest rates, the model or market data all trigger recomputation. Unknown expiry or exercise style keeps `no_expiry/no_exercise_style`, and IV is not inverted on the expiry date. With `forward_source=parity`, the forward is backed out of option prices, so it cannot then be used to verify put-call parity arbitrage on the same set of prices.

Daily-frequency CTA also needs explicit fees (including close-today), margin, price limits, effective periods for multipliers/tick sizes, delivery constraints and the time at which signals become available. These should be versioned external inputs to the strategy; do not assume zero fees or no constraints by default. Current contract metadata is not a strict historical PIT parameter library. [SHFE business parameters](https://www.shfe.com.cn/reports/businessdata/prmsummary/) and [INE daily settlement parameters](https://www.ine.com.cn/reports/tradedata/dailyandweeklydata/) are candidate sources for building historical parameters later; the existing lake has not yet verified and imported them day by day. INE's [official handbook](https://www.ine.com.cn/upload/20240410/1712738949255.pdf) also explicitly distinguishes the margin rate the exchange charges members from the actual margin rate charged to clients. Inventory/warehouse receipt/member ranking CTAs need separate datasets and cannot be derived from the existing OHLC.

SHFE provides monthly adjustment tables and daily settlement parameter files. The project currently only parses and stores the raw parameter evidence; it has not yet published them as day-by-day fee or margin tables ready for backtesting. Monthly files cannot capture day-by-day changes; files are usually updated after the close, and member parameters are not the same as clients' actual rates. In research, check the announcements, effective trading days, holiday adjustments and close-today fees yourself, and keep the parameter version you used.

## Minute data

```bash
cne backfill futures_minute_bars --config configs/cnequity.futures.toml --symbols CU2611.SHF,M2701.DCE --plan
cne backfill futures_minute_bars --config configs/cnequity.futures.toml --symbols CU2611.SHF,M2701.DCE
```

This fetches only the latest window and rejects historical date arguments. Exceeding the watchlist limit raises an error instead of silently dropping contracts. Once enabled, it must be scheduled every day; even if the derivatives group is set to weekly, the rolling minute window runs on trading days, while historical daily bars still follow the group's cadence. If you need daily CTA signals, the daily bar group must also be set to daily. Night sessions are currently mapped by the trading calendar, which is not yet a complete per-product night session/ad hoc closure calendar; do not treat the window data as a complete tick/order book history.

## Official annual packages: verified formats and usage limits {#official-annual-packages}

The `download.json` on SHFE's [official data download](https://www.shfe.cn/reports/tradedata/datadownload/) page lists the specific annual ZIP links, which may change with revisions. The 2026 current-year package contains per-contract monthly XLSX files for SHF and INE, and a single workbook contains both futures and options; you cannot split the package contents by the web page's "futures/options" button names.

`cnequity.adapters.futures_exchange.shfe_archive.iter_archive(path, year=2026)` provides local read-only parsing and returns `(file name, {dataset name: DataFrame})` per workbook. The parser rejects unverified headers, cross-year dates, non-integer lot counts, duplicate primary keys and oversized archives. Verified so far: single-sided `.xlsx` for 2002–2008, grouped `.xls` for 2009–2019 that carry a "双边计算" (double-sided calculation) note (volume, turnover and open interest are converted to single-sided), and single-sided grouped `.xls` for 2020–2024. These rules cannot be applied to other years or different headers. Default daily file backfill still takes the original path; annual ZIPs can only be imported explicitly offline:

```bash
cne import shfe-archive /data/shfe-2025.zip --dataset futures_bars --year 2025 --accept-partial-fields --config configs/cnequity.futures.toml
cne import shfe-archive /data/shfe-2025.zip --dataset futures_bars --year 2025 --accept-partial-fields --config configs/cnequity.futures.toml --apply
cne import shfe-archive /data/shfe-2025.zip --dataset option_bars --year 2025 --accept-partial-fields --config configs/cnequity.futures.toml --apply
```

The import makes no source network requests; it copies the original ZIP into the lake's `meta/derivatives/annual_archives/` and records the hash, import time, members, rejected rows and missing fields. If you know the official original link and download time, you can add `--archive-url https://www.shfe.com.cn/...` and `--archive-downloaded-at 2026-09-27T09:00:00+08:00` as provenance evidence. `--plan` only describes the import scope and field limitations; it does not parse the ZIP. Each successfully verified workbook is staged independently and published automatically; if a later member is corrupt, the verified members can still be published, while a nonzero status and the gaps are returned. Any primary key that already has a day-by-day row is skipped, so annual rows never overwrite complete daily bars. The import is recorded with `coverage_status=partial_fields` and does not produce a "complete day-by-day" receipt; research that needs fields only available in daily files, or strict coverage queries, still requires backfilling the daily files. If the original download time has no independent evidence, the import does not pass off the local file modification time as the download time.

The format of annual workbooks varies by year and exchange: they may be `.xls` or `.xlsx`, grouped by contract,
or omit the contract code on subsequent rows. The parser accepts only recognized headers and units and rejects unknown formats;
verifying a sample from one year cannot be extrapolated to complete coverage of all historical years. INE historical file routing may also differ from SHF;
research windows must still be checked for gaps by exchange, contract and trading day.

Some official historical rows themselves violate OHLC bounds, and early daily files may also lack turnover.
CLI import quarantines nonconforming rows into the rejection ledger and returns a warning; it does not declare that trading day complete;
without an independent daily file to compare against, turnover in the annual package cannot be claimed as verified day by day either.

Annual workbooks lack `oi_change`, and options additionally lack exercise volume, Delta and implied volatility fields; these are left empty, not filled with 0. The annual package can be used for cold-start price/volume history, but it is not a substitute for complete daily files. After importing, you can fill gaps from daily files and rebuild contracts and derived series. The annual package does not provide authoritative historical lifecycles either.
