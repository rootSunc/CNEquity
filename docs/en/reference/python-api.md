# Python API

Module: `cnequity.query`. Read [Query and research](../datasets/query-guide.md) for the semantics first; this page lists parameters, return values, and version boundaries. Every function accepts `config=` or `data_root=` to choose a lake. With neither, the lookup is `CNE_CONFIG` → `configs/cnequity.toml` in the working directory → the per-user config, the same as the CLI.

```python
from cnequity.query import load, scan, read_scope, list_datasets, dataset_state
```

## load() {#load}

```python
load(
    dataset, *, start=None, end=None,
    adjust=None,            # "qfq" | "hfq" | "total_return"
    profile=None,           # versioned universe
    universe=None,          # compatibility: "all_a_sh_sz"
    as_of=None, pit_mode=None, all_vintages=False,
    items=None, symbols=None,
    strict_adj=False, strict_universe=False, include_otc=False,
    revision=None, revision_map=None,
    config=None, data_root=None,
) -> polars.DataFrame
```

| Parameter | Meaning |
|---|---|
| `start`, `end` | Inclusive date window on the dataset's main date column |
| `adjust` | `qfq` forward-adjusted, `hfq` backward-adjusted, for daily, minute, and other price-volume datasets. `total_return` applies to `daily_bars` only: fund cash distributions are reinvested, and stocks read as `hfq`. Adds `adj_open`, `adj_high`, `adj_low`, `adj_close`, and `adj_is_exact` |
| `strict_adj` | Raise when an adjustment factor is missing instead of returning `adj_is_exact=False`. It checks factor coverage only and cannot prove historical factors correct |
| `profile` | A versioned universe such as `cn_a_sh_sz_research_v1`. List them with `cne profile list` or `list_universe_profiles()`; research profiles turn on strict evidence checks |
| `universe` | Legacy parameter that only accepts `all_a_sh_sz` (the Shanghai/Shenzhen subset). `universe="all_a"` was removed in 0.25.0 and now raises an error; pass `profile="legacy_all_a"` for the same rows |
| `strict_universe` | Raise when the universe lacks security master data, daily trading status, or historical ST evidence |
| `as_of`, `pit_mode` | PIT cutoff. `best_effort` (default) keeps values backfilled later but disclosed before the cutoff and marks them `pit_is_exact=False`; `strict` returns only versions the lake had observed by the cutoff |
| `all_vintages` | Return every version before the cutoff, for studying restatements; leave it off for cross-sectional screens, where it counts one fact several times |
| `items` | Financial statement item codes |
| `include_otc` | By default, OTC quotes from before a Beijing exchange security entered the exchange market are excluded |
| `revision`, `revision_map` | Pin revisions; see below |

PIT datasets also return `available_at`, `source_published_at`, `observed_at`, `revision_id`, `pit_is_exact`, and `pit_quality`. An unknown dataset, no data, or a failed strict check raises `ReaderError`, a subclass of `ValueError`.

## Pinning revisions {#revisions}

An adjusted or universe query can depend on `daily_bars`, `adj_factors`, `instruments`, `trading_status`, `trading_calendar`, and more. `revision` pins only the main dataset; `revision_map={"daily_bars": 12, "adj_factors": 7}` pins each dependency by its own revision number or revision id. A pinned revision that retention has removed raises `RevisionConsistencyError` rather than falling back to the latest. Dependencies left out read their current revision.

### load_with_receipt() {#load-with-receipt}

```python
from cnequity.query import load_with_receipt

result = load_with_receipt(
    "daily_bars", start="2024-01-01", end="2024-12-31",
    symbols=["600519.SH"], adjust="hfq", require_replayable=True,
)
bars, receipt = result.frame, result.receipt
```

It takes the same parameters as `load()`. It first captures the committed revision of every dependency, then reads with that set; the receipt records the selected revisions and contract fingerprints, the sources and fetch times of the returned rows, the PIT mode, the requested window, and the actual date bounds. `receipt_id` is a stable digest of these. `coverage.completeness` is `not_assessed`: the date bounds do not prove that every day is complete. With `require_replayable=True`, a dependency that cannot be pinned is refused before reading. The receipt pins data only; it does not freeze quality reports, configuration, or software versions.

A local `cne serve` also offers `GET /api/read/{dataset}`, returning rows with the receipt of the same read. The window is at most 366 days and more than 1000 rows are refused; use Python for bulk research.

## scan() and read_scope() {#scan}

```python
import polars as pl
from cnequity.query import read_scope, scan

with read_scope():
    lf = scan("daily_bars", start="2020-01-01", symbols=["600519.SH"])
    df = lf.filter(pl.col("close") > 0).collect()
```

`scan()` returns a raw `polars.LazyFrame` with date-partition and symbol filtering only; it applies no adjustment, universe, or PIT. Its parameters are `start`, `end`, `symbols`, `revision`, `revision_map`, `config`, and `data_root`.

A lazy query reads files only when it is collected. Build and collect it inside `read_scope()` so retention cannot delete the files it needs in the meantime; collecting outside the scope can fail on files that were cleaned up.

## load_profile() {#load-profile}

Returns what was knowable about several securities, at several cutoffs, for several fields, as a long table with one row per `(symbol, as_of, field)`.

```python
from datetime import date
from cnequity.query import load_profile

p = load_profile(
    ["listed", "status.st", "shares.total", "fsi:net_profit", "industry.sw.l1"],
    symbols=["600519.SH"], as_of_dates=[date(2024, 4, 30)],
    pit_mode="best_effort",    # or "strict", "published"
)
```

| Field | Source |
|---|---|
| `listed` | `instruments`: 1 when the cutoff falls within the listing period |
| `status.trading`, `status.st` | `trading_status`: the latest row on or before the cutoff; an empty ST means unknown |
| `shares.total`, `shares.float`, `shares.free_float` | `share_structure`: both the change date and the announcement date have passed |
| `holders.count` | `shareholder_counts`: known after announcement |
| `fsi:<item code>` | `financial_statement_items`: the latest report period visible at the cutoff |
| `industry.<sw\|eastmoney>.l<1\|2\|3>` | `industry_members`: the latest snapshot on or before the cutoff |

Returned columns are `value_num`, `value_str`, `label`, `field_date`, `visible_from`, `source`, `pit_quality`, `age_days`, and `missing_reason`. `missing_reason` distinguishes before listing, after delisting, outside coverage, not yet published, value unknown, missing PIT evidence, and no record. `pit_mode="published"` accepts only values with a recorded publication time, and `cutoff=time(15)` requires that time to be before 15:00 Beijing time on the cutoff day. `canonical=True` first maps each symbol to the code in effect on that day.

## Lake state {#lake-state}

| Function | Returns |
|---|---|
| `list_datasets()` | One row per dataset: layer, main date column, history mode, backfill source, PIT declaration, whether it has data, coverage bounds, watermark, current revision, and contract fingerprint. Date bounds do not prove there are no gaps in between |
| `dataset_state(dataset)` | `DatasetState`: current revision number, revision id, commit time, run id, schema version, contract fingerprint, content digest, and changed partitions. Downstream caches use it to tell whether data changed; do not rely on the latest date alone |
| `dataset_revisions(dataset, after=revision_id)` | Committed revisions after that one, oldest first. Raises `ValueError` when the history after it is incomplete; callers should then treat the dataset as changed |
| `dataset_attempt(dataset)` | The most severe step outcome for the dataset in the latest collection (stage, status, reason, run id). A failed collection does not remove the previous readable data, so show the data date and the failure reason together |
| `dataset_schema(dataset)` | Declared columns and Polars types |

## Minute resampling {#resample}

`resample_trade_bars(frame, "5m")` resamples complete 1m trade bars to 5m, 15m, 30m, or 60m, and 5m bars to 15m, 30m, or 60m. Morning and afternoon sessions align separately, and incomplete constituent minutes raise an error. `resample_minute_history(minute_1m, minute_5m, "15m")` uses 1m where a symbol-day has it and 5m otherwise, recording the input in `resampled_from`. Neither writes to the lake; to store the result, run `cne derive minute_bars_15m`.

## Security identifier continuity {#identifiers}

`cnequity.domain.security_identifiers` is a public read contract offering `canonical_symbol`, `canonicalize_dated_identifiers`, `identifier_aliases`, `identifier_changes`, and `transitions_on`. The registry lists only code changes whose effective date and exchange ratio are supported by announcements. `canonicalize_dated_identifiers(frame, date_col=..., key_columns=...)` maps dated rows to the code in effect on that day. Removing or renaming these functions requires a deprecation period.

## SQL {#sql}

`cne query --sql` and the DuckDB view database read published data, with one view per dataset under its own name. `daily_bars_adj` adds adjusted columns, anchoring forward adjustment at each symbol's latest bar in the lake; for forward-adjusted aggregates over an explicit window, use `daily_bars_qfq(DATE '2020-01-01', DATE '2020-12-31')`. SQL does not apply the strict PIT, universe, or missing-factor checks that `load()` applies to ordinary tables.
