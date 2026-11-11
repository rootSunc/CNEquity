# Python API

Module: `cnequity.query`. Read the [query guide](../datasets/query-guide.md) first to understand the semantics; this page is for looking up parameters, return values, and version boundaries.

```python
from cnequity.query import load, scan, list_datasets, dataset_schema
```

## load()

```python
def load(
    dataset: str,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    adjust: Literal["qfq", "hfq", "total_return"] | None = None,
    universe: Literal["all_a", "all_a_sh_sz"] | None = None,
    profile: UniverseProfileLike | None = None,
    universe_profile: UniverseProfileLike | None = None,
    as_of: str | date | None = None,
    items: list[str] | None = None,
    symbols: list[str] | None = None,
    strict_adj: bool = False,
    strict_universe: bool = False,
    all_vintages: bool = False,
    pit_mode: Literal["strict", "best_effort"] | None = None,
    include_otc: bool = False,
    config: Config | None = None,
    data_root: str | Path | None = None,
    revision: RevisionSelection | None = None,
    revision_map: Mapping[str, int | str] | None = None,
) -> pl.DataFrame
```

### Parameters

| Parameter | Description |
|------|------|
| `dataset` | Registered dataset name |
| `start`, `end` | Inclusive date window (on the dataset's primary date column) |
| `adjust` | `hfq` / `qfq`; applies to price/volume datasets such as `daily_bars`, `minute_bars`, and `minute_bars_5m`. `total_return` applies only to `daily_bars`: fund cash distributions are reinvested, and stocks behave as `hfq`; see the [query guide](../datasets/query-guide.md#total-return-adjustment-total_return) |
| `universe` | Compatibility parameter: `"all_a"` is all A-shares across Shanghai, Shenzhen and Beijing (deprecated, emits a warning, removed in 0.25.0; `profile="legacy_all_a"` returns the same rows); `"all_a_sh_sz"` is the Shanghai/Shenzhen subset. New research should choose a versioned `profile` |
| `as_of` | PIT cutoff date; visibility rules are determined by `pit_mode`. Strict mode checks announcement, availability/publication time, and observation time, then selects the effective version by fact key |
| `items` | List of financial statement item codes |
| `symbols` | Symbol allowlist |
| `strict_adj` | When True, missing adjustment factors raise `ReaderError` |
| `strict_universe` | When True, a supported universe raises if instruments, per-day trading_status coverage, or versioned historical ST evidence receipts are missing; suited to research reads |
| `all_vintages` | When True, returns **all** versions before `as_of` (for studying financial statement revisions); do not enable it for cross-sectional stock selection, as it counts the same fact more than once |
| `pit_mode` | PIT evidence mode: omitted or `None` defaults to `best_effort`, keeping historical backfills collected after `as_of` and returning `pit_is_exact=False`; announcement and known availability/publication dates still obey the cutoff. `strict` excludes reconstructed backfills and returns only evidence the lake held by the cutoff |
| `include_otc` | Defaults to False: `daily_bars` and `adj_factors` exclude Beijing securities' quotes before exchange trading began. True retains those OTC quotes. The exchange start is the later of the listing date and 2020-07-27 |
| `config` / `data_root` | Lake location; when omitted, looked up as `CNE_CONFIG` → `configs/cnequity.toml` in the current directory → the per-user config |
| `profile` / `universe_profile` | Versioned universe policy; the two are aliases. Official research profiles enable strict evidence checks automatically |
| `revision` | Pins the primary dataset by revision number or ID; a per-dataset mapping is also supported. A single number does not pin factor or universe dependencies |
| `revision_map` | Pins the retained revision of each dependency dataset; an explicitly requested missing version raises `RevisionConsistencyError` instead of falling back to latest |

While initialization is still in progress, `load("daily_bars", symbols=[...])` can read unadjusted daily bars that have already been sealed. Queries without `symbols`, `scan()`, and SQL without symbols still see only published data. Adjustment factors become available only after `adj_factors` is published.

### Pinned data versions for combined reads {#pinned-data-versions}

An adjusted universe query may depend on `daily_bars`, `adj_factors`, `instruments`, `trading_status`, and
`trading_calendar`. `revision_map` uses each dataset's own revision, and the numbers need not match; filling in only
the primary table does not pin the whole result. A single `load()` selects one generation for each dependency it uses, and later universe
and coverage reads reuse that selection. Unspecified dependencies use the current version; there is no lake-wide atomic transaction.

This pins only the data. For long-term research, use `SnapshotStore.create(name, datasets, research=True)`
or the CLI `snapshot create --research`, which packages the full dependencies, ST/delisting evidence, catalog, calendar seed, and non-sensitive read configuration together. Snapshots do not include API tokens, and after restoring one you can explain the original source capabilities offline.
On read, the frozen canonical policy is checked against the requested built-in profile, and a version mismatch raises an error; snapshots do not bundle executable software.
Long-term research should also save the dependency version map, query parameters, profile, configuration, and software version; old generations
are subject to retention limits. Do not use watermarks or `fetched_at` in place of the revision identity of the full set of dependencies.

### Queries with a read receipt

```python
from cnequity.query import load_with_receipt

result = load_with_receipt(
    "daily_bars",
    start="2024-01-01",
    end="2024-12-31",
    symbols=["600519.SH"],
    adjust="hfq",
    require_replayable=True,
    data_root="/path/to/lake",
)
bars, receipt = result.frame, result.receipt
```

`load_with_receipt()` takes the same filter parameters as `load()`. It first captures the committed
revision of each dependency dataset, then reads using that mapping. The receipt records the revisions actually selected and their contract fingerprints, the sources and fetch times of the returned rows,
the PIT mode/quality, and the date bounds of both the requested window and the actual rows. `receipt_id` is a stable digest of this content.
`coverage.completeness="not_assessed"` states explicitly that the rows' start and end dates **cannot** prove market-wide or day-by-day completeness.
When an old lake lacks revisions, `replayable=false` and `unpinned_datasets` lists them; setting
`require_replayable=True` refuses before scanning. The receipt pins the data version but does not freeze quality reports,
operational configuration, or the software version; long-term reproduction still needs a research snapshot and a research artifact manifest.

The local HTTP service also provides `GET /api/read/{dataset}`: pass `start`, `end`, and usually
`symbol`, and it returns `rows` together with the `receipt` for that same read. PIT data also requires `as_of` and uses
strict mode by default. The HTTP window is at most 366 days, multi-day queries must give a security code, and results over 1000 rows are refused;
bulk research should still use the Python API. This endpoint requires every read dependency to have a replayable revision and never silently downgrades.

### Returns

- Unadjusted datasets: the original columns
- PIT `load()` also returns the optional bitemporal columns `available_at`, `source_published_at`,
  `observed_at`, and `revision_id`, plus the quality flags `pit_is_exact` / `pit_quality`;
  when old Parquet files lack these columns, the read side fills them in
- Non-empty `adjust`: appends `adj_open`, `adj_high`, `adj_low`, `adj_close`, `adj_is_exact`

### Exceptions

`strict_adj` and `adj_is_exact` describe factor coverage only, not historical accuracy. The `adjustment` summary from `load_with_receipt()` reports sources and advisory historical diagnostics for the revision read; unavailable diagnostics are unassessed.

`ReaderError` (a subclass of `ValueError`): unknown dataset, no data, strict_adj failure, and so on.

## load_profile()

Fetch what could be known about many securities on many dates, for several fields at once, as a long table with one row per `(symbol, as_of, field)`.
Each source dataset is read once and joined as-of per security, so more dates cost a join, not another read.

```python
from datetime import date
from cnequity.query import load_profile

p = load_profile(
    ["listed", "status.trading", "status.st", "shares.total", "holders.count",
     "fsi:net_profit", "industry.sw.l1", "industry.eastmoney.l3"],
    symbols=["600519.SH"],
    as_of_dates=[date(2024, 4, 30)],
    pit_mode="best_effort",   # or "strict"
)
```

| Field | Source | Meaning |
|---|---|---|
| `listed` | instruments | 1 while listed on the date |
| `status.trading` / `status.st` | trading_status | Latest row on or before the date; a null ST is unknown, not "not ST" |
| `shares.total` / `shares.float` / `shares.free_float` | share_structure | Known once both the change date and the announcement have passed |
| `holders.count` | shareholder_counts | Known from its announcement |
| `fsi:<item_code>` | financial_statement_items | Latest report period visible on the date; `value_str` is the period, `field_date` its end |
| `industry.<sw\|eastmoney>.l<1\|2\|3>` | industry_members | Industry code at that level in the latest snapshot on or before the date; `label` is its name |

Columns: `value_num`, `value_str`, `label`, `field_date` (the date the value describes), `visible_from` (earliest date it could be known), `source`,
`pit_quality` (`strict`: first-hand and observed by this lake by the date; `reconstructed`: known by its announcement but recovered later; `snapshot_only`: a snapshot date only),
`age_days` and `missing_reason`.

`missing_reason`: `before_listing`, `after_delisting`, `outside_coverage` (the date precedes anything the dataset holds), `not_yet_published`,
`value_unknown` (a record exists but leaves the value unknown, e.g. ST on a derived status row), `no_pit_evidence` (`strict` only) and `no_record`;
a value older than the field's age limit is returned with `stale`. Nothing is returned outside a listed life.

`pit_mode="published"` accepts only values with a recorded publication time (`available_at`); announcement and ingestion dates never count, and a value is not visible before the date it took effect (e.g. a share change).
`cutoff=time(15)` requires that time to fall before 15:00 Shanghai on the as-of date, otherwise the value counts from the next day; without it the whole day counts.
Industry snapshots have no publication time and are `no_pit_evidence` in that mode; listing and trading status are session facts and read the same in every mode.
`canonical=True` first re-keys each row onto the code effective on its date (as `canonicalize_dated_identifiers` does); `data_root` and `revision_map` go to every read.

## scan()

Returns a raw `pl.LazyFrame`, suited to custom lazy pipelines over large windows. It only does date-partition and
symbol filtering and does not apply `load()`'s price adjustment, universe, PIT, or strict coverage semantics; use `load()` when you need
those semantics. The current parameters are:

```python
def scan(
    dataset: str,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    symbols: list[str] | None = None,
    config: Config | None = None,
    data_root: str | Path | None = None,
    revision: RevisionSelection | None = None,
    revision_map: Mapping[str, int | str] | None = None,
) -> pl.LazyFrame
```

```python
import polars as pl

lf = scan("daily_bars", start="2020-01-01", symbols=["600519.SH"])
df = lf.filter(pl.col("close") > 0).collect()
```

## list_datasets()

```python
def list_datasets(
    *,
    config: Config | None = None,
    data_root: str | Path | None = None,
) -> pl.DataFrame
```

Main columns: `dataset`, `layer`, `date_col`, `fetch_semantics`, `history_mode`, `backfill_source`, `history_horizon_days`, `pit`, `pit_quality`, `pit_storage_columns`, `has_data`, `coverage_start`, `coverage_end`, `watermarked`, `watermark`. It also returns `snapshot_date`, `revision`, `revision_id`, `schema_version`, `contract_fingerprint`.

The date bounds are for discovering data and do not prove the window is complete; the version fields come from state, and the revision pointer remains authoritative for which committed version is read.

`history_mode` ∈ `by_date` / `snapshot_with_backfill` / `snapshot_only`; together with `coverage_*` it forms the contract for where usable history starts.

## historical_universe_validity()

The historical research gate lives in `cnequity.quality.historical_validity` and by default validates all A-shares across Shanghai, Shenzhen and Beijing:

```python
from cnequity.quality.historical_validity import historical_universe_validity
from datetime import date

report = historical_universe_validity(
    cfg,
    start=date(2020, 1, 1),
    end=date(2024, 12, 31),
    universe="all_a_sh_sz",
)
assert report["universe_ready"]
```

`universe="all_a_sh_sz"` makes the daily-bar range, historical ST evidence, and delisting coverage checks apply only to the Shanghai/Shenzhen
subset; it does not hide the BJ historical evidence gaps of all A-shares (`all_a`). Only when `universe_ready`
is true can that explicit scope be used for historical research.

## dataset_state() and minute resampling

`dataset_state(dataset, config=..., data_root=...)` returns a `DatasetState`, used to obtain a downstream cache identity. See `query/state.py` for the fields; the standard library's `dataclasses.asdict()` converts it to a dict. Do not judge whether data has changed by the maximum date alone.

`dataset_attempt(dataset, config=..., data_root=...)` reads only the most severe step receipt for that dataset in the most recent fetch run (phase, status, time, error reason, and run ID). It is a separate dimension from the published revision: a failed fetch does not delete the previous readable snapshot, and consumers should show both the snapshot date and the failure reason. It returns `None` for old lakes without fetch receipts.

`resample_trade_bars(frame, "5m")` resamples from complete 1m trade data and supports 5m / 15m / 30m / 60m; it can also resample 5m into 15m / 30m / 60m, for history that 1m does not cover. Morning and afternoon sessions are aligned separately. `resample_minute_history(minute_1m, minute_5m, "15m")` prefers 1m per stock and trading day, otherwise uses 5m, outputs 15m / 30m / 60m, and marks the source in a `resampled_from` column. It does not overwrite the raw lake and raises an error when component minutes are missing; see the [query guide](../datasets/query-guide.md#trade-based-minute-resampling) for details.

## dataset_schema()

```python
def dataset_schema(dataset: str) -> dict[str, pl.DataType]
```

Returns the Polars type mapping registered in `domain/schemas.py`.

## Security identifier continuity

Module: `cnequity.domain.security_identifiers`. This is a public read contract that downstream consumers (such as EquityLab's backtest ledger and research samples) import directly; removing or renaming it requires a deprecation cycle.

```python
from cnequity.domain.security_identifiers import (
    canonical_symbol,
    canonicalize_dated_identifiers,
    identifier_aliases,
    identifier_changes,
    transitions_on,
)
```

The registry holds only code changes whose effective date and share ratio are backed by an archived announcement; never infer a change from similar prices or names. `canonicalize_dated_identifiers(frame, date_col=..., key_columns=...)` moves dated rows back to the code effective on that date, and keeps the native row when a native and a vendor-restated row share a business key. Downstream consumers may hash the module source as the identity of frozen samples, so editing the registry invalidates those samples.

## Configuration resolution

```python
from cnequity.query.reader import resolve_config

cfg = resolve_config(config=my_cfg)
cfg = resolve_config(data_root="/path/to/lake")
```

Precedence: `config` > `data_root` > default toml path.

## Examples

### hfq Shanghai/Shenzhen research scope

```python
bars = load(
    "daily_bars",
    start="2024-01-01",
    end="2024-12-31",
    adjust="hfq",
    profile="cn_a_sh_sz_research_v1",
    strict_adj=True,
)
```

### PIT financial statements

```python
roe = load(
    "financial_statement_items",
    items=["roe"],
    as_of="2024-04-30",
    pit_mode="strict",
)
```

### Index bars

```python
idx = load("index_bars", start="2024-01-01", symbols=["000300.SH"])
```

### Explicit data_root (no toml needed)

```python
bars = load("daily_bars", start="2024-06-01", data_root="/data/cnequity")
```

## DuckDB entry point

Views are maintained by `query/views.py`; see `daily_bars_adj` for the adjusted columns. SQL does not automatically apply `load()`'s strict PIT, universe, or missing-factor rejection logic to plain table queries; express and verify them yourself, or prefer `load()`. See [DuckDB and Polars](../recipes/duckdb-polars.md) for examples.

## Related docs

- [Query guide](../datasets/query-guide.md)
- [Product scope](../architecture/overview.md)
