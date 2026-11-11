# Repository scripts

Path: `scripts/`

| Directory | Contents |
|---|---|
| `scripts/` | Operations tools: backfill acceptance, delisted catalog rebuild, catch-up runs, survivorship bias measurement |
| `scripts/dev/` | Repository maintenance: docs sync (`sync_*`), release checks, offline benchmarks and end-to-end checks, probes |

These scripts need a source checkout and are not installed with the PyPI package. For scheduled updates use `cne schedule install` (it ships with the package and works on every platform); the old `scripts/scheduler/` has been removed, see the [runbook](runbook.md#legacy-scripts) for migrating.

> The scripts below target **self-hosted machines/VPS** (including server backfills). Open-source contributors only need
> the CLI (`cne init` / `run`); scheduling and alerting are optional and not the only way to deploy.

## Production scripts

### run_catchup.py

**Purpose**: bring the gate up to date after a missed run / a weekend — `daily:core`, then `market_breadth` + `compact`
(the latter skipped with `--core-only`). It does not pass `--backfill` (a full CA scan is fragile over an overseas egress).

```bash
python scripts/run_catchup.py                          # latest trading day
python scripts/run_catchup.py --trade-date 2026-07-17
python scripts/run_catchup.py --trade-date 2026-07-17 --all-groups
```

**The exit code follows the gate, not the extra groups**: a core or market_breadth failure exits 1; failures of groups in `--extra-group` /
`--all-groups` are only reported and do not change the exit code — the EastMoney-heavy groups are flaky over an overseas egress anyway.
Parts whose watermark has already reached the target day are marked `skipped_already_fresh` and not rerun.

Formerly `cne run catchup`. It is orchestration rather than a capability (each step is `cne run daily` on one schedule
group), so it lives here rather than in the published CLI.

## Init and acceptance

Historical initialization and resume use the public CLI directly:

```bash
cne init --profile full --since 2016-01-01   # after an interruption, rerun the same command to resume
```

For source capability diagnostics, start with `cne sources probe --list` to pick the smallest probe; before a backfill, check the range and request estimate from
`cne backfill <dataset> --plan`.

### accept_backfill.py

Backfill acceptance tool:

```bash
python scripts/accept_backfill.py snapshot --out /tmp/counts.json
python scripts/accept_backfill.py check --compare /tmp/counts.json
```

Checks idempotency and the stability of curated row counts.

### delisted_ops.py

Four subcommands for rebuilding the delisted universe (survivorship bias repair). **Reading** the catalog and fetching quotes stay in the CLI
(`cne delisted status` / `cne backfill daily_bars --profile delisted`) — those two have a day-to-day form; these four do not.

| Subcommand | Description |
|--------|------|
| `discover [--limit N]` | Scans the issued code space and classifies codes as once listed / never issued; resumable, and codes whose probe failed stay pending rather than being recorded as "never issued" |
| `reconcile [--apply]` | Read-only check of catalog end dates by default; `--apply` only corrects end dates disproved jointly by the formal delisting date, the trading calendar and positive-volume quotes, and writes a backup and a SHA-256 receipt |
| `repair [--since]` | **Does not refetch quotes**: writes `instruments.delist_date` from the existing `daily_bars` span and clears the `认购款` (subscription payment) placeholders |
| `coverage [--start] [--end]` | **Read-only strict gate**: verifies discovery completeness, window overlap, the last valid trade and instruments identity; exits 1 if it does not pass |

```bash
cne delisted status                                   # how many are known
python scripts/delisted_ops.py discover --limit 500
cne backfill daily_bars --profile delisted --start 2016-01-01
python scripts/delisted_ops.py repair
python scripts/delisted_ops.py reconcile
python scripts/delisted_ops.py reconcile --apply
python scripts/delisted_ops.py coverage --start 2016-01-01 --universe all_a_sh_sz
```

Only `coverage` is a gate: it exits 1 when verification does not pass, so any workflow claiming to be "survivorship-safe" should pass it first.

The pass claim of `coverage` is deliberately narrow: it proves that the delisted catalog has been fully scanned and that the known delisted securities overlapping the window have a consistent
last valid trade and security master data; it does not prove that every trading day between the two ends is continuous. Sources may keep
zero-volume placeholder rows before a suspension or formal delisting, and the gate does not mistake them for the last trade. Securities whose catalog end date is after the window but which have no quotes in the window proving they were
listed go into `unknown_overlap` by default. When the complete Baostock security master explicitly gives an IPO after the window, they are listed as `not_yet_listed`; when every expected trading day in the complete trading calendar has independent Baostock suspension evidence, they are listed as `verified_nontrading`. Derived quote gaps, calendars missing dates or incomplete suspension records cannot serve as grounds for passing; both categories are kept item by item in the report. Before a security identity record is updated, a content-addressed historical copy is kept.

`reconcile --apply` does not take the "last record" returned by a single vendor as the truth: automatic changes are allowed only when there is a curated positive-volume end point,
that end point is no later than `instruments.delist_date`, and the old catalog date also falls after the formal delisting date or on a non-trading day.
It refuses to run if any active ingestion run is detected; the catalog before the change is saved in
`meta/state/history/`, and the quality receipt is written to `meta/quality/`.

Formerly `cne delisted discover / reconcile / repair / coverage`.

## Partition rewrites (`cne repair repartition`)

Rewrites historical partitions into the period configured in `DatasetSpec.partition_granularity`
(see [partition granularity](../architecture/lake-layout.md#day-to-day-management)). Without arguments it only lists the datasets awaiting a rewrite,
so the listing form can be run at any time.

```bash
cne repair repartition                          # datasets awaiting a rewrite
cne repair repartition --all                    # preview the impact first
cne repair repartition trading_calendar --apply # a single dataset
```

The read path resolves the layout from the directory shape, so changing the granularity itself **does not need** a migration; this only gathers up fragmented files. Writes first build a temporary directory,
write each partition and check the total row count, then swap it in with a single rename, so a crash midway leaves the original data untouched; repeated runs are idempotent.

It used to be the repository script `scripts/migrations/repartition.py` (earlier `cne repartition`); it now ships with the package, previews by default, and rewrites only with `--apply`. What triggers it is a change of registry granularity under an existing lake, not routine operations. One-off data migrations for lakes written by older versions are `cne migrate`'s job.

## Tests and smoke runs

### smoke_daily_e2e.py

End-to-end smoke test: runs a miniature daily path with mocks or a lightweight config, for CI/local regression.

## Docs sync and translation

### dev/sync_docs.py

Regenerates the parts of the docs that code decides, in both languages: the field tables, the derivatives capability table, the CLI options table, the command side-effect inventory, and the README used on PyPI. `--check` only checks and writes nothing; CI runs it. CLI help in both languages comes from the package catalog (English in the code, Chinese in `src/cnequity/locale/zh_CN/LC_MESSAGES/cnequity.po`), and `python scripts/dev/i18n_catalog.py` fails on help with no Chinese; the English for side-effect notes lives in `scripts/dev/i18n/cli_surface.en.yml`, and adding a command without its English fails the check.

### dev/i18n_catalog.py

Maintains the translation catalog for the CLI, terminal progress and the console's server-side text. Text is written in English in the code and marked with `_()` (translated at run time) or `N_()` (only registered, translated when shown, as for Click help and log templates) from `cnequity.i18n`; a command's docstring is its help and is registered automatically. The Chinese lives in `src/cnequity/locale/zh_CN/LC_MESSAGES/cnequity.po`, and the compiled `.mo` is committed with it, so installing needs no build step:

```bash
python scripts/dev/i18n_catalog.py update    # add new text, drop text no longer in the code
python scripts/dev/i18n_catalog.py compile   # rebuild the .mo from the .po
python scripts/dev/i18n_catalog.py           # check; exits 1 on any problem
```

The check covers: marked text with no entry or no Chinese, entries no longer in the code, fuzzy entries, placeholders that differ from the English, leading or trailing newlines that differ, and a `.mo` that does not match its `.po`. `tests/unit/test_i18n_catalog.py` runs it, and `tests/unit/test_cli_untranslated.py` also catches Chinese literals that bypass the catalog. Babel is a development dependency only; the installed package reads the `.mo` with the standard library.

### dev/doc_translations.py

Lists English pages that are behind their Chinese originals. Each English page records, in `scripts/dev/i18n/doc_sources.json`, a digest of the Chinese page it was translated from; when the Chinese changes and the English does not follow, `tests/unit/test_docs_i18n.py` fails. After updating the English, mark it:

```bash
python scripts/dev/doc_translations.py --mark reference/mcp.md
```

Generated tables are left out of the digest, so regenerating a table never asks for a new translation.

## Related docs

- [Operations runbook](runbook.md)
- [Quickstart](../getting-started/quickstart.md)
