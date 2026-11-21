# Design

CNEquity is a research data lake for the China market that runs on your own machine. It collects continuously from public sources or sources you are licensed for, normalizes the data, stores it as open Parquet, and serves it through Python, SQL, a dashboard, and a read-only MCP server. This page explains the design choices and how the parts fit together; each section links to the page that covers usage. The maintainer sets product direction and scope; use Issues to report use cases and problems.

## Scope {#scope}

| Need | Fit |
|---|---|
| The same history is reused across many studies, and you need to know where it came from, what it covers, and when it changed | Primary use |
| An occasional quote or table | Calling the source API directly is simpler |
| Data that requires a vendor license or service commitment | Collecting public sources does not replace a license or an SLA |
| Backtesting, model training, trade execution | The lake can be an input; strategy and execution belong downstream |
| Letting an AI agent query history | `cne mcp` offers read-only tools |

The package ships no market data. You provide the disk, the scheduling, and access to the upstream sources. Snapshot-only data accumulates only from the day you enable it. The code license grants no right to redistribute upstream data; see [License and sources](../legal-and-data-sources.md).

## Principles {#principles}

- **Describe quality honestly.** Gaps in free sources are normal. Missing values are stored as `NULL`, closed sessions get no invented bars, and a failed source keeps the old data and marks the run `degraded`. An empty table, a default value, or today's state never stands in for verified history.
- **The data is yours.** It is stored locally in open formats that common analysis tools read. Configuration decides what is collected, from which sources, and when.
- **Research semantics are checkable.** Raw prices and adjustment factors are stored separately. Historical universes, PIT, and adjustment each have evidence requirements; when evidence is missing, a strict query refuses instead of quietly degrading.
- **Corrections are traceable.** Published data is never edited in place; a correction publishes a new revision. Pinned revisions, research snapshots, and read receipts make results reproducible.
- **Recoverable.** A failed step never damages published data, and rerunning resumes where it stopped.

## Components {#components}

```text
source adapters → collection steps → staging → write validation → publish (one catalog transaction) → query: Python / SQL / dashboard / MCP
                       ↑                                                  │
                    planner ←──────────── coverage ledger ←────────────────┘
```

The code has two layers. `equitylake_core` is a market-neutral lake engine: the catalog, writes, the coverage ledger, the planner, and locks; it imports no market code. `cnequity` is the China A-share package: the dataset registry, source adapters, scheduling, quality checks, query semantics, and the CLI.

Datasets are grouped by research use into L0–L9 (reference, prices, corporate events, fundamentals, capital flows, structure and industry, macro, sentiment, risk and compliance, derivatives). Each dataset declares its primary key, column types, units, partitioning, and history mode (`by_date`, `snapshot_with_backfill`, `snapshot_only`). See [Data and coverage](../datasets/catalog.md) for the list and sources; the contracts are versioned in `contracts/`.

## Lake catalog and storage {#lake-layout}

`[data].root` sets the lake root:

| Path | Contents |
|---|---|
| `lake/catalog.sqlite` | The catalog database: which files belong to which snapshot, each dataset's revisions, the coverage ledger |
| `lake/data/main/<dataset>/<partition>/` | Published Parquet data files |
| `staging/` | Batches being collected and partitions checked out for publishing; resuming a failed run depends on it |
| `_quarantine/` | Candidate partitions stopped by the publication gate and isolated rows, kept for inspection |
| `meta/` | The run manifest (`manifest.db`), migration record (`lake.json`), scheduler markers, resume checkpoints, source caches and decisions, quality reports, raw response archive, snapshots, and locks |
| `duckdb/` | The SQL view database |
| `logs/`, `backups/` | Command logs; the default location for metadata backups |

The catalog uses the DuckLake format with a SQLite catalog database. **Every commit is one snapshot**, and the snapshot number is the version of the whole lake: a reader that pins one snapshot number gets a consistent set of tables.

Storage is "files as partitions": each file belongs to exactly one partition, the directory name is the partition value, files are written sorted by primary key, and a file is never written again once it exists. Publishing registers the new files and drops replaced files whole; checking out a partition uses hard links rather than copies. External tools should not read `lake/data` directly; use `cne storage export` to get standalone Parquet.

## Writing and publishing {#write-path}

Data from one run reaches readers through these stages:

1. **Collect.** Adapters request what the plan asks for and land the results in `staging/` as batches. Batches send heartbeats; a crashed or failed batch is left for `cne run retry`, and successful batches are not fetched again.
2. **Check out and merge.** The partitions to change are checked out of the catalog into `staging/_checkout/<run_id>`, merged with the new rows, and deduplicated. For one primary key, the row kept depends on evidence class, `fetched_at` order, and source priority; PIT datasets keep every version.
3. **Validate.** Rows with a NULL key, a wrong type, a non-finite number, or a broken declared rule (such as `high < low`) are not written. Most datasets reject the whole batch; a few (derivative bars, Beijing exchange limit-band violations, and similar) isolate the offending rows in `_quarantine/`. Vendor placeholders for missing values become `NULL` according to each dataset's value rules.
4. **Publication gate** (optional, `[quality].publication_gate`). The partitions to change are registered in a copy of the catalog database, the copy is audited, and the result is compared with the committed baseline. If new or worse `error` findings appear, the rehearsal is repeated without each candidate dataset in turn, so only the dataset that caused them is stopped.
5. **Commit.** One catalog transaction replaces the changed partitions and records the revisions and coverage answers. Readers see all of it or none of it.

Only the commit holds the lake lock; reading, merging, and the publication gate run without it. Before committing, the writer checks that nothing else has moved the base revision it checked out, the input revisions it declared, or the revisions its plan read. If any of them changed, the commit is refused, published data is unchanged, and running the command again is enough.

Datasets publish separately; there is no long transaction across the whole lake. Datasets written by the same run commit in the same snapshot.

## Coverage and planning {#coverage}

A dataset's time is cut into cells (sessions, days, weeks, months, quarters, or years, and per security where needed). A cell is known in three ways:

- **Expected** follows from the declared cut, the trading calendar, and the security universe. `[datasets.history_from]` and `[datasets.history_until]` narrow a lake's history for a dataset.
- **Covered** is computed from the published rows every time; nothing keeps a second copy that could disagree. Watermarks are derived the same way.
- **Asked and empty, or answered in part**, cannot be read from rows that do not exist, so it is recorded in the coverage ledger `_attempts` and committed in the same snapshot as the data. An empty answer counts only after its confirmation period; a partly answered cell is still owed.

Building a lake, the daily update, backfill, and repair all ask the same question: which cells in this range are expected and not yet there? The planner reads the lake and sends no requests; it hands the gaps to the sources. That is why rerunning an interrupted command continues where it stopped, and why `cne backfill` skips days that are covered or confirmed empty by default.

## Quality {#quality}

Three layers each do one job:

| Layer | When | On failure |
|---|---|---|
| Write validation | Every write | Bad rows do not enter the lake |
| Publication gate `publication_gate` | Before commit, on the candidate data | `block` refuses new or worse errors; `shadow` only records; default `off` |
| Audit gate `audit_gate` | After publishing, in `cne audit` / `cne check` | Decides the run status only: `block` fails the run without retracting published data; default `shadow` |

Findings carry `info`, `warning`, or `error` severity and are written under `meta/quality/`. Cross-source comparisons (for example, against the closing prices the exchanges publish) report differences and do not rewrite rows. Each row's provenance columns are `source`, `data_version`, and `fetched_at`; there is no row-level quality tier. Freshness, coverage completeness, and correct semantics are three separate checks: a fresh latest date does not mean the history is complete.

## Revisions, retention, and reproducibility {#revisions}

Every publication of a dataset creates a revision (a revision number and a revision id), recorded in the catalog. Reads can pin each dependency with `revision` or `revision_map`; `load_with_receipt()` returns the revisions and data range a read actually used. See the [Python API](../reference/python-api.md).

At the end of `cne backfill` and `cne run daily`, each dataset keeps its latest `keep_generations` revisions (default 5), and revisions registered with `cne storage hold` are always kept. A removed revision keeps its record, but reading it fails explicitly instead of reading a newer one. Retention avoids the files that running long queries, exports, and snapshot creation are reading; protect a Python lazy scan with `read_scope()`.

For long-term reproduction, keep a research snapshot (`cne snapshot create --research`) together with the query parameters, configuration, and software version. Snapshots contain no credentials.

## Concurrency and locks {#locks}

| Lock | Purpose |
|---|---|
| Run locks (`meta/locks/`) | One job of a kind at a time; daily updates and event streams use separate locks, so they can run in parallel |
| Write lock (`lake/write.lock`) | Held while committing revisions |
| Maintenance lock (`lake/maintenance.lock`) | Shared by managed readers and writers; held exclusively by operations that need an idle lake, such as upgrades |
| File locks (`lake/readers.lock`, `snapshot-locks/`) | Shared by readers; retention deletes files only when no reader holds them |
| Scheduler locks (`locks/`) | Shared by the system timer and the dashboard operations page |

The operating system holds the locks and releases them when the process exits. A lock file on disk does not mean a process still holds it; do not delete lock files to recover a run.

## Runs and statuses {#runs}

Runs, batches, steps, and dataset results share one set of statuses: `running`, `success`, `degraded`, `failed`, `skipped`, `blocked`, `interrupted`, `superseded` (batches also use `queued` and `stale`). `degraded` means the result is usable but did not cover the whole requested range. The reason for a skip is in `reason_code` (such as `non_trading_day`, `not_scheduled`, `locked`, `disabled`). `success`, `degraded`, and these skips all exit with 0.

The daily update runs schedule groups in dependency order, then the event streams (announcements and news), which ignore the trading calendar. `cne schedule install` installs a system timer that runs maintenance (audit, metadata backup, retention) after each session's daily update. See [Daily updates](../operations/runbook.md).

## Query semantics {#query}

`load()` carries the research semantics: adjustment (`qfq`, `hfq`, `total_return`), versioned historical universes (`profile`), PIT (`as_of` and `pit_mode`), and strict evidence checks. `scan()` and SQL read raw columns and apply none of these. The dashboard lets people browse the lake and start allowlisted jobs; MCP serves models and is read-only. See [Query and research](../datasets/query-guide.md).
