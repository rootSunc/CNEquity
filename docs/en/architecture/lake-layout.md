# Lake layout

The lake root is determined by `[data].root` in the configuration. Demo lakes and production lakes should use separate paths.

| Path | Purpose |
|---|---|
| `staging/` | Batch data waiting to be merged; failure recovery may depend on it |
| `curated/`, `derived/` | Merge working copies and the compatibility layout; scanning them directly does not pin a published version |
| `meta/revisions/` | Data version receipts, current-version pointers, and immutable data files |
| `meta/state/` | Update watermarks and coverage state |
| `meta/quality/` | Quality check and source discrepancy reports |
| `meta/raw/` | Archive of raw responses |
| `meta/raw_storage.json` | Optional external payload locations; source sidecars stay in `meta/raw/`, and snapshots materialize external payloads without machine-specific paths |
| `meta/source_snapshots/` | Snapshots used for source verification |
| `meta/locks/` | Locks used to coordinate single runs, publishing, and writes |
| `locks/` | Scheduler locks shared by the scheduling scripts and the operations page. Can be moved elsewhere with `CNE_SCHEDULER_LOCK_DIR` or `CNE_LOCK_DIR` |
| `meta/snapshots/` | Default portable data snapshots. Contains the selected datasets with their state, contracts, and revisions; does not contain the original configuration or credentials |
| `meta/serve_jobs/` | Operations page task records and cancellation markers |
| `duckdb/` | SQL view database |
| `logs/` | Run logs |
| `backups/` | Optional location for in-lake backups. The scheduling scripts put metadata tars here by default; the operations page can also put data snapshots here. A metadata tar cannot restore market data, and a data snapshot cannot replace a full-disk copy |

## Day-to-day management {#day-to-day-management}

Read published data through the Python API or `cne query`. Do not modify version pointers or generation files directly, and do not put backups into `curated/`. For long-term storage use [research snapshots](../reference/python-api.md), and keep logs and reports in your own local directories.

Before cleaning up staging, confirm that the task has finished and been merged. The presence of lock files in `meta/locks/` or `locks/` does not mean a process still holds the lock; do not recover a run by deleting lock files. See [Troubleshooting](../operations/troubleshooting.md).

A partition can hold several Parquet files whose primary keys do not overlap; readers take all of them. Backfill and the daily run write only new keys as a new file and rewrite only the files that hold changed keys, instead of rewriting the whole partition; `cne run maintain` merges partitions that reach 4 files back into one each day (or run `cne storage merge-files`). A partition over 200,000 rows written whole is split into buckets by symbol, `part-bNNN-of-MMM.parquet`, so an update rewrites only its bucket. External programs that read the Parquet directly must read every file in a partition directory, not only `part-merged.parquet`.

When the partition layout changes, old directories may still be readable, but overlapping with the new directories produces duplicate primary keys. Follow the [migration script notes](../operations/scripts.md) to dry-run and back up first, then unify the layout.
