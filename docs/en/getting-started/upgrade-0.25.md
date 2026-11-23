# Upgrade to 0.25.0

0.25.0 changes how data is stored: all published data and revision records move into the lake catalog under `lake/`. The `curated/` and `derived/` folders and the pointers and receipts under `meta/revisions/` are no longer used. An existing lake needs one run of `cne upgrade --apply`.

## Who needs to upgrade {#who}

- **Lakes created by 0.25.0**: no upgrade.
- **Lakes created by 0.24.0 or earlier**: upgrade once.
- **0.25.x is the last series that upgrades these older lakes.** Later releases do not recognize a lake from before 0.25.0; upgrade it with 0.25.x first.
- **To check**: `cne upgrade` lists the steps still owed, and `cne check` says so in its verdict.
- **Until the upgrade**: reads work as before; writes (daily runs, backfills, derives, repairs) are refused with a pointer to `cne upgrade`.

## Before you upgrade {#before}

- **Stop everything that uses the lake**: schedulers, `cne serve`, `cne mcp`, running queries and scripts. The upgrade waits up to 30 seconds for reads in progress; if a process is still reading or writing, it stops, explains why, and changes nothing.
- **Back up the whole data root (`data_root`)**:
    - The upgrade deletes the old layout (`curated/`, `derived/`, `meta/revisions/`), and this cannot be undone.
    - The upgrade's own backup (`backups/upgrade-<time>/`) holds the lake's records, not its data files.
    - Rolling back needs this full copy.
- **Disk space**: on a filesystem with hard links, data files are linked into the lake catalog and take almost no extra space. Without hard links, room for one more copy of the data is needed. The `cne upgrade` preview gives an estimate.
- **DuckDB extensions**: the lake catalog needs DuckDB extensions, downloaded on first use. If the official extension site is unreachable, set `EQUITYLAKE_EXTENSION_DIR` (a local folder) or `EQUITYLAKE_EXTENSION_MIRROR` (a mirror).
- **Old snapshots**: 0.25.x can still verify and restore snapshots made by `cne snapshot create` before 0.25.0; later releases cannot read them. To keep one, restore it with 0.25.x (`cne snapshot restore`), or create a new snapshot after the upgrade.

## Steps {#steps}

1. Install 0.25.x: `python -m pip install -U "cnequity==0.25.*"`.
2. Check the config:
    - A config that extends the built-in defaults (`extends = "builtin"`) needs no change.
    - For a fully expanded config, review `cne config upgrade --dry-run`, then run `cne config upgrade`.
3. Preview: `cne upgrade`. Review the migrations and the disk and extension checks. For anything marked `BLOCKED`, follow its message; nothing has been changed.
4. Upgrade: `cne upgrade --apply`. It backs up the lake's records, then runs the migrations in order and reports progress. If it stops, fix the reported problem and run the same command again; it continues where it stopped.
5. Verify: run `cne status --datasets` and `cne check`, then check key results with your usual queries.
6. Resume: restart `cne serve` and `cne mcp`, and re-enable scheduled jobs.

## What the upgrade does {#migrations}

`cne upgrade --apply` runs these migrations in order and records each result in `meta/lake.json`:

- `lake-catalog`: registers every published dataset in the lake catalog and switches over only after a row-by-row comparison matches.
- `revision-records`: imports the revision records into the lake catalog.
- `legacy-datasets`: publishes datasets that an old sample or demo lake stored without a revision.
- Data repairs from 0.18.0–0.23.0: only those this lake has not run yet; some need network access.
- `attempts-ledger`: imports the coverage records from `meta/state/` into the coverage ledger.
- `explicit-holds`: releases holds that were derived from revision ids found in text files; holds from `cne storage hold` and case dependencies stay.
- `drop-generations` (cannot be undone): deletes the old layout the lake catalog replaced; held revisions stay for now.
- `archive-held-revisions`: moves the revisions that are still held into the lake catalog and deletes their old folders once the rows match. They remain readable by revision id.
- `meta-layout`: moves what is left in `meta/state/` into its own folders and retires `meta/state/`.

To preview a single migration: `cne migrate --only <migration ID>`.

## Changes after the upgrade {#changes}

- **External tools that read files directly**: stop reading `curated/` and `derived/`. Use instead:
    - `cne storage export <datasets…> --out <folder>`, which writes plain Parquet;
    - `cne query --sql …`, Python `load()`, or MCP.
- **Snapshots**: can only be created on an upgraded lake. The incremental commands `cne snapshot delta create|verify|apply` are removed.
- **Removed commands and options**:
    - `cne storage sync-catalog`, `cne storage merge-files`, `cne storage apply`;
    - `cne repair layout`, `cne repair repartition`;
    - `cne audit --research-universe`; use `--universe-profile`.
- **Holds**: come only from `cne storage hold` (optionally `--until <date>`) or case dependencies. A revision id appearing in a file no longer keeps that revision.
- **Python API**:
    - `load(universe="all_a")` raises an error; use `profile="legacy_all_a"`.
    - The old universe-profile function names (`get_universe_profile`, `list_profiles`, `show_profile`, `profile_json`) are removed; use `resolve_universe_profile`, `list_universe_profiles` and `show_universe_profile`.
    - `revision_receipt` from `load_with_receipt()` and `dataset_state()` is `None`; the revision number, revision id and content digest are unchanged.
- **`meta/state/` is retired**: scripts that read it should use `cne status` or `dataset_state()`.
- **Run status values**: `warning` is now `degraded`, and `skipped_*` is now `skipped` with a reason code.
- **Config keys that no longer have an effect**: `publish` under `[storage]`, and `planned` and `planner_only` under `[datasets]`. Leaving them in the config only produces a warning.

## If something goes wrong {#troubleshooting}

- **The preview shows `BLOCKED`**: provide the extension or disk space and run it again; nothing has been changed.
- **Another process is reading or writing the lake**: stop `cne serve`, `cne mcp`, queries or scheduled jobs and run it again; nothing has been changed.
- **A migration fails**: the steps before it are complete and recorded. Fix the reported cause and run `cne upgrade --apply` again.
- **To roll back**: install the previous release and restore the full pre-upgrade copy of the data. Restoring `backups/upgrade-*` alone is not enough.
