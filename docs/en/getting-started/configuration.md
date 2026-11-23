# Configuration reference

The configuration is a TOML file. The packaged template is authoritative for every key and default, and each of its sections is commented: see the repository copy [`configs/cnequity.example.toml`](https://github.com/rootSunc/CNEquity/blob/main/configs/cnequity.example.toml), or run `cne config show --defaults`. This page explains how configuration takes effect and what each section controls.

```bash
cne config create      # write a per-user config holding only extends and [data].root
cne config path        # the active config, why it was chosen, and the lake it points to
cne config show        # the full effective config, marking values set in the file, with credentials masked
cne config validate    # validate
cne config diff        # report steps and settings a full config lacks compared with the built-in defaults
```

## Lookup order {#lookup}

The CLI, `load()`, MCP, and the timers share one rule: `--config` (`config=` in Python) → the `CNE_CONFIG` environment variable → `configs/cnequity.toml` in the working directory → the per-user `~/.config/cnequity/config.toml` (honoring `XDG_CONFIG_HOME`; `%APPDATA%\cnequity\config.toml` on Windows). A bare command therefore points at the same lake from any directory.

Relative paths in a config (`[data].root`, `[duckdb].path`, credential files) resolve against the config file's directory. `cne config create` and `cne init` write absolute paths.

## Extending the built-in defaults {#extends-builtin}

```toml
extends = "builtin"

[data]
root = "/abs/path/to/lake"
```

A config with `extends = "builtin"` holds only what differs from the defaults; everything else comes from the packaged template of the installed version, adjusted for the platform (for example, `workers = 1` on macOS and Windows). Settings and schedule steps added by a new release take effect after upgrading.

- Tables merge key by key; scalars, arrays, and arrays of tables (such as `[[failover.datasets]]`) replace the default whole.
- `[data].root` never comes from the defaults and must be in the file.
- Change schedule groups with `steps_add`, `steps_remove`, and `enabled = false`, so swapping one step does not freeze the rest of the group. `steps = [...]` replaces the whole group, and `cne config diff` reports when it hides steps a new release added.

```toml
[job.daily.groups.capital]
steps_remove = ["fund_flow"]
steps_add = ["fund_flow_ths"]

[job.daily.groups.research]
enabled = false
```

A full config without `extends` still works, with missing keys falling back to built-in values. When upgrading, run `cne config upgrade` to add new steps, or `cne config upgrade --minimize` to rewrite it in the extending form once (`--dry-run` previews; the original is backed up).

## Lake and resources {#lake-and-resources}

| Section | Meaning |
|---|---|
| `[data]` | `root`: the lake root. Use different paths for different lakes (production, demo) |
| `[duckdb]` | `path` (accepts the `{data.root}` placeholder), `memory_limit`, `threads`: resources for query connections |
| `[duckdb.<workload>]` | Optional overrides of `memory_limit`, `threads`, `temp_directory`, and `max_temp_directory_size` for `write`, `audit`, `export`, and `maintenance` connections. Built-in values: 4GB for write and export, 512MB and one thread for audit, 8GB for maintenance |
| `[query]` | `scan_budget_rows` (default 4000000): the most rows dashboard browsing and MCP `query_dataset` may select before paging |
| `[logging]` | `console_level` and `file_level` (default `INFO`); `heartbeat_seconds` (default 30; report progress after this long without output, 0 turns it off); `retention_days` (default 30; `cne run clean` removes older command logs, 0 keeps them) |

## Collection scope {#ingest-scope}

| Section | Meaning |
|---|---|
| `[universe]` | `ingest`: the security classes the daily update covers, `all_a` (Shanghai, Shenzhen, and Beijing A shares, default), `all_a_sh_sz`, or `all_instruments`. It limits what is fetched and is unrelated to research universes; ST, suspended, CDR, and delisted stocks are always kept. `ingest_listed_funds` and `ingest_eligible_etfs` add ETFs and LOFs |
| `[datasets]` | `best_effort`: best-effort datasets are not scheduled unless listed here (`["all"]` enables all of them). Datasets not enabled show freshness `removed` rather than stale |
| `[datasets.history_from]` | Where this lake's history of a dataset begins, such as `fund_flow = 2023-09-11`; the planner stops counting earlier days as gaps. Move the date earlier to backfill further back |
| `[datasets.history_until]` | For a dataset whose only source has stopped, where its history ends; later days are no longer owed. Remove the line once a new source is set up |
| `[incremental]` | `negative_evidence_ttl_days` (default 7): how long "the source has nothing here" stays valid; `deep_reconciliation_dow` (default 6): the weekday of the weekly deep reconciliation, 0 for every run |
| `[minute_bars]`, `[trade_ticks]`, `[futures]` | Minute bars, ticks, and futures and options, off by default. Request volume and disk use grow with scope; check usage on a small scope first |
| `[orchestrator]` | Concurrency and retries: `workers`, `tdx_daily_workers`, `source_concurrency` (in-flight requests per source), `batch_size`, `max_retries`, `batch_stale_seconds`, `batch_stall_seconds` |
| `[on_demand]` | Datasets fetched on demand (default `stock_news`, `research_reports`), triggered by `cne query --dataset X --symbol Y` |
| `[raw_archive]` | Raw source responses are compressed into `meta/raw`. Request credentials, proxy settings, cookies, and authorization headers are never archived. Register large originals in an external directory with `cne storage raw-external` |

## Sources {#sources}

Each `[sources.<name>]` section controls one source or request channel. `enabled` and `min_interval_seconds` (the cross-process request interval) are common to all of them; EastMoney adds per-host pacing, daily budgets, circuit breakers, and proxy settings, and Baostock adds batch cooldowns. The template comments explain each key. These are conservative client settings, not safe quotas promised by the sources. Write `enabled = false` for sources you do not use instead of deleting their section.

`[tdx_protocol]` controls the TDX market data protocol (hosts, pacing, timeouts). `[failover]` configures primary and backup source snapshots and comparisons and never switches the primary source automatically. `[exchange_audit]` compares daily bars with the closing prices the exchanges publish; its findings are advisory and never fail a run.

When several lakes or processes share one network egress, give them the same `CNE_RATE_LIMIT_ROOT` so they share pacing, cooldown, and budget ledgers.

### Credentials {#credentials}

Optional paid sources (the `[sources.tushare]` token and the `[sources.ths_official]` key) are read in this order: the environment variable `TUSHARE_TOKEN` / `HITHINK_FINANCE_API_KEY` → the file `token_file` / `api_key_file` (first line only) → a value written in the config. Timers do not see variables exported in your login shell, so a scheduled lake should use a file with mode 600. On loopback access, the `cne serve` settings page can also save, replace, or remove these keys directly: it writes the secret to a separate local file with mode 600, records only `token_file` / `api_key_file` in the config, and checks the key immediately. `cne doctor` warns when a credential is written in the config or the config file is readable by other users. Without credentials, these sources send no requests.

Tushare is used only for on-demand backfill by default (`[sources.tushare].daily = false`). When set to `true` (or enabled in `cne serve` settings), the daily update runs the `tushare` group only while the token check passes, and Tushare-primary datasets count toward freshness only while daily collection is active and verified.

## Scheduling {#schedule}

| Section | Meaning |
|---|---|
| `[[job.daily.waves]]` | The dependency order of the daily backbone |
| `[job.daily.groups.<name>]` | Daily schedule groups: `steps`, `at` (reference time), `cadence` (`daily` or `weekly`), `weekday`. A weekly group rests only datasets that can catch up by date; snapshot datasets still run every session, because a missed day cannot be recovered |
| `[job.daily]`, `[job.stale]` | `run_at` in Beijing time: 17:30 for the daily update and 21:00 for the snapshot catch-up by default. The system timer runs each once per trading day after that time, regardless of the machine's time zone |
| `[job.events.groups.<name>]` | Event stream groups (announcements, regulatory events, news): they ignore the trading calendar and use a separate run lock, so they can run alongside the daily update |
| `[[job.events.schedules]]` | The timer installed by `cne schedule install --events` follows these: `group`, `every_minutes`, `jitter_seconds`, `quiet_hours`, `quiet_every_minutes` |
| `[job.maintenance]` | `cne run maintain`: `full_audit_weekday` (weekday of the whole-lake audit, 0 never), `backup_dir`, `backup_retention_days`, `backup_retention_count`, `gate_groups` (failures of these groups fail maintenance at once), `soft_fail_max_days` |
| `[job.init.phases]` | The phase order of `cne init` |
| `[source_health]` | `vantage`: a label for this machine's network egress; source probe results are read per label |

`cne run daily` without arguments runs every daily group in config order, then every event group; `--group <name>` runs one group.

## Quality and storage {#quality-and-storage}

| Section | Meaning |
|---|---|
| `[quality]` | `publication_gate`: audit candidate data before publishing, `off` (default), `shadow`, or `block`. `audit_gate`: what a run does when the post-publication audit reports `error`, `off`, `shadow` (default; records to `meta/quality/audit_gate.jsonl`), or `block` (the run fails, but published data is not retracted). `run_failure_window_hours` (default 24) and `run_failure_streak` (default 3, 0 turns it off): report when this many consecutive runs inside the window are unhealthy |
| `[storage.retention]` | `keep_generations` (default 5): the latest revisions kept per dataset, 0 for no automatic cleanup. `keep_days` (default 7, 0 for no limit): a replaced revision is kept only if it was current within this many days. The current revision and revisions registered with `cne storage hold` are always kept |
| `[storage.free_space]` | Free space on the lake's disk, in GB. Below `warn_gb` (default 50), retention keeps replaced revisions for one day and `cne run maintain` fails; below `stop_gb` (default 10), runs, backfills, derives and repairs refuse to start. 0 turns either check off |
| `[research]` | `holdout_start`: `cne decision-data` refuses windows reaching this date, keeping out-of-sample data hidden from evidence inventories |

See [Design](../architecture/overview.md#quality) for how the two gates divide the work. Before switching to `block`, read the shadow-mode records to learn how often the gate would fire.

## Machines with little memory or disk {#small-machines}

The defaults assume at least 16GB of memory and a few hundred GB of free disk. On a smaller machine, adjust the settings below first, start with a narrow collection scope, and widen it while watching `cne storage usage`.

**Memory (4GB or 8GB).** Each DuckDB connection has its own memory limit: 2GB for queries, 4GB for writes and exports, 8GB for maintenance. On Linux, daily-bar collection also starts 4 processes by default, each with its own memory. On 8GB, lower at least the maintenance limit; on 4GB, lower all of them and cut concurrency to 1 or 2:

```toml
[orchestrator]
workers = 2
tdx_daily_workers = 1

[duckdb]
memory_limit = "1GB"
threads = 2

[duckdb.write]
memory_limit = "1.5GB"

[duckdb.export]
memory_limit = "1.5GB"

[duckdb.maintenance]
memory_limit = "2GB"    # "4GB" on an 8GB machine
```

DuckDB spills what exceeds a limit to its temporary directory, which is slower but does not fail; `temp_directory` and `max_temp_directory_size` set where and how much. When memory runs out the system may end a process and the log shows `worker pool broke`; see [Troubleshooting](../operations/troubleshooting.md).

**Disk (around 50GB).** The lake grows with the instrument scope, the datasets enabled, and the revisions kept. On a small disk:

- Narrow the scope: `[universe].ingest = "all_a_sh_sz"`, and leave `[minute_bars]`, `[trade_ticks]` and `[futures]` off.
- Keep fewer old revisions: set `[storage.retention].keep_generations` to 2 or 3.
- Lower the space checks: `[storage.free_space].warn_gb` defaults to 50, which on a 50GB disk warns all the time and fails daily maintenance; use values such as `warn_gb = 10` and `stop_gb = 3`.
- Run `cne run clean` regularly.

**How much a copy takes depends on the file system.** Snapshots and rehearsal copies are clones on macOS APFS and on Linux btrfs and XFS (reflink), and take almost no extra space. On Windows with Python 3.12 or newer, ReFS and Dev Drive volumes (from Windows 11 24H2) clone as well. Linux ext4 and Windows NTFS cannot clone, so each copy takes its full size; on a tight disk, make fewer snapshots and rehearsal copies.

## Environment variables {#environment}

| Variable | Purpose |
|---|---|
| `CNE_CONFIG` | Config path, lower precedence than `--config` |
| `CNE_LOG_DIR` | Command log directory, default `{data.root}/logs`. Log files are named `cne-<command>-<UTC time>-<pid>.log` |
| `CNE_RATE_LIMIT_ROOT` | Directory of the shared pacing, cooldown, and budget ledgers |
| `CNE_PUSH2_PAUSED=1` | Pause EastMoney push2 requests locally, the same as `[sources.eastmoney] push2_paused` |
| `CNE_SOURCE_VANTAGE` | Override `[source_health].vantage` for one run |
| `CNE_SCHEDULER_LOCK_DIR`, `CNE_LOCK_DIR` | Scheduler lock directory, default `{data.root}/locks` |
| `EQUITYLAKE_EXTENSION_DIR`, `EQUITYLAKE_EXTENSION_MIRROR` | A local directory or mirror for the DuckDB extensions the catalog needs, when the official extension site is unreachable |

## Settings with no effect {#retired}

| Setting | Note |
|---|---|
| `[storage].publish` | Since 0.25.0 every write publishes into the catalog; keeping it is not an error |
| `[datasets].planned`, `[datasets].planner_only` | Trial settings; since 0.25.0 coverage planning always applies. Keeping them only warns |
