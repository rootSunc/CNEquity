# Runbook

For users who already have a production lake: scheduling, health gates, backup and restore, and backfill acceptance. For building a lake for the first time, see the [initialization guide](../getting-started/initialization.md).

| Task | Entry point |
|---|---|
| Scheduled updates | `cne schedule install`: each trading day's update, then maintenance (audit, gates, metadata backup); see [Installing the schedule](#install-schedule) |
| Transient failure | [Troubleshooting](troubleshooting.md): first identify the failed run / batch, then retry by scope |
| Extending history | [Initialization and resume](../getting-started/initialization.md); review `--plan` before a backfill |


## Installing the schedule {#install-schedule}

```bash
cne schedule install        # registers the current user's timer after confirmation
cne schedule status         # installed? active? when did it last check?
cne schedule remove         # pauses and unregisters it; running jobs finish
```

The timer belongs to the current user: launchd on macOS, the crontab on Linux, Task Scheduler on Windows. It checks once a minute; on each trading day, once `[job.daily] run_at` (Beijing time) has passed, it runs `cne run daily` and then `cne run maintain`; after `[job.stale] run_at` it retries only the snapshot data the day missed. Each trading day counts once: it runs after `run_at` and the day stays due until 09:15 on the next trading day. The local time zone and daylight saving do not matter; nothing runs while the machine sleeps, and the next check after waking catches up.

`--pack` updates only the selected research packs' schedule groups (without event streams); `--events` runs event streams on the config's [`[[job.events.schedules]]`](../getting-started/configuration.md#event-schedules) (several entries, each with its own interval, jitter and quiet hours); without schedules in the config, `--events-group` runs one event group every `--events-every` minutes; `--no-stale` leaves out the closing catch-up. Options not given keep their current settings, so installing from the command line never undoes a backup or event schedule set on the dashboard. It is the same timer the `cne serve` operations page manages, and either place shows and changes it. When legacy `scripts/scheduler` launchd jobs are installed it asks you to remove them first, or the lake is updated twice.

### Maintenance after the daily update (`cne run maintain`) {#maintain}

The timer runs `cne run maintain` once after each trading day's update; you can also run it by hand:

1. the last closed session's daily group runs: a failed `[job.maintenance] gate_groups` group (default `core`) fails at once; any other failed group is reported and fails after `soft_fail_max_days` (default 3) sessions in a row, counted under `meta/state/soft_streak/` (the shell pipeline's counts carry over);
2. the run audit; on `[job.maintenance] full_audit_weekday` (Saturday by default) the whole-lake `audit --full --quality-only` instead, which reads every Parquet file and can take hours on a large lake;
3. the freshness gate `status --datasets --gate`, failing only on the schedule groups the timer actually runs (`--groups` overrides);
4. source probes `sources probe --stale-only` (labelled with the `[source_health] vantage` egress) and `sources slo`;
5. run evidence over the last 20 trading days, `verify --runs --days 20`;
6. a metadata backup: the manifest database (a consistent copy through SQLite's backup API), incremental state, revision and source receipts and quality findings, written as `meta-<time>.tar.gz` to `[job.maintenance] backup_dir` (default `{data.root}/backups`) and rotated by `backup_retention_days` / `backup_retention_count`;
7. the expiry preview of `cne run clean`.

It exits 1 when a gate group failed, another group reached its consecutive-failure limit, or the audit, the freshness gate or the backup fails; source probe and run evidence problems are reported without failing. On macOS the timer adds `--notify` for a desktop notification on problems.

## Legacy repository scripts (deprecated) {#legacy-scripts}

`scripts/scheduler/` exists only in a source checkout and is deprecated; it is removed only after the packaged timer covers everything it does and has been verified on a real lake, announced in the changelog beforehand. To migrate, run `scripts/scheduler/uninstall_scheduler.sh` to remove the launchd jobs (delete cron entries by hand), then run `cne schedule install`. `backup_meta.sh` now calls the same backup code as `cne run maintain`.


| Capability | Script | Purpose |
|------|------|------|
| Scheduling | `cne serve` → Operations → Scheduled tasks | Enable, pause or retime across platforms; daily update and closing catch-up results are all viewed in the web UI |
| Scheduling | `scripts/scheduler/daily_pipeline.sh` | Runs the enabled daily-update groups serially in configured order + health check + backup |
| Scheduling | `scripts/scheduler/install_scheduler.sh` | Installs macOS launchd (runs once per trading day after `[job.daily] run_at` Beijing time, independent of the local time zone) |
| Scheduling | `scripts/scheduler/uninstall_scheduler.sh` | Uninstalls launchd |
| Alerting | `scripts/scheduler/health_notify.sh` | Daily audit, weekly whole-lake quality check + per-group freshness + macOS notifications |
| Backup | `scripts/scheduler/backup_meta.sh` | Rotating tar archives of metadata and revision receipts; use snapshots for full data |

The scripts use the repository's `.venv/bin/cne`, and resolve paths relative to the repository root on their own.

### Installing the launchd jobs (legacy)

```bash
cd /path/to/cnequity
scripts/scheduler/install_scheduler.sh
```

The installer keeps host selection separate from the templates: a new install selects the enabled groups from the configuration; a reinstall keeps the installed daily job's
`CNE_GROUPS`, `CNE_SOURCE_VANTAGE`, custom configuration and each job's run time.
Use environment variables to override groups and the network vantage explicitly; for example, a first install on an overseas host:

```bash
CNE_GROUPS=core CNE_SOURCE_VANTAGE=overseas scripts/scheduler/install_scheduler.sh
scripts/scheduler/install_scheduler.sh --check
scripts/scheduler/install_scheduler.sh --dry-run /tmp/cnequity-scheduler-review
scripts/scheduler/install_scheduler.sh --daily-only                  # sync only the existing daily job, do not add the evening job
scripts/scheduler/install_scheduler.sh --stale-only                  # sync only the catch-up job
```

**Run times are configured in Beijing time, independent of the local time zone.** Both the daily update and the catch-up wake up once an hour (the daily update at minute 7 of each hour,
the catch-up at minute 37), and `scripts/scheduler/scheduler_gate.py` decides whether this wake-up is "the real one":

```toml
[job.daily]
run_at = "17:30"   # Beijing time; runs once per trading day after this time (default 17:30)
[job.stale]
run_at = "21:00"   # Beijing time; after that day's daily update has run, retries only the failed snapshot data (default 21:00)
```

- Each runs once per trading day: it runs once `run_at` has passed, and everything up to the 09:15 open of the next trading day counts as that day;
  if it still has not run by the open (for example, the computer stayed off), that day is not made up — historical data is filled by date in the next daily update, and snapshot data misses that day.
- You do not need to care which time zone the machine is in or whether it observes daylight saving time, and you do not need to reinstall; if the computer is asleep at `run_at`, it runs at the next wake-up after it resumes.
- Running `scripts/scheduler/daily_pipeline.sh` by hand is not subject to this check and runs immediately as usual.
- Trading days that have already run are recorded in `meta/state/scheduler/`; a day that already has a daily-update record in the manifest also counts as run,
  so after switching to this mechanism or running by hand, a full pass is not repeated.
- The old `--stale-at` is retired; use `[job.stale] run_at` instead.

`--check` compares the installed files with the templates generated from the host selection and exits 1 if they differ or a job is missing;
it does not start jobs and does not check whether launchd has loaded them. `--dry-run` only writes the preview directory.
A regular install includes three jobs: daily, stale and events; stale inherits the daily groups and limits the catch-up scope through
`cne run daily --stale-only --groups ...`. The interval and groups of an existing events job are kept;
additional manually installed events jobs are not removed by this installer.

The catch-up retries only datasets scheduled as snapshots (fund flow, valuation, popularity ranking, etc.); historical data is left for the next daily update to fill by date.
The catch-up follows the same push2 configuration, budget and circuit-breaker policy as the main job. Whether a separate historical source exists is defined by the dataset contract.

Quality errors in the daily job and lag in the actually scheduled `CNE_GATE_GROUPS` (default core) make the job exit 1.
Soft groups still escalate by failure count, but retries for the same date are not counted as multiple days.
Each week, quality is checked with `audit --full --quality-only`, and freshness is gated separately by per-group `status`,
so unscheduled minute bars do not trigger "数据异常" (data anomaly) merely because they lag. The whole-lake health report still shows this lag.

- Generates `~/Library/LaunchAgents/com.cnequity.daily.plist`
- Wakes up every hour and **runs once per trading day after `[job.daily] run_at` Beijing time (default 17:30)**;
  independent of the local time zone and daylight saving time; a reinstall does not keep an old local time.
- Non-trading days are skipped automatically (exit 0)
- **Missed runs / weekend catch-up**: `python scripts/run_catchup.py` fills core and breadth for the most recent trading day; parts whose watermark has already reached the target date are recorded as `skipped_already_fresh`. To rerun all schedule groups for a given day, use `scripts/scheduler/daily_pipeline.sh YYYY-MM-DD` or `CNE_TRADE_DATE=...`.
- **Choose schedule groups by the current network vantage**: check source health first, then enable the groups that can actually be maintained. A failure from one vantage does not prove that a whole region is unusable; when refused, cool down first instead of switching vantage to continue the same round of fetching. See [Fetch policy and source protection](fetch-policy.md).

```bash
launchctl list | grep cnequity
launchctl start com.cnequity.daily   # trigger manually
scripts/scheduler/uninstall_scheduler.sh
```

<a id="web-schedule"></a>

### Managing cross-platform scheduled tasks in the web UI

Package users can set up scheduled tasks on the operations page of `cne serve` without copying the repository scripts: choose automatic daily update, closing catch-up, daily data backup or standalone event stream, set the time and scope, preview, then confirm. The daily update and the catch-up are each attempted once per trading day; the backup is attempted once after its time on each calendar day in Beijing time, with a choice of datasets and backup directory; the event stream picks event groups from the configuration and runs at an interval of 1–1440 minutes, including weekends and holidays. The system checks once a minute; results and logs appear under recent tasks, and tasks keep running after serve is closed. All web tasks share a single task slot; when it is occupied they wait for a later check. The event stream does not accumulate missed runs, and even after a failure the interval is counted from when the attempt started.

macOS uses the current user's launchd and Windows uses the current logged-in user's Task Scheduler; both require the user to stay logged in. Linux uses the current user's crontab and requires the cron service to be running. Nothing runs while the system is asleep; after it resumes, the next check makes up the daily update and catch-up within the trading-day window; the backup only makes up the current day, and the event stream is judged by the current interval. The page also shows the system task status and the most recent check; if there has been no check for more than 20 minutes, investigate user login, machine sleep and the cron service. A successful system registration means the task definition is installed; only a recent check record shows that the system has actually triggered it.

Daily update and catch-up times are saved in the original configuration's `[job.daily] run_at` / `[job.stale] run_at`; a `.schedule.bak` is saved before the first change, and other settings and comments are preserved. Backup and event stream settings are stored with the lake; backups never delete old copies automatically, so keep an eye on storage space. Pausing blocks subsequent automatic runs; a task that has already started is cancelled separately in the task details. Even if the system task cannot be removed because of a permission change, the pause setting still stops the package entry point from fetching; the page then prompts you to check the system task manually.

The web UI manages only the system tasks it created itself and does not take over existing launchd scripts or other cron / Windows tasks. Check for and disable duplicate entry points before migrating. The web daily update runs `cne run daily` and then `cne run maintain` (audit, gates, metadata backup), the daily backup runs `cne snapshot create` for the selected datasets, and the standalone event stream runs `cne run events` for the selected groups. `cne schedule install|status|remove` manages the same task from the command line. System tasks are pinned to the Python environment, working directory and configuration path in effect when they were created; set them up again after moving the environment, and the configuration that data sources require must also be available in the unattended environment.

**Linux cron (manual setup)**:

```cron
# Wake up every hour; CNE_SCHEDULED=1 makes the script itself use the Beijing-time run_at to run only once per trading day
7 * * * * CNE_SCHEDULED=1 /path/to/cnequity/scripts/scheduler/daily_pipeline.sh
37 * * * * CNE_SCHEDULED=1 /path/to/cnequity/scripts/scheduler/stale_pipeline.sh
# The event stream runs independently, including weekends; adjust to the frequency you need
20 20 * * * /path/to/cnequity/scripts/scheduler/events_pipeline.sh
```

**Windows Task Scheduler** (native Win10/11; `daily_pipeline.sh` does not work in PowerShell):

1. First make sure `cne doctor` and `cne config validate` pass, and use a short absolute path for `data.root` (such as `D:\lake`).
2. Open Task Scheduler → Create Basic Task → daily at 16:05 (or any time after the close; it also triggers on weekends).
3. For the action, choose "Start a program":

| Field | Example |
|------|------|
| Program/script | `C:\path\to\.venv\Scripts\cne.exe` |
| Add arguments | `run daily --config C:\path\to\configs\cnequity.toml` |
| Start in | `C:\path\to` (the directory containing the repository or configuration) |

This one task already includes the event stream (announcements, regulatory events, news), so there is no need to create a separate task for `run events`; on non-trading days the daily-update groups are skipped automatically while the event stream runs as usual. Times in Windows Task Scheduler and plain cron are local time; only jobs that use the repository's scheduler gate are judged by the configured Beijing time. To run the event stream more often, schedule `cne run events --group news_wire` separately; when it runs at the same time as the daily update, the event-stream segment of the daily update is recorded as `skipped_locked` and does not count as a failure.

> If Chinese text is garbled in the console: run `chcp 65001`, or set the user environment variable `PYTHONUTF8=1`.

## Daily pipeline

```
core → capital → signals → fundamentals → macro_risk → research
  → health_notify.sh
  → backup_meta.sh
  → group summary (gate vs soft)
```

- A failure in one group does not stop the following groups (collect as much data as possible)
- The final summary distinguishes **gate** (default `CNE_GATE_GROUPS=core`) from **soft** (EastMoney, etc.)
- Default `CNE_SOFT_FAIL_OK=1`: when gate is OK, soft failures are **warn-only, exit 0** (only transient failures are tolerated; persistent failures escalate);
  if any group failure should block, set `CNE_SOFT_FAIL_OK=0` so that soft failures still exit 1
- EastMoney timeouts/connection failures are not retried (`[sources.eastmoney] timeout_sec`, default 15s)
- The production `daily_pipeline.sh` usually sets `workers=1` (compatibility of the TDX client with multiprocessing)

For the mapping of groups to steps, see [Configuration — schedule groups](../getting-started/configuration.md#schedule-groups).

## Logs

Directory: `{data.root}/logs/`

| File | Contents |
|------|------|
| `daily-YYYYMMDD.log` | cne output for each group |
| `health-YYYYMMDD.log` | Full audit / status text |
| `launchd.out.log` / `launchd.err.log` | launchd standard streams |

## Routine inspection commands

```bash
cne status --datasets --gate                 # freshness; exits 1 on STALE
cne audit --full                      # lake-level health; exits 1 on UNHEALTHY
cne stats show                        # row count overview
cne sources slo --enforce             # 30-day availability of key sources (fail-closed even when history is missing)
cne sources resilience --enforce      # independent fallback gate for core datasets and single-source blast radius
cne verify --runs --days 20 --enforce # run evidence over consecutive trading days
```

The operations page of `cne serve` can start the freshness check, whole-lake audit and `cne verify` from this list, as well as the reruns, retries and catch-ups below. The page runs only one task at a time and takes the same lock as the scheduler scripts; a scheduled job that hits it skips and decides again in the next hour.



## Handling failures

1. Check `daily-*.log` to locate the failed group
2. Rerun a single group: `cne run daily --group <name>` (the operations page's "跑一个调度组" (Run one schedule group) is the same command)
3. Batch-level failure: `cne status` → `cne run retry --run-id <id>` ("Retry this run" in the run details)
4. Recheck: `cne audit --full` + `cne status --datasets --gate`

The daily update and scheduled tasks started from the operations page use the same scheduler lock, in the directory `{data.root}/locks` (`CNE_SCHEDULER_LOCK_DIR` or `CNE_LOCK_DIR` can still move it elsewhere). Running only one schedule group, a makeup run or stale-only does not mark the day as "scheduled daily update already run"; the marker is written only after all schedule groups have run and that day's `run_at` has passed.

`degraded` means coverage is limited; write commands return 0 and an explicit quality gate may return 2, but you still need to look at the steps with gaps: tables that were published successfully remain readable, but this does not mean all data in the group has arrived.
For example, after push2 is paused, fund flow tries to write the THS variants into `fund_flow_ths` / `sector_fund_flow_ths`;
the original EastMoney tables still report as missing, and the fallback tables must not be treated as a like-for-like replacement. Pass `--run-id` to retry the failed batches of that degraded run.
Makeup runs across days should use the original run's retry entry point; real-time snapshots remain limited by their observable time window, and past snapshots cannot be fabricated.

See [Troubleshooting](troubleshooting.md) for details.

## Service level objectives (SLO)

| Metric | Target |
|------|------|
| Daily update success rate | Pipeline exits 0 on ≥99% of trading days over two weeks |
| Alert latency | Notification within minutes of the end of the failing run |
| Freshness | No STALE in `status --datasets --gate` at T+1 (quarterly datasets per `max_staleness_days`) |

## Backup and restore {#backup-and-restore}

The operations page provides "创建数据备份" (Create data backup), the backup directory and list, "校验" (Verify) and "恢复到新目录" (Restore to a new directory). When creating a backup, explicitly choose published datasets; you can choose a disk directory outside the lake, or enable a daily backup in the schedule settings. New backups inside the lake may only be placed in `meta/snapshots` or `backups`, to avoid mixing them into business data directories. The list only reads manifests and does not mean the files have passed full verification. Verify and restore each run as separate tasks, and progress and results can be viewed in the task details.

Web backups reuse portable lake snapshots: they contain the selected data and the corresponding state, contract, revision and lineage information, but not the original TOML configuration, credentials or the full run database; they are not a disaster-recovery image of the whole system. A restore first shows the backup scope and target; after confirmation it fully verifies the file digests, then copies into a new or empty directory outside the current data lake and the backup directory. If the manifest changes after the preview, preview again; a restore does not proceed if verification fails. After a restore, serve still uses the original data lake; accept the restore target with a separate configuration, and switch only after confirming. Backups never delete old copies automatically; for disk-level disaster recovery, choose external storage and save the configuration and credentials separately.

`backup_meta.sh` backs up only the manifest, state, quality, revision receipts, source snapshots and run evidence;
it **does not include** the data version files in `curated/`, `derived/` or `meta/revisions/data/`.
Some history can be refetched, but some snapshot data cannot be recovered from the original source once its window is missed, so a metadata archive must not be treated as
a full lake backup. To be able to restore research data, create and verify a separate portable snapshot, or take a consistent backup of the whole lake.

```bash
scripts/scheduler/backup_meta.sh /abs/path/to/lake /path/to/offsite/cne-meta 30 30
```

`DATA_ROOT` is the lake directory, not the TOML configuration path; when omitted, the script uses `CNE_DATA_ROOT`,
otherwise the repository's `data/cnequity`. By default archives are stored inside the lake; for disk-level disaster recovery, specify a directory outside the lake.
Archives are rotated by whichever of the day and count limits is stricter; see [script parameters](scripts.md#backup_metash).

Before restoring, stop collection, unpack into an isolated directory to inspect the contents, and make sure you have data files that match the receipts;
do not overwrite a lake that is still running or whose data versions do not match with old metadata:

```bash
mkdir -p /tmp/cnequity-meta-review
tar -xzf /path/to/offsite/cne-meta/meta-YYYYMMDD-HHMMSS.tar.gz \
  -C /tmp/cnequity-meta-review
```

After checking, proceed according to your actual recovery plan; restoring metadata alone cannot rebuild missing Parquet files or revision generations.

To freeze the Parquet files a reproducible experiment depends on, use a portable snapshot with checksums, revision receipts, contract fingerprints and
run lineage:

```bash
cne snapshot create research-20260828 \
  --dataset daily_bars --dataset instruments --dataset trading_status
cne snapshot verify research-20260828
cne snapshot restore research-20260828 /new/empty/cnequity-restore
cne config create --config configs/cnequity.restore.toml \
  --data-root /new/empty/cnequity-restore
cne status --datasets --gate --config configs/cnequity.restore.toml
```

The restore command accepts only a new or empty directory, refuses the active lake root, and never overwrites existing files. For acceptance,
use a separate configuration **pointing at the restore target**; then run the research consumers' contract tests, and switch only after confirming.

## 20-trading-day acceptance

`cne verify --runs` takes the most recent window from the authoritative `trading_calendar`, gathers every `daily:*` run per trading day,
takes the newest receipt per dataset and stage, and judges only the 12 core datasets (see [support levels](../datasets/catalog.md#support-levels)),
whichever schedule group fetched them. A missing run, a failed or blocked core dataset, or only an old `warning` without a dataset receipt
all fail; a non-core dataset that degrades or fails does not. Levels come from the current registry, so past days are judged by the same rule. The report is written to
`meta/stability/latest.json` and an immutable history directory. Never backfill it by hand or treat calendar days as trading days.

## Environment variables

`CNE_CONFIG` and `CNE_LOG_DIR` are read by `cne` itself; the rest are read by the shell scripts in [scripts.md](scripts.md).

| Variable | Default | Purpose |
|------|------|------|
| `CNE_CONFIG` | `configs/cnequity.toml` | Default for `--config` in all commands (an explicit `--config` takes precedence); set an absolute path in schedulers — a relative path alone does not let you skip the `cd` |
| `CNE_DATA_ROOT` | the repository's `data/cnequity` | Default lake directory for `backup_meta.sh`; with a custom `data.root`, set it explicitly or pass the first argument |
| `CNE_LOG_DIR` | `{data.root}/logs` | Logs; long-running `cne` commands also leave a copy here |
| `CNE_GROUPS` | Enabled groups selected from the configuration | Overrides the pipeline group list; the public configuration runs 6 regular groups by default, and optional groups join as their switches are enabled |
| `CNE_PYTHON` | the repository's `.venv/bin/python`, else the `python3` on PATH | Interpreter the scheduler scripts use for the gate and the scheduler lock; it must be able to import cnequity |
| `CNE_NOTIFY` | `1` | `0` turns notifications off |
| `CNE_BACKUP_DIR` | backups inside the lake | Backup directory |
| `CNE_BACKUP_RETENTION_DAYS` | 14 | Days to keep |
| `CNE_BACKUP_RETENTION_COUNT` | 30 | Maximum number of copies to keep; `0` disables the count limit |

## Data lake directory (after init)

```
{data.root}/
  staging/
  curated/
  derived/
  meta/manifest.db
  meta/quality/
  meta/source_snapshots/
  meta/on_demand/
  duckdb/cnequity.duckdb
```

## Per-group cron example

In group mode (`--group`), each group automatically runs compact→audit at the end, and data is written to curated:

The regular daily-update groups in the public configuration are listed below; for a custom configuration, `[job.daily.groups]` is authoritative.

| `--group` | What it updates |
|---|---|
| `core` | Universe, trading calendar and status, corporate actions, stock and index daily bars, price adjustment and industry index derivations |
| `capital` | Fund flow, northbound holdings and flows, margin trading, valuation, sector membership |
| `signals` | Dragon-tiger list, block trades |
| `fundamentals` | Financial statements, scheduled disclosure calendar, index and industry membership, share capital, shareholder counts |
| `macro_risk` | Macro indicators, market breadth, share unlock calendar, commodity quotes |
| `research` | ETF profiles, institutional holdings, analyst consensus, popularity, sector quotes and fund flow, sentiment scores |

`cne run daily` runs these groups in order, continues with the following groups after a single group fails, and runs the event stream last.
It returns 1 if anything failed; 2 if nothing failed but something was degraded; 0 if everything succeeded or was skipped normally.
Groups configured as weekly run their historical-data steps according to the trading calendar, while their snapshot steps still run daily.
Data such as northbound follows the upstream's actual disclosure frequency; a group running every day does not mean every table gets a new date.
Sentiment scores depend on news already written to disk; announcements, regulatory events and news are updated by the event stream at the end of the same command (or separately with `cne run events`).
Top-10 shareholders are not in the default daily-update groups; use `cne backfill top_holders` as needed.

```cron
# Core reference + quotes + derivations (Monday to Friday 16:05)
5 16 * * 1-5 cd /path/to/cnequity && cne run daily --group core --config configs/cnequity.toml

# Capital (16:35)
35 16 * * 1-5 cd /path/to/cnequity && cne run daily --group capital --config configs/cnequity.toml

# Signals (17:05)
5 17 * * 1-5 cd /path/to/cnequity && cne run daily --group signals --config configs/cnequity.toml
```

For production, `scripts/scheduler/daily_pipeline.sh` (see above) is recommended instead: it runs all groups serially and does the health check and backup.

## Closing catch-up {#closing-catch-up}

Regular catch-up uses the separately scheduled `scripts/scheduler/stale_pipeline.sh`. In a later window it handles only snapshot datasets that are still behind, which avoids scheduling timeouts caused by waiting inside the main daily-update job; it shares the lock with the main job and skips if they overlap. `daily_pipeline.sh` keeps the `CNE_STALE_RETRY=1` compatibility switch, but it is off by default.

The catch-up follows the same push2 configuration as the main job and does not additionally turn that source off. A banned vantage can set `[sources.eastmoney] push2_paused=true` in its personal configuration, or pass `CNE_PUSH2_PAUSED=1` explicitly; a shared circuit breaker or budget that has already tripped still blocks requests.

| Environment variable | Default | Description |
|---|---|---|
| `CNE_STALE_RETRY` | `0` | Old in-process wait-and-catch-up; set to `1` only when needed for compatibility |
| `CNE_STALE_RETRY_DELAY_SEC` | `1800` | Takes effect only on the old compatibility path |
| `CNE_SOURCE_HEALTH` | `1` | After each daily update, reuses real collection evidence and actively probes only endpoints that are stale or were not reached; set `0` to turn off |
| `CNE_SOURCE_VANTAGE` | `local` | Stable label for the current vantage; do not label overseas samples as `cn` |

**Why it is needed.** Snapshot daily updates fetch only the day of the run; a second window on the same day can make up that day's failed collection.
Once missed, replaying an old `--trade-date` must not be used to fake an observation from that time. Some datasets have a separate historical backfill source,
but its origin, coverage and PIT semantics may differ from the same-day snapshot; check `history_mode` and
`backfill_source` first, and do not judge all snapshot datasets wholesale as either recoverable or permanently unrecoverable.

**Staggering the window is the point.** An immediate retry is very likely to hit the same outage, so the catch-up is launched by a separate job at a later time.

With the default `CNE_SOFT_FAIL_OK=1`, a soft group failing on a given day only raises a warning; consecutive failures escalate as configured. The standalone catch-up has its own exit code and log. Failures of the old compatibility catch-up appear in the main job's `stale-retry:` summary.

### What to do if an outage lasts several hours

The standalone job can be given a later scheduling window:

```cron
5 20 * * 1-5 cd /path/to/cnequity && scripts/scheduler/stale_pipeline.sh
```

When there are no lagging datasets, `--stale-only` creates no run and exits 0 directly, so scheduling it repeatedly is harmless.

Related visibility:

```bash
cne status --datasets --gate # exits 1 if anything is STALE
cne serve             # the dashboard's first screen lists STALE datasets
```

## Init and resources

```bash
cne init --config configs/cnequity.toml
```

Whole-market initialization pages through by instrument and by the source's history range; time and disk usage depend on scope, cache, reachability and failure resumption. Check the configuration and free space first; single-dataset backfills also have `backfill --plan`; keep production jobs to a single instance. On macOS/Windows, `cne config create` uses a conservative worker count by default; check with `cne config validate` before changing it.

## Intraday data (minute_bars / minute_bars_5m) {#intraday-data-minute_bars--minute_bars_5m}

**Off by default and not in the default schedule.** The number of minute-data requests grows with the number of securities, the source's available window and the paging depth; even for a single day, the whole market means scanning many securities. Storage also grows with the retained frequencies and dates, and space is needed for staging and revisions while running. First validate the real scope with a small `--symbols` set, and use `backfill --plan` to check the source switches, window and slices.

TDX minute bars page backward from the current tip, so seeding is sharded by **instrument** (`backfill_chunk_symbols=200`); sharding by date would reread the newer pages repeatedly. The request interval and maximum in-flight count are shared across processes; raising `fetch_workers` can only reduce idle waiting, it cannot raise the configured request rate, much less mean that the source permits that rate. A connection failure or a failed batch of securities is recorded as a failed range for resumption; do not keep hitting the same source through unlimited retries or by switching vantage. For minute bars, run a small-scope acceptance first before expanding to a large scope; actual batch progress and gap reports are more reliable than a static timing table.

### Hooking it up

```toml
[minute_bars]
enabled = true
scope = "index:000300.SH" # or "watchlist" + symbols, or "all"
frequencies = ["5m"]      # 5m is the only frequency with real history
fetch_workers = 4
```

```bash
# One-off seed (sharded, resumable)
cne backfill minute_bars_5m --start 2024-08-01 --end 2026-07-31

# Pull just a few, without changing the configuration (--symbols temporarily overrides scope and enables fetching for this run)
cne backfill minute_bars_5m --start 2026-05-01 --end 2026-07-31 \
  --symbols 600519.SH,000001.SZ

# Daily update: a separate group of its own, do not stuff it into core
cne run daily --group intraday
```

A `--start` beyond the source's horizon is rejected outright with the earliest available start date — see [catalog.md history horizons](../datasets/catalog.md).

Minute-bar acceptance cannot look only at the last date. Within the enabled frequencies and the configured security scope, the audit uses daily bars with positive volume
to check for missing whole sessions, reporting `minute_bars_missing_session`; days without trading are not taken as evidence of a gap. Intraday completeness,
sessions and volume/amount reconciliation of existing minute records are still checked separately. The default lookback is 7 calendar days; recovery over a long window needs its own full-window acceptance.

The complete lists of failed and empty-return symbols for each fetch are stored in the run manifest under
`metrics.stages.<dataset>.source_metrics.tdx_protocol`, in the fields `failed_symbols` and
`empty_symbols`, together with `frequency/start/end`. Failures follow the engine's warning/degraded gates;
an empty return only shows that no records were fetched, and must be checked against daily bars or source suspension evidence — it cannot be taken directly as a normal suspension.
A batch that comes back entirely empty is still judged a failure, and clears the TDX host cached in the process so that later retries recheck reachability; it does not retry
endlessly at the point of failure. Besides the market median, volume/amount reconciliation also checks individual security-days with the existing 0.95..1.05 tolerance, reporting
`minute_bars_daily_outliers`. A discrepancy does not by itself prove the minute source is wrong; keep the raw daily-bar and minute evidence before deciding.

## Backfill acceptance {#backfill-acceptance}

After the compact + derive of init or the first full backfill succeeds and `cne status` is success, run the following checks in the same maintenance window before hooking up cron / connecting downstream consumers.

### Prerequisites

```bash
cne status --config configs/cnequity.toml # success, failed batch = 0
cne audit  --config configs/cnequity.toml # no mock_source / pk_duplicate error
ls data/cnequity/curated/daily_bars/      # should contain trade_date=YYYY-MM-DD partitions
```

If `[adj_factors].adjust_types` in the configuration has only `qfq` and you want hfq, first add `"hfq"` and rerun
`cne derive adj_factors`.

### Idempotence

```bash
.venv/bin/python scripts/accept_backfill.py snapshot \
  --config configs/cnequity.toml --out /tmp/curated-counts.json

cne run daily --config configs/cnequity.toml   # one skeleton pass is enough; the acceptance is about idempotence

.venv/bin/python scripts/accept_backfill.py check \
  --config configs/cnequity.toml --compare /tmp/curated-counts.json
```

Row counts of core datasets (`daily_bars`, `instruments`, `adj_factors`, etc.) should match those before the rerun.

### Spot-checking conventions

```bash
.venv/bin/python scripts/accept_backfill.py check \
  --config configs/cnequity.toml \
  --symbol 600519.SH --start 2024-01-01 --end 2024-12-31
```

Compare the unadjusted close and the hfq adj_close against market software (sample one day on each side of an ex-rights date).

### Coverage by year

```bash
.venv/bin/python scripts/accept_backfill.py check --config configs/cnequity.toml
# look at === daily_bars by year ===
```

Normal shape: symbols grow gradually from 2016 to recent years, each year has `rows ≈ symbols × ~240` trading days, and no single year is cut in half.
If a year is clearly below 70% of the median, run `cne backfill daily_bars` or a targeted retry for that year's window.

### Consumer-layer smoke test

```python
from cnequity.query import load

tradable = load(
    "daily_bars",
    start="2024-06-01",
    end="2024-06-30",
    adjust="hfq",
    profile="cn_a_sh_sz_research_v1",
    strict_adj=True,
)
assert "adj_close" in tradable.columns
```

### Acceptance checklist

| # | Item | Pass criterion |
|---|-----|----------|
| 1 | Idempotence | Core dataset row counts unchanged after rerunning the same window |
| 2 | Conventions | Benchmark stocks' close/adj_close match market software (manual) |
| 3 | Coverage | No abnormal cliffs in per-year row counts; partitions continuous from 2016 |
| 4 | Consumption | The explicit research profile's evidence gate passes; `adj_close` is computable and the factors are exact |
| 5 | Audit | Latest run audit has no error; `source=mock` row count = 0 |

## Fallback source policy

1. Primary source fails → batch retries with backoff (up to 3 times)
2. Still failing → mark the batch failed; optionally write the fallback source into `meta/source_snapshots`
3. `cne audit` compares the primary source with the snapshot, and a human decides whether to switch sources
4. Never silently overwrite curated canonical rows with the fallback source

## Per-security daily bar coverage

```bash
cne verify --bars --config configs/cnequity.toml --start 2026-09-07 --end 2026-09-15
```

This command checks security × trading day against the configured ingest universe, including securities with no quotes at all over the whole window.
No quotes are required before listing or after delisting; dates with explicit non-trading status can be exempted. An empty response from a vendor is not proof of suspension.
In the JSON, `unresolved_keys` is the number of keys missing quotes / non-trading evidence, and `unknown_listing_symbols` are codes whose expected
range cannot be determined; it exits 1 if either is unresolved. It does not refetch or modify quotes, and cannot be replaced by an ordinary watermark check.
A full-history check is chunked into blocks of 64 trading days.

## Related documents

- [Script reference](scripts.md)
- [Troubleshooting](troubleshooting.md)
- [Schema contract](../datasets/schema.md)
- [Per-source limitations](../datasets/sources.md)

## Derivatives backfill and repair

First run `backfill futures_bars/option_bars --plan --exchange ... --start ... --end ...`, check the scope, then drop `--plan`. Use `--refresh` for missing rows; the backfill updates the contract table and derivations automatically. On refusal, keep the persistent backlog and put the source in cooldown; do not raise concurrency. After deploying a new version, rebuild old inferred dates and continuous contracts/Greeks; for detailed steps and the evidence directory, see the [derivatives guide](../recipes/derivatives.md).
