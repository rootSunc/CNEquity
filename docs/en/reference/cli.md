# CLI reference

Options and defaults are generated automatically by Click in the [options reference](cli-options.md); the network/write boundary of every command is in the [full side-effect inventory](cli-surface.md); source selection and request cost are in [Fetching and source protection](../operations/fetch-policy.md).

Conventions shared by every command (`--config` resolution, case, logs, display language, failure records) are collected under [Conventions](#conventions).

## Command index

Top-level entries fall into five sections, ordered the way a lake gets used, and matching the sections of `cne --help`.
The sections are defined by `SECTIONS` in `cnequity/cli/_root.py`; leaving any command out makes a test fail.

### Getting started

| Command | Purpose |
|------|------|
| [`cne config`](#cne-config-create) | Create, upgrade, validate or diff the configuration (`create` / `upgrade` / `validate` / `diff`) |
| [`cne doctor`](#cne-doctor) | Check the environment, optional dependencies and silent configuration faults; runs without configuration or network |
| [`cne init`](#cne-init) | Create the lake and run the init phases. `--profile demo\|sample\|quick\|full` |

### Running the pipeline

| Command | Purpose |
|------|------|
| [`cne run daily`](#cne-run-daily) | Run one day's update: every daily schedule group, then the event streams |
| [`cne run maintain`](#cne-run-maintain) | Post-update maintenance: audit, gates, source probes, run evidence, metadata backup |
| [`cne schedule`](#cne-schedule) | Install, inspect or remove the current user's timer |
| [`cne run events`](#cne-run-events) | 7×24 event streams (announcements, news), on the natural calendar rather than the trading calendar |
| [`cne run retry`](#cne-run-retry) | Retry one run, or the latest failed run of each daily group |
| [`cne run compact`](#cne-run-compact) | Merge finished, unpublished staging into curated |
| [`cne run clean`](#cne-run-clean) | Preview expired staging, snapshots, logs and old versions |
| [`cne storage`](#cne-storage) | Version-retention register, cleanup plans and in-place pending-deletion marks |
| [`cne backfill`](#cne-backfill-dataset) | Backfill one dataset |
| [`cne derive`](#cne-derive-name) | Compute derived datasets |
| [`cne import shfe-archive`](#cne-import-shfe-archive) | Import an official SHFE annual package offline |
| [`cne migrate`](#cne-migrate) | Preview or run the one-off migrations this lake still owes, and record the results |
| [`cne repair`](#cne-repair) | Offline repair of stored data layout or field semantics; previews by default, `--apply` publishes a new version |

`compact` is already a step built into every schedule group, so `retry` / `compact` / `clean`
are **manual exits after a failure**, not part of a normal day.

### Checking the lake

| Command | Purpose |
|------|------|
| [`cne check`](#cne-check) | **Is this lake usable**: freshness and coverage, data quality and size accepted in one command |
| [`cne status`](#cne-status) | Latest run status; `--datasets` shows per-dataset freshness |
| [`cne verify`](#cne-verify) | **Did what should land actually land**. By dataset × trading day by default, `--bars` by security × session, [`--runs`](#cne-verify---runs) by run evidence over consecutive trading days |
| [`cne audit`](#cne-audit) | **Is what landed correct**; `--full` gives a whole-lake health snapshot |
| [`cne decision-data payment-gaps`](#cne-decision-data-payment-gaps) | Freeze cash-dividend payment-date gaps by revision, classified by tradable period and ex-date rules |
| [`cne decision-data stock-terms`](#cne-decision-data-stock-terms) | Freeze, by revision, events pending review where a bonus share issue and a capital-reserve conversion fall on the same day |
| [`cne decision-data cash-rights`](#cne-decision-data-cash-rights) | Export holder-class cash entitlements verified against the issuer's original announcements, and reconcile them item by item with the current revision |

`audit` and `verify` do not ask the same question: one asks about correctness, the other about completeness.

The three `decision-data` commands only read the data lake and freeze their inputs by committed revision; results can be written to an output directory, with files named by the SHA-256 of their content, so repeated runs never overwrite an earlier manifest.
When [`[research] holdout_start`](../getting-started/configuration.md#research) is configured, a window that touches that date is rejected before the lake is read.

### Consuming the lake

| Command | Purpose |
|------|------|
| [`cne query`](#cne-query) | Run DuckDB SQL, or pull a dataset on demand |
| [`cne serve`](#cne-serve) | Lake dashboard: browsing, fetching from the operations page, web-confirmed cleanup (default `127.0.0.1:8787`) |
| [`cne mcp`](#cne-mcp) | Serve the lake to AI agents over MCP (stdio or HTTP) |

### Governance and inspection

| Command | Purpose | Subcommands |
|------|------|--------|
| [`cne snapshot`](#cne-snapshot) | Create, verify and safely restore portable snapshots | `create` `verify` `restore` `export` `import` · `delta {create,verify,apply}` |
| [`cne contract`](#cne-contract) | Inspect and validate registered data contracts | `show` `diff` `validate` |
| [`cne profile`](#cne-profile) | Inspect versioned research universe profiles | `list` `show` |
| [`cne stats`](#cne-stats) | Metrics tables under `meta/stats` (row counts, bytes, source distribution) | `rebuild` `show` |
| [`cne sources`](#cne-sources) | Probe the data sources the lake depends on and inspect the evidence | `probe` (network) `limits` `slo` `resilience` `policy` `substitutes` |
| [`cne delisted`](#cne-delisted) | Read the delisting directory | `status` |
| [`cne ths-official`](#cne-ths-official) | Cross-check against / backfill from the official THS API (key required) | `capture` `backfill` `repair-bars` `resource-sectors` |

## Renamed commands

When you type an old name, the CLI tells you the new spelling directly, instead of Click's default "No such command":

```
$ cne retry
Error: `cne retry` 已改名，请改用 `cne run retry`。
```

(The message reads: "`cne retry` has been renamed; use `cne run retry` instead.")

| Old | New | Why |
|----|----|--------|
| `cne demo` / `cne demo --sample` | `cne init --profile demo` / `--profile sample` | How big a lake to build is **one axis**: demo / sample / quick / full. Making a first-time user choose between two commands first is an unnecessary fork |
| `cne config init` | `cne config create` | It differs by one word from `cne init`, which builds the whole lake, so a typo costs far more |
| `cne retry` / `cne compact` / `cne clean` | `cne run retry` / `run compact` / `run clean` | They act only on runs, so under `run` is where people will look for them |
| `cne verify-bars` | `cne verify --bars` | It asks the same question as `cne verify`, only at a different granularity; two top-level commands differing by one hyphen |
| `cne stability` | `cne verify --runs` | Same as above, a third granularity: trading day × run |
| `cne ths-official snapshot` | `cne ths-official capture` | It used to share a name, but not a meaning, with the top-level `cne snapshot` (lake snapshots) |
| `cne delisted backfill --since DATE` | `cne backfill daily_bars --profile delisted --start DATE` | Delisted price data uses the same dataset backfill entry point; the old spelling keeps a migration hint |
| `cne servers test` | `cne sources probe --only tdx_protocol` | Long deprecated, announced for removal in 0.9.0 but kept until 0.10 |

The top-level mapping is defined by `MOVED` in `cnequity/cli/_root.py`; old names inside a group are hinted by `moved_hints`. The hint does not run the old command.

## Conventions

Command: `cne` (`cnequity.cli.main:cli`)

Leaf commands that support configuration accept `--config PATH`; resolution order is explicit `--config` → `CNE_CONFIG` → `configs/cnequity.toml` in the current directory → the per-user `~/.config/cnequity/config.toml` (honours `XDG_CONFIG_HOME`; `%APPDATA%\cnequity\config.toml` on Windows); `cne config path` shows the config chosen and why. For example, `cne status --config /abs/path/to/cnequity.toml`.

**Command names are case-insensitive**: `cne STATUS`, `cne run DAILY` and `cne config CREATE` are equivalent to their lowercase forms (Click's `token_normalize_func`). `-h` is short for `--help`. **There is no prefix matching**: `cne stat` is rejected with a hint pointing to `stats` / `status`, rather than guessing one and running it.

**Every command has a process log.** The pipeline's own INFO records are attached to stderr once, at the root of the command tree, so the JSON contract on stdout is unaffected. Long-running ingestion and maintenance commands also tee their logs into `{data.root}/logs/cne-<command>-<timestamp>.log` (`cne run clean --log-retention-days` can preview expired logs). `cne mcp` and `cne serve` manage their own logging: the former's stdout is the JSON-RPC wire, so stderr must stay at WARNING; the latter hands logging to uvicorn. An interactive terminal gets a short view: time of day only, warnings and errors tagged, no logger name, with steps, heartbeats and progress in the display language. Redirected output (a pipe, a scheduler capturing stderr, CI) keeps the full format, and the log file is always in the full English format, so it stays searchable and can be pasted into an issue.

**Display language.** Help, prompts, errors and closing summaries are available in English and Chinese. The language comes from the `CNE_LANG` environment variable (`en` or `zh_CN`; spellings like `zh-CN` also work), then the system locale (`LC_ALL` / `LC_MESSAGES` / `LANG`; on Windows, the UI language), and is English when neither says; a Traditional Chinese locale gets English. To read Chinese on an English system: `export CNE_LANG=zh_CN` (PowerShell: `$env:CNE_LANG = "zh_CN"`). What never changes with the language: JSON output such as `--json` (its explanatory text is always English), markers like `HEALTHY` / `STALE`, exit codes, data files and log files. The `cne serve` console shows the language chosen on the page, following the browser until one is chosen; scheduled tasks keep the language they were installed in.

**Every failure leaves a log record.** Click only prints one `Error:` line to stderr and stops, and scheduled jobs read the log file, not the terminal; if a failure never becomes a log record, all that remains is a file whose last line stops halfway. The record is written at the single entry point of the command tree, which all registered commands share, and there is exactly one per failure: usage errors (a mistyped option, an unknown name) are logged at `WARNING`, everything else at `ERROR`, and unexpected exceptions also carry a traceback. Exit codes and Click's rendering are unchanged.

**Six commands do not accept `--config`**: `cne contract show|validate|diff`, `cne profile list|show`, `cne sources policy`. They read the registry shipped with the package rather than a lake, so they give the same answer whichever lake they point at. `cne doctor` is the opposite: it accepts `--config`, but also runs without a configuration (which is exactly the situation it exists for).

## cne init --profile demo | sample

A small-scale trial, as two profiles of `cne init` (formerly `cne demo`): `demo` pulls recent real-source daily bars for a few liquid stocks, and `sample` generates, fully offline, a small synthetic lake clearly marked `source=mock`. Neither is the whole market; that is `--profile quick|full`, see [cne init](#cne-init).

The options below apply only to these two profiles; `--config` / `--resume` / `--layout-only` / `--since` apply only to `quick|full`. Using one on the wrong side is rejected by name.

| Option | Description |
|------|------|
| `--symbols` | Comma-separated symbols (default Kweichow Moutai / Ping An Bank / Wuliangye / CATL / Ping An Insurance) |
| `--days` | About how many trading days of `daily_bars` (default 30) |
| `--intraday` | Also fetch 1m bars for the same symbols (at most about 5 trading days) and print one full session |
| `--research` | Also derive hfq factors from Sina and print a raw / hfq return comparison; extends the window to about 3 years |
| `--data-root` | Separate lake root directory (default `data/cnequity-demo`; sample: `data/cnequity-sample`) |
| `--profile sample` | No network access; generates a synthetic sample for verifying the installation, queries and DuckDB views; cannot be combined with `--research` / `--intraday` |
| `--trade-date` | As-of date YYYY-MM-DD (default today / the latest trading day) |
| `--config-out` | Write a small configuration for later `cne query` use (default `configs/cnequity.demo.toml`; sample: `configs/cnequity.sample.toml`) |
| `--force` | Allow overwriting an existing `--config-out` with different content; by default the original file is kept and an error is raised; identical content can simply be rerun |

Flow: create directories → probe TDX → pull instruments and trim to the demo universe → trading calendar → `daily_bars` + compact → print sample tables; with `--research` it also derives Sina hfq and checks exact coverage, and with `--intraday` it also runs `minute_bars`. The terminal shows staged progress and INFO logs. TDX must be reachable; `allow_mock` is not turned on.

To verify only the research semantics, without initializing the whole market:

```bash
cne init --profile demo --research --symbols 600519.SH
```

`--research` needs additional access to Sina; on a restricted network, run the basic demo without that option first.

If TDX is entirely unreachable, you can first verify the local read/write and query path:

```bash
cne init --profile sample
```

Synthetic rows are prominently marked `source=mock`, and the quality audit does not treat them as real data; do not reuse this demo's `data_root` for research or production.

## cne init

Initialize the data lake and run the init phases. quick/full automatically recover known delisted daily bars within the configured market and history window, fetch the current trading status, and derive historical suspensions once the daily bars are published. The whole-market historical ST scan uses the separate `cne backfill trading_status` and is not a completion condition for init.

When no configuration is found, `cne init` first generates the per-user configuration from the example shipped with the package (same as `cne config create`); a missing file named by an explicit `--config` or by `CNE_CONFIG` is not generated. For an interrupted, partially covered, or old-version unfinished init, rerunning the same `cne init` resumes automatically, and data already fetched is kept.

| Option | Description |
|------|------|
| `--config` | Configuration file path; generated automatically when the default path is missing |
| `--layout-only` | Only create directories, the manifest and DuckDB views |
| `--trade-date YYYY-MM-DD` | Trading day init runs up to (default today) |
| `--resume` | Resume the latest unfinished init |
| `--run-id` | Resume the given init run (implies resume) |
| `--keep-going` | Continue with later phases after any phase fails (the corporate-action, index and trading-status phases, and the derives and audit after the bars publish, already continue by default) |
| `--json` | Print the full machine-readable result at the end instead of the summary |
| `--no-preflight` | Skip the network preflight before the run; `CNE_SKIP_PREFLIGHT=1` does the same |
| `--profile demo\|sample\|quick\|full` | How big a lake to build. `quick` (default) = all market symbols, last 3 years; `full` = all market, each step's own start (`daily_bars` from 2016-01-01), with more requests and more data on disk; `demo` / `sample` are small lakes of a few stocks, see [the previous section](#cne-init---profile-demo--sample) |
| `--since YYYY-MM-DD` | Explicit history start; overrides `--profile` |
| `--quiet` | Keep only warnings and above; no per-batch progress |

**Progress is printed by default.** A whole-market backfill can take a while; each batch reports the number completed, rows, elapsed time
and estimated time remaining. The values below only illustrate the output format:

```
14:22:07 INFO ...worker_pool: daily_bars 1/2 batches · 240 rows · 1m24s elapsed · ~1m24s left
```

**`quick` is shallower, not narrower.** Not a single market symbol is left out; each just has a few fewer years. Trimming by symbol would build into the lake the very survivorship bias it exists to remove, and an absent symbol looks exactly like "this stock never traded"; fewer years of history, by contrast, are recorded honestly by `coverage_start`.

The window is written into run metadata and reused automatically by `--resume`; otherwise resuming from a new process days later would default back to full depth and fetch the years you deliberately skipped.

Deepening later does not require rerunning init:

```bash
cne backfill daily_bars --start 2016-01-01 --end COVERAGE_START
```

In the table above, `--config` / `--layout-only` / `--resume` / `--run-id` / `--keep-going` / `--since` apply only to `quick|full`; `--symbols` / `--days` / `--data-root` / `--config-out` / `--intraday` / `--research` apply only to `demo|sample`. Passing one to the wrong side is rejected by name, not ignored.

**A failed corporate-action, index-bar or trading-status phase does not stop the build.** When one of these phases fails, init still fetches daily bars, publishes them and derives adjustment factors, then lists the steps left undone at the end; rerunning the same `cne init` fills only those. A failure in instruments, the trading calendar or daily bars still stops where it is; what was downloaded is kept, and rerunning resumes once the cause is fixed.

**It ends with a summary.** The terminal shows whether the lake is usable, which steps are still owed, where the log is, and what to run next (`cne check`, `cne serve`, reading from Python, connecting MCP). The full result is saved to `{data.root}/logs/cne-init-<run_id>.json`; add `--json` to get it on stdout.

Exit: success, source-limited outcomes (`degraded` / `warning`, including 0 rows this time), and a build whose daily bars are published but which still owes only the non-blocking steps above return 0; other execution errors return 1.

## cne config create

Write a short user configuration that extends the built-in defaults (`extends = "builtin"` plus `[data].root`); see [Extending the built-in defaults](../getting-started/configuration.md#extends-builtin).

| Option | Description |
|------|------|
| `--config` | Output path (default: `configs/cnequity.toml` when the current directory has one, otherwise the per-user `~/.config/cnequity/config.toml`) |
| `--data-root` | Written into `[data].root` |
| `--force` | Overwrite an existing file |

Platform-dependent defaults (such as `orchestrator.workers = 1` on macOS) come from the built-in defaults and are not written into the file. Source of the defaults: `cnequity.config.templates` (kept in sync with the repository's `configs/cnequity.example.toml`).

## cne config show

Print the full effective configuration (TOML) with credentials masked; for a config that extends the defaults, it marks which values come from the file and which groups `steps_add` / `steps_remove` edited. `--defaults` prints every setting with its default and explanation.

## cne config validate

Validate the TOML and step references. Exits 1 on errors.

## cne config diff

Compare the current configuration with the example template in the package, listing what **the template has and you do not**.

The user configuration is written once by `cne config create`, never updated afterwards, and gitignored. Steps that later versions add to schedule
groups do not appear in it on their own: the feature is installed but never scheduled, and `cne config validate`
still answers `Configuration OK`. This command supplies that missing signal.

The report has three categories, ordered by consequence:

| Category | Consequence | Effect on exit code |
|------|------|-----------|
| Unscheduled step | **Loses data**: the step exists but is not in any group / wave, so it never runs | Exit 1 if any |
| Missing configuration section | Built-in defaults are used | None |
| Missing configuration key | Built-in defaults are used | None |

Steps missing from schedule groups are reported separately: a step that the example configuration's group of the same name has, and none of your groups has, is not run by `cne run daily` without arguments, even if some wave still lists it.

Machine-specific values such as `[data].root` and `orchestrator.workers` do not count as drift; extra custom content in your configuration is not reported either. A config that extends the defaults cannot miss keys; only a whole-group `steps = [...]` that shadows a new default step is reported, while `steps_remove` and `enabled = false` are deliberate opt-outs, not drift.

## cne config upgrade

Run this after upgrading to add the schedule steps and schedule groups new in the current version to your configuration:

```bash
pip install --upgrade cnequity
cne config upgrade
```

- Steps that the example configuration's group of the same name has, and none of your groups has, are appended to your group of the same name;
- Daily / event-stream schedule groups you do not have are appended, comments and all, to the end of the file;
- Steps that are only in an example wave are appended to your wave of the same name.

Before making changes, the original file is backed up as `cnequity.toml.bak-<timestamp>`, and the configuration is validated after writing. Other settings (times, concurrency, credentials, switches) are untouched; missing configuration keys already use built-in defaults and are not written to the file, so that current defaults are not pinned. `--dry-run` only lists the changes. Groups written as inline tables or dotted keys cannot be edited automatically; the command lists the steps to add by hand and returns 1.

A config that extends the defaults needs no upgrade step. `--minimize` rewrites a full config into the extending form (rules in [Extending the built-in defaults](../getting-started/configuration.md#extends-builtin)): it first lists the settings kept, the schedule groups' `steps_add` / `steps_remove`, the groups set to `enabled = false`, and every setting whose behaviour changes on loading; with `--dry-run` it only previews.

## cne sources resilience

Show source concentration, blast radius and the independent-fallback gate by failure domain.

| Option | Description |
|------|------|
| `--with-availability` | Join the probe history this lake has accumulated onto the failure domains (reads the lake, needs `--config`) |
| `--window-days` | Availability statistics window (default 30 days) |
| `--enforce` | Exit 1 when a key dataset lacks an independent fallback source |
| `--out PATH` | Write to a file instead of printing |

Concentration alone cannot decide the choice of primary and fallback sources. `--with-availability` puts the probe samples saved in this lake
and the failure domains in the same report; each domain shows the lowest availability among the probes it depends on. Probes with no observations
are not counted as success or failure, and the corresponding domain stays unlabeled. Concrete numbers depend on your own network egress and observation window.

## cne contract

View and maintain the machine-readable JSON contracts of all registered datasets.

| Subcommand | Description |
|--------|------|
| `show [DATASET]` | Output one dataset's contract or the full registry contract; `--out PATH` writes to a file instead of printing |
| `validate [PATH]` | Validate a file; with PATH omitted, validates the current registry. For a file, add `--against-registry` for an exact sync check |
| `diff OLD [NEW]` | Compare two contracts; with NEW omitted, compares against the current registry. Exits 1 on breaking changes by default; add `--allow-breaking` for inspection reports |

diff treats dropped columns, changed types, changed primary keys, and changes in unit/PIT/history semantics as breaking; added
columns and added datasets are compatible.

> `cne contract export` has been merged into `cne contract show --out`: the two were always the same document,
> differing only in whether a file is written.

## cne profile

View versioned research universe profiles (the `cnequity.domain.universe_profiles` registry).

| Subcommand | Description |
|--------|------|
| `list` | Output registry records. `--include-compatibility` by default, including legacy compatibility profiles; `--official-only` excludes them |
| `show NAME` | Output one profile and its `scope_hash`; `--symbol` is repeatable, binding concrete symbols and adding `concrete_scope_hash` |

A profile binds exchange/board, CDR/ETF, ST/suspension, delisting and PIT evidence rules. Research reads use
`load(..., profile="cn_a_sh_sz_research_v1")` and record `name` / `version` / `scope_hash`
together in the output. See [Universe profiles](universe-profiles.md).

## cne doctor

Environment and configuration checkup: whether `data.root` is an absolute path / writable, and whether declared dependencies can be imported. No network access; runs without a configuration (fresh install). Exits 1 on material risk.

| Option | Description |
|------|------|
| `--json` | Machine-readable output |

`cne doctor --fix` has been removed (it only served the conflict fix for the since-removed mini-racer).

## cne run daily

| Option | Description |
|------|------|
| `--group` | Run only one group: `core` \| `capital` \| `signals` \| `fundamentals` \| `macro_risk` \| `research` \| `intraday` |
| `--no-events` | Skip the event streams when run without arguments |
| `--core-only` | Run only the `[[job.daily.waves]]` core skeleton (the old behavior without arguments) |
| `--all-groups` | Run all schedule groups only, without the event streams; kept for old scheduled jobs that schedule `cne run events` separately |
| `--backfill` | Force backfill semantics (use with care) |
| `--repair-gaps` | Before the daily/stale run, first repair historical gaps that are verified and have an honest source |
| `--stale-only` | Refetch only datasets still behind the last trading day (mutually exclusive with `--group`) |
| `--quiet` | Keep only warnings and above; no per-step progress |

### --stale-only: a second chance for the day {#--stale-only当天的第二次机会}

Snapshot-type daily updates fetch only the day of the run. Once that day's window is missed, a new observation cannot be passed off as a historical snapshot by replaying an old `--trade-date`; whether it can be recovered from an independent historical source depends on the dataset's `history_mode` and
`backfill_source`. `--stale-only` gives data not yet complete for the day a second collection window.

Schedule it a few hours after the main pipeline:

```cron
# Main pipeline
5 16 * * 1-5 cne run daily && cne run maintain

# Closing catch-up: run only what is still behind; idles if nothing is
5 20 * * 1-5 cd /path/to/cnequity && cne run daily --stale-only
```

The freshness criterion is exactly the same as `cne status --datasets` (including each dataset's `max_staleness_days` tolerance), so the two never disagree. When no dataset is behind, no run is created and it exits 0 directly, so it is safe to put on a timer.

Derived datasets are not included: they are recomputed from curated, so what should run is `cne derive`, not a refetch.

**Without arguments, it is a complete day.** It runs every group of `[job.daily.groups]` serially in configuration order, runs one `audit` over the whole lake (it reads the whole lake, so it comes after all groups have landed; skipped when no group actually ran), then runs the `[job.events.groups]` event streams: one failed group does not interrupt later groups, the exit code is the worst one, and groups whose datasets are all disabled are skipped automatically. On non-trading days the schedule groups are skipped automatically while the event streams run as usual, so one scheduled job running once a day is enough; the timer `cne schedule install` installs runs `cne run maintain` (audit, gates and metadata backup) after it. When the event-stream lock is already held by a separate `cne run events`, this part is recorded as `skipped_locked`, which is not a failure; when `--backfill` reruns a trading day, the event streams are not run. When the configuration has fewer schedule steps than the current version, you are prompted to run `cne config upgrade`.

`--core-only` runs the `[[job.daily.waves]]` DAG, which is only the core skeleton; when the configuration has no schedule groups, running without arguments also takes this path, and with no waves it fails outright instead of pretending to succeed. The `intraday` group requires `[minute_bars].enabled` first; when its datasets are disabled, the whole group is skipped automatically.

Success, a valid partial result, or `skipped_non_trading_day` exits 0; execution failure returns 1.

## cne run maintain

Maintenance after the daily update; the timer `cne schedule install` installs runs it once after each trading day's update: the run audit (the whole-lake audit on `[job.maintenance] full_audit_weekday`), the freshness gate, source probes and SLO, run evidence over the last 20 trading days, a metadata backup (manifest database, receipts, quality findings, never curated Parquet, rotated by the `[job.maintenance]` retention), and the preview of `cne run clean`.

| Option | Description |
|------|------|
| `--groups` | Schedule groups the freshness gate fails on (space- or comma-separated); default: the groups the scheduled update runs |
| `--notify` | On problems, also show a desktop notification (macOS) |

It exits 1 when the audit, the freshness gate or the backup fails; source probe and run evidence problems are only reported. See the [runbook](../operations/runbook.md#maintain) for each step.

## cne schedule

Manages the current user's timer (launchd on macOS, crontab on Linux, Task Scheduler on Windows), the same task the `cne serve` operations page manages. It checks once a minute; on each trading day, after `[job.daily] run_at` (Beijing time), it runs `cne run daily`, then `cne run maintain`; after `[job.stale] run_at` it retries the missed snapshots.

| Subcommand | Description |
|------|------|
| `install` | Registers or updates the timer after confirmation. `--pack` updates only the selected research packs' schedule groups; `--no-stale` leaves out the closing catch-up; `--events` runs event streams on the config's `[[job.events.schedules]]`; without them, `--events-group` / `--events-every` run one event group on an interval; options not given keep their current settings; `--yes` skips the confirmation |
| `status` | Installed? active? what it runs and when it last checked; `--json` for machine-readable output; flags legacy `scripts/scheduler` launchd jobs |
| `remove` | Pauses and unregisters the system task after confirmation; running jobs finish |

## cne run events

7x24 event streams: announcements, regulatory events, news. **It does not consult the trading calendar** (these sources publish on weekends and holidays too),
and it takes its own `events_ingestion` lock, so it does not compete with the evening batch for `daily_ingestion`.

| Option | Description |
|------|------|
| `--group` | One group in `[job.events.groups]`; by default runs all groups in configuration order |
| `--trade-date` | Calendar day `YYYY-MM-DD` (default today); weekends/holidays are equally valid |
| `--quiet` | Keep only warnings and above |

```cron
# Full event streams once a day (weekends included)
0 14 * * *  cne run events --config /abs/path/to/cnequity.toml

# Intraday freshness for news only: a single live page, very cheap
*/30 9-22 * * *  cne run events --group news_wire --config /abs/path/to/cnequity.toml
```

The `disclosures` group rereads a 30-day reconciliation tail window every time, so running it at high frequency pays that cost over and over;
`news_wire` has no tail window and suits high frequency. Groups run in configuration order, so `regulatory` can read the announcements
`disclosures` has just published.

## cne backfill DATASET

Every backfillable dataset supports `--plan`: it shows the range, registered sources and effective switches, cooldowns, repair mode, outstanding debt and slices, without going online or writing the lake. An explicit daily-bar security range gives the minimum cold-cache request count; derivatives plans also give an estimate, and other unknown request counts stay `null`. An explicit `--symbols` cannot be empty, and duplicate codes are deduplicated.


Single-dataset backfill. A snapshot dataset with no `backfill_source` finishes normally and reports the capability limit, keeps the existing snapshots, and gives a plan for daily collection going forward. When a historical request exceeds the source's known boundary, the range that is still available is fetched, and the requested range and the actual range are recorded separately.

After fetching, valid results in the current run that passed sealing checks are compacted automatically; a partial source failure does not block publishing independent facts.

**Downstream derivations follow their inputs, without waiting for the next day's daily update.** Whatever inputs the backfill changes, the derived data that depends on them is recomputed and checked on the spot:

| Backfill | Automatic follow-up |
|---|---|
| `corporate_actions` | Compare the published corporate actions before and after the backfill, take only securities whose adjustment-relevant terms (cash dividends, bonus shares and conversions, splits and reverse splits, rights issues, reference prices) were added, removed or changed, recompute their adjustment factors, then check that each security's factor coverage reaches its latest traded daily bar. Changing only the payment date does not trigger a recompute |
| `daily_bars` (including `--profile delisted`) | Compare the daily bars within this window before and after the backfill (by security × month), realign adjustment factors for changed securities using the cached factors (no refetch) and check coverage; then rebuild suspensions from daily-bar gaps from 90 days before the first changed month to the last changed month. Both are skipped when the daily bars did not change |

The CLI prints this summary in Chinese; it reads: corporate actions backfill complete, 1,234 rows written, 87 symbols affected; deriving adjustment factors: 87 processed, 84 updated, 3 skipped (2 without daily bars, 1 CDR); corporate actions and adjustment factors in sync.

```
公司行为回填完成
  写入：            1,234 条
  受影响标的：      87

派生复权因子…
  处理：            87
  更新：            84
  跳过：            3（无日线 2, CDR 1）

✓ 公司行为与复权因子已同步
```

The result JSON also has `adj_factors_sync` / `trading_status_derive` fields. Securities that did not keep up (fetch failed, or not covering the latest daily bar) are listed, and the command finishes as `degraded` with a rerun command; errors in downstream derivation are likewise recorded as `degraded`, and the published backfill results are reported as usual.

| Option | Description |
|------|------|
| `--start` / `--end` | Request window; the part beyond a source's historical depth is reported as a coverage gap, and the available range is still fetched |
| `--symbols` | A temporary symbol scope for intraday, `daily_bars`, `trading_status`, `corporate_actions` and `share_structure` (`share_structure` fetches every share change of a security at once, for the share lag the audit reports); other datasets keep the configured scope |
| `--outstanding` | Repair exactly the keys tolerated gaps left owed: scope and window come from the ledger, not from `--symbols`/`--start`/`--end`. Filled keys are cleared at once; keys still missing stay owed |
| `--refetch` | Ignore completion receipts and checkpoints and fetch the whole range again (`sector_bars`, `futures_bars`, `option_bars`); circuit breakers still apply. Without it a backfill resumes |
| `--source` | Source to use this time, without editing the config; currently for `margin_trading` (`exchange` / `eastmoney`) |
| `--workers` | `margin_trading` only: concurrency of the date walk; each request still goes through the shared rate limiter |
| `--exchange` | Derivatives: restrict to a publisher; repeatable |
| `--profile delisted` | `daily_bars` only: the confirmed delisting catalog as the universe; `--start` maps to the old command's `--since`; `--plan` is offline |
| `--plan` | Read-only plan; no network, no writes |

**Dataset-specific repairs and imports have moved out of `backfill`.** They run on the same backfill machinery (window, scope, downstream derive follow-up, plan output) but follow the repair convention: a preview by default, `--apply` to run. The old flags have been removed; use the right-hand column:

| Old form | New command |
|---|---|
| `backfill corporate_actions --payment-date-repair` / `--issuer-notice-repair` | [`cne repair payment-dates`](#cne-repair) (`--issuer-notices-only`) |
| `backfill corporate_actions --baostock-repair` / `--ths-repair` | `cne repair delisted-actions --source baostock\|ths` |
| `backfill corporate_actions --eastmoney-date-repair --ex-dates ...` | `cne repair ex-dates --ex-dates ...` |
| `backfill daily_bars --tdx-amount-repair` | `cne repair bar-amounts` |
| `backfill daily_bars --turnover-repair [--turnover-peer-review]` | `cne repair bar-turnover [--peer-review]` |
| `backfill daily_bars --bse-tip-repair --start D --end D` | `cne repair bse-tip-turnover --date D` |
| `backfill valuation_metrics --fill-em-outage` | `cne repair valuation-outage` |
| `backfill futures_bars --shfe-annual-archive ZIP --archive-year Y ...` | [`cne import shfe-archive ZIP --dataset futures_bars --year Y ...`](#cne-import-shfe-archive) |
| `--refresh` (derivatives) / `--force` (`sector_bars`) | `--refetch` |
| `--retry-failed` (`sector_bars`) | No longer needed: a backfill resumes by default |
| `--margin-source` | `--source` |

The former `--eastmoney-bj-repair`, `--bj-amount-repair` and `--tdx-volume-repair` have been removed: they only fixed data written by 0.17 or earlier (`cne repair bar-amounts` replaces `--bj-amount-repair`). The daily update never needs these repairs; rules at write time keep new data right:

| Problem once handled by a repair flag | How it is handled now |
|---|---|
| Missing BJ current-period turnover | The daily update uses the BSE quote board as the current-period primary source; rows missing turnover at the exchange stage raise a warning before merge |
| A batch of BJ securities missing on a session | The completeness rule before merge records the session in the gap ledger, and the next run refetches it |
| Out-of-limit moves with no event | Quarantined; the committed value is kept and marked for refetch |
| BJ adjustment steps Sina missed | Factors are computed from corporate actions, `pre_close` checks the steps, and Sina is only a cross-check |

```bash
cne backfill minute_bars_5m --start 2026-05-01 --end 2026-07-31 \
  --symbols 600519.SH,000001.SZ

# Fill only current-partition turnover for existing BJ daily bars, without refetching Sina history (preview first, then --apply)
cne repair bse-tip-turnover --date 2026-08-21 --symbols 920000.BJ,920001.BJ --apply
```

### Derivatives backfill

| Option | Description |
|------|------|
| `--plan` | Read-only plan: sources, dates, cold-cache request estimate, rate limits and follow-up steps; no network, no writes |
| `--exchange` | Restrict to a publisher, repeatable; INE is routed via SHF, and 2018 futures also fetch the energy-center daily files |
| `--refetch` | futures_bars / option_bars ignore receipts and the old cache and refetch; circuit breakers still apply |
| `--symbols` | Explicit contract list for futures_minute_bars, e.g. CU2611.SHF; daily bars do not support per-contract files |

A daily-bar backfill updates the matching contract table and derivations automatically and outputs `followup`; minute bars have only a rolling window and refuse date ranges. Continuous futures are recomputed in full and refuse `derive --start/--end`. SHFE annual packages are imported offline with [`cne import shfe-archive`](#cne-import-shfe-archive). The full workflow, source constraints and acceptance are in the [derivatives guide](../recipes/derivatives.md).

Historical reference replay and request bounds of derivatives contract backfills are in the [derivatives research guide](../recipes/derivatives.md). `--start/--end` of a contract table limit the reference file dates and do not trim the final contract set.

### sector_bars

It resumes from the checkpoint by default, skipping finished boards; `--refetch` clears the checkpoint and refetches everything. Checkpoint: `meta/state/sector_bars_backfill.json`. When more than 50% fail the step status is `warning`, but the successful part is still written.

**Network**: goes through THS `d.10jqka.com.cn` (the same source for daily and history), rate-limited under `[sources.ths]`.
It has nothing to do with EastMoney; `[sources.eastmoney] proxy` does not apply to it.

```bash
cne backfill sector_bars              # resume
cne backfill sector_bars --refetch    # full refetch, first time or after a source change
```

## cne import shfe-archive

```bash
cne import shfe-archive ZIP --dataset futures_bars --year 2018 --accept-partial-fields           # preview
cne import shfe-archive ZIP --dataset futures_bars --year 2018 --accept-partial-fields --apply   # import
```

Imports an official SHFE annual package offline: it checks the format, keeps the missing-field marks, skips daily-bar keys already present, and never overwrites complete daily-file data. `--accept-partial-fields` is required and accepts that the annual package lacks fields only the daily files have; `--url` / `--downloaded-at` attach the original source evidence; `--start` / `--end` stay within `--year`. The default backfill still uses complete daily files; see the [derivatives guide](../recipes/derivatives.md) for details.

## cne run compact

| Option | Description |
|------|------|
| `--run-id` | Publish only this run (by default handles every run awaiting publication) |

Without `--run-id`, it publishes, in chronological order, every run that is "finished, has staged files, and has not yet compacted successfully", so you no longer have to find and name each run_id. Runs still in progress and init runs (resumed by `cne init`) are left alone.

Merges results in staging that passed sealing checks into curated. Active batches, old-version unchecked partial staging, and integrity errors remain protected by the gate; failed requests are kept for retry.

## cne delisted

Read the delisting directory. Delisted price data is filled with `cne backfill daily_bars --profile delisted`. **Rebuilding** the directory (scanning the code space, checking end points, fixing instruments,
the coverage gate) is a one-off engineering task, in [`scripts/delisted_ops.py`](../operations/scripts.md#delisted_opspy).

| Subcommand | Description |
|--------|------|
| `status [--since]` | Directory summary: counts, years, not yet ingested |

Recommended order (across the CLI and scripts):

```bash
cne delisted status                                 # how many are known
python scripts/delisted_ops.py discover --limit 500 # scan the code space; resumable
cne backfill daily_bars --profile delisted --start 2016-01-01  # pull price data for the delistings found
python scripts/delisted_ops.py repair               # write delist_date once bars are in the lake
python scripts/delisted_ops.py reconcile            # dry-run first
python scripts/delisted_ops.py reconcile --apply    # only when there is no active ingestion run
python scripts/delisted_ops.py coverage --start 2016-01-01 --universe all_a_sh_sz
```

The pass claim of `coverage` is deliberately narrow: it proves that the delisting directory has been fully scanned, and that delisted symbols known to overlap the window have a consistent last valid trade and security master data; it does not prove that every trading day between the two ends is continuous. A data source may keep zero-volume placeholder rows before a suspension or formal delisting, and the gate does not mistake them for the last trade. Symbols whose directory end date is later than the window but that have no price data within the window to prove they were listed go into `unknown_overlap`, rather than being silently excluded.

`reconcile --apply` does not take the "last record" returned by a single vendor as the truth: automatic changes are allowed only when there is a curated
positive-volume end point, that end point is no later than `instruments.delist_date`, and the old directory date also falls
after the formal delisting date or on a non-trading day. The command refuses to run if it detects any active ingestion run;
the directory before modification is saved in `meta/state/history/`, the quality receipt is written to
`meta/quality/`, and the SHA-256 of the pre-modification backup and the modified directory are recorded.

## cne derive [name]

| name | Description |
|------|------|
| `adj_factors` (default) | Compute Sina hfq factors |
| `trading_status` | Derive historical suspension records (`--start` / `--end` rebuild in yearly chunks); runs automatically by window after `init` and `cne backfill daily_bars` |
| `sector_routing` | Optional: EM sector × TDX 88xxxx name mapping table (**does not drive** sector_bars collection) |
| `sector_code_map` | BK* ↔ BOARD_CODE identity mapping (lake-only; recommended for constituent joins) |
| `futures_continuous` | Futures dominant/second-dominant continuous contracts, rolled on T-1 open interest and only forward; fully rebuilt from futures_bars (requires `[futures] enabled`) |
| `option_greeks` | Option implied volatility and Greeks (Black-76 / BAW), automatically detecting changes in quotes, contracts, rates and model dependencies; `--full` recomputes everything, `--start`/`--end` restrict the window |
| `minute_bars_15m` / `minute_bars_30m` / `minute_bars_60m` | Not computed by default. Manually computes 15m / 30m / 60m into the lake: for a given stock and day, use 1m if present, otherwise 5m; stocks missing bars needed to build a bar are skipped for that day and counted. With no window, only trading days not yet computed or whose 1m / 5m have been updated are computed; `--full` recomputes everything, `--start`/`--end` restrict the window. Rules in [15 / 30 / 60-minute bars](../recipes/minute-bars-15-30-60.md) |
| `adj_factor_source` | Use Baostock to arbitrate contradictions between adjustment factors and corporate actions (step missed by Sina, spurious Sina step, event missing from the lake, questionable event); for Shanghai/Shenzhen stocks where Sina is proven wrong (the trading day before the ex-date is within 10 days, not within the 2005-04-29 to 2007-12-31 share-reform period, the two vendors' steps differ by more than 0.45%, and a native ex-reference price supports Baostock) and Baostock agrees with the remaining events, `--apply` switches the whole factor series to Baostock, with evidence written to `meta/quality/evidence/`. Baostock results are cached per batch for 7 days, and only complete results covering this query's start and end dates are reused; each security uses its latest complete snapshot, and fragments from failed requests do not take part in arbitration. With the same window, a rerun after interruption fetches only securities not yet finished, and an `--apply` after a preview can reuse the preview's data; evidence is refetched when the query end date advances or an old cache lacks coverage dates |

```bash
cne derive trading_status --start 2001-01-01 --end 2001-12-31
```


## cne migrate

Fixes data that older versions wrote, after an upgrade. When a release fixes a writer, rows the old writer stored still need a one-off correction; each such correction is a migration. `meta/state/lake.json` records the version that built the lake (migrations introduced earlier do not apply) and every migration this lake has run or checked and found nothing to do.

```bash
cne migrate             # preview: how many rows each migration would change; writes nothing
cne migrate --apply     # runs them in order and records the results
```

| Option | Description |
|------|------|
| `--apply` | Run the owed migrations and write the results to `meta/state/lake.json` |
| `--only` | Comma-separated migration ids; runs them even when already recorded |
| `--json` | Machine-readable output |

Each migration holds the lake mutation lock, publishes through the repair gate, and keeps the previous revision readable. When one fails, the run stops; later ones neither run nor get recorded. `fund-listing-dates` reads the exchanges' official fund lists and `misdated-valuation` reads the EastMoney datacenter when it has rows to replace; the rest read only the lake. `cne doctor` reports migrations not yet checked. Migrate a lake written by 0.12 or earlier with 0.19 or earlier before upgrading.

## cne repair

By default it outputs a plan based on data already in the lake; with `--apply` it publishes as a new version, the old version is kept, and both can be read by version. `corporate-action-gaps --apply` fetches evidence from data sources for each gap.


Before publishing, `--apply` compares offline quality checks between the committed version and the candidate version. Historical factor/action contradictions are advisory diagnostics. New or worsened structural problems, including invalid factor values, corrupt files and primary-key errors, block publication; the candidate version is moved into `_quarantine`, and the report is written to `meta/quality/publication/`. This repair gate is not affected by the ordinary daily-update publication setting `[quality].publication_gate`.

| Subcommand | Description |
|------|------|
| `layout DATASET` | Merge files in a partitioned dataset's root directory or in wrongly keyed directories into the registered partitions. Multiple observations of the same primary key fill each other's blanks only when their non-null values agree exactly; for conflicting keys one whole row is kept by the canonical rule. All original observations of duplicate keys are written to `_quarantine`. When publishing a new version detects such a layout, it suggests running this command |
| `repartition [DATASET] [--all]` | Rewrite a dataset's partitions at its registered granularity; without arguments it lists the datasets that need it, with one it previews the file, partition and size change, and `--apply` rewrites under the lake lock and swaps it in. It reclaims space and file handles and fixes no correctness problem; the audit points here when it finds mixed granularity |
| `payment-dates` | `corporate_actions` cash payment dates: reviewed issuer announcements first, then Baostock; `--issuer-notices-only` uses issuer announcements only and applies reviewed bonus/transfer terms. Needs `--symbols`/`--start`/`--end` |
| `delisted-actions --source baostock\|ths` | Fill historical dividends and ex-rights of delisted securities: `baostock` for Shanghai/Shenzhen, `ths` for Beijing |
| `ex-dates --ex-dates ...` | Request historical ex-rights rows from EastMoney day by day for the given ex-dates; the only way to reach rows before 2015-09-29 |
| `bar-amounts` | Cross-check Sina-filled historical daily bars against TDX and fill turnover when OHLC match and volume differs by under one lot; prices and volumes untouched |
| `bar-turnover [--peer-review]` | Replace Shanghai/Shenzhen stock daily bars whose turnover is missing, zero or misaligned with the whole same-day Baostock row; `--peer-review` reviews conflicts against exchange history, TDX and EastMoney |
| `bse-tip-turnover --date D` | Fill Beijing turnover for the latest session from the BSE quote board, without refetching Sina |
| `valuation-outage` | Fill the `--start`/`--end` window of an EastMoney snapshot outage from the datacenter valuation report; writes only when the whole market is retrieved |
| `corporate-action-gaps` | Fill ex-rights events where both the Sina and Baostock factors have a step but the lake has no record, based on the evidence of the most recent `cne derive adj_factor_source`. A record dated within 10 days nearby is moved to the step date if its terms explain the step; for the rest, dividends are fetched from Baostock by security and year, matched by effective trading day, and only rows whose terms explain the step are accepted; for those Baostock cannot explain either, CNINFO is searched for the issuer's restructuring capital-conversion announcement, and the event is recorded as `reorg_transfer` only if the ex-rights reference price stated in the announcement explains the step. This command previews offline, and only `--apply` requests Baostock and CNINFO |
| `status-traded --start --end` | Read-only preview by default: fill missing stock/ETF status keys within a known listed life from committed positive-volume daily bars, using `derived_traded` with null ST. Existing statuses are preserved. `--apply` recomputes against current inputs, publishes a new version and keeps the old one. Use annual or monthly windows |
| `stock-listing-dates` | Use the first traded session of stocks lacking listing dates to locate CNINFO IPO calendar requests, or repeat `--event-date YYYY-MM-DD` for stocks without bars. Only explicit code/date-matched listed events establish dates; earlier real trades must not contradict them. Replays archives by default; `--refresh-sources` fetches evidence and `--max-sessions` bounds batches. `--apply` publishes while retaining known dates and old revisions; a stale delisting date earlier than the confirmed listing is cleared in the same revision |
| `index-snapshots` | Replay complete, dated official membership and weight files; `--refresh-sources` refreshes archives and `--indices` selects indices. `--apply` replaces whole snapshots, converts percentages to ratios and preserves membership/weight file dates without backdating |
| `financial-originals` | Select stocks with `--symbols` or `--all-stocks`, years with `--start-year/--end-year` and periods with `--quarter` (Q1/Q2/Q3/Q4; annual by default), and bound batches with `--max-reports`; pass `next_cursor` to `--after` to continue. Offline preview by default; `--refresh-sources` archives complete CNINFO queries and original PDFs, and `--apply` appends identifiable current-year consolidated figures. `--reparse` replays existing reports while retaining unchanged facts’ first observation times. Unidentified layouts/items remain missing; observation times are never backdated |
| `adj-factors` | Unified stock factor and ETF/LOF unit conversion repair. Offline preview saves a plan; `--refresh-sources` explicitly fetches evidence and `--apply` rechecks inputs before publishing supported corrections. Supports `--symbols`, event window `--start/--end`, `--max-symbols` and `--plan`; unresolved events retain values and diagnostics. See [historical factor repair](../datasets/sources.md) |
| `fund-volume` | Repair zero-volume, positive-amount Shanghai and Shenzhen exchange-listed fund bars from complete SSE auction K-lines. `--refresh-sources` archives evidence; offline replay is the default. `--apply` replaces only rows within the listing window with matching OHLC and rounded amounts, retaining old revisions. When stored turnover conflicts, archived unadjusted EastMoney OHLC and amount must corroborate the complete official bar; peer whole-lot volume never supplies the quantity. Compatible raw archives are reused before sending another request. Volume is never inferred from turnover. Later zero-volume, positive-amount observations cannot overwrite a restored usable quantity. |
| `fund-no-print` | Reconcile fund bar debts using explicit zero-yuan turnover in historical SZSE reports or SSE responses matching the security and date with explicitly absent auction prices and a positive reference price. Offline preview by default; `--refresh-sources` fetches and archives reports, and `--max-sessions` bounds sessions. `--apply` approves evidence and refreshes coverage watermarks. Missing responses do not prove absence; normal tradability does not imply an actual print. Rounded zero turnover alone is insufficient; SSE evidence proves absence of auction OHLC, not absence of all transactions. Creates no bars or halt rows. Unusable evidence reopens debts, and actual traded bars take precedence |

```bash
cne repair layout futures_bars
cne repair layout futures_bars --apply
```

## cne audit

| Option | Description |
|------|------|
| `--run-id` | Findings of the given run (defaults to the latest run); repeat to combine step evidence from these runs into one lake audit |
| `--full` | Lake-level health snapshot (not a per-run file) |
| `--quality-only` | With `--full`: gate only on quality errors; check schedule freshness separately with `cne status` |
| `--research-start YYYY-MM-DD` | Used with `--full`; strictly validates the selected historical universe and exits 1 if it fails |
| `--research-end YYYY-MM-DD` | End of the research window; defaults to the latest `daily_bars` partition |
| `--research-universe all_a\|all_a_sh_sz` | Historical research scope; default `all_a`, `all_a_sh_sz` excludes BJ's source capability gaps |

`--full` with UNHEALTHY exits 1. When `--research-start` is passed explicitly, a failing research universe also exits 1; the last line then shows `HEALTHY (operational; research BLOCKED)`, meaning the lake's operational health and its research usability are two independent gates. When `--research-start` is not passed explicitly, the historical-universe status is still written to health and `historical-validity-latest.json`, but it does not change the operational-health exit code. The snapshot also records `historical_universe`, so a scoped result is not misread as all A-shares.

## cne verify

Checks "did what should land actually land" at four granularities. By default by dataset × trading day; `--bars` by security ×
trading day; `--runs` by trading day × run; `--derivatives` checks a derivatives research window. The modes are mutually exclusive, and options that do not apply raise errors.

| Option | Mode | Description |
|------|------|------|
| `--dataset` | default / `--derivatives` | Check only these datasets (comma-separated); default all registered datasets |
| `--repair` | default | Run backfills for repairable gaps, by dataset from newest to oldest; rechecks automatically afterwards and exits 1 if gaps remain |
| `--kind` | default | Look only at these gap types: `empty,stale,interior,shallow` |
| `--bars` | — | Switch to per-security coverage checks, see below |
| `--start` / `--end` | `--bars` | Coverage window; `--start` is required, `--end` defaults to the last complete trading day |
| `--derivatives` | — | Read-only window check; requires a single `--dataset futures_bars` or `option_bars`, plus `--start` and `--end` |
| `--runs` | — | Switch to checking run evidence over consecutive trading days, see [cne verify --runs](#cne-verify---runs) |
| `--days` / `--as-of` / `--enforce` | `--runs` | See the section below |

(`--bars` was formerly `cne verify-bars`, `--runs` was formerly `cne stability`.)

**It does not ask the same question as `cne audit`.** `audit` asks "is the data that landed correct", `verify` asks
"did what should land actually land"; the latter is the failure produced when a step throws an exception as soon as it is touched. Without it, a dataset can
fail on every run for weeks, while each run records just one failed batch and nothing shows at the lake level.

**Gaps are separated by whether they can be filled, not by size.** A `by_date` dataset missing a trading day is a fault;
a `snapshot` dataset missing a trading day is its natural shape, and no backfill can honestly fill it
(filling it would mean fabricating rows). `--repair` only runs the former.

```bash
cne verify                                  # full-table checkup
cne verify --dataset daily_bars,adj_factors
cne verify --kind interior --repair   # fill only interior holes; rechecks automatically afterwards
cne verify --bars --start 2026-09-07  # per security × session, including securities with zero rows in the window
cne verify --runs --days 20 --enforce # run evidence over consecutive trading days
```

`--derivatives` outputs JSON: exchange missing days, missing rows for known contracts on adjacent trading days, missing metadata, outstanding debt within the window, and per-product coverage. Exit 1 means gaps were found, exit 2 means insufficient evidence; without a complete historical listed-contract list it never exits 0. It does not support `--repair` and does not go online or rewrite data. The first day reads the previous trading day's evidence; an authoritative lifecycle can also check known contracts with no price data across the whole window. For DCE via Sina, missing rows for zero-volume contracts are not judged incomplete, and INE's source-side start boundary may still be unprovable. This check cannot replace acceptance of historical parameters, derivation validity or trade executability.

```bash
cne verify --derivatives --dataset futures_bars --start 2026-09-01 --end 2026-09-24
```

`--bars` checks the "security × session" cell, including securities with not a single row in the window; the default mode aggregates by dataset
and cannot see this kind of absence. Explicit non-trading states such as suspensions are not gaps. Exits 1 when incomplete.

## cne decision-data payment-gaps

Lists cash dividends with no payment date, by the current committed `corporate_actions` revision, labeling each with:

- `bar_relation`: whether the event falls `within_observed_bar_span` / `before_first_observed_bar` / `after_last_observed_bar` / `no_observed_bar` of the code's observed price data. No position can receive cash for an event outside the tradable period; this is only a routing hint and does not prove the issuer's identity on that date.
- `ex_date_rule_eligible`: whether it is a Shanghai, Shenzhen or Beijing A-share stock for which the ex-date can stand in under the CSDC ex-date settlement rule (see [corporate_actions payment dates](../datasets/sources.md#corporate_actions)).

In the summary, `within_span_count` is the gaps that may affect positions, and `within_span_rule_ineligible_count` is the part of those the rule cannot fill either, which truly needs a source. `unreviewed` only means not yet reviewed event by event; it does not mean the announcement is missing.

| Option | Description |
|------|------|
| `--start` / `--end` | Start and end dates; `--start` defaults to 2016-01-01, `--end` defaults to today (or the day before the holdout when one is configured) |
| `--output-dir` | Immutable JSON directory, default `{data.root}/meta/decision_data_gaps` |
| `--config` | Select the data lake configuration |

## cne decision-data stock-terms

Lists events that have both a positive `bonus` and a positive `transfer` on the same ex-date. Their coexistence is only a review signal, since a real plan can have both; the issuer's announcement is authoritative. Options as above; output prefix `stock-terms-`.

## cne decision-data cash-rights

Expands reviewed original issuer announcements by holder class into an immutable JSON evidence view. Each entitlement must agree with the tradable A-share amount, payment date and original announcement PDF hash in the current `corporate_actions` revision; a corporate action flagged with differentiated entitlements fails if it lacks review evidence. It does not infer the record date, and it is not yet a general corporate-action ledger dataset.

| Option | Description |
|------|------|
| `--start` / `--end` | Same as `payment-gaps` |
| `--output-dir` | Content-addressed JSON directory, default `{data.root}/meta/decision_cash_rights` |
| `--config` | Select the data lake configuration |

## Command results and exit codes {#command-results-and-exit-codes}

When fetching, initialization, backfill, retry and derivation finish an attempt, they report execution, coverage and publication separately. They keep the old `status` field and adopt the independent result fields of version 2:

| Field | Values and meaning |
|---|---|
| `result_schema_version` | `2` for new results; old records stay `1`, and unknown coverage is not automatically upgraded to complete |
| `execution_status` | `queued` / `running` / `completed` / `skipped` / `failed` / `interrupted`, describing this execution |
| `coverage_status` | `complete` / `partial` / `unknown` / `not_applicable`, describing evidence-backed coverage of the requested range; row counts or success messages cannot prove completeness |
| `publication_status` | `pending` / `published` / `partial` / `unchanged` / `none` / `rejected`, describing version publication by the current run |
| `fallback` | When sources are limited, lists the readable data already present, registered source roles and switches, the requested range and the retry command; the source list does not mean those sources were tried or are currently reachable |
| `reason_code` | Dataset receipts and batches keep a structured reason, for example `source_transient`, `source_unavailable`, `input_unavailable`, `storage_failure`, `execution_error` |
| `usable_result` | There is a valid request result, a covered range, or explicit evidence of no data; it can be false when sources are limited and this run has 0 rows. A daily run does not count that as an execution failure; `cne backfill` exits 1 then, see below |

`completed + partial` can be a normal final state. Verified facts can be published, while missing ranges, unscanned securities, cooldowns and retry evidence stay in the ledger. A derivation missing inputs explains the skip; derived versions record input versions and coverage evidence and require a recompute when inputs change. When all sources are unavailable and this run has no valid request result, it also ends normally as insufficient coverage and keeps the existing lake (see below for the exit code of `cne backfill`); program, configuration, storage and integrity errors are still reported as failures.

Fetch / write commands: `success` and source-limited `warning` / `degraded` (including 0 rows) return 0, `failed` returns 1. The exception is `cne backfill` (including `--outstanding`): it returns 1 when the status is not `success`, `usable_result` is false and this run wrote 0 rows, because a backfill exists to fill that range and filling none of it is a failure. Unavailable sources and sources that cannot serve the range (`capability_limit`) both count; the JSON result and `fallback` are unchanged. Explicit quality checks (`audit`, `verify`, `status --gate`) continue to return non-zero according to their own quality rules. Read-only reports return 0 once they have read and displayed successfully, regardless of whether the data in the report is fully healthy.

**Upgrading scheduling scripts:** if you previously relied on the exit code of `status --datasets` to trigger alerts, switch to `status --datasets --gate` (also add `--groups` when scheduling by group). Scripts that depend on daily-update / backfill exit code 2 should read `coverage_status`, or run an explicit quality gate separately. Existing data directories, manifests, checkpoints and successful batches do not need to be deleted; old metadata is migrated incrementally. Old partial staging without a verified seal stays under the conservative gate and is published after a retry over the original range.

## cne check

Accepts the whole lake in one command, reporting in order:

1. **Freshness and coverage**: the same as `cne status --datasets --gate`, including the cross-section check on the latest trading day and unfinished init;
2. **Data quality**: the audit of the most recent run and the most recent whole-lake audit snapshot and its age; errors in either are listed and count toward the exit code;
3. **Size**: datasets, rows and volume; stale statistics tables are recomputed automatically first.

| Option | Description |
|------|------|
| `--full` | Rerun the whole-lake audit on the spot. It reads every historical partition and may take hours on a large lake; by default the most recent result is read |

The exit code is the worst of the three: 0 usable; 1 gaps or quality errors; 2 cannot be proven (no audit record, instruments missing, and so on). Scheduling scripts can still use only the lighter `cne status --datasets --gate`.

## cne status

| Option | Description |
|------|------|
| `--datasets` | Per-dataset freshness table (dataset / layer / freshness / coverage range / watermark); with `--gate`, exits 1 if anything is STALE. freshness values: `fresh` / `STALE` / `empty` (never fetched) / `no source` (source retired with no replacement) / `retired` (source retired but the lake captured through its last day, such as `northbound_flows`) / `removed` (taken out of the schedule on purpose with `steps_remove` or `enabled = false`, and no other group runs it; keeps its real last date, is not stale, and `cne check`, the audit and the gate do not count it) / `n/a` (disabled in configuration, or freshness not judged by day) |
| `--gate` | Explicit quality acceptance: failure / unacceptable freshness returns 1, insufficient coverage or degradation returns 2; an ordinary read-only query returns 0 on success |
| `--all-columns` | With `--datasets`: print all columns of `list_datasets` (contract fingerprint, revision, PIT storage columns, etc.) rather than only freshness |
| `--groups` | With `--datasets`: in `--gate` mode, fail only on datasets owned by these schedule groups (space- or comma-separated). Datasets of other groups are still listed and still reported as schedule gaps, but do not trigger exit 1. A host that schedules only `core` should specify this scope, so optional groups with no scheduled collection do not keep tripping the gate. Datasets nobody schedules (`(unscheduled)`) still fail and must be handled explicitly |
| `--scope` / `--no-scope` | With `--datasets`: whether to run the symbol cross-section check on the latest trading day (on by default). For each dataset keyed by "securities active that day" (`daily_bars`, `trading_status`), it compares the tip partition with the securities table: securities with data, with explicit suspension evidence, already recorded in the pending-fill ledger, or with no-data evidence covering that day (for example a new-listing code that a daily-bar probe proved "not yet listed", accepted by both tables) count as covered, and the rest are judged INCOMPLETE. It needs to read these datasets' tip partitions plus `instruments`; `--no-scope` returns this command to pure metadata |
| `--run <id\|latest>` | Select a run (default `latest`); the summary contains each dataset stage's `dataset_results` and the aggregate `dataset_status`. The alias `--run-id` has been removed |

`--datasets` also reports an init that has not finished: a fresh date only says the existing data is fresh, not that whole-market coverage is complete. If a step missing from init has since been run successfully by a later run (for example `derive_industry_index`, which the daily update runs every day), it no longer counts as unfinished; resuming with `cne init` still judges by the strict criterion.
init does not belong to any schedule group, so it is not exempted by `--groups`; the cross-section check is governed by the group that owns `daily_bars`.

No options: outputs a JSON summary of the latest run and returns 0 when the query succeeds, including when the queried run itself failed. `--gate` runs quality acceptance on the selected report: failure returns 1, degradation returns 2.

## cne run retry

Retry failed batches / fill missing init steps. init runs go through `resume_init`.

| Option | Description |
|------|------|
| `--run-id <id>` | Retry the given run |
| `--failed-groups` | Retry, each in its own process, the latest failed run of each `daily:*` group; if that group already has a newer successful run, the old failure is skipped |

Exactly one of the two must be chosen.

Success or source-limited outcomes exit 0 and deliver the valid partial results obtained plus the fallback; real execution errors or `RunLockError` exit with an error.

Newly written backfill runs store the dates, symbols, minute frequency, tick range, derivatives exchanges and contracts, plus one-off source selection and repair parameters. Using the `cne run retry --run-id ...` from the fallback restores these parameters, so a new process does not skip fetching or fetch the wrong range. Parameters not recorded by old runs follow the current configuration; credentials, proxies and access policy are still read from the current configuration. The sector `--force` resets the checkpoint only on the first execution; a retry continues the unfinished part.

## cne run clean

Preview compacted terminal run staging, overage orphans, source snapshots, logs and old versions. Even without `--dry-run` it never marks or deletes files; scheduled scripts can only produce notices.

Backfill and the daily run drop older generations at the end, using [`[storage.retention].keep_generations`](../getting-started/configuration.md#storage-retention). The default is 5. This command does not delete. Set the value to `0` to delete none.

| Option | Description |
|------|------|
| `--dry-run` | Compatibility option; cannot be combined with `--reconcile-runs`, which modifies run state |
| `--orphan-retention-days` | Days to keep orphan staging with no manifest (default 7) |
| `--snapshot-retention-days` | Days to keep source snapshots (default 14); the newest one per dataset/source is kept |
| `--keep-revision-generations` | Keep the latest 5 generations per dataset, additionally protecting current and hold; `0` skips the version preview |
| `--log-retention-days` | Preview `logs/cne-*.log` older than the given number of days (default 30); `0` skips |
| `--reconcile-runs` | Explicitly mark orphaned running runs that meet the quiet condition as failed; this still modifies run state |
| `--reconcile-after-seconds` | Override the reconciliation quiet window, default `[orchestrator].batch_stale_seconds` |
| `--force` | Also include staging that is not cleanup-ready in the preview, without deleting or downgrading batches |

Outputs `dry_run: true`, `confirmation_required: "serve_storage_page"`. Actual-deletion fields such as `removed` / `removed_run_ids` are empty or 0, and `bytes_freed` is always 0; candidates are in `candidate_run_ids`, `candidate_run_dirs` and `candidates`, and the logical size is in `logical_bytes_selected`. Old versions and terminal staging runs with successful compaction and no incomplete batches can be selected, reviewed and confirmed for deletion on the serve storage page. Confirmation rechecks run locks, holds, run state and per-file fingerprints. It processes only the original list and retains resumable deletion receipts. Orphan staging, uncompacted runs and runs with incomplete batches are excluded from web deletion. Source snapshots and logs are still report-only.

The preview also returns `staging_inventory`, `staging_run_count`, and `skipped_reasons`, listing every run's status, size, age, incomplete batches, and candidate/retention reason. A run lock protects active retries; `--force` cannot bypass run locks or holds. Incomplete batches are not automatically retired based only on age or the existence of a newer successful run.

## cne storage

The version lifecycle comprises reference registration, immutable plans, an in-place observation period, and physical reclamation within a maintenance window. Nothing is deleted automatically after 7 days. Backfill and the daily run remove older unretained generations at [`[storage.retention].keep_generations`](../getting-started/configuration.md#storage-retention) (default 5); that is the only deletion that skips the observation period and the web confirmation. Set it to `0` to delete none. External readers must be stopped by the operator.

| Command | Behavior |
|---|---|
| `inspect --keep 5` | Read-only listing of the versions that still exist and their retention reasons. `logical_bytes` is the sum of file sizes; `exclusive_bytes` is what deleting that generation would release, and a file hard-linked from another generation is not included. An APFS clone is a separate file and still counts, so this is not a promise of freed space on that filesystem. Without imported references it is only a first-pass screen |
| `explain OBJECT_ID` | Show the grounds for a version, such as current/recent/hold |
| `import --manifest FILE` | Validate and merge a reviewed local reference manifest; never lifts existing protection automatically |
| `hold OBJECT_ID --reason TEXT` | Add a retention reason for a version or cleanup resource, canceling the corresponding pending-deletion mark |
| `plan --keep 5 --phase mark` | After validating reference sources, save a content-addressed mark plan and return a `plan_id` |
| `plan --keep 5 --phase purge` | Select only candidates whose observation period has ended and whose content is unchanged, checking SHA-256 file by file; outputs `purge_ids` and `logical_bytes_to_purge` |
| `apply PLAN_ID --phase mark` | Revalidate pointers, references, registrations and file identity, and start an in-place observation period of at least 7 days |
| `apply PLAN_ID --phase purge` | Disabled; must go to the serve storage operations page to recheck and confirm |

Every subcommand accepts `--config`. A version `OBJECT_ID` is `revision/<dataset>/<revision_id>` within the current lake; a plan is additionally bound to the lake identity and the metadata root, and cannot be used directly on another lake. A version that is referenced but temporarily missing can also be protected through manifest registration, so it stays protected after it is restored.

The reference manifest schema is `schema_version: 1`, containing an absolute `meta_root`, `holds` (object ID to a non-empty list of reasons), `reference_roots` (absolute directories with an optional relative `exclude` list), `reference_fingerprint` and optional `experiments`. Integrations can use `cnequity.storage.lifecycle.reference_files()` to enumerate reference sources, generate holds after classifying them, and use `reference_fingerprint()` to compute a fingerprint of the file set and SHA-256; pass both `meta_root=` the lake's `meta` directory. With that directory as a root, what the lake rewrites on every run (source caches, `state`, `checkpoints`, `quality`, `source_snapshots`, derivatives capture records and so on; the full list is `RUNTIME_META_PATHS`) is not counted as a reference; otherwise any run would invalidate the import and cleanup would refuse to run. Import only checks that the manifest still corresponds to those files; it does not replace review of reference semantics, and exclusions and unregistered external consumers must be verified by the operator.

Adding, deleting or modifying reference files stops an old plan from continuing to mark; it must be re-reviewed and imported again. Repeated imports keep existing holds. There is currently no command that lifts a hold automatically; changing a retention commitment requires a separate review. Directories without a receipt, without a complete file list, or without a current pointer never become cleanup candidates.

The reference manifest can also carry `cases`: each has an immutable `case_id`, `roots` (a list of object IDs) and `dependencies` (`from`, `to`, `kind`). `requires_bytes` produces holds recursively from the roots; `provenance` only records origin and does not retain bytes recursively. Circular dependencies are counted once. A case can be added on its own through Python `LifecycleStore.register_case()`, which requires `evidence` (an absolute `path` and `sha256`); this only adds protection and cancels pending-deletion marks on the affected objects; it does not lift old holds, nor does it prove that a full replay is available.

**Physical deletion requires web confirmation and a maintenance window.** The operator must confirm on the web page that new scheduling, other services and all external queries have stopped, and wait for deferred reads to finish. The dashboard only pauses this instance's data requests and waits for background scans to finish; it does not stop external processes automatically. The executor also tries to take the publish lock, and refuses if the manifest still has running runs. Pausing only collection while keeping external LazyFrame reads does not satisfy this condition.

Every candidate is revalidated before deletion. The executor first records its intent, then atomically moves a single generation into `meta/lifecycle/trash/<plan-id>/` on the same file system, then deletes the content. Receipts are kept; reading a specific old version fails explicitly. Errors are never silently skipped and never widen the scope; the operation record is at `meta/lifecycle/purges/<plan-id>.json`. While the protection conditions are unchanged, after re-reviewing under "An unfinished deletion is still open" on the web page and confirming the original list, the leftover files are verified and the original plan continues; a completed plan only returns its original result and deletes nothing again. If a hold, reference or publication was added in the meantime, the retry stops; the affected leftover content must first be checked and restored by hand, and a new plan cannot be used to bypass an unfinished operation.

`logical_bytes_deleted` only accumulates versions that were deleted completely; bytes from partial failures are not counted, and `partial_deletion_possible` is reported. File-system free space is recorded separately before and after, and may be affected by other system activity. Neither pending-deletion marks nor deletion receipts replace backups.

`logical_bytes_selected` comes from the versions' file lists and is not the actual disk space freed; APFS clones and system snapshots can make the two differ greatly. During the mark phase all bytes stay in place.

The other existing cleaners share holds: `staging/<run_id>`, `source_snapshot/<dataset>/source=<source>/data_version=<version>/run_id=<run>`, `log/<cne-*.log filename>`. Existing objects can be protected with `storage hold`, or through a case's `requires_bytes`. Age, compact/readiness and newest-snapshot-per-type rules still apply; `--force` does not bypass holds. When a registered reference source changes or becomes unreadable, these cleaners also stop and require the manifest to be refreshed.

A full snapshot saves the lifecycle retention grounds and the external object list, marked `external_bytes_materialized: false`. This does not pack external artifacts and all old versions into the snapshot. After restoring, reference sources must be rebound, reviewed and imported; import inherits the saved holds/cases and creates a new lake identity, without inheriting pending-deletion state or this machine's deletion plans. Lakes with lifecycle registrations do not support delta packages for now: creating and applying them is explicitly refused; use a full snapshot to keep these dependencies.

## cne serve

Lake dashboard: layered overview, per-dataset coverage and freshness, provenance distribution, coverage heatmap, operations page, and storage operations that require explicit confirmation.

| Option | Default | Description |
|------|------|------|
| `--host` | `127.0.0.1` | A loopback address accepts both `127.0.0.1` and `localhost`. A non-loopback address **must** be paired with `--token` |
| `--port` | `8787` | |
| `--token` | none | Requires `Authorization: Bearer <token>` or `?token=` |
| `--read-only` | off | Browse only. Write entry points for the operations page and storage cleanup are not registered |
| `--allow-remote-ops` | off | Allow starting fetches from the operations page on a non-loopback address too. The token is in the URL and travels in plain text on a LAN, so this is off by default |

```bash
cne serve
```

The page is at `/`, a single dataset at `#/dataset/<name>` (three tabs: status / metadata / data), operations at `#/ops`, runs at `#/runs` (with a live Gantt chart, 100 per page, paging through every run in the manifest), quality at `#/quality`, and OpenAPI at `/api/docs` (generated from the handlers, so it cannot drift from the implementation). The sidebar can switch between Chinese and English. Datasets show a Chinese or English name on the page, with the registered identifier kept under the name; commands and APIs still use that identifier. The choice is remembered in the local browser. The run list and run details state the reason for each failed batch directly, such as the source status code or the cooldown explanation; "one or more core steps failed" on the run no longer stands in for those reasons. When a degraded run has no run-level error, the batch reasons are shown as well. The dataset list goes straight to a catch-up when data is behind; a single dataset's page puts the available backfill, catch-up or recompute next to the title. When initialization has not finished, that run's primary button is to continue initialization. The runs page can manually start commands that leave a run behind: today's daily update, one schedule group, catching up on lagging data, the event streams, backfilling one dataset, recomputing one derivation. What opens is the same form as on the operations page, preview first and then confirm, and only one runs at a time. When today is not a trading day and no session is pending, "Run today" is not offered. The list filters by all, running, needs attention and succeeded, still 100 per page. Needs attention counts only runs that still need handling: if the same task later succeeded or is being rerun, or an earlier failure has been superseded by a newer attempt, it no longer counts; a skipped non-trading day does not clear an earlier failure. A missing initialization step that was later completed no longer counts either. Batches that were retried successfully or are obsolete no longer count toward failure reasons. The list and the run details open the same form for this run: retry, retry failed daily groups, publish staging, or continue an unfinished initialization. Health checks, backups, inspections and fetch settings remain on the operations page.

**The operations page is organized by task, and what it starts is still the corresponding `cne` command.** The four blocks are initialization, daily update, manual update and common commands. Initialization sits at the top when the lake is empty or unfinished, and collapses into a one-line status once complete, no longer offering "initialize again" as the primary button. The daily update is the everyday main action, showing whether today is a trading day, the scheduled time and whether this session has run. When today is a trading day, the primary button runs today now; when today is not a trading day, running today is not offered, and if a scheduled session has not yet run, the button runs that trading day instead. The event streams and closing catch-up remain available. Manual update, common commands, schedule settings, fetch settings and backups are collapsed by default. A manual update starts by choosing a scope: the research pack or one schedule group with a trading day, the event streams with a calendar day, one dataset or one derivation with start and end dates, or only catching up on data still behind. Only one command starts at a time; you cannot tick several datasets in one task and backfill a stretch of each. Common commands list command templates from the same allowlist (without the local configuration path); clicking "Use this" opens the corresponding form; `cne repair`, `cne query`, configuration writes and the delisting engineering scripts run only in a terminal. The command line is always previewed before confirmation. Only one runs at a time; the task is in a separate process, so closing the browser or stopping `cne serve` does not stop it. Progress is written to `{data.root}/logs/cne-serve-job-*.log`, and the associated run is on the runs page. Cancel is equivalent to Ctrl-C on POSIX and ends the process tree on Windows. A daily update covers only one trading day. To fill a stretch of daily bars, change the manual update's scope to one dataset, choose `daily_bars`, and enter a start and end; daily bars are requested per symbol, so even filling one day scans the market over that range. A snapshot such as trading status that missed its day cannot be recovered by changing to an old date. Manually running one schedule group or one backfill does not mark that day's scheduled daily update as done.

**Fetch settings write back to the current configuration and do not start a command.** Only these can be changed: pausing push2 (`[sources.eastmoney] push2_paused`), the `enabled` switch of TDX, EastMoney, Sina daily bars, Baostock, the BSE website, THS pages and CNINFO, minute bars, trade ticks, futures, futures minute bars, and `[universe] ingest` and `ingest_eligible_etfs`. The page first lists the keys it will change and writes only after confirmation, leaving a timestamped backup in the same directory. Other keys, comments, intervals, budgets and credentials are untouched. Symbol lists, frequencies and exchanges are shown but not editable. `CNE_PUSH2_PAUSED=1` still takes precedence over the configuration. After saving, the scheduled daily update, closing catch-up and later commands all run with the new values. `--read-only` offers no writes; remote changes likewise require `--allow-remote-ops`.

**Scheduled jobs are managed in the daily-update block.** You can choose automatic daily update, closing catch-up, daily data backup and standalone event streams, preview the system job definitions and scope, then confirm to enable them; untick to preview and confirm a pause. The page registers launchd (macOS), crontab (Linux) or Task Scheduler (Windows) for the current user, checking once a minute. The daily update runs all configured daily groups and event streams; the closing catch-up only handles lagging snapshots and waits until that day's daily update has run; each is attempted once per trading day. The backup chooses published datasets, a directory and a Beijing time, is attempted once per calendar day once the time arrives, and never deletes old backups automatically. Standalone event streams choose event groups and run at an interval of 1–1440 minutes, including weekends and holidays; after a failure the interval is also counted from the start of the attempt, it waits while busy, and missed runs do not accumulate for catch-up after recovery. On failure, check from the job record and retry manually. Only system jobs created by the page are managed here; check existing script-based scheduling first to avoid duplicate runs.

**The backup list can read the default directory or a specified external directory.** Create, verify and restore reuse `cne snapshot`, backing up only the explicitly chosen datasets plus their state, contracts, revisions and lineage, without saving the original configuration, credentials or the full run database. After reading the manifest, the list shows "Not verified"; a verify task recomputes the file digests. The restore preview shows the scope and target; execution rechecks the manifest and verifies fully, and the target must be a new or empty directory outside the current data lake and the backup directory. The current service keeps using the original data lake; the steps for accepting the restore result and switching the configuration are in [Backup and restore](../operations/runbook.md#backup-and-restore).

Scheduled jobs keep working after the web page or `serve` is closed, and their results appear in recent jobs and the logs. Windows and macOS use the current user's login session, so nothing runs after the user logs out; Linux needs crontab installed and the cron service running. Nothing runs during sleep; after waking, the next check decides: daily updates and catch-ups past the window of 09:15 on the next trading day are not run retroactively, and backups only catch up the same day. The page shows the most recent check and prompts you to investigate if nothing has triggered for more than 20 minutes. Installation uses the current Python environment and an absolute configuration path, so after moving or deleting the environment it must be reconfigured; system jobs do not save temporary environment variables such as proxies or credentials present when serve was started.

Changing the time only changes `[job.daily] run_at` and `[job.stale] run_at` in the configuration, keeping other settings, and creates a `.schedule.bak` backup in the same directory before the first change. Non-standard inline TOML tables are refused for automatic editing. When system registration fails, automatic jobs stay paused and the page shows the error; refresh the status and retry. Pausing does not terminate a job in progress; to interrupt the current run, open that job and click cancel. `--read-only` disables schedule settings, and remote changes likewise require a token and `--allow-remote-ops`.

Job logs and backfill previews use UTF-8 throughout, with no need to set an encoding separately for the Windows terminal. On Windows, cancel checks the PID and the process creation time; when the record lacks identity information (for example a job started before an update) or cannot be checked, the page refuses to send the termination signal; handle it through the system Task Manager. After a forced cancel, you can check the published data from the associated run and retry the unfinished work.

When no configuration is found and there is no explicit `--config` / `CNE_CONFIG`, the dashboard enters first-time setup: fill in the data directory, review `cne doctor`, then start `cne init`. Initialization cannot start before the checkup finishes. The page shows the command to be run first and starts it only after confirmation. When the directory already contains a data lake, it first explains that it will take over the lake and not wipe it. Explicitly pointing at a file that does not exist still raises an error and is not generated automatically. When the lake has no curated data yet, daily and manual updates are tucked into a secondary position, and the primary action is initialization. An initialization that has not finished, whose missing steps no later run has completed successfully, can be resumed directly. When the missing steps were later completed successfully, the page no longer prompts to continue initialization. A scheduled daily update that is due but has not run names that trading day. The daily-update form explains whether today is a trading day, which session is pending, and whether this manual run counts as that scheduled one. After initialization or a daily update ends, the page offers the overview or a retry entry point.

When opened from another machine (`--host` is not a loopback address), browsing and storage cleanup use `--token`; the operations page cannot start commands by default, and `--allow-remote-ops` must be given as well. First-time setup always accepts only the local machine.

**Storage operations are at `#/storage`.** On opening, you first see the old versions that can be deleted, summarized by dataset with the largest logical size first. Versions in use and protected versions are in two other counts and cannot be ticked. All sizes are logical; under APFS clones they cannot be taken as a promise of freed space. Browsing, refreshing and the end of an observation period never perform deletion.

Tick datasets or individual versions and click "Delete selected". The confirmation sheet lists the items to be deleted; tick the irreversibility and external-jobs-stopped confirmations, then click "Permanently delete", and only then are they verified and deleted. More than 200 items at once are verified in batches. You can select old versions still within an unexpired observation period, and versions among the latest 5 generations that are not the current pointer. The current version, versions still referenced, manual holds, experiments still in use, and items lacking a receipt or archive cannot be selected. After deletion finishes, the page lists the deleted versions, their logical size, and the disk free space before and after; when free space did not increase, it says so directly, and the logical size cannot be taken as space actually freed.

You can also click "Check due items" by scope, which builds a list of only the items already due. Wait for the integrity and reference checks, then review the list. It executes only when you tick the irreversibility confirmation and the external-reads-and-writes-stopped confirmation and click "Permanently delete"; the confirmation credential expires after 10 minutes, and also when the service restarts. Protection conditions and file digests are checked again before execution. Unmarked objects can first go through "Check unmarked items", and after confirmation an observation period of at least 7 days begins; marking frees no space. A failure may leave partially deleted content; the unfinished record must be re-reviewed, and deletion cannot be retried automatically.

Operations and storage cleanup both accept only same-origin requests from the current page plus the page's confirmation credential. Anonymous access uses localhost / loopback IPs; remote access requires a token. `meta/stats` is still rebuilt on demand in the background. The health page only shows probe reports already written, and never requests third-party hosts because a page was opened.

All numbers come from artifacts already on disk (the registry, directory layout, `meta/stats`, `meta/quality/health-latest.json`, the manifest); **curated is never scanned on the request path**. So: row counts and sizes appear only after `cne stats rebuild`; findings show the snapshot of the last `cne audit --full`, with its date marked on the page.

The dashboard is for viewing health, coverage and samples; for handling gaps see the [troubleshooting guide](../operations/troubleshooting.md).

## cne stats

The lake's self-measurement tables, written to `meta/stats/`. `list_datasets()` only looks at directory names and cannot answer "how many rows does this partition have, how big is it, who wrote it"; those answers are here.

Artifacts:

| File | Granularity | Columns |
|------|------|-----|
| `partition_stats.parquet` | dataset + partition | `granularity`, `period_start/end`, `row_count`, `file_count`, `bytes` |
| `provenance_stats.parquet` | dataset + partition + source + data_version | `row_count`, `fetched_at_min/max` |
| `stats-latest.json` | — | `generated_at`, `latest_run_id`, totals |

Two tables rather than one: `bytes` / `file_count` are properties of a directory, while `row_count` is split by source; hanging file-level numbers on the finer grain would make them look additive, and adding them up would double count.

No `tier` / `layer` / `history_mode`: those live in `domain/datasets.py`, and a copy written into data files would only go stale.

Parquet rather than a duckdb file: writes are "temporary file + atomic rename", so readers never block; a duckdb file needs an exclusive write lock, which would make `cne serve` and the nightly batch block each other.

### cne stats rebuild

| Option | Description |
|------|------|
| `--dataset` | Rebuild only these datasets (repeatable); **other datasets keep their existing rows** and are not deleted |
| `--if-stale` | Rebuild only when "the lake has changed", otherwise return as a no-op. Use this on a timer |
| `--json` | Output the result as JSON |

A full rebuild scans published data; the time taken varies with the number of partitions, number of files and storage performance. The statistics tables are not
the data itself; a rebuild only updates the summaries under `meta/stats/`.

**The criterion for `--if-stale` is the run id, not the clock.** What changes the lake is ingestion, so a table built after the last run is current no matter how old, and one built before it is stale no matter how new: comparing `latest_run_id` in `stats-latest.json` with the manifest's latest run is enough, reading only a small JSON file plus one SQLite row.

Concurrency converges through a non-blocking lock: when a dashboard request, cron and the nightly batch all want to rebuild at once, only one actually does it, and those that do not get the lock return immediately instead of queuing; blocking a web request behind a full scan is worse than seeing numbers that are one run old.

`--if-stale` judges the whole-lake watermark, so it cannot be combined with `--dataset`; the command raises an error rather than guessing one of the two.

Refresh strategy (`meta/stats` does not refresh itself):

```bash
# Safety net: run on a timer; a no-op when nothing has changed
cne stats rebuild --if-stale
```

The dashboard refreshes stale statistics on demand; scheduled jobs can use `--if-stale` to avoid repeated scans when nothing has changed.

> `cne stats refresh` has been merged into `cne stats rebuild --if-stale`; the old `--force` is simply the default behavior without `--if-stale`.

### cne stats show

| Option | Description |
|------|------|
| `--dataset` | Per-partition detail for a single dataset |
| `--by-source` | Show the source / data_version distribution instead |
| `--json` | Machine-readable output |

**Stale statistics tables are recomputed automatically first** (same rule as `cne stats rebuild --if-stale`, yielding when another recompute holds the lock); statistics tables needed by `--dataset` / `--by-source` that have not been generated yet are also generated first.

**With no stats tables and only the summary requested, curated is scanned directly** (formerly `cne catalog`): only dataset / files / rows, with no byte counts,
source distribution or per-partition detail, but a lake that has never built statistics tables should not need a build before it can answer "what is in it".

> `cne catalog` has been merged into this command's fallback path; `--json` is its original output.

## cne query

SQL queries against the local lake; `--dataset` and `--symbol` are used as a pair, and data is fetched only on a cache miss or with `--refresh`. `[on_demand].enabled = false` forbids on-demand queries; when the source is disabled, refreshing is not possible. Fetch errors exit non-zero, and cached content must be refreshed explicitly to be updated.

With `--dataset X --symbol CODE`, it fetches on demand and reads the local cache; adding `--refresh` forces a refetch and overwrites the corresponding cache variant. `--dataset` and `--symbol` must appear as a pair, otherwise the command raises an explicit error; a DuckDB SQL query runs only when neither is given.

**DuckDB mode** (default):

| Option | Default |
|------|------|
| `--sql` | `SELECT COUNT(*) AS n FROM daily_bars` |

**On-demand mode**:

| Option | Description |
|------|------|
| `--dataset` | On-demand dataset name |
| `--symbol` | For example `600519.SH` |

## cne mcp

Connect this lake to AI agents (MCP, stdio by default, `--http` switches to Streamable HTTP). Read-only, and it does not offer serve's storage deletion entry point.

| Option | Description |
|------|------|
| `--config` | Configuration file path; when omitted, the shared lookup order applies (the client launches the process from an unpredictable directory, and the per-user config is found from any of them). When given, **an absolute path is recommended** |
| `--live` | Fetch on the fly what is not in the lake, without writing it to disk. Supports only `resolve_symbol` and unadjusted daily bars; other tools refuse explicitly |
| `--http` | Switch to Streamable HTTP, listening on `/mcp`. For clients that only accept a remote URL (ChatGPT), used together with a tunnel |
| `--host` / `--port` | Listen address and port for `--http`, default `127.0.0.1:8788`. A non-loopback address must be paired with `--token` |
| `--token` | The token every request must carry under `--http`: `Authorization: Bearer`, the `/mcp/<token>` path, or `?token=`. Required when exposed publicly through a tunnel |

You do not type it by hand: the MCP client launches it and speaks JSON-RPC over the pipe. Choose among three paths depending on what you have:

```bash
cne init                                                  # no lake yet: build the whole-market core first
cne mcp --config /abs/path/cnequity.toml
cne mcp --config /abs/path/cnequity.toml --live
```

The `cne mcp ...` above is a standard MCP stdio server command, and Claude is just one kind of
client. Codex, Cline, Cursor, Windsurf, Gemini CLI, VS Code and other compatible clients all
use the same `command` / `args`; the clients' registration entry points differ, but the server does not need to change.
ChatGPT only accepts a public HTTPS URL; connect it with `--http --token` plus a tunnel. For each client's exact setup, see the
[MCP reference](mcp.md#connecting-clients).

`--live` is **off by default and never inferred automatically**: a user whose lake is broken must get `no parquet data` and go fix it, rather than quietly receiving a similar-looking answer from somewhere else. Each call is limited to 50 symbols / 800 days, and `symbols` must be given explicitly. Every response carries `origin: "lake" | "live"`.

The 6 tools (`describe_lake` / `resolve_symbol` / `query_bars` / `query_fundamentals` / `query_dataset` / `run_sql`), semantics returned with each response, and `run_sql` accepting only a single SELECT: see the [MCP reference](mcp.md).

## cne sources

Everything on the data-source side: `probe` can probe live, and `substitutes --probe` probes too; the other entry points read local state, policy or evidence.

| Subcommand | Description |
|--------|------|
| `probe` | Probe public data sources and write the report into the lake |
| `slo` | Aggregate the historical samples in `meta/source_health` into availability SLOs by probe/vantage, and write deduplicated incident payloads. `--window-days` (default 30), `--minimum-observations` (default 10), `--enforce` (exit 1 when a key source misses its target) |
| `resilience` | Compute source concentration, failure-domain blast radius and the independent-fallback gate for core datasets from the registry. `--out PATH` writes JSON, `--enforce` (exit 1 if any core table lacks an independent fallback) |
| `policy [SOURCE]` | Look up the source usage policy in `sources/SOURCES.yml`. Omitting SOURCE outputs everything; give SOURCE plus `--profile personal\|commercial\|cache\|redistribution` for a conservative judgment, where unknown permissions always fail closed (exit 1). `--redistribution` is short for `--profile redistribution` |
| `limits` | Offline display of the shared cooldown for the same egress, the EM daily budget and circuit breakers, request/retry telemetry from this lake's recent runs, and resume commands for outstanding debt; it does not create a lake or probe sources |
| `substitutes` | Suggest independent fallback sources from saved probe evidence; probes actively only with `--probe` |

> Formerly `cne sources` (probing) + `cne source <sub>` (derived conclusions): two top-level entries one letter apart,
> with `cne source --help` forced to spend a sentence telling itself apart from its neighbor. Now they converge into one noun.

### cne sources probe

`cne sources probe --list` lists valid source names offline, with no configuration needed. `--only` rejects empty values and unknown source names; probing shares the ingestion rate limits and cooldowns.

Probes the public data sources this lake depends on. Each source gets a minimal probe; handshakes, pagination or downloads may produce multiple requests. Probes assert on the response body, run serially and respect each source's rate limit.

| Option | Description |
|------|------|
| `--config` | Configuration file path (probing does not read the lake, but uses the rate limits and timeouts in it) |
| `--vantage` | Which egress this probe is sent from: `cn` / `overseas` / any label (default `local`) |
| `--only` | Comma-separated probe keys, default all; an empty string or unknown name is a usage error |
| `--stale-only` | Skip active probing if the same egress and probe have verified real ingestion evidence within 12 hours; the daily job uses this mode |
| `--out` | JSON report path. Defaults to `meta/source_health/<vantage>.json` in the lake, which is where `cne serve` reads |

```bash
cne sources probe --vantage cn
cne serve                    # → http://127.0.0.1:8787/source-health
```

**Probing happens in the CLI, display in serve.** The health page only shows existing reports and will not go request a dozen third-party hosts for you, for the same reason it does not trigger ingestion. Multiple probes (different `--vantage`) are shown side by side, not merged.

**`--vantage` labels the actual egress.** Reachability is affected by routing, credentials, service status and request history, and cannot be judged by region alone. Labels allow letters, digits, dots, underscores and hyphens, must start with a letter or digit, and are at most 64 characters; a report represents only that one observation.

**A dead source does not affect the exit code.** A source turning red is this command's output, not its failure.

Five states: `ok` available · `empty` empty response · `blocked` refused · `down` unreachable · `skipped` not probed. `empty` gets its own state because it looks healthier than a failure but is actually more dangerous (backfills get silently truncated).

For the semantics and how to add a new source, see [Source health](../operations/source-health.md).

### cne sources substitutes

**Which source is down, and who else can step in.** `probe` answers "who is alive"; this one answers "which of the living can do the dead one's job".

| Option | Description |
|------|------|
| `--config` | Configuration file path |
| `--vantage` | Which egress's report to read (default `local`) |
| `--probe` / `--no-probe` | Measure live instead of reading the archive; same request volume as `cne sources probe`. **Default `--no-probe`**: this command's job is to interpret existing evidence, not to fire another round of requests |
| `--json` | Machine-readable output |

```bash
cne sources substitutes         # read meta/source_health/local.json
cne sources substitutes --probe # measure live
```

Substitutes are ranked **independent first, then fast**: an endpoint behind the same risk-control surface as the failed source is not a second opinion; when EastMoney's history host is down, EastMoney's snapshot host cannot stand in for it. The exit code is 1 when, for some dataset, "a failed endpoint exists and no endpoint is available".

Format example (assuming EastMoney `push2his` is unreachable; timings are illustrative). The CLI prints this in Chinese: the header line means "has an independent substitute", `失败` is "failed", `可用` is "available", `独立` is "independent", and `同域 eastmoney` is "same domain as eastmoney":

```
daily_bars  —  有独立替代
    失败：eastmoney_push2his
    可用：sina                    1970ms  独立
    可用：ths_kline               2347ms  独立
    可用：baostock                5117ms  独立
    可用：eastmoney_push2         2807ms  同域 eastmoney
```

This command only reads reports, and **the ingestion path does not read it**: reachability measured from some egress an hour ago is not a promise about the next request, so failover is still done by the ingestion chain itself, trying source after source.

## cne snapshot

Copies selected datasets into an immutable, checksummed portable snapshot, for freezing the Parquet that reproducible experiments depend on.

| Subcommand | Description |
|--------|------|
| `create NAME --dataset D [--dataset D ...]` | Create a snapshot; the manifest pins each Parquet file's size/SHA-256, dataset state, contract fingerprint and run lineage |
| `verify NAME` | Verify size and hash file by file; exits 1 on failure |
| `restore NAME TARGET` | Restore into a new or empty directory (refuses the active lake root, never overwrites existing files). After restoring, run `cne status --datasets` on TARGET before switching |
| `export NAME [DEST]` | Pack into one portable tar archive. `--compression auto` uses `tar.zst` when zstd is available, otherwise falls back to `tar.gz`; writes a `.part` in the same directory first and renames atomically only after the compressor finishes cleanly |
| `import ARCHIVE` | Verify first, then land: each tar member is checked to reject absolute paths, `..`, duplicate names, soft and hard links and device nodes; the extracted directory is fully verified against the manifest first, then published by an atomic directory rename. `--name` overrides the snapshot name (default from the archive file name), `--overwrite` replaces a snapshot of the same name only after verification passes |

`--config` / `--snapshot-root` are common to all subcommands; the default root is `meta/snapshots`.

### Delta packages `cne snapshot delta` {#增量包-cne-snapshot-delta}

A whole-lake snapshot suits freezing experiment dependencies; **for routinely syncing an existing lake, use delta packages**: they move only changed files,
and carry enough preconditions that "applied to the wrong baseline" becomes a failure rather than silent contamination.

| Subcommand | Description |
|--------|------|
| `delta create NAME --from A --to B` | Compare two **data roots** (not `curated` roots) byte by byte into an immutable add/replace/delete package. `--to` defaults to the active lake of the current configuration; `--dataset` is repeatable, and when omitted the datasets common to both roots are used |
| `delta create NAME --from-revision N` | Use a committed revision number as the precondition. A revision receipt records the changed files, not a copy of the old lake, so this mode emits `replace` with `allow_missing`; when you need strict old-file hash preconditions, use the two-root mode above |
| `delta verify NAME` | Verify every add/replace payload hash and the semantics of every changed path |
| `delta apply NAME TARGET` | Apply to a **non-empty** TARGET lake root. `--dry-run` only verifies preconditions without writing |


apply's safety boundary deserves its own mention: each add/replace/delete is checked against the baseline fingerprint (for revision deltas, against
each dataset's revision), writes go through temporary files in the same directory, and every overwritten file keeps a backup until both the whole-package change and
the post-apply target fingerprint pass; an exception midway rolls back what has been done, entry by entry, so the caller
never observes a known half-finished state. Writable paths are also narrowed to an allowlist under `curated/`, `derived/` and
`meta/`, so a delta package cannot use this to write arbitrary files in the target root.

## cne ths-official

Cross-check against and backfill from the **official THS API** (key required). **Without a key, every command here reports `skipped` and changes nothing**: the lake keeps the sources it already has.

Optional keyed sources do not serve as the default primary source: `ths_official` can be a `backup_source` / `backfill_source` and can go into failover and audit configuration, but it is never a `primary_source`.

**Verification and rewriting are two switches.** `[sources.ths_official].verify` is on by default and only allows writing `meta/source_snapshots` and findings; `[sources.ths_official].backfill` is off by default and must be turned on explicitly before curated is changed. Holding credentials, enabling the source, and allowing it to change data are three decisions, not one.

| Subcommand | Description |
|--------|------|
| `capture` | Capture counterpart-source snapshots to feed the arbitration checks in `cne audit`. **Never writes curated rows**. `--what corporate-actions\|daily-bars\|financials\|valuations\|all` (default all), `--days` (bar window, default 45 days), `--sample` (number of symbols sampled for bars, default 400) |
| `backfill` | Fill balance-sheet and cash-flow gaps for 2016–2024. Requires `backfill = true`. `--start` / `--end` (default 2016-01-01 ~ 2024-12-31), `--chunk-size` (default 200), `--workers` (default 4) |
| `repair-bars` | Switch deep history from credential-free scraping to the authorized API. **Reports only by default; writes only with `--apply`**. `--adjudicator FILE` passes an independent source's (symbol, trade_date, close) parquet, `--diff-out FILE` writes the disputed rows, `--start` / `--end` (default 2005-01-01 ~ 2015-12-31) |
| `resource-sectors` | Switch `sector_bars` from scraping to the authorized endpoint. Likewise writes only with `--apply`, and requires `backfill = true`. `--start` (service floor 2022-01-04), `--end`, `--workers` |

**`capture` is not optional.** `adj_factor_arbitration` and `daily_bars_arbitration` in `cne audit` both read the snapshot store; if it has never run, these two checks stay **silent forever**, and the lakes that most need a second opinion get none at all. The daily update's `finalize` wave contains the `ths_official_snapshot` step, which skips itself when there is no key.

**`repair-bars` and `resource-sectors` change where the data comes from.** They rehearse by default and write only with an explicit `--apply`. `repair-bars` can be paired with an independent `--adjudicator` to check disputes; two candidates from the same source cannot be treated as independent arbitration.

`backfill`, `resource-sectors --apply` and `repair-bars --apply` write to staging and then compact and publish automatically; the `compact` field in the result is the publication result.

For configuration, usage and failure handling, see [THS integration](../getting-started/configuration.md#ths-official-api).

## cne verify --runs

(The third mode of `cne verify`, options above; formerly `cne stability`.)

Takes the most recent window from the authoritative `trading_calendar`, gathers every `daily:*` run per trading day, takes the newest receipt per dataset, judges only the core datasets, and verifies run evidence over consecutive trading days.

| Option | Description |
|------|------|
| `--days` | Number of consecutive trading days that must pass (default 20) |
| `--as-of` | `YYYY-MM-DD` cutoff, inclusive of that day |
| `--enforce` | Exit 1 if the gate does not pass |

A missing run, a failed core stage, or only an old `warning` with no dataset receipt all count as failures. The report is written to
`meta/stability/latest.json` and an immutable history directory. The daily-update script runs it every day but without `--enforce`;
release governance enforces on day 20.

## cne --version

Package version number.

## Exit code summary

| Code | Situation |
|----|------|
| 0 | Success, skipped non-trading day, normal source-limited fetch final state (including partial or 0 rows), read-only report read successfully, health check passed |
| 1 | Real execution error, health check failed, validation failed, or explicit gate not passed |
| 2 | Run degraded under explicit `status --gate`; coverage cannot be proven under `status --datasets --gate` (evidence unreadable or `instruments` missing), kept separate from "proven incomplete" (1) |

## Related documentation

- [Quickstart](../getting-started/quickstart.md)
- [Product boundaries](../architecture/overview.md)

## Research input snapshots and pre-publication audit

```bash
cne snapshot create research-baseline --dataset daily_bars --research
cne snapshot verify research-baseline
```

`--research` includes prices, hfq factors, security identity, trading status and the calendar, and raises an error if any dependency is missing. The snapshot manifest also verifies ST/delisting coverage evidence, the directory, the calendar seed and the non-sensitive read configuration, without including API tokens. After restoring into a separate directory, run strict queries to verify coverage, save the query parameters, and use the recorded software version.

```toml
[quality]
publication_gate = "block"  # off (default), shadow, block
```

This is the pre-publication candidate gate: it compares, offline, the full audit results of the committed lake and the whole candidate batch. `block` rejects new or worsened errors (the same problem is identified by check, dataset and scope) or audit anomalies, quarantining the candidate and keeping pointers and watermarks; `shadow` only records, suitable for first measuring false positives and scan cost. Reports are in `meta/quality/publication/`. It is configured separately from the post-publication `audit_gate`; for what the setting means, see [Product boundaries](../architecture/overview.md).
