# Network and write inventory

Checked against the Click command registry by `scripts/dev/sync_docs.py`. “When run” means when the command body executes; `--help` executes nothing. For source selection and limits, see [Fetching and source protection](../operations/fetch-policy.md).

| Command | Third-party fetching | Local effects |
|---|---|---|
| `cne audit` | conditional | Reads the lake and prints the audit; accesses sources when external comparison is enabled |
| `cne backfill` | when run | Backfills history, writing staging and published data; --plan only reads config and state |
| `cne check` | none | Reads status, freshness, audit results and stats; recomputes stale stats, --full reruns the full-lake audit |
| `cne config` | none | create writes a personal config that extends the built-in defaults; upgrade backs up and adds schedule steps, --minimize backs up and rewrites it to extend the defaults; validate/diff/show/path are read-only |
| `cne contract diff` | none | Reads contracts and prints the diff |
| `cne contract show` | none | Reads the contract; writes a file when an output path is given |
| `cne contract validate` | none | Validates the contract |
| `cne decision-data cash-rights` | none | Reads the lake and prints decision data |
| `cne decision-data payment-gaps` | none | Reads the lake and prints decision data |
| `cne decision-data stock-terms` | none | Reads the lake and prints decision data |
| `cne delisted status` | none | Reads delisting status |
| `cne derive` | conditional | Writes derived data; modes such as adj_factors may access sources |
| `cne doctor` | none | Checks config and environment offline |
| `cne import shfe-archive` | none | Read-only preview by default; --apply verifies a local SHFE annual package and writes its partial-field daily rows without overwriting existing keys, keeping the package as evidence |
| `cne init` | conditional | sample is offline; demo/quick/full fetch data; layout-only creates directories |
| `cne mcp` | conditional | Reads the lake by default; --live may fetch source data; --http listens on a local port |
| `cne migrate` | conditional | Read-only preview by default; --apply runs the owed migrations in order, publishes through the repair gate keeping the old version, and writes the results to meta/state/lake.json; fund-listing-dates reads the exchanges' fund lists, misdated-valuation reads the EastMoney datacenter when it has rows to replace |
| `cne profile list` | none | Lists built-in scopes |
| `cne profile show` | none | Shows a built-in scope |
| `cne query` | conditional | SQL reads the lake; fetches data on an on-demand cache miss or refresh |
| `cne repair adj-factors` | --refresh-sources only | Offline preview with a saved plan by default; --apply publishes supported corrections, retains old revisions and unresolved diagnostics, and saves bounded historical progress |
| `cne repair bar-amounts` | only with --apply | Read-only preview by default; --apply requests TDX to cross-check Sina-filled daily bars and fills turnover only, never prices or volumes |
| `cne repair bar-turnover` | only with --apply | Read-only preview by default; --apply requests Baostock (with --peer-review also exchange history, TDX and EastMoney) and replaces whole Shanghai/Shenzhen stock bars with bad turnover |
| `cne repair bse-tip-turnover` | only with --apply | Read-only preview by default; --apply requests the BSE quote board and fills turnover for the latest session only |
| `cne repair corporate-action-gaps` | with --apply | Offline preview by default; --apply queries Baostock and CNINFO per gap, publishes a new version after verification and keeps the old one |
| `cne repair delisted-actions` | only with --apply | Read-only preview by default; --apply fills delisted securities' historical dividends and ex-rights from Baostock (Shanghai/Shenzhen) or THS (Beijing) and writes the lake |
| `cne repair ex-dates` | only with --apply | Read-only preview by default; --apply requests historical ex-rights rows from EastMoney day by day for the given ex-dates and writes the lake |
| `cne repair financial-originals` | --refresh-sources only | Replays original periodic reports offline by default; --apply appends identifiable current-period consolidated figures, publishes through the repair gate and retains existing vintages and honest observation times |
| `cne repair fund-no-print` | --refresh-sources only | Offline preview by default; source refresh archives reports and updates caches; --apply approves explicit official fund auction-bar coverage evidence, retires corresponding debts and refreshes watermarks without creating bars or halt rows |
| `cne repair fund-volume` | only with --refresh-sources | Offline preview by default; online refresh archives historical K-lines from the SSE quote service and reuses or collects EastMoney evidence when turnover conflicts; --apply repairs verified Shanghai and Shenzhen fund volume, passes publication checks and retains old revisions |
| `cne repair index-history` | --refresh-sources only | Replays the reviewed official attachment catalog for the selected SZSE Component or STAR 50 index offline by default; --apply replaces whole books on effective dates, retaining official dated weights and old revisions; unknown historical weights remain null |
| `cne repair index-snapshots` | --refresh-sources only | Replays official files offline by default; --apply replaces complete dated constituent snapshots and real weights, publishes through the repair gate and retains old revisions |
| `cne repair layout` | none | Preview only by default; --apply publishes a new version, keeping the old version and duplicate-observation evidence |
| `cne repair payment-dates` | only with --apply | Read-only preview by default; --apply repairs cash payment dates from reviewed issuer announcements (CNINFO, BSE) and Baostock, published through the backfill gate |
| `cne repair repartition` | none | Read-only listing or preview by default; --apply rewrites partitions at the registered granularity under the lake lock and swaps them in |
| `cne repair status-traded` | none | Read-only preview by default; --apply fills missing status keys from committed positive-volume bars, leaves ST unknown, publishes a new version and keeps the old one |
| `cne repair stock-listing-dates` | --refresh-sources only | Replays IPO calendars offline by default; --apply fills stock listing dates confirmed by official listed events, publishes through the repair gate and retains old revisions |
| `cne repair valuation-outage` | only with --apply | Read-only preview by default; --apply requests the EastMoney datacenter valuation report for the outage window and writes only when the whole market is retrieved |
| `cne run clean` | none | Only previews expired files; an explicit --reconcile-runs may modify run state |
| `cne run compact` | none | Publishes finished, unpublished staging data to the lake |
| `cne run daily` | when run | Fetches incrementally and writes the lake; includes all schedule groups and event streams by default |
| `cne run events` | when run | Fetches events and writes the lake |
| `cne run maintain` | when run | Post-update maintenance: audit, freshness gate, source probes and SLO, run-evidence check (read-only); writes a metadata backup and rotates old ones by retention; expired files are only previewed |
| `cne run retry` | when run | Retries failed scopes and writes the lake |
| `cne schedule install` | none | After confirmation, registers the current user's system timer (launchd/cron/Task Scheduler) and writes schedule settings; keeps the backup and event schedules set on the dashboard; --events runs event streams on the config's [[job.events.schedules]] |
| `cne schedule remove` | none | After confirmation, pauses scheduling and unregisters the system task; jobs already running continue |
| `cne schedule status` | none | Read-only: the system timer's state and its last check |
| `cne serve` | conditional | Starts the lake dashboard. On a loopback address the operations page can start allowlisted fetches and daily updates; --read-only disables write endpoints; remote fetches require --allow-remote-ops; storage cleanup still needs web confirmation; fetch toggles are written back to the config, with a backup, after preview confirmation and do not start a fetch |
| `cne snapshot create` | none | Creates a local snapshot |
| `cne snapshot delta apply` | none | Applies a local delta package; --dry-run only validates |
| `cne snapshot delta create` | none | Creates a local delta package |
| `cne snapshot delta verify` | none | Verifies a local delta package |
| `cne snapshot export` | none | Exports a local snapshot |
| `cne snapshot import` | none | Imports a local snapshot |
| `cne snapshot restore` | none | Restores a local snapshot to the target directory |
| `cne snapshot verify` | none | Verifies a local snapshot |
| `cne sources limits` | none | Reads egress budgets, cooldowns and local debt; does not probe sources |
| `cne sources policy` | none | Reads source policies |
| `cne sources probe` | conditional | --list is offline; probing accesses sources and writes a report |
| `cne sources resilience` | none | Reads registered sources and probe evidence; may write a report |
| `cne sources slo` | none | Reads probe history and writes stats |
| `cne sources substitutes` | conditional | Reads reports by default; --probe accesses sources |
| `cne stats rebuild` | none | Rebuilds local stats |
| `cne stats show` | none | Reads local stats; recomputes automatically when stale or when a detail view needs it |
| `cne status` | none | Reads run state and dataset coverage |
| `cne storage apply` | none | Rechecks the version plan and marks; CLI purge is disabled, deletion must be confirmed in the serve web UI |
| `cne storage explain` | none | Reads the retention reasons for versions |
| `cne storage hold` | none | Adds object protection and clears pending-deletion marks |
| `cne storage import` | none | Validates and registers a reference manifest; only adds protection |
| `cne storage inspect` | none | Reads versions and retention reasons |
| `cne storage plan` | none | Validates retention dependencies and saves a version plan; deletes no data |
| `cne storage raw-external` | none | Registers external payload storage; retains local source sidecars and does not move or delete files |
| `cne ths-official backfill` | when run | Calls the credentialed source and fills gaps |
| `cne ths-official capture` | when run | Calls the credentialed source and writes comparison evidence |
| `cne ths-official repair-bars` | when run | Fetches even in a dry run; --apply writes repairs |
| `cne ths-official resource-sectors` | when run | Fetches even in a dry run; --apply writes staging |
| `cne verify` | conditional | Local validation by default; --repair accesses sources and repairs |
