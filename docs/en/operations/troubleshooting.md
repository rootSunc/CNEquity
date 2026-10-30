# Troubleshooting

Locate the problem by symptom: for installation/path problems, start with `doctor` and the configuration; for collection failures, start with the run and its batches; when rate-limited, start with `sources limits` and resume only after the cooldown ends. After a fix, verify according to the affected scope.

For first-time use, see [Quickstart](../getting-started/quickstart.md); for request cost and protection mechanisms, see [Fetch policy](fetch-policy.md).

## Symptom: CNI historical adjustment file does not exist

When the publisher returns “样本历史调样文件不存在”, the client raises `CNI publisher does not serve adjustment history`. The historical file is unavailable; this is not a valid empty membership list. Retain existing history and failure records. Recovery requires complete official historical membership books or a verified chain of adjustment announcements. Current constituents and weights cannot replace historical constituents and weights or justify advancing historical coverage watermarks.

## Symptom: EastMoney datacenter `code=9501` / `列不存在`

| Observation | Cause | Fix |
|------|------|------|
| `EastMoney datacenter RPT_… rejected schema: XXX列不存在 (code=9501)` ("column XXX does not exist") | The columns returned by the source do not match the current client contract | First stop the affected group and keep the report name and error code; check whether a fixed release exists, or file a redacted Issue. Do not treat the failure as empty data |
| The whole daily-update group fails, with a report name in the error | The same column-contract error blocks publishing for that group | After updating to the fixed release, retry the failed run and recheck the watermark and the actual columns; do not push the watermark forward by repeating requests or skipping validation |

Maintainers must reconcile the semantics of the old and new columns in an isolated environment before updating the adapter and contract tests; users do not need to patch the installed package by hand.

## Symptom: baostock / free source "blacklist" or frequent failures

| Observation | Cause | Fix |
|------|------|------|
| `baostock` blacklist (`10001011`) or "今日请求已达 50000 次上限" ("today's requests have reached the 50,000 limit") | The daily API request limit is 50,000, and concurrent connections are not allowed. Each blacklisting within the current calendar year freezes access for `count × 6` hours; when the response gives no release time, there is no refresh for at least 5 minutes | When the client exceeds the limit it stops locally and cools down for the duration above; if a connection already exists it queues for at most 10 minutes, and if it is still not released it does not log in. Symbols already fetched are kept. Do not switch IPs or rerun before the cooldown ends; afterwards, rerun the same command at the default rate and it continues from the symbols not yet completed |
| EastMoney 429 / Empty reply / connection dropped | 429 means rate limiting; disconnects can also come from a proxy, the network or a service fault | Pause the affected source and respect the shared cooldown and host circuit breaker; to disable push2, set `push2_paused = true` — see "[EastMoney 502](#eastmoney-502-connection-reset)" below |
| Intermittent cninfo / pboc failures | Same-source risk control / site instability | `rate_limit` is already applied per page / per call; the main total social financing write fetches the full series year by year, and a failure in any year blocks this macro write until the next complete retry |

Principle: **time can wait; the cost of a ban is far higher than waiting one more day.** Do not turn off `min_interval` to speed things up, or run multiple processes against the same free source.

Valuation history backfill also has a **single-flight lock** (`RunLock("baostock")`): concurrent
`valuation_2001` / float_mv market scans are skipped outright and leave a `baostock_single_flight` warning.
Beyond that, the Baostock connection lease fails before a second `login()` and does not open another connection.

## Symptom: valuation_metrics watermark is "fresh" but coverage falls off a cliff / STALE

| Observation | Cause | Fix |
|------|------|------|
| Only a few symbols in the last few days, `valuation_bars_low_coverage` | The current-day valuation snapshot may be incomplete | Check the `capital` group and source status; the watermark gate refuses to treat a sparse partition as complete current-day coverage |
| `cne status --datasets` shows valuation STALE + insufficient coverage | The historical window or tip fetch may be incomplete | Check the gap dates and sources. The historical part uses backfillable sources; the current snapshot can only repair the current day, and you cannot label today's data as history via `--trade-date` |

```bash
# First check the historical repair range, registered sources and cost; fill in dates from the actual gap
cne backfill valuation_metrics --start 2026-09-01 --end 2026-09-04 --plan
# The current-day capital and valuation snapshot is collected by the daily group
cne run daily --group capital
cne status --datasets
```

The audit finding `valuation_watermark_coverage_gate` flags an inconsistency between the watermark and the current-day cross-section;
repair the actual gap and then check again — do not look only at the maximum date.

## Symptom: many zombie runs with status=running in the manifest

| Observation | Cause | Fix |
|------|------|------|
| `cne status` shows `orphaned_running_runs > 0` | The process was killed or hit OOM, leaving the run in `running` | Confirm the original process has exited; the next `cne run daily` / `cne run retry` reconciles and recovers orphaned runs |
| Need to clean up immediately | — | `cne run clean --reconcile-runs` (skips live runs that still hold a lock) |

Long tasks (baostock backfill) are kept alive by a **batch heartbeat** and are not killed by mistake merely because `started_at` is more than 1h ago.

### BrokenProcessPool / poisoned worker pool

| Observation | Cause | Fix |
|------|------|------|
| Log line `worker pool broke (likely OOM under load); retrying … serially` | One child process exited, making the worker pool unusable | Unfinished batches are retried serially; keep the logs to check for OOM and the ranges that still fail, and do not delete successful batches by hand |
| Frequent OOM / pool crashes on macOS | The TDX client is not fork-safe + `workers>1` | `cne config validate` **rejects** `workers>1` on Darwin; use `workers = 1` in production (see runbook / `daily_pipeline.sh`) |
| `import fcntl` / file lock fails on Windows | Old versions used the Unix-only `fcntl.flock` | Upgrade to a version that includes `cnequity.file_lock`; locks have the same semantics on Win/POSIX |
| DuckDB views empty / wrong paths on Windows | Backslashes ended up in the `read_parquet` SQL | New versions use `as_posix()`; confirm `data.root` is readable and writable, then rerun `cne init` / refresh the views |
| PowerShell `&&` syntax error on Windows | PS 5.1 does not support `&&` | Run the commands on separate lines, or use PowerShell 7+ / cmd |

## Symptom: load() does not see new data

| Possible cause | Check | Fix |
|----------|------|------|
| Data is still in staging | `ls staging/*/run_id=*` | `cne run compact` publishes all finished but unpublished runs, or use `cne run retry`; **compact first, then** `cne run clean` (no `--force`, otherwise after demotion the data can only be refetched) |
| Group run was not compacted | Do the group's steps include `compact`? | Fix the configuration and rerun the group |
| compact was skipped by a gate | Check failed batches in `cne status` | `cne run retry --run-id <id>` |
| Wrong path | `config.data_root` | `cne config path` shows the config in effect and its data root |
| Initialization is still running, and `load("daily_bars")` without `symbols` is empty | An unpublished full-market scan is not treated as published data | Specify symbols that are already sealed, e.g. `load("daily_bars", symbols=["600519.SH"])`; adjustment factors require `adj_factors` to be published |

## Symptom: daily_bars "interior symbol×session key(s) remain absent; refusing to checkpoint"

The core group fails every trading day, the `daily_bars` watermark does not advance, and the rows fetched that day stay in staging unpublished.

| Observation | Cause | Fix |
|------|------|------|
| `RuntimeError: daily_bars <start>..<end>: N interior symbol×session key(s) remain absent` | Some symbols in the window are missing an interior trading day, and neither the primary source nor any fallback source can fill it | First see which symbols are missing (below), then decide whether to narrow the scope or fix the source |
| Almost all missing codes start with 158/159/160/51x/52x/56x | These may be ETFs/LOFs rather than the default A-share scope | Check `[universe].ingest` against your actual research scope; the default `all_a` does not fetch ETFs/LOFs, and after explicitly widening the scope you must separately accept source coverage |
| The missing ones are real A-shares, and only today is missing | The fallback source's quota was used up by other symbols (log shows `circuit opened … leaving N symbol(s) unresolved` / `sina bars HTTP 456`) | Wait for the next round, or `cne run retry --run-id <id>` to reuse the batches already fetched |
| `expected key(s) remain unknown after failover`, and the log shows `EastMoney kline circuit opened` | The EastMoney history host (`push2his`) is unreachable from the current egress; one source returning empty is not enough to prove no bar is needed for that day | Run `cne sources limits` to check cooldowns, then use `cne sources substitutes` to check independent fallback sources; keep the unresolved keys and fill them in a targeted way once the source recovers. Do not treat an empty result as evidence of suspension |

Missing-key details are written to `meta/quality/findings/<run_id>.json`, with `missing_symbols` and `sample_keys`:

```bash
cne status                      # get the failed run_id
python - <<'EOF'
import json, sys
run = "RUN_ID"
d = json.load(open(f"data/cnequity/meta/quality/findings/{run}.json"))
for f in d["findings"]:
    if f["check"] == "daily_bars_interior_gap":
        print(f["missing_keys"], "keys /", len(f["missing_symbols"]), "symbols")
        print(f["sample_keys"])
EOF
```

`daily_bars` has a configured reconciliation lookback window, but reuse of existing coverage, explicit ranges and gap trimming affect the actual requests. Both single-day and multi-day paths exist; judge which gate is in effect from this run's plan, batch windows and findings, not from the command name.
Certification runs **before** the gate: symbols proven to have no data get negative evidence recorded (TTL in `[incremental]`),
and are not requested again within the TTL; the gate only errors on keys still missing after certification.

> Do not tune `min_interval_*` or loosen circuit-breaker thresholds to get around it — this error means the gap is real;
> speed comes from narrowing the fetch scope (`[universe].ingest`), not from faster requests.

## Symptom: cne run daily fails

1. `cne status` to view `run_summary` and failed batches
2. Check `error_message` in the logs (TDX disconnect, HTTP 429, schema validation failure, etc.)
3. `cne run retry --run-id <id>`
4. For TDX problems: `cne sources probe --only tdx_protocol`; change `[tdx_protocol.hosts].standard`
5. If a single dataset keeps failing: `cne backfill <dataset>` (requires backfill support)

### daily_bars: TDX batch fails but the tip still has data

- **Observation**: log lines `daily_bars_clist_gapfill` / `routed … through EastMoney clist`; some rows have `source=eastmoney`.
- **Cause**: when the TDX primary source partly or fully fails, on the tip day the **missing keys** go through EastMoney push2 clist (full-market pagination), while multi-day windows use per-symbol kline. This is missing-key gap filling, not a silent switch of the primary source.
- **Fix**: continue if acceptable; if you need a pure-TDX tip, fix TDX and rerun the day with `cne run daily --group core --trade-date …` (the canonical merge of ordinary bars picks rows by fetch time and source tie-break rules, and publishes a revision for business changes).
- **Multi-day backfill** failures still go through kline gap-fill (slow); clist **cannot** fabricate history.

### Watermark behind after a weekend / missed run

- **Observation**: when today is not a trading day, `cne run daily` → `skipped_non_trading_day`; the `daily_bars`
  watermark stays at the trading day before last; downstream freshness gates fail.
- **Fix** (catch up core + `market_breadth`):

  ```bash
  uv run python scripts/run_catchup.py                      # default: the most recent trading day
  uv run python scripts/run_catchup.py --trade-date 2026-07-17
  # From a mainland egress, also catch up capital/research (EastMoney failures do not block the gate, exit 0):
  uv run python scripts/run_catchup.py --trade-date 2026-07-17 --all-groups
  # Or step by step:
  uv run cne run daily --group core --trade-date 2026-07-17
  ```

  To catch up all groups: `scripts/scheduler/daily_pipeline.sh 2026-07-17` (or `CNE_TRADE_DATE=...`).
  **Do not** casually add `--backfill` for a missed day: the EastMoney CA full scan often fails outright from overseas.
  Choose the schedule groups to run based on reachability from the current egress; data that was not collected successfully keeps its gap, and lag cannot be marked normal on the basis of geography alone.

### EastMoney 502 / connection reset (egress IP banned) {#eastmoney-502-connection-reset}

- **Observation**: in `cne sources probe`, `eastmoney_push2` reports `HTTP 502` and `eastmoney_push2his`
  reports `Connection closed abruptly` / Empty reply; the daily core update is fine (bars come from TDX), while the capital / sector groups fail.
- **Diagnosis**: 502s and connection resets can come from routing, a proxy, a source fault or access control; results from a single egress cannot prove a whole region is blocked, and a status code alone cannot prove an IP blacklist.
- **Fix**: stop repeated requests first. `[sources.eastmoney] push2_paused = true` or `CNE_PUSH2_PAUSED=1` pauses push2-family requests locally; other sources can continue under the existing configuration.
- **Recovery**: first read the cooldown deadline and budget with `cne sources limits`; once the shared cooldown and circuit breaker have expired, lift the explicit pause and run `cne sources probe --only eastmoney_push2` once. The probe respects the collection safeguards; do not bypass the budget or delete the shared ledger.
- **Preventing recurrence**: keep push2 snapshot reuse, the 4-second interval, one request in flight, the daily budget and the circuit breaker; narrow the backfill range on failure. If `Retry-After` is longer, keep waiting. The defaults are not a server-side commitment; the actual request count depends on that day's scope and pagination.
- **Multiple lakes**: on the same egress, share `CNE_RATE_LIMIT_ROOT` and schedule centrally; the EM daily budget and circuit breaker are also coordinated through that directory. See [Fetching and source protection](fetch-policy.md).

A proxy is network configuration, not a way to lift a ban. Do not rotate egress to continue a fetch that has already been refused; check the availability and semantics of independent data sources separately.

### sector_bars backfill fails heavily

- **Observation**: log lines `THS sweep: BKxxxx (板块名) failed: ...` (板块名 = sector name); a high `failed_sectors` count.
- **Cause**: **this is a THS source, not EastMoney** — `[sources.eastmoney] proxy` has no effect on it.
  `d.10jqka.com.cn` rate-limits or even bans dense requests.
- **Fix**: after the cooldown, raise `[sources.ths] min_interval_seconds` (default 1.0, not a guaranteed safe threshold), then `cne backfill sector_bars`;
  it resumes from the checkpoint by default; after switching sources, use `--refetch` for a full refetch. Checkpoint: `meta/state/sector_bars_backfill.json`.
  When the failure rate exceeds 50% the step status is `warning`, and sectors that succeeded are still written.

### Deploying on another machine

First set up an independent environment and personal configuration following the installation guide, then confirm availability on a small scale with `sources probe --only <key>`. An HTTP proxy may not carry Baostock/TDX TCP protocols; do not guess whether a source is available based on geography.

When you need historical backfill, use `cne backfill sector_bars` or `cne backfill trading_status --symbols <code>` directly; this keeps the existing checkpoint and does not automatically force a refetch of the whole lake. Move data with the reviewed snapshot workflow; do not copy the whole working directory, with its personal credentials, logs and data, as an installation package.

## Symptom: audit --full UNHEALTHY

| Finding type | Meaning | Fix |
|--------------|------|------|
| `pk_unique` | Duplicate PK; `--full` checks the full history | Check the most recent compact; if necessary, backfill and rerun that partition |
| `mixed_partition_granularity` | Fine-grained partitions still sit on disk on top of year/month partitions, duplicating the same PK across granularities | Prefer `cne repair repartition <dataset> --apply` (or `--all`) to rewrite atomically according to `DatasetSpec`; only if the tool cannot run, move the fine-grained directories to `_quarantine/`. Historical derivation of `trading_status` must go through `partition_for` (monthly partitions); do not write daily directories again |
| `mock_source` | Mock data in production | Turn off `allow_mock`; clear the mock partitions and recollect |
| `adj_close_discontinuity` | Abnormal adjusted returns | `cne derive adj_factors`; check the Sina source |
| `missing_corporate_action` | No corp action on an ex-date | `cne backfill corporate_actions` |
| `trading_status_coverage_start` | ST coverage starts late | Insufficient evidence for the research window; backfill by market and check the ST receipts — do not just ignore the warning |
| `partition_row_count_mutation` | Sudden change in row count | Check for a mistaken compact or a change in source semantics |
| `unregistered_curated_dir` | Unregistered directory under `curated/` (e.g. `*.bak*`) | `mv curated/<stray> {data_root}/backups/`; before deleting, confirm it is not live data moved there by mistake |

Findings file: `meta/quality/findings/{run_id}.json`

Contradiction checks between factors and corporate actions compare within each security's factor coverage interval: a flat factor is still checked for missed recorded events, and an empty corporate-action table is still checked for factor jumps. The first factor observation date has no prior value, so no missed step is inferred from it; fund cash dividends that cause no factor jump are counted separately as a semantics notice. Full audits and local repair checks apply the same rules, so contradictions that appear after an upgrade may be previously missed reports now surfaced; check the specific security and event dates first.

Peer-source corporate actions are also matched on each security's effective trading days, while exact-date backfill hints still use the original ex-date recorded by the peer source. When an event is absent from the snapshot and the collection date precedes the event, the report counts it as unarbitrated (`peer_snapshot_uncovered`); this cannot be taken as proof that the event does not exist. Events the snapshot explicitly contains can still be confirmed.

Financial-statement comparisons match on security, reporting period, statement type and line item, with both sides selecting the latest version by disclosure date and observation time. Zero is also a comparable value: both sides zero counts as agreement, zero versus non-zero counts as a difference.

## Symptom: status --datasets STALE

1. Confirm whether the pipeline ran on the most recent trading day
2. Find the dataset's most recent successful run: `cne status`
3. Rerun the corresponding group: `cne run daily --group <name>`
4. Quarterly datasets (`northbound_holdings`) tolerate 100 days — not a fault

`is_stale()` logic: `domain/datasets.py`

## Symptom: init interrupted

Keep the data and configuration and rerun the same `cne init`: it finds the unfinished run and resumes it, and batches that already succeeded are not refetched. Treat `warning` / `degraded` in the result the same way; there is no need to split it into separate gap-filling commands. Use `cne init --run-id INIT_RUN_ID` only when you need to target a specific older run.

`--keep-going`: continue with later phases after a single phase fails (to backfill as much as possible).

## Symptom: RunLockError

First use `cne status` and process information to confirm who holds the lock. An active `retry` / `compact` must be waited for or stopped normally; the operating system releases the lock when the process exits, and a lock file left in `meta/locks/` does not mean the lock is still held. Do not delete lock files to get around an active write.

The scheduler scripts and the operations page share a different lock, whose directory is `{data.root}/locks` (`CNE_SCHEDULER_LOCK_DIR` or `CNE_LOCK_DIR` can move it elsewhere). While a task from the dashboard is still running, the scheduled daily update waits and decides again on the next round.

## Symptom: the browser cannot open the dashboard

| Observation | Cause | Fix |
|------|------|------|
| `localhost:8787` cannot connect, but `127.0.0.1:8787` works | Only IPv4 is listened on, and macOS resolves `localhost` to `::1` first | Use the current version of `cne serve`: the loopback address accepts both `127.0.0.1` and `localhost`, and the startup message prints only localhost |
| The page stays at "正在打开面板" ("opening dashboard") | The first-screen API has not returned yet | Wait for the overview to appear; if it never does, check the errors in the serve terminal and confirm you opened the address that was printed |

## Symptom: low disk space / staging bloat

1. `cne run compact`: automatically finds and publishes all finished runs that have staging data and have not been successfully compacted
2. `cne run clean` to view candidates; it only previews and does not delete. Backfill and the daily run keep the newest 5 generations per dataset ([`[storage.retention].keep_generations`](../getting-started/configuration.md#storage-retention)). Lower that number on a small disk, or set it to `0` to keep every generation. Older generations can also be confirmed for deletion on the serve storage page. `exclusive_bytes` in `cne storage inspect` is the space deleting that generation would actually release. When a publish does not fit, the command says how much space is short instead of only exiting 1. Staging is report-only for now
3. Incomplete / uncompacted failed runs are kept by default for `cne run retry`; use `--force` only when you have confirmed they can be discarded
4. Compress or archive old `meta/source_snapshots/` (it grows over time)
5. Do not delete curated data; recollect with backfill rather than deleting parts of it

## Symptom: many missing adjustment factors

```bash
cne derive adj_factors
cne audit --full
```

With `strict_adj=True`, missing factors raise an error — check the coverage of `derived/adj_factors` and `adj_factors_cache`.

## Diagnostic command cheat sheet

```bash
cne config validate
cne sources probe --only tdx_protocol
cne status
cne status --datasets
cne stats show --json
cne audit --full
cne run retry --run-id RUN_ID
cne run clean --dry-run
```

## Related documentation

- [Operations runbook](runbook.md)
- [Data flow](../architecture/data-flow.md)
- [Initialization and resume](../getting-started/initialization.md)
