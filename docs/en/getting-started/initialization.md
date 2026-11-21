# Initialization, scope and resume

Use this page to build the production lake, widen the history window or handle interruptions. If this is your first time, start with the [Quickstart](quickstart.md).

## Before initialization

```bash
cne init
```

When no config is found (no `configs/cnequity.toml` in the current directory and no per-user `~/.config/cnequity/config.toml`), `cne init` first generates the per-user config from the bundled example (the same as `cne config create`: writes an absolute `data.root`, defaults to `workers=1` on macOS / Windows), then starts initialization. To change settings such as the data directory first, run `cne config create --data-root /path/to/lake`, edit it, then run `cne init`. If an explicitly passed `--config` or `CNE_CONFIG` points to a file that does not exist, nothing is generated automatically, so a typo does not create a new lake. After upgrading with an existing config, run `cne config upgrade` to add new schedule steps.

If you cannot reach the data sources, run `cne doctor` (offline checkup) first, then see the [troubleshooting guide](../operations/troubleshooting.md).

Before it starts, `cne init` (quick / full) spends at most 8 seconds on a network preflight using the same probes as `cne sources probe`: if TDX is unreachable it stops at once and says why, instead of retrying for minutes inside the first step; the exchanges and EastMoney are only reported, and when they are down the steps that use them degrade without stopping the build. Offline, or when you know the network is fine, add `--no-preflight` or set `CNE_SKIP_PREFLIGHT=1`. A config with `[tdx_protocol] allow_mock = true` skips the check.

You can also do this without opening a terminal first: when the default config does not exist yet, `cne serve` enters first-time setup, where you fill in the data directory on a local page, review the checkup results, and then start the same `cne init` as above. Missing directories are created automatically. Initialization cannot start while the checkup is still running. The page first shows the command that will be run; review it, then click “开始初始化” (Start initialization). If the directory already contains a data lake, the page explains that it will take over that directory without clearing it, and gives the command only after you confirm. If the config has been generated but the initialization parameters are wrong or the task is temporarily locked, fix the parameters or wait for the lock to clear, then preview again. After changing the depth, start date or “continue after failure”, you also need to preview again. An initialization that is already running does not stop when you close the page. An unfinished initialization can be resumed directly from the operations page.

`init` follows `[job.init.phases]` to create directories, the manifest and views, and to backfill securities, the calendar, corporate actions, daily bars, indices, trading status and derived data. To build only the layout, use `cne init --layout-only`; it produces no research data.

## Init: scope, disk and resume {#init-scope}

The default `cne init` (`--profile quick`) is the core of the last 3 years for the whole Shanghai, Shenzhen and Beijing market. `--profile full` still covers the full market; it only deepens the core to each dataset's own history start, with daily bars from 2016-01-01.

| Command | Actual scope | Request cost profile |
|---|---|---|
| `cne init` (`--profile quick`) | Full Shanghai, Shenzhen and Beijing market × last 3 years; core such as securities, calendar, daily bars, trading status | Full market, paged by security and gap |
| `cne init --profile full` | Still the full market; the core is fetched from each dataset's default start, daily bars from 2016-01-01 | Deep history, more pages and more data on disk |
| `cne init --since 2018-01-01` | Full market, from the given date | In between the two |
| `cne backfill trading_status` | Fills in historical ST evidence for the full market | Runs per security at the source's pace; schedule a long window |

TDX / Baostock reachability, egress, upstream rate limiting, retries and machine configuration all change the run time; `init` prints its scope at startup; single-dataset backfills can additionally be reviewed offline with `cne backfill DATASET --plan`. `init` has no `--plan` parameter.

Disk usage grows with security scope, history start, frequency and revisions, and staging needs extra working space. Minute bars, 5-minute bars and trade ticks are off by default; full-market minute data is usually far larger than the daily core. Before widening the scope, measure on a small sample first; see [Runbook · intraday data](../operations/runbook.md#intraday-data-minute_bars--minute_bars_5m).

The default init fetches the current trading status and, after daily bars are published, derives historical suspensions within the window. **The full-market historical ST scan is not part of init and is not a completion condition for initialization**; when you need historical ST research, run `cne backfill trading_status` explicitly. That command has no per-round cap of 400 stocks; it keeps scanning at the source's pace and saves checkpoints. Coverage still depends on the market, source permissions and evidence receipts; Baostock does not provide historical ST for the BSE.

Initialization automatically recovers daily bars for known delisted stocks in the configured markets within the history window, verifies the coverage after publishing, and then continues fetching daily bars. When a source is missing, it keeps the valid recovery results and the ranges still to be filled, and continues with the subsequent phases it can run; program, storage or integrity errors still fail. Published history is reused in later backfills. This flow uses known delisted identities and catalogs; it does not perform discovery across the whole code space.

```bash
cne init --profile full --config configs/cnequity.toml

# Optional: when you need historical ST research, backfill it separately with an explicit window
cne backfill trading_status --start 2016-01-01 --end YYYY-MM-DD --config configs/cnequity.toml

# Then run once a day: all daily update groups + event streams
cne run daily --config configs/cnequity.toml

# To keep only market data and fundamentals: the initialization download is unchanged; the daily update runs only the matching schedule groups
cne init --pack market --pack fundamentals --schedule
cne check --pack market --pack fundamentals
```

`--pack` does not change what this initialization downloads. `market` is the market-data core; `fundamentals` is financial statements, share capital and the disclosure schedule (its daily update groups also bring index constituents, industry constituents and shareholder counts; valuation history uses `cne backfill valuation_metrics` separately); `universe` only marks that historical ST is needed and adds no daily update requests. Without `--pack`, the saved selection is reused; the first time it is `market`. `--schedule` installs, after initialization finishes successfully, the current user's scheduled daily update and closing catch-up for these research packs, excluding announcements and news; an installation failure does not mark an already built lake as failed. If a snapshot such as trading status misses a day, the next date-based daily update cannot recover that day.

Once a batch of daily bars is sealed, `load("daily_bars", symbols=[...])` can read the completed unadjusted bars. Queries without `symbols` still see only published data, so a half-finished scan is never mistaken for the full market. Adjustment factors become available only at the end of initialization, after `adj_factors` is published.

Historical ST checkpoints are keyed by start date, end date and universe; when resuming across days, keep these three unchanged. Current trading status and suspensions derived from daily bars are not complete historical ST evidence.

For an init left unfinished before the upgrade because of the 400-stock ST cap or the delisted-bar delegation, simply rerun the same `cne init` after upgrading; it resumes the original run automatically. The resume switches to a current-status snapshot, recovers delisted daily bars automatically, and keeps successful batches and ST rows already fetched into staging; existing historical ST checkpoints are not marked complete. Later historical ST needs still use the separate backfill command.

“Complete” here does not mean “every dataset has unlimited history”. The project has no single command that pulls the full history of every dataset at once. `init` is responsible for the core: securities, calendar, corporate actions, stock/index daily bars, trading status and derived factors; minute bars, 5-minute bars and trade ticks are off by default, and snapshot-type data cannot backfill history the source never provided. When a backfillable dataset needs earlier history, use `cne backfill <dataset> --start ... --end ...`; for how far back each dataset can go, see the [dataset catalog](../datasets/catalog.md).

A failed corporate-action, index-bar or trading-status phase (usually a source that is temporarily unavailable) does not stop the build: daily bars still publish, and the summary at the end lists the steps left undone. Errors in what runs after the bars are published (adjustment factors, industry indices, the quality audit and the suspension derive) likewise no longer mark the published lake unusable or skip the phases after them; rerunning `cne init` redoes only those. Failures in the instrument directory, trading calendar, daily bars or publishing still stop the build.

On macOS and Windows, `cne init` holds off idle system sleep while it runs, so an unattended build does not doze off and lose its connections; closing the lid or choosing Sleep still sleeps, and the hold ends with the process. Set `CNE_ALLOW_SLEEP=1` to turn it off.

Daily bars are fetched before corporate actions. The corporate-action phase asks TDX for every instrument's full ex-rights history whatever the window; during init only the final adjustment-factor derive reads it, so bars no longer wait for it. The sweep opens as many TDX connections as `[source_concurrency].tdx_protocol` allows (at most 4), and every request stays under the same TDX pacing, so the whole market takes roughly fifteen minutes. When fund ingestion is off (`[universe].ingest_listed_funds` / `ingest_eligible_etfs`), ETFs and LOFs are left out, about a quarter fewer requests; if you turn fund ingestion on later, the daily corporate-action step sweeps the funds not swept yet (at most 500 per run) and warns, listing the funds still missing history, until it is done.

Adjustment factors take one paced Sina request per security, about half an hour for the whole market. Init prefetches them into the local cache in the background while it downloads daily bars and sweeps corporate actions, so the final derive reads the cache and requests only what is still missing; a security the prefetch could not fetch is requested again by the derive and does not affect the build.

If you pressed Ctrl-C midway, the process was killed, the result is `degraded`, or the summary lists steps left undone, do not delete `data/` and do not start over: rerun the same `cne init`; it automatically finds the unfinished run and continues from the failed batches and missing phases. Use `cne check` for acceptance afterwards. Use `cne init --run-id RUN_ID` only when you need to target a specific old run.

After Ctrl-C, the command first stops the running workers, then records unfinished batches as `failed` so they can be retried immediately; batches that already succeeded are not fetched again. It is also fine if the process is killed outright: the next command recognizes the orphaned run from the run lock. Active batches and old staged data that has not passed seal verification remain protected by the publication gate; independent facts that have passed seal verification can be published. Derived steps missing their inputs explain why they were skipped. The end of a run does not mean evidence for every security in the window is complete.

`fresh` in `status --datasets` only means **the dates already written to disk** are recent enough. The production data lake also runs a lightweight check against the latest daily-bar cross-section, comparing the configured `[universe].ingest`, the securities active that day, and explicit suspension evidence: every active security in scope must have a daily bar or explicit suspension evidence. If an entire configured market is missing (for example, `all_a` with no BJ), the instruments phase reports partial coverage, and the catalog already obtained can still be published. An unfinished init, securities missing evidence, or a cross-section that does not reconcile are each reported separately. A plain `status` query returns 0 on success; for scheduled acceptance, use `cne status --datasets --gate`, which returns non-zero on gaps or failures.

“Full market” includes the BSE: listing status, suspensions/resumptions and ST come from the BSE's own pages, daily-bar history goes through the dedicated TDX BJ path first, and Sina fills in symbols not covered; missing turnover in historical Sina rows can be filled by row-by-row comparison against TDX. The default universe `all_a` covers A-shares on the Shanghai, Shenzhen and Beijing markets. The date each dataset actually reaches is recorded in `coverage_start`.

When you need longer history, you can pull it all at once or deepen history later:

```bash
cne init --profile full

# Or backfill history for a single dataset
cne backfill daily_bars --start 2016-01-01 --end COVERAGE_START
```

**Where is it now?** While running, it prints these kinds of lines; the values in the table below are only examples of the output format,
the actual security counts, batches and timings depend on your own lake and sources:

| Line | Meaning |
|------|------|
| `Step <name> starting` / `Step <name> success in Ns` | Step entry and exit |
| `daily_bars: 120 symbol(s) over … → 2 batch(es) … on 1 lane(s)` | The size of this scan, printed before it starts |
| `daily_bars 1/2 batches · 240 rows · 1m24s elapsed · ~1m24s left` | Rolling progress; the remaining time appears only after a full round of lanes |
| `instruments 2/5 Beijing board · 41s elapsed · 7412 so far` | Instrument-directory progress: one line as each source (TDX, the Beijing board, listing dates and so on) starts |
| `still working: daily_bars 4m12s (no output for 1m02s)` | Heartbeat; names the current step after 60 seconds of silence |

The `Logging to …/logs/cne-init-<timestamp>.log` line printed at startup is this run's log file (the directory can be overridden with `CNE_LOG_DIR`). You can also check from another terminal:

```bash
cne status --run latest --config configs/cnequity.toml
```

**Cost scales with the number of symbols, not the number of days.** The main historical path of `daily_bars` fetches per symbol, so `--start D --end D` pulls just one day but by default still scans the whole configured scope; the difference for a multi-year window is mainly how many bars each symbol returns. For a quick check, narrow the scope with `--symbols` and run `--plan` first:

```bash
cne backfill daily_bars --symbols 600519.SH,000001.SZ --start 2026-09-15 --end 2026-09-15 --plan
```


## End of a run and insufficient coverage

`init`, the daily update, event streams, backfills and retries share three states: execution, coverage and publication. Verified partial results can be published, and the command ends with `degraded` and returns 0; gaps, source cooldowns and unscanned ranges are kept. Even when all sources are unavailable and this run obtained 0 rows, the command ends normally with insufficient coverage and returns 0, keeps the existing lake, and does not fabricate complete coverage. The `fallback` in the result lists usable data, registered source capabilities and the retry command for the original range; program errors, storage failures or integrity errors still return failure.

Source limitations within initialization's responsibilities do not leave behind an execution plan that can only be closed by manually running another command. An init whose phases were attempted or explained as skipped despite source limits is not automatically treated as an unfinished run; `--resume` is for interruptions or real execution errors. To improve coverage, backfill or retry the specific range explicitly. Historical ST is always an optional, separate task.

Tasks can still run for a long time because of source pacing, lock waits and computation time; the project does not promise a fixed completion time and does not make 100% coverage a success condition for every command. For result fields and migrating old scheduling scripts, see the [CLI result contract](../reference/cli.md#command-results-and-exit-codes).

## Accept the result, not just the completion message

```bash
cne check
```

This one command reports, in order, freshness and coverage (the same as `cne status --datasets --gate`), data quality (the latest run's audit and the full-lake audit snapshot) and size, and finally gives a verdict; exit code 0 means usable, 1 means gaps or quality errors, 2 means it cannot be proven. The full-lake audit reads every historical partition, so by default the latest result is read; add `--full` to rerun it on the spot. `--pack` additionally reports the window, gaps and next steps per research pack; missing data or uncovered historical ST worsens the exit code. Without `--pack`, the verdict is the same as before.

`fresh` is about freshness; a healthy audit also does not mean any historical window is researchable. A full audit writes reports and runs external verification according to the source switches; see [command side effects](../reference/cli-surface.md). When you need a historical universe, run `audit --full --research-start ... --research-end ...` for your actual research window, or query with a strict profile.

With a source checkout, `scripts/accept_backfill.py` can verify idempotency, coverage start and the consumption layer; see [backfill completion acceptance](../operations/runbook.md#backfill-acceptance). These scripts are not installed with the PyPI package.

## Next

- Ongoing operation: [Runbook](../operations/runbook.md); schedule the daily update and event streams separately.
- Before widening the scope: [Request cost and source protection](../operations/fetch-policy.md).
- Querying research data: [Query guide](../datasets/query-guide.md) and [research recipes](../recipes/README.md).
