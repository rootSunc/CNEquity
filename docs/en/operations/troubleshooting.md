# Troubleshooting

Find the scope of the failure before acting: `cne status` shows the latest run, `cne status --run <run id>` its stages, `cne status --datasets` each dataset's coverage and freshness, and `cne doctor` checks the installation, configuration, and lake state. Findings are written to `meta/quality/findings/<run id>.json`. No failed step changes published data, so do not delete the lake directory, lock files, or data files to make an error go away.

## Queries and publishing {#query-and-publish}

| Symptom | Cause and fix |
|---|---|
| New data does not show up | Check the lake with `cne config path` first. If the data is still unpublished in `staging/`, `cne run compact` publishes every finished but unpublished run, or `cne run retry --run-id <ID>` resumes one |
| Write commands are refused with a prompt to run `cne upgrade` | The lake was built by an earlier version; upgrade as described in [Install and upgrade](../getting-started/installation.md#cne-upgrade). Reads are unaffected |
| A commit is refused because a dataset's revision changed during the run | Another write published a dataset this run read or meant to publish. Published data is unchanged; run the same command again |
| The publication gate stopped candidate data | Published data is unchanged, and the candidate partitions move to `_quarantine/`. Use the new or worse checks in the report to find the source problem, fix it, and rerun |
| `ReaderError`: missing adjustment factors or insufficient universe evidence | Strict reads refuse by design. Fill the data first (for example, `cne derive adj_factors`), or relax the semantics explicitly |
| `RevisionConsistencyError` | Retention removed the pinned revision. Register revisions you need long-term with `cne storage hold`, or keep a research snapshot |

## Runs and locks {#runs-and-locks}

| Symptom | Cause and fix |
|---|---|
| `skipped` with `reason_code` `locked` | A job of the same kind is running. Wait for it to finish; independent timers that collide only skip, so run groups in order from one scheduler entry |
| `RunLockError` or a timeout waiting for the lake lock | Use `cne status` and the process list to find the holder. The operating system holds locks and releases them when the process exits; a lock file on disk does not mean anyone holds it |
| A local file lock error on Windows | Check permissions on the lake directory and what has it open; the error includes the lock path and the original system error |
| `cne status` shows runs stuck in `running` | The process was killed or ran out of memory. The next `cne run daily` or `cne run retry` reconciles and recovers them; to clean up now, run `cne run clean --reconcile-runs`, which skips runs that still hold their lock |
| `worker pool broke` in the log | A child process exited (often from running out of memory); unfinished batches are retried serially |
| `no batch finished and no worker reported progress` in the log | A worker hung; after `[orchestrator].batch_stall_seconds` the batches rerun in the current process. If it keeps happening, set `tdx_daily_backend = "thread"` and open an Issue |
| Only `still working` for a long time | The next line names the batch and how long it has been silent. On Linux and macOS, run `kill -USR1 <pid>` as the log suggests to write every thread's stack to stderr |
| Initialization was interrupted | Keep the data and config and rerun the same `cne init`; successful batches are not fetched again |

## Sources and rate limits {#sources}

Rule: time can wait, and a ban costs far more than waiting another day. Do not lower request intervals, run several processes against the same free source, or rotate egress to continue requests that were refused.

| Symptom | Cause and fix |
|---|---|
| Requests are refused, time out, or reset | Check cooldowns and budgets with `cne sources limits`, and resume after the cooldown. To verify, run a bounded probe with `cne sources probe --only <source>` |
| EastMoney push2 returns 502 or drops connections | The egress may be restricted. Set `[sources.eastmoney] push2_paused = true` or `CNE_PUSH2_PAUSED=1` to stop push2 requests while other sources continue; after the cooldown and circuit breaker expire, lift the pause and probe once |
| Baostock blacklist or "daily request limit reached" | 50,000 requests a day over one connection, with blacklist durations that grow with each occurrence in the year. The client stops locally and cools down, keeping symbols already fetched; after the cooldown, rerun the same command |
| EastMoney data center reports `rejected schema` | The source's columns no longer match the client contract, and the group does not publish. Keep the report name and error code, check for a fixed release or open an Issue; do not treat it as empty data |
| `daily_bars` reports `remain absent` and the watermark stops | A security is missing a session inside the window and neither the primary nor backup sources can fill it. Check `daily_bars_interior_gap` in the findings: if the codes are ETFs or LOFs, check `[universe].ingest`; if only today is missing, `cne run retry` after the backup source's budget recovers; check independent backups with `cne sources substitutes` |
| CNI indexes report `does not serve adjustment history` | The source does not provide historical rebalance files; this is not an empty list. Current constituents cannot stand in for historical ones, and existing history is kept |
| THS sector backfill fails often | THS throttles dense requests, and the EastMoney proxy setting does not apply to it. After a cooldown, raise `[sources.ths] min_interval_seconds` and rerun `cne backfill sector_bars`, which resumes from its checkpoint |

## Coverage and freshness {#coverage}

| Symptom | Cause and fix |
|---|---|
| `cne status --datasets` shows a dataset as stale | Check that its group ran on the latest trading day and rerun it with `cne run daily --group <name>`. Quarterly datasets are judged by their own cycle, and weekly groups by the last session they were due |
| `cne run daily` shows `skipped` on a non-trading day | `reason_code` is `non_trading_day`, which is normal. Fill a missed trading day with `cne run daily --trade-date <date>` |
| History has gaps | Run `cne backfill <dataset> --start ... --end ... --plan` to see the plan, then run it without `--plan`. Days confirmed empty are skipped by default; `--redo empty` asks again. Snapshot-only datasets cannot recover the past |
| A dataset's source has stopped | Use `[datasets.history_until]` to state where its history ends; later days are no longer owed |
| Disk is full | Below `[storage.free_space].stop_gb`, writes refuse to start and say why. Start with `cne storage usage`: where the space goes and what deleting each part frees at least (snapshots and rehearsal copies are APFS clones, which `du` overcounts). `cne run clean` lists old revisions, merged staging, expired logs, and source snapshots that can be removed, and deletes after interactive confirmation; unfinished runs are kept for resuming by default. On small disks, lower `[storage.retention].keep_generations` |

## Audit findings {#audit}

`cne audit --full` reads all data and can take hours on a large lake. Common checks:

| Check | Fix |
|---|---|
| `pk_unique` | Duplicate primary keys. Inspect recent publications and backfill the range if needed |
| `mixed_partition_granularity` | Fine-grained partitions from the old layout overlap new ones; run `cne upgrade --apply` |
| `unregistered_curated_dir` | `curated/` holds a directory that belongs to no dataset (such as `*.bak`); confirm and move it to a backup directory |
| `adj_close_discontinuity` | Abnormal adjusted returns; run `cne derive adj_factors` and check the factor source |
| `missing_corporate_action` | An ex-date has no corporate action record; `cne backfill corporate_actions` |
| `trading_status_coverage_start` | Historical ST evidence starts after the research window; backfill by market and check the evidence rather than ignoring the warning |

`warning` and `info` never fail a run; whether `error` does is set by `[quality].audit_gate`.

## Dashboard {#dashboard}

| Symptom | Fix |
|---|---|
| `localhost:8787` does not open but `127.0.0.1:8787` does | Upgrade to the current version, where the loopback address accepts both |
| The dashboard still shows the old lake after changing `data.root` | `cne serve` keeps the lake it started with; restart it to switch |

If you cannot locate the problem, open an Issue with the `cne doctor` output, the command, the run id, and redacted logs. Do not post credentials or personal data paths in an Issue.
