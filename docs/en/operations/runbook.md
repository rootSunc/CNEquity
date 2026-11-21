# Daily updates

## Configuration and daily updates {#install-schedule}

Run `cne config path` to see the active config and `cne config validate` to check it. With `extends = "builtin"`, keep local overrides such as `[data].root` in your config. See the [configuration reference](../getting-started/configuration.md) for what each section controls.

```bash
cne run daily
cne status --datasets
cne check
```

On trading days, `cne run daily` without extra options runs the daily groups and then the configured event streams. On non-trading days, it skips the daily groups and can still run the event streams.

Use `cne schedule install` to install the current user's system timer and `cne schedule status` to inspect it. The system timer checks once per minute. Daily and catch-up `run_at` times use China Standard Time. See `cne schedule install --help` for research packs, event streams, and catch-up options. Custom schedulers use their own timezone settings.

## Results and coverage {#results}

| Signal | Meaning and next step |
|---|---|
| `success` or `degraded` | Both run statuses map to exit code 0. A degraded run produced usable partial results. Check the remaining gaps. A daily command takes the worst exit code across its stages. |
| `skipped` | Read the `reason_code`, for example `non_trading_day` or `locked`. |
| `failed` or `blocked` | Inspect `cne status --run <run ID>` and the logs. Address the cause before retrying. |

Freshness, coverage of the requested scope, and data quality are separate checks. Use `cne status --datasets` to inspect local coverage and `cne check` to evaluate readiness. For an individual run, check its result and the affected datasets.

The Today tab in `cne serve` shows recent stock and index daily bar coverage, data freshness, and run history. Recent coverage gives the date range from the lake's coverage plan and opens a backfill preview for that range. Use the quality reports link to see existing findings.

## Backfill and retry {#backfill}

Inspect the plan before sending requests to a provider. This example limits the scope to one symbol. Use dates that match the actual gap.

```bash
cne backfill daily_bars --symbols 600519.SH --start 2026-06-01 --end 2026-06-30 --plan
```

The plan fetches no data and writes nothing to the lake. After reviewing it, remove `--plan` to collect data. Rerun the same `cne init` after interrupted initialization. Use `cne run retry` for failed updates or add `--run-id` for a specific run. A provider that only offers current snapshots may be unable to fill earlier gaps.

| Symptom | Check |
|---|---|
| A query misses new data | Check `cne config path`, the configured `data.root`, coverage, and publication state. |
| The provider rejects or times out | Start with `cne doctor` and `cne sources limits`. To check connectivity, use `cne sources probe --list`, then run a targeted probe such as `cne sources probe --only tdx_protocol`. |
| A writer or lock is active | Check run status and processes, then allow the holder to finish. A lock file by itself does not prove a process is active. |
| The command asks for an upgrade | Follow the [upgrade instructions](../getting-started/installation.md) for the target release. |

See [troubleshooting](troubleshooting.md) for more symptoms. Run `cne <command> --help` for options, and check [network and write effects](../reference/cli-surface.md) before execution.

## Snapshots, inspection, and recovery {#backup-and-restore}

```bash
cne snapshot create baseline --dataset daily_bars
cne snapshot verify baseline
cne snapshot restore baseline /path/to/empty-restored-lake
```

A snapshot contains the selected datasets. Restore into a new or empty directory, then point a separate config at it and verify your queries. A metadata backup preserves records. Recovering the data also requires the corresponding files. Updates and maintenance can remove old versions under the retention policy. Save a snapshot of the inputs you need to reproduce a result.

Local `cne serve` can start supported jobs from its operations page. Use `--read-only` for browsing. Before an upgrade, keep an independently restorable data copy. See [install and upgrade](../getting-started/installation.md).
