---
description: Install CNEquity with pip, build a local China A-share data lake with one command, and run your first stock price query.
---

# Quickstart: from install to your first query

One command builds a full-market data lake; then you read your first result. The commands below do not require cloning the repository; for Python and system requirements, see [Installation](installation.md).

## 1. Initialize

Run this in the directory where you intend to keep the data long term:

```bash
pip install cnequity
cne init
```

The first run generates the per-user config `~/.config/cnequity/config.toml` and writes data to `data/cnequity/` under the current directory (the config records the absolute path); an existing config is reused as is, and there is no need to run `cne config create` first. It then builds the core of the last 3 years for the whole Shanghai, Shenzhen and Beijing market: securities, calendar, corporate actions, stock and index daily bars, adjustment factors and industry indices. It automatically recovers daily bars for known delisted stocks within the window, fetches the current trading status and derives historical suspensions, then audits and publishes.

Full-market initialization can take several hours and has no fixed completion time; the terminal prints batch progress and an ETA in real time. Once a batch of daily bars is sealed, you do not have to wait for the full market to finish: `load("daily_bars", symbols=["600519.SH"])` can read that stock's unadjusted daily bars. Queries without `symbols` still see only published data; adjustment factors are published only at the end of initialization.

**Interrupted, or is there a `warning` in the result? Rerun the same `cne init`.** It resumes the unfinished initialization and keeps the batches that already succeeded. `warning` / `degraded` means the data has been published but some sources temporarily have insufficient coverage; the command returns 0, and the gaps are left for a rerun or the daily update to fill. Only program, storage or integrity errors cause a failure.

If you need deeper history, use `cne init --profile full` from the start (daily bars from 2016-01-01). For scope, disk and resume details, see the [initialization guide](initialization.md).

## 2. Read your first result

```python
from cnequity.query import load

bars = load("daily_bars", symbols=["600519.SH"])
print(bars.select("symbol", "trade_date", "close", "volume", "source").tail(10))
```

```bash
cne query --sql "SELECT symbol, trade_date, close, source FROM daily_bars ORDER BY trade_date DESC LIMIT 10"
cne check
```

`cne check` gives a verdict on freshness and coverage, data quality and size in one command. `load` and the `cne` commands find this config from any directory (a `configs/cnequity.toml` in the current directory takes precedence); `cne config path` shows which one is in use. `fresh` only means the dates already written to disk are recent enough; it does not mean the history is complete.

## 3. Check in the browser {#browser}

```bash
cne serve
```

Open <http://127.0.0.1:8787> or <http://localhost:8787> to view datasets, watermarks and sample rows. The operations page can start initialization, the daily update, catch-up and inspection runs; closing the page or this process does not stop tasks that have already started. Press `Ctrl-C` to stop the server. If you only want to browse, use `cne serve --read-only`.

## 4. Daily update

```bash
cne run daily
```

Run it once a day (weekends included): on trading days it runs all daily update groups in configured order, then the event streams for announcements, regulatory events and news; on non-trading days it runs only the event streams. Calling this one command daily from the system scheduler (cron, launchd, Windows Task Scheduler) is enough.

| Next step | Purpose |
|---|---|
| `cne check` | Acceptance: freshness and coverage, data quality, size |
| `cne run retry` | Retry the latest failed run of each daily update group |
| `pip install -U cnequity && cne config upgrade` | Upgrade the version and add the new version's schedule steps to the config |

Minute bars, trade ticks and per-contract derivatives are off by default. For historical ST and delisting coverage, see the [initialization guide](initialization.md).

<a id="init-scope"></a>

!!! tip "Scope, disk and resume"
    For quick vs full, delisting and historical ST backfill, disk cost, recovery after Ctrl-C and acceptance, see [Initialization, scope and resume](initialization.md#init-scope).

## Troubleshooting

| Symptom | Next step |
|---|---|
| TDX cannot connect | Run `cne doctor` for an offline checkup; then run `cne sources probe --only tdx_protocol` once more |
| `no parquet data` | Check which config is in use, the absolute `data.root`, and whether that dataset has been compacted |
| Initialization interrupted or has warnings | Keep the data and rerun the same `cne init`; see [resume](initialization.md) |
| Historical PIT returns no rows | Freshly backfilled data is not necessarily evidence that was observed in the past; see the [PIT example](../recipes/pit-rebalance.md) |

## Just want to try a few stocks first

When you do not want to build a full-market lake, `--profile demo` uses real data sources to write 5 stocks and roughly the last 30 trading days into a separate `data/cnequity-demo/`, with config `configs/cnequity.demo.toml`; `--profile sample` generates synthetic data of the same shape offline (`source=mock`) into `data/cnequity-sample/` with config `configs/cnequity.sample.toml`, and is only for verifying the installation. Neither affects the production lake, and neither replaces full-market initialization.

```bash
cne init --profile demo
cne init --profile sample
```

When the demo finishes it prints a `claude mcp add ...` command with the config's absolute path; copy it to hand this small lake to an AI agent, and see [MCP integration](../reference/mcp.md) for other clients. Run `cne serve --config configs/cnequity.demo.toml` to browse it; the dashboard only evaluates the datasets a demo lake actually holds.

Next: [Research recipes](../recipes/README.md) · [Configuration](configuration.md) · [Runbook](../operations/runbook.md) · [Troubleshooting](../operations/troubleshooting.md).

The end of a run does not mean evidence from every source is complete; valid partial results can be published and gaps are kept. Use `cne check` for acceptance. See [result contract and upgrades](../reference/cli.md#command-results-and-exit-codes).
