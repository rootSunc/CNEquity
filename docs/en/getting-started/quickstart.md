# Quickstart

## Run an offline query {#offline-sample}

Run these commands in an empty working directory. See [installation](installation.md) for system requirements.

```bash
python -m pip install cnequity
cne init --profile sample
cne query --config configs/cnequity.sample.toml --sql "SELECT symbol, trade_date, close, source FROM daily_bars ORDER BY trade_date DESC LIMIT 5"
```

The sample writes synthetic data to `data/cnequity-sample/`. The result identifies its rows as `source=mock`, for checking installation and queries.

## Choose a live collection scope {#init-scope}

| Command | Scope |
|---|---|
| `cne init --profile demo` | Collect recent daily bars for a few symbols from real sources into a separate demo lake. Query it with `configs/cnequity.demo.toml`. |
| `cne init` | The `quick` profile, with about three years of baseline data across the configured universe. |
| `cne init --profile full` | The same universe with deeper history. The default backfill starts on 2016-01-01, with actual coverage varying by dataset and source. |

To choose a data directory before initializing a production lake, run `cne config create --data-root /path/to/lake`. The default lookup uses an existing `configs/cnequity.toml` in the working directory or the user config. Run `cne config path` to see the active path.

Live initialization time depends on the history window, source responses, and network. Rerun the same `cne init` command to resume an interrupted initialization. For a `degraded` result, use `cne check` and `cne status --datasets` to inspect gaps. See [run results](../operations/runbook.md#results) for exit codes and verification.

## Inspect and update the lake {#browser}

```bash
cne serve --read-only
```

Open <http://127.0.0.1:8787> to inspect rows, coverage, and run status. Add the corresponding `--config` to open a sample or demo lake. After initializing a production lake, use `cne run daily` to update it.

![The cne serve dashboard showing offline sample daily bars and their source](../assets/cne-serve-sample.en.png)

Continue with [queries and research](../datasets/query-guide.md), [data coverage](../datasets/catalog.md), [daily updates](../operations/runbook.md), or the [design](../architecture/overview.md).
