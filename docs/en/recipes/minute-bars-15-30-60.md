# 15 / 30 / 60-minute bars

CNEquity does not fetch 15 / 30 / 60-minute bars from data sources; it resamples them from the 1m (`minute_bars`) and 5m (`minute_bars_5m`) bars already in the lake. There are two ways to use them:

| Mode | How | Stored in the lake? | Best for |
|---|---|---|---|
| Compute at query time | Call `resample_minute_history` in Python | No, results live only in memory | Research written in Python, ad hoc frequency changes |
| Derive into the lake | `cne derive minute_bars_15m`, or start it from the operations page of the web dashboard | Yes, written as a derived dataset | Reading via SQL, the HTTP API, MCP or the dashboard, or repeated use |

Both modes use the same computation rules. **Not computed by default**: daily updates, `cne init` and scheduled jobs never generate these three datasets; they enter the lake only when you run the derivation yourself.

## Prerequisites

The lake needs at least one of `minute_bars` or `minute_bars_5m`. Both are off by default; enable them under `[minute_bars]` in the configuration and backfill. See the [configuration reference](../getting-started/configuration.md) and the [dataset catalog](../datasets/catalog.md).

| Input | History kept by the data source | Frequencies it can produce |
|---|---|---|
| 1m | About 95 trading days | 5m, 15m, 30m, 60m |
| 5m | About 491 trading days (about 2 years) | 15m, 30m, 60m |

Minute bars already collected in the lake are kept indefinitely; the table above only limits how far back a backfill can reach.

## Computation rules

1. **Input selection**: chosen per "stock × trading day". If a stock has 1m on a given day, only 1m is used; 5m is used only when 1m is absent. Each stock's 1m history starts at a different date, so there is no single switchover date.
2. **Alignment**: the morning counts from 09:30 and the afternoon from 13:00, never across the lunch break. `bar_time` is the closing minute of the interval; for example, 60m has four bars a day: 10:30, 11:30, 14:00, 15:00.
3. **OHLC**: only input bars with trades count (`volume > 0` or `amount > 0`; the latter keeps small trades whose share count rounds to zero). When an interval has no trades at all, all four prices take the last quote and volume and amount are zero; that price is not a trade price.
4. **Volume and amount**: the volume and amount of the input bars in the interval are summed directly.
5. **Missing component bars**: an interval with only some of its input bars is not filled in. Query-time computation raises an error; derivation into the lake skips that stock for that day and counts it. A day that has 1m but incomplete 1m does not switch to 5m, so that the same day never mixes two bases. Skips fall into three categories:
   - **Suspension**: no trades at all during the day (`skipped_halted_1m` / `skipped_halted_5m`). A suspended stock's 1m data is usually one bar short; this is normal.
   - **Intraday suspension and resumption**: the stock traded only part of the session, so bars start mid-session or end early, but there is no hole in between (`skipped_partial_session_1m` / `skipped_partial_session_5m`), for example a resumption at 13:41. This is also a market event, not an anomaly.
   - **Incomplete data**: the stock traded that day, but there is a hole between bars (`skipped_incomplete_1m` / `skipped_incomplete_5m`), usually because data was missing from the fetch. The run is marked degraded, and `incomplete_examples` lists the first few cases; backfill the corresponding minute bars and derive again.
6. **Other invalid input**: duplicate timestamps, bars during the lunch break or outside the session, invalid prices and the like make resampling raise an error. Derivation into the lake skips only the affected "stock × trading day" and writes the rest as usual; `skipped_invalid` in the result counts them, `invalid_examples` lists the first few reasons, and the run is marked degraded.
7. **Source marker**: the result has an extra column `resampled_from`, with value `1m` or `5m`.
8. **No price adjustment**: prices are unadjusted, like the input; use `adjust="hfq"` / `"qfq"` at read time to adjust as needed.

### Basis differences between 1m and 5m

The vendor's 5m bars are built directly from all 1m bars, so quotes carried forward through minutes without trades are also counted in the open, high and low. Whenever a 5m bar has trades, its OHLC enters the computation as is; as a result, 15 / 30 / 60-minute bars computed from 5m can differ in open, high and low from those computed from 1m in a few intervals, while close, volume and amount are essentially the same. Dates covered by 1m always use 1m first; when research needs a uniform basis, filter or group by `resampled_from`.

## Mode 1: compute at query time

```python
from cnequity.query import load, resample_minute_history

window = dict(start="2025-01-02", end="2026-09-30", symbols=["603869.SH"])
bars_30m = resample_minute_history(
    load("minute_bars", **window),
    load("minute_bars_5m", **window),
    "30m",
)
```

To use only one kind of input, use `resample_trade_bars(frame, "15m")`: pass either 1m or 5m, not a mix. See the [query guide](../datasets/query-guide.md#trade-based-minute-resampling).

## Mode 2: derive into the lake

### Command line

```bash
cne derive minute_bars_15m
```

```bash
cne derive minute_bars_60m --start 2025-01-02 --end 2025-12-31
```

| Option | Effect |
|---|---|
| No options | Computes only trading days that need it: not computed yet; the 1m / 5m content changed (including 1m backfilled later, a rollback or a restore from backup); the computation rules changed with a version upgrade; or the output was modified. Decided by content, not file times |
| `--start` / `--end` | Computes only trading days in this window that have minute bars |
| `--full` | Recomputes all trading days that have minute bars |

The command outputs JSON: rows written, trading days, the number of "stock × trading day" computed from 1m / 5m, skip counts (`skipped_halted_*`, `skipped_partial_session_*`, `skipped_incomplete_*`, `skipped_invalid`), and `sessions_cleared`: trading days that previously had results but have no usable data after recomputation. Those trading days' partitions are replaced with empty partitions, so old data does not linger in the lake. When the lake has neither 1m nor 5m, the command raises an error and suggests backfilling minute bars first.

The three frequencies are three independent datasets; derive whichever you need.

### Web dashboard

After `cne serve` opens the dashboard, there are three entry points, all opening the same form:

1. **Operations → Today → What to run → More → Recompute a derived dataset**: in the form that opens on the right, set "Derived" to "15-minute bars · minute_bars_15m" (or 30m, 60m).
2. **Operations → Commands → Backfill → Recompute**: the same form.
3. **Datasets page**: open "15-minute bars" or another of these datasets under "Datasets", then click "Recompute" at the top of the page; the form opens with that dataset preselected.

Once a minute bar target is selected, the form explains the computation rules above. Leaving both start and end empty computes only trading days not yet computed or whose input has been updated; check "Full rewrite" to recompute all trading days. Once the fields are filled, the form previews the command that will run; after you confirm, it runs as a background job, and its progress and results appear under "Recent jobs" and on the job timeline. Closing the page or serve does not interrupt the job.

You can also open `http://127.0.0.1:8787/#/ops?op=derive.run&name=minute_bars_15m` directly, and the form opens with the target preselected. Read-only mode (`--read-only`) and non-loopback addresses cannot start it from the page by default; see the [CLI reference](../reference/cli.md#cne-serve).

### Storage and publishing

- Location: `{data.root}/derived/minute_bars_15m/trade_date=YYYY-MM-DD/`, partitioned by trading day; likewise for 30m and 60m.
- Primary key: `(symbol, trade_date, bar_time, frequency)`; for columns, see [columns and units](../datasets/schema.md#minute_bars_15m--minute_bars_30m--minute_bars_60m).
- Publishing: like other derived datasets, a new version is generated under the write lock, recording the 1m / 5m versions used; programs that are reading never see half-written data.
- Freshness: these three datasets have no watermark. `cne status` and the dashboard do not flag them as stale for not being updated daily; if never derived, they show as empty, which is not an error. After minute bars are updated, run `cne derive` again.
- Lag notice: for a dataset that has been derived before, if 1m / 5m has trading days not yet recomputed, `cne status --datasets` prints 「输入有 N 个交易日尚未重算」 ("input has N trading days not yet recomputed") below the table; the dataset page of the dashboard shows the same number under "Coverage", with a "Recompute" link.

## Reading data stored in the lake

| Channel | Usage |
|---|---|
| Python | `load("minute_bars_15m", start=..., symbols=[...], adjust="hfq")` |
| SQL | `cne query --sql "SELECT * FROM minute_bars_15m WHERE symbol = '603869.SH'"` |
| HTTP | After `cne serve`, read `/api/read/minute_bars_15m?start=2026-09-01&end=2026-09-30&symbol=603869.SH`; limits below |
| MCP | `query_bars` with `dataset` set to `minute_bars_15m`; `adjust` can be passed |
| Web dashboard | Browse samples on the dataset page |

The HTTP API and MCP only read data already stored in the lake and cannot compute on the fly; to get data from these two channels, derive into the lake first.

The HTTP read endpoint `/api/read/<dataset>` is meant for small, bounded windows:

- `start` and `end` are required, and the window may not exceed 366 calendar days; spanning multiple days requires `symbol` (singular, one stock at a time).
- At most 1000 rows are returned per request; more returns 413. For a single stock, that is at most about 62 trading days per request for 15m and about 125 for 30m; 60m hits the 366-day window limit first, at about 240 trading days. Read longer histories in segments, or use Python / SQL instead.
- `adjust=hfq` / `qfq` requires a published version of `adj_factors`.

## Limitations

- 15 / 30 / 60-minute bars are not fetched directly from data sources. TDX does offer these frequencies, but their history depth is the same as 5m, and their open, high and low likewise include quotes carried forward through minutes without trades.
- There is no minute history earlier than 5m, and resampling cannot fill it in.
- Derivation does not correct its input: if the input minute bars are missing or wrong, the results are equally missing or wrong.
