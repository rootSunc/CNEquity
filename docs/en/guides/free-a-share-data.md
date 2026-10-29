---
description: How to get China A-share historical data for free - no registration or token; one command downloads daily bars, adjustment factors, financial statements and corporate actions for the whole market, with daily incremental updates.
---

# Free China A-share historical data, no registration or token

CNEquity collects China A-share data from public sources such as the exchanges, CNINFO, TDX, Sina, East Money and Baostock, and stores it as local Parquet files. Basic collection needs no account, token or points.

## Try it in a minute

Start with 5 stocks in a separate directory that does not touch your real data later:

```bash
pip install cnequity
cne init --profile demo
```

## Download the whole market

```bash
cne init
```

By default this builds the core datasets for the Shanghai, Shenzhen and Beijing markets over the last 3 years: instruments, the trading calendar, corporate actions, stock and index daily bars, adjustment factors and industry indices, and it recovers daily bars for stocks delisted within the window. The first run can take several hours; if it is interrupted, rerun the same command to resume without re-downloading what is done.

For longer history use `cne init --profile full`, with daily bars from 2016-01-01. History start and source limits per dataset are in [Sources and history limits](../datasets/sources.md).

## Read the data

```python
from cnequity.query import load

bars = load("daily_bars", symbols=["600519.SH"], adjust="hfq")
print(bars.tail())
```

Or write SQL directly:

```bash
cne query --sql "SELECT symbol, trade_date, close FROM daily_bars ORDER BY trade_date DESC LIMIT 10"
```

## Keep it current

Run `cne run daily` once a day from cron, launchd or Windows Task Scheduler, or install the scheduled job with `cne schedule install`.

## What you get

57 datasets, grouped by use into market data, corporate events, fundamentals, fund flows, industries and indices, macro and more; the full list is in the [Dataset catalog](../datasets/catalog.md).

Limits to keep in mind:

- **Snapshot data cannot be backfilled.** Popularity rankings, per-stock fund flow and analyst consensus only accumulate from the day you enable them.
- **Public sources change.** Reachability and history depth can shift; the project keeps up but offers no commercial SLA.
- **It uses local disk.** Scope and disk cost are in [Initialization, scope and resume](../getting-started/initialization.md).

## Related pages

- [Quickstart](../getting-started/quickstart.md)
- [AKShare, Tushare or Baostock](../architecture/comparison.md)
- [Avoiding look-ahead bias in backtests](avoid-look-ahead-bias.md)
