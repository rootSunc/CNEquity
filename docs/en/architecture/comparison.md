---
description: AKShare, Tushare or Baostock? How three ways of fetching China A-share data differ from the CNEquity local data lake, when to use each, and how to combine them.
---

# AKShare, Tushare or Baostock: how they differ from a local data lake

These tools are often compared, but they solve problems at different stages:

- **AKShare**: an open-source Python data-fetching library. Call it once and you get a DataFrame back.
- **Tushare Pro**: a data API service you call with a token after registering.
- **Baostock**: a free Python data-fetching library; no registration, it logs in anonymously and pulls Shanghai and Shenzhen history from its own servers.
- **CNEquity**: a local data lake. It collects the data and keeps maintaining it, and research reads from local storage.

So the question is not which replaces which, but which stage your work is missing. The sections below compare only ways of working and do not give static ratings of the other tools' coverage, pricing, or licensing. Those change with versions and terms; rely on their current documentation.

## Ways of working at a glance

| | AKShare | Tushare Pro | Baostock | CNEquity |
|---|---|---|---|---|
| Form | Python library; functions return DataFrames directly | Remote API service, called through a Python SDK | Python library that queries the Baostock servers | Local Parquet data lake, with CLI, Python / SQL queries, a dashboard, and MCP |
| Account and credentials | Not needed | Registration and a token are required; interface permissions and rate limits are tied to points | No registration, anonymous login; daily request cap | Not needed for basic collection; a few supplementary sources need your own credentials |
| Where the data lives | Not stored; the caller stores it | Not stored; the caller stores it | Not stored; the caller stores it | Stored locally, managed by dataset, partition, and immutable version |
| Incremental updates and resume | Implemented by the caller | Implemented by the caller | Implemented by the caller | Built-in watermarks, batch-based resume, retry of failed ranges |
| Quality checks | Done by the caller | Cleaned by the service provider | Provided by the service; the caller verifies | Audits both before and after publishing; results are visible in the dashboard and `cne check` |
| Research semantics | Depends on the specific interface | Depends on the specific interface | Adjusted prices and factors provided; ST is a yes/no flag only | Raw prices and adjustment factors stored separately; delisted stocks kept; financial statements distinguish strict PIT from reconstructed after the fact |
| Provenance | No row-level record | No row-level record | No row-level record | Every row carries `source`, `data_version`, `fetched_at` |
| Use by AI agents | You wrap it yourself | You wrap it yourself | You wrap it yourself | `cne mcp` provides read-only tools, over both stdio and HTTP |
| Coverage | Very broad: beyond A-shares, also Hong Kong and US stocks, funds, macro, global markets, etc. | Broad, unlocked by points | Shanghai and Shenzhen A-share history: prices, valuation and financials; no Beijing exchange | Focused on China markets: A-shares and their fundamentals, fund flows, and events, plus domestic futures and options, 55 datasets in total |

## When to use which

**If you only occasionally fetch a table**, such as today's dragon-tiger list or the latest value of a macro indicator, using AKShare, Tushare or Baostock directly is lighter. Building a lake for that is not worth it.

**When the same history is reused across many studies**, the cost of a data-fetching library shifts onto you: pulling again every time, storing the files yourself, handling interruptions and gaps yourself, and remembering which day and which interface each dataset came from. CNEquity makes these the data layer's responsibility.

**When a backtest must not get the semantics wrong**, data-fetching libraries usually give you "today's view": the list of stocks still trading today, financial statements restated after the fact, prices under some particular adjustment. CNEquity writes these semantics into the query contract:

- Historical universes keep delisted symbols to avoid [survivorship bias](../reference/universe-profiles.md).
- Financial statements are sliced by `as_of`, and `pit_mode="strict"` returns only data that had been disclosed at the time; see the [PIT example](../recipes/pit-rebalance.md).
- Raw prices and adjustment factors are stored separately, and the adjustment method is chosen at query time; see the [adjustment example](../recipes/research-baseline.md).

**When you want AI to query historical data directly**, what the agent needs is data with clear coverage and semantics written into the responses, not web interfaces called on the spot. `cne mcp` first reports what is in the lake and then answers questions; see [MCP integration](../reference/mcp.md).

## Using them together

- **For instruments CNEquity does not cover**, such as Hong Kong and US stocks, mutual fund NAVs, or overseas macro data, keep pulling them with AKShare or Tushare, then join them with the lake's Parquet in DuckDB.
- **Data you already have through Tushare points** can serve as a reference source for spot-checking whether the same field in the lake matches.
- **Baostock is already a backup source inside the lake**: historical ST, valuation and delisted-stock backfills query it, so there is nothing extra to wire up; see [Sources and history limits](../datasets/sources.md).
- **When migrating from AKShare**, note that CNEquity's code format is `600519.SH`, dates are of type `date`, and volume units are described in [Fields and units](../datasets/schema.md).

## What CNEquity costs

- **It takes local disk space, and you need somewhere to run scheduled jobs.** The first initialization of the core datasets for the whole market over roughly the last 3 years takes several hours.
- **Public sources change.** Reachability, history depth, and release cadence can all shift; the project keeps up, but it cannot offer an SLA like a service provider.
- **Accumulation starts only once you enable it.** Snapshot-type data (such as the popularity ranking, per-stock fund flow, and analyst consensus) cannot be backfilled for the period before you enabled it; see the [dataset catalog](../datasets/catalog.md).
- **It is still at 0.x.** Read the [changelog](../changelog.md) before upgrading.

To get a feel for it first, use `cne init --profile demo` to build a small lake with just 5 stocks in a minute; see the [Quickstart](../getting-started/quickstart.md).
