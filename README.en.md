<h1 align="center">CML · CNMarketLake</h1>
<p align="center"><b>A daily-refreshable local A-share research lake for humans and AI agents</b></p>

<p align="center">
  <a href="https://github.com/rootSunc/cn-market-lake/actions/workflows/ci.yml"><img src="https://github.com/rootSunc/cn-market-lake/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://pypi.org/project/cn-market-lake/"><img src="https://img.shields.io/pypi/v/cn-market-lake.svg" alt="PyPI"></a>
  <a href="https://www.python.org/downloads/"><img src="https://img.shields.io/badge/python-3.10+-blue.svg" alt="Python 3.10+"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License: Apache 2.0"></a>
  <a href="https://rootsunc.github.io/cn-market-lake/"><img src="https://img.shields.io/badge/docs-site-2f80ed.svg" alt="Docs site"></a>
  <a href="README.md"><img src="https://img.shields.io/badge/文档-中文-lightgrey.svg" alt="中文"></a>
</p>

<p align="center">
  No token or signup. Self-hosted, daily-refreshable, and queryable from Python, DuckDB, Polars, or AI agents.<br>
  <b>42 registered datasets · adjustment / historical universes / PIT · 6 MCP tools · row-level provenance</b>
</p>

<p align="center">
  <img src="docs/assets/cml-serve-hero-demo.png" alt="CNMarketLake illustrative dashboard with synthetic full-coverage heatmap" width="1100" />
</p>

> This is a synthetic README demo, explicitly marked `ILLUSTRATIVE DEMO`; the full-coverage heatmap is not a claim about current production data.

## Architecture

<p align="center">
  <img src="architecture-diagram-v2.png" alt="CNMarketLake architecture diagram" width="1100" />
</p>
<p align="center"><sub>Public sources → adapters and orchestration → local Parquet lake → quality, query, and read-only services</sub></p>

The boundary is deliberate: adapters fetch, the orchestrator schedules and retries, staging becomes curated and derived, quality audits the result, and query / service layers stay read-only. More: [architecture overview](docs/architecture/overview.md).

## Data in ~30 seconds

```bash
pip install cn-market-lake    # no token, credit, or signup
cml demo                   # real data: 5 names × 30 sessions
```

Measured at about 25 seconds. Needs **TDX quote hosts** reachable (mainland access is more reliable);
if it fails, try `cml sources --only tdx_protocol`. The demo writes to its own
`data/cn-market-lake-demo/` directory and never touches a full lake.

<p align="center">
  <img src="docs/assets/cml-demo.png" alt="cml demo: phased fetch with sample daily bars" width="820" />
</p>

```python
from cn_market_lake.query import load

bars = load("daily_bars", data_root="data/cn-market-lake-demo")
```

If you only need a current quote, a fetch API may be enough. Build a lake when you need reproducible history,
survivorship-safe universes, or point-in-time research.

To see why the adjustment contract matters (this also queries Sina and expands to about three years):

```bash
cml demo --research --symbols 600519.SH
# raw return -24.25% → hfq return -14.39% (example output; changes with the as-of date)
```

## Why a lake

<p align="center">
  <img src="docs/assets/survivorship-gap.svg" alt="Same basket, same dates — the only difference is whether the delisted names are still in it" width="820" />
</p>

The same equal-weight buy-and-hold, the same dates. The only difference is
**whether names that later delisted are still in the basket**. Use "stocks that
exist today" as a historical universe — all a current-roster vendor can give
you — and the 2016–2021 five-year return goes from **5.9% to 12.0%**, twice
what it was.

The error **is not visible**: those names are not zero, they are absent.
Delisted names, adjustment factors, and PIT are first-class here — not an
afterthought on a coverage list.

```bash
python scripts/survivorship_gap.py --svg docs/assets/survivorship-gap.svg
```

## Your own lake, in four commands

```bash
pip install cn-market-lake
cml config init            # writes configs/cn-market-lake.toml
cml init                   # every symbol × the last 3 years (~1 hour)
cml run daily              # this one line, each trading day after
```

`cml init` defaults to **shallow, never narrow**: the last 3 years, every symbol.
Trimming symbols instead would build the survivorship bias this lake exists to
avoid straight into it, whereas shallow is honest — `coverage_start` records it.
Want everything: `cml init --profile full` (~3x the time). Deepen any time:

```bash
cml backfill daily_bars --start 2016-01-01 --end <your coverage_start>
```

**Wire into an AI agent** (optional, once the lake exists):

```bash
claude mcp add cn-market-lake -- cml mcp --config "$(pwd)/configs/cn-market-lake.toml"
```

With MCP wired up, ask in plain language:

- "How much did Moutai return over the last five years, adjusted?"
- "Where does Moutai's PE sit in its own five-year distribution?" ★
- "This factor's IC in 2018 — no look-ahead." ★
- "What did the last 60 sessions look like for stocks that delisted?" ★

★ need a local history — a fetch-on-demand tool structurally cannot answer
them. **Fetch tools give you now; a lake gives you history.**

<p align="center">
  <a href="#what-you-can-ask-it">What you can ask</a> ·
  <a href="#why-not-just-akshare--tushare--a-fetch-skill">vs. the alternatives</a> ·
  <a href="#datasets">Datasets</a> ·
  <a href="#self-hosted-daily-lake">Self-hosted lake</a> ·
  <a href="#serve-it-to-an-ai-agent">AI agents</a> ·
  <a href="#glance-at-the-lake">Glance at the lake</a> ·
  <a href="#architecture">Architecture</a> ·
  <a href="#faq">FAQ</a>
</p>

## What you can ask it

| What you want to know | How you get it |
|--|--|
| Moutai five-year return, adjusted | `load("daily_bars", symbols=[...], adjust="hfq")` |
| ★ Moutai PE historical percentile | `valuation_metrics` + window percentile |
| ★ Factor IC in 2018, no look-ahead | `load("financial_statement_items", as_of="2018-04-30")` |
| ★ Last 60 sessions before delisting | `delisting_events` + `daily_bars` |
| ★ Equal-weight return, no survivorship bias | `scripts/survivorship_gap.py` (chart above) |
| Dragon-tiger / unlocks / sector flow | `dragon_tiger` · `share_unlock_schedule` · `sector_fund_flow` |
| ★ CSI 300 / Shenwan membership years ago | `index_constituents` · `industry_members` |

## Why not just AkShare / Tushare / a fetch skill

AkShare and agent fetch skills answer "how do I fetch?" — a snapshot of now,
with no history contract. Tushare is cloud wide tables. Qlib / vn.py are
research / trading platforms. **CML** owns the middle: many sources, one
contract, a resumable local Parquet lake.

| What you care about | **CNMarketLake** | AkShare / fetch skills | Tushare Pro | Qlib / vn.py |
|--|--|--|--|--|
| Local, resumable data base | **Lake + daily jobs** | On-demand; you own orchestration | Cloud credits | Platform-tied |
| Provenance | **Row-level** | Usually no shared contract | Platform fields | Varies |
| Research semantics | **`load()`: adjust / universe / PIT** | DIY | DIY | Platform |
| When a source fails | **Fail the batch**, retry by batch | Up to caller | Up to vendor | Varies |

Point by point: [comparison](docs/comparison.md).

## Datasets

**39** registered datasets (synced with `domain/datasets.py`). Columns:
[schema](docs/datasets/schema.md); orchestration:
[catalog](docs/datasets/catalog.md).

| Category | Datasets |
|----------|----------|
| Reference (3) | `instruments` · `trading_calendar` · `trading_status` |
| Market data (8) | `daily_bars` · `index_bars` · `minute_bars` / `5m` · `trade_ticks` · `commodity_bars` · `adj_factors` · `delisting_events` |
| Corporate events (3) | `corporate_actions` · `announcement_index` · `earnings_disclosure_schedule` |
| Fundamentals / valuation (3) | `financial_statement_items` · `valuation_metrics` · `analyst_consensus` |
| Capital flow (7) | `fund_flow` · `margin_trading` · `northbound_*` · `dragon_tiger` · `block_trades` · `institutional_holdings` |
| Structure / industry (4) | `sector_members` · `index_constituents` · `industry_members` · `industry_index` |
| Macro (3) | `macro_indicators` · `market_breadth` · `economic_calendar` |
| Sentiment / rotation (6) | `sentiment_scores` · `hot_rank` · `sector_bars` · `sector_fund_flow` · `news_headlines` · `flash_news_wire` |
| Risk (2) | `share_unlock_schedule` · `regulatory_events` |

Intraday (1m / 5m / ticks) is **off by default** — see
[runbook](docs/operations/runbook.md#日内数据minute_bars--minute_bars_5m).

## Keeping it current

`cml run daily` runs every group for the day. Put it in crontab and that is the
whole daily job:

```bash
# after the close on weekdays; non-trading days skip themselves
30 16 * * 1-5  cd /path/to/lake && cml run daily >> logs/daily.log 2>&1
```

```bash
cml status          # per-dataset freshness: FRESH / STALE / EMPTY
cml serve           # http://127.0.0.1:8787 — coverage, size, tiers
cml sources         # health of the 14 upstream hosts
cml retry <run_id>  # re-run only the failed batches
```

A step that fails does not take the run with it: it is recorded as a failed
batch, everything else still lands, and `cml retry` picks up just those.

```python
from cn_market_lake.query import load

bars = load("daily_bars", start="2020-01-01", end="2025-12-31", adjust="hfq", universe="all_a")
roe = load("financial_statement_items", items=["roe"], as_of="2024-04-30")
```

The demo root (`data/cn-market-lake-demo/`) and the daily lake do not overwrite each
other. Install and scheduling:
[installation](docs/getting-started/installation.md) ·
[runbook](docs/operations/runbook.md).

## Serve it to an AI agent

`cml mcp` exposes the lake to a model (read-only; ingestion stays on the CLI).

```bash
# You have a lake — full contract
claude mcp add cn-market-lake -- cml mcp --config /abs/path/to/cn-market-lake.toml

# No lake yet — run cml demo first, then point at the demo config
# No lake at all — add --live (no adjust / universe / PIT; responses say so)
```

`--config` must be an **absolute** path. Six tools by question shape (not one
per dataset); the contract travels in the responses. Details:
[MCP reference](docs/reference/mcp.md).

## Glance at the lake

Once the lake is up, `cml serve` shows coverage, freshness, and bytes by tier
(read-only — it never writes the lake):

```bash
cml serve     # http://127.0.0.1:8787
cml sources   # health of 14 upstream hosts (probe on CLI, display on serve)
```

Details: [serve](docs/modules/serve.md) ·
[source-health](docs/operations/source-health.md).

## FAQ

**Q: How long does `cml init` take, and how much disk?**
The default (last 3 years, whole market) is about an hour and GBs.
`--profile full` starts at **2016** and measured roughly 3x that. Both fetch the
*full* cross-section — filtering symbols instead builds survivorship bias into
the lake. Going shallower than 3 years buys little: once the window is short the
per-symbol round trip dominates, so 1 year and 3 years cost about the same
while only one of them supports a multi-year factor window.

For daily bars back to 2001 — TDX does serve them, but no profile defaults
there:

```bash
cml init --since 2001-01-01                    # deep on the first run
cml backfill daily_bars --start 2001-01-01     # or deepen afterwards
```

**Q: Why store only back-adjusted factors?**
Forward-adjusted prices move with "today". Disk stores hfq only; qfq is derived
in `load(adjust="qfq")`
([ADR-0004](docs/adr/0004-store-hfq-derive-qfq-at-query.md)).

**Q: EastMoney 403 / connection reset?**
Run `cml sources --only eastmoney_push2,eastmoney_push2his` first. Daily-path
bars come from TDX, outside that WAF blast radius.

**Q: Why can't I get minute bars from two years ago?**
The vendor keeps ~95 trading days of 1m and ~491 of 5m — vendor retention, not
this lake's backlog.

**Q: Can I redistribute the data commercially?**
Code is Apache-2.0. **Bars and filings on disk are not.** See
[legal](docs/legal-and-data-sources.md).

More: [troubleshooting](docs/operations/troubleshooting.md) ·
[runbook](docs/operations/runbook.md).

## Project status and docs

Personal project: issues and PRs welcome, responses best-effort. See the current [ROADMAP](ROADMAP.md).
[CONTRIBUTING](CONTRIBUTING.md) · [SECURITY](SECURITY.md) ·
[CHANGELOG](CHANGELOG.md).

Full index: [docs/README.md](docs/README.md). Common entry points:
[MCP](docs/reference/mcp.md) ·
[installation](docs/getting-started/installation.md) ·
[catalog](docs/datasets/catalog.md) ·
[CLI](docs/reference/cli.md).

Code is [Apache-2.0](LICENSE). Landed market data remains under upstream terms; this
repo ships no data lake and grants no redistribution rights.

For papers or research reports, use the standard [CITATION.cff](CITATION.cff) and record the
package version, coverage window, and adjustment / PIT contract alongside your results.

---

If it saved you the work of building a data base layer, a ⭐ helps other A-share
researchers find it.
