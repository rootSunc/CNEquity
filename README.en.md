# A-Share Data Lake

[![CI](https://github.com/rootSunc/ashare-lake/actions/workflows/ci.yml/badge.svg)](https://github.com/rootSunc/ashare-lake/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rootSunc/ashare-lake/graph/badge.svg)](https://codecov.io/gh/rootSunc/ashare-lake)
[![PyPI](https://img.shields.io/pypi/v/ashare-lake.svg)](https://pypi.org/project/ashare-lake/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![中文](https://img.shields.io/badge/文档-中文-lightgrey.svg)](README.md)

**Stop re-fetching and hand-rolling adjust factors.** One install drops an
A-share research lake you can refresh automatically every trading day onto
local Parquet — many sources, one contract, row-level provenance, query with
DuckDB / Polars / `load()`.

<p align="center">
  <img src="docs/assets/asl-serve.png" alt="asl serve overview: FRESH/STALE/EMPTY counts, rows and bytes by tier, a 250-session coverage heatmap" width="860" />
</p>

<p align="center">
  <b>39 datasets</b> · <b>daily bars back to ~2001</b> · <b>row-level provenance</b> ·
  <b>one command to stand up a lake</b> · <b>read-only dashboard</b> · <b>MIT</b>
</p>

- **Real data in minutes:** `pip install` → `asl demo` (not a mock)
- **Daily jobs that stay up:** watermarks / retry / audit; the author runs it
  every trading day
- **Stable research semantics:** adjust · universe · PIT; 39 registered datasets;
  daily bars back to ~2001
- **Serves AI agents:** `asl mcp` exposes the whole lake as MCP tools, with the
  query contract travelling in the responses

CLI: `asl` · package: `ashare_lake` · **data layer only** (backtests stay
downstream) · opt-in intraday data (1m / 5m bars, transaction records; all off
by default) · read-only dashboard `asl serve`

<p align="center">
  <a href="#why-a-lake-rather-than-fetching-on-demand">Why a lake</a> ·
  <a href="#data-in-30-seconds">30 seconds</a> ·
  <a href="#what-you-can-ask-it">What you can ask</a> ·
  <a href="#why-not-just-akshare--tushare">vs. the alternatives</a> ·
  <a href="#self-hosted-daily-lake">Self-hosted lake</a> ·
  <a href="#serve-it-to-an-ai-agent">AI agents</a> ·
  <a href="#datasets">Datasets</a> ·
  <a href="#faq">FAQ</a>
</p>

## Why a lake, rather than fetching on demand

<p align="center">
  <img src="docs/assets/survivorship-gap.svg" alt="Same basket, same dates — the only difference is whether the delisted names are still in it" width="820" />
</p>

The same equal-weight buy-and-hold, the same start and end dates. The only
difference is **whether the names that later delisted are still in the basket**.
Use "the stocks that exist today" as a historical universe — which is all a
current-roster vendor can give you — and the 2016–2021 five-year return goes
from **5.9% to 12.0%**, twice what it was.

That is a floor, not an estimate: delisted names are carried to their last
printed bar (usually before a long suspension, well above what a holder
recovered), only names with an exact adjustment factor are counted, and this
lake's own delisted coverage may still be incomplete. All three shrink the
measured gap rather than widen it.

The point is that the error **is not visible**. Those names are not zero, they
are absent, so nothing in the output looks wrong. That is why delisted names,
adjustment factors and PIT are first-class here rather than a 40th dataset on a
coverage list.

Reproduce it on your own lake:

```bash
python scripts/survivorship_gap.py --svg docs/assets/survivorship-gap.svg
```

## Data in ~30 seconds

```bash
pip install ashare-lake
asl demo
# optional: asl demo --intraday   # also print one full 1m session
```

Five liquid names × ~30 trading days, landed in a separate data root — **not** a
full-market lake. The demo lands daily bars only; everything else in the catalog
goes through the self-hosted path below. Needs reachability to **TDX quote
hosts** (mainland egress is more reliable) — if it is down, try
`asl servers test`, or `asl demo --symbols 600519.SH,000001.SZ --days 10`.

<p align="center">
  <img src="docs/assets/asl-demo.png" alt="asl demo: phased fetch with sample daily bars" width="820" />
</p>

```python
from ashare_lake.query import load

bars = load("daily_bars", data_root="data/ashare-lake-demo", adjust="hfq")
```

```bash
asl query --config configs/ashare-lake.demo.toml --sql "
  SELECT symbol, trade_date, close, volume, source
  FROM daily_bars
  WHERE symbol = '600519.SH'
  ORDER BY trade_date DESC
  LIMIT 10
"
```

<p align="center">
  <img src="docs/assets/asl-query.png" alt="asl query: DuckDB SQL with provenance source column" width="720" />
</p>

Commands work on macOS / Linux / Windows (PowerShell or cmd). Venv and
schedulers: [installation](docs/getting-started/installation.md) /
[runbook](docs/operations/runbook.md).

## What you can ask it

Once the lake is built, these are one-liners. **The ★ rows are impossible
without a local history** — not unimplemented, impossible: an on-demand HTTP
call cannot produce a series it was never given.

| What you want to know | How you get it |
|--|--|
| How much Moutai returned over five years, adjusted | `load("daily_bars", symbols=[...], adjust="hfq")` |
| ★ Where Moutai's PE sits in its own five-year distribution | `SELECT quantile_cont(pe_ttm, 0.5) FROM valuation_metrics WHERE symbol=…` |
| ★ This factor's IC in 2018, with no look-ahead | `load("financial_statement_items", as_of="2018-04-30")` |
| ★ What the last 60 sessions looked like for stocks that delisted | `delisting_events` + `daily_bars` (delisted names are still here) |
| ★ Equal-weight market return, free of survivorship bias | `scripts/survivorship_gap.py` (the chart above) |
| Who was on today's dragon-tiger list, and who was buying | `load("dragon_tiger", start=…, end=…)` |
| How this name's margin balance has moved | `load("margin_trading", symbols=[…])` |
| Whether anything unlocks in the next three months | `load("share_unlock_schedule", start=…)` |
| Which sectors money went into today | `load("sector_fund_flow")` |
| ★ How Shenwan classified the market in 2021 | `load("industry_members")` (monthly revision history, 2020+) |
| ★ Who was in CSI 300 three years ago | `load("index_constituents")` (CNI rebalance history) |
| Today's filings | `load("announcement_index", as_of=…)` |

With [`asl mcp`](#serve-it-to-an-ai-agent) wired up, you ask these in plain
language — the query contract (adjustment, PIT, which series have no history)
comes back with the data, so you are not re-explaining it every turn.

## Why not just AkShare / Tushare

AkShare / efinance answer “how do I fetch?”; Tushare answers “cloud wide tables”;
Qlib / vn.py answer “research / trading platform”.
**ashare-lake** owns the middle layer: many sources into one contract, as a
resumable, provenance-tagged, auditable local Parquet lake.

| What you care about | **ashare-lake** | AkShare / efinance | Tushare Pro | Baostock | Qlib / vn.py |
|--|--|--|--|--|--|
| Local, resumable data base | **Lake + daily jobs** (watermarks / retry / audit) | In-memory fetch; you own orchestration | Cloud credits, not a self-hosted lake | Session fetch, no lake | Tied to platform data subsystem |
| Provenance / auditability | **Row-level provenance** + write-time schema checks | Usually no shared contract | Platform fields | No lake contract | Varies |
| Cross-source validation | **Primary curated + backup snapshots**, diffable, never silent replace | One call, one source | One vendor | One source | Varies |
| Stable research semantics | **`load()` contract**: adjust / universe / PIT `as_of` | DIY | DIY | DIY | Platform semantics |
| When a source fails | **Fail the batch**, surface it, retry by batch | Up to caller | Up to vendor | Up to caller | Varies |
| Standalone research data base? | **Yes** (lake + daily jobs + `load()`) | No — you still build landing/orchestration | Cloud tables, not self-hosted | No — session fetch | Yes, but platform-tied |

Point by point: [comparison](docs/comparison.md) (Chinese).

## Self-hosted daily lake

First `asl init` backfills (slow, multi-GB). Afterwards: incremental + read.
`load()` reads `data.root` from `configs/ashare-lake.toml` under the cwd by
default.

```bash
pip install ashare-lake
# macOS / Linux:
asl config init --data-root /Users/you/ashare-lake
# Windows: asl config init --data-root D:/ashare-lake
# macOS / Windows default workers=1; Linux example template uses 8
asl init          # layout + first backfill
asl run daily     # every trading day afterwards (no intraday data)
asl status
```

**Start shallow.** `asl init --profile quick` backfills only the last three
years, with the full cross-section. It is *shallower, never narrower* — filtering
symbols would build the bias in the chart above straight into the lake, while
fewer years is recorded honestly by `coverage_start`. Deepening later does not
mean re-running init:

```bash
asl init --profile quick                    # or --since 2019-01-01
asl backfill daily_bars --start 2016-01-01 --end <your coverage_start>
```

```python
from ashare_lake.query import load

bars = load(
    "daily_bars",
    start="2020-01-01", end="2025-12-31",
    adjust="hfq",              # None | "qfq" | "hfq"
    universe="all_a",
)
roe = load("financial_statement_items", items=["roe"], as_of="2024-04-30")
```

<p align="center">
  <img src="docs/assets/asl-load.png" alt="Python load(): read daily bars from local curated Parquet" width="720" />
</p>

```bash
asl query --sql "
  SELECT symbol, trade_date, adj_close
  FROM daily_bars_adj
  WHERE trade_date >= '2025-01-01'
"
```

> Demo lane: `data/ashare-lake-demo/` + `configs/ashare-lake.demo.toml` (pass
> that `--config` when querying).  
> Daily lane: `configs/ashare-lake.toml` from `asl config init`.  
> The two do not overwrite each other.

No extras: `pip install ashare-lake` brings every runtime source. After the
initial backfill, run the [acceptance checks](docs/operations/runbook.md)
before wiring a scheduler.

### Optional: intraday data (minute bars / transaction records)

**All off by default, and none of them ride on `asl run daily`.** Each has its
own config block and its own scheduling group, so a daily run never switches
them on by accident:

| Dataset | Source horizon | Cost | How to enable |
|--|--|--|--|
| `minute_bars` (1m) | ~**95** trading days | ~35MB/day, ~8.4GB/year full-market | `[minute_bars]` + `--group intraday` |
| `minute_bars_5m` (5m) | ~**491** trading days (the only frequency with real history) | ~6MB/day full-market | same |
| `trade_ticks` | fixed floor **2024-01-02** (does not roll with today, so the horizon grows) | ~4.5MB/day and ~1 minute for a 200-name watchlist | `[trade_ticks]` + `--group ticks` |

**Transaction records are not individual trades.** A-share Level-1 is a
3-second snapshot, so one TDX record aggregates every trade in that frame —
measured, 6–33 real trades on average. Timestamps have minute precision, so a
row is identified by `tick_seq` rather than by its time. Good for direction
splits and large-order structure; not for order-flow imbalance. Full semantics:
[catalog](docs/datasets/catalog.md#trade_ticks-是什么不是什么).

```toml
[minute_bars]
enabled = true
scope = "index:000300.SH"     # or watchlist / all
frequencies = ["1m", "5m"]

[trade_ticks]
enabled = true
scope = "watchlist"           # or index:<symbol>; "all" is refused
symbols = ["600519.SH", "000001.SZ"]
max_symbols = 200             # hard ceiling, checked before any request
```

```bash
asl backfill minute_bars_5m --start 2024-08-01 --end 2026-07-31
asl backfill trade_ticks --symbols 600519.SH --start 2026-07-01 --end 2026-07-31
asl run daily --group intraday    # daily refresh: each in its own group
asl run daily --group ticks
```

Disk and runtime:
[runbook](docs/operations/runbook.md#日内数据minute_bars--minute_bars_5m).
Compliance boundary: [legal and data sources](docs/legal-and-data-sources.md).

## Look at the lake: `asl serve`

A read-only dashboard. Coverage, freshness, source mix, audit findings and run
history live here (that is the screenshot at the top); running, retrying and
cleaning stay with the CLI.

```bash
asl serve                      # http://127.0.0.1:8787
asl serve --port 9000 --config configs/ashare-lake.toml
```

The heatmap counts gaps in **each dataset's own period**, not in days — a
year-partitioned dataset is not reported as "missing 364 days" because one
directory covers the year.

A dataset opens into three tabs: **state** (coverage, gaps, provenance,
findings, recent batches), **metadata** (contract / schema / primary key /
horizon), **data** (real rows, with adjust and PIT controls).

<p align="center">
  <img src="docs/assets/asl-serve-dataset.png" alt="trade_ticks metadata tab: primary key symbol/trade_date/tick_seq, source horizon from a fixed 2024-01-02 floor, row grain 分笔" width="860" />
</p>

Everything on that tab comes from `domain/datasets.py` and
`domain/schemas.py` — the dashboard keeps no second copy of the contract,
because a second copy is a copy that drifts.

Binding to a non-loopback address requires `--token`. Details:
[serve module docs](docs/modules/serve.md).

## Serve it to an AI agent

`asl serve` shows the lake to a person; `asl mcp` shows it to a model. Same
read-only stance — ingestion stays on the CLI, where a person runs it.

```bash
claude mcp add ashare-lake -- asl mcp --config /abs/path/to/ashare-lake.toml
```

**Six tools, not 39.** An agent picks from a flat list every turn, so one tool
per dataset would spend most of the context window on names it will not call.
These are cut by question shape, and the dataset becomes an argument:

| Tool | What it is for |
|--|--|
| `describe_lake` | What is here, how far back, and the rules that make an answer correct |
| `resolve_symbol` | "茅台" → `600519.SH`, delisted names included |
| `query_bars` | Daily / index / minute bars, with `adjust` and `universe` |
| `query_fundamentals` | Statement items; `as_of` is **required** |
| `query_dataset` | Any other dataset |
| `run_sql` | One read-only DuckDB SELECT, for aggregation across datasets |

**The contract travels in the responses, not in the docs** — a model does not
read `docs/`. Bars without `adjust` come back with a warning; bars with it
report how many rows had no factor and silently used 1.0; `query_fundamentals`
refuses to default `as_of` and says why. Every payload carries `total` /
`truncated`, so a page of 200 out of 4,300 rows cannot be averaged and reported
as the market's.

`run_sql` accepts exactly one SELECT, decided by DuckDB's own parser rather than
a regex — the lake ingests `news_headlines` and `flash_news_wire`, vendor text
nobody here wrote, so SQL reaching the tool can be shaped by ingested content.

Questions this answers that a fetch-on-demand toolkit structurally cannot: "the
percentile of Moutai's PE over the last five years", "this factor's IC in 2018,
without look-ahead", "what the last 60 sessions looked like for stocks that
delisted". Not *not built yet* — **impossible without a lake**.

**No new dependencies**: the stdio JSON-RPC loop is hand-written, because the
official `mcp` SDK resolves to 15 additional packages including a second HTTP
stack. Details: [MCP reference](docs/reference/mcp.md).

## Source health: `asl sources`

Is EastMoney blocking you today? Did Shenwan's certificate expire again? These
sources are not this project's: AkShare, every fetch-on-demand skill, and your
own scraper all hit the same dozen endpoints, and when one changes there is
nowhere to look it up — you spend an afternoon suspecting your own code first.
This lake sweeps them every trading day anyway, so one extra request per source
answers it.

```bash
asl sources --vantage cn     # probe once; the report lands in meta/source_health/
asl serve                    # -> http://127.0.0.1:8787/source-health
```

**Probing is a CLI action, viewing is the dashboard.** The dashboard is
read-only and will not reach out to a dozen third-party hosts on your behalf —
the same reason it does not trigger ingestion.

Three rules make the table trustworthy:

- **HTTP 200 is not "up".** EastMoney answers a challenge page with 200, Sina
  answers an empty array with 200. Every probe asserts on the *payload*, and
  `empty` is its own status — it looks healthier than a failure and is worse,
  because it truncates a backfill silently.
- **`blocked` is not `down`.** Several sources refuse non-mainland egress at the
  WAF, so the same host can be green from Shanghai and red from Virginia at the
  same second. `--vantage` records which egress a report came from, and vantages
  are rendered side by side, never merged.
- **One probe is not an SLA.** One request per source, run serially — a health
  check should not cause the outage it exists to observe.

Details and how to add a source: [source health](docs/operations/source-health.md).

## Datasets

All **39** registered datasets (36 curated + 3 derived; kept in sync with
`domain/datasets.py`). Names are the first argument to `load()`. Columns:
[schema](docs/datasets/schema.md); orchestration:
[catalog](docs/datasets/catalog.md).

| Category | Datasets (`load()` name · meaning) |
|----------|-------------------------------------|
| Reference | `instruments` security master · `trading_calendar` trading calendar · `trading_status` suspensions / ST |
| Market data | `daily_bars` daily bars (unadjusted) · `index_bars` index bars · `minute_bars` 1m (opt-in) · `minute_bars_5m` 5m (opt-in) · `trade_ticks` transaction records (opt-in; 3-second snapshot aggregates, not tick-by-tick) · `commodity_bars` commodity main-continuous (opt-in) · `adj_factors` adjust factors (derived) · `delisting_events` delisting endings (derived) |
| Corporate events | `corporate_actions` corp actions (XDXR) · `announcement_index` announcement index · `earnings_disclosure_schedule` earnings disclosure timetable |
| Fundamentals / valuation | `financial_statement_items` financial statement items (PIT) · `valuation_metrics` valuation metrics · `analyst_consensus` analyst consensus |
| Capital flow | `fund_flow` stock fund flow · `margin_trading` margin trading · `northbound_flows` northbound flows · `northbound_holdings` northbound holdings · `dragon_tiger` dragon-tiger list · `block_trades` block trades · `institutional_holdings` institutional holdings |
| Structure / industry | `sector_members` sector members · `index_constituents` index constituents · `industry_members` industry membership · `industry_index` industry index (derived) |
| Macro | `macro_indicators` macro indicators · `market_breadth` market breadth · `economic_calendar` economic calendar (placeholder; source retired) |
| Sentiment / rotation | `sentiment_scores` sentiment scores · `hot_rank` popularity rank · `sector_bars` sector bars · `sector_fund_flow` sector fund flow · `news_headlines` news headlines · `flash_news_wire` 24/7 flash wire |
| Risk | `share_unlock_schedule` share unlock schedule · `regulatory_events` regulatory events |

**On-demand** (not on the curated daily path): `stock_news`, `research_reports`, etc. — see [catalog](docs/datasets/catalog.md).

## Architecture

<p align="center">
  <img src="docs/assets/architecture-overview.png" alt="ashare-lake architecture: sources → ASL Daily Pipeline → staging/curated/derived → load()/DuckDB/Polars" width="900" />
</p>

On-disk layout under the daily lake's `data.root`:

```
{data_root}/
  curated/   {dataset}/{partition}=v/part-*.parquet
  derived/   adj_factors/...
  staging/   raw landing per run (cleanable after compact)
  meta/      manifest, quality findings, watermarks, on-demand cache
  duckdb/    ashare-lake.duckdb
```

## Known limitations

- **Survivorship bias:** delisted names need `asl delisted backfill` + `repair`
  before trusting return series
- **Network:** some HTTP / sector backfills need mainland egress; demo needs TDX
- **Intraday horizon:** TDX keeps ~95 trading days of 1m and ~491 of 5m; older
  windows return nothing — see [catalog](docs/datasets/catalog.md)
- **Year/month partitions** (e.g. `index_bars`): prefer `asl query` / `load()` so
  hive labels do not collide with real dates — see
  [lake-layout](docs/architecture/lake-layout.md)

More: [runbook](docs/operations/runbook.md) ·
[troubleshooting](docs/operations/troubleshooting.md) ·
[legal](docs/legal-and-data-sources.md).

> Getting-started docs are Chinese-first; this English README is the short path.
> Full index: [docs/](docs/README.md).

## FAQ

**Q: How long does `asl init` take, and how much disk?**
A full backfill is hours and multiple GB. If you do not want to wait,
`asl init --profile quick` fetches only the last three years — for the *full*
cross-section. Filtering symbols instead would build the survivorship bias in
the chart above straight into the lake, and a missing name looks exactly like a
name that never traded, where fewer years is recorded honestly by
`coverage_start`. Deepen later with `asl backfill daily_bars --start 2016-01-01`;
no need to re-run init.

**Q: Why store only back-adjusted factors and derive forward-adjusted at query time?**
Forward-adjusted prices move with "today" — compute one now and again next week
and the same 2015 bar has different numbers. What lands on disk has to be
immutable, so only hfq is stored and qfq is derived in `load(adjust="qfq")` as
`hfq_factor / hfq_anchor` ([ADR-0004](docs/adr/0004-store-hfq-derive-qfq-at-query.md)).
A price written adjusted cannot be undone; a factor series can be recomputed.

**Q: Does `universe="all_a"` already exclude historical ST names?**
**No.** `trading_status` covers ST/suspension from roughly 2016, while
`daily_bars` reaches 2001. Windows before that are not ST-filtered, so a long
backtest includes ST names in its early cross-sections. `asl audit` reports the
gap as `trading_status_coverage_start` — do not assume it is clean.

**Q: EastMoney is returning 403 / resetting connections. Now what?**
Run `asl sources --only eastmoney_push2,eastmoney_push2his` first — if that is
red too, it is not your code. The EastMoney hosts
(push2 / push2his / datacenter) share one WAF and go dark together, and
`push2his` is the most sensitive to non-mainland egress (this project ships
Chrome TLS impersonation and CDN stickiness for it). Bars on the daily path come
from TDX, which is a different protocol and a different blast radius.

**Q: baostock stops responding partway through a sweep.**
The free tier's binding constraint is cumulative volume, not spacing: measured,
about 43 queries in a session earns a blacklist with a ~40 minute cooldown. The
defaults (`batch_size=20`, `batch_rest_seconds=120`) carried 1,658 symbols of
valuation history without a single block. Do not raise them for speed.

**Q: Why can't I get minute bars from two years ago?**
The vendor keeps about **95** trading days of 1m and **491** of 5m. That is the
vendor's retention, not this lake's backlog — an earlier window returns nothing,
not less, and no backfill source extends it. `asl backfill minute_bars --start`
refuses a window before the horizon instead of scanning a day for nothing.
15m/30m/60m are not stored because they aggregate from 5m exactly.

**Q: Why was AkShare dropped after v0.3?**
It wraps the same public EastMoney / THS / Sina APIs this project already called
directly, so it bought a parsing layer, 14 transitive packages and a mini-racer
collision rather than a second source. Everything is direct HTTP now, and the
TDX binary client is vendored (mootdx upstream is dormant; the protocol is not).

**Q: Why is `workers` pinned to 1 on macOS?**
The TDX client is not fork-safe and a ProcessPool dies with BrokenProcessPool,
so `asl config init` writes `workers = 1` there. Windows uses spawn and does not
have that problem — its default of 1 is merely conservative, and you can raise
it once `asl doctor` is clean. The Linux template ships 8.

**Q: Can I redistribute the data commercially?**
The code is MIT. **The bars and filings on disk are not** — they remain subject
to their upstream terms. This repo ships no data lake and grants no
redistribution right. See [legal](docs/legal-and-data-sources.md).

## Project status

Personal project: issues and PRs welcome, responses best-effort.
[CONTRIBUTING](CONTRIBUTING.md) · [SECURITY](SECURITY.md). Docs are
Chinese-first; [CHANGELOG](CHANGELOG.md) and [ADRs](docs/adr/) stay in English.

## Docs

Full index: [docs/README.md](docs/README.md). Common entry points:
[installation](docs/getting-started/installation.md) ·
[quickstart](docs/getting-started/quickstart.md) ·
[catalog](docs/datasets/catalog.md) ·
[runbook](docs/operations/runbook.md) ·
[CLI](docs/reference/cli.md) ·
[MCP](docs/reference/mcp.md) ·
[serve dashboard](docs/modules/serve.md).

## License

Code is [MIT](LICENSE). Market data and announcements you land locally remain
subject to upstream terms; this repo ships no data and grants no redistribution
rights [legal](docs/legal-and-data-sources.md).

---

If it saved you the work of building a data base layer, a ⭐ helps other A-share
researchers find it.

[![Star History Chart](https://api.star-history.com/svg?repos=rootSunc/ashare-lake&type=Date)](https://star-history.com/#rootSunc/ashare-lake&Date)
