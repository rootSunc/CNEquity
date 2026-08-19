# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
the project adheres to [Semantic Versioning](https://semver.org/).

## [0.7.2.1] — 2026-08-19

**主要变更：修复 `trading_status`（ST / 停牌）数据获取，新增 baostock 备份数据源。**

### Added

- **`cne config validate` now rejects malformed `[[failover.datasets]]` entries.** Unknown dataset names, unrecognized primary/backup sources, and duplicate entries fail validation instead of silently disabling the trading_status failover.
- **`trading_status` failover to a baostock daily snapshot.** When the EastMoney
  ST/suspension fetching fails, SH/SZ fall back to `query_all_stock(day)`
  (single-request snapshot) with a freshness gate, per-symbol fill
  classification (a previously non-tradable name is never washed to `normal`),
  counted BJ defaults, dynamic provenance, and `source_snapshots` audit trail.
  Opt-in via `[[failover.datasets]] name = "trading_status"`; removing the entry
  restores the previous behavior.
- **EastMoney suspension fetch adapted to the 2026-08 datacenter contract.**
  `RPT_CUSTOM_SUSPEND_DATA_INTERFACE` now requires a `DATETIME`/`MARKET`
  filter (five markets, deduplicated) and the renamed
  `SUSPEND_START_DATE`/`SUSPEND_END_TIME` columns; an all-market empty batch
  fails loudly instead of masquerading as "no suspensions".

### Fixed

- **`trading_status` daily runs no longer fail end-to-end when EastMoney
  push2/datacenter legs are unreachable** (IP throttle or contract drift):
  with the failover entry configured the step degrades to the baostock
  snapshot and is tagged `warning` with audit findings.

## [0.7.2] — 2026-08-16

### Fixed

- **Source adapters fail closed on truncated or malformed upstream payloads.**
  Calendar, CNINFO, EastMoney, Sina, TDX, THS, and Baostock now reject incomplete
  pages and contract-breaking rows instead of writing them through.
- **Dataset watermarks and freshness follow coverage, not the newest file.**
  Session-dense datasets stop the watermark at the first calendar gap, so a
  sparse tip can no longer look complete.
- **Delisted-stock backfill coverage is accurate and resumable.** Receipts and
  repair track which symbols actually landed, and a retry continues from the
  unreceipted remainder.
- **No-trade placeholders no longer leak into derived datasets.** Adjustment
  factors, industry indexes, market breadth, sentiment, and trading-status
  history skip placeholder rows that are not real sessions.
- **Ingestion steps reject partial or malformed snapshots.** Bars, capital,
  fundamentals, structure, and related steps refuse incomplete batches instead
  of compacting them as success.
- **Quality audits no longer treat placeholders or partial reports as coverage.**
  Dataset checks, cross-checks, derived checks, PIT checks, and the
  historical-validity contract require real rows and a complete audit artifact.
- **The query layer is placeholder-aware and PIT-correct.** Partition scans,
  universe membership, and `load()` drop no-trade placeholders and honor as-of
  semantics.
- **CLI backfill recovery and reporting match what actually landed.** Progress,
  receipts, and exit status no longer claim a completed window when only part
  of the request was stored.
- **ST coverage checkpoints survive an all-A universe growing.** A later run
  with a larger compatible symbol set inherits completed symbols instead of
  restarting from scratch.

## [0.7.1] — 2026-08-16

### Fixed

- **Cross-process rate limiting no longer sleeps while holding the file lock.**
  Requests reserve a shared `next_allowed_at` slot in a short lock transaction,
  then wait after releasing the lock. Lock acquisition now has a bounded timeout
  and fails explicitly instead of bypassing the limiter or hanging forever.
- **Corporate-actions backfill is resumable at symbol-batch granularity.**
  Successful chunks are staged immediately and recorded in the manifest; a
  retry skips those chunks and fetches only the unreceipted symbols.
- **Hardened source and storage boundaries across the ingestion pipeline.**
  Malformed or incomplete adapter responses, invalid TDX payloads, unsafe
  backfill windows, incomplete pipeline results, mixed partition layouts, and
  non-atomic published artifacts are now rejected or handled explicitly.
- **Aligned query and derived-data semantics.** Shard merging, primary-key
  deduplication, partition reads, and industry/sector computations now share
  the same canonical behavior.

### Changed

- Added rate-limit and corporate-actions recovery observability, manifest
  heartbeat updates, and release/configuration documentation for the new
  failure and retry behavior.

## [0.7.0] — 2026-08-15

### Changed

- **Renamed the project from `ashare-lake` to `CNEquity`.** The old name
  collided with established projects in the same space (mpquant/Ashare,
  AKShare) and did not surface in searches for "A股 数据湖". Package:
  `pip install cnequity` (`ashare-lake` on PyPI will no longer be updated).
  CLI: `cne` (was `asl`). Import: `from cnequity...` (was `from
  ashare_lake...`). Config/data defaults: `cnequity.toml`, `data/cnequity/`
  (was `ashare-lake.toml`, `data/ashare-lake/`) — existing local configs and
  data directories are not renamed automatically.

## [0.6.0] — 2026-08-10

### Fixed

- **Shenwan (`sw`) fetches failed TLS verification on every attempt.**
  `swsresearch.com` sends its leaf certificate and no intermediate — one cert
  deep on every handshake — so certifi cannot build a path to a root it trusts.
  Browsers and macOS curl hide this by following the leaf's Authority
  Information Access extension; Python does not. Measured httpx 0/5,
  curl_cffi 0/6. The public DigiCert intermediate now ships with the package
  (`asl sources --only sw`: 0/5 → **5/5**), which restores the path without
  weakening verification — the root must still be trusted and the hostname must
  still match. Affects `industry_members` backfill, and it is a property of the
  server, so mainland users hit it identically.
- **`core` could not finish inside its schedule slot, silently costing the next
  group.** Scheduled `daily*` groups share one non-blocking `daily_ingestion`
  lock: a group still running when the next fires does not queue, the next one
  aborts. Full-market `daily_bars` measured 543ms/symbol — ~49min for ~5400
  symbols — against a 30-minute gap to `capital`. The shipped schedule now
  gives `core` 60 minutes and spaces the rest off measured durations, and the
  collision message says a group is being skipped instead of naming an internal
  lock.

- **`share_unlock_schedule` failed on every run.** EastMoney's datacenter now
  rejects range comparisons on date columns outright — `参数预处理错误:
  org.antlr.v4.runtime.InputMismatchException (code=9501)` — so the
  `(FREE_DATE>=…)(FREE_DATE<=…)` filter took the step from working to raising
  with no change on this side. It now pages the report newest-first and stops
  at the first page that ends before the window, then applies the horizon
  locally: 63 pages of 500 down to ~7, measured 141.8s → **13.7s**, with the
  result verified row-for-row against a full scan.
- **`commodity_bars` burned 151s per run to return nothing.** The fail-fast
  predicate was inverted: it retried exactly the transport failures
  `is_transport_fail_fast` says a retry cannot fix, and gave up immediately on
  the transient ones. `clist` and `datacenter` both break on the same
  predicate. Measured 151.2s → **17.5s** against an unreachable push2his.
- `fetch_datacenter` takes an optional `stop_after` predicate for early-stopping
  a sorted report. It suppresses the declared-`count` completeness guard, since
  a short read is the point; without it the guard is unchanged.

- **`northbound_flows` was never northbound.** It read
  `push2his /stock/fflow/kline/get?secid=1.000001` and mapped f52 → 沪股通 and
  f53 → 深股通. Those fields are 上证指数's 主力净流入 and 小单净流入 — two legs
  of a zero-sum decomposition, which is why the two "channels" were opposite-signed
  on 13 of 14 days and why 3 of 28 rows exceeded the exchange's own 520亿 daily
  quota cap, topping out at 777.9亿. When that host was unreachable the fallback
  wrote `kamt`'s northbound fields, which have been a hard zero since the feed
  retired — so the column held wrong numbers on good days and invented flat
  sessions on bad ones. It now reads 沪深港通资金历史
  (`RPT_MUTUAL_DEAL_HISTORY`, `MUTUAL_TYPE` 001/003), which also fills
  `buy_amount` / `sell_amount` — previously hardcoded to 0.0.

  **Existing rows are wrong and must be replaced**: drop the dataset's
  partitions and its watermark, then `asl backfill northbound_flows`.

- **`northbound_flows` gains real history: 2014-11-17 → 2024-08-16.** The
  exchanges stopped publishing daily northbound net flow after 2024-08-16, so
  rows from 2024-08-19 on carry a null amount. Those are **dropped, not
  zero-filled** — a zero would claim a flat session where no figure exists.
  The watermark consequently freezes at 2024-08-16 and `asl status` reports the
  dataset STALE forever; that is the source's state, not a pipeline fault.

  The step also fetches the whole outstanding window in one request rather than
  one request per session, because a frozen watermark would otherwise grow the
  daily gap window without bound.

### Changed

- **Synchronized the CLI reference with `asl demo --research`.** The public command table now
  documents the Sina hfq comparison and no longer describes removed EastMoney sticky state.

- **Added maintenance automation.** Weekly source-health probes publish a clearly labelled
  GitHub Actions / overseas report, and version tags now require a matching package version,
  `twine check`, and a clean distribution build before optional PyPI Trusted Publishing.

- **Published a searchable documentation site and copy-paste research Recipes.** The GitHub Pages
  build covers first-run onboarding, adjustment semantics, PIT financials, DuckDB / Polars, MCP,
  and operations; pull requests build it in strict mode so navigation drift is visible before merge.

- **Modernized package license metadata.** The build now uses the SPDX license expression and
  `project.license-files`, avoiding deprecated setuptools tables while preserving the Apache-2.0
  license and bundled NOTICE file.

- **The EastMoney client is plain `httpx` again, and `[sources.eastmoney].proxy`
  is the one overseas lever.** Removed the curl_cffi Chrome-JA3 impersonation,
  the `CURLOPT_RESOLVE` CDN pinning with its DoH / `dig` / hardcoded-seed-IP
  ladder, the sticky last-good-edge file (`meta/state/push2his_endpoint.json`),
  and the egress circuit breaker that existed to make that ladder's failure
  mode affordable — about 430 lines that bought nothing for a mainland route
  and broke whenever EastMoney rotated an edge or a TLS fingerprint. The proxy
  now covers every EastMoney host rather than only push2his kline, and
  `httpx.ProxyError` joins the fail-fast set so a dead proxy stops a batch
  instead of burning its retry budget.
- **The shipped example config is paced for a mainland route**
  (`min_interval_seconds` 3.0 → 0.5, `batch_size` 15 → 50,
  `batch_rest_seconds` 60 → 5), which takes a full ~991-board `sector_bars`
  sweep from ~2h to ~10min. The previous values were sized for a hostile
  overseas egress and every mainland user was paying for them.

### Removed

- **`asl push2his remember` / `asl push2his probe`.** Both existed only to
  drive the sticky CDN-edge machinery above. Overseas users set
  `[sources.eastmoney].proxy` and verify with
  `asl sources --only eastmoney_push2,eastmoney_push2his`.

### Docs

- **`sector_bars` is documented as a 同花顺 dataset, which it has been for a
  while.** The catalog, the steps reference, the CLI reference and the EastMoney
  adapter page all still described it as EastMoney clist daily plus a
  `push2his` kline backfill under `backfill_source="eastmoney_kline"`. The
  registry says `ths`, the step raises when `[sources.ths]` is disabled, and
  daily and history are deliberately one source — mixing them once spliced two
  index bases into one series and produced a fake +79% median jump across 439
  boards. The troubleshooting entry also quoted a log line no code emits, and
  pointed at `[sources.eastmoney].proxy`, which does nothing for this dataset.
### Removed (config)

- **`[sources.eastmoney].batch_size` / `.batch_rest_seconds` are gone.** They
  were parsed into `Config` and never read by anything — the batch cool-down is
  a baostock mechanism — so they read as pacing that was wired up while every
  EastMoney sweep ran on `min_interval_seconds` alone. Unknown keys are
  ignored, so a config still carrying them loads unchanged; delete the two
  lines when convenient.

## [0.5.0] — 2026-08-03

### Changed

- **License: MIT → Apache License 2.0.** Project source is now under
  [Apache-2.0](LICENSE); third-party notices (including vendored tdxpy, still
  MIT) live in [NOTICE](NOTICE). Landed market data remains outside the
  software license — see [legal](docs/legal-and-data-sources.md).
- **README hero rebranded to ASL · cnequity.** Shorter pitch, survivorship
  chart and demo up front; cropped serve scorecard and architecture diagram in
  their own sections. No upgrade step for existing lakes.
- **`asl servers test` and `asl push2his` are off the top-level command list.**
  Both keep working — they are in the quickstart and in runbooks — but `servers
  test` is now an alias for `asl sources --only tdx_protocol`, which asserts that
  real bars came back rather than that a socket opened, and `push2his` debugs one
  CDN host. They were competing for attention with the commands that make up the
  pipeline's actual state machine.

### Added

- **Standard citation metadata.** `CITATION.cff`, the docs citation page, and the package metadata
  URL make it easy to cite a versioned research dependency without implying data redistribution rights.

- **`asl mcp`: the lake as an MCP server, read-only, over stdio.** Six tools cut
  by question shape rather than one per dataset — `describe_lake`,
  `resolve_symbol`, `query_bars`, `query_fundamentals`, `query_dataset`,
  `run_sql`. An agent picks from a flat list every turn, so 39 dataset tools
  would spend most of the context window on names it will not call and still
  leave it guessing which one answers the question.

  **The query contract travels in the responses, not in the docs**, because a
  model does not read `docs/`. `describe_lake` returns the adjustment, PIT,
  `snapshot_only` and `universe` rules; a bar query without `adjust` comes back
  with a warning; one with `adjust` reports how many rows had no factor and
  silently used 1.0; `query_fundamentals` refuses to default `as_of` and says
  why. Every payload carries `total` / `returned` / `truncated`, so a page of
  200 out of 4,300 rows cannot be averaged and reported as the market's.

  `run_sql` accepts exactly one SELECT, decided by DuckDB's own parser rather
  than a regex: the lake ingests `news_headlines` and `flash_news_wire`, vendor
  text nobody here wrote, so SQL reaching the tool can be shaped by ingested
  content. A read-only connection alone would still allow `COPY ... TO`.

  **No new dependencies.** The stdio JSON-RPC loop is ~200 lines rather than the
  official `mcp` SDK, which resolves to 15 additional packages — cryptography,
  pyjwt and truststore for an OAuth flow a local server never performs,
  opentelemetry for tracing nothing exports, and a second HTTP stack beside the
  pinned httpx. `pip install cnequity` with no extras stays intact.

- **`asl init --profile quick` / `--since`** — a first backfill that is
  *shallower, never narrower*. `quick` fetches the last three calendar years for
  the full cross-section; filtering symbols instead would build the survivorship
  bias this lake exists to repair straight into it, and a missing name looks
  exactly like a name that never traded, where fewer years is recorded honestly
  by `coverage_start`. The window is written to the run metadata so `--resume`
  reuses it instead of silently reverting to full depth days later.

- **`asl sources`: a local A-share source health board.** Fourteen probes over
  the endpoints this lake depends on — TDX, three EastMoney hosts, Sina, CNINFO,
  both THS hosts, baostock, both exchanges, Shenwan, PBoC, NBS. The report lands
  in `meta/source_health/<vantage>.json` and `asl serve` renders it at
  `/source-health`. These endpoints are not this project's: AkShare, agent skill
  files and hand-rolled scrapers all depend on them, and there is nowhere to look
  up whether one changed.

  Probing is a CLI action and viewing is the dashboard. A GET that reaches out to
  a dozen third-party hosts is what the dashboard's read-only stance exists to
  prevent — the same reason nothing there triggers ingestion.

  **HTTP 200 is not "up".** EastMoney answers a challenge page with 200, Sina
  answers an unknown symbol with an empty array, THS answers a rate-limited page
  with 200 and no rows. Every probe therefore asserts on the payload — `total`,
  `klines`, the `PK` magic of an xlsx — and `empty` is its own status, because a
  source answering politely with nothing is what truncates a backfill silently
  and looks healthier than a failure.

  **Vantage is recorded and never merged.** Several of these refuse non-mainland
  egress at the WAF, so one column can be green and another red for the same host
  at the same second. `--vantage` labels each report and the page renders them
  side by side; merging would invent a fact neither probe established.

  Probes call the adapters' own URL constants and clients, so the fragile part is
  the part under test. They run serially and once per source: a health check that
  trips a rate-limit ban would cause the outage it exists to observe.

- **`asl mcp --live`: MCP for an agent with no lake.** Where the lake holds
  nothing, symbol lookup and unadjusted daily bars are fetched from the vendor on
  demand and never written. Everything else refuses by name with the reason —
  fundamentals because a vendor returns today's view of a restated figure and
  there is no honest `as_of`; `run_sql` because it queries parquet that live mode
  does not produce. Every payload carries `origin: "lake" | "live"`, both
  labelled, so a missing field cannot default to "lake". Off unless asked: a user
  whose lake is broken must get "no parquet data" and go fix it. Capped at 50
  symbols and 800 days per call, with `symbols` required.

- **Progress while a long fetch runs.** `asl init` and `asl run daily` set up no
  logging at all, so an hours-long backfill printed nothing until the closing
  JSON — indistinguishable from hung, and a process that looks hung gets killed
  along with the hours it had banked. The steps and the worker pool already
  logged; nothing was listening. Adds a line per batch from the parent, with
  rows, elapsed and a rough estimate. `--quiet` opts out.

- **`survivorship_gap.py --lang`** — the chart's labels are localised, so the
  Chinese README embeds a Chinese chart. Same numbers, same geometry.

- **`scripts/survivorship_gap.py`** — measures the bias on the lake's own bars
  and emits a dependency-free SVG. Same equal-weight basket, same dates, the
  only difference being whether delisted names are still in it: 2016–2021 reads
  5.9% complete against 12.0% survivors-only. A floor rather than an estimate —
  delisted names are carried to their last printed bar, only exact-adjustment
  names count, and the lake's own delisted coverage may be incomplete, all of
  which shrink the measured gap.

- **`trade_ticks`: transaction records (分笔), opt-in and watchlist-scoped.**
  Two new TDX wire commands (`0x0fc5` same-session, `0x0fb5` historical),
  an adapter that assembles a session whole or not at all, and the dataset
  with its own `[trade_ticks]` config block, `ticks` step group and quality
  checks. Off by default and on no schedule.

  **These are not tick-by-tick trades.** A-share Level-1 is a 3-second
  snapshot, so one row aggregates however many real trades landed in that
  frame — measured, 6.3 on average for 600519 and 33.4 for 000001. The wire
  timestamp has minute precision (the protocol never carried seconds), so rows
  are keyed by `tick_seq`, their position in the session. `direction` is TDX's
  own tick-rule inference, not an exchange field, and its `after_hours` value
  covers 15:05–15:30 fixed-price trading, which the exchange's daily volume
  does not count.

  History reaches back to 2024-01-02 for every symbol — a *fixed floor*, not
  the rolling per-symbol bar count the minute bars have, which is why
  `DatasetSpec` gains `history_floor_date`. Cost is ~1.85 requests and ~2,700
  rows per symbol-session, ~8.4 bytes a row on disk: about a minute and 4.5MB
  a session for a 200-name watchlist. `[trade_ticks].scope = "all"` is refused
  at config validation and `max_symbols` (200) stops a resolved scope before
  the first request.

- **`DatasetSpec.history_floor_date`** — a source edge expressed as a calendar
  date rather than a rolling trading-day count. `earliest_available()` prefers
  it, and the backfill guard drops the "narrow your scope" advice when it
  fires, since no scope reaches past a fixed floor. Both fields now reach the
  dashboard and `/api/datasets/{name}`, which previously could not express
  which mechanism produced an `earliest_available` — and so called a
  date-limited source unlimited.

- **`DatasetSpec.row_grain`** — what one row covers (`1m` / `5m` / `tick`),
  descriptive only. Separate from `intraday_frequency`, which drives fetch,
  checks and the reader and which `trade_ticks` deliberately leaves unset; with
  only the latter, intraday transaction records were displayed as a daily
  dataset. The dataset panel's 日内频率 fact is now 行粒度.

### Fixed

- **The dashboard's own tests depended on the wall clock.** Freshness is judged
  against the last trading day *today*, so a fixture with fixed dates passed on
  the day it was written and reported every dataset stale from then on.

- **A backfill with a non-default start was silently lenient.** Whether a bar
  batch fetches strictly — raising on a mid-pagination failure instead of
  keeping the pages that arrived — was inferred from `start == 2016-01-01`,
  which was the only start a backfill ever had. `asl init --since` picks its
  own, so `_window_backfill` now asks the orchestrator's `_backfill` flag and
  keeps the date test as a fallback. Without this, the shallow init path would
  have been the one that loses a symbol's older years without saying so.

- **Intraday backfill no longer date-slices tip-paged TDX walks.** Minute-bar
  sources page backwards from today, so a 10-day chunk sitting near the horizon
  still had to re-fetch every newer page — CSI300 1m paid ~8× the necessary
  wire traffic. `minute_bars` / `minute_bars_5m` now use
  `backfill_chunk_symbols=200` (one tip→horizon walk per symbol, compacted per
  batch). `resume_from_symbol` replaces date `resume_from` on that path.
  Default `[minute_bars].fetch_workers` raised to 4 (still capped at ~10 req/s
  by the cross-process limiter).

## [0.4.0] — 2026-08-01

### Upgrading from 0.3.x

1. **`daily_bars.volume` is always 股 (`data_version = v2`).** Lakes written under
   0.3.x mix units by source and need a one-off rewrite before trusting turnover
   or liquidity factors:

   ```bash
   scripts/migrate_daily_bars_volume_v2.py --config configs/cnequity.toml --dry-run
   scripts/migrate_daily_bars_volume_v2.py --config configs/cnequity.toml --apply
   ```

   Back up curated first; the script is idempotent and does not restamp
   `fetched_at`.

2. **AkShare is gone.** Delete `[sources.akshare]` from any hand-edited config
   (the example template no longer has it). Add `[sources.pboc]` for 社融, and
   optionally `[sources.nbs]` / `[sources.exchange]` for publisher cross-checks
   in `asl audit`. Orphan packages left behind by pip/uv:

   ```bash
   pip uninstall akshare mini-racer py-mini-racer
   ```

   `asl doctor --fix` is removed — it only repaired the mini-racer collision.

3. **Macro self-heal.** The next `macro_indicators` run rewrites the bad
   `m2_yoy` history and backfills `social_financing` from the PBOC; no separate
   migration. Rows keep the newest `fetched_at` per `(indicator_id, obs_date)`.

4. **Intraday is opt-in.** `[minute_bars].enabled` defaults to `false` and is
   not on the daily waves. Enable it, then `asl run daily --group intraday`
   (or `asl demo --intraday` / `asl backfill minute_bars_5m …`). TDX keeps ~95
   trading days of 1m and ~491 of 5m — older windows return nothing.

### Added

- **`minute_bars` / `minute_bars_5m` — opt-in intraday bars.** Separate datasets
  (one frequency each) because the source keeps 95 trading days of 1m against
  491 of 5m, and a dataset carries one watermark, one `coverage_start` and one
  horizon. Registered with schema, PK `(symbol, trade_date, bar_time, frequency)`,
  day partitions, `steps/intraday.py` (`group="intraday"`), `load()` with
  `adjust="qfq"/"hfq"`, and four audit checks. Off by default
  (`[minute_bars].enabled = false`); full-market 1m is ~35MB/day (8.4GB/year)
  against 468MB for the entire daily lake 2001–2026.
  `[minute_bars].scope` defaults to `index:000300.SH` (~2MB/day at 1m).

  `bar_time` is the bar's **closing** minute (TDX labelling): a full session is
  240 bars over 09:31–11:30 and 13:01–15:00; the 15:00 bar carries the closing
  auction. Prices are unadjusted; adjustment joins the day's factor at query
  time. 15m/30m/60m are not stored — they aggregate exactly from 5m
  (`docs/datasets/catalog.md` has the resampling snippet).

- `asl demo --intraday`, `[minute_bars].fetch_workers` (threaded concurrent TDX
  connections; does not raise the request rate), `asl backfill <intraday>
  --symbols`, `DatasetSpec.history_horizon_days` / `backfill_chunk_days`, and
  intraday audit checks (`minute_bars_off_session`,
  `minute_bars_trade_date_mismatch`, `minute_bars_session_coverage`,
  `minute_bars_daily_reconciliation`). `asl backfill` refuses a `--start`
  before the source horizon.

- **Publisher cross-checks (`quality/authority_checks.py`).** Reach the
  publisher, not only the lake's internal consistency:

  - `macro_pmi_vs_nbs` — 制造业 PMI against the 国家统计局 release
  - `st_labels_vs_exchange` — ST designations against SSE / SZSE listings

  Gated on `[sources.nbs]` / `[sources.exchange]`, defaulting off when absent.
  Results land in `meta/quality/source_diffs/authority-{date}.json` even when
  everything agrees. The NBS query API is not used (403 from non-mainland
  egress); the release sentence is parsed instead. M2 is not covered: the PBOC
  publishes levels only and revised the M1 caliber from 2025-01.

- **`st_label_crosscheck`** — `trading_status` ST labels vs the ST prefix on
  the instrument's exchange short name (TDX short name × EastMoney risk board;
  no network). Replaces the retired AkShare ST union, which queried the same
  push2 endpoint as the EastMoney adapter and could never disagree.

- **`macro_checks.py`** — freshness and revision tracking for monthly macro
  (issue #10): `macro_indicator_stale`, `macro_value_revised`.

- **`adapters/pboc/`** — 社会融资规模增量 from the PBOC Excel attachments
  (bilingual headers, explicit `单位：亿元人民币`). `[sources.pboc]` in the
  example config. Coverage through 2026-06; staleness threshold 75 days.

- `daily_bars_volume_unit` audit check (`quality/unit_checks.py`): per-source
  median `amount / close / volume` outside [0.8, 1.25] fails the run.

### Changed

- **`daily_bars` is `data_version = v2`** — v2 guarantees `volume` is 股; v1
  meant the unit depended on `source`. Resolved per dataset via
  `domain.schemas.data_version_for`; every other dataset stays on v1.
  `index_bars` and `sector_bars` keep TDX's own volume unit (see
  `docs/datasets/schema.md`).

- **`social_financing` comes from the PBOC.** 社会融资规模 is a PBOC statistic;
  an intermediate MOFCOM republisher path (never shipped) lagged two release
  cycles and served a superseded vintage. A backup that quietly carries stale
  values is not a safe backup (ADR-0003). Year workbooks are read newest-first
  so restated vintages win; percentage tables stacked under the 亿元 table are
  skipped by each table's own unit declaration.

- Macro monthly series (`pmi`, `m2_yoy`) read EastMoney datacenter reports
  directly (`RPT_ECONOMY_PMI` / `RPT_ECONOMY_CURRENCY_SUPPLY`) with the
  project's retry, throttle and TLS handling. Each row stamps its own `source`.

- README architecture diagram refreshed (`docs/assets/architecture-overview.png`):
  drops AkShare, adds `pboc` under official sources.

### Fixed

- **`m2_yoy` held M0 month-over-month growth, not M2 year-on-year.** The old
  AkShare path matched columns by Chinese substring with a
  `next(..., columns[-1])` default; `"M2-同比增长"` never matched
  `"货币和准货币(M2)-同比增长"`, so every fetch fell through to
  `流通中的现金(M0)-环比增长`. Now read as field `BASIC_CURRENCY_SAME`.
  Next `macro_indicators` run rewrites the series (full history refetch +
  compact keeps newest `fetched_at`).

- **`social_financing` never wrote a single row** under the AkShare-era path
  (compact `YYYYMM` months were dropped as unparseable). The PBOC adapter
  backfills from 2015-01.

- **`daily_bars.volume` mixed 股 and 手, off by exactly 100×.** Only `ths` and
  `baostock` already wrote 股; `tdx_protocol` passed 手 through and Sina
  divided by 100. Every adapter now normalizes to 股 at its boundary
  (`cnequity.domain.units`).

- **No-trade bars stored a denormal turnover instead of zero.** TDX's packed-
  float decoder maps raw zero to `2**-127`; fixed at
  `adapters/tdx_protocol/_decode.py` for daily and intraday. Existing rows are
  cleaned by the volume v2 migration script.

- **A backfilled intraday slice near the historical edge silently returned
  zero rows.** `max_pages` sized off the slice width (`end - start`) while the
  wire always pages backward from today; near-horizon slices exhausted
  `max_pages` before reaching the requested dates. Now sized off
  `trade_date -> start` (real walk depth).

- **A single reconnect failure could abort an entire full-market intraday
  sweep**, discarding staged-but-uncompacted batches. Connect retries once
  against a re-probed server; batch-level failures are recorded rather than
  aborting the step; batch size 50 → 200.

### Removed

- **AkShare is no longer a dependency.** Neither former call site was a second
  source: the ST board hit the same EastMoney push2 filter already queried
  in-tree, and the PMI / money-supply wrappers hit the same datacenter reports.
  Dropping it removes 15 transitive packages including `mini-racer`, plus
  `asl doctor --fix` and `diagnostics/repair.py` (only ever about that
  collision). `[sources.akshare]` is gone from the example config.

## [0.3.1] — 2026-07-29

### Changed

- Lowered the supported Python floor from 3.11 to **3.10** (`requires-python = ">=3.10"`).
  EastMoney compact `YYYYMMDD` kline dates now parse via `strptime` (3.10
  `date.fromisoformat` only accepts dashed ISO forms). CI / classifiers cover
  **3.10–3.13**.
- README architecture diagram is a single bilingual JPG
  (`docs/assets/architecture-overview.jpg`); the Pillow renderer and separate
  zh/en PNGs are gone.

### Fixed

- `asl config init` always writes an **absolute** `data.root` (resolving the
  template's `./data/cnequity` against the current working directory) so
  `asl doctor` is green on the default first-run path.
- Default `[on_demand].datasets` is only `stock_news` and `research_reports`.
  `announcement_body` / `financial_reports` raise `NotImplementedError` instead
  of caching empty placeholder JSON. Failed research_reports fetches are not
  cached either.
- ImportError hints for baostock / pandas no longer recommend removed extras
  (`[valuation]` / `[structure]`); they point at reinstalling `cnequity`.

## [0.3.0] — 2026-07-29

### Upgrading from 0.2.x

Neither pip nor uv removes a package that merely stopped being a dependency, so
an upgraded environment keeps `mootdx` and its `py-mini-racer`. The latter then
shares the `py_mini_racer` import package with the `mini-racer` that AkShare
brings in, and one silently overwrites the other. Nothing this project fetches
is affected — none of the AkShare endpoints it calls evaluate JS — but AkShare's
own cninfo and sina APIs would break if you call them directly.

    asl doctor        # reports it
    asl doctor --fix  # resolves it

`mootdx` itself is left behind as dead weight and can be uninstalled. A fresh
environment has none of this.

### Changed

- `pip install cnequity` is the whole install. Every runtime source — AkShare,
  Baostock, SnowNLP, and the pandas/openpyxl/xlrd trio that parses the Shenwan
  and CNI constituent spreadsheets — is a hard dependency, so no daily or
  backfill step can silently lose a source because an extra was forgotten. Costs
  roughly 217MB over the previous minimal install.
- TDX quotes now use a vendored wire client (`adapters/tdx_protocol/_wire`,
  derived from tdxpy, MIT) instead of `mootdx`. Both `mootdx` and `tdxpy` were
  last released in 2024 and are unmaintained. Verified byte-identical to the
  previous implementation against live servers, including the full 51478-row
  security list.
- `httpx` is no longer capped at `<0.26`; that ceiling came from `mootdx`.
  Installs now resolve to 0.28.x.
- The bundled fallback TDX host list is now maintained in-tree
  (`adapters/tdx_protocol/hosts.py`). A probe of all 49 known hosts found every
  one of mootdx's 38 dead; the four that serve real bars are ordered first.
  Server selection went from failing across 16 probes to resolving in ~3s.

### Added

- Native Windows 10/11 (64-bit) support: cross-platform file locks replace
  Unix-only `fcntl.flock` in run locks, watermark writes, rate limiting, and
  staging cleanup (`cnequity.file_lock`).
- CI `windows-latest` job running the offline unit suite.
- `asl config init` defaults `workers = 1` on Windows (same as macOS); raising
  workers later is allowed — Windows uses spawn, not the unsafe macOS fork path.
- Installation docs cover PowerShell / cmd, path forms, and the supported
  Windows scope (x86-64; 32-bit / ARM64 deferred).
- `asl doctor` — checks what `asl config validate` deliberately cannot, since
  that command is environment-blind: whether `data.root` is absolute, present and
  writable, and whether every declared dependency actually imports. `--fix`
  repairs the `py_mini_racer` distribution collision cross-platform.
- Guards (`tests/unit/test_tdx_decoupling.py`) that fail the build if `mootdx`,
  `tdxpy` or the racer packages are imported or re-declared as dependencies.
- README (zh/en) leads with a **shortest path to data** (demo vs daily lake),
  then datasets and the peer comparison table — less duplicate install/read
  sections for first-time readers.
- README shows a layered architecture PNG (zh/en) above the shortest path;
  drops the one-line peer punchline under the comparison table.
- README screenshots re-rendered without the retired `mootdx` probe line;
  banner copy tracks current `asl demo`.
- Docs hub reordered (install/quickstart first); `architecture.md` and
  `datasets/README.md` folded into overview/catalog stubs; module
  cli/query/config pages reduced to source maps.
- Architecture PNGs list primary + supplement adapters (`ths` / `sw` / `cni` /
  `macro`, plus calendar seeds note).

### Fixed

- `bars()` now honours the market derived from the exchange suffix. `mootdx`
  had no such parameter, so the value this project computed was silently
  discarded and re-derived from the code prefix.
- `asl demo` no longer appears to hang. It left its probe client open, and the
  heartbeat thread is not a daemon, so the interpreter stayed alive after all
  six steps had already printed. The client is closed now.
- The vendored client grew back `do_heartbeat`, which the trim to five methods
  had dropped. `HeartBeatThread` calls it by name every 10s, so every keepalive
  raised AttributeError — invisible to any test short enough not to reach the
  first interval.
- DuckDB view globs now use POSIX paths (`as_posix()`), so Windows backslashes
  no longer break `read_parquet(...)` SQL literals.
- Polars recursive scans go through `parquet_glob()` (same POSIX rule); the
  instruments planner uses `Path.rglob` instead of `glob.glob(f"{Path}/…")`.
- `asl config init --data-root` no longer strips escaped backslashes when the
  path is a Windows `C:\…` form (callable `re.sub` replacement).
- `asl demo` writes a TOML-safe `data.root` (escaped POSIX path) so follow-up
  `asl query --config configs/cnequity.demo.toml` works on Windows.
- `asl doctor` probes writability with a real create/delete (not `os.access`) and
  suggests an ACL fix on Windows instead of `chmod`.
- EastMoney sticky IP / CLI sticky reads always use UTF-8.
- Atomic parquet replace retries briefly on `PermissionError` (WinError 32 when
  DuckDB / Explorer still holds the destination).
- TDX heartbeat thread is daemon and is joined on disconnect, so spawn workers
  do not linger after close.
- Test helpers embed `data.root` via `path_for_toml()` so Windows CI no longer
  dies on `TOMLDecodeError: Invalid hex value` from unescaped `C:\Users\…`.
- Windows CI: `path_for_toml(Path("/tmp/…"))` assertion accepts drive-letter
  POSIX forms (`D:/tmp/…`) on `win32`.
- Offline unit coverage expanded across EastMoney / cninfo / failover /
  sentiment / sector helpers so project branch coverage sits above 80%.
- Stale docs: removed retired pip extras (`[macro]` / `[valuation]` / …);
  clarified `ASL_*` env vars are script-only (`asl` CLI does not read them);
  fixed eastmoney adapter CLI relative link and quickstart Init anchor.

### Removed

- All extras (`tdx`, `macro`, `nlp`, `valuation`, `structure`, `all`).
  `pip install "cnequity[tdx]"` from an older doc still installs correctly:
  pip warns that the extra is not provided and continues, uv says nothing.
- Contributor tooling moved from the `dev` extra to a PEP 735 dependency group:
  `pip install -e . --group dev` (pip >= 25.1) or `uv sync`.

## [0.2.0] — 2026-07-27

### Added

- `asl config init` writes the packaged example TOML (no repo checkout needed);
  forces `orchestrator.workers = 1` on macOS
- Packaged template at `cnequity.config.templates` (kept in sync with
  `configs/cnequity.example.toml`)

### Fixed

- PyPI project page: ship a short Chinese `README.pypi.md` with absolute GitHub
  links (full `README.md` relative paths break on pypi.org)

### Changed

- Document `pip install "cnequity[tdx]"` as the primary install path
- `pyproject.toml` `readme` points at `README.pypi.md` instead of `README.md`
- Getting-started docs use `asl config init` instead of `git clone` + `cp`;
  quickstart separates one-minute demo from full-market init

## [0.1.0] — 2026-07-19

First public release of the self-hosted A-share Parquet data layer.

### Added

- Multi-source ingest (TDX/mootdx, EastMoney, Sina, CNINFO, optional Baostock/AkShare)
  into a staged → curated → derived lake layout
- CLI (`asl`) for `init`, `run daily`, `backfill`, `compact`, `derive`, `audit`,
  `status`, `retry`, `query`, `catalog`
- Python `load()` API with `adjust` / `universe` / point-in-time `as_of`
- DuckDB views over curated Parquet
- Dataset coverage across reference, bars, corporate actions, fundamentals,
  capital flow, sector/industry structure, macro, news/sentiment, and risk events
- Quality audit (PK, mock-source guard, adj-factor reconciliation, cross-checks)
- Optional extras: `tdx`, `valuation`, `macro`, `nlp`, `structure`, `dev`
- Ops scripts for daily pipeline, health notify, and meta backup
- Docs: comparison vs AkShare/Tushare/Baostock, legal notes, schema contract,
  per-source limits, runbook

### Security / hygiene

- Ignore runtime logs and local tool/editor dirs
- TLS verify on by default for HTTP clients
- Project URLs point at `rootSunc/cnequity`

[0.6.0]: https://github.com/rootSunc/cnequity/releases/tag/v0.6.0
[0.5.0]: https://github.com/rootSunc/cnequity/releases/tag/v0.5.0
[0.4.0]: https://github.com/rootSunc/cnequity/releases/tag/v0.4.0
[0.3.1]: https://github.com/rootSunc/cnequity/releases/tag/v0.3.1
[0.3.0]: https://github.com/rootSunc/cnequity/releases/tag/v0.3.0
[0.2.0]: https://github.com/rootSunc/cnequity/releases/tag/v0.2.0
[0.1.0]: https://github.com/rootSunc/cnequity/releases/tag/v0.1.0
