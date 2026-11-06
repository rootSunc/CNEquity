# Fetching, speed and source protection

If you suspect a network or installation problem, first run the offline `cne doctor`, or verify the installation offline with `cne init --profile sample`. Full-market initialization and historical backfill can take hours. Do not use full-market tasks to test the network, and do not keep retrying health probes to find out whether a source has recovered.

```bash
cne sources limits                               # offline view of cooldowns, budgets and resume entry points
cne sources probe --list                         # offline list of probe names
cne sources probe --only tdx_protocol --vantage local
cne backfill daily_bars --symbols 000001.SZ --start 2026-09-01 --end 2026-09-04 --plan
```

`--plan` does not go online and does not create a lake or run record. For ordinary datasets it shows the effective source switches, shared cooldowns, rate limits/concurrency, repair mode, known outstanding debt and slices; for an explicit stock daily-bar range it also gives the minimum request count with a cold cache and whether the whole-board snapshot is skipped. Derivatives estimate request counts from exchange capabilities. Pagination, connection discovery, caching and gaps change the real cost; unknown counts are shown as `null`, and one day is never presented as one request. Remove `--plan` to execute.

## Targeted historical backfill

All users reach these strategies through the same `backfill` entry point; `--config` points to your own configuration,
and data is written to that configuration's `data.root`, with no dependency on the maintainer's local scripts or data lake.

```bash
# First check symbols, dates, source switches and request strategy offline
cne backfill daily_bars --config configs/cnequity.toml \
  --symbols 000001.SZ,600519.SH --start 2018-01-01 --end 2018-01-31 --plan
# Once the scope is confirmed, remove --plan to execute
```

`daily_bars`, `minute_bars` and `minute_bars_5m` support an explicit list of securities and a date window;
minute data must first be enabled in the configuration, and the start must fall within the source's retention period.
`--symbols` is not supported by every dataset; for other datasets, `backfill --help` is authoritative.
`--outstanding` repairs from this lake's outstanding-debt ledger and cannot be combined with `--symbols/--start/--end`.
It first reconciles against published bars and writes off keys that already exist, then requests the source only for months that are truly missing;
keys that are still missing do not get their retry count increased by this offline reconciliation. When bars cannot be obtained, check listing, delisting and suspension evidence first,
and do not keep requesting dates on which there should have been no trading.

TDX K-lines only have record offsets; there is no server-side date filter parameter. When backfilling an earlier window, the project first
widens the probe interval by date, locates the window by bisection, then reads the target window contiguously; pages read during location are reused directly.
The request volume therefore drops from "read every page from the latest back to the start for each security" to "location pages + window pages for each security".
Location grows logarithmically with history depth; a complete multi-year history still requires reading all needed records, and no window can be promised as one request.
Windows close to the latest date, and backfilling a whole stretch of history back from the latest, keep using sequential reads without extra location.
After a page jump, the timestamp of the latest page is checked once more; if the position has changed or a request fails, that symbol errors out, to avoid committing misaligned partial results.
If dates cannot be parsed, it falls back to sequential reads; rate limits, history boundaries and completeness checks remain in effect.

To reduce requests, narrow the set of securities first, and merge adjacent dates for the same security into one backfill; minute backfill is already sliced by security,
so there is no need to loop over days by hand. THS annual files, exchange daily files and the like are still read and cached at their own file granularity.
The number of securities usually increases requests linearly; the time span determines the window data volume; how far the target date is from today mainly increases TDX location cost.
A paused or refused source does not recover because of date location, and minute data past its retention period or missing historical snapshots do not become recoverable because of it either.

## Which CLI commands go online

The table covers every leaf command. Offline means no requests to data sources; writing reports, indexes, snapshots or configuration still counts as a local write.

| Command | Network use and side effects |
|---|---|
| `config create/validate/diff`, `doctor` | Offline; create writes a personal configuration, doctor may test directory writability |
| `init` | quick/full fetch history; demo fetches a small sample; sample is synthesized offline; `--layout-only` only creates local directories and metadata |
| `run daily/events/retry` | Collects online, writes staging and metadata, and publishes according to the steps; retry should target the failed run/group |
| `run compact/clean` | Offline; compact publishes staged data, clean deletes qualifying local data. clean's `--dry-run` previews the deletion scope and cannot be combined with `--reconcile-runs`, which changes run state |
| `backfill` | Usually goes online to fill history; completed slices/securities resume from each dataset's checkpoint; the delisted scope uses `backfill daily_bars --profile delisted`; `backfill --plan` is offline. `cne import shfe-archive` is an explicit local ZIP import that makes no data-source requests, but it stores the original package and publishes rows with missing fields |
| `derive` | `adj_factors` refreshes external factors and fallback sources; other derive modes mainly read local data and write derived results or mappings. Do not treat derive as a whole as an offline command |
| `verify` | Checks local coverage by default; only `--repair` runs an online backfill |
| `audit` | A run audit may perform enabled external comparisons against NBS/exchanges; `--full` reads the lake and existing evidence and is not equivalent to an active source probe |
| `query --sql` | Queries the lake; may update local DuckDB views |
| `query --dataset … --symbol …` | Reads on a cache hit; goes online when missing or with `--refresh`. Constrained by `[on_demand].enabled` and source switches; exits non-zero on failure |
| `mcp`, `serve` | Query the lake by default; `mcp --live` explicitly enables limited source queries. Never runs full-market collection automatically |
| `sources probe` | Bounded probe per source, writes a report; `--list` is offline. Official DCE, the THS catalog, Baostock and CNI history files are actively probed only when named explicitly with `--only`; handshake, pagination and download each count once |
| `sources substitutes` | Reads existing reports by default; only `--probe` goes online |
| `sources slo/resilience/policy/limits` | Read local source policy, probe history, cooldowns, budgets and statistics; may write reports, never trigger fetching |
| `status`, `delisted status`, `profile list/show`, `contract show/diff/validate` | Local status, catalog or contract checks |
| `decision-data payment-gaps/stock-terms/cash-rights` | Generate local decision lists from committed data; no collection |
| `stats show/rebuild` | Read local Parquet/statistics; may refresh statistics files |
| `snapshot create/verify/restore/export/import`, `snapshot delta create/verify/apply` | Local snapshots, verification and copying; no fetching from market data sources; restore/apply write to the target |
| `ths-official capture/backfill/repair-bars/resource-sectors` | Optional credentialed source; capture writes comparison evidence, backfill fills gaps. The default dry run of repair/resource **also goes online**; `--apply` controls writing and does not mean network permission or an unlimited quota |

## Data source selection and request cost

Source selection is predicated on data semantics, coverage and reviewability; the same vendor on a different domain does not count as an independent fallback source. Snapshots cannot pass as history, and sector indexes from different vendors cannot be spliced together directly. For full fields and exceptions, see [Data sources](../datasets/sources.md) and the [Dataset catalog](../datasets/catalog.md).

The public default configuration enables EastMoney with `push2_paused=false`. push2 is used for snapshot data, daily-bar gap filling and independent comparison; it is not a uniform primary source for all datasets: stock daily bars use TDX as primary, and valuation prefers the dated EastMoney datacenter, which provides TTM semantics, falling back to push2 only when that is not yet published or unavailable. When the egress is blocked, pause push2 only in that deployment's personal configuration; the ordinary daily update and the evening catch-up use the same source policy, budget and circuit-breaker ledger.

| Data family | Current acquisition method | Speed and risk trade-offs |
|---|---|---|
| Shanghai/Shenzhen daily bars, indexes, minute bars, trade ticks | TDX connection reuse and pagination; daily bars fall back to EM and others for gaps | For the full market on the current day, exchange board snapshots are used first; explicit history or daily-bar backfills of at most 4 symbols skip the whole-board snapshot and go straight to the limited scope; minutes are sliced by security to avoid repeatedly pulling tip pages per date |
| Beijing daily bars | BSE official current-day board snapshot; history routed to TDX/Sina by coverage, with field repairs based on source evidence | For the full market on the current day, the board is fetched page by page, avoiding multiple per-security requests; explicit history or backfills of at most 4 symbols skip the whole-board snapshot, and turnover that Sina does not provide is not fabricated |
| Corporate actions, price adjustment, historical ST/valuation | EM/TDX corporate actions; Sina factors, Baostock fallback and historical evidence; issuer announcements fill in payment dates | The factor cache refreshes only securities that need updating; THS corporate-action pages used by explicit BJ repairs are cached for 24 hours only after an implemented event is identified, and the raw-response archive for cache reuse keeps the original fetch time; Baostock is serial with rests between batches; missing events remain gaps |
| Financials, shareholders, capital, news and snapshots | EM datacenter / push2 / news APIs; THS fund flow stored in a separate table | push2 snapshot reuse, per-source budgets; a day whose snapshot was lost cannot be turned into history by refetching today; financial statements are sliced by reporting period × statement and top-ten shareholders by quarter; a resume skips complete units already verified: within a run those already staged, and after an interrupt those published in the last 7 days (matched by schema version and symbol scope); `--refetch` fetches them all again. When shareholder pagination fails on later pages, positive facts from verified pages may be published marked as partial coverage, with the failed window kept in outstanding debt and refetched; pages from different points in time cannot be stitched into a complete snapshot |
| Announcements, regulation | CNINFO paginated POST, subdivided by date and publishing board | Bounded retries, per-window checkpoints; page-count limits cannot be solved by higher concurrency |
| Margin trading, dragon-tiger list, block trades | Exchanges are the default for margin; dragon-tiger/block trades use EM as primary and exchanges as fallback | Fetched in daily batches; fields not published by exchanges are left empty, never fabricated for completeness |
| Industries, index constituents, sector bars | Current snapshot + SW/CNI history files; THS sector K-lines with consistent semantics | Files first, catalog caching; SW industry files and CNI per-index adjustment files are cached for 24 hours after validation; valid THS annual K-line files for sectors, stocks and indexes are reused per URL, with older years cached for up to 30 days and the last two years for up to 1 day; empty files are not cached, and sector `last.js` is always read live; switching sources for indexes/sectors requires explicit review |
| Macro, commodity research series | EM, PBOC published files, Sina continuous quotes; NBS for limited comparison | Files and complete series are downloaded once, then sliced into windows; the NBS PMI index is cached for 1 hour after validation and parsed body text for 24 hours; main-continuous research series cannot replace the real contract life cycle |
| Futures, options | Exchange files; configured routing for DCE, limited Sina minute windows | Cached per exchange file, with completion receipts and gap resume; source history boundaries are not exceeded; see the [Derivatives guide](../recipes/derivatives.md) |
| Optional keyed APIs | `ths_official` / Tushare, called only when enabled | Never a hard dependency of the free core chain; large files are preferred over per-symbol calls, and the public THS website and the keyed service are rate-limited independently; Tushare's complete valid-trading-day results for 2016 `bak_basic` are cached for 7 days isolated per credential, keeping the original `fetched_at` on reuse; empty or wrong-date results are not taken as evidence of normal status, and after a failure previously successful dates are reused first |

## Shared rate limits and cooldowns

Requests are subject to two constraints at once: the maximum in-flight count per source, and the minimum interval between adjacent requests. A concurrency slot is acquired first, then the interval is waited out right before the actual network call, so a slow request finishing does not release a batch of requests that grabbed time slots in advance. Endpoint aliases of the same public site also share the source family's base interval, so they cannot each respect their own interval while exceeding the rate together; slower page/history endpoints still keep their own interval without slowing down the same vendor's ordinary endpoints. Each time a waiter wakes up it rechecks the shared time, so it sees cooldowns set later by other processes.

The code also sets conservative default intervals and in-flight limits for integrated sources: when you use a minimal TOML or construct `Config` directly, omitting `[sources.*].min_interval_seconds` and source concurrency limits does not leave requests unthrottled. The public example gives the same common values; explicit configuration values for ordinary sources (including a 0-second interval) still take precedence. For TDX, when both `tdx_protocol.min_interval_ms` and the source interval are set, the stricter one applies. New sources that cannot be assigned to a known source family default to a 1-second interval, and a dedicated policy should still be added for the source's characteristics when integrating it. `sources limits` shows the actual defaults and the values in effect today.

HTTP `429`, and `403`, `412` and `456` without an authentication challenge, record a shared cooldown of at least 300 seconds at integrated request boundaries; when the server's `Retry-After` is longer it is followed, in both seconds and HTTP-date form. `401`, or `403` with `WWW-Authenticate`, is treated as a credential problem and reported to the caller, without blocking the whole egress where another set of credentials lives. A plain `404` is not treated as a ban. Endpoints that expect JSON also cool down on an explicit CAPTCHA/challenge HTML page, as do EastMoney datacenter's "请求过于频繁" ("requests too frequent") and keyed THS's business rate-limit codes.

Ordinary `5xx` errors are treated as temporary server faults and go through the adapter's existing bounded retries and backoff, rather than being judged directly as a ban of the whole egress; the EastMoney host circuit breaker has its own, stricter 5xx rules. The real request volume to a source may include connection discovery or pagination, so IP risk cannot be inferred from HTTP status alone.

During a cooldown, subsequent requests fail locally. After it expires, the same source family lets only one recovery request through; on failure it cools down again, and only on success does it open up. A 401 from public THS pages is also handled as a refusal from that public site and is not confused with the credentialed `ths_official`. Baostock follows the provider's rules: at most 50,000 API requests per Shanghai calendar day (login, query and logout each count once), and only one connection at a time; a `10001011` or blacklist response freezes access for the current calendar year's cumulative count × 6 hours, and when the response gives no release time there is no refresh for at least 5 minutes, with no login until the cooldown ends. When a scan stops at the limit or the blacklist, symbols already fetched are kept; after the cooldown, rerun the same command and it continues from the symbols not yet completed. Explicit business rate-limit messages from the optional Tushare and HTTP 429 from keyed THS are also not retried back to back. Only business refusals recognized by an adapter enter this flow; there is no guarantee that every anti-bot page inside an HTTP 200 is recognized. Each refusal keeps the failure/gap for a later resume.

`cne sources limits` only reads the ledger. It shows configured values and the strictest interval/concurrency actually in effect across processes today, recovery-probe status, the remaining EastMoney budget, Baostock's remaining requests for the day, and the requests and retries recorded by the most recent run; per-host lanes for exchange futures files are also listed separately. `metered_attempts_today` counts once per source family and lane after the shared rate limiter admits a request and before the network call; local cooldown or disabled refusals are not counted. It covers integrated request scopes, including TDX connection discovery and pagination, but one scope may send several underlying requests, so it cannot be taken as an exact HTTP/TCP packet count. `wire_responses_today` aggregates the number of responses returned through integrated response hooks, the HTTP status, the decoded body size and a hash of the endpoint path; it does not store query parameters and does not count requests that returned no response. `cache_reuse_today` records reuse counts of integrated response caches and does not count as a new source observation. `request_events_today` records the extra retries that are tagged for futures files, BSE, EastMoney datacenter and Tushare, as well as EastMoney direct-connection fallbacks; other paths are not tagged uniformly yet. The run's `metrics.requests` still only covers telemetry passed in by adapters, and some prefetches and actual transferred bytes are not unified yet, so it cannot be used to claim a project-wide reduction in real requests.

`[sources.eastmoney].daily_budget` can set a shared daily cap per egress for push2 and datacenter; the default 0 only counts, and before deployment you should choose a conservative value based on the actual request ledger. The intervals and individual budgets of the two lanes still apply independently.

This follows how HTTP expresses rate limiting and retry timing; 300 seconds is this project's conservative policy, **not a quota published by the source, and no guarantee that the IP will never be banned**. [HTTP 429](https://www.rfc-editor.org/rfc/rfc6585.html#section-4), [Retry-After](https://www.rfc-editor.org/rfc/rfc9110.html#section-10.2.3).

The existing EM push2 same-day circuit breaker/budget, the datacenter circuit breaker and the derivatives host challenge circuit breaker remain in effect. EastMoney's daily budget and circuit breaker now share the egress ledger; on first use, this lake's old ledger for the day is merged in. When multiple processes have different settings, the strictest positive budget and circuit-breaker threshold seen is used. A cooldown does not clear these stricter limits. Do not respond to refusals by deleting state files, rotating IPs, adding workers or turning off circuit breakers; pause the affected source, wait out the cooldown, run one small-scale verification, and repair the failed range.

Default rate limits and cooldowns live in `{data.root}/meta/rate_limits`. **Multiple lakes/CLI processes on the same egress must share the directory**:

```bash
export CNE_RATE_LIMIT_ROOT="$HOME/.local/state/cnequity/egress"
```

HTTP, TDX and the EM daily budget/circuit breaker all read this variable. All processes should use the same conservative source settings; the directory must support reliable local file locks. This mechanism does not coordinate across machines and does not limit traffic from browsers or other programs; on a single egress, prefer one collection scheduler.

## Order of operations

1. Run the offline `doctor`, `config validate` and `config upgrade --dry-run`; confirm the configuration and an absolute `data.root` first, then look at `backfill --plan`.
2. Verify reachability and data semantics on a small scale; range arguments cannot be empty strings. Health-probe source names can be listed with `--list`; misspellings raise an error.
3. Use incremental `run daily` / `run events` day to day; backfill only failed windows. `verify --repair`, `--force` and `--refresh` have different costs, so read the help before running them.
4. When refused, keep the logs, run ID and checkpoint, and pause tasks against the same source. After recovery, resume the gaps, avoiding repeated initialization or full scans.
5. For performance comparisons, record the source, scope, cache warmth, request count and elapsed time; keep observations from your own egress in local working notes, and do not turn them into public rate guarantees.
