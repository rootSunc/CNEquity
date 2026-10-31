# Configuration reference

Config file format: TOML. The template ships with the package in `cnequity.config.templates`; the repository copy is `configs/cnequity.example.toml`.

```bash
cne config create                              # Recommended: writes the per-user ~/.config/cnequity/config.toml
 # Optional on first creation: cne config create --data-root /data/cnequity
cne config path                                # Shows the config in effect, why it was chosen, and its data root
cne config show                                # Prints the full effective configuration, marking what the file sets
cne config validate
```

Loading and validation: `cnequity.config.loader`. Personal files are not part of the open-source default config; see [Upgrades and feedback](installation.md#upgrades-and-compatibility). Config lookup order: explicit `--config` → `CNE_CONFIG` → `configs/cnequity.toml` in the current directory → the per-user `~/.config/cnequity/config.toml` (honours `XDG_CONFIG_HOME`; `%APPDATA%\cnequity\config.toml` on Windows). The CLI, `load()`, MCP and the scheduled tasks share this rule, so a command run from any directory reaches the same lake. `cne config create` writes an absolute `data.root` by default; a hand-written relative path (`data.root`, `duckdb.path`) resolves against **the config file's directory**. A relative path written for the old working-directory rule keeps working, with a warning, while only its old location exists; `cne config upgrade` pins it to the absolute path.

This page is organized by config section. For first use, you usually only need to confirm `[data].root`, then enable minute bars, trade ticks or [derivatives](../recipes/derivatives.md) as needed. Defaults in the tables refer to the bundled template, which is what an `extends = "builtin"` config inherits; constructing `Config()` directly may give different results, so `cne config show` is authoritative.

When multiple processes/data lakes share one egress, use `CNE_RATE_LIMIT_ROOT` to unify HTTP/TDX rate limiting and shared cooldowns. For source protection, CLI network boundaries and order of use, see the [fetch guide](../operations/fetch-policy.md).

## Extending the built-in defaults (`extends = "builtin"`) {#extends-builtin}

The config written by `cne config create` and by a first `cne init` is a few lines long:

```toml
extends = "builtin"

[data]
root = "/abs/path/to/lake"
```

Every other setting comes from the built-in defaults of the installed version (the bundled template, adjusted for this platform, e.g. `workers = 1` on macOS / Windows). The file holds only the settings you want to change; after an upgrade, the new version's settings and schedule steps apply directly, with no `cne config upgrade`.

- Tables merge key by key, recursively (inline tables too); any other value (scalars, arrays, arrays of tables such as `[[failover.datasets]]`) replaces the default whole.
- `[data].root` never comes from the defaults and must be written in the file.
- Schedule groups (`[job.daily.groups.*]`, `[job.events.groups.*]`) accept `steps_add`, `steps_remove` and `enabled = false`, so swapping one step does not freeze the rest of the group:

```toml
[job.daily.groups.capital]
steps_remove = ["fund_flow"]
steps_add = ["fund_flow_ths"]

[job.daily.groups.research]
enabled = false
```

Step order inside a group does not affect execution: the engine orders steps by dependency, finalize order and name. Writing `steps = [...]` still replaces the whole group; when it shadows a step a new version adds by default, `cne config diff` reports it.

`cne config show` prints the full effective configuration, marks which values come from the file, and masks credentials; `cne config show --defaults` prints every setting with its explanation.

A full config without `extends` keeps working as before: missing keys fall back to built-in values in the code, and an upgrade needs `cne config upgrade` to add schedule steps. `cne config upgrade --minimize` rewrites it once into the extending form: keys equal to their default are dropped, schedule group differences become `steps_add` / `steps_remove`, default schedule groups the config lacks become `enabled = false`, and `data.root` is written as the absolute path it resolves to now. Before rewriting it lists every setting whose behaviour changes on loading (keys the old config lacked and the defaults now supply); `--dry-run` only previews, and the original is backed up.

## `[data]`

| Key | Type | Default | Description |
|----|------|------|------|
| `root` | string | `./data/cnequity` | Data lake root directory; a relative path resolves against the config file's directory; **an absolute path is recommended in production** |

Derived paths (computed automatically in code, no config needed):

- `{root}/staging` — raw landing area for the current run
- `{root}/curated` — canonical datasets
- `{root}/derived` — derived datasets (such as adj_factors)
- `{root}/meta` — manifest, watermarks, quality findings
- `{root}/duckdb/cnequity.duckdb` — DuckDB view database

## `[orchestrator]`

| Key | Default | Description |
|----|------|------|
| `workers` | 8 | Number of multiprocess workers for `daily_bars` |
| `batch_size` | 100 | Number of stocks per batch |
| `max_retries` | 3 | Batch-level retry count |
| `retry_backoff_seconds` | 5 | Retry backoff |
| `batch_stale_seconds` | 3600 | A running batch with no heartbeat times out → stale → failed; the compact gate skips unfinished datasets. **Crashed runs are not bound by this window**: a run holds a lock throughout, and once the process dies it is reclaimed within 60 seconds |

## `[tdx_protocol]`

| Key | Default | Description |
|----|------|------|
| `enabled` | true | When disabled, TDX-related steps fail |
| `min_interval_ms` | 100 | Cross-process rate-limit interval (≥100 recommended, so multiple jobs do not overwhelm it) |
| `lock_timeout_sec` | 15.0 | Maximum wait to acquire the TDX rate-limit lock; on timeout it fails explicitly and does not bypass the rate limit |
| `servers` | `"auto"` | `"auto"` or `"host:port"` to pin a single server |
| `connect_timeout_sec` | 10 | Connection timeout |
| `allow_mock` | false | **Tests only**: returns `source="mock"` data when the source is unavailable; must be false in production |

### `[tdx_protocol.hosts]`

| Key | Description |
|----|------|
| `standard` | List of A-share standard quote hosts probed in parallel first when `servers="auto"`; if empty, the built-in fallback list is used (`adapters/tdx_protocol/hosts.py`) |

## `[sources.<name>]`

Main sources and request channels in the template:

| name | Description |
|------|------|
| `eastmoney` | Primary source for daily updates: announcements, financials, fund flow |
| `cninfo` | Paged POST for announcements / regulatory events |
| `pboc` | Monthly aggregate financing series |
| `sina` | News, adjustment factors |
| `sina_bars` | Sina daily-bar fallback; **has its own slower rate limiter**, because the factor endpoint and the per-symbol kline endpoint behave differently under upstream rate limiting |
| `baostock` | Historical market-data fallback; with batch cooldowns for full-market backfills |
| `nbs` | Audit only: comparison against PMI press releases |
| `exchange` | SSE / SZSE's own pages (margin trading details, trading status, `[exchange_audit]` price comparison) |
| `bse` | BSE official current quote snapshot; **the primary source for BJ same-day tip bars and turnover**. It is not a historical source; BJ history windows still go through Sina |
| `ths` | THS public pages (industry, valuation) |
| `ths_bonus` | THS dividends and rights-issue page, with a more conservative rate limit (default 3.0s) |
| `ths_pages` | kline pages on `d.10jqka.com.cn` |
| `ths_official` | **THS official API (keyed)**, see [`cne ths-official`](../reference/cli.md#cne-ths-official) |
| `sw` / `cni` | Shenwan historical industry and CNI historical constituent files; default interval 1 second, a single request in flight |
| `futures_exchange` | Futures exchange files, with per-host rate limiting and caching |
| `tushare` | Optional Tushare Pro: BJ historical ST evidence (`stock_st`). Requires a token: the environment variable `TUSHARE_TOKEN` or `token_file`; do not put credentials in the config |


| Key | Description |
|----|------|
| `enabled` | An explicit false blocks the source and its sub-channels at the HTTP request boundary. When the whole section is omitted, the default behavior of the various legacy paths is not entirely consistent; write `enabled = false` for sources you do not use (in a config that extends the defaults too), rather than expressing "off" by deleting the section |
| `min_interval_seconds` | Cross-process request interval; must be a non-negative finite number. 0 disables the fixed interval but not the rejection cooldown. It waits outside the lock and rechecks the shared time on waking; before the actual request it also acquires a concurrency slot |
| `js_runtime` (ths) | Optional. Path to Deno used to generate the THS `hexin-v` token; if unset, it searches `PATH` and common install locations. The token script runs in Deno **with no permissions at all** (no file read/write, no network, no environment variables). Without Deno, the THS fund-flow fallback fails and is logged, without affecting other data |
| `ths_data` (rate-limit channel) | THS data center pages (fund-flow fallback) default to 3 seconds per page and share 1 concurrency slot with other THS pages; adjust with `[sources.ths_data] min_interval_seconds` |
| `proxy` (eastmoney) | Optional HTTP(S) proxy URL, applied to all EastMoney hosts. Set it according to your own network conditions; if unset, `HTTPS_PROXY` still works. A rotating proxy must not be used in place of cooldowns |
| `push2_paused` (eastmoney) | Off by default, i.e. requests are allowed. When on, push2 / push2his / push2delay requests fail locally and are never sent; use it to stop requests and cool down when the egress IP is banned. datacenter and other EastMoney hosts are unaffected. The environment variable `CNE_PUSH2_PAUSED=1` has the same effect; both the daily update and the evening catch-up honor the local config and this environment variable, and the catch-up never turns push2 back on by itself |
| `push2_breaker` (eastmoney) | On by default. After the first push2-family rejection (403 / 429 / 5xx, connection dropped, timeout), no push2 host is requested for the rest of the day (local time, until midnight), and it does not switch to a backup host |
| `push2_daily_budget` (eastmoney) | Default 150. Daily (local time) request cap for the push2 family, accumulated across processes; it stops when exhausted. 0 means unlimited. Set the cap from this machine's ledger; do not borrow another lake's allowance |
| `push2_shared_snapshot` (eastmoney) | On by default. instruments, valuation_metrics, fund_flow and the clist quote fallback share one full-market pagination (fetching the union of fields), reused from after the close until the next day's open |
| `push2_min_interval_seconds` / `push2_max_concurrency` (eastmoney) | Default 4.0 seconds / 1. A separate rate-limit channel for push2, not shared with datacenter |
| `datacenter_breaker` / `datacenter_breaker_strikes` (eastmoney) | Default on / 3. After 3 consecutive datacenter rejections (403 / 429 / 5xx, connection refused/reset/dropped), it is disabled for the day; read timeouts do not count; if “请求过于频繁” (too many requests) persists after backoff, it is disabled immediately |
| `news_breaker` / `news_breaker_strikes` (eastmoney) | Default on / 3. After 3 consecutive fast-news sweeps are refused (403 / 429 / shared cooldown), the lane (handshake + list host) is closed until the next local day; a dead proxy or a read timeout does not count, and one success resets the count |
| `datacenter_daily_budget` (eastmoney) | Default 0: count only, no cap (the count is in `CNE_RATE_LIMIT_ROOT/eastmoney_guard.json`, by default this lake's `meta/rate_limits/`). Set a cap based on measured usage |
| `daily_budget` (eastmoney) | Default 0: count only; when set, it enforces a shared daily cap on push2 and datacenter requests from the same egress, so that the vendor does not receive too many requests in total even when neither exceeds its own cap. The shared ledger lives in `CNE_RATE_LIMIT_ROOT`. |
| `datacenter_min_interval_seconds` / `datacenter_max_concurrency` (eastmoney) | Default 1.0 seconds / 2. A separate rate-limit channel for datacenter; other EastMoney hosts still follow `min_interval_seconds` and `source_concurrency.eastmoney` |
| `batch_size` / `batch_rest_seconds` (baostock) | Extra batch cooldown for full-market backfills. The 50,000 requests/day cap, single connection and blacklist freeze duration are hard-coded and cannot be raised through config |
| `verify` (ths_official) | **On** by default. It may only write `meta/source_snapshots` and findings and never touches curated rows, so it is safe to leave on if you have a key |
| `backfill` (ths_official) | **Off** by default. It changes the lake's contents, so it must be turned on explicitly. Holding credentials, enabling the source and allowing it to modify data are three separate decisions |
| `api_key` / `api_key_file` (ths_official) | Prefer the environment variable `HITHINK_FINANCE_API_KEY` or `api_key_file` over writing it into the config |

The fetch settings in `cne serve` only change `enabled` for TDX, EastMoney, Sina daily bars, Baostock, the BSE website, THS pages and CNINFO, plus EastMoney's `push2_paused`. The minute-bar, trade-tick and futures switches and `[universe] ingest` / `ingest_listed_funds` / `ingest_eligible_etfs` are on that page too. Before confirming, it lists the differences, and it backs up before writing; intervals, budgets, breakers and credentials are left untouched. The environment variable `CNE_PUSH2_PAUSED=1` still takes precedence over the pause switch in the config.

`cne serve` keeps using the lake it started with. If you edit the config file and change `data.root`, restart serve to switch; until then, every console page shows the new directory from the file next to the one still in use, with the restart command. If the config file cannot be read, the notice gives the line and column of the error. In both cases you can keep browsing the original lake, but the console will not start jobs or change fetch settings or schedules, because a job would read the file and work on the other lake.

Template request intervals (conservative client-side settings, not a guarantee of a safe quota on the source side):

| source | `min_interval_seconds` | Notes |
|--------|------------------------|------|
| eastmoney | 0.5 | General HTTP channel in the template; push2 is 4.0 / single in flight, datacenter is 1.0 / two in flight. A bare client does not represent the CLI's shared policy |
| cninfo | 1.0 | Paged POST for announcements/regulatory events |
| pboc | 1.0 | Monthly aggregate financing series; one index request + one workbook per year |
| nbs | 1.0 | Audit only: comparison against PMI press releases, two requests each time |
| exchange | 1.0 | Exchange bulk endpoints for margin trading, status and comparison |
| sina | 0.3 | Adjustment factors; shares the in-flight cap and rejection cooldown with Sina daily bars |
| sina_bars | 1.0 | BJ/delisted daily-bar fallback; rate limited independently of adjustment factors, with limited retries on HTTP 456 |
| baostock | 1.0 + batch 20/120s | Historical market cap/ST. There is also a hard-coded cap of 50,000 per Shanghai calendar day and a single connection; the blacklist freeze is the number of occurrences this year × 6 hours |

Credentials (the `[sources.tushare]` token and the `[sources.ths_official]` api_key) are looked up in this order: the environment variable (`TUSHARE_TOKEN` / `HITHINK_FINANCE_API_KEY`) → a file (`token_file` / `api_key_file`; a relative path resolves against the config file's directory, and only the first line is read) → the value written in the config. Scheduled runs started by launchd / cron do not see variables exported in your login shell, so a scheduled lake should use a file with mode 600. `cne doctor` warns when a credential is written in the config, and when such a config file is readable by other users.

## THS official API {#ths-official-api}
`ths_official` is an optional API that requires account credentials; it is configured, rate limited and recorded as a source separately from the `ths` public pages. It must not become a hard dependency of the free core data chain. For source registration and data-use conditions, see the [source matrix](../legal-and-data-sources.md#source-compliance-matrix); the client code license is not a data redistribution license.

### Configuration and entry points

Provide the key through the environment variable `HITHINK_FINANCE_API_KEY` or `api_key_file`; do not put the key in public templates, logs or issue reports. Explicitly enable `[sources.ths_official] enabled = true` in your personal config. `verify` controls comparison evidence and `backfill` controls content backfill; the latter is off by default.

| Command | Fetch and write boundary |
|---|---|
| `cne ths-official capture` | Captures counterpart-source snapshots per `--what`; writes only snapshots and run evidence, never replaces canonical data |
| `cne ths-official backfill` | Fills financial statement gaps by window; supports `--symbols` to narrow the scope; requires the content switch |
| `cne ths-official repair-bars` | Historical market-data reconciliation; reports by default, writes repair results only with `--apply` |
| `cne ths-official resource-sectors` | Explicit sector source switch; reports by default, `--apply` writes and publishes automatically |

Dry runs of the last two still go online and consume source quota. Without a key, with the source not enabled, or without the corresponding capability allowed, the commands may return `status=skipped`; this does not prove that data was fetched. Each command's `--help` is authoritative for parameters.

### Data contract

- Financial `net_profit` uses the attributable-to-parent definition and maps to `parent_holder_net_profit`; it must not be concatenated by name with total net profit that includes minority interests.
- A disclosure date needs traceable corresponding evidence; a current restated value cannot be promoted to original PIT just because a date was filled in.
- For adjustment events, download the full dump first and slice it locally; event streams and factor series have different structures and are compared through a dedicated arbitration check.
- Bonus shares, capitalization issues and rights issues must follow the [product boundaries](../architecture/overview.md); the same dilution fact must not be stacked across sources.
- ETF, stock and sector endpoints differ in window and coverage; the adapter limits requests to known ranges. An empty response does not prove a delisted security does not exist and must not automatically delete existing rows.
- Inserting missing primary keys and replacing an existing source are different operations; before repairing, check the [product boundaries](../architecture/overview.md) and [data source limitations](../datasets/sources.md).

### Failure handling

For authentication failures, check credentials and switches first; when rate limited or rejected over HTTP, wait for the shared cooldown, then do one small-scope diagnosis. An empty result is a different state from failure or skip and cannot be used as proof of complete coverage. See [fetching and source protection](../operations/fetch-policy.md).

## `[adj_factors]`

| Key | Default | Description |
|----|------|------|
| `source` | `"sina"` | Source of adjustment factors |
| `adjust_types` | `["hfq"]` | Only hfq (backward-adjusted) factors are stored; qfq is derived at query time |

## `[sentiment]`

| Key | Default | Description |
|----|------|------|
| `use_snownlp` | false | Optional SnowNLP for on-demand `stock_news` (the package is installed by default); daily batches use keywords |
| `news_symbol_limit` | 50 | Symbol cap for the HTTP `stock_news` fallback fetch (the main channel is curated `news_headlines`) |

## `[failover]`

Multi-source snapshots and diffs; never switches canonical automatically.

| Key | Description |
|----|------|
| `enabled` | Master switch |
| `backfill_snapshots` | `false`; whether to fetch fallback-source snapshots on the critical path of historical backfills. Off by default so a slow fallback source does not block the canonical backfill; turn it on explicitly when you need cross-source historical diffs. The EastMoney snapshot for corporate_actions goes back to 2015-09-29 by default, while the main backfill still uses the research floor of 2001-01-01 |

### `[[failover.datasets]]`

| Key | Description |
|----|------|
| `name` | Dataset name |
| `primary` | Primary source adapter name |
| `backup` | Fallback source (writes a snapshot when a primary-source batch fails) |
| `compare_fields` | Fields compared in the audit diff |
| `price_tolerance_bps` | Price tolerance (basis points) |

Default config: `daily_bars` (TDX primary / EM fallback), `corporate_actions` (EM primary / TDX fallback).

## `[universe]`

| Key | Default | Description |
|----|------|------|
| `default` | `"all_a"` | Default universe in the config; `load()` does not filter automatically just because `universe` is omitted |
| `ingest` | `"all_a"` | Categories of symbols covered by daily update fetching |

`ingest` only constrains the **fetch scope** and is unrelated to the research selection definitions (`domain/universe_profiles.py`):

| Value | Meaning |
|----|------|
| `all_a` | Shanghai/Shenzhen/Beijing A-shares (default) |
| `all_a_sh_sz` | Additionally excludes the BSE |
| `all_instruments` | Every code listed by `instruments`, including ETF/LOF quote codes |

ST, suspended, CDR and delisted names are kept under every value — dropping them is exactly the survivorship bias this lake exists to avoid.

Explicitly enabling `[universe].ingest_listed_funds = true` adds listed ETF/LOF from the security master to daily fetching; research eligibility only filters research selections. Known prelisting and post-delisting sessions are excluded. Unknown listing dates are still requested and listed separately in per-security coverage; they cannot certify completeness. An unreadable catalog fails explicitly, and empty source responses remain gaps. This switch takes precedence over the legacy `ingest_eligible_etfs`. The legacy switch still fetches only research-eligible ETFs, but a failed, missing or more than 14-day-old eligibility catalog now raises an error.

With fund ingestion off, the full corporate-actions sweep skips ETF/LOF. If either switch is turned on later, the daily corporate-actions step sweeps the xdxr history of funds never swept before, at most 500 per run (about 2,200 market-wide, so it takes a few days); newly listed funds are swept once as well. Until the sweep finishes, each run warns `fund_history_unswept` and lists some of the codes; meanwhile those funds' adjustment factors lack their older unit splits.

## `[datasets]` {#datasets}

Best-effort datasets (`best_effort`, see [support levels](../datasets/catalog.md#support-levels)) are not scheduled by default: live snapshots whose history starts the day a lake turns them on, a retired source, and feeds that fail often. List the ones to run:

```toml
[datasets]
best_effort = ["news_headlines", "flash_news_wire", "hot_rank"]   # or ["all"]
```

A best-effort dataset named in the config file's own schedule-group `steps` or `steps_add` is also on; only what the file itself says counts, never the built-in defaults under it. So a full legacy config keeps running what it lists, and a new `extends = "builtin"` config starts with all of them off. A dataset left off is dropped from every schedule group and its freshness reads `removed`, not `stale`; `cne backfill` can still run it explicitly. `cne config minimize` writes the set a full config enabled into `best_effort`, so its behaviour does not change.

## `[job.daily.waves]`

Wave DAG: each wave has `name`, `parallel` (whether steps within the wave run in parallel) and `steps` (a list of step names).

Four waves by default:

1. `reference` — instruments, trading_calendar (in parallel); trading_status runs after instruments completes (to pick up securities newly listed that day)
2. `corp_actions_to_bars` — corporate_actions → daily_bars (serial)
3. `parallel_core` — index_bars
4. `finalize` — compact, derive_adj_factors, audit

`validate_config` requires at least one wave, and every step name must be in `STEP_REGISTRY`.

## Schedule groups {#schedule-groups}

`[job.daily.groups.<name>]` (see [Extending the built-in defaults](#extends-builtin) for `steps_add` / `steps_remove` / `enabled = false`): `at` (reference time for documentation/scheduling), `steps` (including the trailing `compact`).

The daily update and catch-up entry points use Beijing time `[job.daily].run_at` (default 17:30) and `[job.stale].run_at` (default 21:00). The in-group field `at` is a run reference; config alone does not install a system scheduler.

`core`, `capital`, `signals`, `fundamentals`, `macro_risk` and `research` are the routine data groups; minute bars, trade ticks and derivatives are enabled by their own switches. The bundled template is authoritative: a config that extends the defaults picks up new steps on upgrade, and a full config adds them with `cne config upgrade`. Do not copy timings from a personal host to set task intervals for everyone.

All daily tasks share a non-blocking write lock. Prefer a single scheduling entry point that runs the needed groups in sequence; multiple independent scheduled tasks may collide on the lock and be skipped, so you must check exit codes and run status.

`cne run daily` without arguments runs all groups in configured order and then the event streams; `cne run daily --group <name>` runs only that group's steps.

### Run times (`run_at` of `[job.daily]` / `[job.stale]`)

```toml
[job.daily]
run_at = "17:30"   # Beijing time (default 17:30)
[job.stale]
run_at = "21:00"   # Beijing time (default 21:00), catches up snapshot-type data only
```

The scheduled task wakes every hour and runs once on each trading day after this Beijing time has passed, independent of the local time zone and daylight saving time.
See the [runbook](../operations/runbook.md) for details.

### Fetch frequency (`cadence` / `weekday`)

Each group can set its own frequency; the default is daily:

```toml
[job.daily.groups.fundamentals]
cadence = "weekly"   # "daily" (default) or "weekly"
weekday = 5          # A weekly group runs on the last trading day on or before this day, ISO 1=Monday…5=Friday (default 5)
```

- **A weekly group only rests datasets that "can be filled by date"**: the next run fills day by day from after the watermark, so no data is lost;
  the total request volume stays roughly the same, just concentrated into one run.
- **Snapshot-type datasets still run every day**, even if their group is weekly (fund flow, hot lists, ST/suspension status and the like only have "today";
  missing a day means that day is missing forever); the same-group steps they depend on (for example, `trading_status` needs that day's `instruments`) run too.
  To see which are snapshot-type, check the [sources page](../datasets/sources.md) or `DatasetSpec.fetch_semantics`.
- A weekly group runs on the last trading day of the week (no later than `weekday`): if Friday is a market holiday it moves up to Thursday, and the week is never skipped entirely.
- On a rest day, when the whole group has no steps to run, `cne run daily --group` outputs `skipped_not_scheduled`, and the daily update script records SKIPPED.
- Freshness follows the frequency: a historical dataset owned only by a weekly group is judged stale against the trading day that group was last due to run,
  so `cne status`, the catch-up at `[job.stale].run_at` (default 21:00 Beijing time) and the health gate do not treat it as behind on rest days and fetch it again.
- When a dataset is in both a daily and a weekly group, daily wins.

## `[source_health]`

| Key | Type | Default | Description |
|---|---|---|---|
| `vantage` | string | `local` | Label of this machine's network egress (`cn`, `overseas`, or your own, e.g. `cn-sh`), carried by every source probe and health record; some sources refuse non-mainland egress, so results are read per label. `CNE_SOURCE_VANTAGE` and `--vantage` override it for one run |

## `[job.maintenance]`

Settings of `cne run maintain`, which the timer `cne schedule install` installs runs after each trading day's update; see the [runbook](../operations/runbook.md#maintain).

| Key | Type | Default | Description |
|---|---|---|---|
| `full_audit_weekday` | int | `6` | ISO weekday (1 = Monday … 7 = Sunday) of the whole-lake audit, which reads every Parquet file and can take hours on a large lake. 0 never runs it |
| `backup_dir` | string | `{data.root}/backups` | Metadata backup directory; a relative path resolves against the config file's directory. Another disk is best |
| `backup_retention_days` | int | `14` | Delete backups older than this many days; 0 disables the age limit |
| `backup_retention_count` | int | `30` | Keep only this many newest backups; 0 disables the count limit |
| `gate_groups` | list | `["core"]` | A failed run of these daily groups fails maintenance at once |
| `soft_fail_max_days` | int | `3` | Any other failed daily group only warns, until it has failed this many sessions in a row; 0 never escalates |

## Event-stream schedule groups (24/7)

`[job.events.groups.<name>]`: the same fields as schedule groups (`at`, `steps`, `parallel`), but they belong to
a different task family — `cne run events`. There are only two differences, both required:

- **It ignores the trading calendar.** Listed companies publish announcements on Saturdays too, and news sources update around the clock; `daily*` tasks
  return `skipped_non_trading_day` directly on non-trading days, but event streams do not.
- **A different lock.** Event streams take `events_ingestion`, not `daily_ingestion`, so event streams can still run while the evening batch
  is halfway through, and vice versa.

| Group | Typical time | Contents | Cost |
|------|----------|------|------|
| `disclosures` | 20:00 | `announcement_index` | Rereads a 30-day reconciliation tail window each time; not suited to high frequency |
| `regulatory` | 20:20 | `regulatory_events` | Projected from **committed** announcements; must run after `disclosures` |
| `news_wire` | 21:00 | `news_headlines`, `flash_news_wire` | A single real-time page; for intraday freshness, run this group alone at high frequency |

`cne run events` runs each group in the order listed in the config file (each publishing with its own `compact`),
and `--group <name>` runs just one. `cne run daily` without arguments also runs all event-stream groups after the daily update groups. For the timer, see
[`scripts/scheduler/events_pipeline.sh`](../operations/scripts.md) and the `com.cnequity.events` agent.

`validate_config` enforces two rules here: a group may only contain **calendar-day** datasets
(`DatasetSpec.session_scope = "calendar"`), and the same step must not appear in both `[job.daily]`
and `[job.events]` — the two tasks hold different locks, so fetching the same dataset at the same time would mean concurrent writes to the same staging.

`sentiment_scores` stays in the `research` group: it reads announcements and news **already committed to the lake**,
never ones fetched in the same run, so its behavior is unchanged by the split.

### Event schedules (`[[job.events.schedules]]`) {#event-schedules}

The packaged timer (`cne schedule install --events`) runs event groups on these schedules; without any, it uses the single group and interval set on the dashboard. Write one entry per event group:

| Key | Default | Description |
|---|---|---|
| `group` | omitted = all | A group from `[job.events.groups]`; omitted runs every group in config order |
| `every_minutes` | `60` | Minutes between attempt starts (1–1440) |
| `jitter_seconds` | `0` | After each run, push the next one out by a random 0–N seconds (0–3600), so requests do not land on the hour |
| `quiet_hours` | omitted | Beijing-time quiet window, e.g. `"1-7"` (01:00–06:59); `"23-6"` wraps midnight |
| `quiet_every_minutes` | `60` | Spacing inside the quiet window, with 5 minutes of slack |

```toml
[[job.events.schedules]]
group = "news_wire"
every_minutes = 15
jitter_seconds = 120
quiet_hours = "1-7"
quiet_every_minutes = 60

[[job.events.schedules]]
every_minutes = 360
jitter_seconds = 300
```

A group may appear only once, and it must exist. The dashboard's schedule page shows these schedules read-only; change them in the config.

## `[minute_bars]`

Optional intraday bars. Off by default and **not** in `[job.daily.waves]`; their request volume and disk usage grow with symbol scope, lookback window and trading activity, and never automatically become a cost of `cne init`. Once enabled, use `cne run daily --group intraday` or `cne backfill`, and check actual usage on a small scope first.

| Key | Default | Description |
|----|------|------|
| `enabled` | `false` | Master switch |
| `scope` | `"index:000300.SH"` | `index:<symbol>` / `watchlist` / `all` |
| `symbols` | `[]` | Explicit list when `scope = "watchlist"` |
| `frequencies` | `["1m"]` | `"1m"` → `minute_bars`; `"5m"` → `minute_bars_5m` |
| `fetch_workers` | `4` | Number of concurrent TDX connections; the shared interval still limits when requests start, and more connections do not mean the source allows a higher rate |

The **source-side horizon** is a rolling window; the current adapter rejects a `--start` earlier than the available start in the [dataset catalog](../datasets/catalog.md) to avoid useless scans; the actual upstream retention may change. For request and disk cost, see the [intraday data runbook](../operations/runbook.md#intraday-data-minute_bars--minute_bars_5m).

## `[futures]`

Per-contract futures/options default to `enabled=false` and are not part of `init`. Once enabled, they run in the `derivatives` daily update group; minute quotes also need `minute_enabled` and a watchlist. Exchange selection, history floors, contract reference and Greeks parameters are collected in the [derivatives guide](../recipes/derivatives.md); the bundled template is authoritative for the full set of keys.

## `[trade_ticks]`

Trade tick records. **A section of its own**, not a switch inside `[minute_bars]` — the two differ by an order of magnitude in volume, and enabling minute bars should not quietly bring this along.

| Key | Default | Description |
|----|------|------|
| `enabled` | `false` | Master switch |
| `scope` | `"watchlist"` | `index:<symbol>` / `watchlist` / `all` |
| `symbols` | `[]` | Explicit list when `scope = "watchlist"` |
| `max_symbols` | `200` | Cap on symbols per fetch |
| `fetch_workers` | `4` | Number of concurrent TDX connections; the shared request interval still limits starts, and more connections do not raise the configured request rate |

**These are not tick-by-tick trades.** A-share Level-1 data is a 3-second snapshot, and one row aggregates all real trades that fell in that time slice. Timestamps only go down to the minute — the protocol has never carried seconds — so rows are identified by `tick_seq` (position within the session). `direction` is TDX's own guess of the aggressor side using the tick rule, not an exchange field.

**History horizon**: TDX goes back to 2024-01-02 for every symbol; this is a **fixed floor** rather than a rolling window, and is unrelated to the per-symbol bar count cap for minute bars. A `cne backfill trade_ticks --start` earlier than this is rejected outright.

Collect with `cne run daily --group ticks` or `cne backfill trade_ticks`.

## `[quality]`

| Key | Default | Description |
|----|------|------|
| `audit_gate` | `"shadow"` | What happens to this run when the lake audit reports an `error` |

Three levels:

- `off` — records nothing and never fails (behavior before 0.8.2)
- `shadow` — records what "should have been stopped" but lets the run succeed
- `block` — fails the run, visible in `cne status` and in the pipeline exit code

The audit step depends on `compact`, so it runs **after the rows are already in curated**: `block` fails the run; it does not prevent the write. In shadow mode, each affected run appends a line to `meta/quality/audit_gate.jsonl` — read it before switching to `block`. A gate turned on without knowing how often it will fire soon gets turned off.

Only `error` triggers the gate; `warning` and `info` do not. See [product boundaries](../architecture/overview.md).

## `[incremental]`

| Key | Default | Description |
|----|------|------|
| `negative_evidence_ttl_days` | `7` | Validity period of negative evidence such as "the source is empty here"; set `0` to retry missing keys every time. A revision of the instrument catalog still invalidates evidence within the validity period |
| `deep_reconciliation_dow` | `6` | Day of the week for the weekly deep reconciliation, 1=Monday … 7=Sunday; `0` turns tiering off so every run reconciles deep. `announcement_index` looks back 7 days on ordinary runs and 30 on the deep day; `daily_bars` fetches only new sessions on ordinary runs (exchange whole-board quotes and TDX batch quotes) and re-checks the last 5 sessions symbol by symbol on the first session on or after that weekday |

Announcements may be indexed late. The weekly deep reconciliation rechecks older windows and reduces routine repeated requests, but it may also delay discovering late announcements. Strict PIT also checks the in-lake observation time; an announcement collected late cannot pass as known at an earlier `as_of`.

## `[raw_archive]`

| Key | Default | Description |
|----|------|------|
| `enabled` | `true` | Whether to store compressed raw source responses in `meta/raw` |
| `compression` | `"gzip"` | Compression method |
| `max_payload_bytes` | `33554432` | Archive cap per response (32MB) |
| `datasets` | Built-in critical/snapshot set | Leave empty to use the default set; paged raw responses from financial statement and shareholder backfills are archived page by page to keep evidence from pages that succeeded midway. An explicit list can narrow it to fit the disk budget. |

**Request credentials, proxy settings, cookies and authorization headers are never archived.**

Large payloads such as financial report PDFs can live on a separate drive or in iCloud Drive while source, observation-time and hash sidecars stay local. Create the destination directory, then register it:

```bash
cne storage raw-external --dataset financial_statement_items --source cninfo \
  --root "$HOME/Library/Mobile Documents/com~apple~CloudDocs/CNEquity/archives/my-lake"
```

By default only new PDF payloads go to the external folder; other responses stay local. The lake stores the registration in `meta/raw_storage.json`, which all archive readers recognize automatically. Historical `payload_path` values stay unchanged and no symlinks are used. Registration does not migrate existing files. Copy them under the external root at their original `payload_path`, verify bytes and hashes, and confirm cloud upload before removing local payloads. Keep the local `.json` source sidecars.

Replay requires the external folder to be available; cloud files may need downloading first. An unavailable folder fails explicitly rather than being recreated empty or fetching the provider again. Portable snapshots materialize external payloads without machine-specific locations, which may download cloud files. macOS manages iCloud synchronization and Remove Download; registration alone does not confirm upload.

## `[storage.retention]` {#storage-retention}

Every backfill and daily publish leaves an immutable generation. At the end of `cne backfill` and `cne run daily`, each dataset keeps the newest 5 generations and the older generation directories are removed. That is the same count the storage page uses for a recent generation. The generation currently being read, a generation registered with `cne storage hold`, a generation whose receipt is missing or unusable, and all staging are never removed by this automatic cleanup. Receipts stay under `meta/revisions/` as the record of what was published; reading a removed generation fails explicitly and does not follow a newer one. An interactive `cne run clean` can confirm deletion after the preview. Deleting a generation inside the newest 5 that is not the current pointer is still confirmed on the storage page of `cne serve`.

| Key | Default | Description |
|----|------|------|
| `keep_generations` | `5` | Newest generations to keep per dataset. `0` does not delete automatically |

A file hard-linked from a generation that is staying does not disappear with the old directory. `freed_bytes` counts only bytes that no kept generation still points at. With no imported reference inventory, "not registered" is treated as "no external reference". To pin one publication for a reproduction, run `cne storage import` and then `cne storage hold`, or set `keep_generations` to `0`. A small disk can lower the count.

A retention failure (a stale reference inventory, or an unfinished deletion) does not fail a publish that already succeeded. The reason is printed on stderr, and the next backfill or daily run tries again.

## `[exchange_audit]`

| Key | Default | Description |
|----|------|------|
| `price_tolerance_bps` | `10` | Close price deviation tolerance (basis points) |
| `turnover_tolerance_bps` | `100` | Turnover deviation tolerance (basis points) |
| `turnover_max_fraction` | `0.15` | Share of the whole universe needed to trigger a finding |

Compares `daily_bars` against the close prices published by SSE and SZSE themselves — **the only price check in the whole lake that reaches the publisher rather than a second reseller**. Controlled by `[sources.exchange]`; findings are advisory and never fail a run.

SSE only provides the session it is currently publishing, so SH is same-day arbitration; SZSE can be queried for any historical date.

Exchange daily totals and daily-bar data may differ in statistical scope. The turnover check uses a separate tolerance and triggers a finding based on the share of symbols that deviate; when it warns, check the definitions and sources first.

## `[margin_trading]`

| Key | Default | Description |
|----|------|------|
| `source` | `"exchange"` | Source of margin trading details |

`"exchange"` reads the margin trading details from SSE and SZSE directly; they are aggregated from member firms' reports — with no reseller in between.

## `[job.init.phases]`

| Key | Description |
|----|------|
| `names` | Ordered list of init phases |

Default:

```toml
names = [
  "phase1_reference",
  "phase2c_daily_bars_backfill",
  "phase2a_corporate_actions",
  "phase3_index_and_status",
  "phase4_finalize",
  "phase5_derive_and_publish",
]
```

For the phase → step mapping, see `orchestrator/init_phases.py`. Since 0.17.0 the daily-bar phase runs before corporate actions; a config that still carries the old default order verbatim (corporate actions first) runs in the new order, an edited list runs as written, and an init already started resumes in the order it recorded.

## `[on_demand]`

| Key | Description |
|----|------|
| `enabled` | OnDemandService switch |
| `datasets` | List of dataset names fetched on demand. By default only `stock_news` and `research_reports`; `announcement_body` / `financial_reports` are not implemented yet |

Cache path: default requests use `meta/on_demand/{dataset}/{symbol}.json`; when parameters that change the result are present, a variant file in the same directory carrying a request digest is used, so queries with different dates, counts or sentiment models do not reuse each other's results. Access via `cne query --dataset X --symbol Y`; append `--refresh` to force an update. Failed or unimplemented results are not written to the cache.

## `[duckdb]`

| Key | Default | Description |
|----|------|------|
| `path` | `{data.root}/duckdb/cnequity.duckdb` | Supports the `{data.root}` placeholder |
| `memory_limit` | `2GB` | DuckDB memory limit |
| `threads` | 4 | Number of query threads |

## `[research]` {#research}

Optional. Affects only the `cne decision-data` evidence inventory command, not ingestion or queries.

| Key | Default | Description |
|----|------|------|
| `holdout_start` | Unset | Holdout period start date (for example `2025-01-01`). A window that touches this date is rejected before reading the lake, and `--end` defaults to the day before it, so the research evidence inventory cannot see data held out for out-of-sample testing |

## Environment variables

The following variables are used by the [operations scripts](../operations/scripts.md). The CLI also reads `CNE_CONFIG` (lower priority than an explicit `--config`) and `CNE_LOG_DIR`; `CNE_RATE_LIMIT_ROOT` is used by shared source protection. The other script variables should not be treated as general CLI parameters.

| Variable | Default | Purpose |
|------|------|------|
| `CNE_CONFIG` | `configs/cnequity.toml` | Path the scripts pass to `cne --config` |
| `CNE_LOG_DIR` | `{data.root}/logs` | Log directory. `cne init` / `cne backfill` / `cne run` also read it, write this run's log as `cne-<command>-<timestamp>.log`, and print the path at startup |
| `CNE_GROUPS` | Enabled schedule groups selected in configured order | Overrides the groups the pipeline runs |
| `CNE_NOTIFY` | `1` | `0` disables macOS notifications |
| `CNE_BACKUP_DIR` | backups inside the lake | Metadata backup directory |
| `CNE_BACKUP_RETENTION_DAYS` | 14 | Backup retention in days |

## How config relates to code

```
cnequity.toml
    → load_config() → Config dataclass
    → validate_config() → validity of referenced steps/groups
    → JobEngine(cfg) / load(..., config=cfg)
```

`Config` also provides: `staging_root`, `curated_root`, `derived_root`, `meta_root`, `manifest_path`, `rate_limit(source)`.

## Pre-publication and post-publication audits

`[quality].publication_gate` supports `off` (default), `shadow` and `block`. Before an ordinary compact publish, it runs an offline full audit of the whole candidate batch and compares it against the committed baseline; block mode rejects new or worsened errors, and also rejects on audit exceptions. Issues are identified by "check + dataset + scope (partition, field, source, date, etc.)"; only an increase in a measure such as row count or key count counts as worsening; changes in wording or samples, and fewer issues, do not count as new. The report's `issue_changes` lists new, worsened, improved, unchanged and resolved issues separately. Structural checks read only the changed partitions of candidate datasets; cross-dataset checks run only when their inputs include a candidate dataset, and other check results are identical before and after, so they are not recomputed. Derived publishes use the same gate.

`cne repair` subcommands with `--apply` always run a blocking structural audit. Historical factor/action contradictions are reported separately as advisory diagnostics and never reject publication. Invalid factor values, corrupt files and primary-key errors can still block. Rejected candidates go to `_quarantine`, preserving the old version.

`[quality].audit_gate` still determines post-publication run status; neither replaces the other. A full-history scan has I/O cost, so you can start with shadow and then move to block. See [product boundaries](../architecture/overview.md).
