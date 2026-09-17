# Changelog

All notable changes to this project are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
the project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

Coverage and provenance. The Beijing exchange becomes a first-class source, two
gates stop passing for the wrong reason, and four defects that were writing bad
rows every day are fixed — with migrations for what they already left behind.

### Added

- Explicit fund unit split/consolidation events (`unit_split`, `split_factor`)
  and matching adjustment-factor checks. Corporate actions use schema v2;
  legacy events retain neutral unit multipliers. The next development package
  is `0.10.0.dev0`; the published v0.9.0 contract remains unchanged.
- Trade-only intraday resampling, APFS copy-on-write revision copies, and
  bounded Baostock login/logout with query deadlines that survive SDK handlers.

- **The Beijing exchange's own board** now supplies BJ listings, halts and the
  ST designation, in one paginated read shared with the quote path. TDX serves
  SH/SZ only, so BJ had entered the catalogue solely by replaying the last
  manual code-space sweep; on 2026-09-15 that left 16 names trading with real
  volume and no row anywhere in the lake. Beijing encodes a halt as *absence*,
  so it is only read as one after a complete walk, never from a board still
  showing the previous session, and only for live names — 241 retired codes
  would otherwise be permanent halts.
- **The SSE and SZSE boards** as an independent `trading_status` backup: a halt
  is a zeroed open/high/low beside a reference close, the designation is in
  证券简称. Measured over 5,219 symbols for 2026-09-15, ST agreed 100.000% and
  halts 99.923%, with all four disagreements being EastMoney calling a name
  halted while the exchange published a full session.
- `[universe].ingest` bounds every implicit fetch scope (`all_a`,
  `all_a_sh_sz`, `all_instruments`).
- `DatasetSpec.supplementary_sources`, so a source that writes rows is declared
  rather than reported as unrouted.
- `cne status --datasets --groups` gates only on the schedule groups a host
  actually runs; `cne config diff` reports drift against the shipped template.
- **Proof that a long run is alive** (#37). Every step names itself when it
  starts, not only when it ends; a heartbeat names whatever is still running
  after 60s of silence; and a bar sweep states its scope — symbols, window,
  batches, lanes — before the first batch can land. Measured against the gaps
  this closes: `instruments` ran 183s behind one line, and a one-session
  `backfill daily_bars` waited 68s for its first sign of life.
- **A fourth vendor in the `daily_bars` failover chain.** baostock has been
  declared supplementary since the spec was written, but nothing live ever
  called it — the code path existed only for delisted recovery. That left two
  per-symbol vendors, and when EastMoney's history host is unreachable (every
  `push2his` host dropped the connection from the measured vantage while
  `push2` kept serving clist) one remains, which can never certify a no-data
  key: certification needs two independent empties. A full-market session whose
  missing names had simply not traded failed with "expected key(s) remain
  unknown". baostock answers from its own failure domain, serves live symbols
  exactly (matching Sina to the cent on the session that failed) and reports a
  suspended session as no row rather than as an error. Opt-in through
  `[sources.baostock]`, like the THS link.
- **A backup for `adj_factors`**, the one core dataset that had a single vendor
  behind it. Sina bans by account rather than by endpoint, so the HTTP 456 that
  stops an unrelated futures sweep also stops every return in the lake from
  being computable. Baostock publishes the same series raw and back-adjusted,
  and their ratio is this factor in the stored multiplier convention; rescaled
  to 1.0 at the tip it is Sina's level convention too. Measured on 600519.SH
  across its 2026-06-26 ex-date: implied step 0.976883 against Sina's 0.976880,
  which is the rounding in Baostock's two-decimal adjusted closes. Rows now
  carry the vendor they actually came from, because one run can mix both.
- **A probe for Sina's futures host.** `commodity_bars` — the domestic
  main-continuous board and the offshore one — is served by
  `stock2.finance.sina.com.cn`, which no probe covered, while the only endpoint
  still declaring the dataset was the EastMoney history host that Sina replaced
  for being unreliable. Reachability is per host: that is the same lesson
  `push2` and `push2his` taught.
- **`cne sources substitutes`** answers the question a probe report does not:
  this endpoint is down — what reachable one can carry its datasets, and is it
  independent of what failed. Ranked independent-first, then fastest; exits
  non-zero when a dataset has nothing left. Dataset→source comes from the
  registry rather than from the probe table's hand-maintained `powers` lists,
  which had drifted: reading those made `commodity_bars` look stranded and
  `corporate_actions` too, while the latter's declared TDX backup sat there
  healthy. The probes answer the other half — which *endpoint* of a source is
  reachable — so a vendor that is up on one host and down on another reports as
  exactly that.
- **Rolling progress for the sweeps that had none.** The baostock valuation
  backfill, the shareholder/top-holder window walks, the Sina adjustment-factor
  fan-out and `compact` now report as they go, through one `sweep_progress`
  helper factored out of the TDX xdxr sweep that already did this.
- **A log file per long run.** `cne init`, `cne backfill` and `cne run` write
  `cne-<command>-<timestamp>.log` under `CNE_LOG_DIR` (default
  `{data.root}/logs`) and print the path on startup, so a run that failed after
  three hours leaves something to read. `CNE_LOG_DIR` was read by the pipeline
  scripts and by nothing in the CLI.

### Fixed

- **Run logs are pruned.** `attach_log_file` writes one timestamped file per
  invocation into `{data.root}/logs/` and nothing ever removed them — a daily
  pipeline leaves a file per group per day, plus one per retry and backfill,
  for as long as the lake exists. `cne run clean --log-retention-days` (default
  30) drops the aged ones, touching only files this CLI names.
- **`cne backfill` refuses a reversed date range instead of sweeping on it.**
  `--start 2026-01-02 --end 2026-01-01` walked an empty window for 24 seconds
  of real requests, the step raised, the engine logged the traceback — and the
  command still printed `status: success` with `rows_written: 0`. `derive`,
  `verify --bars` and `audit` all checked this already.
- **`cne verify --dataset` refuses a name it does not know.** The library
  warns and skips an unknown dataset, which is right when sweeping the whole
  registry and wrong for a name the caller typed: a misspelt `--dataset`
  printed "覆盖完整：没有可修复的缺口" and exited 0, so a typo read as proof
  the lake was fine. It now fails with the same near-miss suggestion
  `cne backfill` gives.
- **`cne sources resilience` no longer ignores a `--config` it cannot find.**
  The report is computed from the registry, so the option is only read for
  `--with-availability` — which meant a typo'd path was silently accepted and
  the command exited 0. An explicitly passed `--config` is now resolved either
  way. Its `--help` also claimed a domain carries 29 datasets; the measured
  figure is 30.
- **The progress heartbeat can be stopped, and no longer rides on `time.sleep`.**
  Its thread looped on `time.sleep(5)` with no stop signal, so in a test process
  it outlived the test that started it. A later test faking `time.sleep` to
  capture its arguments — `module.time` is the shared `time` module, so the fake
  applies process-wide — turned that loop into a full-speed spin: one run put
  34,735,321 entries into an unrelated assertion. It only lined up under CPU
  contention, so it flaked rather than failed. The thread now waits on its own
  `threading.Event`, `stop_heartbeat()` ends it, and a test fixture clears it
  between tests. Interval, poll and log output are unchanged.
- **`cne ths-official` runs are recorded in the manifest.** The three commands
  that stage rows minted a run id, staged against it and told you to pass it to
  `cne run compact` — but never opened a run, so the id named nothing. The rows
  reached curated; what was missing was the parent row. `cne status` could not
  see the run, and `cne run clean` files staging it cannot prove is finished
  under *skipped*, which is never reclaimed rather than aged out the way a
  manifest-less orphan is: three `ths-*` runs had stranded 23MB that way.
- **The `daily_bars` tip-key failure says what to do about it.** It reported a
  count and nothing else — not which keys, not which vendor was down, not which
  command resumes the run. It now names the findings file, `cne sources probe`,
  the exact `cne run retry --run-id`, and a `--symbols` repair for just those keys.
- **A blocking lock wait is bounded and says who it is waiting for.** `compact`
  and every lake mutation queue behind a peer with `blocking=True`, and that
  wait had no deadline and no output — indistinguishable from a hang, which is
  what it becomes when the holder is alive but stuck. A crashed holder was
  never the problem: the kernel releases its lock as the process dies. The wait
  now announces itself ("waiting for compact.lock — another process holds it"),
  gives up after `DEFAULT_LOCK_WAIT_SECONDS` (1h, generous so a legitimately
  long compact still queues), and the error names the lock file, `lsof`, and
  `cne status`. A nested acquire in the same process still fails immediately
  rather than timing out against itself.
- **A crashed run stops lying about being alive.** Liveness was inferred from
  heartbeat age, so a run killed mid-flight kept `status=running` in the
  manifest for `batch_stale_seconds` — an hour by default — and every command
  that reads run status was wrong for that hour: `cne run retry --failed-groups`
  answered "No failed daily group run to retry" about the very crash the
  operator was looking at, and `cne status` showed a ghost. Every run now holds
  `meta/locks/{run_id}.lock` for its lifetime, which the kernel releases the
  moment the process dies, so an unlocked `running` row only has to outlive a
  short grace window (60s, covering the gap between recording the run and
  taking the lock) instead of the full stale timeout. A shorter configured
  timeout still wins — the grace period caps how long a corpse may linger, it
  never makes anyone wait longer than they asked. `retry --failed-groups`
  reconciles before it selects, so it sees the crash rather than the ghost.
- **A killed `cne init` can be started again.** An init that looked hung, got
  killed and was run again was refused — the one command the operator had left,
  answering "no" to the one thing they were trying to do. The refusal is right
  for a *live* peer and wrong for a dead one, and nothing distinguished them:
  init took no lock, so a run left `running` by SIGKILL looked exactly like a
  run in progress. It now holds `init_job` for its duration, so a second `cne
  init` resumes the unfinished run when that lock is free and refuses only when
  another process is really holding it. It still never starts a second full
  init over an incomplete one.
- **A one-session backfill says the window is free to widen.** TDX returns up
  to 800 bars per request, so the sweep costs the same whether it asks for one
  session or every year on record; filling a month a day at a time pays for
  thirty sweeps and receives what one would have returned. Said only for a
  backfill — the daily job's one-session window is its point.
- **A batch progress line no longer overstates a failure.** One symbol TDX had
  no rows for was reported as `(100 symbols FAILED)` — the batch size, not the
  scope — on a sweep that then recovers most of them through failover. It now
  reads `(1/100 symbols failed)`.
- **The batch ETA stops swinging.** Estimating from wall clock over batches
  done charges the sweep's one-off setup, and the first `lanes` batches land
  together one batch-latency in: a measured 53-batch run printed ~34m, ~17m,
  ~11m, ~8m for ten minutes of work. The estimate is now timed from the first
  drained round of lanes, and no number is offered before there is one.
- **`trading_status` no longer asserts facts its source never served.**
  EastMoney's ST board selects Shenzhen and Shanghai only and its suspension
  feed does not reach Beijing either, yet both were stored as findings:
  `risk_warning=False` for all 343 live BJ names while the exchange listed
  \*ST康乐/\*ST田野/\*ST同辉, and 15,515 BJ rows every one `is_trading=True`
  against 116 halts in 148,933 SH/SZ rows. Unserved exchanges get null, and
  Beijing's rows come from Beijing. A delisted name with no 简称 is likewise
  unknown rather than clean — 253 retired BJ codes were published as
  not-under-risk-warning on every session since delisting.
- **`valuation_metrics.ps_ttm` read EastMoney `f45`**, an amount in yuan rather
  than a ratio: 96.8% of values above 1000, median 2.05e7 against baostock's
  3.2. The field is `f130`. An audit shape check now fails any ratio column
  holding amounts, per source.
- **Provenance can no longer be defaulted.** `normalize_with_source` defaulted
  to `"tdx_protocol"`, which is how 58,672 `trading_status` rows came to name a
  vendor serving no status feed. The argument is mandatory and all eight call
  sites name their vendor.
- **A pre-IPO code no longer blocks the market snapshot.** A code with no
  `list_date` and no traded bar anywhere has not started trading, so a missing
  tip for it is a pending listing, not a coverage gap; two such codes were
  failing `daily_bars` on every run.
- **The freshness gate matched the scheduler.** A core-only host has twenty-odd
  datasets no job fetches, so the unscoped gate failed daily — 21 to 25 stale on
  2026-09-12/13/14 — and three genuinely UNHEALTHY days went unread inside the
  noise. A pure freshness miss is also titled 数据滞后, not 数据异常.
- The Sina futures sweep backs off on HTTP 456 instead of raising straight out
  and failing the whole `commodity_bars` step.
- An inverted `delist_date` is cleared on the merged frame, not just the
  incoming snapshot, since the same codes are dropped from the live list again.
- SLO targets come from a vantage class (`cn` / `overseas`), never a location,
  so an overseas host is not held to mainland availability it cannot reach.

### Changed

- **Command names are case-insensitive, and `-h` works.** `cne STATUS` used to
  be a dead end: Click's suggestions run on edit distance, and an all-caps
  spelling is too far from its own lowercase to be offered, so the error named
  no way forward — while `cne Status` did get one. One `token_normalize_func`
  makes commands, subcommands and options agree; `cne config CREATE` normalises
  in the command body, since a free-form argument bypasses that hook. Prefix
  matching is still off, so `cne stat` is refused rather than guessed at.
- **Stale counts and a superseded contract claim, in the published docs.** The
  steps module page said 40 registered steps against 47; the `cne sources
  resilience` sample output and the prose beside it still showed the old blast
  radii (eastmoney 29/4, tdx 9/6, exchange 2/1 against a measured 30/4, 8/5,
  4/3); and the contract page described `pit_quality` falling back to the
  literal `strict` for non-PIT tables, with the `not_applicable` convergence
  written as a future breaking change — it had already shipped, and 29 datasets
  carry it today. A test now checks these counts against the registries.
- **Every `bash` block in the docs is valid shell.** Placeholders were written
  `<run_id>`, `<id>`, `<coverage_start>`, which bash parses as a redirect, so
  seven blocks failed `bash -n` and could not be copy-pasted. They now use
  Click's own uppercase convention (`RUN_ID`, `COVERAGE_START`).
- **The published docs cover what the code actually has.** `cne ths-official`
  had no section in the CLI reference at all; six config sections (`[quality]`,
  `[incremental]`, `[raw_archive]`, `[exchange_audit]`, `[margin_trading]`,
  `[trade_ticks]`) were undocumented; the source list said 7 sources where the
  template ships 14, and described `exchange` as audit-only after it became the
  `margin_trading` primary; the module tree omitted `provenance`, `diagnostics`,
  `compliance` and `mcp_server`; and `stale_pipeline.sh` — a third scheduled
  job with its own launchd template — appeared nowhere in the ops scripts page.
- **The top-level command surface is 19 entries, sectioned** (was 25, flat and
  alphabetical). `cne --help` now groups them the way a lake is used — set up,
  run, check, consume, govern — instead of listing `audit`, `verify`,
  `verify-bars`, `stability` and `status` side by side as if choosing between
  them were obvious. Typing a moved name prints where it went.
  - `cne verify-bars` → `cne verify --bars`; `cne stability` → `cne verify
    --runs`. All three ask "did what should have landed, land?", at three
    grains. Options are refused by name across modes, not ignored.
  - `cne retry` / `cne compact` / `cne clean` → `cne run retry` / `run compact`
    / `run clean`. Every schedule group already ends in a `compact` step, so
    these are the manual path back from a failure, not part of a healthy day.
  - `cne demo` → `cne init --profile demo` (and `--sample` → `--profile
    sample`). How much of the market to build is one axis: demo, sample, quick,
    full.
  - `cne config init` → `cne config create`, one word away from `cne init`,
    which builds a lake and is far more expensive to run by mistake.
  - `cne ths-official snapshot` → `cne ths-official capture`, which no longer
    collides with the unrelated `cne snapshot`.

- The daily audit inspects active partitions; the whole-lake sweep moves to
  once a week (`CNE_FULL_AUDIT_DOW`). It reads every historical Parquet file —
  1h39m wall for 7m of CPU on a 25 GB lake — and had been running every
  trading day.
- Steps persist their findings, so a failed run explains itself.

### Removed

- Two byte-identical image duplicates: `docs/assets/architecture-overview.png`
  (same bytes as `architecture-diagram-v2.png`) and `og-image.png` (same bytes
  as `og-image-brand.png`), plus `cne-serve-dataset.png` — nothing embedded it
  and it printed `cne retry --run-id`, a command that no longer exists.
- `cne servers test`, deprecated in favour of `cne sources probe --only
  tdx_protocol` and scheduled for removal in 0.9.0. `cne snapshot delta-create`
  and `cne status --run-id`, both aliases of a spelling that already existed.

### Migrations

Each quarantines before it edits and republishes through the revision store, so
curated and committed readers cannot disagree afterwards. All default to a dry
run; `--apply` is required.

- `migrate_null_bad_eastmoney_ps_ttm.py` — the `f45` values above
- `migrate_relabel_trading_status_provenance.py` — 58,672 rows naming TDX
- `migrate_drop_unsupported_suspensions.py` — 1,188 suspensions that rested on
  nothing but "no source answered"
- `migrate_drop_nav_series_bars.py` — fund NAV series stored as daily bars

## [0.9.0] — 2026-09-13

One breaking data-contract change, and the storage/quality work that came with
the audit. `cne contract diff contracts/v0.8.1.json contracts/v0.9.0.json`
reports 29 breaking and 0 compatible differences, all of them the same field.

### Changed — BREAKING (data contract)

- **`pit_quality` gains `not_applicable`, and 29 datasets move to it.**
  The value fell back to the literal `strict` for any dataset that makes no
  point-in-time claim, so 29 of the 42 registered datasets published
  `pit_quality: "strict"` — `daily_bars`, `trade_ticks` and `trading_calendar`
  among them — while `announcement_index` was the only genuine one. The
  contract was complete (it also published `pit: false` and
  `pit_grade: "none"`), but a consumer reading `pit_quality` alone got the
  strongest possible claim for a table nobody had considered.

  **Migration.** No data changes and no re-fetch: this is a metadata value
  only. A consumer that treats `pit_quality == "strict"` as "safe for
  point-in-time research" should now read `pit` (boolean) or `pit_grade`,
  which were already correct. A consumer that enumerates the vocabulary must
  accept the fourth value. `announcement_index` is unchanged.

  Rationale and the rejected alternatives are in the naming-debt section of
  [ADR-0011](docs/adr/0011-bitemporal-columns-are-carried-not-required.md).


### Added

- **`cne run clean --keep-revision-generations N` (default 5), and
  [ADR-0010](docs/adr/0010-bounded-generation-retention.md).** Every commit
  copies the whole dataset into a new immutable generation and nothing ever
  removed one: 307 generations and 16 GB against 14 GB of curated data, of
  which `adj_factors` alone was 46 generations and 9.5 GB. Retention drops the
  bytes of old generations while keeping every receipt — the receipt is the
  lineage record and costs a few KB — and never touches the generation
  `current.json` resolves to. On the measured lake it reclaims 11.2 GB of
  16 GB. The ADR records why hard-linking unchanged files was implemented and
  reverted instead.

- **`[quality].audit_gate` and pipeline escalation, with
  [ADR-0012](docs/adr/0012-the-audit-gates-in-shadow-first.md).** 84 checks ran
  after every load and could not change what a run reported: `step_audit`
  recorded `success` unconditionally. It can now record `failed` on an `error`
  finding, and defaults to `shadow` — the run still succeeds, but every
  affected run appends its severity breakdown to
  `meta/quality/audit_gate.jsonl`, so the gate can be armed against measured
  evidence. Separately, `CNE_SOFT_FAIL_MAX_DAYS` (default 3) escalates a soft
  group that fails several days running: `CNE_SOFT_FAIL_OK=1` made every soft
  failure exit 0, which is how a group stayed down for three days unnoticed
  (`docs/operations/runbook.md`). One bad day stays warn-only; three in a row
  exits 1.

- **`scripts/sync_schema_docs.py`, gated in CI.** `docs/datasets/schema.md` was
  hand-maintained and had drifted: 11 of the 42 registered datasets had no
  section at all (`top_holders`, `share_structure`, `delisting_events`,
  `news_headlines`, …), and nothing tied a column added in `domain/schemas.py`
  to the page. The script syncs column names, order and types from the registry
  while carrying the hand-written `说明` cell across by column name — including
  rows that stand for a group (`open / high / low / close`), whose shared note
  now reaches each column instead of being dropped. Prose sections are never
  touched; `--check` fails the build when the two disagree.

- **`PIT 双时态扩展列` is its own section.** It sat under `#### instruments`,
  which is not a PIT dataset, so it read as a property of the wrong table.

### Fixed

- **`pit_quality` is documented as a placeholder, not a claim.** It falls back
  to the literal `strict` for any dataset with no point-in-time claim, so 29 of
  42 datasets publish `strict` — `daily_bars` included — while
  `announcement_index` is the only genuine one. The contract is complete (it
  also publishes `pit: false` and `pit_grade: "none"`), but reading
  `pit_quality` alone inverts the meaning. `docs/datasets/contract.md` now
  names `pit` as the field to read. Renaming the value is breaking on 29
  datasets and is left to a versioned release; see the naming-debt section of
  [ADR-0011](docs/adr/0011-bitemporal-columns-are-carried-not-required.md).

- **The bitemporal PIT columns now reach disk.** `available_at`,
  `source_published_at`, `observed_at` and `revision_id` are declared in the
  contract and normalised on read, but `validate_dataframe` projects to exactly
  the registered columns and therefore dropped all four on the way to a write —
  no PIT dataset could ever store one, and `reader.py` compensated by splitting
  them out and `hstack`ing them back after validation. Validation now carries
  them through when a frame already has them (registered schemas are unchanged;
  they stay optional), compaction materialises them, and a file that predates
  them is rewritten once. Reading a `financial_statement_items` partition drops
  from 0.55 s to 0.003 s, and the full dataset from ~19 s to ~0.1 s, because
  `revision_id` is no longer re-hashed row by row on every read.

  Two consequences worth knowing. `observed_at` is excluded from the business
  digest alongside `fetched_at` — it is the same fact under its bitemporal
  name, and counting it would mint a revision on every reconciliation pass.
  And storing a digest per row is not free, so `revision_id` is now a
  96-bit (24 hex character) truncated SHA-256 rather than the full 64: the
  chance of any collision across the current 12.2M PIT rows is ~1e-15, and
  the full width would have added 389 MB to 190 MB of curated parquet instead
  of 73 MB — every byte of which is also copied into each committed
  generation. Nothing had persisted the old width, so no migration is needed.

### Removed

- **`ROADMAP.md` and `CONTRIBUTING.md`.** The product boundary they described is
  already in [comparison](docs/comparison.md), and the development conventions —
  package layout, the commit convention, the CI gates and their local
  equivalents — now live in
  [development/conventions](docs/development/conventions.md), where the rest of
  the developer documentation is.

### Changed

- **`CODE_OF_CONDUCT.md` is three rules and a sentence.** A personal project does
  not need a reporting apparatus it cannot staff.

## [0.8.1] — 2026-09-13

No dataset contract changes: `cne contract diff` reports zero breaking and zero
compatible differences against 0.8.0. A lake without a HiThink key is unaffected
by everything in the Added section — the source stays inert and its peer checks
stay silent, per [ADR-0008](docs/adr/0008-optional-keyed-sources.md).

### Added

- **An optional licensed peer for the datasets the lake already holds.** Daily
  bars, financial statements, valuations, sector bars and corporate actions can
  be sourced from a licensed endpoint and used to arbitrate disagreements — a
  disputed value is switched only when a third source backs it. Includes the
  2016–2024 balance and cash-flow backfill, ETF codes routed to the fund
  endpoint, and valuation snapshots accumulated daily because the upstream keeps
  no history.
- **`cne ths-official` — the command group for that source.** `backfill`,
  `repair-bars`, `resource-sectors` and `snapshot`, each reporting `skipped`
  rather than failing when no key is configured.
- **Three checks the audit could not make before.** Balance sheets must balance;
  a valuation column holding amounts instead of ratios is caught; and the
  compliance register is checked against what `curated` actually holds rather
  than against its own routing table. The last one found two real gaps: `bse`
  wrote to `daily_bars` and `trading_status` with no policy entry, and six
  registered sources wrote to datasets that do not route to them.
- **The Beijing Stock Exchange is registered.** It was writing rows while absent
  from `sources/SOURCES.yml`, so its terms could not be looked up at all. Every
  permission field stays `unknown` — the site answers 403 from this egress.
- **ADR-0008 (optional keyed sources) and ADR-0009 (share dilution is one
  fact).**

### Changed

- **Chinese README now introduces the serve console.** The new 数据运维页面
  section explains what `cne serve` shows and embeds the labelled dashboard
  illustration.
- **The community files follow open-source convention.** `NOTICE` carries
  attribution only rather than a second copy of the licence text;
  `CODE_OF_CONDUCT.md` is in Chinese to match the rest of the project's
  first-language docs, and gains scope, a private contact and stated
  consequences; `SECURITY.md` states supported versions as a table.
  `CONTRIBUTING.md` moves to `.github/`, where GitHub still surfaces it, and now
  documents the commit convention and the CI gates with their local
  equivalents.
- **`ROADMAP.md` no longer tracks shipped work.** It had been left at "Now ·
  0.6" and "Next · 0.7" while the project shipped 0.8.

### Fixed

- **Every documentation link served a 404.** GitHub Pages is case-sensitive, and
  13 links — including `pyproject`'s `Documentation` URL shown on PyPI and
  mkdocs' own `site_url` — pointed at `rootsunc.github.io/cnequity` rather than
  `/CNEquity`.
- **Windows CI could not parse the test config.** Four `ths_official` config
  tests wrote a raw `Path` into a TOML string, so the runner's `D:\a\…` became
  invalid escapes. This was the only failing job on 0.8.0's last commit.
- **Open-end funds were classified as exchange-traded.** A `51` prefix swept all
  188 `519xxx` codes into the tradable universe, and TDX answered with a NAV
  series: 436,533 rows in `daily_bars` carrying a close but zero volume and zero
  turnover on every one.
- **A security that never traded could be inferred as delisted.** No bar history
  looked identical to history that stopped; absence of evidence is no longer
  evidence.
- **`.git-blame-ignore-revs` matched nothing.** August's history rewrite gave
  both formatting commits new ids, so GitHub silently ignored the file.
- **A test read a gitignored config.** `test_the_shipped_configs_run_it_before_audit`
  named `configs/cnequity.toml` — the user's own config — so it passed on a
  developer's machine and failed on every clean checkout. It now covers the two
  templates that actually ship.

## [0.8.0] — 2026-09-06

### Added

- **`cne run events` — a job for the feeds that publish 7x24.** Disclosures and
  news do not stop for a weekend, but `announcement_index`, `regulatory_events`,
  `news_headlines` and `flash_news_wire` were scheduled inside `daily` groups:
  the trading-day gate skipped the whole job on the very days they kept
  publishing, and every `daily*` group shares one non-blocking
  `daily_ingestion` lock, so an event sweep could only run in the gaps the
  heavy evening groups left. They now live in `[job.events.groups]`, run on the
  natural calendar, and take their own `events_ingestion` lock, so neither job
  waits for or skips the other. `validate_config` keeps the split honest: an
  events group may only schedule calendar-scoped datasets, and a step may not
  be scheduled in both jobs. Shipped scheduling adds
  `scripts/events_pipeline.sh` and a `com.cnequity.events` launchd agent (every
  calendar day, no weekday filter). `sentiment_scores` stays in `research` and
  is unaffected: it always read committed announcements and headlines out of
  the lake, never same-run fetches. Existing configs keep working untouched —
  the four steps go on running inside their daily groups until you move them
  into `[job.events.groups]`, which is how `configs/cnequity.example.toml` now
  ships them. `cne run daily --stale-only` skips whatever the events job owns,
  so the two never re-fetch the same feed under different locks.
- **`DatasetSpec.session_scope`** declares whether a dataset's date axis is an
  exchange session or a natural calendar day — one registry entry now decides
  the incremental walk, the empty-day guard and what the events job may
  schedule, instead of three separate lists.
- **The local dashboard has a rebuilt console shell.** Dataset state, run
  health and operational actions now share one responsive visual hierarchy and
  the same success/degraded/failed vocabulary as the CLI and manifest.
- **Python 3.14 is a tested interpreter, not just an installable one.**
  `requires-python` is `>=3.10` with no upper bound, so pip has been installing
  this package on 3.14 since its release while CI stopped at 3.13 — the same
  untested-claim gap the httpx floor job exists to close, one version up. 3.14
  now runs the suite on Linux and is the interpreter the macOS job pins, and the
  classifier says so. No source change was needed: every runtime dependency
  imports and the full suite passes there unmodified.
- **Checks against the bodies that publish the numbers, not a second vendor.**
  Every price arbiter in the lake compared one redistributor against another,
  which can show that two feeds do not differ but never that either is right.
  Three checks now reach past them (ADR-0006):
  - `daily_bars_vs_exchange` compares curated OHLC and turnover against the
    closes the SSE and SZSE publish themselves. Measured 2026-08-28: OHLC
    matched **exactly** on all 5,212 shared symbols, so the price tolerance is
    tight (10 bps) and a breach is an error. Turnover carries a one-directional
    definitional gap — the exchange daily total folds in trading a
    continuous-auction bar excludes — so it is judged on the share of the
    universe that diverges rather than per symbol. Suspension placeholders,
    which SZSE publishes at zero volume and a quote feed omits, are excluded
    from the missing-bar check.
  - `adj_factor_corporate_action_divergence` recomputes every hfq factor step
    from curated `corporate_actions` (a different vendor) using the ex-rights
    continuity identity, and reports where the stored series disagrees. This
    catches a step of the wrong size or on the wrong day; the existing
    continuity tripwire only saw breaks above 20x. Configured under
    `[adj_factors] crosscheck_*`.
  - `adj_factor_action_implies_nonpositive_price` flags action rows whose own
    terms cannot be right (a dividend exceeding the entire prior close).
- **`margin_trading` now reads from the exchanges that compile it.** The SSE
  and SZSE aggregate 融资融券 from member-firm reports and publish the
  per-security detail; EastMoney could only copy that file. Verified against
  the curated EastMoney day for 2026-08-26: all four fields matched exactly
  over 3,522 shared symbols, with 4,100 securities against EastMoney's 3,857.
  `[margin_trading] source` selects the owner and still accepts `"eastmoney"`;
  the switch is an operator's, never automatic.
- **Machine-readable dataset contracts.** `DatasetSpec` now carries
  `schema_version`, `contract_level`, `pit_quality`, `availability_col`,
  `unit_contract` and a compatibility policy, inferred so existing positional
  constructors are unaffected. `cne contract show/export/validate/diff` (and
  `cnequity.domain.contracts`) render the registry, schemas and primary keys
  into a deterministic fingerprinted JSON document; `diff` classifies column
  drops, type and primary-key changes, and unit/PIT/history changes as
  breaking. See `docs/datasets/contract.md`.
- **Point-in-time query mode.** `load(..., pit_mode="strict")` returns only
  vintages whose disclosure, publication/availability and lake observation are
  all provably on or before `as_of`, and excludes reconstructed backfill rows;
  `"best_effort"` keeps them and adds `pit_is_exact` / `pit_quality`. Four
  optional bitemporal columns (`available_at`, `source_published_at`,
  `observed_at`, `revision_id`) are filled on read, and
  `scripts/migrate_pit_vintages.py` writes them into old files in place.
- **Versioned universe profiles.** `cnequity.domain.universe_profiles` is a
  registry of reproducible research scopes (`cn_a_sh_sz_research_v1`,
  `cn_a_all_experimental_v1`, plus legacy records) with a stable `scope_hash`.
  `load(profile=...)` binds exchange/board, CDR/ETF, ST and delisting-evidence
  rules and enables the strict checks. `cne profile list/show`. See
  `docs/reference/universe-profiles.md`.
- **Committed dataset revisions and portable snapshots.** Compaction now
  commits a monotonic `revision` plus a content digest and per-file hashes
  when curated files change, exposed through `StateStore` and
  `cnequity.query.dataset_state` so a cache invalidates on a repair that never
  moves the watermark. `cne snapshot create/verify/restore` produces
  checksummed, immutable lake snapshots that also carry the contract
  fingerprint and run lineage.
- **Portable snapshot archives and incremental lake packages.** `cne snapshot
  export/import` streams a snapshot to one `tar.zst` (gzip fallback) and back,
  verifying the manifest before an atomic publish; every tar member is checked
  first, so absolute paths, `..`, duplicates, links and device nodes are
  rejected rather than extracted. `cne snapshot delta create/verify/apply`
  moves only what changed between two lake roots — a whole-lake snapshot is the
  wrong tool for a daily sync. A delta carries add/replace/delete preconditions
  (byte hashes for a two-root delta, the committed revision number for
  `--from-revision`), so applying one to the wrong baseline fails instead of
  silently corrupting the target. `apply` backs up every overwritten file until
  the full change set and the post-apply fingerprint both pass and rolls back on
  any error, and delta paths are confined to `curated/`, `derived/` and an
  allow-list under `meta/`.
- **Run-level dataset receipts.** A `dataset_results` table records one row
  per logical dataset and stage (fetch/stage/compact/derive/audit/
  publish_revision) with core/research/advisory criticality, with an additive
  migration for hand-copied manifests. `cne status --run <id|latest>` reports
  it.
- **Source-use policy register.** `sources/SOURCES.yml` records access type,
  terms-review status and a conservative use conclusion per source label; an
  unreviewed permission is the literal `unknown` and never satisfies an allow
  check. `cnequity.compliance.source_policy` and `cne sources policy` evaluate
  it and fail closed. See `docs/legal/source-matrix.md`.
- **Source SLO, resilience and stability gates.** `cne sources slo` turns
  stored probe history into per-source availability SLOs and de-duplicated
  incident payloads; `cne sources resilience` derives source concentration,
  failure domains and a fail-closed backup gate for the core datasets from the
  registry with no network calls; `cne verify --runs` checks consecutive clean
  trading days without filling gaps. Each supports `--enforce`.
- **Run provenance on receipts.** Revision and snapshot receipts carry
  non-secret code and config identity (package version, git commit, config
  fingerprint) via `cnequity.provenance`.
- **Supply-chain CI.** A new `security.yml` workflow runs `pip-audit` and
  emits a CycloneDX SBOM on dependency changes and weekly. `docs/development/
  release-governance.md` records the version and data-contract policy.

### Changed

- **`regulatory_events` is derived from `announcement_index` instead of fetched
  again.** CNINFO's endpoint has no server-side filter for it, so the dataset
  re-issued `announcement_index`'s exact request and kept the few rows whose
  title matched a keyword — 46 pages and 1,375 announcements for 6 events on
  2026-01-01, and ~220 pages twice on a dense day, against a source paced at
  one request per second. The two fetches also ran an hour apart, so the
  datasets could disagree about the same day; now one is a projection of the
  other. `cne backfill regulatory_events` reads the indexed announcements
  instead of sweeping CNINFO a second time, and derives only the part of a
  window they actually cover (see Fixed).
- **Scheduled ingestion separates the normal daily run from late stale-data
  recovery.** The daily launchd agent covers all six groups and no longer
  waits inline for delayed sources; a second stale-only agent runs later.
  Both wrappers share a non-blocking, PID-aware lock, and installation renders
  both plists atomically with XML-safe paths.

- **`trading_status` splits `status` from a new `risk_warning` column
  (ADR-0007).** `status` used to carry both the trading state and the ST / *ST
  designation, resolved by an `if/elif` in which halting won — so a halted ST
  name lost its designation in stored history. Seen live: 000711.SZ was `st` on
  2026-08-27 and `suspended` on 2026-08-28 without leaving risk warning, and
  `market_breadth` consequently priced that session against the ±10% band
  instead of ±5%. `status` is now the trading state alone
  (`normal`/`suspended`/`delisted`) and `risk_warning` is its own nullable
  boolean. Reads accept both encodings, so an existing lake stays correct;
  `scripts/migrate_trading_status_risk_warning.py` makes the physical schema
  uniform (dry-run by default).
  ST and *ST are still not distinguished — no source feeding this dataset ever
  did — and the finer designation remains in the exchange 简称.
- **`margin_trading` trails by about one session under the new default.** SZSE
  publishes a business day after SSE, and a day is written only once both have
  — a half-market day would advance the watermark and strand the other half.
- **`short_balance` is null on SH rows under the exchange source.** The SSE
  does not publish 融券余额 (its `rqylje` field is null for every row). It is
  reconstructible as 融券余量 × close, but stamping local arithmetic with
  `source="exchange"` would attribute it to the exchange. Select
  `[margin_trading] source = "eastmoney"` if the field is required.
- **Run status contract.** A step-level `warning` is now reported as
  `degraded` at run level. `cne status` and the run commands
  (`run daily`, `run --stale-only`, `init`, `retry`) exit `2` for a degraded
  run — the core spine completed, research/advisory work did not — and `1`
  only for a core failure; a bare `cne status` previously always exited `0`.
- **Research-source failures no longer fail the run.** An `adj_factors` or
  `industry_index` derive whose source is unavailable degrades a run and keeps
  the committed raw `daily_bars` revision, instead of marking the whole run
  failed.
- **`list_datasets()` gains columns** `pit_quality`, `pit_storage_columns`,
  `revision`, `revision_id`, `schema_version` and `contract_fingerprint`.
- **PyYAML is now a direct runtime dependency** (used to parse
  `sources/SOURCES.yml`) and ships as a wheel data file.
- **The CLI surface is 33 commands, down from 43.** An audit found 58% of it was
  reachable from neither the README nor any automation — one-off tooling that a
  published CLI turns into a permanent compatibility obligation. Nothing lost a
  capability:
    - `cne sources probe --only tdx_protocol` remains as a deprecated, hidden compatibility alias
      through the 0.8.x line. It keeps the v0.7.3 payload-probe semantics and
      warns that removal is planned for 0.9.0; use `cne sources probe --only
      tdx_protocol` as the replacement.
    - `cne stats refresh` → `cne stats rebuild --if-stale`. Its `--force` was
      already what plain `rebuild` does. `--if-stale` cannot be combined with
      `--dataset`: it decides on the whole lake's watermark.
    - `cne contract export` → `cne contract show --out PATH`. Same document,
      differing only in whether it was written to a file.
    - `cne catalog` → the no-stats fallback of `cne stats show`, with `--json`
      as its output. A lake that has never built its stats tables should not
      need a build step to answer what is in it.
    - `cne run catchup` → `scripts/run_catchup.py`; `cne repartition` →
      `scripts/repartition.py`; `cne delisted discover/reconcile/repair/coverage`
      → `scripts/delisted_ops.py`. Composition and one-off migrations, which is
      what `scripts/` is for. `cne delisted status` and `cne delisted backfill`
      stayed.
- **`cne sources` is now a group; the probe is `cne sources probe`.** This is
  the one breaking rename here: `cne sources` has shipped since the source
  health board landed, and scripts calling it need the extra word. The `slo`,
  `resilience` and `policy` subcommands join it under the same plural noun —
  they were added earlier in this same unreleased cycle as `cne source <sub>`
  and never shipped under the singular, so nothing that exists in a release is
  affected by that half.

  The reason: `source` and `sources` were two top-level entries one letter
  apart, and typing the wrong one runs a different command rather than erroring.
  `cne source --help` had to spend a sentence saying which was which, which is
  what a naming defect looks like. Renamed before 1.0 deliberately — afterwards
  it would need a deprecation cycle.
- **`cli/main.py` is split by what a command does.** One 2,828-line module
  became `setup_cmds`, `run_cmds`, `backfill_cmds`, `maintain_cmds`,
  `quality_cmds`, `govern_cmds`, `consume_cmds` and `delisted_cmds`, with the
  group in `_root` and shared pieces in `_shared`; `main` only imports them to
  register. `--config`, hand-written 34 times, is now one `config_option`
  decorator. Tests patch the module that binds the name — `main` deliberately
  no longer re-exports internals, so a stale patch target raises instead of
  silently patching a name nothing reads.
- **The vendored TDX tree is excluded file by file, not wholesale.** Ruff,
  `coverage` and Codecov skipped everything under `adapters/tdx_protocol/_wire`
  on the rationale that it is upstream code kept byte-close for re-syncing. Two
  of those files are ports that fix three tdxpy defects and return raw prices,
  and `_wire/__init__.py` (`TdxWireClient`, the page caps, the heartbeat choice)
  never existed upstream at all — and tdxpy, unmaintained since 2024, is why the
  tree is vendored, so there is nothing to re-sync from. Those three are now
  linted and measured like the rest of the package; the genuinely untouched
  files are still listed and still skipped.

### Deprecated

- **`cne sources probe --only tdx_protocol`** remains for one minor release as a compatibility
  spelling. It runs the same `tdx_protocol` payload probe as v0.7.3, emits both
  a `DeprecationWarning` and a CLI warning, and is planned for removal in
  0.9.0. Use `cne sources probe --only tdx_protocol` as the replacement.
- **`universe="all_a"` in the query layer.** It still resolves and keeps its
  permissive legacy semantics but emits a `DeprecationWarning`; choose an
  explicit profile such as `cn_a_sh_sz_research_v1`.
- **`load()` without an explicit `pit_mode` for PIT datasets.** The omitted
  case keeps the old `fetched_at` cutoff and makes no exactness guarantee.
  Research code should pass `pit_mode` and record the choice.

### Fixed

- **A closed day with no disclosures is no longer a failed fetch.**
  `announcement_index` walks calendar days, so its window contains days the
  exchanges never opened, and a market-wide zero-row Sunday (2026-08-02, for
  one) raised `no rows returned` — failing the step, the `capital` group, and
  every step that depended on it. A feed that is queried one calendar day at a
  time now tolerates an empty non-session day; on a day the market did hold,
  zero rows still fails loud, and a live page (`flash_news_wire`) keeps failing
  loud on any day, because an empty page says nothing about the calendar.
- **A run killed mid-DAG is no longer retried into a `success`.** The retry
  path only checked for never-started steps on `init` runs. A daily job killed
  by the OOM killer left no batch at all for the steps it never reached, so the
  ledger looked clean: `cne run retry` repaired what had failed and closed the run
  as `success` with the rest of the day silently missing. Every run now records
  the step list it was started with (`planned_steps`), the retry runs the
  planned steps that never started — skipping any whose input in that run is
  still failing — and refuses to mark a run `success` while a planned step has
  never run. Runs created before this release carry no plan and keep their old
  behaviour rather than having one inferred from today's config.
- **`scripts/delisted_ops.py repair` no longer treats `degraded` as success.**
  The merge listed only `failed` and `warning`, so a `degraded` step fell
  through onto the success branch — the same shape as the chunked backfill
  that reported success for a sweep whose every slice had failed. Anything
  other than success now survives the merge as a warning.

- **`scripts/health_notify.sh` honours `CNE_BIN` like the pipeline that runs
  it.** `daily_pipeline.sh` and `stale_pipeline.sh` both resolve `cne` through
  `CNE_BIN`, the documented override; the health gate they call hardcoded the
  repo's own `.venv`, so setting it left the alerting step probing a different
  — or entirely absent — binary while the groups used the real one.
  `china_egress_backfill.sh` now accepts `CNE_BIN` too, keeping its older
  `CNE` spelling working. The daily pipeline's gate-vs-soft exit logic — the
  code that decides whether a failure pages anyone — has tests for the first
  time, as does the `STALE` exit code its stale probe gates on.

- **Ordinary CLI mistakes stop printing Python tracebacks.** Three of the most
  common ones raised straight through Click: a typo in any date option
  (`date.fromisoformat`'s bare `ValueError`, from thirteen separate call
  sites), a mistyped dataset name for `cne backfill` (a `KeyError` from the
  registry), and a SQL error in `cne query --sql`. They are now the one-line
  errors Click reports for every other bad input — the date says which option
  and what shape it wants, the dataset name suggests its near misses, and
  DuckDB's own message, which already names the line, column and near miss, is
  kept intact.

- **Naming a snapshot that does not exist is an error, not a traceback.**
  `cne snapshot verify|restore|create` and `snapshot delta verify|apply|create`
  printed a Python stack trace when the snapshot was missing or unreadable —
  `export` and `import` already treated that as operator input and said so in
  one line. They all do now, and the message names the snapshot the operator
  typed instead of a manifest path they never wrote.

- **`cne status --datasets` is readable again.** The flag is the freshness
  probe the runbooks reach for, but `list_datasets` has grown to twenty
  columns — contract fingerprints, revision ids, PIT storage lists — and the
  table forced all of them into the terminal, shredding every value into
  vertical slivers a character or two wide. It now prints what the flag
  promises: dataset, layer, freshness, coverage range and watermark. The full
  inventory moves to `--datasets --all-columns`; the STALE exit code that
  scripts gate on is unchanged.

- **A chunked `cne backfill` no longer reports success for a sweep that failed
  outright.** `aggregate_run_status` deliberately calls a non-core step's
  failure a *degraded* run rather than a failed one — in the daily job the
  other datasets still landed and the lake stays usable — but the date- and
  symbol-chunked sweeps only ever propagated `failed` and `warning`, so a
  `degraded` slice left the aggregate at `success`. 35 of the 46 registered
  steps are non-core, `announcement_index`, `financial_statement_items`,
  `minute_bars` and `valuation_metrics` among them, so a sweep in which every
  slice raised still printed `"status": "success"` and exited 0 — the
  scheduled-pipeline failure mode where nothing was written and nothing said
  so. A slice whose own receipt failed now fails the sweep and stops it with a
  `resume_from`; a slice that was merely degraded keeps the sweep going but no
  longer lets it claim success. The unchunked path already behaved this way.

- **`cne backfill regulatory_events` no longer dies on its first slice.** The
  CLI defaults that sweep to a 2010 floor and walks 31-day slices, stopping at
  the first failure; the derive refuses a window with no announcements in it,
  and its coverage guard clamped only the tail. So on any lake whose
  announcements begin later — this project's own init starts at 2016 — slice
  one asked for 2010, found nothing, and killed the sweep with advice to go
  fetch disclosures that history never had. The window is now clamped at both
  ends to what `announcement_index` actually indexed, and a slice outside that
  range is reported as `before_source_coverage` rather than failed. A hole
  *inside* the indexed range is still refused, and an incremental run's
  reconciliation tail reaching past the first indexed day stays quiet — it is a
  fixed lookback, not a coverage claim, and reporting it would leave a lake
  younger than the tail permanently degraded.

- **One unreadable day no longer blinds a whole reconciliation window.**
  `announcement_index` re-reads the last 30 days on every run, and a single day
  the source refused failed the step — discarding every other day in that
  window — until the bad day rolled out of the tail. Days are now fetched
  independently: the complete ones are staged, the failures are recorded as a
  `fetch_failed_days` audit finding, and the step reports `degraded`. A window
  where nothing was readable still fails loud, and a dataset with no
  reconciliation tail keeps the all-or-nothing contract, because nothing would
  come back for its hole.
- **A dense disclosure day is readable again, and every day is now read to the
  end.** `hisAnnouncement/query` returns at most 100 pages (3,000 rows) for one
  query: `pageNum>100` silently replays page 1 while `totalpages` keeps
  reporting the real count (measured 2026-08-28: 6,537 rows, `totalpages=217`,
  page 101 == page 1). Two further contract facts came out of the same live
  probe: `totalpages` is `floor(records / 30)`, so it omits the final partial
  page (2026-01-01: 1,375 rows, `totalpages=45`, page 46 held the last 25), and
  `column` is not an exchange selector at all — `szse`, `sse` and `bj` return
  the identical all-market result.
  - The walk now follows the record total rather than the page count, so the
    tail of an ordinary day is no longer dropped, and `totalpages=0` reads as
    "fewer than one page" instead of a contradiction.
  - A day over the cap is split by a filter that actually partitions it —
    `plate` market, then its boards, then CSRC industry, then disclosure
    category — and each split is accepted only when the deduplicated buckets
    reconcile exactly to the source's own `totalRecordNum`. A cover that cannot
    prove coverage is skipped for the next one; when none can, the day fails
    loudly rather than publishing the part that was reachable. Verified live:
    2026-08-28 reads 6,537/6,537 rows through two splits.
  - One walk per day instead of one per exchange column, halving the request
    count for every announcement and regulatory fetch.
- **Derived suspensions reach committed readers.** `derive_suspension_history`
  wrote `is_trading=false` rows straight into the mutable curated directory, so
  no committed reader ever saw them and the next compact rebuilt the partition
  from the committed generation without them — which is why a `daily_bars`
  interior gap could not be excused by the suspension that explained it. The
  rows now go through `staging → compact → commit` like every other write, as a
  registered `trading_status_derive` step in the daily job (90-day tail), a new
  `phase5_derive_and_publish` init phase (whole history, after the bars are
  committed), and behind `cne derive trading_status`.
- **A restated vendor snapshot no longer erases better evidence.**
  `trading_status` rows are now chosen by evidence class before recency, shared
  by the polars and DuckDB read paths: an exchange record or a board read after
  that session closed outranks a derived bar-gap suspension, which outranks a
  current-state board stamped onto a session it never observed.
- **CNINFO announcement and regulatory backfills are bounded, resumable and
  fail closed.** Historical requests run in 31-day slices with range-scoped
  checkpoints and unique staging identities. Pagination contract failures,
  expired checkpoints, missing dates, incomplete exchange columns and partial
  split trees can no longer publish truncated history. Exact wire archives now
  retain a top-level invocation identity, and offline replay applies the same
  page, date, total-count and retry-attempt rules as the live fetch without
  mixing observations across runs, retries or date windows.
- **Sparse event days no longer block compaction.** A successful bounded
  CNINFO response is valid negative evidence for calendar days with no
  announcement or regulatory event; those days are distinguished from source
  failures instead of turning an otherwise complete backfill into a warning.
- **Retry and receipt telemetry is truthful across adapters and orchestration.**
  Worker/non-worker requeues and source-request retries have separate durable
  counters, multi-slice failures retain the cumulative request total, legacy
  manifests migrate additively, and a successful self-recording retry cannot
  inherit the failed receipt from its previous attempt.

- **Python 3.10 could not import the CNINFO adapter.** `datetime.UTC` is 3.11+;
  announcements and its tests now use `timezone.utc`, as the rest of the tree
  already does. That import is on the package path, so 3.10 CI never reached
  collection.
- **Python 3.10 could not collect `test_contracts`.** The module imported
  stdlib `tomllib` at the top; that module is 3.11+. The config loader and
  the other TOML tests already fall back to `tomli`.
- **Windows refused the raw-archive symlink-boundary fixture.** POSIX
  `rename()` replaces an empty destination directory; Windows raises
  `FileExistsError`. The test now moves the dataset directory onto a path that
  does not already exist.
- **Windows CI flaked the cross-process rate-limiter assertion.** `time.sleep`
  returned at 48% of a 50ms interval; the floor is now 30% of 100ms — still
  several times a no-op wait. The source-concurrency hold uses the same
  30% floor: Windows CI measured 0.0527 against an 0.064 threshold.
- **Windows CI executed the Unix scheduler wrappers as PE binaries.**
  `test_scheduler_scripts` drove `.sh` entry points with `subprocess`
  (`WinError 193`). Those invocations now skip on Windows, matching
  `backup_meta.sh`. The launchd template assertions still run.
- **Windows CI aborted the unit suite with `KeyboardInterrupt` after the
  process-pool rate-limiter tests.** A worker exiting on Windows can inject
  `CTRL_C_EVENT` into the parent's console group (CPython 33725); the session
  now ignores that signal. A leftover worker atexit can still flip a fully
  green run to exit code 1 — CI now reaps those processes and keeps pytest's
  status.

- **A frame with no columns gained a row when it was stamped.** `pl.DataFrame()`
  is the codebase's "nothing to report" value, and polars broadcasts a literal
  against a zero-*column* frame to length one — so stamping provenance onto it
  fabricated a row carrying only the literals. That row has no primary key, so
  a strict validate rejects it, but a `pl.concat(..., how="diagonal_*")` with a
  real day's rows runs first in several paths, fills its keys with nulls, and
  launders it into the lake. Hit live in `step_trading_status`, where a session
  with an empty vendor universe produced a row with no symbol and no
  `trade_date` that then failed the *next* session's date check and blamed the
  wrong day. `with_provenance` (28 call sites) and `normalize_pit_storage_columns`
  now no-op on such a frame, `cnequity.domain.frames.with_columns_unless_blank`
  covers the remaining literal stamps, and
  `scripts/probe_blank_frame_broadcast.py` is a pytest plugin that sweeps the
  suite for new occurrences — currently zero outside polars itself.
- **Delisted securities were published as normally trading, every session.**
  The daily writer classified anything neither halted nor on the risk board as
  `normal` with `is_trading=True`, and had no notion of delisting. Measured on
  a full lake for 2026-08-28, that was **611 symbols carrying a `delist_date`**
  — the oldest delisted 1999-07-12 — none of which had a bar that day. A vendor
  board cannot report on a security that has left the market, so these rows now
  come from `instruments` as `status=delisted`, `is_trading=False`,
  `source=derived_delisted`, with `risk_warning` read from the final 简称; the
  symbols are dropped from the board request entirely. The migration does not
  back-fill history — re-run the daily step over a window to correct it.
- **`cne snapshot`'s three subcommands had no help text at all.** `create`,
  `verify` and `restore` listed as bare names with no description and no option
  help — the only commands in the CLI with none.
- **`cne verify --runs` and `cne sources policy` had no CLI test.** Both are
  fail-closed gates whose exit code is the entire point, and `stability` runs in
  `scripts/daily_pipeline.sh` every day. A wrapper that stopped raising would
  have reported the failure and still exited 0.
- **The two TDX transaction parsers had no test.** `trade_ticks` is a
  single-source dataset whose prices are delta-encoded per page, so one
  mis-read field corrupts every later row — and the only verification was a
  one-off byte comparison against a live server that CI cannot repeat.
  `tests/unit/test_tdx_tick_parsers.py` now pins the wire layout both parsers
  assume: field order, the four filler bytes only the historical response
  carries, the absent per-record trade count, integer quantities, and undivided
  prices.
- **The vendored tree described itself as keeping "the five calls this project
  makes"** after the two transaction commands brought it to seven.
  `test_tdx_decoupling` now pins the kept set, so the count is a contract rather
  than a comment.
- **ETF/LOF adjustment factors and deep history are now complete.** Sina's
  fund payloads use `s` (while `f` is only a placeholder); the adapter now maps
  fund hfq directly from `s` and derives qfq as `1/s`. ETF/LOF symbols are
  included in factor self-healing and coverage audits, EastMoney supplies their
  listing dates, and the THS pre-2016 raw-history plan includes them while
  continuing to exclude undated subscription placeholders.
- **TDX instrument discovery excludes unlisted exchange placeholders.** SH/SZ
  security lists can advertise IPO and fund-application codes with a positive
  sub-tick `pre_close` sentinel. Those rows are now excluded before they enter
  the instrument universe and generate guaranteed-empty daily-bar batches.
- **Transient worker failures consume a durable, bounded retry budget.** Batch
  restarts no longer reset `retry_count`; one retry/resume invocation repeats
  network-failed worker batches with configured pacing until they recover or
  reach `max_retries`, and reports exhausted batches explicitly.
- **Retry dispatch follows logical task identity.** Steps that write into a
  different physical dataset, including historical bar backfills, resume by
  `task_id` while retaining a compatibility fallback for older auxiliary
  receipts.

## [0.7.3] — 2026-08-23

### Added

- **Research-universe scoping for quality and health.** Historical-validity
  checks accept an explicit research universe, and the health entrypoints and
  `serve` API report readiness against that scope instead of the whole lake, so
  a source that never covered a board no longer reads as a hole in the data.
- **Explicit `all_a_sh_sz` universe in the query layer.** Callers can name the
  Shanghai/Shenzhen universe directly rather than approximating it with a
  filter. See Changed for the rejection behavior that comes with it.
- **Scoped Baostock corporate-action repair.** A bounded repair for corporate
  actions, including historical Beijing events, replaces what previously needed
  a full-history refetch. The adjustment-factor reconciliation finding also
  carries a sample of the delisted SH/SZ symbols Baostock can actually repair
  (`baostock_repairable_sample`), so the audit output names the candidates
  rather than only counting them.
- **Optional Beijing ST evidence back to 2016.** Baostock's ST history covers
  SH/SZ only. With an explicitly configured Tushare Pro token, BJ evidence is
  now collected from `stock_st` (2017-01-01 onward) and `bak_basic` name history
  (2016). Pre-2016 BJ stays blocked as a declared source limitation rather than
  being read as clean, and an unconfigured Tushare still blocks BJ explicitly.
- **`cne query --refresh` for on-demand datasets.** Forces a refetch and
  overwrites the matching cache variant; on-demand cache entries are keyed by
  request parameters so different dates, row counts, and sentiment models no
  longer reuse each other's results.
- **Beijing tip turnover supplementation.** Beijing daily bars fill turnover at
  the tip from a secondary snapshot instead of landing incomplete.

### Fixed

- **`cne run retry` resumes a run on its own trade_date, not today's.** The CLI
  never passed a `trade_date`, so a retry fell through to today by default.
  Retrying a run after its session rolled over — the ordinary case — replayed
  every failed batch against today's date instead of the run's, so a backfill
  for a past date silently asked sources for data dated today and the retry
  always "succeeded" without ever closing the original gap. The run's real
  trade_date was already recorded and already read back for other purposes;
  now it's used for this too. `resume_init` shares the same fix.
- **`cne config validate` rejects malformed `[[failover.datasets]]` entries.**
  An unregistered dataset name, a `primary`/`backup` that names no real
  source, or a duplicate entry for the same dataset all passed silently
  before — failover for that dataset just never did anything, discovered
  during an actual outage instead of at config time.
- **The Windows deadline fallback for Baostock queries is now a real timeout.**
  Without `SIGALRM` (every query on Windows, regardless of thread), the
  watchdog only fired a socket-close side effect and hoped it would interrupt
  the blocked call — a query blocked on anything else, or where the close
  didn't propagate, could run past its declared deadline indefinitely. The
  fetch now runs in a daemon worker thread and the caller's own wait is what's
  bounded, so the deadline holds regardless of what the query is doing.
- **EastMoney list requests no longer lose their query string on httpx 0.28+.**
  httpx 0.27 and earlier merged a `params` dict into a URL's existing query;
  0.28 replaces the query instead. The push2 `ut` token was injected as a
  one-key `params` dict, so on 0.28 every clist request collapsed to
  `?ut=...` — dropping `fs`, `fields`, `pn`, `pz` and `fid`, and silently
  under-fetching fund flow, the ST board, instruments, rotation and valuation.
  Nothing caps httpx above `>=0.25`, so this affected any fresh install even
  though the pinned dev lockfile (0.25.2) and the offline test suite both hid
  it. The token is now merged into the URL, which behaves identically on every
  supported httpx version, and the suite is green on 0.25.2 and 0.28.1 alike.
- **One canonical source precedence across the whole lake.** Storage, query,
  views, calendar, sector mapping, adjustment factors, intraday and tick checks,
  and every quality consumer now resolve multi-source rows the same way:
  timestamped observations win, ties break deterministically, and legacy rows
  without timestamps rank consistently. Previously each consumer could pick a
  different row for the same key.
- **Daily-bar failover is scoped, resumable, and lossless.** Retries narrow to
  the batches that actually failed, verified batches are reused instead of
  refetched, valid rows survive a partially failed batch, edge gaps in a
  requested window are detected, and a source outage degrades that source alone
  rather than the whole run. Large partial tip snapshots are rejected outright.
- **Hung upstream requests are interrupted instead of blocking a run.** TDX bar
  requests and native socket reads are bounded, stalled Baostock queries are
  interrupted, and long trading-status backfills heartbeat so a stall is visible
  rather than indistinguishable from slow progress.
- **Adapters fail closed on malformed payloads.** TDX (unknown price
  coefficients, malformed quantity and action amounts), EastMoney (action
  values), Sina (all-malformed klines), CNI (unsupported workbooks), BSE
  (truncated quote pagination), Tushare (ST evidence rows), and the news index
  (unstable article identities) reject bad rows instead of writing them through.
  THS transfer-increase plans are parsed rather than dropped.
- **Expected gaps are distinguished from real ones.** Retired datasets, on-demand
  backup gaps, known index-source limits, Sina turnover gaps, and unsupported
  exchanges are classified as informational and carry their reason into the
  audit artifact and the health API, so a blocked research question is
  distinguishable from a healthy lake with a known source limitation.
- **ST evidence receipts are drift-checked and composable.** Overlapping
  coverage receipts merge, a later run over the same universe extends an earlier
  one, duplicate symbols are rejected, in-progress checkpoints are visible, and
  receipts that no longer match the data they claim are detected. Strict
  universes and the MCP surface gate on real all-A evidence.
- **Placeholder and stub rows stay out of the lake.** Padded subscription
  placeholders are stripped on ingest and purged during compaction, subscription
  stubs are excluded from symbol universes, and delisted placeholder tails are
  counted canonically.
- **Historical calendar and closure modeling.** Weekend rows in historical index
  data are sanitized, the calendar keeps weekdays only, verified historical
  market holidays are excluded, and early market closures are modeled from
  evidence rather than inferred.
- **Corporate-action history is complete at its boundaries.** Source precedence
  is aligned, the history floor matches what sources actually publish, pre-2016
  backfill rows and the earliest snapshot coverage are preserved, and optional
  snapshots failing does not fail the backfill.
- **Adjustment factors report incomplete coverage.** Partial factor spans are
  detected, uncached Beijing factors are fetched rather than assumed present,
  and factors unavailable for delisted symbols are classified instead of
  silently missing.
- **PIT fundamentals are bounded by collection date**, so a backfill cannot
  surface a report earlier than the lake could have known it.
- **Backfill recovery reflects what landed.** Orphaned staging is recovered,
  valuation backfill windows are honored, empty CNI backfills degrade safely,
  derived coverage repairs route to the right dataset, disabled intraday
  captures are not audited as gaps, and delisted symbols are kept out of both
  intraday sweeps and the active universe.

### Changed

- **Strict universe queries now raise where they previously returned rows or an
  empty frame.** This is the one caller-visible break in this release. An
  unsupported universe name raises `ValueError` instead of passing the frame
  through unfiltered (supported names are `all_a` and `all_a_sh_sz`). Under
  `strict`, a `daily_bars` scope that returns no rows, missing curated
  instruments, and missing ST evidence each raise `UniverseCoverageError`
  instead of yielding a silently unproven population, and the MCP surface
  enforces the same all-A evidence requirement. Code that relied on a strict
  query degrading quietly must now catch `UniverseCoverageError` or supply the
  coverage. This is deliberate: a strict universe that cannot prove its coverage
  was returning survivorship-biased results.
- **Quality and query scans are bounded or streamed rather than full-lake.**
  Cross-dataset scans, audit aggregates, and valuation coverage stream or
  combine their passes; ST universe scans, adjustment audits, daily coverage
  boundaries, current-day status reads, and Baostock repair windows are pruned
  to the window that matters; the factor audit drops a full anti-join; ST
  receipt revalidation is cached; and duplicate intraday scans and duplicate
  daily-bar failover requests are eliminated. Bounded Sina fallback fetches run
  in parallel. Note that the bounding is not purely a speed change: a scan that
  now covers a narrower window can legitimately produce a different finding set
  for the same lake, so audit output may shift alongside the runtime.

### Docs

- Documented scoped research validity, corporate-action repair scope, explicit
  universe semantics for MCP, retired-source semantics, the index-history source
  limitation, and ST coverage with Baostock pacing. Source coverage counts no
  longer hard-code numbers that go stale.

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

[Unreleased]: https://github.com/rootSunc/CNEquity/compare/v0.8.1...main
[0.8.1]: https://github.com/rootSunc/CNEquity/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/rootSunc/CNEquity/compare/v0.7.3...v0.8.0
[0.7.3]: https://github.com/rootSunc/CNEquity/releases/tag/v0.7.3
[0.7.2]: https://github.com/rootSunc/CNEquity/releases/tag/v0.7.2
[0.7.1]: https://github.com/rootSunc/CNEquity/releases/tag/v0.7.1
[0.7.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.7.0
[0.6.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.6.0
[0.5.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.5.0
[0.4.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.4.0
[0.3.1]: https://github.com/rootSunc/CNEquity/releases/tag/v0.3.1
[0.3.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.3.0
[0.2.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.2.0
[0.1.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.1.0
