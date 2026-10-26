# Sources and history limits

Primary source, update frequency and known limits for each dataset.

### Legend

- **Wave:** the step name in the daily batch
- **On-demand:** fetched by `OnDemandService` on first query

### MVP-P0

#### instruments

| Item | Value |
|------|-------|
| Wave | `instruments` (Wave 0) |
| Primary | tdx_protocol (built-in security_list) |
| Fallback | baostock (`--backfill` only, to add delisted securities) |
| Supplementary | bse (BSE quote board, the only source for the listed BJ roster and short names); sina (the live-but-missing bucket from the code-space scan) |
| Frequency | Daily; historical backfill starts from 2001-01-01 by default, `--start` narrows the window |
| Primary key | symbol |
| Universe | The research universe uses the SH/SZ/BJ prefix allowlist 60/68/00/30/92; ETF/LOF stay in `instruments` as a separate category, and the scope of fund daily bars is set by the ingestion config and the formal eligibility catalog |
| Known limits | `delist_date` is inferred when a security disappears from the snapshot; EastMoney fills `list_date` from the A-share and ETF/LOF clists separately, and a published exchange ETF catalog can fill in missing fund listing dates. When push2 is unavailable, newly listed stocks may temporarily lack a listing date, and the daily bars step probes securities without a listing date one by one; ETF/LOF do not enter the `all_a` stock research universe |
| BSE | The TDX security list only covers Shanghai and Shenzhen; reusing the old code-space scan alone misses BJ securities listed later. The BSE quote board is now read once a day to complete the roster and short names |
| Order | Both supplementary sources are merged **before** `list_date` enrichment. The other way round, the rows they bring in would always have an empty `list_date`, and "no `list_date` and never had a bar" is exactly the test for a not-yet-listed placeholder — discovered but never fetched |

#### etf_profiles

| Item | Value |
|------|-------|
| Group | research@18:15 |
| Primary | SSE ETF catalog; SZSE ETF catalog and fund catalog; official index methodology documents verified code by code |
| Frequency | Current snapshot daily; history not yet observed cannot be backfilled |
| Primary key | (symbol, as_of_date) |
| Classification threshold | SSE: marked `eligible` only with an explicit domestic equity index category and a tracked index. SZSE: requires both an equity fund category and an official methodology document matched exactly by tracked index code. The methodologies of 399006 and 399330 directly prove A-share scope; 399673 must prove its constituent scope together with the upstream 399006 methodology. No extrapolation to other indices |
| Completeness | The SSE total must equal the actual row count; the ETF code sets in the two SZSE catalogs must match; the raw responses of all three catalogs and of the official methodologies used are archived first, and only then may the data be published; when a methodology changes or cannot be fetched, the old snapshot is kept and the failure is reported |

#### trading_calendar

| Item | Value |
|------|-------|
| Wave | `trading_calendar` (Wave 0) |
| Primary | tdx_protocol |
| Fallback | Exchange CSV |
| Frequency | Yearly refresh + daily check |
| Primary key | trade_date |

#### trading_status

Historical status coverage depends on first-hand evidence retained by this lake; a current snapshot cannot establish earlier daily history. The Baostock history path covers SH/SZ stocks, without complete historical status for ETFs or BSE stocks. Preview print-derived missing rows with `cne repair status-traded --start YYYY-MM-DD --end YYYY-MM-DD`; ST remains unknown. See the [status contract](schema.md#trading_status).

Old staging status merges also recheck listing windows: normal status after the catalogue's delisting date cannot be reintroduced. Traded quotes contradicting that date preserve the records for verification.

| Item | Value |
|------|-------|
| Wave | `trading_status` (core; depends on `instruments` and runs after it) |
| Primary | eastmoney (ST board + suspension list) |
| Fallback | exchange (SSE and SZSE quote boards, an independent failure domain) |
| Supplementary | derived (`derived_bar_gap` / `derived_delisted` / `derived_traded`); bse (BSE quote board) |
| Frequency | Daily |
| Primary key | (symbol, trade_date) |
| Historical ST backfill | Baostock covers SH/SZ; optional Tushare Pro `bak_basic` (2016) + `stock_st` (from 2017-01-01) covers BJ and needs a token; without source coverage the audit stays at warning |
| Historical command boundary | `cne backfill trading_status --symbols ...` rejects the Baostock historical ST path for BJ codes; for BJ use an independent source with real coverage credentials, and do not interpret an empty result as normal |
| Column semantics | `status` only represents trading status (`normal`/`suspended`/`delisted`); ST/*ST is in a separate column, `risk_warning`. The two are orthogonal: when an ST stock is suspended, both fields hold at once |
| Delisted securities | The quote board is not asked (it cannot answer); the decision comes from `instruments.delist_date`, writing `status=delisted`, `is_trading=false`, `source=derived_delisted`, with `risk_warning` taken from the final short name; **without a short name it is null, not false** — the short name is the only ST evidence left after delisting, and its absence is an absence of evidence |
| BSE | EastMoney's ST board and suspension list do not cover BJ; BJ same-day status comes from the official BSE quote board. Fields the source does not cover stay null; normal cannot be inferred from absence from a list. |
| BJ historical ST | Announcement keywords, undated former names and the current ST sector cannot prove normal status for each historical day. The project has no free adapter that fully backs BJ historical ST status; optional Tushare adds evidence within its actual coverage. Without credentials, narrow the research window to what the evidence in this lake supports; do not treat zero hits as normal, and do not claim that every future free source is unavailable. |
| Not-yet-listed codes | Codes with no listing date that have never had a bar need same-day exchange quote board evidence before a status is written; new securities of the day are passed through the run context. A failed request is not evidence of not-yet-listed/normal. |
| First listing day | The instruments step writes new securities into the run context; the trading status and daily bars steps depend on it and fetch new stocks without waiting for the whole group to compact. |
| Known limits | ST and *ST are not distinguished (none of the sources feeding this dataset make that distinction: Baostock only has an `isST` boolean, and the Tushare adapter already normalizes `ST`/`*ST`). The finer marker is in the exchange short name, available via `instruments.name` |

#### daily_bars

Fetch planning, batch gap checks and coverage reports use the same suspension evidence: status inferred from bar gaps, carried cached status and current boards restated onto past dates cannot settle bar debts. Conflicting first-hand suspension and trading observations still require a quote check.

Sources counting lots can round a fund trade smaller than one lot down to zero. When a TDX or EastMoney daily bar reports zero volume with positive turnover, that key is withheld from zero-trade publication and checked against another source; shares are never inferred from turnover.

| Item | Value |
|------|-------|
| Wave | `daily_bars` (Wave 1, depends on corporate_actions) |
| Primary | tdx_protocol (unadjusted; main path for Shanghai and Shenzhen, BJ history through a dedicated market id 2 path) |
| Fallback / routing | Currently available exchange board snapshots and TDX batch quotes; gaps go to eastmoney clist/kline. For BJ, the current period prefers BSE and history prefers TDX, with Sina filling the remaining range; explicit small-range requests skip unrelated full-board scans |
| Frequency | Daily incremental; full backfill at init; deep history is backfilled from THS by listing year (supported for both stocks and ETF/LOF) |
| Primary key | (symbol, trade_date) |
| ETF/LOF scope | By default `all_a` fetches stocks. Enabling `[universe].ingest_listed_funds` adds listed ETF/LOF from the security master independently of research eligibility. Unknown listing dates are still requested and reported separately; unreadable catalogs fail explicitly, and empty source responses remain gaps. Legacy `ingest_eligible_etfs` adds only replayable, `eligible` ETFs from a catalog within 14 days; failed, missing or stale catalogs raise errors. Explicit `--symbols` repairs follow their stated scope. Prelisting debts proved by formal listing dates are retired only after per-key evidence is saved. |
| Refetch | Securities with an ex-date in that day's `corporate_actions` |
| Known limits | TDX rate limits; keep workers ≤ 8; clist only has the same-day snapshot and must be stamped with the run's `trade_date`; ETF/LOF without a listing date are treated as not-yet-listed placeholders and their deep history is not fetched blindly. Codes that newly appear in the instruments table that day are merged into that day's request scope through the run context, so data is fetched on the first listing day without waiting for the next compact. **Stocks** with no listing date that have never had a bar are probed once per window: if a bar comes back, the stock is listed and enters the lake as usual; if the source cleanly returns empty, it is recorded as "not yet listed" (`not_yet_listed` negative evidence, covering only this window and probed again the next day) and does not count as an unresolved gap; failed requests reach no conclusion and are still treated as unresolved gaps. |

#### index_bars

| Item | Value |
|------|-------|
| Wave | `index_bars` (Wave 2) |
| Primary | tdx_protocol |
| Fallback | eastmoney |
| Frequency | Daily |
| Primary key | (symbol, trade_date, frequency) |
| **Known limits** | The deep history of `399001.SZ` has an 18-trading-day hole (1991–1995). The THS history API and the raw TDX API were each checked, and neither returns these dates; this is a gap shared by the sources' historical series, so no bars are fabricated and the dates are not written into `CLOSED_DATES`. `cne audit` keeps an `info` finding; a new gap outside the verified set is still reported as `warning` |

#### trade_ticks

| Item | Value |
|------|-------|
| Group | `ticks` (not on any default schedule; `cne run daily --group ticks`) |
| Primary | tdx_protocol (tick command `0x0fb5`) |
| Fallback | **None, on purpose** (see below) |
| Frequency | On demand / manual |
| Primary key | (symbol, trade_date, tick_seq) |

**Why there is no fallback source.** A fallback is worth having when it can still deliver the same data after the primary fails, and trade ticks have no such substitute:

| Candidate | History depth | Verdict |
|------|---------|------|
| TDX historical ticks | **Back to 2024-01-02** | The only free source with historical depth → primary |
| Tencent (`stock_zh_a_tick_tx_js`) | Last trading day only | Cannot fill history |
| EastMoney (`stock_intraday_em`) | Last trading day only | Same as above |
| Sina (`cn_bill.php`) | Recent, and **only large orders of ≥400 lots** | Incomplete |
| Exchange Level-2 | Complete tick-by-tick | **Requires a paid license; explicitly a non-goal** |

Writing a fallback that can only fill one day merely creates the illusion of "having a fallback" — in the scenario that actually needs a fallback (backfilling history) it cannot fill a single day.
So `failover` registers no fallback for `trade_ticks`: **a single source is the contract**, and when TDX is unreachable this dataset simply cannot be fetched.

#### commodity_bars

| Item | Value |
|------|-------|
| Group | `macro_risk` (daily) |
| Primary | **sina** (15 domestic main continuous contracts + a narrow overseas set, COMEX gold `GC0.CMX`) |
| Fallback | eastmoney push2his — **no automatic fallback**, must be enabled explicitly; this source may reject requests, so check the shared cooldown/budget first and accept within a limited scope; push2his traffic is not increased automatically because the primary failed |
| Coverage | Each contract goes back to its own listing date: CU0/AL0 2005, TA0 2006, ZN0 2007, AU0 2008, RB0 2009, J0 2011, AG0 2012, I0/JM0 2013, HC0/MA0 2014, NI0 2015, SC0 2018, LC0 2023 |
| Backfill | `cne backfill commodity_bars` (from 2020-01-01 by default; `--start`/`--end` available) |
| Primary key | (symbol, trade_date) |
| Known limits | The main continuous contract is not a real delivery month; night sessions belong to the settlement day; the watermark approximates the SSE calendar; this Sina endpoint provides no turnover, so `amount` is empty (after stitching the main continuous contract, price×volume is not the day's turnover, so it is not derived); London gold and similar are not included |

#### macro_indicators

| Item | Value |
|------|-------|
| Group | `macro_risk` (daily) |
| Primary | eastmoney datacenter: `cnbond_yield_10y` (`RPTA_WEB_TREASURYYIELD`.`EMM00166466`), `shibor_3m` (`RPT_IMP_INTRESTRATEN`, `INDICATOR_ID=203`), `lpr_1y` (`RPTA_WEB_RATE`.`LPR1Y`), `pmi_manufacturing`, `m2_yoy` |
| Supplementary | pboc: `social_financing` (increment in aggregate financing to the real economy) |
| Primary key | (indicator_id, obs_date) |
| Daily update | The three rates are requested day by day (`='{date}'` filter), only for trading days after the watermark; the two daily rates additionally look back over the last 5 trading days (one range query). When the value for the run day has not been published yet, only a `daily_series_pending` notice is recorded and the write proceeds as usual, with the next day's lookback filling it in; only values still missing once they are no longer the run day become a `daily_series_gap` warning, which blocks the merge and is retried. PMI, M2 and aggregate financing read the whole history every time |
| Backfill | `cne backfill macro_indicators --start 2016-01-01`: the two daily rates each issue one paginated range query per window (`(COL>='…')(COL<='…')`) instead of daily requests; the usual same-day daily fetch is also done. Default start 2016-01-01; `--end` does not go past today |
| History depth | The 10Y treasury yield and Shibor start at different dates; missing trading days are recorded as `daily_series_gap`; the actual coverage report is authoritative. |
| Trading days only | The interbank market also opens on make-up working weekends, and range queries may bring those days back; backfill filters by the project's trading calendar to match the row density of the daily update |
| Known limits | `lpr_1y` does not use range backfill: from 2013-10-25 to 2019-08 the `LPR1Y` column is the old daily loan prime rate, and only after that is it the reformed monthly LPR, so a range query would record two benchmarks under the same indicator. It is still only written on the day of the daily update |

#### futures_bars / option_bars / futures_contracts / option_contracts

| Item | Value |
|------|-------|
| Group | `derivatives` (daily, 18:30; fetched only with `[futures] enabled = true`) |
| Primary | **futures_exchange**: per-contract daily quote files from the exchanges' official websites. SHFE `data/tradedata/{future,option}/dailydata/kx{date}.dat` (JSON, **also containing INE products**) and `busiparamdata/*/ContractBaseInfo{date}.dat`; CZCE before 2015-10 `cn/exchange/{year}/datadaily/{date}.txt` (comma-separated, no header), afterwards `DFSStaticFiles/{Future,Option}/…DataDaily.txt` (pipe-separated, header columns renamed, encoding GBK→UTF-8) and reference XML; GFEX `interfacesWebTiDayQuotes/loadList` (POST) and the contract information API; CFFEX `sj/hqsj/rtj/{month}/{day}/{date}_1.csv` and `sj/jycs/{month}/{day}/index.xml` |
| Coverage | Futures: SHFE from 2002-01-07 (earlier not tested), CZCE from 2010-01-04 (earlier HTML archives are blocked by a JS challenge), CFFEX from 2010-04-16, GFEX from 2022-12-22. Options: CZCE from 2017-04-19, SHFE 2018-09-21, CFFEX 2019-12-23, GFEX 2022-12-23 |
| Self-consistency check | For each product (for CZCE options, each expiry series) the "subtotal" must equal the sum over contracts: volume and open interest exactly; CZCE and GFEX turnover is rounded per row to 0.01 (in units of 10,000 yuan), so each row may differ by half a rounding step (0.005, i.e. 50 yuan). A file that does not reconcile is rejected as a whole |
| No-data signals | Market holidays: CFFEX 302 redirect; SHFE 404 HTML; CZCE 404 "当日无数据" (no data for the day); GFEX 200 but with only one all-zero "总计" (total) row. All DCE endpoints return a 412 JS challenge, classified as blocked rather than no data, and this project does not bypass it. Network-layer errors such as connection resets are retried once (2-second interval); timeouts are not retried |
| Partial failure | When one exchange has not published, the other exchanges are still written; the missing one is recorded as `futures_exchange_missing` and filled in by a 3-trading-day lookback |
| Backfill | `cne backfill futures_bars --start 2010-04-16`, `cne backfill option_bars --start 2019-12-23`, then `cne backfill futures_contracts` / `option_contracts` rebuild the contract tables from the bars |
| Primary key | bars: (symbol, trade_date); contract tables: symbol |
| Conventions | For SHFE/INE, and for CZCE before 2019-12-31, volume, open interest and turnover are two-sided counts (options likewise) and are halved on ingestion; two-sided values must be even. GFEX and CFFEX have always been one-sided. Turnover ten-thousand yuan→yuan; IV percent→decimal; SHFE/INE IV is published per expiry series and stored as `series_implied_vol`. Early SHFE files list unlisted far months as placeholder rows with settlement price 0 and open interest 0; such rows do not enter the lake. On option expiry day, a settlement price of 0 for out-of-the-money contracts is a real price and is ingested as 0 (a 0 for futures is still treated as no price) |
| DCE | Default `dce_route = "sina"`: per-contract history from roughly mid-2018, missing zero-volume days and turnover; batch quotes only correspond to the most recent session and cannot prove that zero-volume contracts are complete; options are not collected. History is cached persistently, verified historical days are reused routinely, and revisions use `--refetch`. `official` is an experimental mapping: the official route has not yet passed availability and data-completeness acceptance; on a 412 or challenge page it is marked blocked, and the project does not run challenge scripts. A reachable probe only means the request is reachable; the required fields, dates, totals and schema must still be verified independently, and it cannot be taken as production acceptance |
| Known limits | SHFE per-contract turnover only exists from 2021/2022 onward and is empty before that; CFFEX files give no exercise volume or IV; from 2026 CZCE has a second expiry series marked `MS`, and the canonical symbol keeps the marker (e.g. `CF2701MSC14200.CZC`); exchange option IV/Delta follow different conventions from the values derived in this lake; the official SHFE archive is missing the futures files for two trading days, 2004-06-25 and 2007-06-04 (404 on the day, the days before and after are fine, and the old `data/dailydata/kx` path has been taken offline entirely), and the CZCE archive likewise lacks 2010-06-21, 2010-07-01 and 2010-08-13 (404 on the day, neighbours fine); no backfill can recover them, and the gap check exempts these days (`UNPUBLISHED_FUTURES_SESSIONS` in `domain/derivatives.py`) |

<!-- derivative-capabilities:start -->

| Publisher | Route | Futures since | Options since | Lifecycle reference | Historical reference | Status |
|---|---|---|---|---|---|---|
| SHF | official | 2002-01-07 | 2018-09-21 | yes | yes | supported |
| CZC | official | 2010-01-04 | 2017-04-19 | yes | yes | supported |
| GFE | official | 2022-12-22 | 2022-12-23 | yes | no | supported |
| DCE | sina | 2018-06-01 | not supported | no | no | supported |
| CFE | official | 2010-04-16 | 2019-12-23 | yes | yes | supported |
| DCE | official | 2000-01-04 | 2017-03-31 | no | no | experimental |

Generated by `scripts/dev/sync_docs.py` from the reader registry. Start dates are adapter route boundaries; they do not prove that the source or this lake is continuous and complete. `experimental` routes have not yet passed acceptance on real payloads. INE futures for 2018 come from the energy center's own daily files; from 2019 they are published together with the SHF route, so the SHF dates do not apply to INE's separate start.

<!-- derivative-capabilities:end -->

#### corporate_actions


**Cash payment date** (`payment_date` / `payment_source`) stores only dates reported by a source; unknown values stay empty and are not filled with the ex-date:

| Evidence | `payment_source` | Entry point (all need `--symbols --start --end`) |
|------|------------------|------|
| Issuer implementation notice | `issuer_notice:…` (announcement number, page number, PDF SHA256) | `cne repair payment-dates --issuer-notices-only`: reviewed list, CNINFO (Shanghai and Shenzhen), BSE announcements (EastMoney mirror of the original); does not request Baostock |
| Vendor | `baostock:dividPayDate` | `cne repair payment-dates`: issuer notices first, then the remainder is matched against Baostock |

- A notice is written only when it uniquely proves the security code, ex-date, pre-tax cash and payment date; multiple matches, correction chains and unverified same-day bonus/transfer shares keep the gap. The original PDFs and responses are archived with the revision, and the per-event conclusions are in `meta/payment_date_repairs/<run_id>.json`.
- CNINFO and exchange announcements are registered in the source policy only as explicit repair evidence and take no part in the normal daily update or automatic fallback. If an ETF dividend has no verifiable complete history, a fund total-return series cannot be inferred from a single notice.
- When Baostock numeric fields have only six decimal places, the precision is recovered from the pre-tax "10派…元" (cash per 10 shares) plan in the same response, and rounding consistency is checked; after a match, the amount already in the lake is kept. B-share dates and notional ex-rights amounts in the same notice are not mixed into A-share events.
- A payment date earlier than the ex-date is a source typo (CSDC pays A-share cash on the ex-date; two kinds have been seen: last year's template not updated, and the record date written by mistake): it is rejected at parse time, and stored ones are treated as unknown by the repairs above and cleared if the real date cannot be found.
- When the same event is fetched several times, **the higher evidence level beats the newer fetch** (issuer notice > vendor date > none); only at the same level does recency decide. For field descriptions see [corporate_actions](schema.md#corporate_actions). An ordinary historical backfill does not wipe verified amounts, payment dates or reviewed bonus/transfer terms.
- The BSE website's [old/new code mapping table](https://www.bse.cn/service/code_mapping.html) (248 pairs) is used only as aliases for searching issuer notices; it is **not** proof of the effective date of the code change or of position continuity.
- Consumers who need a complete cash schedule may, for codes where `ex_date_rule_applies(symbol)` (`cnequity.domain.action_evidence`) is true — Shanghai, Shenzhen and Beijing A-share stocks — temporarily estimate the missing payment date with the ex-date according to their own research policy, clearly marked as an estimate; do not write it back as the source-reported `payment_date`. The actual payment date may differ. This does not apply to funds, CDRs or B shares, which still need a reported date.

| Item | Value |
|------|-------|
| Wave | `corporate_actions` (Wave 1, before daily_bars) |
| Primary | eastmoney datacenter (daily) |
| Fallback / backfill | tdx_protocol ex-rights (historical backfill per security; `xdxr` category 11 "扩缩股" (share expansion/consolidation) becomes `unit_split` with the ratio taken from `suogu`; category 12 "非流通股缩股" (non-tradable share consolidation) does not move the trading price and is therefore excluded); explicit repair: THS historical dividend page (BJ); old-code migration catch-up: EastMoney 920xxx targeted report |
| Frequency | Daily |
| Primary key | (symbol, ex_date, action_type) |
| Output | manifest metadata `symbols_to_rebackfill` |
| Self-healing | Daily updates check the last 10 trading days: missing events are queried from TDX, while missing steps are evaluated using independent Baostock evidence through the unified repair. Historical contradictions remain advisory, including unassessed events and unavailable sources. Fund cash distributions are separate from unit conversions. |
| **Known gaps** | Historical dividends and ex-rights of delisted securities may still be missing. The number of gaps varies with the delisted securities in the lake, the adjustment factors and the audit window; **the `missing_corporate_action_delisted` finding in the latest `meta/quality/health-latest.json` is authoritative, and the docs do not fix a sample count**. **Both default sources were verified directly**: `tdx_protocol`'s `xdxr()` may still return 0 records for delisted securities even with the correct market number (market=2); `eastmoney`'s historical snapshots (`meta/source_snapshots/corporate_actions`, covering from 2015-09-29) may likewise have no records. Neither source reliably provides complete history for securities that have disappeared from its online security list. You can now use an explicit `cne repair delisted-actions --source baostock --apply` to fetch Baostock dividend, bonus share and transfer share events for delisted SH/SZ securities, and `cne repair delisted-actions --source ths` to fetch the THS historical dividend page for delisted BJ securities. There is also `cne repair ex-dates --ex-dates ...`: the primary source for backfill is TDX, and EastMoney only reaches rows before 2015-09-29 with the equality filter of the daily update (the report itself goes back to 1991), so this one fills day by day for the specified ex-dates, each date with its own capture scope and batch. None of the three take part in the daily update or ordinary backfill by default, and all repaired rows keep their own `source` provenance. These default gaps are not caused by rate limiting but by the different historical retention ranges of each source. `cne audit` classifies unrepaired batches separately as `missing_corporate_action_delisted` (info level), not mixed with `missing_corporate_action` (warning level) for securities still trading. Share restructurings such as "缩股/减资/合股" (share consolidation/capital reduction/share merger) are not among this dataset's four kinds of dividend and ex-rights events; the adjusted-return check uses `share_structure.change_reason` as a secondary explanation and records `adjustment_explained_by_share_structure` (info), so that recorded share restructurings are not misreported as missing ex-rights |

#### adj_factors (derived) {#adj_factorsderived}

| Item | Value |
|------|-------|
| Step | `derive_adj_factors` (finalize wave) |
| Primary | sina (qfq/hfq factor series); for the BSE exchange stage, `derived_actions`: computed from corporate actions and previous closes in the lake according to the exchange's ex-rights rules, with Sina as a cross-check |
| Fallback | baostock (factor derived from the ratio of raw / hfq close) |
| Input | daily_bars trading days + external factor API |
| Listing window | Delisted securities retain historical factors; zero-volume placeholders after their delisting date no longer trigger factor derivation. Traded quotes after that date keep the security in scope for catalogue verification. |
| Frequency | Daily after compact |
| Primary key | (symbol, trade_date, adjust_type) |
| Notes | External cumulative factors are aligned with daily_bars; `adj_close = close * factor` |
| **Known gaps** | Stocks take factors from Sina's `f` field, ETF/LOF from the `s` field (hfq uses `s` directly, qfq uses `1/s`). Sina supports some BSE securities, but newly listed securities that have not traded, delisted securities, or securities without factors at the source may still be missing; the latest `adj_factor_coverage` / `adj_factor_source_unavailable` findings and `meta/quality/health-latest.json` are authoritative. For securities that are formally delisted and for which Sina explicitly returns an empty series, the derivation writes `meta/state/adj_factors.json.source_unavailable_symbols`, stopping pointless retries but never fabricating factors. |
| **Query-side consequences** | `load(adjust="hfq")` defaults to `strict_adj=False`; rows missing a factor are returned with `factor=1.0`, i.e. **unadjusted prices appear in adjusted results**, marked only by `adj_is_exact=False`. The actual number of inexact rows varies with the query window, the securities in scope and the latest factor coverage; rely on `adj_is_exact=False` in the result and the `adj_factor_coverage` finding in the latest `meta/quality/health-latest.json`.|
| **What to do** | To fail strictly instead of silently degrading: `load(..., strict_adj=True)`. **It is not the default**: newly listed stocks are necessarily missing a factor until they get their first one, so strict mode would make hfq queries with `universe="all_a"` raise errors indefinitely. Tolerance by default + the `adj_is_exact` marker + audit warnings is the trade-off between "no silent contamination" and "queries that work" |
| **BSE** | Factors after the exchange listing start (the later of `list_date` and 2020-07-27) are computed from corporate actions: each event steps on its effective trading day by `previous close ÷ ex-rights reference price`, and restructuring capitalization issues use the reference price from the announcement; the series starts from the stored level on the security's first exchange trading day, so securities that already match Sina keep their values. Sina is still fetched; when its steps differ from the computed values by more than the verification tolerance, `adj_factor_computed_vendor_divergence` is reported, and a step in Sina with none in the computed values usually means a corporate action is missing from the lake. Factor rows from the NEEQ period keep their original values |
| **Self-healing** | Each incremental run of `derive_adj_factors` finds securities that "have bars but are not reached by factors" and reschedules their full history, up to 500 per run. So history filled by `cne backfill daily_bars` gets its factors automatically in the following daily update, without `--full` |

### Historical factor repair

`cne repair adj-factors` previews offline and saves a plan. Add `--refresh-sources` to collect independent evidence; `--apply` rechecks the inputs and publishes supported corrections. Use `--symbols`, `--start` and `--end` to scope securities and events, `--max-symbols` to bound a batch, and `--plan` to require the reviewed preview's inputs. Cumulative corrections affect rows after the event; date options do not truncate output. Applying a default historical batch saves progress for subsequent runs.

Missing factors are recovered from the original same-source series without splicing different vendor scales. Replacing an entire stock series requires independent ex-reference prices, complete coverage and agreement on existing event magnitudes. Local stock corrections require event terms to match an independent reference price or independent factor event; actual closing returns alone are insufficient. ETF/LOF unit conversions are rebuilt from original vendor factors. Supported events can publish while unresolved events retain their values. Original data, source choices and event proofs are retained with revisions; repeated application does not compound corrections.

Historical event contradictions never block `audit_gate`, `publication_gate` or repair publication. Invalid factor values, corrupt files and primary-key errors still face structural gates. `strict_adj=True` and `adj_is_exact` check coverage, not historical economic accuracy. `load_with_receipt()`, MCP `query_bars` and row browsing return an `adjustment` summary for the factor revision read, including sources, assessment status and related unresolved events. Reads never fetch evidence or fail because of historical contradictions. Missing diagnostics are explicitly unassessed.

### v1.0-full (second batch)

#### fund_flow

| Item | Value |
|------|-------|
| Group | capital@17:00 |
| Primary | eastmoney |
| Primary key | (symbol, trade_date) |

#### northbound_holdings

| Item | Value |
|------|-------|
| Group | capital@17:00 |
| Primary | eastmoney (`RPT_MUTUAL_HOLDSTOCKNORTH_STA`) |
| Primary key | See [schema.md](schema.md) |
| Known limits | Disclosed quarterly since 2024-08; history can only accumulate going forward (EM returns 0 rows for historical `TRADE_DATE`) |

#### northbound_flows

| Item | Value |
|------|-------|
| Group | capital@17:00 |
| Primary | eastmoney Stock Connect fund history (`RPT_MUTUAL_DEAL_HISTORY`, `MUTUAL_TYPE` 001 Shanghai Connect / 003 Shenzhen Connect) |
| Primary key | See [schema.md](schema.md) |
| Coverage | **2014-11-17 → 2024-08-16** (Shenzhen Connect from 2016-12-05). Backfill: `cne backfill northbound_flows` |
| Known limits | The exchanges stopped disclosing daily northbound net buying from **2024-08-19**, and since then every row has a null `NET_DEAL_AMT`. These rows **are not written** (not zero-filled), so the watermark stays at 2024-08-16 permanently; the registry marks that date as `source_retired_date`, so `cne status` / `cne verify` do not misreport the source retirement as STALE. |
| Units | The report's amount columns are in **millions of yuan** and are converted to yuan when written. Yet `HOLD_MARKET_CAP` in the same row is in yuan — the report mixes units, so recalibrate when changing fields |
| One request at a time | The report rejects `TRADE_DATE` range predicates (`InputMismatchException`), so the full set is fetched and windowed locally; the number of rows actually returned grows with source revisions |

#### margin_trading

| Item | Value |
|------|-------|
| Group | capital@17:00 |
| Primary | exchange (SSE `queryMargin` + SZSE `1837_xxpl` tab2 margin trading details) |
| Alternative | eastmoney (`[margin_trading] source = "eastmoney"`, switched manually only) |
| Primary key | (symbol, trade_date) |
| Known limits | **SSE does not publish the short-selling balance**, so `short_balance` is null for SH rows (no local estimate); SZSE publishes one trading day later than SSE, and a day is written only after both have published, so it lags the EastMoney path by about one trading day |

#### valuation_metrics

| Item | Value |
|------|-------|
| Daily source | `eastmoney_datacenter` (datacenter report `RPT_VALUEANALYSIS_DET`, whole market by date, including BSE, about 2 requests per day); when datacenter has not published yet or fails, it falls back to the eastmoney push2 clist snapshot (source recorded as `eastmoney`) |
| Historical source | baostock (`cne backfill valuation_metrics`; daily PE/PB/PS per security backfilled to 2016; **excludes BSE**, BJ is no longer requested); for EastMoney outage windows use `cne repair valuation-outage`, which reads datacenter |
| Primary key | (symbol, trade_date) |
| Known limits | baostock history includes pe_ttm/pb/ps_ttm; `float_mv`←close×volume/turn (close-price basis), `total_mv`←Q4 totalShare×close (estimate, labeled by `total_mv_basis`); the daily EM snapshot covers the latest trading day. **P/E basis**: push2 f9 is the dynamic P/E, written to `pe_dynamic` and not to `pe_ttm`; datacenter `PE_TTM` matches baostock `peTTM` (true TTM). Market cap fields distinguish vendor-reported values from estimates via `total_mv_basis` / `float_mv_basis` |

#### announcement_index

| Item | Value |
|------|-------|
| Primary | cninfo |
| Group | disclosures@20:00 |
| Primary key | announcement_id |
| Date axis | **Calendar days** (`session_scope = "calendar"`): listed companies also disclose on Saturdays. It therefore belongs to `cne run events` rather than the daily batch, and 0 rows on a non-trading day is normal, not a fetch failure |
| Notes | On-demand body text (`announcement_body`) is not implemented yet; the batch path only indexes |

#### share_structure / shareholder_counts

| Item | Value |
|------|-------|
| Primary | eastmoney (`RPT_F10_EH_EQUITY` / `RPT_F10_EH_HOLDERNUM`) |
| Group | fundamentals@17:35 |
| Primary key | (symbol, change_date, announce_date) / (symbol, count_date, announce_date) |
| Collection method | **Whole-market scan by date range**, not a loop per security and not by reporting period. `RPT_F10_EH_EQUITY.END_DATE` is the share change date; shareholder counts are also disclosed at the end of ten-day periods and months. Scanning only quarter ends silently misses the other disclosure dates |
| Daily range | Looks back 30 days by `NOTICE_DATE`. The window opens on the announcement date rather than the change date: a change that took effect weeks ago is only announced today, and a window keyed on the change date would never see it |
| PIT | `announce_date` comes from `NOTICE_DATE` and is part of the primary key |
| Client backfill start | **1990-01-01** for `share_structure`, **1992-01-01** for `shareholder_counts`; this is the history start set in the registry and does not guarantee that the source has records for every date |

#### top_holders

| Item | Value |
|------|-------|
| Primary | eastmoney (`RPT_F10_EH_HOLDERS` total basis + `RPT_F10_EH_FREEHOLDERS` float basis) |
| Group | **Not in the daily waves**. Two reports × about 110 pages × two reporting periods ≈ 440 pages, 40 times the two above; putting it in fundamentals would crowd out the whole macro_risk group. Run it separately with `cne backfill top_holders` |
| Primary key | (symbol, record_date, holder_scope, holder_rank, holder_name, announce_date). **holder_name must be part of the primary key**: shareholders with the same number of shares share a rank, and deduplicating without the name would simply delete one of them |
| Basis | One table with two bases, distinguished by `holder_scope`: `total` = top ten shareholders, `float` = top ten float shareholders. `holding_pct` has different denominators on the two sides (share of total shares vs share of float shares) and is **not directly comparable** |
| PIT | `RPT_F10_EH_HOLDERS` has no `NOTICE_DATE`; its disclosure date is borrowed from FREEHOLDERS by (symbol, report_period); rows that cannot borrow one are **dropped** rather than padded with the period-end date |
| Collection method | Scanned by `END_DATE` range (the total-basis report has no `NOTICE_DATE`; if the two reports were windowed on different columns, the borrowed disclosure dates would have nothing to match). The daily update looks back 240 days |
| Source history floor | Currently supported from 2003. Earlier total-basis records lack a linkable disclosure date, and the reporting period is not passed off as PIT time; out-of-range backfills are rejected. |
| Pagination | A single period exceeds EastMoney's 100-page limit, so `keyset_column="SECUCODE"` switches anchors to page past it (see `datacenter.py`) |

### On-demand datasets

Not in the daily waves. Cached in `meta/on_demand/`, optionally written to DuckDB tables.

| Dataset | Source | Trigger |
|---------|--------|---------|
| stock_news | eastmoney | `cne query --dataset stock_news --symbol` |
| research_reports | eastmoney reportapi | Per security |
| announcement_body | cninfo | **Not implemented** (do not add to `[on_demand].datasets`) |
| financial_reports | sina / gpcw | **Not implemented** (do not add to `[on_demand].datasets`) |

### Meta datasets

| Dataset | Storage |
|---------|---------|
| ingestion_runs | manifest.db |
| ingestion_batches | manifest.db |
| quality_findings | meta/quality/findings/ |
| source_diffs | meta/quality/source_diffs/ |
| data_catalog | Generated by `cne stats show --json` (direct-scan fallback when there is no stats table) |

### Source availability matrix

| Source | Protocol | MVP use | Fallback | Degradation strategy |
|--------|----------|-----------|--------|---------|
| tdx_protocol | TCP | bars, instruments, calendar | eastmoney clist (tip route) / kline (multi-day) | tip gaps go into curated; snapshot used for diff |
| sina | HTTP | hfq factors (qfq derived at query time) | — | Skip the security + quality finding |
| bse | HTTP | BJ current-period daily bars, security roster and trading status | — | Current-period snapshots cannot be treated as history; filling amount into existing rows requires a per-row consistency check |
| eastmoney | HTTP | Daily primary source for corporate actions, capital flows | — | Skip + quality finding |
| cninfo | HTTP | announcement_index | — | events:disclosures collected by calendar day; regulatory events derived from committed announcements |
| baostock | TCP | Shanghai and Shenzhen historical ST, valuation, delisted backfill and security identity supplements | — | Reuse sessions; at most 50,000 requests per Shanghai calendar day, one connection at a time; blacklisting freezes for this year's count × 6 hours. BJ historical ST needs an independent source |
| pboc | HTTP | Increment in aggregate financing to the real economy (`macro_indicators`) | — | The main write requires the full series; a single-year failure blocks the write for this run, to avoid advancing the watermark across a gap |
| nbs | HTTP | **Audit only**: PMI releases, cross-checked against `macro_indicators` | — | Runs per the source switch; enabled in the public template; failures or the disabled state go into the verification report |
| exchange | HTTP | `margin_trading` **primary**; fallback for `trading_status` / `trading_calendar` / `dragon_tiger` / `block_trades`; `daily_bars` fill for exchange-fund sessions under one lot (exchange day k-line, quantities in shares, amounts rounded to yuan); `[exchange_audit]` price cross-check | — | Margin trading is aggregated from member firm reports with no reseller in between; the dragon-tiger list and block trades are only asked when EastMoney cannot answer, and neither exchange publishes for BSE (recorded in `backup_gaps`); audit findings are advisory and do not fail the run |
| sina_bars | HTTP | Sina daily bar last-resort fallback (rate-limited separately from the adjustment factor endpoint) | — | Skip + quality finding |
| ths | HTTP | THS public pages: industry, valuation | — | Skip + quality finding |
| ths_pages | HTTP | `d.10jqka.com.cn` kline, the sole source of `sector_bars` | — | This dataset has **no second source**; a failure is a gap |
| ths_bonus | HTTP | THS dividend and rights issue pages | — | More conservative rate limit (3.0s); skip + quality finding |
| tdx_fin | HTTP | `financial_statement_items` backup: whole-market quarterly packages (gpcw, MD5-checked) | — | When EastMoney fails, the daily run writes only disclosures from the last 14 days (`tdx_fin`); `cne backfill --source tdx_fin` fills only items the lake lacks, and older periods may carry restated values, so they are labelled `tdx_fin_backfill` and excluded by strict PIT; no `revenue` (the package holds 营业收入) and no indicator ratios |
| ths_official | HTTPS (keyed) | Optional credentialed source: arbitration snapshots, financial statement supplements, explicit historical repairs; versioned corrections follow the versioned publishing rules | — | Without a key everything is `skipped`, and the lake keeps its existing sources unchanged |
| tushare | HTTPS (keyed) | Optional: BJ historical ST evidence (`stock_st`, from 2017-01-01) | bak_basic name evidence (2016) | Without a token, earlier BJ bars stay unresolved and **are not assumed normal** |

> **AkShare is no longer called by any adapter** ([issue #3](https://github.com/rootSunc/CNEquity/issues/3)).
> Its two former call sites both pointed at endpoints this project already calls directly: the ST set used the same EastMoney
> push2 clist sector with the same `fs` filter, and PMI / money supply used the same EastMoney
> datacenter reports. It offered not a second basis but a parsing layer around the same one.
> It has also been removed from the dependencies; `pip install cnequity` no longer installs it.

For scheduling and primary/fallback switching, see the [operations runbook](../operations/runbook.md).

Derivatives caching, the shared egress budget, rejection cooldowns and research boundaries are covered together in the [how-to guide](../recipes/derivatives.md). No fixed request rate can guarantee that an IP will never be banned.
