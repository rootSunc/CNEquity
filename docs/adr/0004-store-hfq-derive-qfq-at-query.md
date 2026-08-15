# ADR 0004: Store hfq factors only; derive qfq at query time

- Status: Accepted
- Date: 2026-07-06
- Supersedes: earlier approach that persisted qfq and forced full-history rewrites

## Context

`adj_factors` is derived from Sina Finance cumulative adjustment series. The
engine stores `factor` such that `adj_price = raw_price * factor` for both
qfq and hfq after normalizing Sina's qfq divisor.

**qfq (forward / 前复权)** is anchored to the *latest* raw close. When a new
corporate action occurs, Sina recomputes the **entire** qfq series. A
`adj_close` computed today for 2018-06-01 will not match one computed after the
next ex-date. Persisting qfq snapshots is therefore **not reproducible** and
forces full-history rewrites on every refresh.

**hfq (backward / 后复权)** is anchored to the IPO/listing basis. Historical
hfq factors for dates before a new corporate action **do not change** when a
later ex-date is added. New events only append rows for dates on/after the
event. Storing hfq is naturally **append-only** and aligns with ex-date-driven
refresh (`symbols_to_rebackfill`).

Consumers still need qfq for many momentum screens (latest price = raw price).
That requirement is met at **query time**, not in `derived/adj_factors`.

## Decision

1. **`derived/adj_factors` stores only `adjust_type = 'hfq'`.**  
   The derive step fetches and caches Sina hfq only. qfq rows are never
   written to the lake.

2. **qfq is derived in the read path** (`load(..., adjust="qfq")` and SQL
   views) from stored hfq:

   ```
   factor_qfq(t) = hfq_factor(t) / hfq_factor(T)
   ```

   where `T` is the **anchor trade date** per symbol within the query window:
   the latest `trade_date` ≤ `end` (or the max bar date when `end` is open).
   At `t = T`, `factor_qfq(T) = 1.0` so the latest raw close is unchanged.

3. **`[adj_factors].adjust_types` defaults to `["hfq"]`.**  
   If config lists `qfq`, derive logs a warning and ignores it (qfq is never
   persisted).

4. **Append-only derive:** refresh only symbols on ex-date (or new listings);
   merge new hfq rows without rewriting prior partitions.

## Consequences

**Positive**

- Historical backtests using `adjust="qfq"` are reproducible for a fixed
  `[start, end]` window: anchor `T` is explicit.
- Ex-date refresh touches only affected symbols; no full-market qfq rewrite.
- Single stored series reduces storage, cache files, and derive HTTP volume.
**Negative / neutral**

- `load(..., adjust="qfq")` requires hfq rows through the anchor date `T` per
  symbol (`adj_is_exact` / `strict_adj` apply as today).
- DuckDB `daily_bars_adj` view uses hfq + window anchor (not a stored qfq join).
- Lakes with only legacy `adjust_type='qfq'` partitions need
  `asl derive adj_factors` after config change.
- External consumers reading `derived/adj_factors` parquet directly must apply
  the same ratio if they need qfq.

## Alternatives considered

| Option | Why rejected |
|--------|----------------|
| Store both qfq and hfq | qfq drift + full rewrite; doubles fetch/storage |
| Store qfq only | hfq also drifts in some vendor APIs; worse for long-horizon research |
| Store event-level ratios (per ex-date) | more portable but larger schema change; hfq + query ratio is sufficient with Sina |
| Query-time hfq only (drop qfq API) | breaks consumer expectation that latest adj ≈ raw; ratio formula is cheap |

## References

- adj_factors cache / silent-staleness concerns (addressed by append-only + audit)
- `src/cn_market_lake/adapters/sina/adj_factors.py` — Sina hfq/qfq fetch
- `src/cn_market_lake/query/reader.py` — `_apply_adjustment`
