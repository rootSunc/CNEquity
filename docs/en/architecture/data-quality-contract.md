# Shared CNEquity / USEquity Data Quality Contract

Version: 1.0 · Status: Active (2026-11-07)  
Scope: Shared data infrastructure across `CNEquity` (China market research data lake) and `USEquity` (US equity personal research infrastructure).

---

## 1. Core Principles

**Permissive ingestion, explicit quality, preserved provenance, prudent derivation.**

The goal of this contract is **"to reliably use imperfect data"** rather than "to accept only perfect data". Both systems rely primarily on freely accessible data sources; the quality framework exists to honestly describe provenance, freshness, and known limitations rather than turning upstream limitations into pipeline failures.

1. **A Data Quality Warning must not automatically become a Pipeline Failure.**
2. **Quality Tiers (`A`/`B`/`C`) are downstream usage recommendations, not database write switches.**
3. **Cross-source validation is an optional enhancement, not an ingestion gate.**
4. **Quality issues are expressed via metadata and `quality_flags` rather than manual approval state machines.**

---

## 2. Three Dimensions of Data Quality

| Dimension | Core Question | Handling Principle |
|---|---|---|
| **Availability** | Can the data be fetched? What if some is missing? | Collect as much useful data as possible; field-level, symbol-level, or date-level gaps are allowed |
| **Consistency** | Are formats, units, and field semantics unified? | Normalize schemas, date formats, units, and `NULL` semantics in the Adapter / Normalization layer while preserving raw values |
| **Reliability** | Where did the data come from and what are its caveats? | Tag every record/batch with provider source, timestamps, Quality Tier (`A`/`B`/`C`), and known caveats/warnings |

---

## 3. Error vs. Warning vs. Caveat Matrix

| Condition | Classification | Pipeline Behavior | Canonical Layer Representation |
|---|---|---|---|
| **Missing optional field** | `NORMAL` | Accept normally | Field set to `NULL` |
| **No trading on a given day / market closed / halted** | `NORMAL` | Accept normally; never fabricate bars | No synthetic bar generated, or status marked `no_trade` / `halted` |
| **Provider returns `0`, `""`, `N/A`, `-999` for missing** | `NORMAL` | Adapter converts to standard `NULL` | Field set to `NULL` with optional `zero_converted_to_null` flag |
| **Source temporarily unavailable (timeout / 429 / 5xx)** | `OPERATIONAL_WARNING` | **Preserve existing data**, log failure | Watermark stays at last success; run marked `partial` / `degraded` |
| **Cross-source value discrepancy** | `QUALITY_WARNING` | Preserve source observations; canonical layer selects primary source | Primary value written with `quality_flags=["cross_source_divergence"]` |
| **Severe anomaly (e.g. unverified price jump)** | `QUALITY_WARNING` | Ingest and downgrade to Tier `C` | Record written with `quality_tier="C"` and warning flag |
| **Structural/schema error (missing PK, `NaN`/`Inf`, `volume < 0`, `high < low`)** | `HARD_ERROR` | **Reject the invalid record** without aborting valid symbols in the batch | Excluded from canonical table; logged for adapter diagnosis |

---

## 4. Quality Tiers (`A` / `B` / `C`)

| Tier | Name | Definition | Typical Examples | Usage Recommendation |
|---|---|---|---|---|
| **A** | **Trusted** | Official regulatory/exchange/central-bank disclosures or verified primary data | Official exchange calendars, SEC EDGAR / CNINFO filings, FRED / central-bank macro series | Suitable for standard fundamental research, screening, and valuation |
| **B** | **Usable with Caveats** | Well-formed data from known free sources with potential coverage gaps, delays, or unverified adjustments | Free daily OHLCV (AKShare / Baostock / Yahoo / Stooq), dividends/splits, company/ETF profiles | **Usable by default**; keep caveats in mind for backtesting |
| **C** | **Unverified** | Uncertain provenance, unverified extraction, or records triggering severe anomaly warnings | Unverified price jumps without matching split records, conflicting filing contexts | Not recommended directly for investment conclusions; for exploration and diagnosis |

---

## 5. Minimal Provenance Envelope

| Field | Type | Required | Description |
|---|---|---|---|
| `source` | `string` | Yes | Provider identifier (`akshare`, `baostock`, `sec_edgar`, `yahoo_finance`, `stooq`, `fred`) |
| `ingested_at` | `timestamp (UTC)` | Yes | UTC timestamp when the record was ingested |
| `published_at` | `timestamp / date` | No | Source publication timestamp (`announce_date` / SEC `acceptance_datetime`; `NULL` if unknown) |
| `quality_tier` | `enum('A','B','C')` | Yes | Quality tier (`A`, `B`, or `C`) |
| `quality_flags` | `list<string>` | Yes | Warning flags (`[]` when clean) |
