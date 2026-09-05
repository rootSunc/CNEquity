# decouple-continuous-events-pipeline: Design

## Context

CNEquity orchestrates daily dataset pipelines through `[job.daily.groups]`, supervised by an Engine that evaluates `is_trading_day()` before execution. 

Historically, textual and event-driven datasets were bundled into daily numerical market groups:
- `announcement_index` was placed in `job.daily.groups.capital` (17:00).
- `regulatory_events` was placed in `job.daily.groups.macro_risk` (17:55).
- `flash_news_wire` and `news_headlines` were placed in `job.daily.groups.research` (18:15).

This coupling creates severe operational and data quality drawbacks:
1. **Missed Evening and Weekend Disclosures**: Filings disclosed between 19:00~23:00 or during weekends are ignored until the following trading day's 17:00 run (up to 48 hours latency).
2. **Blast Radius on Fast Daily Groups**: Network timeouts or pagination issues on external sites (CNINFO) fail or degrade the entire `capital` group, delaying critical fund flow (`fund_flow`, `margin_trading`, `valuation_metrics`) data.
3. **Inability to Schedule Independent Frequencies**: Event feeds need periodic ingestion (every 2~4 hours or multiple evening passes), not a single once-daily run.

## Goals / Non-Goals

**Goals:**
- Decouple `announcement_index`, `regulatory_events`, `flash_news_wire`, and `news_headlines` from the standard daily market groups.
- Enable running these event steps independently via CLI (`cne run events --group <name>` or `cne run daily --group <name> --ignore-calendar`).
- Support 7x24 continuous periodic execution (6-hour interval for news_wire: 00:00, 06:00, 12:00, 18:00; including non-trading days).
- Maintain 100% backward compatibility for downstream parquet schemas, partitions, and consumers.
- Preserve the dependency contract of `sentiment_scores` by establishing clear execution ordering.

**Non-Goals:**
- Converting CNEquity into a streaming real-time daemon (Kafka/Flink); periodic micro-batching remains the architecture.
- Changing `daily_bars` or other L0-L1 core market data steps.

---

## Decisions

### Decision 1: Separation of Datasets into Event Groups

In `configs/cnequity.toml`, daily groups are pruned of unstructured text/event steps:

```toml
# Pruned capital: pure quantitative flow and valuation
[job.daily.groups.capital]
at = "17:00"
steps = [
  "fund_flow", "northbound_holdings", "northbound_flows", "margin_trading",
  "valuation_metrics", "sector_members", "compact",
]

# Pruned macro_risk: market-derived risk metrics
[job.daily.groups.macro_risk]
at = "17:55"
steps = ["macro_indicators", "market_breadth", "share_unlock_schedule", "commodity_bars", "compact"]

# Pruned research: rotation and sentiment derivation
[job.daily.groups.research]
at = "18:15"
steps = [
  "institutional_holdings", "analyst_consensus", "hot_rank",
  "sector_bars", "sector_fund_flow", "sentiment_scores", "compact",
]
```

New event groups are introduced (can be invoked as standalone tasks):

```toml
# Corporate events group (announcements + regulatory actions)
[job.events.groups.corporate_events]
steps = ["announcement_index", "regulatory_events", "compact"]

# Real-time news and wire headlines
[job.events.groups.news_wire]
steps = ["flash_news_wire", "news_headlines", "compact"]
```

### Decision 2: Calendar-Independent Execution in Engine

In `cnequity/orchestrator/engine.py`, the trading-day gate is modified:

```python
# Calendar check applies ONLY to trading-day-bound jobs
is_calendar_exempt = backfill or job_name == "init" or getattr(job_spec, "calendar_exempt", False) or ignore_calendar

if not is_calendar_exempt and not is_trading_day(self.config, trade_date):
    logger.info("Skipping job %s: %s is not a trading day", job_name, trade_date.isoformat())
    self.manifest.finish_run(skip_run_id, "skipped_non_trading_day")
    return {"status": "skipped_non_trading_day", ...}
```

This allows event groups to execute on Saturdays, Sundays, and holidays without being short-circuited.

### Decision 3: Sparse Weekend Data Tolerance

In `cnequity/steps/common.py:fetch_incremental_daily()`, non-trading days naturally have low or zero corporate filings.
The change guarantees that when an event step fetches a non-trading date and receives 0 rows, it logs an informational note and continues without error (`allow_empty = True` behavior on non-trading days).

### Decision 4: Downstream Lineage & Sentiment Scores Contract

`sentiment_scores` in `research.py` reads `curated/announcement_index` and `curated/news_headlines`.
- **Execution Order Contract**:
  - `corporate_events` and `news_wire` must run at or before 18:00 on trading days.
  - When `research` runs at 18:15, `sentiment_scores` consumes the freshest available curated partitions.
  - If a weekend or off-market run writes new announcements, `sentiment_scores` will incorporate them into the next available scoring session.

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
| :--- | :--- | :--- |
| **CNINFO API rate-limiting** | Frequent polling of 28 announcement categories might trigger WAF bans. | Maintain polite pacing (`min_interval_seconds = 1.0`), and schedule periodic runs at moderate intervals (e.g. every 2~4 hours). |
| **Out-of-order execution** | `sentiment_scores` runs before `corporate_events` finishes. | Scheduler DAG dependency: configure `after = ["corporate_events"]` in queue config for trading days. |
| **Staging leftover fragmentation** | Frequent event micro-runs produce small staging folders. | Each event group runs its own `compact` step at the end of the wave, committing staging to curated immediately. |

---

## Verification Plan

1. **Unit Tests**:
   - Verify `cne run events --group corporate_events` executes on a non-trading day (e.g. Saturday) without returning `skipped_non_trading_day`.
   - Verify zero-row return on a holiday does not raise `RuntimeError: no rows returned`.
2. **Integration Verification**:
   - Execute pruned `cne run daily --group capital` on a trading date: verify execution completes in <= 2 minutes and correctly outputs fund flow and valuation metrics.
   - Execute `cne run events --group corporate_events` and confirm `announcement_index` and `regulatory_events` commit to curated correctly.
   - Run `cne run daily --group research` and confirm `sentiment_scores` calculates without errors.
