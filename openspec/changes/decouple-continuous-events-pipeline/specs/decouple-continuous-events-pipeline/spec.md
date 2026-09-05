# decouple-continuous-events-pipeline Specification

## MODIFIED Requirements

### Requirement: Daily groups pruned of unstructured event steps

The daily ingestion pipeline groups SHALL only contain market-cleared datasets strictly bound to the trading calendar. Event steps (`announcement_index`, `regulatory_events`, `flash_news_wire`, `news_headlines`) SHALL be decoupled from daily groups.

#### Scenario: Capital group executes without announcement index
- **GIVEN** `job.daily.groups.capital` configured with `steps = ["fund_flow", "northbound_holdings", "northbound_flows", "margin_trading", "valuation_metrics", "sector_members", "compact"]`
- **WHEN** `cne run daily --group capital` executes
- **THEN** it completes fund flow and valuation steps and compacts them without invoking CNINFO announcement scrapers.

---

## ADDED Requirements

### Requirement: Calendar-exempt execution for event pipelines

The orchestrator engine SHALL support running event groups on non-trading days without aborting with `skipped_non_trading_day`.

#### Scenario: Corporate events executed on non-trading day
- **WHEN** `cne run events --group corporate_events` runs for a Saturday or Sunday
- **THEN** the engine bypasses the `is_trading_day()` gate, executes `announcement_index` and `regulatory_events`, and merges any discovered filings into curated partitions.

#### Scenario: News wire periodic execution
- **WHEN** `cne run events --group news_wire` executes on any calendar day
- **THEN** it incrementally fetches and compacts new flash news and headlines without dependency on daily market closure.

---

### Requirement: Zero-row filing tolerance on non-trading days

Event steps SHALL treat zero-row results on non-trading calendar dates as normal operations rather than unexpected data failures.

#### Scenario: Sunday with zero company filings
- **GIVEN** an incremental announcement scrape for a Sunday where CNINFO returns 0 filings
- **WHEN** `step_announcement_index` processes the empty result
- **THEN** it records zero rows, finishes with status `succeeded` (or `warning`), and does not raise `RuntimeError`.

---

### Requirement: Downstream sentiment scores compatibility

The sentiment derivation step SHALL accept empty or updated event inputs gracefully without breaking daily research runs.

#### Scenario: Sentiment scores computed with decoupled announcements
- **GIVEN** `corporate_events` ran independently and committed filings to `curated/announcement_index`
- **WHEN** `step_sentiment_scores` executes during the daily `research` pass
- **THEN** it reads the committed announcements and produces valid sentiment scores for all covered symbols.
