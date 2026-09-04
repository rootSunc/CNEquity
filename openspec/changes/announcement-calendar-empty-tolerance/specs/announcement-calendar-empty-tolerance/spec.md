## Purpose

Defines empty-response tolerance rules for calendar-date disclosure feeds (such as `announcement_index`), allowing zero-row responses on non-trading days while maintaining strict non-empty failure guards on trading days.

## ADDED Requirements

### Requirement: Non-trading calendar days tolerate zero rows in incremental fetch

The system SHALL tolerate an empty (0 rows) response during daily incremental fetching of datasets configured in `CALENDAR_DATE_DATASETS` if the evaluated date is not a trading day (`is_trading_day == False`).

- An empty non-trading day MUST NOT raise a `RuntimeError`.
- An empty non-trading day MUST be safely omitted from the combined partition frames.
- If non-empty disclosure rows exist on a non-trading day (e.g. Saturday filings), those rows MUST be retained and included in the staged partition.

#### Scenario: Sunday with zero announcements
- **WHEN** incremental fetch walks calendar date 2026-08-02 (Sunday) for `announcement_index`
- **AND** the source returns an empty dataframe (0 rows)
- **THEN** the system logs the occurrence and continues without raising an error

#### Scenario: Saturday with valid announcements
- **WHEN** incremental fetch walks calendar date 2026-08-01 (Saturday) for `announcement_index`
- **AND** the source returns 1,371 rows
- **THEN** all 1,371 rows are included in the staged partition

### Requirement: Normal trading days strictly reject zero rows

The system SHALL continue to enforce the strict non-empty guard on any date classified as a trading day (`is_trading_day == True`).

- If the source returns zero rows for a trading day and `allow_empty` is not explicitly `True`, the system MUST raise `RuntimeError: {dataset}: no rows returned for {date}`.
- This ensures that upstream source outages, crawler bans, or transport silent drops during trading days are loudly reported.

#### Scenario: Trading day returns zero rows
- **WHEN** incremental fetch walks a normal trading day (e.g. Monday 2026-08-03) for `announcement_index`
- **AND** the source returns an empty dataframe (0 rows)
- **THEN** `fetch_incremental_daily` raises `RuntimeError: announcement_index: no rows returned for 2026-08-03`
