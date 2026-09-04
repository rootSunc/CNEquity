## 1. Core Incremental Logic in `src/cnequity/steps/common.py`

- [x] 1.1 In `fetch_incremental_daily` ([`src/cnequity/steps/common.py`](file:///home/bladestone/devspace/gitcode/stockagent/CNEquity/src/cnequity/steps/common.py#L482)), add non-trading calendar date check to empty-response validation:
  ```python
  if part.is_empty():
      if not allow_empty:
          if dataset in CALENDAR_DATE_DATASETS and not is_trading_day(config, d):
              logger.info(
                  "%s: 0 rows returned for non-trading calendar date %s (tolerated)",
                  dataset,
                  d.isoformat(),
              )
              continue
          raise RuntimeError(f"{dataset}: no rows returned for {d.isoformat()}")
  ```
- [x] 1.2 Verify that non-empty rows on non-trading days (e.g. Saturday filings) are unaffected and properly parsed, validated, and staged.

## 2. Unit Tests in `tests/unit/test_incremental_daily.py`

- [x] 2.1 Add unit test verifying that `fetch_incremental_daily` on a `CALENDAR_DATE_DATASETS` feed (e.g. `announcement_index`) successfully tolerates 0 rows when `is_trading_day` returns `False` (e.g. Sunday).
- [x] 2.2 Add unit test verifying that `fetch_incremental_daily` on `announcement_index` still raises `RuntimeError` when a trading day returns 0 rows.
- [x] 2.3 Add unit test verifying a multi-day span (e.g. Saturday with rows, Sunday 0 rows, Monday with rows) returns a concatenated dataframe containing the Saturday and Monday rows without errors.

## 3. Verification & Code Quality

- [x] 3.1 Run `ruff check` and `ruff format --check` to ensure no lint regressions.
- [x] 3.2 Run `pytest tests/unit` to ensure all unit tests pass cleanly.
- [x] 3.3 Verify against real CNINFO query logic for 2026-08-01 through 2026-08-03 to confirm full compatibility.
