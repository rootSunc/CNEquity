# decouple-continuous-events-pipeline: Tasks

## 1. Configuration & Group Decoupling

- [x] 1.1 In `configs/cnequity.toml` and template configs, remove `announcement_index` from `[job.daily.groups.capital]`.
- [x] 1.2 In `configs/cnequity.toml` and template configs, remove `flash_news_wire` and `news_headlines` from `[job.daily.groups.research]`.
- [x] 1.3 In `configs/cnequity.toml` and template configs, remove `regulatory_events` from `[job.daily.groups.macro_risk]`.
- [x] 1.4 Define new event groups under `[job.events.groups]` (`corporate_events` and `news_wire`) with their own `compact` steps.

## 2. Orchestrator Engine & CLI Enhancements

- [x] 2.1 Update `cnequity/orchestrator/engine.py` to support `is_calendar_exempt` flag on job specifications, skipping `is_trading_day()` checks for event groups.
- [x] 2.2 Update CLI parser (`cnequity/cli/main.py`) to add `--ignore-calendar` flag to `cne run daily` or register `cne run events` subcommand.
- [x] 2.3 Verify `Manifest.start_run()` records event job executions with distinct job types (e.g. `events:corporate_events`).

## 3. Data Step Empty Tolerance on Non-Trading Days

- [x] 3.1 In `cnequity/steps/common.py:fetch_incremental_daily()`, ensure zero-row responses on non-trading dates for `announcement_index` and `regulatory_events` do not raise `RuntimeError`.
- [x] 3.2 Ensure incremental watermark progression functions correctly when empty weekend runs occur.

## 4. Downstream Derivation & Regression Testing

- [x] 4.1 Test `step_sentiment_scores` execution when announcements/news were updated by prior independent event runs.
- [x] 4.2 Add unit tests in `tests/unit/test_engine_trading_day.py` verifying that event groups run on weekend dates while daily groups remain gated.
- [x] 4.3 Add integration test for pruned `capital` group verifying fast execution without external CNINFO network calls.
- [x] 4.4 Run full regression suite (`pytest tests/unit`) to ensure zero breakage of existing daily pipelines.
