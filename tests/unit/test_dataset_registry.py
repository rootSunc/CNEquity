"""Invariants tying the DatasetSpec registry to schemas, PKs, and steps."""

from cnequity.domain.datasets import (
    DATASETS,
    FETCH_SEMANTICS,
    PARTITION_COLS,
    TIER_LABELS,
    TIERS,
    WATERMARK_SKIP,
    curated_dataset_names,
    datasets_by_tier,
    derived_dataset_names,
    fetch_semantics,
    get_dataset,
    history_mode,
    is_dataset_enabled,
    pit_dataset_names,
)
from cnequity.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS


def test_every_dataset_has_schema_and_pk():
    for name in DATASETS:
        assert name in DATASET_SCHEMAS, f"{name} missing from DATASET_SCHEMAS"
        assert name in PRIMARY_KEYS, f"{name} missing from PRIMARY_KEYS"


def test_every_schema_has_registry_entry():
    for name in DATASET_SCHEMAS:
        assert name in DATASETS, f"{name} in DATASET_SCHEMAS but not in registry"


def test_partition_and_date_cols_exist_in_schema():
    for name, spec in DATASETS.items():
        schema = DATASET_SCHEMAS[name]
        if spec.partition_col is not None:
            assert spec.partition_col in schema, (
                f"{name}: partition_col {spec.partition_col!r} not in schema"
            )
        if spec.query_date_col is not None:
            assert spec.query_date_col in schema, (
                f"{name}: date_col {spec.query_date_col!r} not in schema"
            )


def test_primary_key_columns_exist_in_schema():
    for name, pk in PRIMARY_KEYS.items():
        schema = DATASET_SCHEMAS[name]
        for col in pk:
            assert col in schema, f"{name}: PK column {col!r} not in schema"


def test_legacy_tables_match_registry():
    # Guards against editing the derived dicts instead of the specs.
    assert set(PARTITION_COLS) == set(curated_dataset_names())
    assert WATERMARK_SKIP == {
        "financial_statement_items",
        "institutional_holdings",
        "earnings_disclosure_schedule",
        # The rolling calendar contains future event dates, so event_date is
        # not a valid freshness watermark.
        "economic_calendar",
        # Fetched per report period, not per date — a watermark over trade dates
        # would advance daily and mean nothing.
        "share_structure",
        "shareholder_counts",
        "top_holders",
        "share_unlock_schedule",
    }
    assert set(FETCH_SEMANTICS) == {
        "trading_status",
        "fund_flow",
        "share_unlock_schedule",
        "valuation_metrics",
        "sector_members",
        "index_constituents",
        "industry_members",
        "analyst_consensus",
        "hot_rank",
        "sector_bars",
        "sector_fund_flow",
        "news_headlines",
        "flash_news_wire",
        "economic_calendar",
    }
    assert fetch_semantics("fund_flow") == "snapshot"
    assert fetch_semantics("trading_status") == "snapshot"
    assert fetch_semantics("daily_bars") == "by_date"
    assert history_mode("daily_bars") == "by_date"
    assert history_mode("fund_flow") == "snapshot_only"
    assert history_mode("trading_status") == "snapshot_with_backfill"
    assert history_mode("share_unlock_schedule") == "snapshot_with_backfill"
    assert history_mode("valuation_metrics") == "snapshot_with_backfill"


def test_layer_partitions():
    assert "adj_factors" in derived_dataset_names()
    assert "adj_factors" not in curated_dataset_names()
    assert pit_dataset_names() == {
        "financial_statement_items",
        "announcement_index",
        # Disclosed weeks after the period they describe; keyed by period alone
        # a July backtest would read August's filing.
        "share_structure",
        "shareholder_counts",
        "top_holders",
    }
    assert get_dataset("daily_bars").partition_col == "trade_date"


def test_market_breadth_is_session_dense():
    assert get_dataset("market_breadth").coverage_mode == "session_dense"
    assert get_dataset("industry_index").coverage_mode == "session_dense"


def test_margin_trading_registry_matches_the_default_exchange_owner():
    spec = get_dataset("margin_trading")
    assert spec.primary_source == "exchange"
    # EastMoney is a deliberate operator-selected alternative, not an
    # automatic fallback that could silently change provenance.
    assert spec.backup_source is None


def test_revisable_datasets_declare_rolling_windows_and_append_only_feeds():
    """The registry is the source of truth for incremental reconciliation.

    Feeds whose source can revise an already-published key must overlap a
    bounded window; genuinely append-only feeds must opt out explicitly so a
    future fetcher cannot accidentally treat them as revisable.
    """
    assert get_dataset("daily_bars").reconciliation_lookback_days == 5
    assert get_dataset("daily_bars").reconciliation_lookback_mode == "trading_day"
    assert get_dataset("corporate_actions").reconciliation_lookback_days == 30
    assert get_dataset("announcement_index").reconciliation_lookback_days == 30
    assert get_dataset("financial_statement_items").reconciliation_lookback_days == 30
    assert get_dataset("share_structure").reconciliation_lookback_days == 30

    trade_ticks = get_dataset("trade_ticks")
    assert trade_ticks.append_only is True
    assert trade_ticks.reconciliation_lookback_days == 0

    for spec in DATASETS.values():
        assert spec.reconciliation_lookback_days >= 0
        if spec.append_only:
            assert spec.reconciliation_lookback_days == 0


def test_every_dataset_lands_in_exactly_one_tier():
    grouped = datasets_by_tier()
    assert set(grouped) == set(TIERS)
    assert set(TIER_LABELS) == set(TIERS)
    placed = [name for names in grouped.values() for name in names]
    assert sorted(placed) == sorted(DATASETS)
    assert len(placed) == len(set(placed))


def test_report_period_datasets_are_partitioned_by_quarter():
    """The directories are ``2016Q1``, so anything else makes audit cry wolf.

    ``check_mixed_partition_granularity`` compares the on-disk period against
    the registry, and a report_period dataset left on the ``day`` default
    reports every one of its partitions as stale, forever.
    """
    for name, spec in DATASETS.items():
        if spec.partition_col == "report_period":
            assert spec.partition_granularity == "quarter", name


def test_registered_fetch_steps_cover_curated_datasets():
    """Every curated dataset is producible by a registered step."""
    import cnequity.steps  # noqa: F401 — register steps
    from cnequity.orchestrator.registry import STEP_REGISTRY

    # market_breadth/sentiment_scores are derive-style steps registered under
    # their dataset names; instruments etc. match step names directly.
    missing = [name for name in curated_dataset_names() if name not in STEP_REGISTRY]
    assert not missing, f"curated datasets without a registered step: {missing}"


def test_is_stale_respects_per_dataset_tolerance():
    from datetime import date

    from cnequity.domain.datasets import is_stale

    anchor = date(2026, 7, 8)
    # daily default tolerance = 1 → 07-07 fresh, 07-06 stale
    assert is_stale("daily_bars", date(2026, 7, 7), anchor) is False
    assert is_stale("daily_bars", date(2026, 7, 6), anchor) is True
    # margin_trading tolerance = 2 (T+1) → 07-06 still fresh
    assert is_stale("margin_trading", date(2026, 7, 6), anchor) is False
    # northbound_holdings quarterly tolerance = 100 → last quarter-end fresh
    assert is_stale("northbound_holdings", date(2026, 6, 30), anchor) is False
    # None / unknown handled
    assert is_stale("daily_bars", None, anchor) is False
    assert is_stale("nope", date(2026, 7, 1), anchor) is True  # default tol=1


def test_optional_capture_freshness_follows_config_switches(tmp_path):
    from cnequity.config import Config

    cfg = Config(data_root=tmp_path / "data")
    assert is_dataset_enabled("trade_ticks", cfg) is False
    assert is_dataset_enabled("minute_bars", cfg) is False
    assert is_dataset_enabled("minute_bars_5m", cfg) is False

    cfg.trade_ticks_enabled = True
    cfg.minute_bars_enabled = True
    cfg.minute_bars_frequencies = ["1m"]
    assert is_dataset_enabled("trade_ticks", cfg) is True
    assert is_dataset_enabled("minute_bars", cfg) is True
    assert is_dataset_enabled("minute_bars_5m", cfg) is False

    cfg.minute_bars_frequencies = ["1m", "5m"]
    assert is_dataset_enabled("minute_bars_5m", cfg) is True


def test_row_grain_agrees_with_intraday_frequency_wherever_both_are_set():
    """The descriptive field and the behavioural one must not drift apart.

    `intraday_frequency` drives fetch, checks and the reader; `row_grain` only
    describes what a row covers. A dataset holding 5m bars that advertises "1m"
    would be a catalog lying about its own contents.
    """
    from cnequity.domain.datasets import DATASETS

    for spec in DATASETS.values():
        if spec.intraday_frequency:
            assert spec.row_grain == spec.intraday_frequency, spec.name


def test_every_intraday_dataset_declares_a_row_grain():
    """Including the one that carries no `intraday_frequency` on purpose.

    trade_ticks omits `intraday_frequency` so it cannot inherit bar-shaped
    checks (see its DatasetSpec). Without `row_grain` it would then be
    indistinguishable from a daily dataset in the catalog and the dashboard,
    which is the confusion this pair of fields exists to avoid.
    """
    from cnequity.domain.datasets import DATASETS

    assert DATASETS["trade_ticks"].row_grain == "tick"
    assert DATASETS["trade_ticks"].intraday_frequency is None
    assert DATASETS["minute_bars"].row_grain == "1m"
    assert DATASETS["daily_bars"].row_grain is None
