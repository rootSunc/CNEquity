"""Invariants tying the DatasetSpec registry to schemas, PKs, and steps."""

from cn_market_lake.domain.datasets import (
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
    pit_dataset_names,
)
from cn_market_lake.domain.schemas import DATASET_SCHEMAS, PRIMARY_KEYS


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
        # Fetched per report period, not per date — a watermark over trade dates
        # would advance daily and mean nothing.
        "share_structure",
        "shareholder_counts",
        "top_holders",
    }
    assert set(FETCH_SEMANTICS) == {
        "fund_flow",
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
    assert fetch_semantics("daily_bars") == "by_date"
    assert history_mode("daily_bars") == "by_date"
    assert history_mode("fund_flow") == "snapshot_only"
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
    import cn_market_lake.steps  # noqa: F401 — register steps
    from cn_market_lake.orchestrator.registry import STEP_REGISTRY

    # market_breadth/sentiment_scores are derive-style steps registered under
    # their dataset names; instruments etc. match step names directly.
    missing = [name for name in curated_dataset_names() if name not in STEP_REGISTRY]
    assert not missing, f"curated datasets without a registered step: {missing}"


def test_is_stale_respects_per_dataset_tolerance():
    from datetime import date

    from cn_market_lake.domain.datasets import is_stale

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


def test_row_grain_agrees_with_intraday_frequency_wherever_both_are_set():
    """The descriptive field and the behavioural one must not drift apart.

    `intraday_frequency` drives fetch, checks and the reader; `row_grain` only
    describes what a row covers. A dataset holding 5m bars that advertises "1m"
    would be a catalog lying about its own contents.
    """
    from cn_market_lake.domain.datasets import DATASETS

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
    from cn_market_lake.domain.datasets import DATASETS

    assert DATASETS["trade_ticks"].row_grain == "tick"
    assert DATASETS["trade_ticks"].intraday_frequency is None
    assert DATASETS["minute_bars"].row_grain == "1m"
    assert DATASETS["daily_bars"].row_grain is None
