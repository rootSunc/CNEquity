"""Readers that used to walk ``curated/<dataset>`` see rows only the lake catalog holds."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest
from tests.catalog_seed import publish_rows
from tests.unit.test_lake_write import _bars

from cnequity.config.loader import Config

FETCHED = datetime(2024, 1, 5, tzinfo=timezone.utc)
PROVENANCE = {"source": "test", "data_version": "v1", "fetched_at": FETCHED}


@pytest.fixture
def cfg(tmp_path):
    from cnequity.query import catalog_read
    from equitylake_core.extensions import ExtensionUnavailableError, ensure_extensions

    try:
        import duckdb

        ensure_extensions(duckdb.connect())
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))
    yield Config(data_root=tmp_path / "data")
    catalog_read.close_all()


def _seed(cfg, dataset: str, rows: list[dict]) -> None:
    from cnequity.domain.schemas import DATASET_SCHEMAS

    frame = pl.DataFrame([{**row, **PROVENANCE} for row in rows])
    schema = DATASET_SCHEMAS[dataset]
    frame = frame.with_columns(
        pl.lit(None, dtype=dtype).alias(name)
        for name, dtype in schema.items()
        if name not in frame.columns
    )
    publish_rows(cfg, dataset, frame.select(list(schema)))
    assert not (cfg.curated_root / dataset).exists()


def test_adj_factor_triggers_read_published_actions_and_listings(cfg):
    from cnequity.derive import adj_factors

    day = date(2024, 1, 5)
    _seed(
        cfg,
        "corporate_actions",
        [
            {"symbol": "600000.SH", "ex_date": day, "action_type": "dividend"},
            {"symbol": "000001.SZ", "ex_date": date(2024, 1, 4), "action_type": "dividend"},
        ],
    )
    _seed(
        cfg,
        "instruments",
        [
            {"symbol": "688001.SH", "list_date": day},
            {"symbol": "600000.SH", "list_date": date(1999, 11, 10)},
        ],
    )

    assert adj_factors._corporate_action_symbols_on(cfg, day) == {"600000.SH"}
    assert adj_factors._new_listing_symbols_on(cfg, day) == {"688001.SH"}
    assert adj_factors._corporate_action_symbols_on(cfg, date(2024, 1, 8)) == set()


def test_index_constituent_dates_come_from_published_rows(cfg):
    from cnequity.steps import structure

    assert structure._existing_as_of_dates(cfg, "index_constituents") == set()
    _seed(
        cfg,
        "index_constituents",
        [{"index_symbol": "000300.SH", "symbol": "600000.SH", "as_of_date": date(2024, 6, 28)}],
    )

    assert structure._existing_as_of_dates(cfg, "index_constituents") == {date(2024, 6, 28)}


def test_latest_sector_boards_come_from_the_newest_published_period(cfg):
    from cnequity.derive import sector_routing

    def board(day, code):
        return {
            "sector_code": code,
            "sector_name": code,
            "board_type": "industry",
            "trade_date": day,
            "close": 1.0,
        }

    _seed(
        cfg,
        "sector_bars",
        [board(date(2024, 1, 31), "BK0001"), board(date(2024, 2, 1), "BK0002")],
    )

    boards = sector_routing._latest_em_boards_from_lake(cfg)

    assert [row["sector_code"] for row in boards] == ["BK0002"]


def test_bar_dates_and_stats_groups_see_catalog_files(cfg):
    from cnequity.adapters.calendar.exchange_calendar import curated_bar_dates
    from cnequity.domain.datasets import DATASETS
    from cnequity.storage import stats

    publish_rows(
        cfg,
        "daily_bars",
        pl.concat(
            [
                _bars(date(2024, 1, 2), {"600000.SH": 10.0}),
                _bars(date(2024, 2, 1), {"600000.SH": 11.0}),
            ]
        ),
    )
    assert not (cfg.curated_root / "daily_bars").exists()

    assert curated_bar_dates(cfg.curated_root, "daily_bars") == {
        date(2024, 1, 2),
        date(2024, 2, 1),
    }
    groups = stats._file_groups(cfg.curated_root / "daily_bars", DATASETS["daily_bars"])
    assert sorted(groups) == ["2024-01", "2024-02"]
    assert all(len(files) == 1 for files in groups.values())


def test_share_counts_are_empty_without_history_and_read_published_rows(cfg):
    from cnequity.query import catalog_read
    from cnequity.query.share_counts import load_share_counts

    assert load_share_counts(cfg).is_empty()
    _seed(
        cfg,
        "share_structure",
        [
            {
                "symbol": "600000.SH",
                "change_date": date(2023, 12, 29),
                "total_shares": 1.0e9,
                "announce_date": date(2023, 12, 29),
            }
        ],
    )
    catalog_read.close_all()

    assert load_share_counts(cfg).rows() == [("600000.SH", date(2023, 12, 29), 1.0e9)]
