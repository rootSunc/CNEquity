"""Copying a committed generation into the lake catalog, and noticing when the copies differ."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config.loader import Config
from cnequity.storage.catalog_bridge import (
    committed_source,
    compare_dataset,
    import_dataset,
    lake_root,
)
from cnequity.storage.revisions import RevisionStore
from equitylake_core.catalog import Catalog
from equitylake_core.extensions import ExtensionUnavailableError

FETCHED = datetime(2026, 7, 23, tzinfo=timezone.utc)


def _rows(day: date, closes: dict[str, float], **extra) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "symbol": symbol,
                "trade_date": day,
                "close": close,
                "source": "test",
                "data_version": "v1",
                "fetched_at": FETCHED,
                **extra,
            }
            for symbol, close in closes.items()
        ]
    )


@pytest.fixture
def cfg(tmp_path):
    config = Config(data_root=tmp_path)
    files = []
    for month, frame in {
        # Older files lack a column newer ones have, as in the real lake.
        "2024-01": _rows(date(2024, 1, 2), {"600000.SH": 10.0, "000001.SZ": 9.0}),
        "2024-02": _rows(date(2024, 2, 1), {"600000.SH": 11.0}, pre_close=10.0),
    }.items():
        path = config.curated_root / f"daily_bars/trade_date={month}/part-merged.parquet"
        path.parent.mkdir(parents=True)
        frame.write_parquet(path)
        files.append(path)
    RevisionStore(config.meta_root, config.curated_root).commit(
        "daily_bars",
        run_id="seed",
        changed_files=files,
        schema_version=1,
        contract_fingerprint="contract",
    )
    return config


@pytest.fixture
def lake(cfg):
    try:
        catalog = Catalog(lake_root(cfg), create=True)
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))
    yield catalog
    catalog.close()


def test_a_dataset_without_a_committed_generation_is_skipped(cfg, lake):
    assert committed_source(cfg, "index_bars") is None
    assert import_dataset(cfg, lake, "index_bars") is None
    assert compare_dataset(cfg, lake, "index_bars") is None


def test_the_imported_table_matches_the_committed_generation(cfg, lake):
    result = import_dataset(cfg, lake, "daily_bars")
    assert result.rows == 3
    comparison = compare_dataset(cfg, lake, "daily_bars")
    assert comparison.matches and comparison.catalog_rows == 3
    # The real date column survives a directory named like a Hive partition of it.
    rows = lake.execute(
        'SELECT symbol, trade_date, pre_close FROM "daily_bars" ORDER BY 2, 1'
    ).fetchall()
    assert rows == [
        ("000001.SZ", date(2024, 1, 2), None),
        ("600000.SH", date(2024, 1, 2), None),
        ("600000.SH", date(2024, 2, 1), 10.0),
    ]


def test_a_changed_value_is_a_mismatch(cfg, lake):
    import_dataset(cfg, lake, "daily_bars")
    lake.execute("UPDATE \"daily_bars\" SET close = close + 0.01 WHERE symbol = '000001.SZ'")
    comparison = compare_dataset(cfg, lake, "daily_bars")
    assert comparison.revision_rows == comparison.catalog_rows
    assert not comparison.same_content and not comparison.matches


def test_a_missing_row_is_a_mismatch(cfg, lake):
    import_dataset(cfg, lake, "daily_bars")
    lake.execute("DELETE FROM \"daily_bars\" WHERE symbol = '000001.SZ'")
    comparison = compare_dataset(cfg, lake, "daily_bars")
    assert (comparison.revision_rows, comparison.catalog_rows) == (3, 2)
    assert not comparison.matches
