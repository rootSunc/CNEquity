"""Shared small market situations for unit checks."""

from datetime import date
from pathlib import Path

import polars as pl
import pytest


@pytest.fixture
def halted_ex_event():
    """An ex-date without a trade, followed by the first effective session."""

    def make(symbol: str, prior: date, ex_date: date, resumed: date) -> dict:
        return {
            "symbol": symbol,
            "ex_date": ex_date,
            "resumed": resumed,
            "actions": pl.DataFrame({"symbol": [symbol], "ex_date": [ex_date]}),
            "sessions": pl.DataFrame({"symbol": [symbol, symbol], "trade_date": [prior, resumed]}),
        }

    return make


@pytest.fixture
def settle(tmp_path):
    """Make a writer's test read the lake catalog alone.

    Tests seed a lake by writing files under ``curated/`` or ``derived/`` and
    publishing them (``tests.catalog_seed``). ``settle(cfg)`` removes those
    hand-written trees for every published dataset, so what the writer reads
    exists only as the catalog's files, and checks at teardown that no writer
    put one back.
    """
    from cnequity.domain.datasets import DATASETS
    from cnequity.query import catalog_read
    from cnequity.storage import revision_records
    from cnequity.storage.revisions import RevisionStore
    from equitylake_core.catalog import Catalog
    from equitylake_core.extensions import ExtensionUnavailableError

    try:
        Catalog(tmp_path / "ducklake-probe", create=True).close()
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))

    moved: list[Path] = []

    def move(cfg):
        import shutil

        catalog_read.close_all()
        revision_records.forget()
        store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root, create=False)
        for dataset in sorted(DATASETS):
            if store.current_pointer(dataset) is None:
                continue
            for layer in (cfg.curated_root, cfg.derived_root):
                shutil.rmtree(layer / dataset, ignore_errors=True)
                moved.append(layer / dataset)
        return cfg

    def on_working_tree():
        pytest.skip("the test goes on to write the working tree by hand")

    move.on_working_tree = on_working_tree
    move.kind = "catalog"
    yield move
    catalog_read.close_all()
    revision_records.forget()
    # The writer read and published through the catalog alone.
    assert [str(path) for path in moved if path.exists()] == []
