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


@pytest.fixture(params=["generation", "catalog"])
def settle(request, tmp_path):
    """Run a writer's test on both kinds of lake.

    ``settle.on_working_tree()`` skips the ``catalog`` run of a test that then
    writes a dataset's working tree itself, as the other writers' tests do.
    ``settle(cfg)`` returns *cfg* as it is, or, for ``catalog``, after moving
    the lake onto its catalog and removing every registered dataset's
    generation and working tree: what the writer reads then exists only as
    the catalog's files, which is where a lake ends up once it publishes there.
    """
    import json
    import shutil

    from cnequity.query import catalog_read

    if request.param == "generation":
        keep = lambda cfg: cfg  # noqa: E731
        keep.on_working_tree = lambda: None
        yield keep
        return
    from cnequity.storage.catalog_migration import migrate_to_catalog
    from cnequity.storage.revisions import RevisionStore
    from equitylake_core.catalog import Catalog
    from equitylake_core.extensions import ExtensionUnavailableError

    try:
        Catalog(tmp_path / "ducklake-probe", create=True).close()
    except ExtensionUnavailableError as exc:
        pytest.skip(str(exc))

    moved: list[Path] = []

    def move(cfg):
        migrate_to_catalog(cfg, apply=True)
        catalog_read.close_all()
        store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
        for path in sorted((cfg.meta_root / "revisions").glob("*/current.json")):
            pointer = json.loads(path.read_text())
            dataset = pointer["dataset"]
            generation = store.current_root(dataset)
            # Mark the published revision as the catalog's, as a publish there does.
            pointer["storage"] = "catalog"
            path.write_text(json.dumps(pointer))
            if pointer.get("receipt"):
                receipt_path = cfg.meta_root / pointer["receipt"]
                receipt = json.loads(receipt_path.read_text())
                receipt.setdefault("metadata", {})["storage"] = "catalog"
                receipt_path.write_text(json.dumps(receipt))
            for layer in (cfg.curated_root, cfg.derived_root):
                shutil.rmtree(layer / dataset, ignore_errors=True)
                moved.append(layer / dataset)
            shutil.rmtree(generation, ignore_errors=True)
        return cfg

    def on_working_tree():
        pytest.skip("the test goes on to write the working tree by hand")

    move.on_working_tree = on_working_tree
    yield move
    catalog_read.close_all()
    # The writer read and published through the catalog alone.
    assert [str(path) for path in moved if path.exists()] == []
