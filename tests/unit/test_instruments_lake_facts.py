"""The rename lineage survives the next daily rebuild.

`prev_symbol` is not a vendor field: it is written by the BJ code migration
from this lake's own old-code map. The catalogue is rebuilt from live sources
every run, compaction dedupes on `symbol` keeping the newest row, and the fresh
null won — measured across the published instruments revisions of 2026-09-18,
the count of recorded renames went 248 -> 2 -> 248, the last step being someone
re-running the migration by hand.
"""

from datetime import date, datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.steps.reference import _carry_lake_facts
from cnequity.storage.layout import init_data_layout

FETCHED = datetime(2026, 9, 19, tzinfo=timezone.utc)


def _lake(tmp_path, rows: list[dict]) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(root / "part-merged.parquet")
    return cfg


def _curated_row(symbol: str, prev: str | None) -> dict:
    return {
        "symbol": symbol,
        "name": f"N-{symbol[:6]}",
        "exchange": symbol.split(".")[1],
        "asset_type": "stock",
        "list_date": date(2021, 11, 15),
        "delist_date": None,
        "prev_symbol": prev,
        "source": "bse",
        "data_version": "v1",
        "fetched_at": FETCHED,
    }


def _fetched(symbol: str, prev: str | None = None) -> dict:
    row = _curated_row(symbol, prev)
    row["source"] = "tdx_protocol"
    return row


def test_a_recorded_rename_survives_a_rebuild_that_never_heard_of_it(tmp_path):
    cfg = _lake(tmp_path, [_curated_row("920001.BJ", "430001.BJ")])

    out = _carry_lake_facts(cfg, pl.DataFrame([_fetched("920001.BJ")]))

    assert out["prev_symbol"].to_list() == ["430001.BJ"]


def test_an_incoming_rename_is_the_authority_on_itself(tmp_path):
    """Only nulls are filled; a source that does report one is not overruled."""
    cfg = _lake(tmp_path, [_curated_row("920001.BJ", "430001.BJ")])

    out = _carry_lake_facts(cfg, pl.DataFrame([_fetched("920001.BJ", "899999.BJ")]))

    assert out["prev_symbol"].to_list() == ["899999.BJ"]


def test_a_symbol_the_lake_knows_nothing_about_keeps_its_null(tmp_path):
    cfg = _lake(tmp_path, [_curated_row("920001.BJ", "430001.BJ")])

    out = _carry_lake_facts(cfg, pl.DataFrame([_fetched("600519.SH")]))

    assert out["prev_symbol"].to_list() == [None]


def test_a_rebuild_without_the_column_still_gets_the_lineage(tmp_path):
    cfg = _lake(tmp_path, [_curated_row("920001.BJ", "430001.BJ")])
    incoming = pl.DataFrame([_fetched("920001.BJ")]).drop("prev_symbol")

    out = _carry_lake_facts(cfg, incoming)

    assert out["prev_symbol"].to_list() == ["430001.BJ"]


def test_an_empty_lake_is_left_alone(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    incoming = pl.DataFrame([_fetched("920001.BJ")])

    assert _carry_lake_facts(cfg, incoming).equals(incoming)
