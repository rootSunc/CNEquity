"""COMEX gold rows captured mid-session are replaced by Sina's final bars."""

from datetime import date, datetime, timezone

import polars as pl
from tests.catalog_seed import commit_tree

from cnequity.config import Config
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.query import load
from cnequity.storage.repairs.offshore_final import repair_offshore_final
from cnequity.storage.revisions import RevisionStore

D1, D2, D3 = date(2026, 8, 18), date(2026, 8, 19), date(2026, 8, 20)
EARLY = datetime(2026, 8, 19, 11, tzinfo=timezone.utc)


def _bar(symbol, day, close, fetched=EARLY):
    return {
        "symbol": symbol,
        "name": symbol,
        "exchange": "CMX" if symbol == "GC0.CMX" else "SHF",
        "trade_date": day,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1,
        "amount": None,
        "open_interest": None,
        "source": "sina",
        "data_version": "v1",
        "fetched_at": fetched,
    }


def _lake(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    root = cfg.curated_root / "commodity_bars" / "trade_date=2026"
    root.mkdir(parents=True)
    rows = [
        _bar("GC0.CMX", D1, 4452.5),
        _bar("GC0.CMX", D3, 4528.5),  # mid-session; final is 4574.8
        _bar("AU0.SHF", D2, 800.0),
    ]
    pl.DataFrame(rows, schema=DATASET_SCHEMAS["commodity_bars"]).write_parquet(
        root / "part.parquet"
    )
    commit_tree(
        RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root),
        "commodity_bars",
        run_id="seed",
        changed_files=[root / "part.parquet"],
        schema_version=1,
        contract_fingerprint="contract",
    )
    return cfg


def _final(cfg, start, end):
    rows = [_bar("GC0.CMX", D1, 4452.5), _bar("GC0.CMX", D2, 4580.7), _bar("GC0.CMX", D3, 4574.8)]
    return pl.DataFrame(rows).drop("data_version", "fetched_at")


def test_stale_rows_are_replaced_missing_ones_added_and_others_untouched(tmp_path, settle):
    cfg = settle(_lake(tmp_path))
    plan = repair_offshore_final(cfg, fetch=_final)
    assert (plan["rows"], plan["replaced"], plan["added"]) == (2, 1, 1)
    repair_offshore_final(cfg, apply=True, fetch=_final)
    rows = load("commodity_bars", config=cfg).sort("symbol", "trade_date")
    gold = dict(
        rows.filter(pl.col("symbol") == "GC0.CMX").select("trade_date", "close").iter_rows()
    )
    assert gold == {D1: 4452.5, D2: 4580.7, D3: 4574.8}
    untouched = rows.filter(pl.col("trade_date") == D1)["fetched_at"].to_list()
    assert untouched == [EARLY]
    assert rows.filter(pl.col("symbol") == "AU0.SHF")["close"].to_list() == [800.0]
    assert repair_offshore_final(cfg, fetch=_final)["replaced"] == 0
