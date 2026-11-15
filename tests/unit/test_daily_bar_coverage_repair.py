from datetime import date

import polars as pl
import pytest
from tests.catalog_seed import commit_tree

from cnequity.config import Config
from cnequity.storage.coverage import resolved_bj_session_dates
from cnequity.storage.repairs.daily_bar_coverage import repair_daily_bar_coverage
from cnequity.storage.revisions import RevisionStore
from cnequity.storage.state import StateStore

DAY = date(2026, 8, 7)


def _commit(cfg, dataset):
    commit_tree(
        RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root),
        dataset,
        run_id="seed",
        schema_version=1,
        contract_fingerprint="fixture",
        changed_files=list((cfg.curated_root / dataset).rglob("*.parquet")),
    )


@pytest.fixture
def lake(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["920001.BJ", "920002.BJ", "920003.BJ"],
            "list_date": [DAY, DAY, date(2026, 8, 10)],
            "delist_date": [None] * 3,
        },
        schema_overrides={"delist_date": pl.Date},
    ).write_parquet(root / "part.parquet")
    _commit(cfg, "instruments")
    root = cfg.curated_root / "daily_bars" / f"trade_date={DAY}"
    root.mkdir(parents=True)
    pl.DataFrame({"symbol": ["920001.BJ"], "trade_date": [DAY], "volume": [100]}).write_parquet(
        root / "part.parquet"
    )
    _commit(cfg, "daily_bars")
    state = StateStore(cfg.meta_root)
    state.record_missing_dates("daily_bars", [DAY], reason="bj_session_incomplete")
    return cfg


def test_unpublished_rows_cannot_clear_debt(lake):
    cfg = lake
    root = cfg.curated_root / "daily_bars" / f"trade_date={DAY}"
    pl.DataFrame(
        {"symbol": ["920001.BJ", "920002.BJ"], "trade_date": [DAY] * 2, "volume": [100] * 2}
    ).write_parquet(root / "part.parquet")
    assert repair_daily_bar_coverage(cfg)["resolved_sessions"] == []
    _commit(cfg, "daily_bars")
    before = StateStore(cfg.meta_root).get_payload("daily_bars")
    assert repair_daily_bar_coverage(cfg)["resolved_sessions"] == [str(DAY)]
    assert StateStore(cfg.meta_root).get_payload("daily_bars") == before


@pytest.mark.parametrize(("source", "resolved"), [("derived_bar_gap", False), ("baostock", True)])
def test_only_independent_halt_proves_a_session(lake, source, resolved):
    cfg = lake
    root = cfg.curated_root / "trading_status" / f"trade_date={DAY}"
    root.mkdir(parents=True)
    pl.DataFrame(
        {"symbol": ["920002.BJ"], "trade_date": [DAY], "is_trading": [False], "source": [source]}
    ).write_parquet(root / "part.parquet")
    _commit(cfg, "trading_status")
    state = StateStore(cfg.meta_root)
    assert bool(resolved_bj_session_dates(cfg, state.get_payload("daily_bars"))) is resolved
    state.record_missing_dates("daily_bars", [DAY], reason="source_failed")
    assert not resolved_bj_session_dates(cfg, state.get_payload("daily_bars"))


def test_apply_updates_only_proven_debts_and_watermark(lake, monkeypatch):
    cfg = lake
    root = cfg.curated_root / "daily_bars" / f"trade_date={DAY}"
    pl.DataFrame(
        {"symbol": ["920001.BJ", "920002.BJ"], "trade_date": [DAY] * 2, "volume": [100] * 2}
    ).write_parquet(root / "part.parquet")
    _commit(cfg, "daily_bars")
    monkeypatch.setattr("cnequity.steps.finalize._watermark_date_for", lambda *a: DAY)
    result = repair_daily_bar_coverage(cfg, apply=True)
    assert result["applied"]
    assert result["after"]["missing_ranges"] == []
    assert result["after"]["complete_through"] == str(DAY)


def test_unknown_new_listings_do_not_reopen_old_known_roster_debts(lake):
    cfg = lake
    root = cfg.curated_root / "instruments"
    pl.DataFrame(
        {
            "symbol": ["920001.BJ", "920002.BJ", "920004.BJ"],
            "list_date": [DAY, DAY, None],
            "delist_date": [None] * 3,
        },
        schema_overrides={"delist_date": pl.Date},
    ).write_parquet(root / "part.parquet")
    _commit(cfg, "instruments")
    root = cfg.curated_root / "daily_bars" / f"trade_date={DAY}"
    pl.DataFrame(
        {"symbol": ["920001.BJ", "920002.BJ"], "trade_date": [DAY] * 2, "volume": [100] * 2}
    ).write_parquet(root / "part.parquet")
    _commit(cfg, "daily_bars")
    assert repair_daily_bar_coverage(cfg)["resolved_sessions"] == [str(DAY)]


def test_prelisting_debt_requires_committed_uncontradicted_dates(lake):
    from cnequity.storage.coverage import resolved_prelisting_keys

    owed = [
        dict(symbol="920003.BJ", trade_date=str(DAY)),
        dict(symbol="920001.BJ", trade_date=str(DAY)),
        dict(symbol="920999.BJ", trade_date=str(DAY)),
    ]
    assert resolved_prelisting_keys(lake, owed) == {("920003.BJ", str(DAY))}
    path = lake.curated_root / "daily_bars" / f"trade_date={DAY}" / "part.parquet"
    pl.DataFrame({"symbol": ["920003.BJ"], "trade_date": [DAY], "volume": [100]}).write_parquet(
        path
    )
    assert resolved_prelisting_keys(lake, owed) == {("920003.BJ", str(DAY))}
    _commit(lake, "daily_bars")
    assert not resolved_prelisting_keys(lake, owed)
