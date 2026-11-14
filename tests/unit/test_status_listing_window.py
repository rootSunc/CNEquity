"""Status rows outside a code's listed life, and the guards that stop new ones."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.domain.trading_status import impossible_delistings, split_before_listing
from cnequity.query import load
from cnequity.storage.repairs.status_listing_window import repair_status_listing_window
from cnequity.storage.repairs.status_outages import plan_status_outages
from cnequity.storage.revisions import RevisionStore

D1, D2, D3 = date(2026, 9, 23), date(2026, 9, 24), date(2026, 9, 28)
FETCHED = datetime(2026, 9, 23, 8, tzinfo=timezone.utc)


def _status(symbol, day, source, status="normal"):
    return {
        "symbol": symbol,
        "trade_date": day,
        "is_trading": status == "normal",
        "status": status,
        "risk_warning": None,
        "source": source,
        "data_version": "v1",
        "fetched_at": FETCHED,
    }


def _commit(cfg: Config, dataset: str) -> None:
    RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root).commit(
        dataset,
        run_id="seed",
        changed_files=sorted((cfg.curated_root / dataset).rglob("*.parquet")),
        schema_version=1,
        contract_fingerprint="contract",
    )


@pytest.fixture
def lake(tmp_path, settle) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {
            # Listed on D2; the old compaction had called it delisted before that.
            "symbol": ["000010.SZ", "000020.SZ", "000030.SZ"],
            "list_date": [D2, date(2020, 1, 2), D3],
            # Formally retired on D2.
            "delist_date": [None, D2, None],
        },
        schema_overrides={"delist_date": pl.Date},
    ).write_parquet(instruments / "part-0.parquet")
    _commit(cfg, "instruments")
    for day, symbols in ((D1, ["000030.SZ"]), (D2, ["000010.SZ"])):
        part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True)
        pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [day] * len(symbols),
                "volume": [100.0],
                "amount": [1000.0],
            }
        ).write_parquet(part / "part-0.parquet")
    _commit(cfg, "daily_bars")
    rows = [
        _status("000010.SZ", D1, "eastmoney"),
        _status("000010.SZ", D1, "derived_delisted", "delisted"),
        _status("000010.SZ", D2, "derived_delisted", "delisted"),
        _status("000010.SZ", D2, "baostock"),
        _status("000010.SZ", D3, "eastmoney"),
        _status("000020.SZ", D1, "eastmoney"),
        _status("000020.SZ", D2, "derived_delisted", "delisted"),
        _status("000020.SZ", D3, "derived_delisted", "delisted"),
        # Its listing date is contradicted by a traded bar on D1.
        _status("000030.SZ", D1, "eastmoney"),
        # No instrument record left at all.
        _status("519001.SH", D1, "derived_delisted", "delisted"),
    ]
    part = cfg.curated_root / "trading_status" / "trade_date=2026-09"
    part.mkdir(parents=True)
    pl.DataFrame(rows, schema=DATASET_SCHEMAS["trading_status"]).write_parquet(
        part / "part-0.parquet"
    )
    _commit(cfg, "trading_status")
    return settle(cfg)


def test_plan_names_rows_outside_the_listed_life(lake):
    plan = repair_status_listing_window(lake)

    assert plan["applied"] is False
    assert plan["rows"] == 4
    assert plan["by_reason"] == {"before_listing": 1, "delisting_not_on_record": 3}
    assert load("trading_status", config=lake).height == 8


def test_apply_keeps_session_records_and_real_delistings(lake):
    report = repair_status_listing_window(lake, apply=True)

    assert report["applied"] is True
    rows = (
        load("trading_status", config=lake)
        .sort("symbol", "trade_date")
        .select("symbol", "trade_date", "source")
        .rows()
    )
    assert rows == [
        ("000010.SZ", D2, "baostock"),
        ("000010.SZ", D3, "eastmoney"),
        ("000020.SZ", D1, "eastmoney"),
        ("000020.SZ", D2, "derived_delisted"),
        ("000020.SZ", D3, "derived_delisted"),
        ("000030.SZ", D1, "eastmoney"),
    ]
    assert repair_status_listing_window(lake)["rows"] == 0


def test_a_successful_old_staging_batch_cannot_resurrect_deleted_statuses(lake, settle):
    from cnequity.storage.parquet import StagingWriter, compact_dataset

    settle.on_working_tree()
    repair_status_listing_window(lake, apply=True)
    rows = pl.DataFrame(
        [
            _status("000010.SZ", D1, "eastmoney"),
            _status("000010.SZ", D2, "derived_delisted", "delisted"),
            _status("519001.SH", D1, "derived_delisted", "delisted"),
            # A genuine delisting and a listing date contradicted by bars survive.
            _status("000020.SZ", D2, "derived_delisted", "delisted"),
            _status("000030.SZ", D1, "eastmoney"),
        ],
        schema=DATASET_SCHEMAS["trading_status"],
    )
    StagingWriter(lake.staging_root).write_batch("trading_status", "old-run", "old-0", rows)
    store = RevisionStore(lake.meta_root, lake.curated_root, lake.derived_root)
    base = store.current_root("trading_status")
    compact_dataset(
        lake.staging_root, lake.curated_root, "trading_status", "old-run", base_root=base
    )
    current = pl.concat(
        [pl.read_parquet(p) for p in (lake.curated_root / "trading_status").rglob("*.parquet")]
    )
    keys = set(current.select("symbol", "trade_date", "source").iter_rows())
    assert ("000010.SZ", D1, "eastmoney") not in keys
    assert ("000010.SZ", D2, "derived_delisted") not in keys
    assert ("519001.SH", D1, "derived_delisted") not in keys
    assert ("000020.SZ", D2, "derived_delisted") in keys
    assert ("000030.SZ", D1, "eastmoney") in keys


@pytest.mark.parametrize("traded_after_retirement", [False, True])
def test_staged_normal_status_after_retirement_requires_a_contradicting_quote(
    lake, traded_after_retirement, settle
):
    from cnequity.storage.parquet import _guard_staged_status

    settle.on_working_tree()
    if traded_after_retirement:
        part = lake.curated_root / "daily_bars" / f"trade_date={D3}"
        part.mkdir(parents=True)
        pl.DataFrame({"symbol": ["000020.SZ"], "trade_date": [D3], "volume": [100]}).write_parquet(
            part / "part.parquet"
        )
        _commit(lake, "daily_bars")
    rows = pl.DataFrame(
        [
            _status("000020.SZ", D1, "eastmoney"),
            _status("000020.SZ", D3, "eastmoney_cached"),
            _status("000020.SZ", D3, "derived_delisted", "delisted"),
        ],
        schema=DATASET_SCHEMAS["trading_status"],
    )
    kept = _guard_staged_status(lake.curated_root, rows)
    assert kept.height == (3 if traded_after_retirement else 2)
    assert kept.filter(pl.col("source") == "eastmoney_cached").height == int(
        traded_after_retirement
    )


def test_the_daily_step_drops_rows_before_a_known_listing(lake):
    from cnequity.steps import reference

    frame = pl.DataFrame(
        [_status("000010.SZ", D1, "eastmoney"), _status("000010.SZ", D2, "eastmoney")],
        schema=DATASET_SCHEMAS["trading_status"],
    )
    kept, findings = reference._drop_before_listing(lake, frame)
    assert kept["trade_date"].to_list() == [D2]
    assert findings[0]["check"] == "trading_status_before_listing_dropped"


def test_a_delisting_before_the_listing_is_not_one():
    spans = pl.DataFrame(
        {
            "symbol": ["000010.SZ", "000020.SZ", "000030.SZ"],
            "list_date": [D2, date(2020, 1, 2), None],
            "delist_date": [D1, D2, D1],
        }
    )
    assert impossible_delistings(spans) == {"000010.SZ"}


def test_split_before_listing_trusts_a_bar_over_the_listing_date():
    rows = pl.DataFrame(
        {"symbol": ["000010.SZ", "000010.SZ", "000030.SZ"], "trade_date": [D1, D2, D1]}
    )
    listings = pl.DataFrame(
        {
            "symbol": ["000010.SZ", "000030.SZ"],
            "list_date": [D2, D3],
            "first_traded": [D2, D1],
        }
    )
    kept, dropped = split_before_listing(rows, listings)
    assert dropped.rows() == [("000010.SZ", D1)]
    assert kept.height == 2


def test_an_outage_does_not_carry_a_status_onto_a_session_before_listing():
    sessions = [date(2026, 9, 21), date(2026, 9, 22), D1]
    history = pl.DataFrame(
        [
            _status("000010.SZ", sessions[0], "eastmoney"),
            _status("000001.SZ", sessions[0], "eastmoney"),
            _status("000010.SZ", sessions[1], "eastmoney"),
            _status("000001.SZ", sessions[1], "eastmoney"),
        ],
        schema=DATASET_SCHEMAS["trading_status"],
    )
    listings = pl.DataFrame(
        {"symbol": ["000010.SZ"], "list_date": [D2], "first_traded": [None]},
        schema_overrides={"first_traded": pl.Date},
    )
    _, carried = plan_status_outages(history, sessions, D1, D1, {}, listings)
    assert carried["symbol"].to_list() == ["000001.SZ"]
