"""Repairs for rows the old outage fallbacks stamped onto sessions they never saw."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.query import load
from cnequity.storage.repairs.status_outages import repair_status_outages
from cnequity.storage.repairs.valuation_snapshots import repair_misdated_valuation
from cnequity.storage.revisions import RevisionStore

SESSIONS = [date(2026, 9, 23), date(2026, 9, 24), date(2026, 9, 28)]
OBSERVED = datetime(2026, 9, 23, 8, tzinfo=timezone.utc)  # 16:00 Beijing
OUTAGE_RUN = datetime(2026, 9, 24, 8, tzinfo=timezone.utc)


def _commit(cfg: Config, dataset: str, layer) -> None:
    RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root).commit(
        dataset,
        run_id="seed",
        changed_files=sorted((layer / dataset).rglob("*.parquet")),
        schema_version=1,
        contract_fingerprint="contract",
    )


def _calendar(cfg: Config) -> None:
    path = cfg.curated_root / "trading_calendar" / "part-merged.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"trade_date": SESSIONS, "is_trading": [True] * len(SESSIONS)}).write_parquet(path)


def _status(symbol, day, source, status="normal", risk=False, fetched=OBSERVED):
    return {
        "symbol": symbol,
        "trade_date": day,
        "is_trading": status == "normal",
        "status": status,
        "risk_warning": risk,
        "source": source,
        "data_version": "v1",
        "fetched_at": fetched,
    }


@pytest.fixture
def status_lake(tmp_path, settle) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    _calendar(cfg)
    d1, d2, d3 = SESSIONS
    rows = [
        _status("000001.SZ", d1, "eastmoney"),
        _status("000002.SZ", d1, "eastmoney", status="suspended", risk=True),
        _status("000003.SZ", d1, "eastmoney"),
        _status("600000.SH", d1, "eastmoney"),
        # The outage: SH's own board answered, SZ came from the old cache,
        # which invented a halt for a name it had never seen.
        _status("600000.SH", d2, "exchange", fetched=OUTAGE_RUN),
        _status("000001.SZ", d2, "eastmoney_cached", status="suspended", fetched=OUTAGE_RUN),
        _status("000009.SZ", d2, "eastmoney_cached", status="suspended", fetched=OUTAGE_RUN),
        # A healthy day on which the board stopped listing 000003.SZ.
        _status("000001.SZ", d3, "eastmoney"),
        _status("000002.SZ", d3, "eastmoney", status="suspended", risk=True),
        _status("600000.SH", d3, "eastmoney"),
    ]
    part = cfg.curated_root / "trading_status" / "trade_date=2026-09"
    part.mkdir(parents=True)
    pl.DataFrame(rows, schema=DATASET_SCHEMAS["trading_status"]).write_parquet(
        part / "part-0.parquet"
    )
    _commit(cfg, "trading_status", cfg.curated_root)
    return settle(cfg)


def test_status_plan_changes_nothing(status_lake):
    plan = repair_status_outages(status_lake, start=SESSIONS[1], end=SESSIONS[2])

    assert plan["applied"] is False
    assert plan["by_date"] == {"2026-09-24": {"dropped": 2, "carried": 3}}
    assert load("trading_status", config=status_lake).height == 10


def test_status_outage_replays_the_first_hand_carry(status_lake):
    report = repair_status_outages(status_lake, start=SESSIONS[1], end=SESSIONS[2], apply=True)

    assert report["applied"] is True
    day = (
        load("trading_status", config=status_lake, start=SESSIONS[1], end=SESSIONS[1])
        .sort("symbol")
        .select("symbol", "status", "risk_warning", "source", "fetched_at")
        .rows()
    )
    assert day == [
        ("000001.SZ", "normal", False, "eastmoney_cached", OBSERVED),
        ("000002.SZ", "suspended", True, "eastmoney_cached", OBSERVED),
        ("000003.SZ", "normal", False, "eastmoney_cached", OBSERVED),
        ("600000.SH", "normal", False, "exchange", OUTAGE_RUN),
    ]
    healthy = load("trading_status", config=status_lake, start=SESSIONS[2], end=SESSIONS[2])
    assert "000003.SZ" not in healthy.get_column("symbol").to_list()
    old = load("trading_status", config=status_lake, revision=1)
    assert "000009.SZ" in old.get_column("symbol").to_list()


def test_a_carried_row_yields_to_a_session_record(status_lake, settle):
    settle.on_working_tree()
    repair_status_outages(status_lake, start=SESSIONS[1], end=SESSIONS[2], apply=True)
    part = status_lake.curated_root / "trading_status" / "trade_date=2026-09"
    later = datetime(2026, 10, 10, tzinfo=timezone.utc)
    pl.DataFrame(
        [_status("000001.SZ", SESSIONS[1], "derived_bar_gap", status="suspended", fetched=later)],
        schema=DATASET_SCHEMAS["trading_status"],
    ).write_parquet(part / "part-derived.parquet")
    _commit(status_lake, "trading_status", status_lake.curated_root)

    row = load("trading_status", config=status_lake, start=SESSIONS[1], end=SESSIONS[1]).filter(
        pl.col("symbol") == "000001.SZ"
    )
    assert row.select("status", "source").rows() == [("suspended", "derived_bar_gap")]


# ---- valuation_metrics ---------------------------------------------------------

LIVE_READ = datetime(2026, 7, 26, 14, 48, tzinfo=timezone.utc)  # Sunday night, Beijing


def _valuation(symbol, day, source, fetched, pe=None):
    return {
        "symbol": symbol,
        "trade_date": day,
        "pe_ttm": pe,
        "pb": 1.0,
        "ps_ttm": 1.0,
        "total_mv": 1e9,
        "float_mv": 1e9,
        "pe_dynamic": 9.0 if source == "eastmoney" else None,
        "total_mv_basis": "vendor_reported",
        "float_mv_basis": "vendor_reported",
        "shares_as_of": None,
        "source": source,
        "data_version": "v1",
        "fetched_at": fetched,
    }


@pytest.fixture
def valuation_lake(tmp_path, settle) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    stamped, live = date(2026, 7, 16), date(2026, 7, 24)
    partitions = {
        stamped: [
            _valuation("600519.SH", stamped, "eastmoney", LIVE_READ),
            _valuation("000001.SZ", stamped, "eastmoney", LIVE_READ),
            _valuation("600000.SH", stamped, "baostock", LIVE_READ, pe=5.0),
        ],
        live: [_valuation("600519.SH", live, "eastmoney", LIVE_READ)],
    }
    for day, rows in partitions.items():
        part = cfg.curated_root / "valuation_metrics" / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True)
        pl.DataFrame(rows, schema=DATASET_SCHEMAS["valuation_metrics"]).write_parquet(
            part / "part-0.parquet"
        )
    _commit(cfg, "valuation_metrics", cfg.curated_root)
    return settle(cfg)


def _datacenter(start, end):
    assert (start, end) == (date(2026, 7, 16), date(2026, 7, 16))
    return pl.DataFrame(
        {
            "symbol": ["600519.SH", "300750.SZ"],
            "trade_date": [date(2026, 7, 16)] * 2,
            "pe_ttm": [19.25, 30.0],
            "pb": [7.0, 5.0],
            "ps_ttm": [9.0, 4.0],
            "total_mv": [2e12, 1e12],
            "float_mv": [2e12, 1e12],
            "total_mv_basis": ["vendor_reported"] * 2,
            "float_mv_basis": ["vendor_reported"] * 2,
        }
    )


def test_valuation_plan_finds_only_the_misdated_snapshot(valuation_lake):
    plan = repair_misdated_valuation(
        valuation_lake, fetch=lambda *a: pytest.fail("a plan must not fetch")
    )
    assert plan == {"applied": False, "rows": 2, "by_date": {"2026-07-16": 2}}


def test_valuation_rows_are_replaced_by_the_dated_report(valuation_lake):
    report = repair_misdated_valuation(valuation_lake, apply=True, fetch=_datacenter)

    assert report["rows_replaced"] == 1
    assert report["rows_without_replacement"] == 1
    stamped = (
        load(
            "valuation_metrics",
            config=valuation_lake,
            start=date(2026, 7, 16),
            end=date(2026, 7, 16),
        )
        .sort("symbol")
        .select("symbol", "source", "pe_ttm")
        .rows()
    )
    assert stamped == [
        ("600000.SH", "baostock", 5.0),
        ("600519.SH", "eastmoney_datacenter", 19.25),
    ]
    live = load(
        "valuation_metrics", config=valuation_lake, start=date(2026, 7, 24), end=date(2026, 7, 24)
    )
    assert live.select("symbol", "source").rows() == [("600519.SH", "eastmoney")]
