"""Repair runs on datasets that hold no data report it and register nothing."""

from datetime import date

import pytest

from cnequity.config import Config
from cnequity.storage.repairs.orphan_symbols import (
    repair_orphan_symbols,
    repair_stale_derived_suspensions,
)
from cnequity.storage.repairs.status_listing_window import repair_status_listing_window
from cnequity.storage.repairs.status_outages import repair_status_outages
from cnequity.storage.repairs.valuation_basis import repair_valuation_basis
from cnequity.storage.repairs.valuation_snapshots import repair_misdated_valuation

REPAIRS = {
    "orphan_symbols": repair_orphan_symbols,
    "stale_suspensions": repair_stale_derived_suspensions,
    "status_listing_window": repair_status_listing_window,
    "status_outages": lambda cfg, apply: repair_status_outages(
        cfg, start=date(2024, 1, 2), end=date(2024, 1, 10), apply=apply
    ),
    "misdated_valuation": repair_misdated_valuation,
    "valuation_basis": repair_valuation_basis,
}


def _registered(cfg: Config) -> list[str]:
    return sorted(p.parent.name for p in (cfg.meta_root / "revisions").glob("*/current.json"))


@pytest.mark.parametrize("apply", [False, True])
@pytest.mark.parametrize("name", sorted(REPAIRS))
def test_an_empty_lake_reports_nothing_to_repair_and_registers_no_revision(tmp_path, name, apply):
    cfg = Config(data_root=tmp_path)
    report = REPAIRS[name](cfg, apply=apply)
    assert report["applied"] is False
    notes = [report.get("note"), *(d.get("note") for d in report.get("datasets", {}).values())]
    assert any("nothing to repair" in (note or "") for note in notes)
    assert _registered(cfg) == []


def test_an_empty_dataset_directory_is_not_registered(tmp_path):
    cfg = Config(data_root=tmp_path)
    (cfg.curated_root / "trading_status" / "trade_date=2024-01").mkdir(parents=True)
    assert repair_status_listing_window(cfg)["note"].endswith("nothing to repair")
    assert _registered(cfg) == []
