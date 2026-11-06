"""One-off lake migrations, and the record of which ones a lake has had.

A release that fixes a writer often leaves rows the old writer stored. The fix
for those rows used to be a command per defect (`cne repair <name>`, marked
deprecated) or a script only a repository checkout had, and nothing remembered
which of them a lake had already been through: an operator had to read the
changelog of every release in between and work it out.

Now every such fix is a `Migration` in `MIGRATIONS`, and ``meta/state/lake.json``
records two things:

* ``created_with`` — the version that built the lake. A migration introduced in
  that version or earlier has nothing to fix there, so a new lake owes none.
* ``migrations`` — each migration this lake has applied, or checked and found
  nothing to do, with the version that did it.

`cne migrate` previews what is still owed and applies it with ``--apply``;
`cne doctor` says when something is owed. Each migration is a repair from
`cnequity.storage.repairs`: it takes the lake mutation lock, rewrites through
the repair gate and keeps the previous revision readable.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from cnequity.i18n import N_

# The lake record lives below the migrations that write to it: layout stamps
# a new lake without importing the repairs these migrations run.
from cnequity.storage.lake_record import (  # noqa: F401 — re-exported
    FORMAT,
    LAKE_FILE,
    _parse,
    _write_lake,
    lake_path,
    package_version,
    read_lake,
    stamp_new_lake,
)


@dataclass(frozen=True)
class Migration:
    id: str
    #: Catalog message (`N_`): what the migration fixes.
    title: str
    #: The first release whose writers no longer produce the defect.
    introduced_in: str
    run: Callable[..., dict]
    #: Catalog message (`N_`) naming the source it reads, or "" when it reads only the lake.
    reads: str = ""

    def apply(self, config, *, apply: bool) -> dict:
        return self.run(config, apply=apply)


def _repair(module: str, name: str, **kwargs: Any) -> Callable[..., dict]:
    def run(config, *, apply: bool) -> dict:
        import importlib

        return getattr(importlib.import_module(f"cnequity.storage.repairs.{module}"), name)(
            config, apply=apply, **kwargs
        )

    return run


def _repartition(dataset: str) -> Callable[..., dict]:
    def run(config, *, apply: bool) -> dict:
        from cnequity.storage.repairs.repartition import RepartitionError, repartition_dataset

        try:
            result = repartition_dataset(config, dataset, dry_run=not apply)
        except RepartitionError:
            # No partitions yet: nothing laid out the old way to rewrite.
            return {"dataset": dataset, "rows_changed": 0}
        stale = result.changed or result.partitions_after != result.partitions_before
        return {
            "dataset": dataset,
            "rows_changed": result.rows if stale else 0,
            "files": f"{result.files_before} -> {result.files_after}",
        }

    return run


def _missing_values(dataset: str) -> Callable[..., dict]:
    def run(config, *, apply: bool) -> dict:
        from cnequity.storage.repairs.missing_values import repair_missing_values

        return repair_missing_values(config, dataset, apply=apply)

    return run


#: In the order they are applied. Each was a `cne repair` command for lakes written
#: by 0.17 or earlier; `status-outages` stays a manual repair, since only the
#: operator knows the outage window it rebuilds.
MIGRATIONS: tuple[Migration, ...] = (
    Migration(
        "orphan-symbols",
        N_("Drop factor and action rows for securities the lake holds no prices for"),
        "0.18.0",
        _repair("orphan_symbols", "repair_orphan_symbols"),
    ),
    Migration(
        "stale-suspensions",
        N_("Drop inferred suspensions that later evidence contradicts"),
        "0.18.0",
        _repair("orphan_symbols", "repair_stale_derived_suspensions"),
    ),
    Migration(
        "status-listing-window",
        N_("Drop trading_status rows outside a security's listing window"),
        "0.18.0",
        _repair("status_listing_window", "repair_status_listing_window"),
    ),
    Migration(
        "missing-values-valuation",
        N_("Turn valuation zero placeholders into nulls"),
        "0.18.0",
        _missing_values("valuation_metrics"),
    ),
    Migration(
        "missing-values-index-weights",
        N_("Turn missing EastMoney and CNI index weights into nulls"),
        "0.18.0",
        _missing_values("index_constituents"),
    ),
    Migration(
        "valuation-basis",
        N_("Unify the P/E and market-cap bases of valuation rows"),
        "0.18.0",
        _repair("valuation_basis", "repair_valuation_basis"),
    ),
    Migration(
        "misdated-valuation",
        N_("Replace push2 valuation snapshots stored under the wrong date"),
        "0.18.0",
        _repair("valuation_snapshots", "repair_misdated_valuation"),
        N_("EastMoney datacenter, when there are rows to replace"),
    ),
    Migration(
        "financial-announcements",
        N_("Add financial statements rebuilt on their original disclosure dates"),
        "0.18.0",
        _repair("financial_announcements", "repair_financial_announcements"),
    ),
    Migration(
        "fund-factors",
        N_("Add the share conversions ETF/LOF factors were missing"),
        "0.18.0",
        _repair("fund_factors", "repair_fund_factors"),
    ),
    Migration(
        "fund-listing-dates",
        N_("Fill fund listing dates from the exchanges' official lists"),
        "0.18.0",
        _repair("fund_listing_dates", "repair_fund_listing_dates"),
        N_("the exchanges' official fund lists"),
    ),
    Migration(
        "daily-bar-coverage",
        N_("Settle Beijing whole-day and pre-listing daily-bar debts already proven"),
        "0.18.0",
        _repair("daily_bar_coverage", "repair_daily_bar_coverage"),
    ),
    Migration(
        "industry-levels",
        N_("Fill industry level columns on industry_members rows written before 0.20"),
        "0.20.0",
        _repair("industry_levels", "repair_industry_levels"),
        N_("EastMoney datacenter industry boards, when EastMoney rows need levels"),
    ),
    Migration(
        "sw-delisted",
        N_("Drop Shenwan industry rows dated on or after the stock's delisting"),
        "0.20.0",
        _repair("sw_delisted", "repair_sw_delisted"),
    ),
    Migration(
        "offshore-final",
        N_("Replace COMEX gold bars stored before their session closed"),
        "0.20.0",
        _repair("offshore_final", "repair_offshore_final"),
        N_("Sina's COMEX gold daily bars"),
    ),
    Migration(
        "sw-never-listed",
        N_("Drop Shenwan industry rows of securities that never listed"),
        "0.20.0",
        _repair("sw_never_listed", "repair_sw_never_listed"),
    ),
    Migration(
        "financial-comparatives",
        N_("Add statement items first seen only as a later filing's comparative on their own date"),
        "0.21.0",
        _repair("financial_announcements", "repair_financial_announcements"),
    ),
    Migration(
        "delist-dates",
        N_("Move delist dates stored as the removal date back to the last listed session"),
        "0.21.0",
        _repair("delist_dates", "repair_delist_dates"),
    ),
    Migration(
        "bar-listing-window",
        N_("Drop zero-volume daily bars dated after a security's delisting"),
        "0.21.0",
        _repair("bar_listing_window", "repair_bar_listing_window"),
    ),
    Migration(
        "daily-bars-month-partitions",
        N_("Rewrite daily_bars history from one file per day into one file per month"),
        "0.23.0",
        _repartition("daily_bars"),
    ),
    Migration(
        "adj-factors-month-partitions",
        N_("Rewrite adj_factors history from one file per day into one file per month"),
        "0.23.0",
        _repartition("adj_factors"),
    ),
    Migration(
        "margin-trading-month-partitions",
        N_("Rewrite margin_trading history from one file per day into one file per month"),
        "0.23.0",
        _repartition("margin_trading"),
    ),
    Migration(
        "valuation-metrics-month-partitions",
        N_("Rewrite valuation_metrics history from one file per day into one file per month"),
        "0.23.0",
        _repartition("valuation_metrics"),
    ),
    Migration(
        "limit-prices-month-partitions",
        N_("Rewrite limit_prices history from one file per day into one file per month"),
        "0.23.0",
        _repartition("limit_prices"),
    ),
    Migration(
        "fund-flow-tushare-month-partitions",
        N_("Rewrite fund_flow_tushare history from one file per day into one file per month"),
        "0.23.0",
        _repartition("fund_flow_tushare"),
    ),
    Migration(
        "chip-distribution-month-partitions",
        N_("Rewrite chip_distribution history from one file per day into one file per month"),
        "0.23.0",
        _repartition("chip_distribution"),
    ),
    Migration(
        "vendor-zero-nulls",
        N_(
            "Turn vendor zero placeholders into nulls, and convertible bond prices on "
            "sessions without a trade"
        ),
        "0.23.0",
        _repair("missing_values", "repair_all_missing_values"),
    ),
)


def owed(config) -> list[Migration]:
    """Migrations this lake has neither had nor been built too new to need."""
    record = read_lake(config)
    done = record.get("migrations") or {}
    created = record.get("created_with")
    return [
        migration
        for migration in MIGRATIONS
        if migration.id not in done
        and (created is None or _parse(created) < _parse(migration.introduced_in))
    ]


def affected(report: Any) -> int:
    """How many rows (or settled keys) a repair report says it touches.

    A report that separates what it read (``rows``) from what it rewrites
    (``rows_changed``) is counted by the latter.
    """
    if isinstance(report, dict) and isinstance(report.get("rows_changed"), int):
        return report["rows_changed"]
    if isinstance(report, dict):
        total = 0
        for key, value in report.items():
            if key in {"rows", "rows_dropped"} and isinstance(value, int):
                total += value
            elif key in {"resolved_sessions", "resolved_prelisting_keys"} and isinstance(
                value, list
            ):
                total += len(value)
            elif isinstance(value, dict):
                total += affected(value)
        return total
    return 0


def record(config, migration: Migration, report: dict) -> None:
    """Note in ``lake.json`` that *migration* ran here, applied or found nothing to do."""
    lake = read_lake(config)
    lake.setdefault("migrations", {})[migration.id] = {
        "status": "applied" if affected(report) else "not_needed",
        "affected": affected(report),
        "at": datetime.now(timezone.utc).isoformat(),
        "version": package_version(),
    }
    _write_lake(config, lake)
