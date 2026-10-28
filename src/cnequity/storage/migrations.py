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
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from cnequity.i18n import N_
from cnequity.storage.atomic import write_json_atomic

LAKE_FILE = Path("state") / "lake.json"
FORMAT = 1


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
)


def package_version() -> str:
    try:
        return version("cnequity")
    except PackageNotFoundError:
        return "0.0.0"


def _parse(value: str) -> tuple[int, ...]:
    parts = []
    for piece in str(value).split("."):
        digits = "".join(ch for ch in piece if ch.isdigit())
        parts.append(int(digits) if digits else 0)
    return tuple(parts)


def lake_path(config) -> Path:
    return Path(config.meta_root) / LAKE_FILE


def read_lake(config) -> dict:
    """``lake.json``, or an empty record for a lake from before it existed."""
    import json

    path = lake_path(config)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _write_lake(config, record: dict) -> None:
    record.setdefault("format", FORMAT)
    write_json_atomic(lake_path(config), record, indent=2, sort_keys=True)


def stamp_new_lake(config) -> None:
    """Record the creating version on a lake that holds no data yet.

    Only an empty lake is stamped: an old lake re-running `cne init` must not
    claim this version built it, or every migration it still owes would vanish.
    """
    from cnequity.query.parquet_scan import dataset_has_parquet

    if lake_path(config).exists():
        return
    curated = Path(config.curated_root)
    if curated.is_dir() and any(dataset_has_parquet(p) for p in curated.iterdir() if p.is_dir()):
        return
    _write_lake(config, {"created_with": package_version(), "migrations": {}})


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
