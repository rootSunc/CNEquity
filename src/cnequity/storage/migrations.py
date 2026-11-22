"""One-off lake migrations, and the record of which ones a lake has had.

A release that fixes a writer often leaves rows the old writer stored. The fix
for those rows used to be a command per defect (`cne repair <name>`, marked
deprecated) or a script only a repository checkout had, and nothing remembered
which of them a lake had already been through: an operator had to read the
changelog of every release in between and work it out.

Now every such fix is a `Migration` in `MIGRATIONS`, and ``meta/lake.json``
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
from typing import Any

from cnequity.i18n import N_

# The lake record lives below the migrations that write to it: layout stamps
# a new lake without importing the repairs these migrations run.
from cnequity.storage.lake_record import (  # noqa: F401 — re-exported
    FORMAT,
    LAKE_FILE,
    _parse,
    _write_lake,
    affected,
    lake_path,
    package_version,
    read_lake,
    record_migration,
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
    #: An optional migration is never owed: it runs only when named with ``--only``.
    optional: bool = False

    def apply(self, config, *, apply: bool) -> dict:
        return self.run(config, apply=apply)


def _repair(module: str, name: str, **kwargs: Any) -> Callable[..., dict]:
    def run(config, *, apply: bool) -> dict:
        import importlib

        return getattr(importlib.import_module(f"cnequity.storage.repairs.{module}"), name)(
            config, apply=apply, **kwargs
        )

    return run


def _lake_catalog(config, *, apply: bool) -> dict:
    from cnequity.storage.catalog_migration import migrate_to_catalog

    return migrate_to_catalog(config, apply=apply)


def _revision_records(config, *, apply: bool) -> dict:
    from cnequity.storage.revision_records import import_receipts

    return import_receipts(config, apply=apply)


def _attempts_ledger(config, *, apply: bool) -> dict:
    from datetime import date

    from cnequity.domain.datasets import DATASETS
    from cnequity.quality.planned_coverage import (
        entity_seeds,
        record_cell_answers,
        record_seeds,
        seed_rechecks,
        seeds,
    )

    found = seeds(config, date.today())
    per_entity = entity_seeds(config, date.today())
    entity_cells = sum(len(cells) for cells in per_entity.values())
    report = {
        "applied": apply,
        "datasets": len({seed.dataset for seed in found} | set(per_entity)),
        "cells": sum(len(seed.periods) for seed in found),
        "entity_cells": {name: len(cells) for name, cells in sorted(per_entity.items())},
        # Ledger rows it adds: what `cne migrate` counts as affected.
        "rows_changed": sum(len(seed.periods) for seed in found) + entity_cells,
        "by_reason": {
            reason: sum(len(seed.periods) for seed in found if seed.reason == reason)
            for reason in sorted({seed.reason for seed in found})
        },
    }
    if apply and found:
        report["snapshot"] = record_seeds(config, found)
    if apply:
        for name, cells in per_entity.items():
            record_cell_answers(config, name, cells, run="seed:meta-state:answered")
        # Where each tiered feed last swept deep and tail, so the first run
        # after the switch does not sweep deep only because nothing is on record.
        report["rechecks"] = seed_rechecks(config, sorted(DATASETS))
    return report


def _legacy_datasets(config, *, apply: bool) -> dict:
    from cnequity.storage.legacy_datasets import adopt_legacy_datasets

    return adopt_legacy_datasets(config, apply=apply)


def _explicit_holds(config, *, apply: bool) -> dict:
    from cnequity.storage.explicit_holds import release_reference_holds

    return release_reference_holds(config, apply=apply)


def _drop_generations(config, *, apply: bool) -> dict:
    from cnequity.storage.drop_generations import drop_generations

    return drop_generations(config, apply=apply)


def _missing_values(dataset: str) -> Callable[..., dict]:
    def run(config, *, apply: bool) -> dict:
        from cnequity.storage.repairs.missing_values import repair_missing_values

        return repair_missing_values(config, dataset, apply=apply)

    return run


#: In the order they are applied. Each was a `cne repair` command for lakes written
#: by 0.17 or earlier; `status-outages` stays a manual repair, since only the
#: operator knows the outage window it rebuilds.
MIGRATIONS: tuple[Migration, ...] = (
    # First: the migrations after it are repairs, and nothing writes to a lake
    # that has not moved onto its catalog.
    Migration(
        "lake-catalog",
        N_(
            "Register every published dataset in the lake catalog, verify it row for row, "
            "and switch the lake to publishing there (one-way)"
        ),
        "0.25.0",
        _lake_catalog,
    ),
    # Second: the revision records the catalog then reads from.
    Migration(
        "revision-records",
        N_(
            "Record every published revision's receipt in the lake catalog, "
            "which revision reads then come from"
        ),
        "0.25.0",
        _revision_records,
    ),
    # Third: folders an earlier release read without a revision (the sample
    # lake, the demo lake's instruments), so the repairs below see their rows.
    Migration(
        "legacy-datasets",
        N_(
            "Publish into the lake catalog the dataset folders an earlier release wrote "
            "without a revision, then delete those folders"
        ),
        "0.25.0",
        _legacy_datasets,
    ),
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
        "vendor-zero-nulls",
        N_(
            "Turn vendor zero placeholders into nulls, and convertible bond prices on "
            "sessions without a trade"
        ),
        "0.23.0",
        _repair("missing_values", "repair_all_missing_values"),
    ),
    Migration(
        "attempts-ledger",
        N_(
            "Record in the lake catalog what meta/state and the run manifest say was asked "
            "of each source and found empty or incomplete, for the coverage planner"
        ),
        "0.25.0",
        _attempts_ledger,
    ),
    # Before drop-generations, which keeps every held generation.
    Migration(
        "explicit-holds",
        N_(
            "Release the holds an earlier release derived from revision ids found in files "
            "under meta/ and the registered notes; manual holds and cases stay"
        ),
        "0.25.0",
        _explicit_holds,
    ),
    # Last: nothing above reads the directories it deletes, and it cannot be undone.
    Migration(
        "drop-generations",
        N_(
            "Delete the curated/ and derived/ directories and the generations, pointers and "
            "receipts under meta/revisions that the lake catalog replaced; held revisions stay"
        ),
        "0.25.0",
        _drop_generations,
    ),
    # After every migration that reads meta/state (attempts-ledger seeds from it).
    Migration(
        "meta-layout",
        N_(
            "Move what is left in meta/state to each record's own directory under meta/, "
            "and drop the coverage ledger and revision copies the lake catalog replaced"
        ),
        "0.25.0",
        _repair("meta_layout", "repair_meta_layout"),
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
        and not migration.optional
        and (created is None or _parse(created) < _parse(migration.introduced_in))
    ]


def record(config, migration: Migration, report: dict) -> None:
    """Note in ``lake.json`` that *migration* ran here, applied or found nothing to do."""
    record_migration(config, migration.id, report)
