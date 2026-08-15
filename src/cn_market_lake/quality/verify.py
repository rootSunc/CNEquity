"""Coverage verification — what the lake *should* hold against what it does.

``cml audit`` answers "is the data that landed correct". This answers the other
half: "did the data that should have landed, land at all". They are different
failure modes and the second one had no home. Every defect this session that
ran for weeks unnoticed was of the second kind — a step raising on contact,
the run recording a failed batch, and nothing ever summing those up into
"``share_unlock_schedule`` has not succeeded since the 3rd".

Four gap kinds, deliberately distinguished because only some are faults:

``empty``    the dataset has no rows at all.
``stale``    its freshest date lags the anchor past ``max_staleness_days``.
``interior`` trading days inside its own span with nothing in them.
``shallow``  its history starts later than the source would actually serve.

The third is the one that needs care. A hole is only a fault on a dataset whose
semantics promise a row per session — ``by_date`` on a daily cadence. A
snapshot dataset *cannot* be given a day nobody ran, because replaying it would
forge rows, and a quarterly one legitimately has nothing on most sessions. That
distinction already exists as ``_gap_meaning`` on the dashboard; this reuses the
same rule rather than inventing a second, differently-wrong one.

Nothing here writes. ``repair_command`` returns the command that would close a
gap, and the CLI decides whether to run it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

from cn_market_lake.config import Config
from cn_market_lake.domain.datasets import DATASETS, DatasetSpec, is_stale
from cn_market_lake.query.parquet_scan import (
    dataset_has_parquet,
    list_hive_partition_dates,
    uses_hive_partitions,
)

logger = logging.getLogger(__name__)

# Interior gaps are reported as a bounded sample: a lake missing a year of
# sessions should say so in one line, not ten thousand.
_MAX_GAP_SAMPLE = 10


@dataclass(frozen=True)
class Gap:
    """One coverage shortfall, and whether anything can be done about it."""

    dataset: str
    kind: str
    detail: str
    repairable: bool
    start: date | None = None
    end: date | None = None
    missing_days: int = 0
    sample: tuple[date, ...] = ()

    def repair_command(self, config_path: str) -> str | None:
        if not self.repairable:
            return None
        cmd = f"cml backfill {self.dataset} --config {config_path}"
        if self.start is not None:
            cmd += f" --start {self.start.isoformat()}"
        if self.end is not None:
            cmd += f" --end {self.end.isoformat()}"
        return cmd


def _is_daily_by_date(spec: DatasetSpec) -> bool:
    """Whether a missing session on this dataset is honestly a fault.

    Mirrors ``serve.lake._gap_meaning``. Kept as one predicate so the dashboard
    grid and this command cannot disagree about what red means.
    """
    return spec.fetch_semantics == "by_date" and spec.max_staleness_days <= 1


def _backfillable(spec: DatasetSpec) -> bool:
    """Whether ``cml backfill`` will accept this dataset at all.

    Same gate the CLI applies: a snapshot dataset can only be replayed when a
    dedicated historical source is registered for it.
    """
    return spec.fetch_semantics == "by_date" or spec.backfill_source is not None


def _effective_anchor(spec: DatasetSpec, anchor: date) -> date:
    """The date this dataset can actually be current to.

    For a retired feed that is the last session it ever published — measuring
    it against today would report a permanent gap and offer a backfill that
    writes nothing, which is the same wrong answer twice.
    """
    retired = spec.source_retired_date
    if retired is not None and retired < anchor:
        return retired
    return anchor


def _dataset_root(config: Config, spec: DatasetSpec):
    root = config.derived_root if spec.layer == "derived" else config.curated_root
    return root / spec.name


def _covered_days(config: Config, spec: DatasetSpec) -> list[date]:
    """Day-partition dates present on disk, or [] when not day-partitioned.

    Read from directory names rather than by scanning rows: an interior-gap
    check that had to open 6,000 parquet files to answer would not be run.
    Only day granularity can be answered this way, which is also the only
    granularity where "this session is missing" is a well-posed question.
    """
    if spec.partition_col is None or spec.partition_granularity != "day":
        return []
    root = _dataset_root(config, spec)
    if not uses_hive_partitions(root, spec.partition_col):
        return []
    return list_hive_partition_dates(root, spec.partition_col)


def _trading_days(config: Config, start: date, end: date) -> list[date]:
    from cn_market_lake.steps.common import list_trading_dates

    if start > end:
        return []
    return list_trading_dates(config, start, end)


def verify_dataset(
    config: Config,
    spec: DatasetSpec,
    *,
    anchor: date,
    watermark: date | None,
) -> list[Gap]:
    """Coverage gaps for one dataset. Read-only."""
    gaps: list[Gap] = []
    root = _dataset_root(config, spec)
    repairable = _backfillable(spec)
    anchor = _effective_anchor(spec, anchor)

    if not dataset_has_parquet(root):
        # An optional dataset with nothing in it is a configuration choice, not
        # a gap — minute bars are off by default and saying otherwise every run
        # is how a report gets ignored.
        if spec.required:
            gaps.append(
                Gap(
                    dataset=spec.name,
                    kind="empty",
                    detail="no rows at all",
                    repairable=repairable,
                )
            )
        return gaps

    days = _covered_days(config, spec)
    first = min(days) if days else None
    last = max(days) if days else None

    # --- stale head ---------------------------------------------------------
    mark = watermark or last
    if mark is not None and is_stale(spec.name, mark, anchor):
        gaps.append(
            Gap(
                dataset=spec.name,
                kind="stale",
                detail=(
                    f"freshest {mark.isoformat()} vs anchor {anchor.isoformat()} "
                    f"(tolerance {spec.max_staleness_days}d)"
                ),
                repairable=repairable,
                start=mark,
                end=anchor,
            )
        )

    if not days:
        return gaps

    # --- interior holes -----------------------------------------------------
    if _is_daily_by_date(spec) and first is not None and last is not None:
        present = set(days)
        expected = _trading_days(config, first, last)
        missing = [d for d in expected if d not in present]
        if missing:
            gaps.append(
                Gap(
                    dataset=spec.name,
                    kind="interior",
                    detail=(
                        f"{len(missing)} trading day(s) inside "
                        f"{first.isoformat()}..{last.isoformat()} have no partition"
                    ),
                    repairable=repairable,
                    start=min(missing),
                    end=max(missing),
                    missing_days=len(missing),
                    sample=tuple(missing[:_MAX_GAP_SAMPLE]),
                )
            )

    # --- shallow history ----------------------------------------------------
    # Only against what the *source* would serve. Reporting "you could have
    # 2001" for a dataset whose vendor keeps 95 sessions would be noise, and
    # `earliest_available` is exactly that limit.
    floor = spec.earliest_available(anchor)
    if repairable and floor is not None and first is not None and first > floor:
        gaps.append(
            Gap(
                dataset=spec.name,
                kind="shallow",
                detail=(
                    f"starts {first.isoformat()}; the source serves back to ~{floor.isoformat()}"
                ),
                repairable=True,
                start=floor,
                end=first,
            )
        )

    return gaps


def verify_lake(
    config: Config,
    *,
    anchor: date,
    datasets: list[str] | None = None,
) -> list[Gap]:
    """Coverage gaps across the lake, ordered by dataset name. Read-only."""
    from cn_market_lake.storage.state import StateStore

    state = StateStore(config.meta_root)
    names = datasets or sorted(DATASETS)
    out: list[Gap] = []
    for name in names:
        spec = DATASETS.get(name)
        if spec is None:
            logger.warning("verify: unknown dataset %r; skipping", name)
            continue
        watermark = state.get_date(name) if spec.watermark else None
        out.extend(verify_dataset(config, spec, anchor=anchor, watermark=watermark))
    return out
