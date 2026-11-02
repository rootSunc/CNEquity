"""Coverage verification — what the lake *should* hold against what it does.

``cne audit`` answers "is the data that landed correct". This answers the other
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

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from cnequity.config import Config
from cnequity.domain.datasets import (
    DATASETS,
    DERIVE_COMMAND_DATASETS,
    DatasetSpec,
    history_mode_for,
    is_dataset_enabled,
    is_stale,
    staleness_counts_trading_sessions,
)
from cnequity.query.parquet_scan import (
    dataset_has_parquet,
    list_partitions,
    partition_dir,
    scan_parquet_root,
    uses_hive_partitions,
)
from cnequity.storage.observation import freshness_mark

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
        # Derived datasets are rebuilt from curated inputs; there is no
        # registered ingestion step named ``adj_factors`` or
        # ``industry_index`` for ``cne backfill`` to invoke.
        if self.dataset in DERIVE_COMMAND_DATASETS:
            cmd = f"cne derive {self.dataset} --config {config_path}"
            if self.dataset == "industry_index":
                if self.start is not None:
                    cmd += f" --start {self.start.isoformat()}"
                if self.end is not None:
                    cmd += f" --end {self.end.isoformat()}"
            return cmd
        cmd = f"cne backfill {self.dataset} --config {config_path}"
        if self.start is not None:
            cmd += f" --start {self.start.isoformat()}"
        if self.end is not None:
            cmd += f" --end {self.end.isoformat()}"
        return cmd


def _is_daily_by_date(spec: DatasetSpec) -> bool:
    """Whether a missing session on this dataset is honestly a fault.

    ``fetch_semantics="by_date"`` only says that a request is keyed by date;
    announcements, corporate actions, and other event feeds are still sparse.
    The registry's explicit coverage mode prevents those datasets from being
    reported as missing on every quiet trading day.
    """
    return spec.coverage_mode == "session_dense"


def _backfillable(spec: DatasetSpec) -> bool:
    """Whether ``cne backfill`` will accept this dataset at all.

    Same gate the CLI applies: a snapshot dataset can only be replayed when a
    dedicated historical source is registered for it.
    """
    # ``snapshot_only`` is an explicit data-contract boundary: replaying a
    # current-state page against an older date would manufacture point-in-time
    # values. Keep this guard here (rather than only in the CLI) so every
    # caller of verify_lake receives the same honest repairability flag.
    if history_mode_for(spec) == "snapshot_only":
        return False
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


def _nonempty_day_partition_dates(root, partition_col: str) -> list[date]:
    """Return day partitions whose Parquet footer contains at least one row."""
    covered: list[date] = []
    for partition in list_partitions(root, partition_col):
        if partition.start != partition.end:
            continue
        files = sorted(partition_dir(root, partition_col, partition.value).rglob("*.parquet"))
        has_rows = False
        for path in files:
            try:
                if int(pq.ParquetFile(path).metadata.num_rows) > 0:
                    has_rows = True
                    break
            except (OSError, pa.ArrowException, ValueError) as exc:
                logger.warning(
                    "verify: skipping unreadable file %s under %s: %s",
                    path,
                    root,
                    exc,
                )
        if has_rows:
            covered.append(partition.start)
    return covered


def _covered_days(config: Config, spec: DatasetSpec) -> list[date]:
    """Session dates present on disk for a dense dataset.

    Read from directory names plus Parquet footers rather than scanning rows:
    an interior-gap check that had to decode 6,000 parquet files to answer
    would not be run.
    Coarser dense layouts (for example yearly ``index_bars``) fall back to a
    date-column-only scan; sparse datasets intentionally return no dates here.
    """
    if spec.partition_col is None or not _is_daily_by_date(spec):
        return []
    root = _dataset_root(config, spec)
    if spec.name in {"market_breadth", "industry_index"}:
        return _complete_derived_days(config, spec)
    if (
        spec.name != "daily_bars"
        and spec.partition_granularity == "day"
        and uses_hive_partitions(root, spec.partition_col)
        and not list(root.glob("*.parquet"))
    ):
        return _nonempty_day_partition_dates(root, spec.partition_col)
    if not dataset_has_parquet(root):
        return []
    return _scanned_covered_days(config, spec, root)


def _scanned_covered_days(config: Config, spec: DatasetSpec, root) -> list[date]:
    """Dates present in a dense dataset, one partition at a time, cached.

    The key of every dense dataset includes its partition column, so a date's
    rows — and the dedupe that decides whether ``daily_bars`` traded that day —
    never span partitions. Decoding all 19M daily bars on every compact (~6 s)
    is therefore the same answer as decoding only the partitions whose files
    changed; see ``storage.partition_cache``.
    """
    from pathlib import Path

    from cnequity.domain.canonical import dedupe_lazy_by_primary_key
    from cnequity.query.parquet_scan import _scan_parquet_paths
    from cnequity.storage.partition_cache import cached_partition_values
    from cnequity.storage.revisions import _partition_of, resolve_committed_root

    committed = Path(resolve_committed_root(root, dataset=spec.name, meta_root=config.meta_root))
    partitions: dict[str, list] = {}
    for path in sorted(committed.rglob("*.parquet")):
        partitions.setdefault(_partition_of(path.relative_to(committed)), []).append(path)
    column = spec.partition_col
    traded_only = spec.name == "daily_bars"

    def dates_in(files: list) -> list[str]:
        lf = _scan_parquet_paths(files, hive=False)
        names = set(lf.collect_schema().names())
        if column not in names:
            return []
        if traded_only:
            if {"symbol", "trade_date"} <= names:
                lf = dedupe_lazy_by_primary_key(lf, "daily_bars")
            if "volume" in names:
                lf = lf.filter((pl.col("volume") > 0) | pl.col("volume").is_null())
        values = lf.select(column).drop_nulls().unique().collect()[column].to_list()
        return sorted(value.isoformat() for value in values)

    answers = cached_partition_values(
        config.meta_root, committed, f"covered_days-{spec.name}", partitions, dates_in
    )
    return sorted({date.fromisoformat(day) for days in answers.values() for day in days})


def traded_daily_bar_symbols(
    config: Config, root, start: date | None, end: date | None
) -> set[str]:
    """Symbols with a traded canonical daily bar in [start, end], from cached day answers.

    The ST coverage receipt asked this of every daily bar on every compact
    (~4.6 s). A day's answer depends only on that day's partition, as for
    `_scanned_covered_days`, so only partitions whose files changed are read.
    """
    from pathlib import Path

    from cnequity.domain.canonical import dedupe_lazy_by_primary_key
    from cnequity.domain.partitions import parse_partition
    from cnequity.query.parquet_scan import _scan_parquet_paths
    from cnequity.storage.partition_cache import cached_partition_values
    from cnequity.storage.revisions import _partition_of

    root = Path(root)
    partitions: dict[str, list] = {}
    for path in sorted(root.rglob("*.parquet")):
        part = _partition_of(path.relative_to(root))
        _, _, value = part.partition("=")
        period = parse_partition(value) if value else None
        if period is None or period.start != period.end:
            raise ValueError(f"daily_bars partition {part!r} is not one day")
        if (start is not None and period.end < start) or (end is not None and period.start > end):
            continue
        partitions.setdefault(part, []).append(path)

    def traded(files: list) -> list[str]:
        lf = _scan_parquet_paths(files, hive=False)
        names = set(lf.collect_schema().names())
        if "symbol" not in names:
            return []
        if {"symbol", "trade_date"} <= names:
            lf = dedupe_lazy_by_primary_key(lf, "daily_bars")
        if "volume" in names:
            lf = lf.filter((pl.col("volume") > 0) | pl.col("volume").is_null())
        return sorted(lf.select("symbol").unique().collect()["symbol"].drop_nulls().to_list())

    answers = cached_partition_values(
        config.meta_root, root, "traded_symbols-daily_bars", partitions, traded
    )
    return {symbol for symbols in answers.values() for symbol in symbols}


def _complete_derived_days(config: Config, spec: DatasetSpec) -> list[date]:
    """Return dates whose derived primary groups are structurally complete."""
    root = _dataset_root(config, spec)
    if not dataset_has_parquet(root):
        return []
    lf = scan_parquet_root(root, partition_col=spec.partition_col, hive=False)
    names = set(lf.collect_schema().names())
    if spec.name == "market_breadth":
        required = {"trade_date", "metric_id", "value"}
        if not required.issubset(names):
            return []
        valid = (
            pl.col("metric_id").is_in(
                [
                    "advance_count",
                    "decline_count",
                    "flat_count",
                    "limit_up_count",
                    "limit_down_count",
                    "advance_ratio",
                    "total_count",
                ]
            )
            & pl.col("value").is_not_null()
        )
        return sorted(
            lf.group_by("trade_date")
            .agg(
                pl.col("metric_id").filter(valid).n_unique().alias("metric_count"),
                pl.col("metric_id").filter(valid).len().alias("valid_row_count"),
                pl.len().alias("row_count"),
            )
            .filter(
                (pl.col("metric_count") == 7)
                & (pl.col("valid_row_count") == 7)
                & (pl.col("row_count") == 7)
            )
            .collect(engine="streaming")["trade_date"]
            .to_list()
        )
    required = {
        "trade_date",
        "industry_code",
        "level",
        "weighting",
        "n_members",
        "n_priced",
        "n_excluded",
    }
    if not required.issubset(names):
        return []
    groups = (
        lf.group_by("trade_date", "industry_code", "level")
        .agg(
            pl.col("weighting").n_unique().alias("weighting_count"),
            pl.len().alias("row_count"),
        )
        .filter((pl.col("weighting_count") != 2) | (pl.col("row_count") != 2))
        .select("trade_date")
        .unique()
    )
    all_days = lf.select("trade_date").drop_nulls().unique().collect(engine="streaming")
    if all_days.is_empty():
        return []
    incomplete = set(groups.collect(engine="streaming")["trade_date"].to_list())
    return sorted(day for day in all_days["trade_date"].to_list() if day not in incomplete)


def _trading_days(config: Config, start: date, end: date) -> list[date]:
    from cnequity.query.calendar import list_trading_dates

    if start > end:
        return []
    return list_trading_dates(config, start, end)


def last_contiguous_dense_date(
    config: Config,
    spec: DatasetSpec,
    *,
    start: date | None = None,
) -> date | None:
    """Newest session before the first hole in a dense dataset's span.

    A raw maximum is not a safe incremental watermark: if a fetch lands on
    Monday and Wednesday but misses Tuesday, starting the next fetch after
    Wednesday makes Tuesday permanently invisible. Return the last session in
    the continuous prefix instead. ``start`` allows a dataset whose legacy
    history predates its reliable incremental source to establish that prefix
    from the source's operational baseline. Sparse datasets deliberately return
    ``None`` because a missing session is not evidence of a defect there.
    """
    if not _is_daily_by_date(spec):
        return None
    days = _covered_days(config, spec)
    if not days:
        return None
    first = max(min(days), start) if start is not None else min(days)
    if first > max(days):
        return None
    expected = _trading_days(config, first, max(days))
    present = {day for day in days if day >= first}
    for index, session in enumerate(expected):
        if session not in present:
            return expected[index - 1] if index else None
    return expected[-1] if expected else None


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

    try:
        days = _covered_days(config, spec)
    except (OSError, pl.exceptions.PolarsError, ValueError) as exc:
        logger.warning("verify: %s is not readable: %s", spec.name, exc)
        gaps.append(
            Gap(
                dataset=spec.name,
                kind="unreadable",
                detail=f"curated data could not be read: {exc}",
                repairable=False,
            )
        )
        return gaps
    first = min(days) if days else None
    last = max(days) if days else None

    # --- stale head ---------------------------------------------------------
    # An event dataset is also as fresh as the last clean look at its source.
    mark = freshness_mark(config, spec.name, watermark or last)
    if mark is not None and is_stale(spec.name, mark, anchor, config):
        gaps.append(
            Gap(
                dataset=spec.name,
                kind="stale",
                detail=(
                    f"freshest {mark.isoformat()} vs anchor {anchor.isoformat()} "
                    f"(tolerance {spec.max_staleness_days} "
                    f"{'trading sessions' if staleness_counts_trading_sessions(spec.name) else 'calendar days'})"
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
    from cnequity.storage.state import StateStore

    state = StateStore(config.meta_root)
    names = datasets or sorted(DATASETS)
    out: list[Gap] = []
    for name in names:
        spec = DATASETS.get(name)
        if spec is None:
            logger.warning("verify: unknown dataset %r; skipping", name)
            continue
        if not is_dataset_enabled(name, config):
            logger.info("verify: %s is disabled in config; skipping coverage checks", name)
            continue
        watermark = state.get_date(name) if spec.watermark else None
        out.extend(verify_dataset(config, spec, anchor=anchor, watermark=watermark))
    return out
