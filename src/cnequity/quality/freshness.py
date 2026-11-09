"""Whether a dataset in a real lake is due, not just by the registry's rule.

``domain.datasets.stale_between`` is the registry's tolerance rule and stays
pure. Judging a lake also needs two facts only the lake and the schedule hold:

* the last clean look at the source (``storage/observation.py``): an event
  dataset's newest row is its newest event, and a quiet window has none;
* the schedule's cadence (``orchestrator/cadence.py``): a history dataset a
  weekly group fetches is due through that group's last session, not today.

``cne status --datasets`` and the dashboard both label datasets with
``dataset_freshness`` against ``orchestrator.run_window.current_anchor``. Each used to keep its own
copy, and the copies drifted: a retired source read ``retired`` in one and
``fresh`` in the other.
"""

from __future__ import annotations

from datetime import date

from cnequity.domain.datasets import (
    DATASETS,
    daily_freshness_owed,
    empty_freshness_label,
    is_dataset_enabled,
    removed_from_schedule,
    stale_between,
    tushare_backfill_dataset,
)
from cnequity.orchestrator.cadence import freshness_anchor
from cnequity.storage.key_checks import key_rejected
from cnequity.storage.observation import freshness_mark


def freshness_owed(config, dataset: str) -> bool:
    """``daily_freshness_owed``, and not a Tushare dataset whose key was refused.

    A key the provider rejected cannot update anything, so its datasets'
    watermarks are not shown as stale. The verdict comes from the last key
    check (see ``cnequity.storage.key_checks``).
    """
    if not daily_freshness_owed(config, dataset):
        return False
    if config is not None and tushare_backfill_dataset(dataset):
        return not key_rejected(config, "tushare")
    return True


def is_stale(dataset: str, mark, anchor, config=None) -> bool:
    """Whether *dataset* lags in this lake (see the module docstring)."""
    if mark is None or anchor is None:
        return False
    if not freshness_owed(config, dataset):
        return False
    if config is not None and not removed_from_schedule(config, dataset):
        mark = freshness_mark(config, dataset, mark)
        anchor = freshness_anchor(config, dataset, anchor)
    return stale_between(dataset, mark, anchor, config)


def freshness_state(dataset: str, mark, anchor, config=None) -> str:
    """``"fresh"``, ``"stale"``, ``"n/a"`` or ``"removed"``.

    ``"n/a"`` is a Tushare backfill dataset this lake does not update daily
    (see ``freshness_owed``). ``"removed"`` is ``removed_from_schedule``.
    """
    if removed_from_schedule(config, dataset):
        return "removed"
    if not freshness_owed(config, dataset):
        return "n/a"
    return "stale" if is_stale(dataset, mark, anchor, config) else "fresh"


def dataset_freshness(config, row: dict, anchor: date | None) -> str:
    """One dataset's freshness label, from a ``list_datasets()`` row.

    ``fresh`` / ``stale`` / ``removed`` (see ``freshness_state``), ``n/a`` for a
    dataset not judged daily, ``empty`` or ``no source`` for one with no rows
    (see ``empty_freshness_label``), ``retired`` for a source that stopped
    publishing at ``source_retired_date``, and ``sample`` for the synthetic
    rows of a sample lake, whose dates mean nothing.
    """
    dataset = row["dataset"]
    profile = getattr(config, "lake_profile", None)
    if profile == "sample":
        return "sample" if row["has_data"] else "n/a"
    # A demo lake leaves most of the registry empty on purpose; calling those
    # empty-and-required painted a first-run lake red.
    if profile == "demo" and not row["has_data"]:
        return "n/a"
    # Optional Tushare backfills are not a daily obligation. Judged before the
    # empty label so a default install does not list them as sourceless.
    if not freshness_owed(config, dataset):
        return "n/a"
    if not row["has_data"]:
        return empty_freshness_label(dataset)
    if not is_dataset_enabled(dataset, config):
        return "n/a"
    # Datasets keyed by report_period have no daily watermark to judge.
    if not row["watermarked"]:
        return "n/a"
    mark = row["watermark"] or row["coverage_end"]
    # A retired source has nothing further to publish, so it is not stale, but
    # calling a 2024 watermark "fresh" reads as current data.
    spec = DATASETS.get(dataset)
    retired = getattr(spec, "source_retired_date", None) if spec else None
    if retired is not None and mark is not None and mark >= retired:
        return "retired"
    return freshness_state(dataset, mark, anchor, config)
