"""Dataset-specific compact behaviour is registered, not branched inline."""

from __future__ import annotations

import inspect
import re

from cnequity.domain.datasets import DATASETS
from cnequity.steps import compact_hooks, finalize
from cnequity.storage import parquet, revisions


def test_hooks_name_registered_datasets():
    assert set(compact_hooks.COMPACT_HOOKS) <= set(DATASETS)
    assert set(parquet._STAGED_ROW_HOOKS) <= set(DATASETS)
    assert set(revisions._CARRIED_RECEIPT_METADATA) <= set(DATASETS)


def test_unregistered_datasets_get_no_hooks():
    hooks = compact_hooks.hooks_for("fund_flow")
    assert hooks.before_merge is hooks.after_merge is hooks.after_commit is None


_BRANCH = re.compile(r"\bds\s*(==|in)\s*[\"'{(]")


def test_the_publish_loop_names_no_dataset_but_instruments():
    """instruments has its own compactor; every other special case is a hook."""
    source = inspect.getsource(finalize._compact_locked)
    branches = [line.strip() for line in source.splitlines() if _BRANCH.search(line)]
    assert branches == ['if ds == "instruments":']


def test_staged_row_hooks_are_safe_per_partition():
    """A hook that needs the whole run must opt the dataset out of partition-wise merging."""
    for dataset in parquet._STAGED_ROW_HOOKS:
        whole_run = dataset in parquet._WHOLE_RUN_DATASETS
        assert whole_run or dataset in {
            "valuation_metrics",
            "index_constituents",
            "corporate_actions",
        }, f"review {dataset}'s hook for partition locality, then list it here"
