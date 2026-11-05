"""Per-dataset steps of ``compact``, kept out of the generic publish loop.

``steps.finalize._compact_locked`` merges, gates and commits every staged
dataset the same way. A few datasets need one more thing at a fixed point of
that sequence — a cross-dataset contract before the merge, a cache to retire
after it, evidence to confirm once the generation is current. Those used to be
``if ds == "…"`` branches inside the loop; they are registered here instead,
so the loop names no dataset and a new special case is one entry, not another
branch in the publish path.

Each hook is the code that used to sit inline, moved as is.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from cnequity.config import Config


@dataclass(frozen=True)
class BeforeMerge:
    """What a pre-merge hook hands back to the compact loop."""

    #: Staged files still to merge (a hook may quarantine some).
    files: list[Path]
    findings: list[dict]


@dataclass(frozen=True)
class CompactHooks:
    #: ``(config, run_id, staged_files, revisions, committed_root, files) -> BeforeMerge``;
    #: ``staged_files(dataset)`` lists that dataset's publishable staging in this run.
    before_merge: Callable[..., BeforeMerge] | None = None
    #: ``(config, merged_files) -> None``, after the mutable candidate is written.
    after_merge: Callable[[Config, list[Path]], None] | None = None
    #: ``(config, dataset) -> None``, after the dataset's revision is committed.
    after_commit: Callable[[Config, str], None] | None = None


def _daily_bar_contracts(
    config: Config,
    run_id: str,
    staged_files: Callable[[str], list[Path]],
    revisions: Any,
    committed_root: Path,
    files: list[Path],
) -> BeforeMerge:
    # Rules that need the previous close or the corporate actions
    # can only be checked where staged rows meet the committed lake.
    from cnequity.storage.staged_contracts import enforce_daily_bar_contracts

    staged_action_files = staged_files("corporate_actions")
    staged_status_files = staged_files("trading_status")
    findings = enforce_daily_bar_contracts(
        config,
        run_id,
        files,
        committed_root=committed_root,
        actions_root=revisions.current_root("corporate_actions"),
        instruments_root=revisions.current_root("instruments"),
        status_root=revisions.current_root("trading_status"),
        staged_status=(
            pl.concat(
                [
                    pl.read_parquet(f).select("symbol", "trade_date", "status", "source")
                    for f in staged_status_files
                ],
                how="vertical_relaxed",
            )
            if staged_status_files
            else None
        ),
        staged_actions=(
            pl.concat(
                [pl.read_parquet(f).select("symbol", "ex_date") for f in staged_action_files],
                how="vertical_relaxed",
            )
            if staged_action_files
            else None
        ),
    )
    return BeforeMerge(files=[f for f in files if f.exists()], findings=list(findings))


def _retire_daily_bar_previews(config: Config, merged_files: list[Path]) -> None:
    if not merged_files:
        return
    from cnequity.research.preview import retire_preview_symbols

    try:
        published_symbols = (
            pl.concat(
                [
                    pl.read_parquet(path, columns=["symbol"])
                    for path in merged_files
                    if path.exists()
                ],
                how="vertical_relaxed",
            )
            .get_column("symbol")
            .unique()
            .to_list()
        )
    except (OSError, ValueError, pl.exceptions.PolarsError):
        published_symbols = []
    retire_preview_symbols(config, [str(symbol) for symbol in published_symbols])


def _confirm_derivative_receipts(config: Config, dataset: str) -> None:
    from cnequity.storage.derivative_evidence import confirm_receipts

    confirm_receipts(config, dataset)


COMPACT_HOOKS: dict[str, CompactHooks] = {
    "daily_bars": CompactHooks(
        before_merge=_daily_bar_contracts,
        after_merge=_retire_daily_bar_previews,
    ),
    "futures_bars": CompactHooks(after_commit=_confirm_derivative_receipts),
    "option_bars": CompactHooks(after_commit=_confirm_derivative_receipts),
}

_NO_HOOKS = CompactHooks()


def hooks_for(dataset: str) -> CompactHooks:
    return COMPACT_HOOKS.get(dataset, _NO_HOOKS)
