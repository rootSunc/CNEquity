"""How a dataset's values are read: one rule set per dataset, kept in the registry.

Vendors mark "no value" in-band. Tushare sends ``0`` for a convertible bond's
open on a session it did not trade, for the price of a THS popularity entry
that has none, and for both limit prices of a stock it has no limits for.
Stored as numbers those zeros are wrong answers, not missing ones: a return
across a ``close`` of 0 is -100%.

Each adapter used to decide this for itself, and most did not, so the defect
reached the lake wherever a new dataset forgot. The rule now lives on the
dataset's registry entry (``DatasetSpec.value_rules``) and is applied in one
place, ``validate_dataframe``, which every writer, compaction, repair and
reader passes through. An adapter that needs the normalized values before its
own later logic calls :func:`apply_value_rules` with the same dataset name.

The same declarations drive the value checks: ``non_null`` columns must hold a
value on every stored row, and a ``market_bar`` dataset's rows must satisfy
the price/volume invariants in :func:`cnequity.domain.schemas.market_bar_violations`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeVar

import polars as pl

Frame = TypeVar("Frame", pl.DataFrame, pl.LazyFrame)


@dataclass(frozen=True)
class ValueRules:
    #: Columns where a vendor ``0`` means "no value"; stored as null.
    zero_is_missing: tuple[str, ...] = ()
    #: Limit ``zero_is_missing`` to rows from these sources; empty means every
    #: source. Index weights: EastMoney and CNI send 0 for "unknown", while
    #: another source's 0 can be a real weight.
    zero_is_missing_sources: tuple[str, ...] = ()
    #: Columns that are placeholders on a row whose ``volume`` is 0 (no trade
    #: that session); stored as null so the row records the session without
    #: inventing a price.
    no_trade_missing: tuple[str, ...] = ()
    #: Columns that must hold a value on every stored row, beyond the primary
    #: key and provenance.
    non_null: tuple[str, ...] = ()
    #: Rows are market bars: prices positive, volume and amount non-negative,
    #: and a traded row's high/low enclose its open and close.
    market_bar: bool = False

    @property
    def rewrites(self) -> bool:
        """Whether the rules can change a stored value."""
        return bool(self.zero_is_missing or self.no_trade_missing)


NO_RULES = ValueRules()


def rules_for(dataset: str) -> ValueRules:
    from cnequity.domain.datasets import DATASETS

    spec = DATASETS.get(dataset)
    return spec.value_rules if spec is not None else NO_RULES


def placeholder_predicates(dataset: str, columns: list[str] | set[str]) -> dict[str, pl.Expr]:
    """Per column, the rows whose stored value is a placeholder under the rules.

    Only columns present in *columns* are named, so historical files that
    predate a column are handled. A predicate is never null.
    """
    rules = rules_for(dataset)
    present = set(columns)
    out: dict[str, pl.Expr] = {}
    if rules.zero_is_missing:
        scope = (
            pl.col("source").is_in(list(rules.zero_is_missing_sources))
            if rules.zero_is_missing_sources and "source" in present
            else pl.lit(not rules.zero_is_missing_sources)
        )
        for column in rules.zero_is_missing:
            if column in present:
                out[column] = (scope & (pl.col(column) == 0)).fill_null(False)
    if rules.no_trade_missing and "volume" in present:
        untraded = pl.col("volume") == 0
        for column in rules.no_trade_missing:
            if column in present:
                predicate = (untraded & pl.col(column).is_not_null()).fill_null(False)
                out[column] = out[column] | predicate if column in out else predicate
    return out


def apply_value_rules(frame: Frame, dataset: str) -> Frame:
    """*frame* with every placeholder its dataset's rules name turned into null."""
    columns = frame.collect_schema().names() if isinstance(frame, pl.LazyFrame) else frame.columns
    predicates = placeholder_predicates(dataset, columns)
    if not predicates:
        return frame
    return frame.with_columns(
        pl.when(predicate).then(None).otherwise(pl.col(column)).alias(column)
        for column, predicate in predicates.items()
    )


def placeholder_counts(frame: pl.LazyFrame, dataset: str) -> dict[str, int]:
    """How many stored values the rules would turn into null, per column."""
    predicates = placeholder_predicates(dataset, frame.collect_schema().names())
    if not predicates:
        return {}
    row = (
        frame.select(predicate.sum().alias(column) for column, predicate in predicates.items())
        .collect(engine="streaming")
        .row(0, named=True)
    )
    return {column: int(count) for column, count in row.items() if count}
