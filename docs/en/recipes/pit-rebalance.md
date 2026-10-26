---
description: Point-in-time financial statements for China A-shares: build rebalance cross-sections by announcement date and avoid look-ahead bias in backtests.
---

# PIT rebalance cross-section

The most common mistake in financial statement research is answering a past rebalance question with revised values seen today. `financial_statement_items` puts `announce_date` in the version key, and strict mode additionally requires that publication, availability and in-lake observation evidence all fall no later than the cutoff date.

## Prepare the data and expected results

Use the production configuration `configs/cnequity.toml`, with `financial_statement_items` already collected. The basic demo does not include financial statements. The dates below are examples; replace them with rebalance dates that your evidence actually covers: 2023 financial statements backfilled today do not automatically become a version observed in 2024, so an empty strict read is a reasonable result.

## 1. Read the facts visible on the rebalance date

```python
from pathlib import Path

import polars as pl

from cnequity.config import load_config
from cnequity.query import load

cfg = load_config(Path("configs/cnequity.toml"))
rebalance_date = "2024-04-30"

facts = load(
    "financial_statement_items",
    items=["roe", "revenue"],
    as_of=rebalance_date,
    pit_mode="strict",
    config=cfg,
)

if facts.is_empty():
    raise RuntimeError("no verifiable PIT facts for this date; check coverage and observation time first")

# For each (symbol, report_period, item_code), keep only the version in effect at as_of.
screen = (
    facts.filter(pl.col("report_period") == "2023Q4")
    .pivot(
        on="item_code",
        index=["symbol", "report_period"],
        values="item_value",
        aggregate_function="last",
    )
    .filter(pl.col("roe") > 0)
)
print(screen.select(["symbol", "roe", "revenue"]))
```

If you want to study the revisions themselves rather than build a rebalance cross-section, turn on `all_vintages=True` explicitly:

```python
revisions = load(
    "financial_statement_items",
    symbols=["000001.SZ"],
    items=["revenue"],
    as_of="2026-07-21",
    pit_mode="strict",
    all_vintages=True,
    config=cfg,
)
```

## 2. Write the semantics into the strategy inputs

We recommend writing the rebalance date, report period, announcement cutoff and lake coverage into the feature snapshot together:

```python
metadata = {
    "rebalance_date": rebalance_date,
    "report_period": "2023Q4",
    "as_of": rebalance_date,
    "pit_mode": "strict",
    "dataset": "financial_statement_items",
    "lake_coverage": "see list_datasets()",
}
```

Exploratory checks can use `pit_mode="best_effort"` explicitly and inspect the returned `pit_is_exact` / `pit_quality`; do not report those results as strict PIT.

`as_of` is not an optional "filter"; it is part of the research question. If the data came from a one-off backfill rather than day-by-day accumulation, early periods may only have the current source version; first check `history_mode` in the [dataset catalog](../datasets/catalog.md) and the PIT limitations in the [query guide](../datasets/query-guide.md).
