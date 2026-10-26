---
description: China A-share price adjustment baseline: compare unadjusted and back-adjusted returns on one stock and confirm adjustment-factor coverage before backtesting.
---

# Price adjustment research baseline

This recipe does a minimal check with a single stock: over the same window, returns from the unadjusted `close` and the hfq `adj_close` can differ; research code must choose its price basis explicitly and confirm that factor coverage is exact.

## 1. Build a small lake you can verify

```bash
pip install cnequity
cne init --profile demo --research --symbols 600519.SH
```

`--research` extends the window to about three years and additionally derives hfq factors from Sina. At the end, the command prints raw / hfq returns and a factor coverage summary; the exact results depend on the current window.

If you only need to confirm TDX connectivity, first run `cne init --profile demo` without `--research`; on a restricted network, do not read a failed research output as "no price adjustment changes".

## 2. Recheck the contract in Python

```python
from pathlib import Path

import polars as pl

from cnequity.config import load_config
from cnequity.query import load

cfg = load_config(Path("configs/cnequity.demo.toml"))
raw = load(
    "daily_bars",
    symbols=["600519.SH"],
    config=cfg,
)
hfq = load(
    "daily_bars",
    symbols=["600519.SH"],
    adjust="hfq",
    strict_adj=True,
    config=cfg,
)

if not bool(hfq["adj_is_exact"].all()):
    raise RuntimeError("factor coverage is not exact; stop the research run")

comparison = (
    raw.select(["trade_date", "close"])
    .rename({"close": "raw_close"})
    .join(
        hfq.select(["trade_date", "adj_close", "adj_is_exact"]),
        on="trade_date",
        how="inner",
    )
    .sort("trade_date")
    .with_columns(
        (pl.col("raw_close") / pl.col("raw_close").first() - 1).alias("raw_return"),
        (pl.col("adj_close") / pl.col("adj_close").first() - 1).alias("hfq_return"),
    )
)

print(comparison.tail(1))
comparison.write_parquet("data/cnequity-demo/research-baseline.parquet")
```

Here `raw_close` is the original price kept in the lake, and `adj_close` is the result of applying the derived hfq factors to the original price at query time. The lake persists only hfq; qfq is derived per query window via `load(adjust="qfq")`. See [product boundaries](../architecture/overview.md).

## 3. Checks before moving to real research

```python
from cnequity.query import list_datasets

print(
    list_datasets(config=cfg)
    .filter(pl.col("dataset").is_in(["daily_bars", "adj_factors"]))
    .select(["dataset", "coverage_start", "coverage_end", "history_mode"])
)
```

Do not judge coverage as complete from row counts alone: `history_mode` may be `snapshot_with_backfill`, and coarse-grained partitions need their coverage confirmed against the actual date column. Universe research should name a versioned profile explicitly (for example, `cn_a_sh_sz_research_v1` when studying only Shanghai and Shenzhen), so that evidence is strictly verified for that scope; do not treat the single-stock demo as proof of full-market coverage.

Production research saves the dependency revision map for prices, factors, security identity, trading status and the calendar, and reads through
`revision_map`. For long-term retention, use `cne snapshot create NAME --dataset daily_bars --research`,
which packages the complete data dependencies together with ST/delisting evidence and the non-sensitive read configuration; if a dependency has not been generated yet, it fails immediately.
Also save the query parameters and software version; a successfully created snapshot does not mean research coverage has passed. See
[version boundaries in the Python API](../reference/python-api.md#pinned-data-versions).
