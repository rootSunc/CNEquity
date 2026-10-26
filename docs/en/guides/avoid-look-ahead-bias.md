---
description: Avoid look-ahead and survivorship bias in China A-share backtests - choosing a price adjustment, reading financials by announcement date (point-in-time), and keeping delisted stocks in the universe.
---

# Avoiding look-ahead bias: A-share price adjustment and point-in-time financials

When a backtest looks too good to be true, the data often contains information that was not known at the time. In China A-shares the usual culprits are price adjustment, financial statements and the stock universe.

## Price adjustment: choose it explicitly

Forward-adjusted prices shift as a whole with each new ex-rights event, so the same historical day can read differently today and tomorrow. CNEquity stores raw prices and adjustment factors separately, and you choose the adjustment at query time:

```python
from cnequity.query import load

bars = load("daily_bars", start="2020-01-01", end="2024-12-31", adjust="hfq")
```

| Argument | Meaning |
|---|---|
| `adjust=None` | Raw unadjusted prices |
| `adjust="hfq"` | Back-adjusted; historical prices do not change with new events |
| `adjust="qfq"` | Forward-adjusted, anchored to the latest bar in the query window |

`adj_is_exact` in the result tells whether the adjustment factors cover the rows fully. The full check is in the [Price adjustment research baseline](../recipes/research-baseline.md).

## Financials: by announcement date, not report period

A Q1 report is not disclosed until the end of April and may be revised later. A backtest that uses Q1 numbers on April 1, or uses later revised values, is looking ahead. CNEquity records every version by announcement date, and you query at a point in time:

```python
items = load(
    "financial_statement_items",
    items=["roe", "net_profit"],
    as_of="2024-04-30",
    pit_mode="strict",
)
```

`pit_mode="strict"` returns only data already announced and collected by `as_of`. Do not use `end=` in place of `as_of`. A full rebalance example is in the [PIT rebalance cross-section](../recipes/pit-rebalance.md).

## Universe: keep delisted stocks

Looking back with only the stocks that still trade today drops the companies that later delisted and systematically overstates returns; that is survivorship bias. CNEquity keeps delisted stocks' identities and prices in the data layer, and a research universe is chosen through a profile:

```python
bars = load("daily_bars", start="2024-01-01", profile="cn_a_sh_sz_research_v1")
```

The profile checks listing, suspension, historical ST and delisting evidence day by day, and refuses to return data when evidence is missing instead of silently dropping stocks. See [Universe profiles](../reference/universe-profiles.md).

## Related pages

- [Query guide](../datasets/query-guide.md): full parameters for adjustment, universes and PIT
- [Free China A-share historical data](free-a-share-data.md)
