# Recipe：复权研究基线

这个 Recipe 用一只股票做最小验证：同一窗口里，未复权 `close` 与后复权 `adj_close` 的收益可能不同；研究代码必须明确选择口径，并确认因子覆盖是 exact。

## 1. 生成可验证的小湖

```bash
pip install cn-market-lake
cml demo --research --symbols 600519.SH
```

`--research` 会把窗口扩展到约三年，额外从 Sina 派生 hfq 因子。命令末尾会打印类似下面的摘要（收益会随 as-of 交易日变化）：

```text
600519.SH: raw return -24.25% → hfq return -14.39% (756 exact rows, ...)
```

如果只需要确认 TDX 连通性，可以先跑不带 `--research` 的 `cml demo`；网络受限时不要把失败的研究输出当成“没有复权变化”。

## 2. 在 Python 中复核合同

```python
from pathlib import Path

import polars as pl

from cn_market_lake.config import load_config
from cn_market_lake.query import load

cfg = load_config(Path("configs/cn-market-lake.demo.toml"))
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
comparison.write_parquet("data/cn-market-lake-demo/research-baseline.parquet")
```

这里的 `raw_close` 是湖内保留的原始价格，`adj_close` 是查询时把派生的 hfq 因子应用到原始价格的结果。湖内只持久化 hfq；qfq 通过 `load(adjust="qfq")` 按查询窗口推导，详见 [ADR-0004](../adr/0004-store-hfq-derive-qfq-at-query.md)。

## 3. 进入真实研究前的检查

```python
from cn_market_lake.query import list_datasets

print(
    list_datasets(config=cfg)
    .filter(pl.col("dataset").is_in(["daily_bars", "adj_factors"]))
    .select(["dataset", "coverage_start", "coverage_end", "history_mode"])
)
```

不要仅凭行数判断覆盖完整：`history_mode` 可能是 `snapshot_with_backfill`，而 `coverage_start` 由实际分区决定。全市场研究还应明确 `universe="all_a"`，并阅读其历史 ST 覆盖限制。
