---
description: A 股复权研究基线：用一只股票对比未复权与后复权收益，确认复权因子覆盖，避免回测口径错误。
---

# 复权研究基线

这个 Recipe 用一只股票做最小验证：同一窗口里，未复权 `close` 与后复权 `adj_close` 的收益可能不同；研究代码必须明确选择口径，并确认因子覆盖是 exact。

## 1. 生成可验证的小湖

```bash
pip install cnequity
cne init --profile demo --research --symbols 600519.SH
```

`--research` 会把窗口扩展到约三年，额外从 Sina 派生 hfq 因子。命令末尾会打印 raw / hfq 收益与因子覆盖摘要，具体结果取决于当前窗口。

如果只需要确认 TDX 连通性，可以先跑不带 `--research` 的 `cne init --profile demo`；网络受限时不要把失败的研究输出当成“没有复权变化”。

## 2. 在 Python 中复核合同

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

这里的 `raw_close` 是湖内保留的原始价格，`adj_close` 是查询时把派生的 hfq 因子应用到原始价格的结果。湖内只持久化 hfq；qfq 通过 `load(adjust="qfq")` 按查询窗口推导，详见 [产品边界](../architecture/overview.md)。

## 3. 进入真实研究前的检查

```python
from cnequity.query import list_datasets

print(
    list_datasets(config=cfg)
    .filter(pl.col("dataset").is_in(["daily_bars", "adj_factors"]))
    .select(["dataset", "coverage_start", "coverage_end", "history_mode"])
)
```

不要仅凭行数判断覆盖完整：`history_mode` 可能是 `snapshot_with_backfill`，粗粒度分区需按实际日期列确认覆盖。股票池研究应明确版本化 profile（例如只研究沪深时用 `cn_a_sh_sz_research_v1`），由该范围严格核验证据；不要将单只股票 demo 当作全市场覆盖证明。

正式研究保存价格、因子、证券身份、交易状态、日历的依赖 revision 映射，并通过
`revision_map` 读取。长期保存使用 `cne snapshot create NAME --dataset daily_bars --research`，
一起封装完整数据依赖和 ST/退市证据、非敏感读取配置；若某依赖尚未生成会直接报错。
同时保存查询参数和软件版本，不能把快照创建成功等同于研究覆盖通过。详见
[Python API 的版本边界](../reference/python-api.md#固定组合读取的数据版本)。
