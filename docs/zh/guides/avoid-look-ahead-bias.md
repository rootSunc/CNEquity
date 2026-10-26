---
description: A股回测怎么避免未来函数和幸存者偏差：复权方式的选择、财报按公告日取数（PIT）、历史股票池保留退市股。
---

# 回测怎么避免未来函数：A股复权与 PIT 财报

回测结果好得不真实，常见原因是数据里混进了「当时还不知道的信息」。A 股最容易出问题的有三处：复权、财报和股票池。

## 复权：口径要明确选

前复权价格会随新的除权事件整体改变，同一天的历史价格今天和明天看到的可能不同。CNEquity 把原始价格和复权因子分开存，复权方式在查询时选：

```python
from cnequity.query import load

bars = load("daily_bars", start="2020-01-01", end="2024-12-31", adjust="hfq")
```

| 参数 | 含义 |
|---|---|
| `adjust=None` | 原始未复权价格 |
| `adjust="hfq"` | 后复权，历史价格不随新事件改变 |
| `adjust="qfq"` | 前复权，以查询窗口内最新一根 K 线为基准 |

结果里的 `adj_is_exact` 标出复权因子是否完整覆盖。完整的验证步骤见[复权研究基线](../recipes/research-baseline.md)。

## 财报：按公告日取，不按报告期取

一季报要到 4 月底才披露，之后还可能修订。如果回测在 4 月 1 日就用了一季报的数据，或者用了后来修订过的数值，就是未来函数。CNEquity 的财报按公告日记录每一版，查询时给出时点：

```python
items = load(
    "financial_statement_items",
    items=["roe", "net_profit"],
    as_of="2024-04-30",
    pit_mode="strict",
)
```

`pit_mode="strict"` 只返回在 `as_of` 当天已经公告并入湖的数据。不要用 `end=` 代替 `as_of`。按调仓日构造截面的完整例子见 [PIT 财报调仓截面](../recipes/pit-rebalance.md)。

## 股票池：保留已退市的股票

只用今天还在交易的股票回看过去，会漏掉后来退市的公司，收益被系统性高估，这就是幸存者偏差。CNEquity 在数据层保留退市股票的身份和行情，研究股票池用画像指定：

```python
bars = load("daily_bars", start="2024-01-01", profile="cn_a_sh_sz_research_v1")
```

该画像按日核对上市、停牌、历史 ST 和退市证据，缺少证据时拒绝返回，而不是悄悄漏掉股票。详见[股票池画像](../reference/universe-profiles.md)。

## 相关页面

- [查询指南](../datasets/query-guide.md)：复权、股票池、PIT 的完整参数
- [A股历史数据免费获取](free-a-share-data.md)
