# CML · CNMarketLake — 免费、自托管的 A 股历史数据层

**别再每次重拉、自己拼复权了。** 一条命令，把可日更的研究数据落到本地。自动保存历史口径，供 Python、DuckDB、Polars 和 AI agent 使用。

CLI：`cml` · 包名：`cn_market_lake` · **Python ≥ 3.10** · **只做数据层**（回测和信号留给下游）。

- **真数上手**：`cml demo` 几分钟出真实日线（不是 mock）；`--research` 可验证复权口径
- **日更能挂着跑**：水位 / 失败重试 / 质量审计
- **研究口径一次定好**：复权 · universe · PIT；相对拉数库多编排，相对云端宽表可本地续跑

## 安装与一分钟体验

需要 **Python 3.10+**，且能访问 TDX 行情主机（大陆出口更稳）。

```bash
pip install cn-market-lake
cml demo
```

写入 `data/cn-market-lake-demo/`（几只流动性股票 × 约 30 个交易日），并打印样例表。

要验证复权口径，可运行 `cml demo --research --symbols 600519.SH`；它会额外读取 Sina 复权因子，
并打印约三年窗口的 raw / hfq 收益对照。

```bash
cml query --config configs/cn-market-lake.demo.toml --sql "
  SELECT symbol, trade_date, close, volume, source
  FROM daily_bars
  WHERE symbol = '600519.SH'
  ORDER BY trade_date DESC
  LIMIT 10
"
```

全量日更（仍不必 clone；在含配置的工作目录执行）：

```bash
cml config init                              # → configs/cn-market-lake.toml（data.root 写为绝对路径）
# 或显式指定：
# cml config init --data-root /data/cn-market-lake --force
cml config validate --config configs/cn-market-lake.toml
cml init --config configs/cn-market-lake.toml
cml run daily --config configs/cn-market-lake.toml
```

<p align="center">
  <img src="https://raw.githubusercontent.com/rootSunc/cn-market-lake/main/docs/assets/cml-demo.png" alt="cml demo" width="820" />
</p>

## 有什么数据

数据集名即 `load()` 的第一个参数。字段见 [schema](https://github.com/rootSunc/cn-market-lake/blob/main/docs/datasets/schema.md)，编排元数据见 [catalog](https://github.com/rootSunc/cn-market-lake/blob/main/docs/datasets/catalog.md)。

| 类别 | 数据集 |
|------|--------|
| 基础参考 | `instruments` · `trading_calendar` · `trading_status`（停复牌 / ST） |
| 行情 | `daily_bars`（未复权） · `index_bars` · `adj_factors` · `minute_bars` / `minute_bars_5m`（可选日内） |
| 公司事件 | `corporate_actions` · `announcement_index` · `earnings_disclosure_schedule` |
| 基本面 / 估值 | `financial_statement_items`（PIT） · `valuation_metrics` · `analyst_consensus` |
| 资金面 | `fund_flow` · `margin_trading` · `northbound_flows` / `northbound_holdings` · `dragon_tiger` · `block_trades` · `institutional_holdings` |
| 结构 / 行业 | `sector_members` · `index_constituents` · `industry_members` |
| 宏观 | `macro_indicators` · `market_breadth` |
| 舆情 / 轮动 | `sentiment_scores` · `hot_rank` · `sector_bars` · `sector_fund_flow` · `news_headlines` |
| 风险 | `share_unlock_schedule` · `regulatory_events` |

## 读数据

```python
from cn_market_lake.query import load

bars = load("daily_bars", start="2020-01-01", end="2025-12-31", adjust="hfq")
roe = load("financial_statement_items", items=["roe"], as_of="2024-04-30")
```

无 extras —— `pip install cn-market-lake` 即装齐所有数据源。

## 完整文档

详细 schema、runbook、定位对照与合规说明以 GitHub 为准：

- [仓库](https://github.com/rootSunc/cn-market-lake)
- [文档站](https://rootsunc.github.io/cn-market-lake/) · [仓库文档](https://github.com/rootSunc/cn-market-lake/tree/main/docs)
- [Changelog](https://github.com/rootSunc/cn-market-lake/blob/main/CHANGELOG.md)

代码 Apache-2.0。落盘行情 / 公告仍受上游条款约束——本包不附带、也不再分发数据湖。
