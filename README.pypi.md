# CNEquity — 开源的中国市场金融数据基础设施

**从 A 股开始，把分散的市场数据变成可复查的本地底座。** 一条命令落地、持续日更，供 Python、DuckDB、Polars 和 AI agent 使用。

CLI：`cne` · 包名：`cnequity` · **Python ≥ 3.10** · **只做数据基础设施**（回测和信号留给下游）。

[![PyPI version](https://img.shields.io/pypi/v/cnequity?logo=pypi&logoColor=white&color=orange)](https://pypi.org/project/cnequity/)

- **真数上手**：`cne init --profile demo` 几分钟出真实日线（不是 mock）；`--research` 可验证复权口径
- **日更能挂着跑**：水位 / 失败重试 / 质量审计
- **研究口径一次定好**：复权 · universe · PIT；相对拉数库多编排，相对云端宽表可本地续跑

## 安装与一分钟体验

需要 **Python 3.10+**，且能访问 TDX 行情主机（大陆出口更稳）。

```bash
pip install cnequity
cne init --profile demo
```

海外或受限网络无法连接 TDX 时，可先运行 `cne init --profile sample`。它不访问网络，生成的合成行全部标记为 `source=mock`，仅用于验证安装、Parquet 落盘和查询链路。

写入 `data/cnequity-demo/`（几只流动性股票 × 约 30 个交易日），并打印样例表。

要验证复权口径，可运行 `cne init --profile demo --research --symbols 600519.SH`；它会额外读取 Sina 复权因子，
并打印约三年窗口的 raw / hfq 收益对照。

```bash
cne query --config configs/cnequity.demo.toml --sql "
  SELECT symbol, trade_date, close, volume, source
  FROM daily_bars
  WHERE symbol = '600519.SH'
  ORDER BY trade_date DESC
  LIMIT 10
"
```

全量日更（仍不必 clone；在含配置的工作目录执行）：

```bash
cne config create                              # → configs/cnequity.toml（data.root 写为绝对路径）
# 或显式指定：
# cne config create --data-root /data/cnequity --force
cne config validate --config configs/cnequity.toml
cne init --config configs/cnequity.toml
cne run daily --all-groups --config configs/cnequity.toml   # 之后每个交易日
```

日更按**调度组**执行，一天 6 个：`core`、`capital`、`signals`、`fundamentals`、`macro_risk`、`research`。
`--all-groups` 按配置顺序串行跑完全部组（某组失败不影响后面的组）。
不带 `--group` / `--all-groups` 的 `cne run daily` 只跑 `[[job.daily.waves]]` 里的核心骨架（行情、日历、
交易状态、公司行为、复权），**不含**估值、财报、融资融券、龙虎榜、北向、指数成分等 —— 只跑那一条，
湖会停在 15/42 新鲜。想按组错开挂 cron 见[运行手册](https://github.com/rootSunc/CNEquity/blob/main/docs/operations/runbook.md)。

<p align="center">
  <img src="https://raw.githubusercontent.com/rootSunc/CNEquity/main/docs/assets/cne-demo.png" alt="cne init --profile demo" width="820" />
</p>

## 有什么数据

数据集名即 `load()` 的第一个参数。字段见 [schema](https://github.com/rootSunc/CNEquity/blob/main/docs/datasets/schema.md)，编排元数据见 [catalog](https://github.com/rootSunc/CNEquity/blob/main/docs/datasets/catalog.md)。

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
from cnequity.query import load

bars = load("daily_bars", start="2020-01-01", end="2025-12-31", adjust="hfq")
roe = load("financial_statement_items", items=["roe"], as_of="2024-04-30")
```

无 extras —— `pip install cnequity` 即装齐所有数据源。

## 完整文档

详细 schema、runbook、定位对照与合规说明以 GitHub 为准：

- [仓库](https://github.com/rootSunc/CNEquity)
- [文档站](https://rootsunc.github.io/CNEquity/) · [仓库文档](https://github.com/rootSunc/CNEquity/tree/main/docs)
- [Changelog](https://github.com/rootSunc/CNEquity/blob/main/CHANGELOG.md)

代码 Apache-2.0。落盘行情 / 公告仍受上游条款约束——本包不附带、也不再分发数据湖。
