---
description: A股历史数据怎么免费获取：无需注册和 Token，一条命令把沪深京全市场日线、复权因子、财报、公司行为等数据下载到本地，并支持每日增量更新。
---

# A股历史数据免费获取：无需注册和 Token

CNEquity 从交易所、巨潮、通达信、新浪、东财、Baostock 等公开来源采集 A 股数据，存成本地 Parquet 文件。基础采集不需要注册账号，也不需要 Token 或积分。

## 一分钟试用

先用 5 只股票试一下，数据写进独立目录，不影响之后的正式数据：

```bash
pip install cnequity
cne init --profile demo
```

## 下载全市场数据

```bash
cne init
```

默认建立沪深京全市场最近 3 年的主干：证券列表、交易日历、公司行为、个股与指数日线、复权因子和行业指数，并恢复窗口内已退市股票的日线。第一次运行可能需要几个小时，中断后重跑同一条命令即可续跑，已下载的部分不会重来。

需要更长的历史，用 `cne init --profile full`，日线从 2016-01-01 起。各数据集的历史起点和来源限制见[数据源与历史限制](../datasets/sources.md)。

## 读取数据

```python
from cnequity.query import load

bars = load("daily_bars", symbols=["600519.SH"], adjust="hfq")
print(bars.tail())
```

也可以直接写 SQL：

```bash
cne query --sql "SELECT symbol, trade_date, close FROM daily_bars ORDER BY trade_date DESC LIMIT 10"
```

## 保持更新

每天运行一次 `cne run daily`，用 cron、launchd 或 Windows 任务计划程序定时调用即可，也可以用 `cne schedule install` 直接安装定时任务。

## 能拿到哪些数据

共 57 个数据集，按用途分为行情、公司事件、基本面、资金面、行业与指数、宏观等，完整列表见[数据集目录](../datasets/catalog.md)。

需要注意的限制：

- **快照类数据无法补回历史。** 如人气榜、个股资金流、一致预期，只能从启用那天开始积累。
- **公共来源会变。** 可达性和历史深度可能调整，项目会跟进，但没有商业服务的 SLA。
- **要占本地磁盘。** 范围和磁盘成本见[初始化、范围与续跑](../getting-started/initialization.md)。

## 相关页面

- [快速开始](../getting-started/quickstart.md)
- [AKShare、Tushare、Baostock 怎么选](../architecture/comparison.md)
- [回测怎么避免未来函数](avoid-look-ahead-bias.md)
