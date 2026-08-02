# A股数据湖

[![CI](https://github.com/rootSunc/ashare-lake/actions/workflows/ci.yml/badge.svg)](https://github.com/rootSunc/ashare-lake/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/rootSunc/ashare-lake/graph/badge.svg)](https://codecov.io/gh/rootSunc/ashare-lake)
[![PyPI](https://img.shields.io/pypi/v/ashare-lake.svg)](https://pypi.org/project/ashare-lake/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![English](https://img.shields.io/badge/docs-English-lightgrey.svg)](README.en.md)

**别再每次重拉、自己拼复权了。** 一行命令把能按天自动更新的 A 股研究湖落到本地 Parquet——多源进同一契约，行级可溯源，用 DuckDB / Polars / `load()` 直接查。

<p align="center">
  <img src="docs/assets/asl-serve.png" alt="asl serve 总览：FRESH/STALE/EMPTY 计数、按分层的行数与体积、250 个交易日的覆盖热力图" width="860" />
</p>

<p align="center">
  <b>39 个数据集</b> · <b>日线回溯约 2001</b> · <b>行级溯源</b> · <b>一条命令起湖</b> · <b>只读面板</b> · <b>MIT</b>
</p>

- **真数上手**：`pip install` → `asl demo`，几分钟出可复权日线
- **日更能挂着跑**：水位 / 失败重试 / 质量审计；作者自用每天自动跑
- **研究口径一次定好**：复权 · universe · PIT；39 个注册数据集，日线可回溯到约 2001
- **能接给 AI agent**：`asl mcp` 把整个湖当 MCP 工具暴露，口径随响应一起返回

CLI：`asl` · 包名：`ashare_lake` · **只做数据层**（回测和信号留给下游）· 可选日内数据（1m / 5m 分钟线、分笔，默认全关）· 只读面板 `asl serve`

## 为什么要一个湖，而不是每次现拉

<p align="center">
  <img src="docs/assets/survivorship-gap.svg" alt="同一篮子、同一区间，唯一差别是退市股还在不在里面" width="820" />
</p>

同一个等权买入持有，同样的起止日期，唯一差别是**后来退市的票还在不在篮子里**。用「今天还在的股票」当历史股票池——几乎所有按当前名单发数的源只能给你这个——2016–2021 五年收益从 **5.9% 变成 12.0%**，虚高一倍。

这是**下限**不是估计：退市股按最后一根真实 bar 计价（通常还在长期停牌之前，远高于持有人实际收回的），只统计有精确复权因子的标的，且本湖的退市覆盖也可能仍不全——三条都让测出来的差距变小而非变大。

关键在于这类错误**看不出来**：那些票不是零，是不在，输出里没有一处显得不对。这就是为什么退市股、复权因子、PIT 在这里是一等公民，而不是覆盖面上的第 40 个数据集。

在你自己的湖上复现：

```bash
python scripts/survivorship_gap.py --svg docs/assets/survivorship-gap.svg
```

## 30 秒拿到真数

```bash
pip install ashare-lake
asl demo
# 可选：asl demo --intraday   # 再看一根完整 1m 会话
```

5 只流动性股票 × 约 30 个交易日，落到独立目录，**不会**变成全市场湖；demo 只落日线，其它数据集走下面的自建湖。需要能访问 **TDX 行情主机**（大陆出口更稳）——不通先 `asl servers test`，或 `asl demo --symbols 600519.SH,000001.SZ --days 10`。

<p align="center">
  <img src="docs/assets/asl-demo.png" alt="asl demo：分阶段拉数并打印样例日线" width="820" />
</p>

```python
from ashare_lake.query import load

bars = load("daily_bars", data_root="data/ashare-lake-demo", adjust="hfq")
```

```bash
asl query --config configs/ashare-lake.demo.toml --sql "
  SELECT symbol, trade_date, close, volume, source
  FROM daily_bars
  WHERE symbol = '600519.SH'
  ORDER BY trade_date DESC
  LIMIT 10
"
```

<p align="center">
  <img src="docs/assets/asl-query.png" alt="asl query：带 source 溯源列的日线" width="720" />
</p>

macOS / Linux / Windows（PowerShell、cmd）命令通用；venv 与调度见 [installation](docs/getting-started/installation.md) / [runbook](docs/operations/runbook.md)。

## 为什么不是 AkShare / Tushare

AkShare / efinance 解决「怎么拉数」；Tushare 解决「云端宽表」；Qlib / vn.py 解决「研究/交易平台」。
**ashare-lake** 专做中间层：多源进同一契约，落成可日更、可溯源、可审计的本地 Parquet 湖。

| 你在意什么 | **ashare-lake** | AkShare / efinance | Tushare Pro | Baostock | Qlib / vn.py |
|--|--|--|--|--|--|
| 本地可续跑的数据底座 | **湖 + 日更编排**（水位 / 重试 / audit） | 只拉到内存，编排自管 | 云端积分，非自建湖 | 会话拉数，无湖 | 绑在平台数据子系统里 |
| 数据从哪来、能否复查 | **行级溯源** + 写前 schema 校验 | 通常无统一契约 | 平台字段 | 无湖契约 | 视模块 |
| 多源交叉核验 | **主源 curated + 备源 snapshot**，可 diff，不静默顶替 | 单次单源调用 | 单平台 | 单源 | 视配置 |
| 研究口径是否稳定 | **`load()` 契约**：复权组合 / universe / PIT `as_of` | 自己拼 | 自己拼 | 自己拼 | 平台口径 |
| 源挂了会怎样 | **fail batch**，暴露问题，可按批 retry | 看调用方 | 看平台 | 看调用方 | 视模块 |
| 能否单独当研究数据底座 | **能**（湖 + 日更 + `load()`） | 否，还需自建落盘/编排 | 云端表，非自建湖 | 否，会话拉数 | 能，但绑平台 |

逐条展开见 [comparison](docs/comparison.md)。

## 自建日更湖

首次 `asl init` 会回填（耗时长、占磁盘）；之后日常增量 + 读取。`load()` 默认读 cwd 下 `configs/ashare-lake.toml` 的 `data.root`。

```bash
pip install ashare-lake
# macOS / Linux：
asl config init --data-root /Users/you/ashare-lake
# Windows：asl config init --data-root D:/ashare-lake
# macOS / Windows 默认 workers=1；Linux 示例模板为 8
asl init          # 建目录 + 首次回填
asl run daily     # 之后每个交易日（不含日内数据）
asl status
```

**先浅后深**：`asl init --profile quick` 只回填最近 3 年，全市场标的一个不少。它是**更浅，不是更窄**——按标的裁剪会把上面那张图里的偏差直接建进湖里，而少几年历史由 `coverage_start` 如实记录。之后加深不必重跑 init：

```bash
asl init --profile quick                    # 或 --since 2019-01-01
asl backfill daily_bars --start 2016-01-01 --end <你的 coverage_start>
```

```python
from ashare_lake.query import load

bars = load(
    "daily_bars",
    start="2020-01-01", end="2025-12-31",
    adjust="hfq",              # None | "qfq" | "hfq"
    universe="all_a",
)
roe = load("financial_statement_items", items=["roe"], as_of="2024-04-30")
```

<p align="center">
  <img src="docs/assets/asl-load.png" alt="Python load()：从本地 curated Parquet 读日线" width="720" />
</p>

```bash
asl query --sql "
  SELECT symbol, trade_date, adj_close
  FROM daily_bars_adj
  WHERE trade_date >= '2025-01-01'
"
```

> demo 线：`data/ashare-lake-demo/` + `configs/ashare-lake.demo.toml`（查数要带该 `--config`）。  
> 日更线：`asl config init` 写出的 `configs/ashare-lake.toml`。两条线互不覆盖。

无 extras：`pip install ashare-lake` 装齐运行时数据源。全量回填后建议按 [回填验收](docs/operations/runbook.md#回填完成验收) 再挂调度。

### 可选：日内数据（分钟线 / 分笔）

**默认全关，也不在 `asl run daily` 里。** 三个数据集各有独立配置节和独立调度组，不会因为跑日更被顺带打开：

| 数据集 | 源端视野 | 容量 | 打开方式 |
|--|--|--|--|
| `minute_bars`（1m） | 约 **95** 个交易日 | 全市场约 35MB/日、8.4GB/年 | `[minute_bars]` + `--group intraday` |
| `minute_bars_5m`（5m） | 约 **491** 个交易日（唯一有较长历史的频率） | 全市场约 6MB/日 | 同上 |
| `trade_ticks`（分笔） | 固定底 **2024-01-02**（不随今天滚动，视野逐日变长） | watchlist 200 只约 4.5MB/日、1 分钟 | `[trade_ticks]` + `--group ticks` |

**分笔不是逐笔成交。** A 股 Level-1 是每 3 秒一帧的快照，通达信的「分笔」是这一帧内所有成交的聚合——实测一条记录平均合并 6–33 笔真实成交，时间戳只到分钟，所以行的身份是 `tick_seq` 而不是时间戳。能做方向拆分和大单结构，做不了订单流不平衡。完整口径见 [catalog](docs/datasets/catalog.md#trade_ticks-是什么不是什么)。

```toml
[minute_bars]
enabled = true
scope = "index:000300.SH"     # 或 watchlist / all
frequencies = ["1m", "5m"]

[trade_ticks]
enabled = true
scope = "watchlist"           # 或 index:<symbol>；不支持 all
symbols = ["600519.SH", "000001.SZ"]
max_symbols = 200             # 硬上限，解析出的范围超了直接报错
```

```bash
asl backfill minute_bars_5m --start 2024-08-01 --end 2026-07-31
asl backfill trade_ticks --symbols 600519.SH --start 2026-07-01 --end 2026-07-31
asl run daily --group intraday    # 日更：各走各的组
asl run daily --group ticks
```

磁盘与耗时见 [runbook](docs/operations/runbook.md#日内数据minute_bars--minute_bars_5m)，合规边界见 [许可与数据源](docs/legal-and-data-sources.md)。

## 看一眼湖：`asl serve`

只读面板，不写湖。覆盖、新鲜度、来源构成、审计 findings、跑批记录都在这里（就是最上面那张图）；跑批、重试、清理仍然只在 CLI。

```bash
asl serve                      # http://127.0.0.1:8787
asl serve --port 9000 --config configs/ashare-lake.toml
```

热力图按**数据集自己的周期**计缺口，不按天——年分区的数据集不会因为一个目录覆盖整年就报「缺 364 天」。

点进单个数据集是三个 tab：**状态**（覆盖、缺口、溯源、findings、最近 batch）、**元数据**（契约 / schema / 主键 / 视野）、**数据**（真的翻行，带复权与 PIT 控件）。

<p align="center">
  <img src="docs/assets/asl-serve-dataset.png" alt="trade_ticks 元数据页：主键 symbol/trade_date/tick_seq、源端历史视野自 2024-01-02 起（固定底）、行粒度分笔" width="860" />
</p>

元数据全部来自 `domain/datasets.py` 与 `domain/schemas.py`——面板不自己存一份契约，不然就有第二份会漂移的契约。

绑到非 loopback 地址必须给 `--token`。细节见 [serve 模块文档](docs/modules/serve.md)。

## 接给 AI agent：`asl mcp`

`asl serve` 把湖给人看，`asl mcp` 把湖给模型用。同样只读——采集仍然只在 CLI 上，由人来跑。

```bash
claude mcp add ashare-lake -- asl mcp --config /abs/path/to/ashare-lake.toml
```

**6 个工具，不是 39 个。** agent 每轮都要从平铺列表里选，按数据集给工具会让上下文里大半是它不会调的名字。这里按问题形状切，数据集降级成参数：

| 工具 | 用途 |
|--|--|
| `describe_lake` | 湖里有什么、覆盖到哪、以及让答案正确的口径 |
| `resolve_symbol` | 「茅台」→ `600519.SH`，含退市股 |
| `query_bars` | 日线 / 指数 / 分钟线，带 `adjust`、`universe` |
| `query_fundamentals` | 财报科目，**必须**给 `as_of` |
| `query_dataset` | 其余任意数据集 |
| `run_sql` | 单条只读 DuckDB SELECT，跨数据集聚合 |

**口径写在响应里，不写在文档里**——模型不会去读 `docs/`。不带 `adjust` 的行情返回 warning；带了但缺因子会报「N/M 行 `adj_is_exact=false`」；不给 `as_of` 直接报错并解释为什么没有默认值。分页永远带 `total` / `truncated`，避免模型把 200 行的均值当全市场报出去。

`run_sql` 只收一条 SELECT，用 DuckDB 自己的解析器判定而不是正则——湖里有 `news_headlines` / `flash_news_wire` 这类供应商文本，到达工具的 SQL 可能被湖里摄入的内容影响。

它能答、而「取数 skill」类项目结构上答不了的问题：「茅台过去五年 PE 的分位数」「2018 年这个因子的 IC，别用未来数据」「过去三年退市的票退市前 60 天什么形态」——**没有湖就做不到**，临时 HTTP 调用变不出历史。

**没有新增依赖**：stdio JSON-RPC 是手写的，官方 `mcp` SDK 会拉进 15 个包（含第二套 HTTP 栈）。细节见 [MCP 参考](docs/reference/mcp.md)。

## 顺带公开：A 股数据源健康度

**[rootsunc.github.io/ashare-lake](https://rootsunc.github.io/ashare-lake/)** —— 每个交易日自动更新。

东财今天被封了吗？财联社那个接口还活着吗？申万的证书是不是又过期了？这些源不是本项目专属的：AkShare、各类取数 skill、你自己写的爬虫，走的是同一批端点。变了通常没地方查，得先花半天怀疑自己的代码。

这个湖本来就每个交易日跑全市场，顺手把结果公开出来，每个源多发一个请求而已。

```bash
asl sources probe --vantage cn --out reports/cn.json
asl sources page --report reports/cn.json --out site/index.html
```

三条让这张表可信的规矩：

- **HTTP 200 不等于可用。** 东财用 200 返回风控页，新浪用 200 返回空数组。每个探测断言**响应体**，「空响应」单独一档——它看起来比失败健康，实际更危险（回填静默截断）。
- **「被拒」不等于「挂了」。** 好几个源在 WAF 层拒绝非大陆出口，同一主机同一秒可以大陆绿、海外红。两个视角**并排**放，不合并成一个结论。
- **一次探测不是 SLA。** 每源只发一个请求，串行——健康检查不该自己制造它要观测的故障。

口径与加新源见 [数据源健康度](docs/operations/source-health.md)。

## 有什么数据

下表覆盖注册表全部 **39** 个数据集（36 curated + 3 derived，与 `domain/datasets.py` 同步）。  
英文名即 `load()` 的第一个参数；字段见 [schema](docs/datasets/schema.md)，编排与主源见 [catalog](docs/datasets/catalog.md)。

| 类别 | 数据集（`load()` 名 · 中文） |
|------|------------------------------|
| 基础参考 | `instruments` 证券主数据 · `trading_calendar` 交易日历 · `trading_status` 交易状态（停复牌 / ST） |
| 行情 | `daily_bars` 日线（未复权） · `index_bars` 指数日线 · `minute_bars` 1 分钟线（可选） · `minute_bars_5m` 5 分钟线（可选） · `trade_ticks` 分笔（可选，3 秒快照聚合非逐笔） · `commodity_bars` 商品期货主连（可选） · `adj_factors` 复权因子（派生） · `delisting_events` 退市事件（派生） |
| 公司事件 | `corporate_actions` 公司行为（除权除息） · `announcement_index` 公告索引 · `earnings_disclosure_schedule` 业绩披露预约 |
| 基本面 / 估值 | `financial_statement_items` 财务报表科目（PIT） · `valuation_metrics` 估值指标 · `analyst_consensus` 分析师一致预期 |
| 资金面 | `fund_flow` 个股资金流 · `margin_trading` 融资融券 · `northbound_flows` 北向资金流向 · `northbound_holdings` 北向持股 · `dragon_tiger` 龙虎榜 · `block_trades` 大宗交易 · `institutional_holdings` 机构持股 |
| 结构 / 行业 | `sector_members` 板块成分 · `index_constituents` 指数成分 · `industry_members` 行业分类成分 · `industry_index` 行业指数（派生） |
| 宏观 | `macro_indicators` 宏观指标 · `market_breadth` 市场宽度 · `economic_calendar` 经济日历（占位，源已下线） |
| 舆情 / 轮动 | `sentiment_scores` 情绪评分 · `hot_rank` 人气榜 · `sector_bars` 板块行情 · `sector_fund_flow` 板块资金流 · `news_headlines` 新闻标题 · `flash_news_wire` 7×24 快讯 |
| 风险 | `share_unlock_schedule` 解禁日程 · `regulatory_events` 监管事件 |

另有 **on-demand**（不进 curated 主路径）：`stock_news`、`research_reports` 等，见 [catalog](docs/datasets/catalog.md)。

## 架构

<p align="center">
  <img src="docs/assets/architecture-overview.png" alt="ashare-lake 架构：数据源 → ASL Daily Pipeline → staging/curated/derived → load()/DuckDB/Polars" width="900" />
</p>

落盘布局（日更湖的 `data.root` 下）：

```
{data_root}/
  curated/   {dataset}/{partition}=v/part-*.parquet
  derived/   adj_factors/...
  staging/   本次 run 原始落地（compact 后可清理）
  meta/      manifest、quality findings、水位、on-demand 缓存
  duckdb/    ashare-lake.duckdb
```

## 已知限制

- **幸存者偏差**：退市股需 `asl delisted backfill` + `repair`；未补齐前收益序列要打折看
- **海外网络**：部分 HTTP / 板块回填依赖大陆出口；行情 demo 需 TDX 可达
- **日内视野**：TDX 约保留 95 个交易日的 1m、491 个交易日的 5m；更早窗口为空——见 [catalog](docs/datasets/catalog.md)
- **年/月分区**（如 `index_bars`）：优先 `asl query` / `load()`，避免 hive 分区标签撞真日期——见 [lake-layout](docs/architecture/lake-layout.md)

更多见 [runbook](docs/operations/runbook.md)、[排障](docs/operations/troubleshooting.md)、[legal](docs/legal-and-data-sources.md)。

## 项目状态

个人项目：issue / PR 欢迎，响应尽力而为。[贡献指南](CONTRIBUTING.md) · [安全策略](SECURITY.md)。文档中文为主；[CHANGELOG](CHANGELOG.md) 与 [ADR](docs/adr/) 为英文。

## 文档

完整索引：[docs/README.md](docs/README.md)。常用入口：[MCP](docs/reference/mcp.md) · [安装](docs/getting-started/installation.md) · [快速开始](docs/getting-started/quickstart.md) · [数据集目录](docs/datasets/catalog.md) · [Runbook](docs/operations/runbook.md) · [CLI](docs/reference/cli.md) · [serve 面板](docs/modules/serve.md)。

## 许可

代码 [MIT](LICENSE)。落盘行情 / 公告仍受上游条款约束；仓库不附带数据湖，也不授予再分发权 [legal](docs/legal-and-data-sources.md)。

---

如果它省了你搭数据底座的时间，点个 ⭐ 让更多做 A 股研究的人看到。

[![Star History Chart](https://api.star-history.com/svg?repos=rootSunc/ashare-lake&type=Date)](https://star-history.com/#rootSunc/ashare-lake&Date)
