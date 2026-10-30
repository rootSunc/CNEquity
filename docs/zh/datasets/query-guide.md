---
description: load() 查询指南：A 股复权、股票池、PIT 财报与常见陷阱。
---

# 查询指南

下游读数的推荐路径是 `cnequity.query.load()`。本文说明复权、Universe、PIT 与常见陷阱。

API 签名见 [Python API 参考](../reference/python-api.md)。

## 选择读取入口

| 入口 | 提供的能力 | 需要自己处理 |
|---|---|---|
| `load()` | 物化数据，按参数做复权、PIT、股票池与严格校验 | 显式选择模式和证据窗口 |
| `load_with_receipt()` | 固定可用修订，并返回行来源、PIT 与观察覆盖凭证 | 将凭证与研究产物一起保存；另核验完整性 |
| `scan()` | 原始 LazyFrame，日期与标的过滤、版本选择 | 不含复权、PIT、股票池 |
| `cne query --sql` | 本地视图上的只读 SELECT | SQL 的口径与覆盖校验 |
| 直读 Parquet | 脱离运行时读取开放文件 | 版本、去重、PIT、复权和股票池 |

初始化进行中，只有带 `symbols` 的 `load("daily_bars", ...)` 能读到已封存、尚未发布的未复权日线。不指定标的的读取仍只看已经发布的数据。

## 基本用法

```python
from cnequity.query import load, scan, list_datasets

# 物化 DataFrame
df = load("daily_bars", start="2024-01-01", end="2024-12-31")

# Lazy scan（大窗口）
lf = scan("daily_bars", start="2020-01-01", end="2024-12-31")

# 湖内数据集概览（含 history_mode / backfill_source / coverage_*）
meta = list_datasets()  # 或 list_datasets(config=cfg)
# snapshot_only → 无诚实历史；coverage_start 为盘上分区起点
```

配置解析顺序：`config=` → `data_root=` → `CNE_CONFIG` → 当前目录的 `configs/cnequity.toml` → 用户级 `~/.config/cnequity/config.toml`

境内股票指数 ETF 使用单独的 `etf_profiles` 当前快照。按研究日筛掉之后的快照，
再选每只基金当时最新的记录；只有 `eligibility_status="eligible"` 可进入该范围。
深交所目录本身未提供足以核实指数资产类别的字段；仅按指数代码匹配并归档了明确
境内 A 股样本空间的官方编制方案后才可升级资格，其余行保持 `unverified`；
历史没有快照的日期也不能用今天的分类倒填。`instruments.asset_type="etf"` 是旧报价
口径，还混有 LOF，不是资格证明。

`list_datasets()` 是研究侧的**数据发现入口**，边界不证明中间无缺口：`history_mode` 区分 `by_date` / `snapshot_with_backfill` / `snapshot_only`；`coverage_start` 按已发布数据与分区边界解析（含 `report_period=YYYYQn`）。详见 [数据集目录 — 历史可用性](catalog.md#历史可用性history_mode)。

## 复权（adjust）

仅适用于含价量列的数据集（主要是 `daily_bars`）。

| 参数 | 行为 |
|------|------|
| `adjust=None` | 原始未复权 OHLC |
| `adjust="hfq"` | 用后复权因子乘价格；增列 `adj_open`…`adj_close`、`adj_is_exact` |
| `adjust="qfq"` | 查询窗口内按最新 bar anchor 将 hfq 因子归一化 |

```python
bars = load(
    "daily_bars",
    start="2020-01-01",
    end="2024-12-31",
    adjust="hfq",
)
```

### strict_adj

```python
bars = load("daily_bars", start="2024-01-01", adjust="hfq", strict_adj=True)
```

- `True`：缺因子行抛出 `ReaderError`，不填充 1.0
- `False`（默认）：缺因子时 `adj_is_exact=False`，价格按 factor=1.0 降级

**研究建议**：量化回测用 `hfq` + `strict_adj=True`；qfq 窗口 anchor 会漂移，不适合长期研究复现。

需要固定结果时，用 `revision_map` 同时固定价格、因子及股票池依赖，并留存所需质量证据、配置和软件版本。具体保证及快照边界见 [Python API](../reference/python-api.md#固定组合读取的数据版本)。

`index_bars` 是指数点位，不是个股价格，不支持 `adjust=`；请直接使用原始指数水平。

### 总收益复权（total_return）

```python
bars = load("daily_bars", start="2024-01-01", adjust="total_return", symbols=["510300.SH"])
```

`hfq` / `qfq` 的含义保持不变：股票的后复权因子已包含分红再投资，基金（ETF/LOF）的后复权因子来自新浪基金因子，只随份额拆分、合并变化，**不含现金分配**，是价格收益口径。

`adjust="total_return"` 仅适用于 `daily_bars`：在后复权基础上，把基金每次现金分配按除息前一交易日收盘价再投资（因子乘以 `前收盘 / (前收盘 − 每份分配)`）；股票与 `hfq` 相同。每只基金在查询范围第一天的价格水平等于后复权，因此跨不同起点的查询请比较收益率而不是价格水平。需要的依赖（`corporate_actions`、`instruments`）会一并记入读取回执。

按用途选择：看价格走势用未复权价；做拆分可比的价格比较用 `hfq`；算投资者实际收益或回测用 `total_return`。

### 存储约定

- 湖内 `daily_bars` 存**未复权**价
- `adj_factors` 在 `derived/`，仅存 `hfq`（`adjust_type="hfq"`）
- ETF/LOF 已记录的拆分和转增会在日常因子派生中校正，0.17 及更早版本留下的历史漏跳由 `cne migrate` 重建；修正来源为 `derived_fund_actions`。保留其他来源台阶，现金分红仍由总回报读取单独计入。
- DuckDB 视图 `daily_bars_adj` / `daily_bars_hfq` / `daily_bars_qfq` 与上述语义一致；`daily_bars_total_return(start, end)` 表宏对应 `adjust="total_return"`，输出 `tr_*` 与 `adj_close`

## Universe 过滤

```python
bars = load(
    "daily_bars",
    start="2024-01-01",
    profile="cn_a_sh_sz_research_v1",
)
```

`cn_a_sh_sz_research_v1` 明确限定沪深两市，并在读取时严格核验证券身份、
逐日交易状态、历史 ST 和退市证据；缺少收据会拒绝返回。它不会把北交所缺口
藏在“全 A”标签下。需要包含北交所时可显式选
`cn_a_all_experimental_v1`，但该画像仍标为实验性，不应当作已验收的研究口径。
画像版本与哈希的用法见[股票池画像](../reference/universe-profiles.md)。

旧的 `universe="all_a"` / `"all_a_sh_sz"` 参数仍供兼容使用；前者会发出
`DeprecationWarning`，两者都不会自动成为严格画像。兼容过滤的基础规则：

1. **instruments**：`list_date <= trade_date`，且未退市或 `delist_date >= trade_date`（`delist_date` 是最后一个上市交易日）
2. 排除 CDR（689xxx.SH）
3. **trading_status**（仅有数据的日期）：剔除 ST/*ST 与停牌

### 历史 ST 限制

日更只抓当天 `trading_status`（交易所板块记录当天停牌）。`init` 和 `cne backfill daily_bars` 之后会按日线窗口自动重建停牌；更早的历史可由 `cne derive trading_status --start/--end` 按年重建，覆盖可与 `daily_bars` 同起点（约 2001）。**ST 标签**由 Baostock 的逐标的 `isST` 历史与可选 Tushare BJ 历史源共同提供；只有生成了完整、版本化的 `historical_st_evidence` 收据，才能把对应窗口用于研究。部分回补（例如仅从 2016 年开始）不能证明 2001 年起的历史 ST 已剔除。

收据可通过重叠的深历史范围与较新尾段范围合并，但新增标的必须有首个交易日证据。北交所（BJ）可通过显式配置的 Tushare Pro 回补：2016 年使用 `bak_basic` 历史简称，2017-01-01 起使用 `stock_st`；接口需要 token，2016 年以前仍会作为源端能力限制阻塞，不能把接口空结果当成 normal。未配置 Tushare 时，BJ 仍会显式阻塞。审计项 `trading_status_coverage_start` 区分总覆盖与 `st_coverage_start`，历史研究应使用 `cne audit --full --research-start ...` 复核。

显式研究画像会自动启用严格校验。若仍使用兼容参数，可加
`strict_universe=True` 校验逐日状态和请求范围的历史 ST 证据；缺收据或
包含无历史 ST 来源的 BJ 标的会抛出 `UniverseCoverageError`。
默认兼容读取适合探索，不应直接作为长历史回测输入。

### 交易所覆盖

`all_a` 包含沪深北。北交所证券名单与简称在日更时由 BSE 行情板补齐；TDX 名单枚举与 TDX 日线协议的市场能力不能混为一谈。

- BJ 日线历史优先走专用 TDX 路径（market id 2），当期 tip 可用 BSE 板快照，Sina 补未覆盖部分。显式小范围历史请求会跳过无关的全市场快照。
- Sina 历史行可能缺 `amount`。`cne repair bar-amounts` 只在 OHLC 一致且成交量差小于一手时补成交额；旧代码等未被 TDX 服务的行仍保留缺失和 finding。
- `cne repair bse-tip-turnover` 只对已存当期日线核对 BSE 快照并补量额，不重新请求 Sina 历史。它不能恢复过去的 BSE 快照。

这些路线补的是行情；BJ 历史 ST 仍需独立证据。完整来源与修复入口见[数据源说明](sources.md#daily_bars)。

### 北交所上市前的新三板行情

北交所证券在交易所上市前的场外行情（新三板做市、集合竞价）交易规则不同，没有可核对的涨跌幅。`daily_bars` 与 `adj_factors` 默认不返回这部分：每只北交所证券从其**交易所上市起点**读起，即 `instruments.list_date` 与精选层开板日 2020-07-27 中较晚者。沪深证券不受影响。

```python
bars = load("daily_bars", symbols=["920826.BJ"])                    # 2021-01-12 起
otc = load("daily_bars", symbols=["920826.BJ"], include_otc=True)   # 含新三板行情
```

`universe="all_a"` 同样按交易所上市起点判定北交所证券是否在册。数据不会删除，仍保存在湖中。

## PIT（Point-in-Time）

PIT 数据集：`financial_statement_items`、`announcement_index`、
`share_structure`、`shareholder_counts`、`top_holders`。

```python
items = load(
    "financial_statement_items",
    items=["roe", "net_profit"],
    as_of="2024-04-30",
    pit_mode="strict",
)
```

- `pit_mode="strict"` 过滤 `announce_date`、已知 `available_at`/
  `source_published_at` 和 `fetched_at`/`observed_at` 均不晚于 `as_of`，并排除
  `reconstructed` 回填行；`pit_mode="best_effort"` 可保留它们，但返回
  `pit_is_exact=False`。
- 同一 `(symbol, report_period, item)` 取 `announce_date` 最新一行
- 禁止用 `end=` 代替 `as_of` 做财报对齐

PIT 仍可使用 `start` / `end` 限定数据自身的日期列：公告与股东数据按
`announce_date` / `change_date` / `count_date` / `record_date` 过滤；财报的
`report_period` 是季度字符串，会按与日期边界相交的季度过滤。`as_of` 仍然是
公告可见时间，不能被日期窗口替代。

`announce_date` 在主键里，所以财报修订是**新增一版**而不是覆盖原值：同一科目可以同时存在
首发值和修订值。默认只返回 `as_of` 当时生效的那一版；要看修订本身（修订幅度和方向本身
就是信号）加 `all_vintages=True`：

```python
# 000001.SZ 2024Q1 营收被改过几次、每次改了多少
load(
    "financial_statement_items",
    symbols=["000001.SZ"], items=["revenue"],
    as_of="2026-07-21", pit_mode="strict", all_vintages=True,
).select("report_period", "announce_date", "item_value")
```

注意：回填行以 `source=eastmoney_backfill` 标记，并视为
`pit_quality="reconstructed"`。省略 `pit_mode` 或传 `None` 默认为
`best_effort`：在 `as_of` 之后采集的历史回填也可读取，并返回
`pit_is_exact=False`；公告及已知可用/发布时间仍受截止日约束。
需要湖在当时已有证据的研究代码请显式传 `pit_mode="strict"` 并记录该选择。
升级前依赖省略参数按采集时间过滤的调用，也应改为显式严格模式。
日更逐日累积、保存真实双时态列的版本才满足
严格模式（见 [schema](schema.md#financial_statement_items)）。
回填默认自 2001 起（东财）；`list_datasets()` 的 `coverage_start` 为盘上实际起点。

## 符号与列过滤

```python
load("daily_bars", symbols=["600519.SH", "000001.SZ"], start="2024-01-01")
load("financial_statement_items", items=["roe"], as_of="2024-06-30", pit_mode="strict")
```

Symbol 格式：`{code}.{SH|SZ|BJ}`，与 `domain/symbols.py` 一致。

## DuckDB SQL

```bash
cne query --sql "SELECT * FROM instruments LIMIT 5"
```

常用视图：

| 视图 | 说明 |
|------|------|
| `daily_bars` | 未复权；不含北交所上市前的新三板行情，与 `load()` 默认一致 |
| `daily_bars_including_otc` | 未复权，含北交所新三板行情（`adj_factors_including_otc` 同理） |
| `daily_bars_hfq` | 后复权价列 |
| `daily_bars_qfq` | 前复权价列 |
| `daily_bars_adj` | 含 adj_* 与 adj_is_exact |
| `daily_bars_total_return(start, end)` | 表宏：总收益复权价 `tr_*`（基金现金分配再投资，股票同后复权） |
| `central_bank_rates` | 各国央行利率决议：按金十指标编号从 `economic_calendar` 取出，含 `bank`、`rate`、`policy`（是否为主要政策利率）、预期 / 前值 / 公布值，以及 `change`（公布值 − 前值）和 `surprise`（公布值 − 预期）；已撤销（`withdrawn`）的事件不计入 |
| `{dataset}` | 各 curated/derived 数据集 |

数据库：`{data.root}/duckdb/cnequity.duckdb`（只读连接）。

## 直读 Parquet

不依赖本项目运行时：

```python
import polars as pl
pl.scan_parquet("data/cnequity/curated/daily_bars/**/*.parquet")
```

需自行实现复权与 universe 逻辑；生产推荐 `load()`。

## 分区裁剪

`query/parquet_scan.py` 按 `partition_col` 与日期范围裁剪 Hive 分区目录。大窗口查询优先 `scan()` + lazy 算子链。

### 成交口径的分钟重采样

TDX 的零成交分钟可能沿用昨收或最近报价。需要成交口径时，从完整的 1m 数据重采样，避免把未成交报价混入 OHLC。1m 覆盖不到的更早历史，可以从 5m 重采样出 15m / 30m / 60m：

```python
from cnequity.query import load, resample_trade_bars

minute = load("minute_bars", start="2026-09-15", end="2026-09-15",
              symbols=["603869.SH"])
five_minute = resample_trade_bars(minute, "5m")

history = load("minute_bars_5m", start="2025-01-02", end="2025-12-31",
               symbols=["603869.SH"])
hourly = resample_trade_bars(history, "60m")
```

1m 输入支持 5m、15m、30m、60m，5m 输入支持 15m、30m、60m；输入必须只有一种频率。都分别按 09:30 和 13:00 对齐，不跨午休。OHLC 只计入 `volume > 0` 或 `amount > 0` 的输入 K 线；后者可保留股数被供应商取整为零的小额成交。区间内无成交时保留末报价且量额为零，该价格仍不是成交价格。重复时间戳、午休记录、不在 5 分钟网格上的 5m 时间、日期错配和缺组成 K 线都会报错，完全没有记录的区间不会凭空补齐。结果不覆写原始湖，也不继承输入行的来源标签；研究应同时记录输入 revision，重采样无法恢复供应商丢失的成交细节。

5m 输入只能做到 5 分钟粒度的成交口径。供应商的 5m K 线由全部 1m 直接聚合，未成交分钟沿用的报价也算进开高低价。只要这根 5m 有成交，它的 OHLC 就原样参与计算，所以少数区间的开盘价、最高价、最低价会与从 1m 重采样的结果不同；收盘价和量额基本一致。1m 覆盖到的日期优先用 1m。

需要用 SQL、HTTP 接口或 MCP 读取时，可以用 `cne derive minute_bars_15m`（30m、60m 同理）按同一规则算进湖，见 [15 / 30 / 60 分钟线](../recipes/minute-bars-15-30-60.md)。

`resample_minute_history` 把两者拼成一条序列：某只股票某天有 1m 就用 1m，没有就用 5m，结果多一列 `resampled_from` 标明每根 K 线来自 `1m` 还是 `5m`。1m 的起点按股票各不相同，所以不按统一日期切换。只要有 1m，那一天就只用 1m；1m 不完整时照常报错，不会悄悄改用 5m。

```python
from cnequity.query import load, resample_minute_history

window = dict(start="2025-01-02", end="2026-09-30", symbols=["603869.SH"])
bars_30m = resample_minute_history(load("minute_bars", **window),
                                   load("minute_bars_5m", **window), "30m")
```

## 错误处理

| 异常 | 常见原因 |
|------|----------|
| `ReaderError: unknown dataset` | 名称拼写或数据集未注册 |
| `ReaderError: no parquet data` | 未 init/compact 或路径错误 |
| `ReaderError` (strict_adj) | 缺 adj_factors 覆盖 |
| PIT 无 `as_of` | 可能包含未来公告（不推荐） |

## 相关文档

- [Python API 参考](../reference/python-api.md)
- [产品边界](../architecture/overview.md)

## 衍生品研究

逐合约期货、期权链、连续序列与 Greeks 不使用股票 universe/复权假设。完整查询与质量验收例子见 [商品期货与期权](../recipes/derivatives.md)。
