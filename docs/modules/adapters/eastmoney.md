# eastmoney 适配器

路径：`src/cn_market_lake/adapters/eastmoney/`

东方财富 HTTP API（datacenter、clist、行情接口等）。资金面、估值快照、结构成员、新闻、ST/停牌等的主要 HTTP 源。

---

## 核心基础设施

| 文件 | 职责 |
|------|------|
| `em_auth.py` | `EastMoneyClient`、NID cookie、请求头 |
| `datacenter.py` | 通用 datacenter API 封装（`code=9501` 列不存在 → fail-loud） |
| `datacenter_contracts.py` | report→columns 契约清单（CI + 直播探针共用） |
| `clist.py` | 分页 clist（全市场列表类接口） |
| `common.py` | EM 代码 ↔ `symbol` 互转 |

限速：`[sources.eastmoney].min_interval_seconds`（跨进程文件锁）。

### datacenter 列契约

EM 改列名会整报 `code=9501`。适配器里的 `_REPORT` / `_COLUMNS` 是运行时真源；
[`datacenter_contracts.py`](../../../src/cn_market_lake/adapters/eastmoney/datacenter_contracts.py)
只做清单导入，供离线测试与直播探针迭代。改列后：

1. 更新对应 adapter 常量  
2. `uv run pytest tests/unit/test_datacenter_contracts.py -q`  
3. `uv run pytest -m network tests/unit/test_datacenter_live_contracts.py -q`

---

## 功能模块

| 文件 | 数据集 / 用途 |
|------|----------------|
| `instruments.py` | instruments `list_date`  enrichment |
| `bars.py` | daily_bars tip **clist** gap-fill + 多日 **kline** 备源；snapshot 供 `source_diff` |
| `corporate_actions.py` | corporate_actions **daily 主源** |
| `capital.py` | fund_flow, margin_trading, northbound_*, dragon_tiger, block_trades |
| `valuation.py` | valuation_metrics 当日快照 |
| `fundamentals.py` | financial_statement_items（日更 NOTICE_DATE；backfill 报告期自 2001，CLI 可裁剪） |
| `sectors.py` | sector_members |
| `industry.py` | industry_members |
| `index_constituents.py` | index_constituents |
| `trading_status.py` | ST 列表、停牌列表 |
| `institutional.py` | institutional_holdings |
| `consensus.py` | analyst_consensus |
| `share_unlock.py` | share_unlock_schedule |
| `stock_news.py` | stock_news（on-demand / sentiment） |
| `rotation.py` | hot_rank, sector_bars, sector_fund_flow, news_headlines |

---

## 语义注意

### snapshot 类

`valuation_metrics`、`fund_flow`、`sector_members` 等接口返回**当前页面快照**，step 用 `trade_date` 打戳写入。历史值不可伪造 — 见 `fetch_semantics="snapshot"`。

历史估值：`valuation_metrics` 的 `backfill_source=baostock`。

### sector_bars 不在这里

板块 OHLC 已整体迁到同花顺，日更与历史同源，见
[逐源限制](../../datasets/sources.md) 与 [steps](../steps.md)。本适配器不再参与
`sector_bars` 采集。

可选 `cml derive sector_routing` 生成 EM×TDX 名称映射（**不参与** sector_bars 采集）。

### 代理

`[sources.eastmoney] proxy = "http://127.0.0.1:7897"` 对**所有**东财主机生效
（push2 / push2his / datacenter / reportapi）；未配置时仍可读 `HTTPS_PROXY` /
`HTTP_PROXY`。这是海外出口唯一需要的开关——大陆网络不需要配。

### 北向持股

2024-08 起逐日披露变化；持股数据季频为主。`northbound_holdings` 的 `max_staleness_days=100`。

### ST / 停牌

`trading_status` 日更主源；**不提供**长历史 ST，需 baostock 回填 + 派生停牌。

---

## 主备角色（Failover）

- `daily_bars`：TDX 主源；tip 缺口东财 **clist** 路由进 curated（ADR-0005）；snapshot 供 diff
- `corporate_actions`：**日更主源**

---

## 相关文档

- [capital step](../steps.md)
- [数据集目录 — L4/L5](../../datasets/catalog.md)
