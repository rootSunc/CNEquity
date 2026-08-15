# steps 模块

路径：`src/cn_market_lake/steps/`

内置采集步骤定义。每个 step 是一个 `@register_step` 函数，负责调用 adapter、校验 schema、写 staging。

**组织原则**：按数据层（L0–L8）一文件一层，`finalize.py` 收尾。

---

## 注册机制

`steps/__init__.py` import 所有子模块触发注册：

```python
from cn_market_lake.steps import reference, bars, events, ...  # noqa
```

当前 **40 个**注册 step（37 采集 + 3 finalize）。

---

## 模块与 step 对照

### reference.py（L0）

| Step | 数据集 | Worker | 主源 |
|------|--------|--------|------|
| instruments | instruments | 否 | tdx_protocol + EM list_date |
| trading_calendar | trading_calendar | 否 | exchange_calendar 种子 |
| trading_status | trading_status | 否 | eastmoney；init 后可选 baostock ST 回填 |

### bars.py（L1）

| Step | 数据集 | Worker | 主源 |
|------|--------|--------|------|
| daily_bars | daily_bars | **是** | tdx_protocol（失败时东财备源快照） |
| index_bars | index_bars | 否 | tdx_protocol |

### intraday.py（L1，可选）

| Step | 数据集 | 频率 | 源端视野 | 全市场体积 |
|------|--------|------|---------|-----------|
| minute_bars | minute_bars | 1m | 95 个交易日 | 约 35MB/日、8.4GB/年 |
| minute_bars_5m | minute_bars_5m | 5m | 491 个交易日（约 2 年） | 约 6MB/日、1.5GB/年 |

两个 step 由 `_register_intraday_steps()` 从注册表生成——加一个频率是加一条 `DatasetSpec`，不是在四个模块里各改一处。

`group="intraday"`，**不在默认 daily wave 上**，且 `[minute_bars].enabled` 默认 false——不该把 8.4GB/年 落在没主动要它的用户头上。入口只有 `cml run daily --group intraday` 和 `cml backfill <dataset>`。

一个数据集只放一个频率：视野差 5 倍，而一个数据集只有一个水位和一个 `coverage_start`，混在一起两边都会说谎。抓哪些频率由 `[minute_bars].frequencies` 列出；范围由 `[minute_bars].scope` 决定：`index:<symbol>`（默认沪深300，约 300 只）/ `watchlist` / `all`。

**5m 是唯一有真历史的日内频率**，且 15m/30m/60m 可从它精确聚合——见 [catalog.md 历史视野](../datasets/catalog.md)。

### events.py（L2）

| Step | 数据集 | 主源 |
|------|--------|------|
| corporate_actions | corporate_actions | 日更东财 / 回填 TDX |
| announcement_index | announcement_index | cninfo |

### fundamentals.py（L3）

| Step | 数据集 | 主源 |
|------|--------|------|
| valuation_metrics | valuation_metrics | 东财快照；回填 baostock |
| financial_statement_items | financial_statement_items | eastmoney |

### capital.py（L4）

| Step | 数据集 |
|------|--------|
| fund_flow | fund_flow |
| northbound_holdings | northbound_holdings |
| northbound_flows | northbound_flows |
| margin_trading | margin_trading |
| dragon_tiger | dragon_tiger |
| block_trades | block_trades |

均走 eastmoney adapter。

### structure.py（L5）

| Step | 数据集 |
|------|--------|
| sector_members | sector_members |
| index_constituents | index_constituents |
| industry_members | industry_members |

### macro_risk.py（L6/L8）

| Step | 数据集 |
|------|--------|
| macro_indicators | macro_indicators |
| market_breadth | market_breadth（derive 自 daily_bars） |
| share_unlock_schedule | share_unlock_schedule |
| regulatory_events | regulatory_events |

### research.py（L4/L7）

| Step | 数据集 |
|------|--------|
| institutional_holdings | institutional_holdings |
| analyst_consensus | analyst_consensus |
| sentiment_scores | sentiment_scores（derive） |

### rotation.py（L7 轮动）

| Step | 数据集 | 主源 |
|------|--------|------|
| hot_rank | hot_rank | eastmoney |
| sector_bars | sector_bars | ths（日更与历史同源，见下） |
| sector_fund_flow | sector_fund_flow | eastmoney |
| news_headlines | news_headlines | eastmoney |

`sector_bars` 为 snapshot 语义；历史由 `cml backfill sector_bars` 写入（一次性）。
日更与历史**刻意同源**（同花顺）：早先用 TDX 历史拼东财日更，同一 `sector_code`
下混进了两个指数基期，拼接日出现跨 439 个板块 +79% 的假跳变。`[sources.ths]`
关闭时该 step 直接报错，不会静默回落到别的源。

### finalize.py

| Step | 作用 |
|------|------|
| compact | staging → curated，更新水位 |
| derive_adj_factors | Sina hfq → derived |
| audit | 质量 findings + source_diff |

---

## 公共工具

### common.py

- `BACKFILL_START = date(2016, 1, 1)`
- `is_trading_day(cfg, d)`
- `incremental_window(cfg, dataset, trade_date)` — 基于水位
- `write_simple(cfg, dataset, run_id, df)` — 非 worker 写 staging

### http_common.py

HTTP 类数据集共用：

- `run_incremental_fetched()` — 按交易日迭代
- `write_fetched()` — 校验 + 写 staging

---

## Step 函数签名

```python
def step_xxx(cfg: Config, trade_date: date, run_id: str, ctx: dict) -> dict:
    # 返回 {"rows": int, "batches": ..., ...} 供 manifest 汇总
```

`ctx` 可传递 wave 内共享上下文（如 `symbols_to_rebackfill`）。

---

## Wave 配置示例

见 `configs/cn-market-lake.example.toml`：

- Wave 1：L0 并行
- Wave 2：corporate_actions → daily_bars 串行（除权触发重抓）
- Wave 3：index_bars
- Wave 4：finalize 链

调度组（`--group`）是 steps 子集 + 末尾 `compact`。

---

## 相关文档

- [adapters](adapters/README.md)
- [数据流](../architecture/data-flow.md)
- [新增数据集](../development/adding-dataset.md)
