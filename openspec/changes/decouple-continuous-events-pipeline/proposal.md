# decouple-continuous-events-pipeline: Proposal

## Why

### 1. 业务背景 (Background)

在 CNEquity 现有的数据编排架构中，所有日常批处理数据集被组织在 `[job.daily.groups]` 体系下（如 `capital`, `research`, `macro_risk`, `core`）。引擎层（`cnequity/orchestrator/engine.py`）对所有 `daily` 任务统一实施了强行日前判定门禁：
`if not backfill and job_name != "init" and not is_trading_day(self.config, trade_date): -> skipped_non_trading_day`

这一设计对严格依赖交易所撮合与结算的结构化数值行情（如 `daily_bars` 日K、`fund_flow` 资金流、`margin_trading` 两融）是极其合理的。然而，随历史演进，四类**全天候流动、非结构化事件与资讯类数据集**被顺手混编入日频行情组中：
1. `announcement_index`（上市公司披露公告）：混在 `capital` 组（17:00）；
2. `regulatory_events`（监管处罚与问询）：混在 `macro_risk` 组（17:55）；
3. `flash_news_wire`（7×24 实时快讯线）：混在 `research` 组（18:15）；
4. `news_headlines`（市场新闻要闻）：混在 `research` 组（18:15）。

这导致了严重的**业务语义与时效性错配**。

### 2. 核心痛点与拆分必要性 (Necessity)

1. **夜间黄金披露期（19:00 ~ 23:00）数据严重滞后 24 小时**：
   A 股上市公司披露有其自身规律——绝大多数董秘并非在盘后 17:00 前发公告，**晚间 19:00~23:00 才是上市公司年报、重大重组、黑天鹅立案、业绩预告发布的最高峰期**。目前 `capital` 组在 17:00 跑完即“关门”，导致当夜所有重大公告必须等到次日下午 17:00 才能入库，量化策略与投研看板整整滞后近 24 小时。
2. **周末非交易日“信息黑洞”**：
   周五晚间至周六日全天，上市公司仍会密集公告（如 2026-08-01 周六单日披露达 1,371 条），部委与央行亦常在周末发布宏观政策与快讯。但在 `is_trading_day` 拦截下，整个周末系统处于“停摆断流”状态。
3. **`capital` 核心资金面流水线的致命“木桶短板”**：
   `fund_flow`、`margin_trading`、`valuation_metrics` 走高速接口，通常 1~2 分钟即可出数，成功率极高；而 `announcement_index` 需遍历巨潮资讯（CNINFO）28 个分类 API，频发翻页截断、长耗时网络超时或空结果报错。它一人经常把 `capital` 组拖长至 10+ 分钟，甚至导致整个资金流流水线被打标为 `degraded` 或 `failed`。
4. **采集频次与生命周期无法独立定制**：
   行情每天收盘算一次即可，而快讯与公告需要作为**全天候周期任务（快讯固定每 6 小时增量抓取一次：00:00、06:00、12:00、18:00）**独立运转。绑死在 `daily` 组中使其完全失去了按独立频次调度的灵活性。

### 3. 业务价值 (Business Value)

- **策略决策与投研时效显著提升**：
  重大突发公告与 7×24 快讯从“次日盘后入库”提升至“发布后数小时内入库”，周末重大政策与公告周一早盘前即可完全沉淀至湖区，为周一量化选股与大模型舆情分析争取最充足的时间窗口。
- **交易日核心流水线稳定性飞跃**：
  `capital` 组剥离公告后，回归纯粹的量化资金与估值计算，成功率趋近 100%，耗时骤降至 1 分钟左右，彻底消除下游出数延迟隐患。
- **架构治理清晰化**：
  在 CNEquity 内部明确划清**“交易日结批处理（Trading-Day EOD Batch）”**与**“全天候周期事件流（Continuous Periodic Event Stream）”**两条平行跑道。

---

## What Changes

- **A. 原有 Daily 组“瘦身”与解耦**：
  - 从 `job.daily.groups.capital` 中移除 `announcement_index`；
  - 从 `job.daily.groups.research` 中移除 `flash_news_wire` 和 `news_headlines`；
  - 从 `job.daily.groups.macro_risk` 中移除 `regulatory_events`。
- **B. 引入独立的事件资讯组与调度指令**：
  - 新增独立的事件任务编排支持：
    - `corporate_events`: 编排 `["announcement_index", "regulatory_events", "compact"]`；
    - `news_wire`: 编排 `["flash_news_wire", "news_headlines", "compact"]`；
  - 允许通过 `cne run events --group <name>` 或带有 `--ignore-calendar` 参数的 CLI 命令独立执行，不再受限于 `is_trading_day` 拦截。
- **C. 下游衍生依赖时序解耦**：
  - `research` 组的 `sentiment_scores`（依赖公告与新闻）保持在 18:15 运行；
  - 规定 `corporate_events` 和 `news_wire` 在交易日安排在 18:00 前完成当日常规写入，确保 `sentiment_scores` 的消费端数据源最新且完整。

---

## Capabilities

### New Capabilities

- `continuous-events-pipeline`: 支持将公告、监管事件、快讯与要闻作为不受交易日历约束的全天候周期性数据流独立拉取、独立落盘与独立 Compact。

### Modified Capabilities

- `daily-sync-scheduler`: 原有的 `capital`、`research` 与 `macro_risk` 组职责纯化为纯量化行情与分析指标，不再承担爬取长耗时外部非结构化文本的任务。

---

## Impact

- **配置文件**：
  - `configs/cnequity.toml` 中的 `[job.daily.groups]` 剔除上述 4 个事件步骤；
  - 增加对应的新独立事件组配置，上层调度（如 Datalake 队列或独立 timer）可按自定义周期（如每 2 小时或工作日夜间+周末）调用。
- **CNEquity 代码改动**：
  - `src/cnequity/cli/`: CLI 子命令支持执行独立事件流水线或增加 `--ignore-calendar` 标志。
  - `src/cnequity/orchestrator/engine.py`: 允许特定 job 绕行交易日历跳过逻辑。
- **存储与数据流转兼容性**：
  - **100% 兼容**：底层的 Parquet 分区模式（`announce_date` / `publish_date`）、Staging 写入逻辑与 Curated 目录完全不变；
  - `daily_bars`、`index_bars`、`derive_adj_factors` 等核心行情数据流转完全不受影响。
