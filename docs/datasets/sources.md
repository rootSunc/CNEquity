# 数据集目录（逐源限制与更新频率）

各数据集的主源、更新频率与已知限制。

### 图例

- **波次（Wave）：** 日更批处理中的 step 名
- **按需（On-demand）：** 首次查询时由 `OnDemandService` 拉取

---

### MVP-P0

#### instruments

| 项 | 值 |
|------|-------|
| 波次 | `instruments`（Wave 0） |
| 主源 | tdx_protocol（内置 security_list） |
| 备源 | baostock（仅 `--backfill`，补退市标的） |
| 频率 | 每日 |
| 主键 | symbol |
| 股票池 | SH/SZ/BJ 前缀白名单 60/68/00/30/92 |
| 已知限制 | 快照中消失时推断 `delist_date`；东财补充 `list_date` |

#### trading_calendar

| 项 | 值 |
|------|-------|
| 波次 | `trading_calendar`（Wave 0） |
| 主源 | tdx_protocol |
| 备源 | 交易所 CSV |
| 频率 | 年度刷新 + 每日检查 |
| 主键 | trade_date |

#### trading_status

| 项 | 值 |
|------|-------|
| 波次 | `trading_status`（Wave 0） |
| 主源 | eastmoney（push2 clist 风险警示板 + datacenter `RPT_CUSTOM_SUSPEND_DATA_INTERFACE`） |
| 备源 | baostock `query_all_stock(day)` 快照（配置 `[[failover.datasets]] name="trading_status"` 门控；东财失败时 SH/SZ 用 baostock，BJ 尽力走东财停牌腿否则记 `n_bj_defaulted` 默认 normal） |
| 频率 | 每日 |
| 主键 | (symbol, trade_date) |
| 已知边界 | BJ 的 ST 标签两源均不覆盖；baostock 未包含当日数据时备份拒绝（宁缺勿假）；停牌接口自 2026-08 起需 `DATETIME`/`MARKET` filter 与 `SUSPEND_START_DATE/SUSPEND_END_TIME` 列 |

#### daily_bars

| 项 | 值 |
|------|-------|
| 波次 | `daily_bars`（Wave 1，依赖 corporate_actions） |
| 主源 | tdx_protocol（未复权，SH/SZ） |
| 备源 / 路由 | tip 缺口：eastmoney **clist**（分钟级）；多日窗口：eastmoney **kline**；BJ：sina |
| 频率 | 每日增量；init 时全量回填 |
| 主键 | (symbol, trade_date) |
| 重拉 | 当日 `corporate_actions` 的除权日对应标的 |
| 已知限制 | TDX 限速；建议 workers ≤ 8；clist 只有当日快照，须用 run 的 `trade_date` 打戳（ADR-0005 routing） |

#### index_bars

| 项 | 值 |
|------|-------|
| 波次 | `index_bars`（Wave 2） |
| 主源 | tdx_protocol |
| 备源 | eastmoney |
| 频率 | 每日 |
| 主键 | (symbol, trade_date, frequency) |

#### trade_ticks

| 项 | 值 |
|------|-------|
| 组 | `ticks`（不在任何默认调度上；`cne run daily --group ticks`） |
| 主源 | tdx_protocol（分笔命令 `0x0fb5`） |
| 备源 | **无，且这是有意的**（见下） |
| 频率 | 按需 / 手动 |
| 主键 | (symbol, trade_date, tick_seq) |

**为什么不设备源。** 备源的价值在于主源失败时还能拿到同一份数据，而分笔没有这样的替代品：

| 候选 | 历史深度 | 判断 |
|------|---------|------|
| TDX 历史分笔 | **回溯至 2024-01-02** | 唯一有历史深度的免费源 → 主源 |
| 腾讯（`stock_zh_a_tick_tx_js`） | 仅最近一个交易日 | 补不了历史 |
| 东财（`stock_intraday_em`） | 仅最近一个交易日 | 同上 |
| 新浪（`cn_bill.php`） | 近期，且**只给 ≥400 手大单** | 残缺 |
| 交易所 Level-2 | 完整逐笔 | **需付费授权，明确非目标** |

写一个只能补一天的备源，只会制造「有 fallback」的错觉——真正需要 fallback 的场景（回填历史）它一天都补不了。
所以 `failover` 不为 `trade_ticks` 登记备源：**单源即契约**，TDX 不可达时这个数据集就是拉不到。

#### commodity_bars

| 项 | 值 |
|------|-------|
| 组 | `macro_risk`（日更） |
| 主源 | **sina**（国内主连 15 个 + 外盘窄集 COMEX 金 `GC0.CMX`） |
| 备源 | eastmoney push2his —— **不自动回退**，需显式开启；该源间歇性拒绝请求（实测直连与大陆出口均 0/12），曾导致每次日更空烧 151 秒 |
| 覆盖 | 各合约回溯至自身上市日：CU0/AL0 2005、TA0 2006、ZN0 2007、AU0 2008、RB0 2009、J0 2011、AG0 2012、I0/JM0 2013、HC0/MA0 2014、NI0 2015、SC0 2018、LC0 2023 |
| 回填 | `cne backfill commodity_bars`（默认自 2020-01-01；可用 `--start`/`--end`） |
| 主键 | (symbol, trade_date) |
| 已知限制 | 主连非真实交割月；夜盘归结算日；水位按 SSE 日历近似；新浪该接口不提供成交额，`amount` 为空（主连拼接后 price×volume 不是当日成交额，故不派生）；伦敦金等未收录 |

#### corporate_actions

| 项 | 值 |
|------|-------|
| 波次 | `corporate_actions`（Wave 1，先于 daily_bars） |
| 主源 | tdx_protocol 除权 |
| 备源 | eastmoney datacenter |
| 频率 | 每日 |
| 主键 | (symbol, ex_date, action_type) |
| 输出 | manifest 元数据 `symbols_to_rebackfill` |
| **已知缺口** | 已退市标的的历史除权除息几乎全缺——2026-08 实测 109/111 个「原始收益率与 hfq 复权收益率不符」的审计发现都是已退市标的（北交所 106 个 + 非北交所 3 个）。**两个源都直接验证过**：`tdx_protocol` 的 `xdxr()` 传对市场号（market=2）后对这批标的仍返回 0 条；`eastmoney` 的历史快照（`meta/source_snapshots/corporate_actions`，覆盖 2015-09-29 起）里这批标的同样一条没有。是两个源都不再对已从其在线标的列表里消失的证券提供除权除息历史，跟标的所在市场、代码前缀无关（92xxxx 前缀里未退市的 328 只覆盖率 96%，同样是 92 前缀但已退市的 1 只覆盖率 0）。baostock 的 `query_dividend_data` 直接拒绝北交所代码（`股票代码未标识sh或sz`），不能顶上。不是本项目的代码缺陷，也不是限流——是这两个源本身对已退市证券的历史除权数据保留策略。`cne audit` 把这批发现单独归为 `missing_corporate_action_delisted`（info 级，一条汇总），不再对仍在交易的标的发出的 `missing_corporate_action`（warning 级）掺在一起。另有「缩股/减资/合股」等股本重组，不属于本数据集的四类分红除权事件；复权收益核对会用 `share_structure.change_reason` 做二次解释，并记为 `adjustment_explained_by_share_structure`（info），避免把已记录的股本重组误报成缺失除权 |

#### adj_factors（derived）

| 项 | 值 |
|------|-------|
| Step | `derive_adj_factors`（finalize 波次） |
| 主源 | sina（qfq/hfq 因子序列） |
| 输入 | daily_bars 交易日 + 外部因子 API |
| 频率 | compact 之后每日 |
| 主键 | (symbol, trade_date, adjust_type) |
| 说明 | 外部累计因子对齐 daily_bars；`adj_close = close * factor` |
| **已知缺口** | 新浪**确实覆盖北交所**（`bj430017` 等都能取到）。此前 260 只股票没有因子并非源的问题，而是本 derive 是**从水位向前追加**的：`cne backfill daily_bars` 补进来的历史日期在水位之后方，永远轮不到。现已自愈（见下），残留 12 只：5 只 2025-04-30 退市的北交所标的 + 7 只已上市未交易的新股，两者新浪都取不到 |
| **查询侧后果** | `load(adjust="hfq")` 默认 `strict_adj=False`，缺因子的行按 `factor=1.0` 返回，即**未复权价出现在复权结果里**，只由 `adj_is_exact=False` 标记。自愈后实测一年窗口 + `universe="all_a"`：**65 行（0.005%）**，其中 46 行 `close>0`——修复前是 10,480 行（0.77%）|
| **怎么办** | 要严格失败而不是静默降级：`load(..., strict_adj=True)`。**它不是默认值**：新上市的票在拿到第一个因子前必然缺，所以严格模式会让 `universe="all_a"` 的 hfq 查询长期抛错。默认容忍 + `adj_is_exact` 标记 + 审计告警，是在「不静默污染」和「查询可用」之间的取舍 |
| **自愈** | `derive_adj_factors` 每次增量运行都会找出「有 bar 但因子够不到」的标的并重排其完整历史，单次上限 500 只。所以 `cne backfill daily_bars` 补的历史会在随后的日更里自动补上因子，无需 `--full` |

---

### v1.0-full（第二批）

#### fund_flow

| 项 | 值 |
|------|-------|
| 分组 | capital@17:00 |
| 主源 | eastmoney |
| 主键 | (symbol, trade_date) |

#### northbound_holdings

| 项 | 值 |
|------|-------|
| 分组 | capital@17:00 |
| 主源 | eastmoney（`RPT_MUTUAL_HOLDSTOCKNORTH_STA`） |
| 主键 | 见 [schema.md](schema.md) |
| 已知限制 | 2024-08 起按季度披露，历史只能向前累积（EM 对历史 `TRADE_DATE` 返回 0 行） |

#### northbound_flows

| 项 | 值 |
|------|-------|
| 分组 | capital@17:00 |
| 主源 | eastmoney 沪深港通资金历史（`RPT_MUTUAL_DEAL_HISTORY`，`MUTUAL_TYPE` 001 沪股通 / 003 深股通） |
| 主键 | 见 [schema.md](schema.md) |
| 覆盖 | **2014-11-17 → 2024-08-16**（深股通自 2016-12-05）。回填：`cne backfill northbound_flows` |
| 已知限制 | 交易所自 **2024-08-19** 起停止披露每日北向净买入，此后所有行 `NET_DEAL_AMT` 为 null。这些行**不落盘**（不补零），因此水位永久停在 2024-08-16，`cne status` 会一直显示 STALE——这是源的事实，不是流水线故障 |
| 单位 | 报表金额列按 **百万元**，落盘换算为元。同一行的 `HOLD_MARKET_CAP` 却是元——该报表混用单位，改字段时要重新标定 |
| 一次一请求 | 该报表拒绝 `TRADE_DATE` 范围谓词（`InputMismatchException`），所以取全量后在本地切窗；两条通道全史约 5k 行 |

#### margin_trading

| 项 | 值 |
|------|-------|
| 分组 | capital@17:00 |
| 主源 | eastmoney |
| 主键 | (symbol, trade_date) |

#### valuation_metrics

| 项 | 值 |
|------|-------|
| 日更源 | eastmoney（clist 实时快照，覆盖当日 trade_date） |
| 历史源 | baostock（`cne backfill valuation_metrics`；按标的每日 PE/PB/PS 回填至 2016） |
| 主键 | (symbol, trade_date) |
| 已知限制 | baostock 历史含 pe_ttm/pb/ps_ttm；`float_mv`←amount/turn，`total_mv`←Q4 totalShare×close；日更 EM 快照覆盖最新交易日 |

#### announcement_index

| 项 | 值 |
|------|-------|
| 主源 | cninfo |
| 主键 | announcement_id |
| 说明 | 正文 on-demand（`announcement_body`）尚未实现；批量路径仅索引 |

#### share_structure / shareholder_counts

| 项 | 值 |
|------|-------|
| 主源 | eastmoney（`RPT_F10_EH_EQUITY` / `RPT_F10_EH_HOLDERNUM`） |
| 分组 | fundamentals@17:35 |
| 主键 | (symbol, change_date, announce_date) / (symbol, count_date, announce_date) |
| 采集方式 | **按日期区间整市场扫**，不是按标的循环，也不是按报告期。`RPT_F10_EH_EQUITY.END_DATE` 是股本变动日；股东户数在旬末/月末也披露（2025-07-10 有 894 行）。只扫季末会捞回一堆看着合理的行，然后静默漏掉其余大部分 |
| 日更范围 | 按 `NOTICE_DATE` 回看 30 天。窗口开在公告日而不是变动日：几周前生效的变动今天才公告，按变动日开窗永远看不到它 |
| PIT | `announce_date` 取自 `NOTICE_DATE`，进主键 |
| 源端历史底 | `share_structure` **1990**（1990 年 19 行，之前没有）；`shareholder_counts` **1992**（1992 年 25 行，1990/1991 为空）。均为固定底，不随今天滚动 |

#### top_holders

| 项 | 值 |
|------|-------|
| 主源 | eastmoney（`RPT_F10_EH_HOLDERS` 全口径 + `RPT_F10_EH_FREEHOLDERS` 流通口径） |
| 分组 | **不在日更波次**。两张报表 × 约 110 页 × 两个报告期 ≈ 440 页，是上面两个的 40 倍；放进 fundamentals 会挤掉 macro_risk 整组。用 `cne backfill top_holders` 单独跑 |
| 主键 | (symbol, record_date, holder_scope, holder_rank, holder_name, announce_date)。**holder_name 必须进主键**：持股数相同的股东共用一个 rank（600010.SH 2025-06-30 第 9 名是博时和易方达两家，各 167,831,580 股），不带名字去重会把其中一家直接删掉，单期全市场 1,730 行 |
| 口径 | 一张表两个口径，靠 `holder_scope` 区分：`total`=前十大股东，`float`=前十大流通股东。`holding_pct` 两边分母不同（占总股本 vs 占流通股），**不可直接比较** |
| PIT | `RPT_F10_EH_HOLDERS` 没有 `NOTICE_DATE`，其披露日按 (symbol, report_period) 从 FREEHOLDERS 借；借不到的行**丢弃**而不是拿期末日期充数 |
| 采集方式 | 按 `END_DATE` 区间扫（全口径报表没有 `NOTICE_DATE`，两张报表若按不同列开窗，借披露日就没得匹配）。日更回看 240 天 |
| 源端历史底 | **2003**，且卡的是 PIT 不是数据可得性。`RPT_F10_EH_HOLDERS` 本身能回到 1990 年代，但它没有 `NOTICE_DATE`，披露日要从 `RPT_F10_EH_FREEHOLDERS` 借——而后者 1999-2002 全是 0 行，2003 年才有 13,853 行。2003 之前的全口径行借不到披露日，按设计会被丢弃（不拿期末日期充数），所以往前回填是取回约 11.2 万行、一行都写不进去。`cne backfill --start` 早于 2003 会直接报错拦下 |
| 分页 | 单期超过 EastMoney 的 100 页上限，靠 `keyset_column="SECUCODE"` 换锚点翻过去（见 `datacenter.py`） |

---

### 按需数据集（On-demand）

不在日更波次中。缓存于 `meta/on_demand/`，可选写入 DuckDB 表。

| 数据集 | 来源 | 触发 |
|---------|--------|---------|
| stock_news | eastmoney | `cne query --dataset stock_news --symbol` |
| research_reports | eastmoney reportapi | 按标的 |
| announcement_body | cninfo | **未实现**（勿写入 `[on_demand].datasets`） |
| financial_reports | sina / gpcw | **未实现**（勿写入 `[on_demand].datasets`） |

---

### Meta 数据集

| 数据集 | 存储 |
|---------|---------|
| ingestion_runs | manifest.db |
| ingestion_batches | manifest.db |
| quality_findings | meta/quality/findings/ |
| source_diffs | meta/quality/source_diffs/ |
| data_catalog | 由 `cne catalog` 生成 |

---

### 源可用性矩阵

| 来源 | 协议 | MVP 用途 | 备源 | 降级策略 |
|--------|----------|-----------|--------|---------|
| tdx_protocol | TCP | bars、instruments、calendar | eastmoney clist（tip 路由）/ kline（多日） | tip 缺口进 curated（ADR-0005）；snapshot 供 diff |
| sina | HTTP | adj_factors（qfq/hfq） | — | 跳过该标的 + quality finding |
| eastmoney | HTTP | 公司行为备源、资金面 | — | 跳过 + quality finding |
| cninfo | HTTP | announcement_index | — | 仅按需 |
| baostock | TCP | 退市标的、历史 ST、估值回补 | — | 仅 `--backfill` |
| pboc | HTTP | 社会融资规模增量（`macro_indicators`） | — | 主写入要求全量序列；单年失败会阻止本次写入，避免带断档推进水位 |
| nbs | HTTP | **仅审计**：PMI 发布稿，对照 `macro_indicators` | — | 缺省关闭；不可达时静默跳过 |
| exchange | HTTP | **仅审计**：上交所/深交所上市列表，对照 ST 标签 | — | 缺省关闭；不可达时静默跳过 |

> **AkShare 已不再被任何适配器调用**（[issue #3](https://github.com/rootSunc/cnequity/issues/3)）。
> 它此前的两个调用点分别指向本项目已经直连的端点：ST 集合走的是同一个东财
> push2 clist 板块与同一个 `fs` 过滤器，PMI / 货币供应量走的是同一批东财
> datacenter 报表。它提供的不是第二个口径，而是同一个口径外面的一层解析。
> 它也已从依赖里移除，`pip install cnequity` 不再装它。

调度与主备切换见 [运维 Runbook](../operations/runbook.md)。
