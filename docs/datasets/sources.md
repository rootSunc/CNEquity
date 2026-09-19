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
| 补充源 | bse（北交所行情板，唯一的在册 BJ 名单与证券简称）；sina（代码空间扫描的 live-but-missing 桶） |
| 频率 | 每日；历史回填默认从 2001-01-01 起，支持 `--start` 缩小窗口 |
| 主键 | symbol |
| 股票池 | 研究股票池使用 SH/SZ/BJ 前缀白名单 60/68/00/30/92；ETF/LOF 使用独立前缀 51/52/56/58/15/16，保留在 instruments/daily_bars |
| 已知限制 | 快照中消失时推断 `delist_date`；东财分别从 A 股与 ETF/LOF clist 补充 `list_date`；ETF/LOF 保留在 instruments/daily_bars，但不进入 `all_a` 研究池 |
| 北交所 | TDX 只服务沪深，BJ 名单原先只能靠重放上一次人工代码空间扫描（`scripts/delisted_ops.py discover`），扫描之后上市的票永远进不来 —— 2026-09-15 实测有 16 只在放量成交却在湖中零行。现在每日读一次北交所行情板补齐，并顺带带回 TDX 侧一直缺的 `name` |
| 顺序 | 两个补充源都在 `list_date` 富化**之前**合并。反过来的话它们带进来的行永远 `list_date` 为空，而「无 `list_date` 且从未有 bar」正是未上市占位符的判据 —— 发现了却永远不取数 |

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
| 主源 | eastmoney（ST 板 + 停牌名单） |
| 备源 | exchange（沪深交易所行情板，独立故障域） |
| 补充源 | derived（`derived_bar_gap` / `derived_delisted`）；bse（北交所行情板） |
| 频率 | 每日 |
| 主键 | (symbol, trade_date) |
| 历史 ST 回补 | Baostock 覆盖 SH/SZ；可选 Tushare Pro `bak_basic`（2016）+ `stock_st`（2017-01-01 起）覆盖 BJ，需 token；缺少源端覆盖时审计保持 warning |
| 列语义 | `status` 只表示交易状态（`normal`/`suspended`/`delisted`），ST/*ST 在独立列 `risk_warning`。两者正交：一只 ST 股停牌时两个字段同时成立 |
| 退市标的 | 不向行情板询问（板答不了），由 `instruments.delist_date` 判定，写 `status=delisted`、`is_trading=false`、`source=derived_delisted`，`risk_warning` 取自最终简称；**没有简称时为 null 而不是 false** —— 简称是退市后仅存的 ST 证据，它缺席就是证据缺席 |
| 北交所 | EastMoney 两个源都不覆盖 BJ：ST 板的 `fs` 只选 m:0/m:1，停牌名单同样够不到 —— 湖中历史 15,515 行 EastMoney 来源的 BJ 记录 `is_trading` 无一为 false，同期沪深是 148,933 行里 116 次。现在 BJ 两列都取自北交所自己的行情板（`source=bse`），EastMoney 对未覆盖交易所一律写 null |
| BJ 历史 ST：免费源实测（2026-09-18） | **没有免费源能给出带生效日的 BJ 历史 ST 状态**，以下都实测过，不必再试一遍：<br>· **北交所公告检索**（`bse.cn/disclosure/announcement.html`，2026-08-17 那次，证据存于 `meta/state/historical_st_evidence/bj_free_source_*.json`）：580 只全查完、0 次失败，但只有 **15 只**有关键词命中、**565 只零命中**——而零命中**不能**标记为 normal；生效日公告页不给，要逐份解 PDF；撤销风险警示搜不到任何一条（`bj_revoke_scan`：0 命中，且这不等于不存在）。最终只落了 7 只票 705 行正例，状态 `positive_only_staged_not_full_coverage`<br>· **同花顺 F10**（`basic.10jqka.com.cn/920090/company.html`）：页面能取到（GBK），但**没有「曾用名」字段**<br>· **东财 F10**（`PC_HSF10/CompanySurvey/PageAjax?code=BJ920090`）：**覆盖 BJ**（与上一行的 ST 板 / 停牌名单不同），返回 `SECURITY_NAME_ABBR=*ST同辉`、`FORMERNAME=同辉信息`，**但无任何日期**，且只列一个曾用名<br>· **巨潮**（`data20/companyOverview/getCompanyIntroduction`）：只有行业、成立日、主营与历史沿革长文本，**无证券简称变更记录**<br><br>根因不是抓取技巧：这些源发布的是**事件**，而 ST 日历要的是**状态** —— 必须能说"这天它不是 ST"。带日期的简称序列（Tushare `bak_basic` 走的就是这条）才等价，免费侧没有。<br>**可选项只有三个**：① Tushare token（唯一能补 2016–2026，BJ 全部 349 只都在其覆盖内，因为湖中 BJ 最早行情是 2016-01-04、2016 年前 0 行）；② 把研究窗口起点设在证据完整之后——自接入北交所行情板起（`source=bse`，2026-09-15 起）每日 BJ 简称与 ST 标志都由交易所自己提供，向前的覆盖是完整的；③ 自建公告 PDF 流水线（不推荐：覆盖 349 只 × 10 年，且仍证明不了 565 只零命中标的的 normal） |
| 已知限制 | ST 与 *ST 不做区分（喂本数据集的源都没有这个区分：Baostock 只有 `isST` 布尔，Tushare adapter 早已把 `ST`/`*ST` 归一）。更细的标识在交易所简称，经 `instruments.name` 获取 |

#### daily_bars

| 项 | 值 |
|------|-------|
| 波次 | `daily_bars`（Wave 1，依赖 corporate_actions） |
| 主源 | tdx_protocol（未复权，SH/SZ） |
| 备源 / 路由 | tip 缺口：eastmoney **clist**（分钟级）；多日窗口：eastmoney **kline**；BJ：sina |
| 频率 | 每日增量；init 时全量回填；深历史由同花顺按上市年份回补（股票与 ETF/LOF 均支持） |
| 主键 | (symbol, trade_date) |
| 重拉 | 当日 `corporate_actions` 的除权日对应标的 |
| 已知限制 | TDX 限速；建议 workers ≤ 8；clist 只有当日快照，须用 run 的 `trade_date` 打戳（ADR-0005 routing）；ETF/LOF 无上市日期时按未上市占位符处理，不盲抓深历史 |

#### index_bars

| 项 | 值 |
|------|-------|
| 波次 | `index_bars`（Wave 2） |
| 主源 | tdx_protocol |
| 备源 | eastmoney |
| 频率 | 每日 |
| 主键 | (symbol, trade_date, frequency) |
| **已知限制** | `399001.SZ` 的深历史存在 18 个交易日空洞（1991–1995）。已分别核对 THS 历史接口与 TDX 原始接口，两者都不返回这些日期；这是源端历史序列的共同缺失，不补造 bar，也不把它们写入 `CLOSED_DATES`。`cne audit` 会保留 `info` finding；若出现不在已核实集合中的新缺口，仍会报告 `warning` |

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
| 主源 | eastmoney datacenter（日更） |
| 备源 / 回填 | tdx_protocol 除权（按标的历史回补；`xdxr` 的 category 11「扩缩股」落成 `unit_split`，比例取 `suogu`，category 12「非流通股缩股」不动交易价故排除）；显式 repair：同花顺历史分红页（BJ）；旧码迁移补抓：EastMoney 920xxx 定向报告 |
| 频率 | 每日 |
| 主键 | (symbol, ex_date, action_type) |
| 输出 | manifest 元数据 `symbols_to_rebackfill` |
| 自愈 | 日更会检查最近 10 个交易日里「因子跳变但无除权记录」的标的，对它们补问 TDX `xdxr`（东财日更报表不含份额折算）；补不上的十个交易日后滚出窗口，由审计的 `unrecorded_ex_event` 继续报告 |
| **已知缺口** | 已退市标的的历史除权除息仍可能缺失。缺口数量随湖中退市标的、复权因子和审计窗口变化，**以最新 `meta/quality/health-latest.json` 的 `missing_corporate_action_delisted` finding 为准，不在文档固化样本数量**。**两个默认源都直接验证过**：`tdx_protocol` 的 `xdxr()` 传对市场号（market=2）后对已退市标的仍可能返回 0 条；`eastmoney` 的历史快照（`meta/source_snapshots/corporate_actions`，覆盖 2015-09-29 起）里同样可能没有记录。两源都不会对已从其在线标的列表里消失的证券稳定提供完整历史。现在可用显式 `cne backfill corporate_actions --baostock-repair` 对已退市 SH/SZ 标的补抓 Baostock 的分红、送股、转股事件，并用 `--ths-repair` 对已退市 BJ 标的补抓同花顺历史分红页；对同花顺旧码页没有历史记录的迁移标的，再用 `--eastmoney-bj-repair` 按随代码库保存的北交所旧码→920xxx 映射定向查询 EastMoney。另有 `--eastmoney-date-repair --ex-dates ...`：回补主源是 TDX，东财只在日更的等值过滤下才够得到 2015-09-29 以前的行（报表本身回到 1991 年），这条按指定除权日逐日补，每个日期单独 capture scope 与 batch。四者默认都不参与日更或普通回填，所有修复行保留独立 `source` provenance。不是限流导致的默认缺口，而是各源的历史保留范围不同。`cne audit` 将未修复批次单独归为 `missing_corporate_action_delisted`（info 级），不与仍在交易标的的 `missing_corporate_action`（warning 级）混在一起。另有「缩股/减资/合股」等股本重组，不属于本数据集的四类分红除权事件；复权收益核对会用 `share_structure.change_reason` 做二次解释，并记为 `adjustment_explained_by_share_structure`（info），避免把已记录的股本重组误报成缺失除权 |

#### adj_factors（derived）

| 项 | 值 |
|------|-------|
| Step | `derive_adj_factors`（finalize 波次） |
| 主源 | sina（qfq/hfq 因子序列） |
| 输入 | daily_bars 交易日 + 外部因子 API |
| 频率 | compact 之后每日 |
| 主键 | (symbol, trade_date, adjust_type) |
| 说明 | 外部累计因子对齐 daily_bars；`adj_close = close * factor` |
| **已知缺口** | 股票从 Sina 的 `f` 字段取因子，ETF/LOF 从 `s` 字段取因子（hfq 直接使用 `s`，qfq 使用 `1/s`）。新浪**确实覆盖北交所**（`bj430017` 等都能取到）。过去曾出现过因 derive 只从水位向前追加而导致的因子缺口；现已修复该路径。新上市未交易、已退市或源端无因子的标的仍可能缺失，具体数量以最新 `adj_factor_coverage` / `adj_factor_source_unavailable` finding 和 `meta/quality/health-latest.json` 为准。对正式退市且新浪明确返回空序列的标的，派生会写入 `meta/state/adj_factors.json.source_unavailable_symbols`，停止无效重试但不会伪造因子。 |
| **查询侧后果** | `load(adjust="hfq")` 默认 `strict_adj=False`，缺因子的行按 `factor=1.0` 返回，即**未复权价出现在复权结果里**，只由 `adj_is_exact=False` 标记。实际不精确行数随查询窗口、标的范围和最新因子覆盖变化；请以结果中的 `adj_is_exact=False` 以及最新 `meta/quality/health-latest.json` 的 `adj_factor_coverage` finding 为准。|
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
| 已知限制 | 交易所自 **2024-08-19** 起停止披露每日北向净买入，此后所有行 `NET_DEAL_AMT` 为 null。这些行**不落盘**（不补零），因此水位永久停在 2024-08-16；注册表将该日期标为 `source_retired_date`，`cne status` / `cne verify` 不会把源停止误报为 STALE。 |
| 单位 | 报表金额列按 **百万元**，落盘换算为元。同一行的 `HOLD_MARKET_CAP` 却是元——该报表混用单位，改字段时要重新标定 |
| 一次一请求 | 该报表拒绝 `TRADE_DATE` 范围谓词（`InputMismatchException`），所以取全量后在本地切窗；两条通道全史约 5k 行 |

#### margin_trading

| 项 | 值 |
|------|-------|
| 分组 | capital@17:00 |
| 主源 | exchange（上交所 `queryMargin` + 深交所 `1837_xxpl` tab2 融资融券交易明细） |
| 备选 | eastmoney（`[margin_trading] source = "eastmoney"`，仅由人工切换） |
| 主键 | (symbol, trade_date) |
| 已知限制 | **上交所不公布融券余额**，SH 行 `short_balance` 为 null（不做本地推算）；深交所比上交所晚一个交易日发布，两边都发布后才写入该日，因此比东财路径滞后约一个交易日 |
| 校验 | 2026-08-26 与东财 curated 逐字段比对：3,522 个共同标的四个字段全部完全一致（0 bps），且交易所侧覆盖 4,100 只 vs 东财 3,857 只 |

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
| 分组 | disclosures@20:00 |
| 主键 | announcement_id |
| 日期轴 | **自然日**（`session_scope = "calendar"`）：上市公司周六也披露。因此它属于 `cne run events` 而不是日更批，非交易日返回 0 行是正常现象而非抓取失败 |
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
| data_catalog | 由 `cne stats show --json`（无 stats 表时的直扫回退）生成 |

---

### 源可用性矩阵

| 来源 | 协议 | MVP 用途 | 备源 | 降级策略 |
|--------|----------|-----------|--------|---------|
| tdx_protocol | TCP | bars、instruments、calendar | eastmoney clist（tip 路由）/ kline（多日） | tip 缺口进 curated（ADR-0005）；snapshot 供 diff |
| sina | HTTP | adj_factors（qfq/hfq） | — | 跳过该标的 + quality finding |
| bse | HTTP | BJ 日线 tip 成交额 | — | 仅在与 Sina OHLCV 精确一致时补 amount；否则保留 null + quality finding |
| eastmoney | HTTP | 公司行为备源、资金面 | — | 跳过 + quality finding |
| cninfo | HTTP | announcement_index | — | 仅按需 |
| baostock | TCP | 退市标的、历史 ST、估值回补 | — | 仅 `--backfill` |
| pboc | HTTP | 社会融资规模增量（`macro_indicators`） | — | 主写入要求全量序列；单年失败会阻止本次写入，避免带断档推进水位 |
| nbs | HTTP | **仅审计**：PMI 发布稿，对照 `macro_indicators` | — | 缺省关闭；不可达时静默跳过 |
| exchange | HTTP | `margin_trading` **主源**；`trading_status` / `trading_calendar` / `dragon_tiger` / `block_trades` 备源；`[exchange_audit]` 价格对照 | — | 融资融券由会员单位报送汇总，中间无转售方；龙虎榜与大宗交易只在东财答不上时才问，且两所都不发布北交所（记在 `backup_gaps`）；审计类 finding 为建议性，不让 run 失败 |
| sina_bars | HTTP | Sina 日线兜底（与复权因子端点分开限速） | — | 跳过 + quality finding |
| ths | HTTP | 同花顺公开页：行业、估值 | — | 跳过 + quality finding |
| ths_pages | HTTP | `d.10jqka.com.cn` kline，`sector_bars` 唯一来源 | — | 该数据集**无第二个源**；失败即缺口 |
| ths_bonus | HTTP | 同花顺分红送配页 | — | 限速更保守（3.0s）；跳过 + quality finding |
| ths_official | HTTPS（keyed） | **从不拥有任何一行**（ADR-0008）：仲裁快照、财报回填、深度历史换源 | — | 无 key 时全部 `skipped`，湖保持已有的源不变 |
| tushare | HTTPS（keyed） | 可选：BJ 历史 ST 证据（`stock_st`，起 2017-01-01） | bak_basic 名称证据（2016） | 无 token 时更早的 BJ bar 保持 unresolved，**不认定为正常** |

> **AkShare 已不再被任何适配器调用**（[issue #3](https://github.com/rootSunc/CNEquity/issues/3)）。
> 它此前的两个调用点分别指向本项目已经直连的端点：ST 集合走的是同一个东财
> push2 clist 板块与同一个 `fs` 过滤器，PMI / 货币供应量走的是同一批东财
> datacenter 报表。它提供的不是第二个口径，而是同一个口径外面的一层解析。
> 它也已从依赖里移除，`pip install cnequity` 不再装它。

调度与主备切换见 [运维 Runbook](../operations/runbook.md)。
