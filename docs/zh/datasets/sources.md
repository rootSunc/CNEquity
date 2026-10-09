# 数据集目录（逐源限制与更新频率）

各数据集的主源、更新频率与已知限制。

### 图例

- **波次（Wave）：** 日更批处理中的 step 名
- **按需（On-demand）：** 首次查询时由 `OnDemandService` 拉取

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
| 股票池 | 研究股票池使用 SH/SZ/BJ 前缀白名单 60/68/00/30/92；ETF/LOF 以独立类别保留在 `instruments`，基金日线范围由采集配置与正式资格目录决定 |
| 已知限制 | 快照中消失时推断 `delist_date`；东财分别从 A 股与 ETF/LOF clist 补充 `list_date`，已发布交易所 ETF 目录可再填补基金缺失的上市日。push2 不可用时新上市股票可能暂缺上市日，日线步骤对无上市日标的逐只探测；ETF/LOF 不进入 `all_a` 股票研究池 |
| 北交所 | TDX 证券名单只服务沪深；仅复用旧代码空间扫描会漏掉后来上市的 BJ 标的。现在每日读一次北交所行情板补齐名单和简称 |
| 顺序 | 两个补充源都在 `list_date` 富化**之前**合并。反过来的话它们带进来的行永远 `list_date` 为空，而「无 `list_date` 且从未有 bar」正是未上市占位符的判据 —— 发现了却永远不取数 |

#### etf_profiles

| 项 | 值 |
|------|-------|
| 分组 | research@18:15 |
| 主源 | 上交所 ETF 目录；深交所 ETF 目录与基金目录；逐代码核验的官方指数编制方案 |
| 频率 | 每日当前快照；不能回补尚未观测的历史 |
| 主键 | (symbol, as_of_date) |
| 分类门槛 | 上交所明确境内股票指数类别且有跟踪指数才标 `eligible`；深交所需同时有股票基金类别和按跟踪指数代码精确匹配的官方编制方案。399006、399330 的方案直接证明 A 股范围；399673 须连同上游 399006 方案证明样本范围。其他指数不外推 |
| 完整性 | 上交所总数必须等于实际行数；深交所两份目录的 ETF 代码集合必须一致；三份目录及使用的官方编制方案原始响应先归档，再允许发布；方案改变或获取失败时保留旧快照并报告失败 |

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
| 波次 | `trading_status`（core；依赖 `instruments`，排在它之后） |
| 主源 | eastmoney（ST 板 + 停牌名单） |
| 备源 | exchange（沪深交易所行情板，独立故障域） |
| 补充源 | derived（`derived_bar_gap` / `derived_delisted`）；bse（北交所行情板） |
| 频率 | 每日 |
| 主键 | (symbol, trade_date) |
| 历史 ST 回补 | Baostock 覆盖 SH/SZ；可选 Tushare Pro `bak_basic`（2016）+ `stock_st`（2017-01-01 起）覆盖 BJ，需 token；缺少源端覆盖时审计保持 warning |
| 历史命令边界 | `cne backfill trading_status --symbols ...` 遇到 BJ 代码会拒绝 Baostock 历史 ST 路径；BJ 应使用有实际覆盖凭证的独立来源，不把空结果解释为 normal |
| 列语义 | `status` 只表示交易状态（`normal`/`suspended`/`delisted`），ST/*ST 在独立列 `risk_warning`。两者正交：一只 ST 股停牌时两个字段同时成立 |
| 退市标的 | 不向行情板询问（板答不了），由 `instruments.delist_date` 判定，写 `status=delisted`、`is_trading=false`、`source=derived_delisted`，`risk_warning` 取自最终简称；**没有简称时为 null 而不是 false** —— 简称是退市后仅存的 ST 证据，它缺席就是证据缺席 |
| 北交所 | EastMoney 的 ST 板与停牌名单不覆盖 BJ；BJ 当日状态取自 BSE 官方行情板。源未覆盖字段保留 null，不能根据不在名单中推断 normal。 |
| BJ 历史 ST | 公告关键词、无日期的曾用名、当前 ST 板块不能证明历史每天的 normal 状态。项目没有能完整背书 BJ 历史 ST 状态的免费适配器；可选 Tushare 按其实际覆盖补证据。无凭证时缩小研究窗口到本湖证据支持的范围，不把零命中当正常、不宣称所有未来免费源都不可用。 |
| 未上市代码 | 无上市日期且从没有过 bar 的代码，需当天交易所行情板证据才写状态；当日新证券通过 run 上下文传递。请求失败不构成未上市/正常的证据。 |
| 上市首日 | 证券表步骤将新增证券写入 run 上下文，交易状态和日线步骤依赖它，不等待整组 compact 才获取新股。 |
| 已知限制 | ST 与 *ST 不做区分（喂本数据集的源都没有这个区分：Baostock 只有 `isST` 布尔，Tushare adapter 早已把 `ST`/`*ST` 归一）。更细的标识在交易所简称，经 `instruments.name` 获取 |

#### daily_bars

| 项 | 值 |
|------|-------|
| 波次 | `daily_bars`（Wave 1，依赖 corporate_actions） |
| 主源 | qmt_bridge（未复权；启用本地 BigQMT 桥时生效，未启用时走 TDX） |
| 备源 / 路由 | 当期可用交易所板快照与 TDX 批量行情；缺口走 eastmoney clist/kline。BJ 当期优先 BSE、历史优先 TDX，Sina 补剩余范围；显式小范围请求跳过无关全板扫描 |
| 频率 | 每日增量；init 时全量回填；深历史由同花顺按上市年份回补（股票与 ETF/LOF 均支持） |
| 主键 | (symbol, trade_date) |
| ETF 范围 | 默认 `all_a` 只取股票；显式开启 `[universe].ingest_eligible_etfs` 后，仅追加最近 14 个日历日内、可重放且判为 `eligible` 的最新完整交易所 ETF 目录中的基金。目录失败、缺失或过期时不新增基金日线请求；显式 `--symbols` 修复仍按指定范围。分类不倒填到首次观测日前。若正式目录上市日证明先前记录的 ETF 日线欠账发生在上市前，逐键保存证据收据再销账；其他缺口仍保留。 |
| 重拉 | 当日 `corporate_actions` 的除权日对应标的 |
| 已知限制 | TDX 限速；建议 workers ≤ 8；clist 只有当日快照，须用 run 的 `trade_date` 打戳；ETF/LOF 无上市日期时按未上市占位符处理，不盲抓深历史。当天证券表新出现的代码经 run 上下文并入当天的请求范围，上市首日即取数，不必等下一次 compact。无上市日期、也从未有过 bar 的**股票**每个窗口探测一次：取到 bar 即已上市照常入湖；源端干净地返回空则记为「未上市」（`not_yet_listed` 负面证据，只覆盖本窗口，次日重新探测），不计入未解决缺口；请求失败的不下结论，仍按未解决缺口处理。 |

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
| 备源 | eastmoney push2his —— **不自动回退**，需显式开启；该源可能拒绝请求，先看共享冷却/预算并限定范围验收，不因主源失败而自动增加 push2his 流量 |
| 覆盖 | 各合约回溯至自身上市日：CU0/AL0 2005、TA0 2006、ZN0 2007、AU0 2008、RB0 2009、J0 2011、AG0 2012、I0/JM0 2013、HC0/MA0 2014、NI0 2015、SC0 2018、LC0 2023 |
| 回填 | `cne backfill commodity_bars`（默认自 2020-01-01；可用 `--start`/`--end`） |
| 主键 | (symbol, trade_date) |
| 已知限制 | 主连非真实交割月；夜盘归结算日；水位按 SSE 日历近似；新浪该接口不提供成交额，`amount` 为空（主连拼接后 price×volume 不是当日成交额，故不派生）；伦敦金等未收录 |

#### macro_indicators

| 项 | 值 |
|------|-------|
| 组 | `macro_risk`（日更） |
| 主源 | eastmoney datacenter：`cnbond_yield_10y`（`RPTA_WEB_TREASURYYIELD`.`EMM00166466`）、`shibor_3m`（`RPT_IMP_INTRESTRATEN`，`INDICATOR_ID=203`）、`lpr_1y`（`RPTA_WEB_RATE`.`LPR1Y`）、`pmi_manufacturing`、`m2_yoy` |
| 补充源 | pboc：`social_financing`（社融增量） |
| 主键 | (indicator_id, obs_date) |
| 日更 | 三个利率逐日请求（`='{日期}'` 过滤），只走水位之后的交易日；两个日频利率另外回看最近 5 个交易日（一次范围查询）。运行当天的值还没发布时只记 `daily_series_pending` 提示、照常写入，次日回看补上；离开运行日仍缺的才是 `daily_series_gap` warning、挡住合并并重试。PMI、M2、社融每次都把整段历史读一遍 |
| 回填 | `cne backfill macro_indicators --start 2016-01-01`：两个日频利率按窗口各发一次分页范围查询（`(COL>='…')(COL<='…')`），不再逐日请求；同时照旧做一次当天的日更抓取。默认起点 2016-01-01，`--end` 不超过当天 |
| 历史深度 | 10Y 国债与 Shibor 的历史起点不同；缺失交易日记录为 `daily_series_gap`，以实际覆盖报告为准。 |
| 只留交易日 | 银行间市场在调休周末也开市，范围查询可能带回这些日子；回填按项目交易日历过滤，与日更的行密度一致 |
| 已知限制 | `lpr_1y` 不走范围回填：`LPR1Y` 列 2013-10-25 至 2019-08 是旧的逐日贷款基础利率，此后才是改革后的月度 LPR，范围查询会把两种基准记在同一个指标下。它仍只在日更当天落盘 |

#### futures_bars / option_bars / futures_contracts / option_contracts

| 项 | 值 |
|------|-------|
| 组 | `derivatives`（日更，18:30；`[futures] enabled = true` 才取数） |
| 主源 | **futures_exchange**：交易所官网的逐合约日行情文件。上期所 `data/tradedata/{future,option}/dailydata/kx{日期}.dat`（JSON，**同时含上期能源品种**）与 `busiparamdata/*/ContractBaseInfo{日期}.dat`；郑商所 2015-10 前 `cn/exchange/{年}/datadaily/{日期}.txt`（逗号分隔、无表头），之后 `DFSStaticFiles/{Future,Option}/…DataDaily.txt`（竖线分隔，表头改过名，编码 GBK→UTF-8）与参考 XML；广期所 `interfacesWebTiDayQuotes/loadList`（POST）与合约信息接口；中金所 `sj/hqsj/rtj/{月}/{日}/{日期}_1.csv` 与 `sj/jycs/{月}/{日}/index.xml` |
| 覆盖 | 期货：上期所 2002-01-07 起（更早未测）、郑商所 2010-01-04 起（更早的 HTML 存档被 JS 挑战拦住）、中金所 2010-04-16 起、广期所 2022-12-22 起。期权：郑商所 2017-04-19、上期所 2018-09-21、中金所 2019-12-23、广期所 2022-12-23 起 |
| 自洽校验 | 每个品种（郑商所期权为每个到期系列）的「小计」必须等于逐合约之和：量与持仓精确相等；郑商所、广期所成交额按行四舍五入到 0.01 万元，允许每行相差半个舍入单位（0.005 万元）。对不上的文件整份拒收 |
| 无数据信号 | 休市日：中金所 302 跳转；上期所 404 HTML；郑商所 404「当日无数据」；广期所 200 但只有一行全零「总计」。大商所所有端点返回 412 JS 挑战，归为 blocked，不当作无数据，本项目也不绕过。连接被重置等网络层错误重试一次（间隔 2 秒），超时不重试 |
| 部分失败 | 某一家没发布时照写其余交易所，缺的那家记 `futures_exchange_missing`，3 个交易日回看补回 |
| 回填 | `cne backfill futures_bars --start 2010-04-16`、`cne backfill option_bars --start 2019-12-23`，之后 `cne backfill futures_contracts` / `option_contracts` 由 bars 重建合约表 |
| 主键 | bars：(symbol, trade_date)；合约表：symbol |
| 口径 | 上期/能源、郑商所 2019-12-31 前量、持仓、成交额为双边统计（期权同样），入湖时折半，双边值必为偶数；广期所、中金所一直单边。成交额万元→元；IV 百分数→小数；上期所/能源 IV 按到期系列发布，存 `series_implied_vol`。早年上期所文件把未挂牌的远月列成结算价 0、持仓 0 的占位行，这类行不入湖。期权到期当天虚值合约的结算价 0 是真实价格，按 0 入湖（期货的 0 仍视为无价格） |
| 大商所 | 默认 `dce_route = "sina"`：逐合约历史约自 2018 年中，缺零成交日和成交额；批量报价只对应最近会话，不能证明零成交合约齐全，期权不收。历史持久缓存，已核对历史日常规复用，修订用 `--refresh`。`official` 是实验性映射：官方路由尚未通过可用性和数据完整性验收；遇到 412 或挑战页时标记为 blocked，项目不执行挑战脚本。探针可达仅代表请求可达，仍须独立核验必需字段、日期、总计及 Schema，不能据此视为生产验收 |
| 已知限制 | 上期所逐合约成交额要到 2021/2022 年后才有，更早为空；中金所文件不给行权量与 IV；郑商所 2026 年起有带 `MS` 标记的第二到期系列，规范符号保留标记（如 `CF2701MSC14200.CZC`）；期权交易所 IV/Delta 与本湖派生值口径不同；上期所官方档案缺 2004-06-25、2007-06-04 两个交易日的期货文件（当天 404、前后两天正常，旧 `data/dailydata/kx` 路径已整体下线），任何回填都补不回，缺口检查对这两天豁免（`domain/derivatives.py` 的 `UNPUBLISHED_FUTURES_SESSIONS`） |

<!-- derivative-capabilities:start -->

| 发布者 | 路由 | 期货起点 | 期权起点 | 生命周期参考 | 历史参考 | 状态 |
|---|---|---|---|---|---|---|
| SHF | official | 2002-01-07 | 2018-09-21 | 有 | 有 | supported |
| CZC | official | 2010-01-04 | 2017-04-19 | 有 | 有 | supported |
| GFE | official | 2022-12-22 | 2022-12-23 | 有 | 无 | supported |
| DCE | sina | 2018-06-01 | 不支持 | 无 | 无 | supported |
| CFE | official | 2010-04-16 | 2019-12-23 | 有 | 有 | supported |
| DCE | official | 2000-01-04 | 2017-03-31 | 无 | 无 | experimental |

此表由 `scripts/dev/sync_docs.py` 从读取器注册表生成。起点是适配器路由边界，不证明源端或本湖连续完整；`experimental` 尚未通过真实载荷验收。INE 2018 年期货使用能源中心独立日文件，2019 年起随 SHF 路由合并发布；其独立起点不能套用 SHF 日期。

<!-- derivative-capabilities:end -->

#### corporate_actions


**现金到账日**（`payment_date` / `payment_source`）只存来源报告的日期，未知留空，不以除权日代填：

| 证据 | `payment_source` | 入口（均需 `--symbols --start --end`） |
|------|------------------|------|
| 发行人实施公告 | `issuer_notice:…`（公告编号、页码、PDF SHA256） | `--issuer-notice-repair`：已审清单、巨潮（沪深）、北交所公告（东财原文镜像）；不请求 Baostock |
| 供应商 | `baostock:dividPayDate` | `--payment-date-repair`：先走发行人公告，余下再匹配 Baostock |

- 公告必须唯一证明证券代码、除权日、税前现金与到账日才写入；多份匹配、更正链、同日送转未核时保留缺口。原 PDF 与响应随修订归档，逐事件结论在 `meta/payment_date_repairs/<run_id>.json`。
- 巨潮与交易所公告在来源策略中仅登记为显式修复证据，不参与普通日更或自动回退。ETF 分红若无可核实的完整历史，不能由单份公告推断基金总回报序列。
- Baostock 数值字段只有六位小数时，用同一响应里的税前“10派…元”方案恢复精度并核对舍入一致；匹配后保留湖中原金额。同一公告里的 B 股日期、虚拟除权金额不会混入 A 股事件。
- 到账日早于除权日是来源笔误（中登在除权日划付 A 股现金，已见上一年模板未改、误写登记日两类）：解析时拒收，已存的由上述修复当作未知处理，找不到真实日期就清空。
- 同一事件多次抓取时，**证据等级高者胜过更新的抓取**（发行人公告 > 供应商日期 > 无），同级才比新旧。字段说明见 [corporate_actions](schema.md#corporate_actions)。普通历史回填不会抹掉已核实的金额、到账日或已审送转条款。
- 北交所官网[新旧代码对照表](https://www.bse.cn/service/code_mapping.html)（248 组）只用作检索发行人公告的别名，**不是**换码生效日或持仓承接证明。
- 需要完整现金日程的消费者，可对 `ex_date_rule_applies(symbol)`（`cnequity.domain.action_evidence`）为真的代码——沪深北 A 股股票——按自己的研究策略暂用除权日估算缺失的到账日，并明确标记为估算值；不要回写为来源报告的 `payment_date`。实际到账日可能不同。基金、CDR、B 股不适用，仍需报告日期。

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
| 主源 | sina（qfq/hfq 因子序列）；北交所交易所阶段为 `derived_actions`：按交易所除权规则由湖内公司行为与前收盘计算，新浪作对照 |
| 备源 | baostock（用 raw / 后复权收盘价之比推导因子） |
| 输入 | daily_bars 交易日 + 外部因子 API |
| 频率 | compact 之后每日 |
| 主键 | (symbol, trade_date, adjust_type) |
| 说明 | 外部累计因子对齐 daily_bars；`adj_close = close * factor` |
| **已知缺口** | 股票从 Sina 的 `f` 字段取因子，ETF/LOF 从 `s` 字段取因子（hfq 直接使用 `s`，qfq 使用 `1/s`）。新浪支持部分北交所标的，但新上市未交易、已退市或源端无因子的标的仍可能缺失；以最新 `adj_factor_coverage` / `adj_factor_source_unavailable` finding 和 `meta/quality/health-latest.json` 为准。对正式退市且新浪明确返回空序列的标的，派生会写入 `meta/state/adj_factors.json.source_unavailable_symbols`，停止无效重试但不会伪造因子。 |
| **查询侧后果** | `load(adjust="hfq")` 默认 `strict_adj=False`，缺因子的行按 `factor=1.0` 返回，即**未复权价出现在复权结果里**，只由 `adj_is_exact=False` 标记。实际不精确行数随查询窗口、标的范围和最新因子覆盖变化；请以结果中的 `adj_is_exact=False` 以及最新 `meta/quality/health-latest.json` 的 `adj_factor_coverage` finding 为准。|
| **怎么办** | 要严格失败而不是静默降级：`load(..., strict_adj=True)`。**它不是默认值**：新上市的票在拿到第一个因子前必然缺，所以严格模式会让 `universe="all_a"` 的 hfq 查询长期抛错。默认容忍 + `adj_is_exact` 标记 + 审计告警，是在「不静默污染」和「查询可用」之间的取舍 |
| **北交所** | 交易所上市起点（`list_date` 与 2020-07-27 中较晚者）之后的因子由公司行为计算：每个事件在其生效交易日按 `前收盘 ÷ 除权参考价` 出台阶，重整转增用公告的参考价；序列从该证券首个交易所交易日的已存水平起算，因此与新浪一致的证券数值不变。新浪仍会取数，台阶与计算值相差超过核验容差时报 `adj_factor_computed_vendor_divergence`，新浪有台阶而计算值没有，通常意味着湖内漏记了公司行为。新三板时期的因子行保持原值 |
| **自愈** | `derive_adj_factors` 每次增量运行都会找出「有 bar 但因子够不到」的标的并重排其完整历史，单次上限 500 只。所以 `cne backfill daily_bars` 补的历史会在随后的日更里自动补上因子，无需 `--full` |

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
| 一次一请求 | 该报表拒绝 `TRADE_DATE` 范围谓词（`InputMismatchException`），所以取全量后在本地切窗；实际返回行数随来源修订增长 |

#### margin_trading

| 项 | 值 |
|------|-------|
| 分组 | capital@17:00 |
| 主源 | exchange（上交所 `queryMargin` + 深交所 `1837_xxpl` tab2 融资融券交易明细） |
| 备选 | eastmoney（`[margin_trading] source = "eastmoney"`，仅由人工切换） |
| 主键 | (symbol, trade_date) |
| 已知限制 | **上交所不公布融券余额**，SH 行 `short_balance` 为 null（不做本地推算）；深交所比上交所晚一个交易日发布，两边都发布后才写入该日，因此比东财路径滞后约一个交易日 |

#### valuation_metrics

| 项 | 值 |
|------|-------|
| 日更源 | `eastmoney_datacenter`（datacenter 报表 `RPT_VALUEANALYSIS_DET`，按日期取全市场，含北交所，每天约 2 次请求）；datacenter 还没发布或失败时退到 eastmoney push2 clist 快照（来源记 `eastmoney`） |
| 历史源 | baostock（`cne backfill valuation_metrics`；按标的每日 PE/PB/PS 回填至 2016；**不含北交所**，不再请求 BJ）；东财断档窗口用 `--fill-em-outage`，读 datacenter |
| 主键 | (symbol, trade_date) |
| 已知限制 | baostock 历史含 pe_ttm/pb/ps_ttm；`float_mv`←close×volume/turn（收盘价口径），`total_mv`←Q4 totalShare×close（估算，按 `total_mv_basis` 标注）；日更 EM 快照覆盖最新交易日。**P/E 口径**：push2 f9 是动态市盈率，写入 `pe_dynamic`，不进入 `pe_ttm`；datacenter `PE_TTM` 与 baostock `peTTM` 一致（真 TTM）。市值字段按 `total_mv_basis` / `float_mv_basis` 区分供应商报告值与估算值 |

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
| 采集方式 | **按日期区间整市场扫**，不是按标的循环，也不是按报告期。`RPT_F10_EH_EQUITY.END_DATE` 是股本变动日；股东户数在旬末/月末也披露。只扫季末会静默漏掉其他披露日 |
| 日更范围 | 按 `NOTICE_DATE` 回看 30 天。窗口开在公告日而不是变动日：几周前生效的变动今天才公告，按变动日开窗永远看不到它 |
| PIT | `announce_date` 取自 `NOTICE_DATE`，进主键 |
| 客户端回填起点 | `share_structure` 为 **1990-01-01**，`shareholder_counts` 为 **1992-01-01**；这是注册表设置的历史起点，不保证来源每个日期都有记录 |

#### top_holders

| 项 | 值 |
|------|-------|
| 主源 | eastmoney（`RPT_F10_EH_HOLDERS` 全口径 + `RPT_F10_EH_FREEHOLDERS` 流通口径） |
| 分组 | **不在日更波次**。两张报表 × 约 110 页 × 两个报告期 ≈ 440 页，是上面两个的 40 倍；放进 fundamentals 会挤掉 macro_risk 整组。用 `cne backfill top_holders` 单独跑 |
| 主键 | (symbol, record_date, holder_scope, holder_rank, holder_name, announce_date)。**holder_name 必须进主键**：持股数相同的股东共用一个 rank，不带名字去重会把其中一家直接删掉 |
| 口径 | 一张表两个口径，靠 `holder_scope` 区分：`total`=前十大股东，`float`=前十大流通股东。`holding_pct` 两边分母不同（占总股本 vs 占流通股），**不可直接比较** |
| PIT | `RPT_F10_EH_HOLDERS` 没有 `NOTICE_DATE`，其披露日按 (symbol, report_period) 从 FREEHOLDERS 借；借不到的行**丢弃**而不是拿期末日期充数 |
| 采集方式 | 按 `END_DATE` 区间扫（全口径报表没有 `NOTICE_DATE`，两张报表若按不同列开窗，借披露日就没得匹配）。日更回看 240 天 |
| 源端历史底 | 当前支持 2003 年起。更早的全口径记录缺少可关联的披露日期，不用报告期冒充 PIT 时间；越界回填会被拒绝。 |
| 分页 | 单期超过 EastMoney 的 100 页上限，靠 `keyset_column="SECUCODE"` 换锚点翻过去（见 `datacenter.py`） |

### 按需数据集（On-demand）

不在日更波次中。缓存于 `meta/on_demand/`，可选写入 DuckDB 表。

| 数据集 | 来源 | 触发 |
|---------|--------|---------|
| stock_news | eastmoney | `cne query --dataset stock_news --symbol` |
| research_reports | eastmoney reportapi | 按标的 |
| announcement_body | cninfo | **未实现**（勿写入 `[on_demand].datasets`） |
| financial_reports | sina / gpcw | **未实现**（勿写入 `[on_demand].datasets`） |

### Meta 数据集

| 数据集 | 存储 |
|---------|---------|
| ingestion_runs | manifest.db |
| ingestion_batches | manifest.db |
| quality_findings | meta/quality/findings/ |
| source_diffs | meta/quality/source_diffs/ |
| data_catalog | 由 `cne stats show --json`（无 stats 表时的直扫回退）生成 |

### 源可用性矩阵

| 来源 | 协议 | MVP 用途 | 备源 | 降级策略 |
|--------|----------|-----------|--------|---------|
| qmt_bridge | 本地终端桥 | daily_bars, index_bars, minute_bars, minute_bars_5m；trading_calendar；corporate_actions 回填；financial_statement_items；shareholder_counts（仅 eastmoney 关闭时） | tdx_protocol 按缺口补齐；FSI 退 eastmoney | 桥不可用或无行时按批/缺口退 TDX；财务空响应不冒充 PIT；tick 历史仅最近 1 个交易日 |
| tdx_protocol | TCP | bars、instruments、calendar | eastmoney clist（tip 路由）/ kline（多日） | tip 缺口进 curated；snapshot 供 diff |
| sina | HTTP | hfq 因子（qfq 查询时推导） | — | 跳过该标的 + quality finding |
| bse | HTTP | BJ 当期日线、证券名单与交易状态 | — | 当期快照不能当作历史；已有行补 amount 需逐行一致性核对 |
| eastmoney | HTTP | 公司行为日更主源、资金面 | — | 跳过 + quality finding |
| cninfo | HTTP | announcement_index | — | events:disclosures 按自然日采集；监管事件从已提交公告派生 |
| baostock | TCP | 沪深历史 ST、估值、退市补数与证券身份补充 | — | 复用会话；上海自然日最多 5 万次请求，同时只一条连接；黑名单按本年次数 × 6 小时冻结。BJ 历史 ST 需独立来源 |
| pboc | HTTP | 社会融资规模增量（`macro_indicators`） | — | 主写入要求全量序列；单年失败会阻止本次写入，避免带断档推进水位 |
| nbs | HTTP | **仅审计**：PMI 发布稿，对照 `macro_indicators` | — | 按源开关执行；公共模板启用，失败或禁用状态进入核验报告 |
| exchange | HTTP | `margin_trading` **主源**；`trading_status` / `trading_calendar` / `dragon_tiger` / `block_trades` 备源；`[exchange_audit]` 价格对照 | — | 融资融券由会员单位报送汇总，中间无转售方；龙虎榜与大宗交易只在东财答不上时才问，且两所都不发布北交所（记在 `backup_gaps`）；审计类 finding 为建议性，不让 run 失败 |
| sina_bars | HTTP | Sina 日线兜底（与复权因子端点分开限速） | — | 跳过 + quality finding |
| ths | HTTP | 同花顺公开页：行业、估值 | — | 跳过 + quality finding |
| ths_pages | HTTP | `d.10jqka.com.cn` kline，`sector_bars` 唯一来源 | — | 该数据集**无第二个源**；失败即缺口 |
| ths_bonus | HTTP | 同花顺分红送配页 | — | 限速更保守（3.0s）；跳过 + quality finding |
| ths_official | HTTPS（keyed） | 可选有凭证来源：仲裁快照、财报补入、显式历史修复；版本化纠错遵循版本化发布规则 | — | 无 key 时全部 `skipped`，湖保持已有的源不变 |
| tushare | HTTPS（keyed） | 可选：BJ 历史 ST 证据（`stock_st`，起 2017-01-01） | bak_basic 名称证据（2016） | 无 token 时更早的 BJ bar 保持 unresolved，**不认定为正常** |

> **AkShare 已不再被任何适配器调用**（[issue #3](https://github.com/rootSunc/CNEquity/issues/3)）。
> 它此前的两个调用点分别指向本项目已经直连的端点：ST 集合走的是同一个东财
> push2 clist 板块与同一个 `fs` 过滤器，PMI / 货币供应量走的是同一批东财
> datacenter 报表。它提供的不是第二个口径，而是同一个口径外面的一层解析。
> 它也已从依赖里移除，`pip install cnequity` 不再装它。

调度与主备切换见 [运维 Runbook](../operations/runbook.md)。

衍生品的缓存、共享出口预算、拒绝冷却与研究边界统一见 [操作指南](../recipes/derivatives.md)。不存在可保证永不封 IP 的固定请求频率。
