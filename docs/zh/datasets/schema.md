# 字段与单位

本页用于查字段、类型和单位。数据集用途见[目录](catalog.md)，读取方式见[查询指南](query-guide.md)，兼容性与版本见[数据契约](contract.md)。列名与类型由 `scripts/dev/sync_docs.py` 同步，解释文字人工维护。

### 按层查找

按[研究分层](catalog.md#数据分层)排列，点名字跳到该数据集的列定义。

| 层次 | 数据集 |
|------|--------|
| **L0** 基础参考 | [instruments](#instruments), [etf_profiles](#etf_profiles), [trading_calendar](#trading_calendar), [trading_status](#trading_status) |
| **L1** 行情 | [daily_bars](#daily_bars), [index_bars](#index_bars), [minute_bars](#minute_bars--minute_bars_5m), [minute_bars_5m](#minute_bars--minute_bars_5m), [trade_ticks](#trade_ticks), [adj_factors](#adj_factors), [minute_bars_15m](#minute_bars_15m--minute_bars_30m--minute_bars_60m), [minute_bars_30m](#minute_bars_15m--minute_bars_30m--minute_bars_60m), [minute_bars_60m](#minute_bars_15m--minute_bars_30m--minute_bars_60m), [delisting_events](#delisting_events) |
| **L2** 公司事件 | [corporate_actions](#corporate_actions), [announcement_index](#announcement_index), [earnings_disclosure_schedule](#earnings_disclosure_schedule) |
| **L3** 基本面 | [financial_statement_items](#financial_statement_items), [share_structure](#share_structure), [shareholder_counts](#shareholder_counts), [top_holders](#top_holders), [valuation_metrics](#valuation_metrics), [analyst_consensus](#analyst_consensus) |
| **L4** 资金面 | [fund_flow](#fund_flow), [margin_trading](#margin_trading), [northbound_holdings](#northbound_holdings), [northbound_flows](#northbound_flows), [dragon_tiger](#dragon_tiger), [block_trades](#block_trades), [institutional_holdings](#institutional_holdings), [fund_flow_ths](#fund_flow_ths) |
| **L5** 结构行业 | [sector_members](#sector_members), [index_constituents](#index_constituents), [industry_members](#industry_members), [industry_index](#industry_index) |
| **L6** 宏观 | [macro_indicators](#macro_indicators), [market_breadth](#market_breadth) |
| **L7** 舆情 / 轮动 | [sentiment_scores](#sentiment_scores), [hot_rank](#hot_rank), [sector_bars](#sector_bars), [sector_fund_flow](#sector_fund_flow), [sector_fund_flow_ths](#sector_fund_flow_ths), [news_headlines](#news_headlines), [flash_news_wire](#flash_news_wire), [economic_calendar](#economic_calendar) |
| **L8** 风险合规 | [share_unlock_schedule](#share_unlock_schedule), [regulatory_events](#regulatory_events) |
| **L9** 衍生品 | [commodity_bars](#commodity_bars), [futures_contracts](#futures_contracts), [option_contracts](#option_contracts), [futures_bars](#futures_bars), [futures_minute_bars](#futures_minute_bars), [option_bars](#option_bars), [futures_continuous](#futures_continuous), [option_greeks](#option_greeks) |

### 全局约定

| 规则 | 取值 |
|------|-------|
| 时区 | 所有 `trade_date` 与业务时间戳使用 `Asia/Shanghai` |
| 股票 Symbol | `{code}.{SH\|SZ\|BJ}`，如 `600519.SH`；衍生品使用各自合约代码与交易所后缀 |
| 股票交易所列 | `SH` / `SZ` / `BJ`；衍生品见对应 schema |
| 溯源列 | 每行必有 `source`、`data_version`、`fetched_at`（UTC 时间戳） |
| 空值语义 | 停牌日：OHLCV 仍有值，`volume=0`、`amount=0` |
| 成交量单位 | A 股个股成交量一律 **股**；供应商报「手」的（TDX 日线、东财）由 adapter 在边界 ×100 |
| Schema 演进 | 兼容新增列可直接演进；破坏性变更须提升 `schema_version` 并附迁移说明 |
| `data_version` | 语义变更（不是加列）才提升；见下「成交量单位」 |

### PIT 双时态扩展列

仅适用于 PIT 数据集：`announcement_index`、`financial_statement_items`、`share_structure`、`shareholder_counts`、`top_holders`。**不适用于其他数据集**。

四列是可选存储列，不进 `DATASET_SCHEMAS` 的必需形状：旧 Parquet 没有它们照样可读，
读侧会补齐。compact 时会写入磁盘，没有这些列的旧分区在下次 compact 时补写一次。

| 列 | 类型 | 说明 |
|--------|------|-------|
| available_at | timestamp, nullable | 该事实在源端可用的时间；未知时必须为 null，不能用回填时的报告期代替 |
| source_published_at | timestamp, nullable | 源端实际发布时间；当前东财历史回填通常未知 |
| observed_at | timestamp, nullable | 湖实际观察到该行的时间；旧文件由 `fetched_at` 兼容补出 |
| revision_id | string, nullable | 稳定的事实/版本身份；96 bit（24 位十六进制）截断 SHA-256，由业务字段和溯源确定性导出，**不含**观察时间戳 |

`observed_at` 与 `fetched_at` 是同一件事的两个名字，因此二者都不参与 compact 的
业务摘要比对——否则每次对账重抓都会被判成业务变更，铸出一个新 revision，
而每个 revision 会整份复制该数据集。

### 分区键（curated）

| 数据集 | 分区 |
|---------|-----------|
| daily_bars | `trade_date`（按日） |
| index_bars | `trade_date`（按年） |
| minute_bars / minute_bars_5m | `trade_date`（按日） |
| minute_bars_15m / minute_bars_30m / minute_bars_60m | `trade_date`（按日） |
| trade_ticks | `trade_date`（按日） |
| trading_status | `trade_date`（按月） |
| corporate_actions | `ex_date`（按年） |
| adj_factors | `trade_date`（按日） |
| financial_statement_items | `report_period` |
| industry_members | `as_of_date` |
| northbound_flows | `trade_date` |

多源快照路径：`meta/source_snapshots/{dataset}/source={source}/data_version={ver}/`

### 主键

| 数据集 | 主键 |
|---------|-------------|
| instruments | `(symbol)` |
| etf_profiles | `(symbol, as_of_date)` |
| trading_calendar | `(trade_date)` |
| trading_status | `(symbol, trade_date)` |
| daily_bars | `(symbol, trade_date)` |
| index_bars | `(symbol, trade_date, frequency)` |
| minute_bars / minute_bars_5m | `(symbol, trade_date, bar_time, frequency)` |
| minute_bars_15m / minute_bars_30m / minute_bars_60m | `(symbol, trade_date, bar_time, frequency)` |
| trade_ticks | `(symbol, trade_date, tick_seq)` |
| corporate_actions | `(symbol, ex_date, action_type)` |
| adj_factors | `(symbol, trade_date, adjust_type)` |
| fund_flow | `(symbol, trade_date)` |
| northbound_holdings | `(symbol, trade_date, channel)` |
| northbound_flows | `(trade_date, channel)` |
| margin_trading | `(symbol, trade_date)` |
| sector_members | `(symbol, sector_code, as_of_date)` |
| valuation_metrics | `(symbol, trade_date)` |
| announcement_index | `(announcement_id)` |
| financial_statement_items | `(symbol, report_period, statement_type, item_code, announce_date)` |
| industry_members | `(symbol, classification_system, as_of_date)` |

### MVP-P0 列定义

#### instruments

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string | 主键 |
| name | string |  |
| exchange | string | SH/SZ/BJ |
| asset_type | string | stock/etf/index；旧 `etf` 值也包含部分 LOF，不可据此判定 ETF 研究资格 |
| list_date | date | 可空 |
| delist_date | date | 可空 |
| prev_symbol | string | 可空 |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### etf_profiles

交易所**当前目录快照**，不回填过去的分类。上交所目录给出 ETF 细分类和跟踪指数；
仅其单市场股票、沪深京跨市场股票及科创板股票类别标为 `eligible`。跨境、债券、
商品等明确不在范围内的类别为 `excluded`。深交所 ETF 列表提供基金和拟合指数，
另一份官方基金列表给出投资类别：债券、货币等非股票基金可排除；股票基金还需按
跟踪指数代码关联官方编制方案。已逐代码核验
[399006 创业板指](https://www.cnindex.com.cn/docs/gz_399006_e.pdf)、
[399330 深证100](https://www.cnindex.com.cn/docs/gz_399330_e.pdf)；
[399673 创业板50](https://www.cnindex.com.cn/docs/gz_399673_e.pdf)的样本空间是创业板指成分股，
须同时归档并核验 399006 的 A 股样本空间，才能标为 `eligible`。其他指数仍为 `unverified`。
两份深交所清单必须逐代码一致。
缺少目录记录也不能按代码前缀推断资格。数据来自[上交所 ETF 列表](https://www.sse.com.cn/assortment/fund/etf/list/)、
[深交所 ETF 列表](https://fund.szse.cn/marketdata/etf/)和[深交所基金列表](https://fund.szse.cn/marketdata/fundslist/)。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string | 证券代码，与 as_of_date 组成快照主键 |
| as_of_date | date | 本湖观察日，与 symbol 组成快照主键 |
| exchange | string | 交易所 |
| name | string | 基金简称 |
| list_date | date | 来源给出的上市日期，可空 |
| tracking_index_code | string | 来源指数代码，可空 |
| tracking_index_name | string | 来源指数名称，可空 |
| fund_category | string | 官方基金类别 |
| investment_category | string | 官方投资类别，无法单独证明股票指数的市场范围 |
| eligibility_status | string | `eligible` / `excluded` / `unverified` |
| classification_basis | string | 分类依据；深市已核验行包含指数代码、官方方案 PDF 的 SHA-256 与 URL；不能仅用名称或代码前缀 |
| source_url | string | 官方目录页面 |
| source | string | 数据来源 |
| data_version | string | 数据版本 |
| fetched_at | timestamp | 本湖采集时间 |

#### trading_calendar

| 列 | 类型 | 说明 |
|--------|------|-------|
| trade_date | date | 主键 |
| is_trading | bool |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### trading_status

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| is_trading | bool |  |
| status | string | **交易状态**：`normal` / `suspended` / `delisted` |
| risk_warning | bool | **风险警示（ST/*ST）**，与 status 正交；可为 null（无证据） |
| source | string | 退市行由 `instruments` 判定，标 `derived_delisted` |
| data_version | string |  |
| fetched_at | timestamp |  |

`status` 表示交易状态，`risk_warning` 表示风险警示，两者独立。停牌不能清除 ST 标志；已知退市证券不应被作为正常交易证券处理。

`derived_traded` 表示已提交日线证明当天有实际成交（成交量大于零），不是一手状态快照。
它只补已知上市期间、没有任何状态行的股票或 ETF 键，写 `normal`、`is_trading=true`，
股票写 `risk_warning=null`，ETF/基金写 `risk_warning=false`（ST 制度不适用于基金，不是未知）。
股票行的 `normal` 不表示“确认非 ST”。可信的当日一手状态优先于此派生行；
它不参与东财中断时的一手记录沿用，也不生成历史 ST 覆盖凭证。零成交量、未知成交量、
未知上市日期和上市期间之外的日线不会生成此类行；已有矛盾状态需另行修复。

历史快照只覆盖本湖开始采集之后的日期，不能根据第一条状态记录宣称此前完整。
`cne check --full` 的 `trading_state_historical_coverage` 按资产类别及月份统计成交键，
包括每只证券首次状态记录之前的历史；缺状态、派生成交、ST 未知和排除原因分别报告。
该报告只检查有成交的键，不证明停牌日期或全部历史 ST 已完整覆盖。

用 `cne repair status-traded --start YYYY-MM-DD --end YYYY-MM-DD` 离线预览补缺，
核对后加 `--apply` 发布并保留旧版本。建议按年或月分段；执行时重新核对当前已提交版本。
严格的历史股票池读取仍需独立 ST 证据，成交补行不会解除该要求。

**读旧湖不会出错。** 旧行把 ST 编码成 `status="st"`，`validate_dataframe` 在读入时
自动升级（见 `cnequity/domain/trading_status.py`），物理文件不需要改写。

迁移**不会**给历史补 `delisted` 行：某一天的状态是当时观测到的事实，用今天的退市日期
倒填会凭空造出当时并不存在的 point-in-time 事实。要修某段历史，重跑那段日更即可。

#### daily_bars

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| open | float64 | 未复权 |
| high | float64 |  |
| low | float64 |  |
| close | float64 |  |
| pre_close | float64 | 可空。交易所公布的前收盘；除权日即除权参考价（见下「前收盘」） |
| volume | int64 | **股**（见下「成交量单位」）；`data_version=v2` 才保证 |
| amount | float64 | 人民币 |
| source | string |  |
| data_version | string | `v2`=volume 为股；`v1`=按源而异，已弃用 |
| fetched_at | timestamp |  |

##### 成交量单位（`daily_bars.volume`）

各家供应商的原生单位并不一致，而 payload 里没有任何字段声明它，所以混在一列里会**正好差 100 倍**——足以毁掉一切换手率/流动性因子，却小到行数、主键、OHLC 检查都发现不了。

**契约：一律存「股」。** 这也是唯一能让 `amount ≈ close × volume` 成立的选择，而这个恒等式正是质量检查赖以从数据本身发现单位错误的依据。每个 adapter 在自己的边界完成换算。

源数据在采集边界统一换算。TDX 日线与分钟线的原生单位不同，不能跨频率复用换算规则。`minute_bars` / `minute_bars_5m` 的成交量同样为股。

质量检查按来源核对量价关系，缺少成交额的数据无法做此项检查。`index_bars` / `sector_bars` 的指数点位不适用个股量价关系。

**迁移（v1 → v2）。** 0.12 及更早版本写入的湖需要把 v1 行改写为股；这个一次性脚本已移除，这样的湖请先用 0.19 或更早版本迁移后再升级。

##### 前收盘（`pre_close`）

交易所行情公布的当日前收盘，来自上交所行情快照、北交所行情板和通达信实时行情（深市经通达信），从 0.13.0 起随日更写入；K 线历史（通达信历史、新浪、Baostock、同花顺）不提供，这些行和既有历史为空。除权除息日的前收盘就是交易所算出的除权参考价，所以 `前一交易日收盘 ÷ pre_close` 就是当日应有的复权因子台阶，平日为 1。派生复权因子时按此核对，偏离超过容差（另加前收盘半分钱的舍入）报 `adj_factor_pre_close_divergence`。之后用 K 线回补同一交易日时，已有的前收盘会保留。`index_bars` 共用这一列，目前没有写入来源。

##### 北交所的成交量和成交额含大宗交易

北交所日线的 `volume` / `amount` 把当日大宗交易也算进去——TDX、北交所官网、同花顺都如此；沪深日线不含。大宗交易常以折价成交，所以北交所某天的 `amount / volume` 可以落在当日最低价之下（溢价时高于最高价）。按沪深口径算北交所的均价、换手率时，先扣掉当日大宗交易（`block_trades` 单位是万股 / 万元）。2021-11-15 开市前的精选层时期，`block_trades` 没有北交所记录（东财不提供），那段日线无法这样扣。

#### index_bars

与 daily_bars 同构，另加 `frequency`（默认 `1d`）。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| open | float64 |  |
| high | float64 |  |
| low | float64 |  |
| close | float64 |  |
| pre_close | float64 | 与日线共用的可空前收盘列；当前指数来源不写入 |
| volume | int64 | **不是股**：TDX `index()` 原值，单位未确证（见下） |
| amount | float64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |
| frequency | string | 默认 `1d` |

**例外：`volume` 不是股。** `index_bars` / `sector_bars` 保留上游指数接口原值，单位未确证，不可直接用于个股换手率或与成分股成交量相加。这两个数据集仍是 `data_version=v1`。

#### minute_bars / minute_bars_5m

日内 K 线，两个数据集共用同一份 schema。**可选**，默认关闭（`[minute_bars].enabled = false`），不在默认 daily wave 上。

| 数据集 | frequency | 一个交易日 bar 数 | 源端视野 |
|--------|-----------|-----------------|---------|
| `minute_bars` | `1m` | 240 | 95 个交易日 |
| `minute_bars_5m` | `5m` | 48 | 491 个交易日（约 2 年） |

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date | 分区列；A 股无夜盘，恒等于 `bar_time` 的日期 |
| bar_time | timestamp（naive） | **bar 的收盘分钟**，`Asia/Shanghai` 墙钟；见下「bar 语义」 |
| frequency | string | `1m` / `5m` / `15m` / `30m` / `60m`；一个数据集只放一个频率 |
| open | float64 | **未复权**；用 `load(..., adjust="hfq")` 在查询侧按 `(symbol, trade_date)` 关联当日因子 |
| high | float64 | **未复权**；用 `load(..., adjust="hfq")` 在查询侧按 `(symbol, trade_date)` 关联当日因子 |
| low | float64 | **未复权**；用 `load(..., adjust="hfq")` 在查询侧按 `(symbol, trade_date)` 关联当日因子 |
| close | float64 | **未复权**；用 `load(..., adjust="hfq")` 在查询侧按 `(symbol, trade_date)` 关联当日因子 |
| volume | int64 | **股**。TDX 日 K 是手、日内 K 原生就是股——日内路径**不得**复用日频的 ×100 换算 |
| amount | float64 | 人民币元 |
| source | string | 溯源列 |
| data_version | string | 溯源列 |
| fetched_at | timestamp | 溯源列 |

**bar 语义。** 标签是 bar 的**收盘时刻**（右标签）：1m 的 `09:31` 覆盖 09:30–09:31，5m 的 `09:35` 覆盖 09:30–09:35；`15:00` 含收盘集合竞价。交易时段为 `09:31–11:30` + `13:01–15:00`，午休无 bar。

落在时段外的 bar 会被过滤，audit 的 `minute_bars_off_session` 会报告违规数据。

**分钟边界可能变化。** 对相同窗口重新采集时，部分成交可能在相邻分钟之间重新归属。使用分钟绝对量做因子时应考虑这一限制，按日汇总后与日线核对；重复采集不保证逐 bar 字节一致。

**无成交分钟。** TDX 的 volume 打包浮点解码把原始 0 映射成 `2**-127`（≈5.88e-39）而非 0.0（见 `_wire/helper.get_volume`）。日内路径显式归零，`volume=0`、`amount=0`，与全湖的停牌约定一致。冷门股一天有几十个这样的分钟，停牌股则是一整个交易日。

**历史视野。** 当前客户端按 1m 95 个交易日、5m 491 个交易日限制回填窗口。上游按根数保留数据，实际最早日期受标的交易活跃度影响，不能把窗口长度当作连续覆盖保证。完整机制与例外见 [catalog.md 历史视野](catalog.md)；`cne backfill` 会直接拒绝越界窗口，`list_datasets()` 的 `history_horizon_days` 是程序化契约。

**为什么一个数据集只放一个频率。** 1m 视野 95 天、5m 视野 491 天，而一个数据集只有一个水位、一个 `coverage_start`、一个 `history_horizon_days`。混在一起，这三样对两个频率都是错的。`frequency` 仍在 schema 与主键里，所以两者共用同一份列定义、同一套质量检查。

#### minute_bars_15m / minute_bars_30m / minute_bars_60m

由湖里的 1m 和 5m 重采样得到的派生数据集（`derive/minute_resample.py`）。**默认不计算**，日更和 `cne init` 都不会生成；需要入湖时手动运行 `cne derive minute_bars_15m`（30m、60m 同理）。不入湖时也可以在查询时用 `resample_minute_history` 按同一规则现算。

某只股票某天有 1m 就用 1m，否则用 5m；那一天有 1m 但某个区间缺组成 K 线时整天跳过，不改用 5m。停牌股的 1m 常少一根，整天无成交的跳过计入 `skipped_halted_1m`，盘中停复牌（K 线不间断、只缺开头或结尾）计入 `skipped_partial_session_1m`；有成交而 K 线中间断档的计入 `skipped_incomplete_1m`，运行记为降级。不给窗口时只重算还没算过、或 1m / 5m 内容与计算规则有变化的交易日（按内容判断，回滚和恢复也会触发），所以后来补回的 1m 会让对应交易日改用 1m 重算；`--start` / `--end` 限定窗口，`--full` 重算全部。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date | 分区列 |
| bar_time | timestamp（naive） | 区间的收盘分钟；上午从 09:30、下午从 13:00 对齐，不跨午休 |
| frequency | string | `15m` / `30m` / `60m`，与数据集一致 |
| open | float64 | **未复权**；OHLC 只计入有成交的输入 K 线，区间内全无成交时沿用末报价 |
| high | float64 | **未复权** |
| low | float64 | **未复权** |
| close | float64 | **未复权** |
| volume | int64 | 股 |
| amount | float64 | 人民币元 |
| resampled_from | string | `1m` 或 `5m`：这根 K 线由哪个输入算出。5m 的开高低价已含未成交分钟沿用的报价，少数区间会与 1m 的结果不同 |
| source | string | 溯源列，恒为 `derived` |
| data_version | string | 溯源列 |
| fetched_at | timestamp | 溯源列；派生时刻 |

#### trade_ticks

分笔成交记录。**可选**，默认关闭（`[trade_ticks].enabled = false`），**独立的**配置节与 step 组（`ticks`），不搭 `[minute_bars]` 的车。

**先说清楚它不是什么：不是逐笔成交。** A 股 Level-1 是**每 3 秒一帧的快照**，一条记录是那一帧里所有真实成交的聚合。
没有逐笔委托，没有十档。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date | 分区列；A 股无夜盘，恒等于 `trade_time` 的日期 |
| tick_seq | int32 | **当日时间升序的 0-based 稠密序号**，行的身份所在（见下） |
| trade_time | timestamp（naive） | **分钟精度**，秒位恒为 `00`——不是被截断，是协议从来没带过秒 |
| price | float64 | **未复权**；`load(..., adjust="hfq")` 会给出 `adj_price` |
| volume | int64 | **股**。源端是手，适配器 ×100，并由与日频的对账确证而非假定 |
| direction | string | `buy` / `sell` / `neutral` / `after_hours`（见下） |
| source | string | 溯源列 |
| data_version | string | 溯源列 |
| fetched_at | timestamp | 溯源列 |

**为什么主键是 `tick_seq` 而不是 `trade_time`。** 时间戳没有秒，一分钟里最多 20 条记录时间戳完全相同。
用 `(symbol, trade_date, trade_time)` 会丢掉绝大多数行，而**同一分钟内的先后正是分笔的价值所在**。

`tick_seq` 是完整 symbol-day 内的顺序编号。分页失败时不能把半天的数据作为完整结果发布；读取时不要把分钟精度的时间戳当作唯一键。

**`direction` 是推断值，不是交易所字段。** 通达信按 tick rule 判断谁主动成交。

`after_hours` 是 15:05–15:30 的**盘后固定价格成交**：价格恒等于当日最后成交价，且**不在交易所当日成交量口径内**。
与 `daily_bars` 对账必须先剔除盘后成交。

**交易时段是四段**，与分钟线的两段不同：`09:25`（开盘集合竞价，每个 symbol-day 恰好 1 条）、`09:30–11:30`、`13:00–15:00`、`15:05–15:30`。
注意 09:25 与 13:00 都是**真实成交**——分钟线里它们不是合法 bar 标签，因为 bar 按收盘分钟标注。
落在外面的会被适配器拒绝，audit 的 `trade_ticks_off_session` 报 error。

**没有 `amount` 列。** 源端不提供。`price × volume` 可以自己算，但要知道它是近似——
一帧里多笔不同价成交被合并成一个代表价。不能把估算值当作上游发布的真实成交额。
落一个看起来像事实的近似值进湖，比让使用者自己算更糟。

**价格标度按品种。** 个股 ÷100、基金 ÷1000（`SECURITY_COEFFICIENT`）。
适配器遇到无法识别的前缀**直接报错而不是回落到个股系数**——错误的标度是隐形的，数字看起来全都像价格。

**历史底是固定日期，不是滚动窗口。** 当前客户端使用 **2024-01-02** 作为最早请求日期；实际覆盖应检查已采集数据。
这是 `history_floor_date`，与分钟线的 `history_horizon_days` 是两种机制，详见 [catalog.md 历史视野](catalog.md)。

**北交所无数据。** TDX 没有 `.BJ` 的分笔路由，且返回空而不是报错——适配器显式抛异常，否则会和「全天停牌」无法区分。


**容量。** 行数随标的范围、交易活跃度和频率增长；压缩率及最终磁盘占用还受原始归档、staging 和版本保留影响。默认 `scope = "index:000300.SH"` 只取一个指数的成分股，扩大到全市场前应从小样的实际分区大小估算。

#### futures_bars

期货、期权**逐合约**日线，取自交易所自己发布的每日行情文件（[产品边界](../architecture/overview.md)）。每个文件列出当天所有在市合约，**没成交也有结算价**。期货与期权分成 `futures_bars` / `option_bars` 两个数据集，共有列口径相同。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string | 规范符号：期货 `CU2511.SHF` / `TA2601.CZC` / `IF2512.CFE`；期权 `IO2512C4000.CFE`（标的 + 月份 + C/P + 行权价） |
| exchange | string | `SHF` / `INE` / `DCE` / `CZC` / `GFE` / `CFE` |
| exchange_code | string | 交易所原始代码（`cu2511`、`TA601`、`IO2512-C-4000`） |
| product | string | 品种代码，大写 |
| trade_date | date | 交易日；夜盘归下一交易日（交易所口径） |
| open | float64 | **未成交为空**：没有成交就没有价格 |
| high | float64 | **未成交为空**：没有成交就没有价格 |
| low | float64 | **未成交为空**：没有成交就没有价格 |
| close | float64 | **未成交为空**：没有成交就没有价格 |
| settle | float64 | 结算价。期货为正数；期权到期当天虚值合约按交易所文件存 **0**（真实价格，不是缺失），极少数到期合约文件里没有结算价时为空 |
| pre_settle | float64 | 前结算价，口径同 `settle` |
| volume | int64 | 手，**单边**。上期所/能源/大商所/郑商所 2020-01-01 前按双边统计，已折半（双边值必为偶数，出现奇数则拒收） |
| amount | float64 | 元（交易所发布万元，×10⁴；同样单边） |
| open_interest | int64 | 持仓，单边 |
| oi_change | int64 | 持仓变化，单边 |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### option_bars

期权逐合约日线，与 `futures_bars` 同源同口径，另有期权字段。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string | 规范符号：期货 `CU2511.SHF` / `TA2601.CZC` / `IF2512.CFE`；期权 `IO2512C4000.CFE`（标的 + 月份 + C/P + 行权价） |
| exchange | string | `SHF` / `INE` / `DCE` / `CZC` / `GFE` / `CFE` |
| exchange_code | string | 交易所原始代码（`cu2511`、`TA601`、`IO2512-C-4000`） |
| product | string | 品种代码，大写 |
| underlying_symbol | string | 标的期货合约；中金所期权为指数（IO→`000300.SH`、MO→`000852.SH`、HO→`000016.SH`） |
| option_type | string | `C` / `P` |
| strike | float64 | 行权价（报价单位） |
| trade_date | date | 交易日；夜盘归下一交易日（交易所口径） |
| open | float64 | **未成交为空**：没有成交就没有价格 |
| high | float64 | **未成交为空**：没有成交就没有价格 |
| low | float64 | **未成交为空**：没有成交就没有价格 |
| close | float64 | **未成交为空**：没有成交就没有价格 |
| settle | float64 | 结算价。期货为正数；期权到期当天虚值合约按交易所文件存 **0**（真实价格，不是缺失），极少数到期合约文件里没有结算价时为空 |
| pre_settle | float64 | 前结算价，口径同 `settle` |
| volume | int64 | 手，**单边**。上期所/能源/大商所/郑商所 2020-01-01 前按双边统计，已折半（双边值必为偶数，出现奇数则拒收） |
| amount | float64 | 元（交易所发布万元，×10⁴；同样单边） |
| open_interest | int64 | 持仓，单边 |
| oi_change | int64 | 持仓变化，单边 |
| exercise_volume | int64 | 行权量（中金所文件不提供，为空） |
| delta | float64 | **交易所发布值**；本湖重算的见 `option_greeks` |
| implied_vol | float64 | **交易所发布的** IV，小数 |
| series_implied_vol | float64 | 上期所/能源按到期系列发布的 IV（不分行权价），小数 |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

两个数据集主键都是 `(symbol, trade_date)`。分区：`trade_date`（futures_bars 按月、option_bars 按日）。
校验：期货结算价 > 0，期权结算价 ≥ 0；成交价 > 0；成交行可以只有收盘价（交割月、期转现），否则 OHLC 满足包络；期权 |delta| ≤ 1.0001（上期所深度实值认沽印成 −1.000001）。违反的行隔离，不拖垮整个交易日。
`required=false`，由 `[futures]` 开启。

#### futures_contracts

期货合约表，单文件 merge，主键 `symbol`，由 `futures_bars` 重建。期权合约见 `option_contracts`，两表共有列口径相同。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string | 规范符号：期货 `CU2511.SHF` / `TA2601.CZC` / `IF2512.CFE`；期权 `IO2512C4000.CFE`（标的 + 月份 + C/P + 行权价） |
| exchange | string | `SHF` / `INE` / `DCE` / `CZC` / `GFE` / `CFE` |
| exchange_code | string | 交易所原始代码（`cu2511`、`TA601`、`IO2512-C-4000`） |
| product | string | 品种代码，大写 |
| product_name | string | 品种中文名，来自 `domain/futures_products.py`（带生效日期）；未知品种为空并产生审计 finding |
| delivery_month | date | 交割月（当月 1 日） |
| list_date | date | 上市日，见 `dates_basis` |
| last_trade_date | date | 最后交易日，见 `dates_basis` |
| multiplier | float64 | 合约乘数，来自 `domain/futures_products.py`（带生效日期）；未知品种为空并产生审计 finding |
| tick_size | float64 | 最小变动价位，来自 `domain/futures_products.py`（带生效日期）；未知品种为空并产生审计 finding |
| quote_unit | string | 报价单位，来自 `domain/futures_products.py`（带生效日期）；未知品种为空并产生审计 finding |
| dates_basis | string | `exchange`：交易所参考文件给出；`observed` / `observed_truncated` / `vendor_observed` / `open`：仅描述观测证据；无权威参考的上市/最后交易日为空，观测边界另存 first_seen_date/last_seen_date |
| first_seen_date | date | 湖里首次出现的交易日 |
| last_seen_date | date | 湖里最后出现的交易日 |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### option_contracts

期权合约表，单文件 merge，主键 `symbol`，由 `option_bars` 重建。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string | 规范符号：期货 `CU2511.SHF` / `TA2601.CZC` / `IF2512.CFE`；期权 `IO2512C4000.CFE`（标的 + 月份 + C/P + 行权价） |
| exchange | string | `SHF` / `INE` / `DCE` / `CZC` / `GFE` / `CFE` |
| exchange_code | string | 交易所原始代码（`cu2511`、`TA601`、`IO2512-C-4000`） |
| product | string | 品种代码，大写 |
| product_name | string | 品种中文名，来自 `domain/futures_products.py`（带生效日期）；未知品种为空并产生审计 finding |
| underlying_symbol | string | 标的期货合约；中金所期权为指数（IO→`000300.SH`、MO→`000852.SH`、HO→`000016.SH`） |
| underlying_kind | string | `future` / `index`（中金所指数期权） |
| option_type | string | `C` / `P` |
| strike | float64 | 行权价（报价单位） |
| exercise_style | string | `american` / `european`，来自 `domain/futures_products.py`（带生效日期）；未知品种为空并产生审计 finding |
| expiry_month | date | 兼容名称：标的合约月份（当月 1 日），不是实际到期月；真实到期用 expiry_date |
| list_date | date | 上市日，见 `dates_basis` |
| expiry_date | date | 到期日，见 `dates_basis` |
| multiplier | float64 | 合约乘数，来自 `domain/futures_products.py`（带生效日期）；未知品种为空并产生审计 finding |
| tick_size | float64 | 最小变动价位，来自 `domain/futures_products.py`（带生效日期）；未知品种为空并产生审计 finding |
| dates_basis | string | `exchange`：交易所参考文件给出；`observed` / `observed_truncated` / `vendor_observed` / `open`：仅描述观测证据；无权威参考的上市/到期日为空，不能由最后一根行情推断到期 |
| first_seen_date | date | 湖里首次出现的交易日 |
| last_seen_date | date | 湖里最后出现的交易日 |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### futures_continuous

主力 / 次主力连续合约（派生，`derive/futures_continuous.py`，规则 `oi_t-1_v2`）。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string | 品种，如 `CU.SHF`、`IF.CFE` |
| series | string | `main` / `second` |
| trade_date | date | 交易日 |
| contract_symbol | string | 当天所用的具体合约 |
| open | float64 | 该合约当天的原始行情 |
| high | float64 | 该合约当天的原始行情 |
| low | float64 | 该合约当天的原始行情 |
| close | float64 | 该合约当天的原始行情 |
| settle | float64 | 该合约当天的原始行情 |
| volume | int64 | 该合约当天的原始行情 |
| open_interest | int64 | 该合约当天的原始行情 |
| rolled | bool | 当天是否换月 |
| adj_ratio | float64 | 累计换月比例因子（缺换月价格或前一交易日后为 null）：原价 × `adj_ratio` 得到连续序列（最早的价格保持原样）；除以最新因子即为后向调整视图 |
| adj_diff | float64 | 累计换月差值因子（失效后为 null）：原价 + `adj_diff` 得到连续序列；减去最新因子即为后向调整视图 |
| roll_yield | float64 | 仅 main：ln(主力/次主力) ÷ 两者交割月间隔（年），正值为贴水结构（backwardation） |
| rule | string | `oi_t-1_v2` |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

换月只用 T-1 收盘的持仓决定，T 日本身的数据不参与；只向更远月份换；已知最后交易日的合约在其前 5 个自然日让位，未知的在进入交割月时让位。没有合法更远月份就缺该序列，不回退；缺失前一交易日不会用更早观测日代替。主键 `(symbol, series, trade_date)`，按月分区，每次由 futures_bars 全量重建。

#### option_greeks

本湖自己的期权隐含波动率与希腊字母（派生，`derive/option_greeks.py`）。交易所发布的 Delta/IV 原样留在 `option_bars`，这里是一套口径统一的重算。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string | 期权规范符号，同 `option_bars` |
| trade_date | date | 交易日 |
| underlying_symbol | string | 同 `option_bars` |
| underlying_price | float64 | 商品期权取同日标的期货结算价；中金所指数期权为看涨看跌平价倒推的远期 |
| forward_source | string | `future_settle` / `parity`（最接近平值的 3 个行权价取中位数） |
| time_to_expiry | float64 | 自然日 ÷ 365，到期当天计 1 天（该日不反解 IV，见 `status`） |
| rate | float64 | `shibor_3m`（小数）；湖里没有时用 2% |
| rate_source | string | `shibor_3m` / `fallback_constant` |
| model | string | `black76`（欧式）/ `baw`（美式，Barone-Adesi–Whaley，零持有成本）/ `unknown`（行权方式缺失，不计算） |
| iv | float64 | 用结算价反解，小数 |
| delta | float64 | 每单位标的价格变动 |
| gamma | float64 | 每单位标的价格变动 |
| vega | float64 | 对应波动率变动 1.00 |
| theta | float64 | 每年 |
| rho | float64 | 对应利率变动 1.00 |
| status | string | `ok`；否则说明原因：`expiry_day`（到期当天，结算价就是行权价值，没有时间价值可反解）、`below_intrinsic`（结算价低于模型下界，常见于深度实值：广期所向下取整到网格、差 1 个最小变动价位，中金所深度实值认沽与平价远期不一致）、`above_bound`、`no_underlying`、`no_expiry`、`no_exercise_style`、`no_price` |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

主键 `(symbol, trade_date)`，按日分区。保存行情、合约、利率、模型代码与输出内容指纹；任一依赖改变、收据缺失或结果过旧均触发重算。`--start/--end` 指定窗口，`--full` 强制全量；局部重算不把其他日期错误标为已更新，也不回拨水位。详见 [衍生品研究指南](../recipes/derivatives.md)。

#### futures_minute_bars

期货 1 分钟线，只覆盖 `[futures] minute_products` / `minute_contracts` 指定的合约。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string | 规范合约符号，同 `futures_bars` |
| exchange | string | 同 `futures_bars` |
| trade_date | date | 交易日；**夜盘 bar 归下一交易日**（周五 21:05、周六 00:30 都属于下周一），所以这里 `trade_date` 不等于 `bar_time.date()` |
| bar_time | timestamp（naive） | K 线**收盘**分钟（09:00 开盘的第一根记为 09:01），北京时间 |
| frequency | string | `1m` |
| open | float64 | 未成交的分钟为空 |
| high | float64 | 未成交的分钟为空 |
| low | float64 | 未成交的分钟为空 |
| close | float64 |  |
| volume | int64 |  |
| open_interest | int64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

主键 `(symbol, bar_time)`，按日分区。新浪每合约只保留最近 1023 根，只能从开启那天往后积累，必须每个交易日都跑。

#### commodity_bars

国内商品期货**主力连续**日 K（东财主连）+ 窄口径外盘（新浪 COMEX 金 ``GC0.CMX``）。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string | 国内 `{根}0.{交易所}`（如 `AU0.SHF`）；外盘 `GC0.CMX`（COMEX 金连续） |
| name | string | 合约中文名 |
| exchange | string | `SHF` / `DCE` / `CZC` / `INE` / `GFE` / `CMX` |
| trade_date | date | 源交易所会话日（外盘为 COMEX 日历；与 A 股对齐在研究侧 as-of） |
| open | float64 |  |
| high | float64 |  |
| low | float64 |  |
| close | float64 |  |
| volume | int64 | 手（东财口径；新浪外盘常为 0） |
| amount | float64 | 成交额（外盘可空） |
| open_interest | float64 | 可空 |
| source | string | 溯源（`eastmoney` / `sina`） |
| data_version | string | 溯源（`eastmoney` / `sina`） |
| fetched_at | timestamp | 溯源（`eastmoney` / `sina`） |

主键：`(symbol, trade_date)`。分区：`trade_date`。

日更：`macro_risk` 组。历史：`cne backfill commodity_bars [--start 2020-01-01 --end …]`。

`required=false`。外盘 v1 **仅黄金**；不进 A 股回测引擎。

#### corporate_actions

| 列 | 类型 | 说明 |
|--------|------|-------|
| payment_date | date | 可空；来源报告的现金到账日，不早于 `ex_date`；未知不以除权日代替 |
| payment_source | string | 可空；到账日证据：`issuer_notice:…`（发行人公告）或 `baostock:dividPayDate`；合并时证据等级优先于抓取新旧 |
| symbol | string |  |
| ex_date | date |  |
| action_type | string | cash_dividend/bonus/transfer/allotment/unit_split/reorg_transfer |
| cash_dividend | float64 | **每股**（元，税前） |
| bonus_ratio | float64 | **每股**（送股：每持有 1 股送出股数） |
| transfer_ratio | float64 | **每股**（转股：每持有 1 股转增股数） |
| split_factor | float64 | 份额拆分/合并：新份额 ÷ 原份额；中性值 1 |
| allotment_ratio | float64 | **每股**（配股：每持有 1 股可配股数），可空 |
| allotment_price | float64 | 配股价（元/股），**不是**比率，可空 |
| reference_price | float64 | 仅 `reorg_transfer`：发行人公布的除权（息）参考价（元/股）；其他类型必须为空 |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

> **单位契约（每股）。** 所有比率/金额均相对于「持有 1 股」，
> 不是通达信（`xdxr`）/东财常见的「每 10 股」口径。Adapter 在入 staging 前
> 把源侧「每 10 股」数值除以 10（例如「10 派 8.5 元」→ 0.85，「10 送 8 股」→ 0.8，
> 「10 转 4 股」→ 0.4，「10 配 3 股」→ 0.3）。下游按真实持股统一核算，无需再除 10：
> `shares_after = shares × (1 + bonus_ratio + transfer_ratio) × split_factor`，
> `cash = shares × cash_dividend`。`allotment_price` 是每股价格而非比率，不做除 10。
> `cash_dividend` 是方案公告的每股派现。公司持有回购专户股份时，交易所按总股本折算的「虚拟分派」计算除权参考价，比公告金额小，复权因子的跳变会相应略小（实测最多约 0.5%）。
> 注意：TDX `xdxr` 不拆分送/转，会把送转合计写入 `bonus_ratio`（`transfer_ratio=0`）；
> 总乘数正确，但送/转拆分仅在东财日更路径可区分。东财一条同时包含派息、送股、
> 转增的方案会拆成多条 `(symbol, ex_date, action_type)` 记录，避免单一 `action_type`
> 把其它分配分量置零。

`unit_split` 单独表达基金份额拆分或合并：1 份变 3 份记 `split_factor=3`，
10 份合为 1 份记 `0.1`，不假称送股或转增。旧记录缺失/空的 `split_factor` 按 1 解释；
新拆分记录必须给出明确、有限、正且不等于 1 的比例。复权核验把拆分乘数纳入除权
参考价计算；彼此冲突的拆分比例不能靠取最大值自动解决。拆分日期须为交易除权生效日，
不能混用权益登记日或公告日期。此字段不代表金额，不应用“每 10 股除以 10”的换算。

`reorg_transfer` 表达破产重整中的资本公积金转增：`transfer_ratio` 仍是每持有 1 股转增的股数，
按上式核算持股；但转增股份多数分给债权人和重整投资人，除权价不按 `1 + transfer_ratio` 计算，
而由发行人按交易所规则公布的除权（息）参考价决定。复权核验对这类记录使用
`前收盘 ÷ reference_price` 作为当日台阶，不再套用送转公式。`reference_price` 缺失、非正，
或出现在其他类型上，写入时即被拒绝。

#### adj_factors

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| adjust_type | string | qfq/hfq |
| factor | float64 | 累计因子；qfq：`1/sina_qfq_factor`，hfq：`sina_hfq_factor` |
| source | string | sina（默认） |
| data_version | string |  |
| fetched_at | timestamp |  |

#### financial_statement_items

时点（PIT）查询在读侧 **必须** 过滤 `announce_date <= as_of`
（`load(..., as_of=)`）；切勿仅按 `report_period` 对齐基本面。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| report_period | string | 如 ``2024Q1`` |
| statement_type | string | income / balance / cashflow / indicator |
| item_code | string | 见下表 |
| item_value | float64 | 金额单位人民币元；比率类为百分数；每股类为元/股 |
| announce_date | date | **PIT 轴** — 首次披露日（取自业绩报表 `RPT_LICO_FN_CPD`） |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

**item_code 一览**（按 `statement_type`）：

| statement_type | item_code |
|----------------|-----------|
| income | `revenue` `operating_cost` `operating_profit` `total_profit` `net_profit` `net_profit_deducted` `income_tax` `sale_expense` `manage_expense` `finance_expense` |
| balance | `total_assets` `total_equity` `total_liabilities` `inventory` `accounts_receivable` `monetary_funds` `fixed_assets` |
| cashflow | `net_cash_operate` `net_cash_invest` `net_cash_finance` `capex` `end_cash` |
| indicator | `roe` `eps` `eps_deducted` `bps` `gross_margin` `ocf_per_share` `revenue_yoy` `net_profit_yoy` |

口径提醒：

- `total_equity` 是**股东权益合计**（含少数股东权益），不是归母净资产；做 B/P 时注意分子口径，
  或改用 `bps`（每股净资产）× 股本。
- `capex` 取「购建固定资产、无形资产和其他长期资产支付的现金」，是代理量而非严格资本开支。
- **回填值是修订后的**：东财只提供某期财务数据的*当前*版本。回填拿到的是修订值，
  但配的是首次披露日，因此注册表将 `financial_statement_items` 标为
  `pit_quality="reconstructed"`（旧别名 `pit_grade="partial"`），而不是严格 PIT。
  `load(..., pit_mode="strict")` 会拒绝这类行；`pit_mode="best_effort"` 可以读取，
  但必须检查返回的 `pit_is_exact` / `pit_quality`。只有逐日累积、同时保存真实
  `available_at`/`observed_at` 的版本才可称为严格 PIT。
- **历史深度**：`cne backfill financial_statement_items` 默认走东财报告期自 **2001** 起
  （可用 `--start` / `--end` 分块）；不走 baostock。盘上实际起点见 `list_datasets().coverage_start`。

#### fund_flow

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| main_net_inflow | float64 | 人民币 |
| super_large_net_inflow | float64 |  |
| large_net_inflow | float64 |  |
| medium_net_inflow | float64 |  |
| small_net_inflow | float64 |  |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### fund_flow_ths

同花顺个股资金流，只在 push2 取不到 `fund_flow` 时写入（`steps/ths_fallback.py`）。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date | 同花顺页面不带日期，按收盘后到下一交易日开盘前所描述的交易日盖戳 |
| inflow | float64 | 流入资金，元（4 位有效数字） |
| outflow | float64 | 流出资金，元 |
| net_inflow | float64 | 净额 = 流入 − 流出，元；**不是**东财的主力净流入 |
| amount | float64 | 成交额，元 |
| change_pct | float64 | 涨跌幅，% |
| turnover_pct | float64 | 换手率，% |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### margin_trading

交易所来源分别采集沪、深两市。单个交易所暂未提供数据或后续分页失败时，已校验的数据仍可发布，但该日期标记为部分覆盖并保留补数缺口；完整重试发布后清除缺口。东财来源也检查两市覆盖，单市场响应不会视为完整日期。上交所未提供的 `short_balance` 保持为空。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| margin_balance | float64 |  |
| margin_buy | float64 |  |
| short_balance | float64 |  |
| short_sell_volume | float64 |  |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### northbound_holdings

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| channel | string | 沪/深股通 |
| holding_shares | float64 |  |
| holding_mv | float64 |  |
| holding_ratio | float64 |  |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### northbound_flows

| 列 | 类型 | 说明 |
|--------|------|-------|
| trade_date | date |  |
| channel | string | SH / SZ |
| net_buy | float64 |  |
| buy_amount | float64 |  |
| sell_amount | float64 |  |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### valuation_metrics

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| pe_ttm | float64 | 滚动十二个月市盈率（TTM）；东方财富 push2 行为空 |
| pb | float64 |  |
| ps_ttm | float64 |  |
| total_mv | float64 | 元；口径见 `total_mv_basis` |
| float_mv | float64 | 元；口径见 `float_mv_basis` |
| pe_dynamic | float64 | 动态市盈率（最新一期年化，push2 `f9`）；与 `pe_ttm` 口径不同，不可混用 |
| total_mv_basis | string | `vendor_reported`、`close_x_share_structure`（收盘价 × 当日有效总股本）或 `close_x_year_end_shares_estimate`（收盘价 × 上一年末总股本，估算）；旧行可空 |
| float_mv_basis | string | `vendor_reported`、`close_x_turn_implied_shares`（收盘价 × 换手率反推流通股）或 `close_x_turn_implied_shares_repaired`（由旧成交均价口径按收盘价/均价换算）或 `vwap_x_turn_implied_shares`（旧成交均价口径，无一致行情可换算）；旧行可空 |
| shares_as_of | date | 计算总市值所用股本的生效或统计日期；供应商报告值为空 |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### sector_members

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| sector_code | string |  |
| sector_name | string |  |
| as_of_date | date | 快照日 |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### announcement_index

PIT 查询过滤 `announce_date <= as_of`。

| 列 | 类型 | 说明 |
|--------|------|-------|
| announcement_id | string | 主键 |
| symbol | string |  |
| title | string |  |
| announce_date | date | **PIT 轴** |
| category | string |  |
| url | string |  |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### earnings_disclosure_schedule

预约披露时间表（EM datacenter `RPT_PUBLIC_BS_APPOIN`，镜像沪深交易所披露日历）。
现值语义、非 PIT：预约变更覆盖 `scheduled_date`，`first_scheduled_date` 保留首次预约，
`actual_date` 实际披露后回填（此前为 null）。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| report_period | string | 如 ``2026Q2``（分区键） |
| scheduled_date | date | 当前有效预约披露日 |
| first_scheduled_date | date | 首次预约披露日 |
| actual_date | date | 实际披露日，未披露为 null |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### dragon_tiger

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| reason | string |  |
| buy_amount | float64 |  |
| sell_amount | float64 |  |
| net_amount | float64 |  |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### block_trades

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| price | float64 | 元/股 |
| volume | float64 | **万股**（东财原表口径，交易所备用源同样保留） |
| amount | float64 | **万元** |
| premium_ratio | float64 | 相对收盘价折溢价 |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### index_constituents

| 列 | 类型 | 说明 |
|--------|------|-------|
| index_symbol | string | 如 ``000300.SH`` |
| symbol | string | 成分股 |
| as_of_date | date | 快照 / 调样日 |
| weight | float64 | 权重比例，可空；东财与国证调样史缺失时写 null；官方权重文件百分数转比例，总和约 1 |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### industry_members

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| classification_system | string | 如 ``sw``、``eastmoney`` |
| industry_code | string | 三级（末级）行业代码；`data_version=v1` 的东财行见下文 |
| industry_name | string | 三级行业名称；申万行等于代码 |
| l1_code | string | 一级行业代码，可为空 |
| l1_name | string | 一级行业名称，可为空 |
| l2_code | string | 二级行业代码，可为空 |
| l2_name | string | 二级行业名称，可为空 |
| l3_code | string | 三级行业代码，可为空 |
| l3_name | string | 三级行业名称，可为空 |
| as_of_date | date | 分类快照日 |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

每只证券每个分类体系每个快照日一行。东财数据中心把每只股票同时列在一、二、三级三个行业板块下，且不标层级：
板块越大层级越高；一级只有一个下级时两者成员相同，靠名称后缀（`Ⅱ`/`Ⅲ`）区分。
申万行只有六位三级代码，一、二级代码由它补零得到（`480101` → `480100` → `480000`），申万文件不发布名称，名称列为空。

`data_version=v1`（0.20 之前写入）的东财行只保存了三个板块中的一个，由来源分页顺序决定，层级逐日不定。
`cne migrate` 的 `industry-levels` 为这些行补上所存板块本身及其上级的层级列，下级保持为空，`industry_code` 不改；
按层级统计请读 `l1_*`..`l3_*`，不要读这些行的 `industry_code`。

行业代码本身不是身份：申万 2021-07-30 换代时复用了旧代码字符串。`cne derive industry_taxonomy` 把每个行业写成
`industry_id = 体系:版本:代码`（如 `sw:2014:480101`、`sw:2021:480101`、`eastmoney:board:1279`）存到
`meta/industry_taxonomy.parquet`，含层级、名称、上级 `parent_id`、首末出现的快照日和最大成员数；申万版本按快照日判定。
`cne derive industry_crosswalk` 在两边都有完整快照的同一天比较申万与东财同层级行业的成员，把每对行业的重叠证据和关系
（`equivalent`、`left_within_right`、`right_within_left`、`partial`）追加到 `meta/industry_crosswalk.parquet`；只是证据，不合并身份。

#### macro_indicators

| 列 | 类型 | 说明 |
|--------|------|-------|
| indicator_id | string | 如 ``shibor_3m``、``cnbond_yield_10y``、``lpr_1y`` |
| obs_date | date | 观测 / 发布日 |
| value | float64 |  |
| frequency | string | ``daily`` / ``monthly`` |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### market_breadth

由 curated ``daily_bars`` 相对前一交易日计算。

| 列 | 类型 | 说明 |
|--------|------|-------|
| trade_date | date |  |
| metric_id | string | ``advance_count``、``decline_count``、``limit_up_count`` 等 |
| value | float64 |  |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### share_unlock_schedule

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| unlock_date | date | 计划解禁日 |
| unlock_shares | float64 |  |
| unlock_ratio | float64 | 占流通/总股本比例（依源） |
| unlock_type | string | 如 IPO 限售、定向增发 |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### regulatory_events

| 列 | 类型 | 说明 |
|--------|------|-------|
| event_id | string | 主键 |
| symbol | string |  |
| event_date | date | 公告日 |
| event_type | string | ``penalty``、``investigation``、``regulatory_letter`` 等 |
| title | string |  |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### institutional_holdings

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| holder_type | string | ``fund``、``qfii``、``social_security`` 等 |
| report_period | string | 如 ``2024Q1`` |
| holding_shares | float64 | 持股数量或家数（依源） |
| holding_ratio | float64 | 占流通/总股本百分比 |
| holding_mv | float64 | 市值 |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### analyst_consensus

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| forecast_date | date | 发布 / 更新日期 |
| forecast_year | int64 | 目标财年 |
| eps_forecast | float64 | 一致预期 EPS |
| pe_forecast | float64 | 隐含 PE |
| target_price | float64 | 平均目标价 |
| rating | string | 如 买入/增持 |
| analyst_count | int64 | 覆盖机构数 |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### sentiment_scores

双通道：``announcement_keywords``（公告标题）与 ``stock_news_nlp``（东财个股新闻 + 关键词/SnowNLP）。

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| score_channel | string | 主键维度；``announcement_keywords`` / ``stock_news_nlp`` |
| sentiment_score | float64 | [-1, 1] |
| headline_count | int64 | 计入评分的标题数 |
| source | string | 溯源 |
| data_version | string | 溯源 |
| fetched_at | timestamp | 溯源 |

#### stock_news（按需缓存）

缓存 JSON：``meta/on_demand/stock_news/{symbol}.json``；经 ``cne query --dataset stock_news --symbol`` 拉取。

| 字段 | 类型 | 说明 |
|-------|------|-------|
| symbol | string | |
| items[].news_id | string | |
| items[].title | string | |
| items[].publish_time | string | |
| items[].publish_date | string | 可解析时为 ISO 日期 |
| items[].sentiment_score | float64 | 单条 NLP 分 |
| items[].sentiment_method | string | ``keyword`` / ``snownlp`` / ``keyword+snownlp`` |
| aggregate_sentiment | float64 | 条目分数均值 |
| headline_count | int64 | |
| source / data_version / fetched_at | | 溯源 |

#### share_structure

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| change_date | date |  |
| total_shares | float64 |  |
| float_shares | float64 |  |
| restricted_shares | float64 |  |
| float_a_shares | float64 |  |
| free_float_shares | float64 |  |
| change_reason | string |  |
| announce_date | date |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### shareholder_counts

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| count_date | date |  |
| holder_count | float64 |  |
| holder_count_change_pct | float64 |  |
| avg_float_shares | float64 |  |
| avg_holding_value | float64 |  |
| announce_date | date |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### top_holders

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| record_date | date |  |
| holder_scope | string |  |
| holder_rank | int32 |  |
| holder_name | string |  |
| holding_shares | float64 |  |
| holding_pct | float64 |  |
| is_institution | bool |  |
| holder_type | string |  |
| announce_date | date |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### industry_index

| 列 | 类型 | 说明 |
|--------|------|-------|
| trade_date | date |  |
| industry_code | string |  |
| level | string |  |
| weighting | string |  |
| ret | float64 |  |
| n_members | int64 |  |
| n_priced | int64 |  |
| n_excluded | int64 |  |
| amount | float64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### hot_rank

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| trade_date | date |  |
| rank | int64 |  |
| rank_change | int64 |  |
| hist_rank | int64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### sector_bars

| 列 | 类型 | 说明 |
|--------|------|-------|
| sector_code | string |  |
| sector_name | string |  |
| board_type | string |  |
| trade_date | date |  |
| open | float64 |  |
| high | float64 |  |
| low | float64 |  |
| close | float64 |  |
| volume | int64 |  |
| amount | float64 |  |
| change_pct | float64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### sector_fund_flow

| 列 | 类型 | 说明 |
|--------|------|-------|
| sector_code | string |  |
| sector_name | string |  |
| board_type | string |  |
| trade_date | date |  |
| main_net_inflow | float64 |  |
| change_pct | float64 |  |
| turnover_pct | float64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### sector_fund_flow_ths

同花顺行业与概念板块资金流，只在 push2 取不到 `sector_fund_flow` 时写入；板块分类是同花顺的，和东财板块代码不对应。

| 列 | 类型 | 说明 |
|--------|------|-------|
| sector_code | string | 同花顺板块代码（行业 881xxx；概念取自详情页链接） |
| sector_name | string |  |
| board_type | string | industry / concept |
| trade_date | date | 同 fund_flow_ths |
| sector_index | float64 | 板块指数点位 |
| change_pct | float64 | 涨跌幅，% |
| inflow | float64 | 流入资金，元（页面单位亿，精确到 0.01） |
| outflow | float64 | 流出资金，元 |
| net_inflow | float64 | 净额，元 |
| company_count | int64 | 公司家数 |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### news_headlines

| 列 | 类型 | 说明 |
|--------|------|-------|
| news_id | string |  |
| publish_date | date |  |
| publish_time | string |  |
| title | string |  |
| summary | string |  |
| related_symbols | string |  |
| channel | string |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### flash_news_wire

| 列 | 类型 | 说明 |
|--------|------|-------|
| wire_id | string |  |
| wire_source | string |  |
| item_hash | string |  |
| publish_date | date |  |
| publish_time | string |  |
| title | string |  |
| summary | string |  |
| related_symbols | string |  |
| importance | int8 |  |
| channel | string |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### economic_calendar

| 列 | 类型 | 说明 |
|--------|------|-------|
| event_id | string |  |
| event_date | date |  |
| event_time | string |  |
| country | string |  |
| indicator | string |  |
| importance | int8 |  |
| forecast | float64 |  |
| previous | float64 |  |
| actual | float64 |  |
| unit | string |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

#### delisting_events

| 列 | 类型 | 说明 |
|--------|------|-------|
| symbol | string |  |
| first_trade_date | date |  |
| last_trade_date | date |  |
| ending_pattern | string |  |
| final_close | float64 |  |
| halt_gap_days | int64 |  |
| worst_final_return | float64 |  |
| final_window_return | float64 |  |
| bars | int64 |  |
| source | string |  |
| data_version | string |  |
| fetched_at | timestamp |  |

### Compact 去重

Compact 时按主键分组，保留 `fetched_at` 最大的一行。

### DuckDB 视图

优先用 `cne init` / compact 生成的 `{data_root}/duckdb/cnequity.duckdb` 视图，
不要手写整层 glob。`hive_partitioning=true` **仅**适用于按日分区的数据集
（目录值为 `YYYY-MM-DD`）；年/月分区必须 `hive_partitioning=false`（真实日期在文件列里）。
见 [lake-layout](../architecture/lake-layout.md)。

```sql
-- daily_bars / adj_factors 为按日分区，hive=true 安全
CREATE VIEW daily_bars_view AS
SELECT * FROM read_parquet('{root}/curated/daily_bars/**/*.parquet', hive_partitioning=true);

CREATE VIEW daily_bars_adj AS
SELECT b.*, b.close * a.factor AS adj_close
FROM daily_bars_view b
LEFT JOIN read_parquet('{root}/derived/adj_factors/**/*.parquet', hive_partitioning=true) a
  ON b.symbol = a.symbol AND b.trade_date = a.trade_date AND a.adjust_type = 'qfq';

-- 反例：index_bars 等按年分区时必须关掉 hive，否则目录 "1993" 会污染 DATE 列
-- SELECT * FROM read_parquet('.../index_bars/**/*.parquet', hive_partitioning=false);
```
