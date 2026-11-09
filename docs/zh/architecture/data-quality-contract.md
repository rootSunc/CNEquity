# CNEquity / USEquity 共享数据质量契约（Data Quality Contract）

版本：1.0 · 状态：目标规范（2026-11-07）；CNEquity 目前实现了哪些、与之有何差异，见[第 7 节](#cnequity-status)  
适用范围：`CNEquity`（A 股个人投资研究数据基础设施）与 `USEquity`（美股个人投资研究数据基础设施）共享基础设施层。

---

## 1. 核心原则

**宽松接入、明确质量、保留来源、谨慎推导。**

本契约的目标是**“可信地使用不完美的数据”**，而不是“只接受完美的数据”。两个系统均主要依赖免费或低成本可访问数据源，数据质量体系用于**诚实描述数据的来源、新鲜度与局限性**，而不是将免费数据源的天然缺陷伪装成系统错误或入库阻断门禁。

1. **Data Quality Warning 不自动变成 Pipeline Failure**。
2. **质量分级（Tier A/B/C）是下游使用建议，不是数据库写入开关**。
3. **不强制多源交叉验证作为入库前提**；多源对比是可选的增强能力。
4. **不在核心入库链路引入人工审批状态机（Review Packet / Grant）**；质量问题通过 Metadata 与 `quality_flags` 表达。

---

## 2. 质量三维模型

所有数据集的质量评估拆分为三个正交维度，不再使用单一的“合格/不合格（Pass/Fail）”标签：

| 维度 | 核心问题 | 处理原则 |
|---|---|---|
| **Availability（可用性）** | 数据能否拿到？缺失怎么办？ | 尽可能获取；允许字段级、标的级或日期级缺失存在，不要求全市场 100% 覆盖率 |
| **Consistency（一致性）** | 格式、单位、字段含义是否统一？ | 在 Adapter / Normalization 层统一数据结构、日期格式、数值单位与 `NULL` 语义；保留原始值备查 |
| **Reliability（可靠性）** | 数据来自哪里？有多可信？有何局限？ | 每条/每批数据明确标记来源（Provider）、采集时间、质量等级（Tier A/B/C）与已知局限（Caveats / Warnings） |

---

## 3. 异常与边缘情况分类处理规范（Error vs. Warning vs. Caveat）

只有违反数据结构、主键或基础物理约束时才拒绝写入（`ERROR`）；业务缺口与源端局限均正常入库并打标。

### 3.1 处理矩阵

| 情况 | 分类 | 管道行为 | 标准层表现 |
|---|---|---|---|
| **某个非主键字段缺失** | `NORMAL` | 正常接受 | 字段置 `NULL` |
| **某天没有交易 / 休市 / 停牌无成交** | `NORMAL` | 正常接受，绝不补造/插值日线 | 不生成虚假 Bar，或在状态字段标记 `no_trade` / `halted` |
| **Provider 返回 `0`、`""`、`N/A`、`-999` 表示缺失** | `NORMAL` | Adapter 负责识别并转换为标准 `NULL` | 字段置 `NULL`，必要时记录原始表示 |
| **某个数据源暂时不可用（超时 / 429 / 5xx）** | `OPERATIONAL_WARNING` | **保留旧数据**，记录采集失败日志，不清空已入库数据 | 水位保持上次成功状态，任务标 `partial` 或 `degraded` |
| **多个来源数值不一致** | `QUALITY_WARNING` | 保留各来源观测值；标准层按预设主源优先级取值，不强行报错阻断 | 写入主源值，附带 `quality_flags=["cross_source_divergence"]` |
| **数据明显异常（如单日暴涨暴跌疑似未记录拆股、极端离群值）** | `QUALITY_WARNING` | 正常入库（或进入隔离视图供排查），标记 Warning 并降级为 Tier `C` | 写入记录，`quality_tier="C"`，附带具体异常标签 |
| **数据格式/结构错误（主键缺失、`NaN`/`Inf`、负成交量、`high < low`、非法日期）** | `HARD_ERROR` | **拒绝该条/该批错误记录**，记录错误明细，不阻断同批次其他合法标的 | 不进入标准表，写入错误日志供 Adapter 修复 |

### 3.2 硬性错误（Hard Error）的严格边界

仅以下 5 类情况允许触发 `HARD_ERROR` 并阻止单条记录写入标准表：

1. **主键或身份缺失**：如缺少内部 `instrument_id` / `security_id` 或无法解析有效时间键（`trade_date` / `period_end`）。
2. **类型或格式损坏**：无法解析的日期字符串、`NaN`、`Infinity`、非预期的 JSON/XML 结构损坏。
3. **基础算术/物理悖论**：
   - 行情数据：`high < low`、`high < open`、`high < close`、`low > open`、`low > close`、`volume < 0`、普通证券价格 `<= 0`（明确允许负值的利率/宏观序列除外）。
4. **重复主键冲突**：同一主键在同一来源版本内出现重复且无法幂等去重的记录。
5. **Schema 契约违背**：缺少必填列或列类型与注册 Schema 不兼容。

---

## 4. 数据质量分级（Quality Tiers）

采用三级分类，作为下游分析、筛选和策略研究的消费参考：

| 等级 | 名称 | 定义与判定标准 | 典型数据示例 | 消费建议 |
|---|---|---|---|---|
| **A** | **Trusted** | 官方监管/央行披露，或来源明确、通过基础完整性与一致性校验的核心数据 | 交易所官方日历、巨潮/上交所/深交所/SEC 官方披露、FRED/央行宏观序列、校验通过的主源日线 | 适合常规基本面研究、筛选、估值与标准分析 |
| **B** | **Usable with Caveats** | 来源明确且格式规范，但存在部分字段缺失、发布延迟、未做跨源交叉验证，或受限于免费源已知局限 | 免费源日线（AKShare / Baostock / Yahoo / Stooq）、分红拆股、Company / ETF Profile、缺少精确秒级披露时间戳的历史财务数据 | **默认可用**；在分析报告或衍生计算中需知晓其局限（如不直接用于严格高频/无偏回测） |
| **C** | **Unverified** | 来源不确定、抽取逻辑未经验证（如未经核对的网页/LLM 抽取），或触发了明显的数据异常 Warning（如疑似未复权跳变、报表恒等式严重失衡） | 出现极端价格跳变未匹配除权拆股的日线、启发式映射候选、新闻正文自动抽取的数字 | 不建议直接用于投资结论；查询接口默认可过滤或显著提示，仅供探索与人工排查 |

> **关键规则**：Tier `A`、`B`、`C` 均正常存储在标准层中。读取 API 默认返回 Tier `A` 和 `B`（并附带各自的 `quality_tier` 与 `quality_flags`），调用方可通过参数显式包含 Tier `C` 或仅筛选 Tier `A`。

---

## 5. 最小溯源与质量元数据（Provenance Envelope）

为避免溯源机制比业务字段更沉重，CNEquity 与 USEquity 的标准层数据集统一采用轻量级公共元数据列：

| 字段名 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `source` | `string` | 是 | 数据源标识（如 `akshare`, `baostock`, `sec_edgar`, `yahoo_finance`, `stooq`, `fred`） |
| `ingested_at` | `timestamp (UTC)` | 是 | 数据采集/写入本系统的 UTC 时间 |
| `published_at` | `timestamp / date` | 否 | 源端披露或发布时间（如财报 `announce_date` / SEC `acceptance_datetime`；未知则为 `NULL`，绝不伪造） |
| `quality_tier` | `enum('A','B','C')` | 是 | 数据质量分级（`A` / `B` / `C`） |
| `quality_flags` | `list<string>` | 是 | 质量告警标签列表；无告警时为空列表 `[]` |

---

## 6. CNEquity 与 USEquity 的共享与独立边界

两套系统共享同一套工程底座与本数据质量契约，但**绝不强行合并市场业务规则**：

| 维度 | 共享基础设施（Shared Infrastructure） | 市场独立实现（Market-Specific） |
|---|---|---|
| **身份管理** | 稳定内部 ID（`instrument_id`）+ 代码有效期映射表结构 | **CN**：`600519.SH` 等交易所代码、A/B/H 股关联<br>**US**：`issuer_id (CIK)` + `security_id`（区分 `GOOG`/`GOOGL`）+ Ticker 历史复用映射 |
| **日历与时段** | 交易日历查询接口与日期区间过滤逻辑 | **CN**：沪深北交易日、午休时段、调休规则<br>**US**：NYSE/Nasdaq 交易日、半日市、盘前/常规/盘后时段 |
| **行情与公司行动** | OHLCV 标准表结构、拆股/分红事件表结构、复权计算接口 | **CN**：涨跌停限制、ST/*ST 标记、停复牌、送转配股<br>**US**：Split / Reverse Split、Cash Dividend、Spin-off、ADR 换股比例 |
| **基本面数据** | 财务事实存储、报告期对齐、标准化指标派生框架 | **CN**：中国企业会计准则（CAS）固定报表结构、按日历季披露<br>**US**：SEC EDGAR / XBRL Company Facts、GAAP/Non-GAAP、公司自定义财年（Fiscal Calendar） |
| **数据质量与运维** | **本契约（Error/Warning/Caveat、Tier A/B/C、Provenance 字段）**、Parquet + DuckDB 存储、CLI / MCP | 各市场专属的异常阈值（如 A 股涨跌停阈值 vs 美股无涨跌停跳变阈值） |

---

## 7. CNEquity 当前实现情况 {#cnequity-status}

本契约描述两个系统共同的目标。CNEquity 目前**没有** `quality_tier`、`quality_flags` 与 `ingested_at` 三列，读取接口也不按质量等级过滤；下游代码不要依赖这些列。下表逐条列出 CNEquity 今天的做法。

| 契约条款 | CNEquity 现状 | 状态 |
|---|---|---|
| `source` | 每个数据集都有 `source`，与 `data_version`、`fetched_at` 一起构成溯源列 | 已实现 |
| `ingested_at` | 对应 `fetched_at`（UTC 时间戳） | 已实现，列名不同 |
| `published_at` | 没有统一列。每个数据集在契约中声明可得时间列 `availability_col`（PIT 表为 `announce_date` 等），PIT 表可另带 `available_at`、`source_published_at` | 部分实现 |
| `quality_tier`（A/B/C） | 没有行级等级。数据集级有两个相近的声明：支持档位 `support`（`core` / `extended` / `best_effort` / `optional`，见[数据集目录](../datasets/catalog.md)）与 PIT 证据等级 `pit_quality`（见[数据契约](../datasets/contract.md)） | 未实现 |
| `quality_flags` | 没有行级标签。质量问题由 `cne check` / `cne audit` 报告为检查结果，带 `info` / `warning` / `error` 严重度 | 未实现 |
| 读取默认排除 Tier `C` | 读取接口不做质量过滤 | 未实现 |
| 缺失值写成 `NULL`，不写 `0` | 数据集登记的取值规则（`value_rules`）在写入校验时把厂商占位 `0` 转为 `NULL`，无成交日的价格占位同样置空。少数期货适配器仍会把空单元格读成 `0`，待修复 | 大部分实现 |
| 无交易 / 停牌不造 Bar | 不补造行情；停牌状态记在 `trading_status`（`normal` / `suspended` / `delisted`） | 已实现 |
| 数据源暂时不可用 | 失败的步骤不提交新版本，已入湖数据保持不变；运行标记为 `degraded`，覆盖标记为 `partial` | 已实现 |
| 跨源不一致 | `cne check` 对照参考源（如 `daily_bars_close_mismatch`），个别不一致报 `warning`，全市场性不一致报 `error`；不写入行标签 | 部分实现 |
| 严重异常降级为 Tier `C` | 没有降级机制；异常只作为检查结果报告 | 未实现 |
| `HARD_ERROR` 只拒绝问题行 | 写入校验发现必填列为空、非有限数值或行情不变量（如 `high < low`）被违反时，**整批**拒绝。只有衍生品行情、北交所涨跌停越界与日线探测占位行按行隔离到 `_quarantine` | 部分实现 |
| 重复主键 | 不拒绝：同一主键按证据类别、`fetched_at` 先后与来源优先级保留一行；PIT 表保留各个版本 | 与契约不同 |
