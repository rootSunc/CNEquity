---
description: 商品期货与期权日频数据：从采集、配置到研究验收，含数据源能力与口径限制。
---

# 商品期货与期权：从采集到研究验收

这条链路适合盘后日频研究。[源能力表](../datasets/sources.md)由读取器注册表生成并由 CI 检查。交易所日文件是主源，新浪补充 DCE 期货及小范围期货分钟线。日线结算价不代表同步可成交价格；当前没有期权盘口，不能据此验证多腿套利可执行性。实时行情接入是独立工作。

## 配置与第一轮验证

先建立独立研究湖配置，无需复制源码模板：

```bash
cne config create --config configs/cnequity.futures.toml --data-root data/cnequity-futures
```

编辑生成文件中已有的 `[futures]` 和 `[sources.futures_exchange]` 段落，不要重复追加同名段。下文均使用这份配置；已有正式湖也可仅合并相关项，不替换其他设置：

```toml
[futures]
enabled = true
exchanges = ["SHF", "CZC", "GFE", "CFE"]
options = true
dce_route = "sina"
minute_enabled = false

[sources.futures_exchange]
enabled = true
min_interval_seconds = 1.0
```

限速键以项目示例中的 `[sources.futures_exchange]` 为准。间隔不是交易所承诺的安全配额；先缩小范围并利用缓存，不要以增加并发解决失败。

保存后执行 `cne config validate --config configs/cnequity.futures.toml`。上面的明确交易所列表不含 DCE；需要新浪大商所期货时将 `"DCE"` 加入 `exchanges`。仅设置 `dce_route="sina"` 不会越过交易所范围筛选，也不会启用 DCE 期权。

先审阅小窗口计划，再执行。以下日期只作示例，应换成自己的研究窗口：

```bash
cne backfill futures_bars --config configs/cnequity.futures.toml --exchange SHF --start 2026-09-21 --end 2026-09-24 --plan
cne backfill futures_bars --config configs/cnequity.futures.toml --exchange SHF --start 2026-09-21 --end 2026-09-24
cne backfill option_bars --config configs/cnequity.futures.toml --exchange SHF --start 2026-09-21 --end 2026-09-24
cne status --config configs/cnequity.futures.toml --datasets --groups derivatives --all-columns
```

独立期货湖用 `--groups derivatives` 限定状态门禁，不要求先采集 A 股。最新交易日的截面按交易所证明：某交易所有当天的官方合约文件（`meta/derivatives/references/` 下，按期货/期权分开），且文件里当天在市的合约都有行情，该交易所就算已证明完整，全部可证明的交易所都满足时显示 `OK`。走第三方路由的交易所（默认 `dce_route = "sina"` 的大商所，无成交日没有行）无法证明，作为限制写在结果旁，不判失败；改用官方路由可以证明。缺当天官方文件时状态为 `UNVERIFIED` 并退出 2，表示证据不足；它与已发现缺口的退出 1 不同，具体范围用下文的窗口验收查看。

`--plan` 不联网、不创建湖，报告交易所、源端历史下限、日期、冷缓存请求估算、限速及后续步骤。行情估算不含重试；参考请求另列上界，不是耗时承诺。DCE 新浪以“合约整段历史”为请求单位，一个日期也可能枚举约 299 个候选代码；跨月或长窗口涉及更多代码。`--exchange` 可重复；INE 归入 SHF 路由，2018 年 3 月 26 日起的期货需同时请求上期所与能源中心日文件，冷缓存估算计入这两个请求。显式回填一个衍生品数据集会开启本次调用所需开关，不改配置文件。

日线回填完成后自动重建对应合约表、compact，再更新连续期货/Greeks。输出的 `followup` 是后续运行状态；采集和派生成功仍不等于研究窗口完整。修复已有历史时可显式执行：

```bash
cne backfill futures_bars --config configs/cnequity.futures.toml --exchange SHF --start 2026-09-21 --end 2026-09-24 --refetch
cne backfill futures_contracts --config configs/cnequity.futures.toml
cne backfill option_contracts --config configs/cnequity.futures.toml
cne derive futures_continuous --config configs/cnequity.futures.toml
cne derive option_greeks --config configs/cnequity.futures.toml
```

合约回填带 `--start/--end` 时，读取该窗口内已有行情日期的历史参考文件，并归档后重建；不带日期时只读各交易所最新参考。这样可恢复已从当前清单退出的历史合约日期。参考历史修订按 `as_of` 排序，旧文件不会覆盖更新的归档证据。拒绝/挑战会停止该来源本批次的参考扫描。先用 `--plan` 看 `reference_requests_upper_bound`；没有参考适配器的路由仍无法补生命周期。广期所参考接口仅提供当前快照，不支持历史回放；显式历史窗口会跳过它并报告 warning，普通运行以实际读取日归档，不能当作过去时点的证据。

因此，GFE 历史行情可能已成功落盘，但自动合约步骤返回 `degraded`，整条 `backfill` 仍退出 1。先检查输出的 `slices`、`followup` 和 `cne status --run RUN_ID --config configs/cnequity.futures.toml`，区分行情缺失与历史参考不足；不要因退出码非零就反复重下同一日文件。不带日期重建合约可补当前参考，不能补造历史证据。

```bash
cne backfill option_contracts --config configs/cnequity.futures.toml --exchange CFE --start 2026-09-18 --end 2026-09-18 --plan
cne backfill option_contracts --config configs/cnequity.futures.toml --exchange CFE --start 2026-09-18 --end 2026-09-18
```

`--refetch` 忽略完成收据和旧响应缓存，但不绕过冷却/熔断。没有收据的旧数据不会仅因“每所有一行”就跳过。`--refetch` 只适用于 `futures_bars`、`option_bars` 和 `sector_bars`，其他数据集报错。连续期货必须全量重算，不能给 `--start/--end`；Greeks 支持窗口和 `--full`，通常自动检测依赖即可。

## 日更接入

完成小窗口回填后，单独更新衍生品使用：

```bash
cne run daily --group derivatives --config configs/cnequity.futures.toml
```

普通日更补最近的回看窗口和有限旧欠账，不会替代首次历史回填。默认组内顺序通过依赖保证先采集日线、补合约元数据，再 compact、派生连续期货及 Greeks；任一来源失败会保留失败或降级状态，不能凭已有派生行判定本轮完整。

正式湖启用 `[futures]` 后，`cne run daily` 和未用 `CNE_GROUPS` 限定组别的 `daily_pipeline.sh` 会自动包含 `derivatives`。旧 launchd 安装保留了显式 `CNE_GROUPS` 时，须把 `derivatives` 加入该列表；配置里没有 `derivatives` 组时运行 `cne config upgrade` 补上；`cne run daily --core-only` 只执行核心骨架，不会采集期货。独立期货湖使用上面的单组命令，避免同时采集 A 股其他组。

## 证据和故障恢复

`meta/derivatives/` 保存以下可检查证据：

| 目录 | 用途 |
|---|---|
| `futures_bars/YYYY-MM-DD/`、`option_bars/YYYY-MM-DD/` | 每发布者收据：解析版本指纹、隔离前规范化/有效/拒收行数、合约集合、内容指纹及状态 |
| `quarantine/` | 被隔离的异常行，不能把“过滤之后合法”当作“当日完整” |
| `references/` | 成功读取的交易所参考快照；过期合约的权威日期不会因参考失败消失 |
| `http_cache/` | 带内容哈希的原始响应缓存；每次复用仍需解析和校验 |
| `greeks_dependencies/` | 各日期的输入内容、模型代码和结果指纹 |

收据状态区分下载成功 `captured`、发布后核对 `committed` 和欠账 `owed`。完成跳过还会重新核对实际落盘内容。失败、拒收和未提交的数据继续欠着，普通日更每次最多重试 3 个旧欠账日期，超出三日回看窗口也不会消失；大量积压用显式日期回填处理。

2018 年能源中心期货须从其[官方日文件](https://www.ine.cn/data/tradedata/future/dailydata/kx20180326.dat)读取；该年首日的上期所文件不含 INE 行。2019 年起，上期所日文件同时发布 SHF 与 INE 行。常规 `SHF` 收据校验合并后的内容；仅导入能源中心历史行时可另记 `INE` 收据，按 INE 行独立校验，不能拿仅含 INE 的行去确认原 `SHF` 收据。

官方历史文件默认缓存 30 天，最近 7 天的文件缓存 1 小时；Sina 活跃/未上市合约历史缓存 1 小时，交割月已过去超过 62 天的历史缓存 30 天，报价缓存 60 秒，分钟窗口缓存 30 秒。DCE 已核对的历史日常规回看复用，源端历史修订通过 `--refetch` 核对。缓存不是永久无修订承诺，也没有无限保留空间的保证，按需要清理 `http_cache` 会增加下次请求量。

HTTP 403/412/429/456 和暂时性服务端拒绝会触发至少 300 秒冷却，尊重更长的 `Retry-After`，并停止该来源当前批次。日更不会用其他交易所的成功掩盖失败。网络断连最多再试一次，不做压力探测，不执行挑战脚本。

同一出口的多个湖/CLI 可设置相同的 `CNE_RATE_LIMIT_ROOT=/absolute/path/to/shared-budget`，共享节流、并发和拒绝状态；所有相关进程必须采用同一路径。未设置时只在单湖内共享。不同主机还需要共同的限流服务/协调，单机目录不能保证跨主机总预算，更不能保证 IP 永不被封。

## 研究验收与查询

不能用最早/最新日期推断中间完整。先明确研究窗口、品种及频率，检查各交易所缺日、已知合约缺行、元数据覆盖、隔离与欠账，再看派生是否失效。`status --datasets` 区分缺口与 `unverified`：前一交易日合约覆盖不是新挂牌清单的完整证明；DCE 新浪尤其缺零成交日。当前没有完整历史在市清单，不能给全市场完整背书。

郑商所旧版官方日文件对 2010-06-21、2010-07-01、2010-08-13 返回 404；已验证的同年其他日期可以使用，但这三天继续记为 `owed`。跨过这些日期的连续调整链保留空值，不能把 2010 年历史标为无缺口。

能源中心 2021-07-28 官方期货日文件中的 LU2108 收盘价高于当日最高价；该行已隔离且 INE 当日收据仍为 `owed`，其余通过校验的合约行可以单独使用。上期所 2004 年部分换月日前官方文件没有所需的重叠合约价格，2018 年燃料油和线材停报区间末日也没有相应行；连续调整因子保持 null，不能插值或置为 1。

可直接用 CLI 检查指定窗口（只读、不联网）：

```bash
cne verify --derivatives --dataset futures_bars --start 2026-09-01 --end 2026-09-24 --config configs/cnequity.futures.toml
cne verify --derivatives --dataset option_bars --start 2026-09-01 --end 2026-09-24 --config configs/cnequity.futures.toml
```

JSON 包含缺日、已知合约缺行、缺元数据、欠账及品种覆盖。退出 1 为已发现缺口，2 为证据不足；最新截面正常不会掩盖窗口中间的缺行。无完整历史在市清单时不会报告全市场完整。此命令不支持自动修复；审阅具体缺口后使用前述有范围的回填命令。它尚不能证明历史交易参数与派生序列在整个研究窗口内有效。

以下读取已发布版本；需要复现时按 [查询指南](../datasets/query-guide.md) 固定 `revision_map`。衍生品不要套股票的 `universe` 或股票复权参数。

```python
import polars as pl
from cnequity.config import load_config
from cnequity.query import load
from cnequity.quality.derivative_checks import exchange_session_gaps
from datetime import date

cfg = load_config("configs/cnequity.futures.toml")
print(exchange_session_gaps(cfg, "futures_bars",
                           start=date(2026, 9, 21), end=date(2026, 9, 24)))

# 当日铜期货期限结构：按实际交割月份排序。
futures = load("futures_bars", config=cfg, start="2026-09-24", end="2026-09-24")
contracts = load("futures_contracts", config=cfg)
curve = (futures.filter(pl.col("product") == "CU")
         .join(contracts.select("symbol", "delivery_month", "last_trade_date", "dates_basis"), on="symbol")
         .select("symbol", "delivery_month", "settle", "open_interest", "last_trade_date", "dates_basis")
         .sort("delivery_month"))
print(curve)

# 同一标的的 T 型期权链；空 expiry/status 不应填成可用定价。
options = load("option_bars", config=cfg, start="2026-09-24", end="2026-09-24")
meta = load("option_contracts", config=cfg)
chain = (options.filter(pl.col("underlying_symbol") == "CU2611.SHF")
         .join(meta.select("symbol", "expiry_date", "exercise_style", "dates_basis"), on="symbol", how="left"))
t_shape = chain.pivot(on="option_type", index=["strike", "expiry_date"], values="settle").sort("strike")
print(t_shape)
```

`first_seen_date/last_seen_date` 是观测边界；只有权威参考填入 `list_date/last_trade_date/expiry_date`。旧 `observed*` 日期必须重建。兼容字段 `expiry_month` 实际是标的合约月份，不能代替真实到期日。规格表也不是完整的历史交易参数库；FB 等交易途中变更且未核实的历史区间保留未知。

连续期货 `oi_t-1_v2` 用前一交易日持仓选合约；无合法更远月份就不输出该序列，不向近月回退。它按交易所各自的已观测期货会话补充共享日历：早年上期所官方日文件有少数日期被共享股票日历标为休市，不能因此丢掉次日连续行。缺前一交易日或换月价格会使调整因子失效并保持 null；不能把 null 填成 1。调整序列可作信号，盈亏应按真实合约与换仓交易计算。

Greeks 根据结算价和模型计算，合约、利率、模型及行情变化均会触发重算。未知到期日或行权方式保留 `no_expiry/no_exercise_style`，到期日不反解 IV。`forward_source=parity` 的远期是由期权价格倒推，不能再用它验证同一组价格的平价套利。

日频 CTA 还需明确手续费（含平今）、保证金、涨跌停、乘数/最小变动价位生效区间、交割约束及信号可用时点。这些应作为策略的外部版本化输入，不能默认零费用或无约束。当前合约元数据不是严格历史 PIT 参数库。[上期所业务参数](https://www.shfe.com.cn/reports/businessdata/prmsummary/)和[上期能源每日结算参数](https://www.ine.com.cn/reports/tradedata/dailyandweeklydata/)是后续建设历史参数的候选来源，现有湖尚未逐日验收并导入。能源中心[官方手册](https://www.ine.com.cn/upload/20240410/1712738949255.pdf)还明确区分交易所向会员收取的保证金率与客户实际保证金率。库存/仓单/会员排名类 CTA 需要另外的数据集，不能由现有 OHLC 补出。

上期所提供月度调整表和每日结算参数文件。项目当前只解析并保存原始参数证据，尚未把它们发布为可直接用于回测的逐日费率或保证金表。月度文件不能覆盖逐日变化；文件更新时间通常在收盘后，会员参数也不等于客户实际费率。研究时应自行核对公告、生效交易日、节假日调整和平今费用，保留所用参数版本。

## 分钟数据

```bash
cne backfill futures_minute_bars --config configs/cnequity.futures.toml --symbols CU2611.SHF,M2701.DCE --plan
cne backfill futures_minute_bars --config configs/cnequity.futures.toml --symbols CU2611.SHF,M2701.DCE
```

这里只取最新窗口，拒绝历史日期参数。超过 watchlist 上限会报错，不再默默截掉合约。启用后必须每天调度；即使 derivatives 组设为 weekly，滚动分钟窗口也会在交易日执行，历史日线仍按组节奏运行。需要每日 CTA 信号时，日线组也必须设为 daily。夜盘目前按交易日历映射，尚非完整的分品种夜盘/临时停市日历；不能把窗口数据当作完整 tick/盘口历史。

## 官方年度包：已验证格式与使用限制

上期所[官方数据下载](https://www.shfe.cn/reports/tradedata/datadownload/)的 `download.json` 列出具体年度 ZIP 链接，链接可能随修订变化。2026 本年度包内含 SHF、INE 的逐合约月度 XLSX，单个工作簿同时包含期货和期权；不能用网页“期货/期权”按钮名称直接划分包内数据。

`cnequity.adapters.futures_exchange.shfe_archive.iter_archive(path, year=2026)` 提供本地只读解析，逐工作簿返回 `(文件名, {数据集名: DataFrame})`。解析器拒绝未验证的表头、跨年日期、非整数手数、重复主键及超限归档。已核验 2002—2008 年的 `.xlsx` 单边口径、2009—2019 年带“双边计算”说明的分组 `.xls`（成交量、成交额、持仓量按单边折算），以及 2020—2024 年的分组 `.xls` 单边口径。对其他年份或不同表头不能套用这些规则。默认日文件回填仍走原路，年度 ZIP 只能显式离线导入：

```bash
cne import shfe-archive /data/shfe-2025.zip --dataset futures_bars --year 2025 --accept-partial-fields --config configs/cnequity.futures.toml
cne import shfe-archive /data/shfe-2025.zip --dataset futures_bars --year 2025 --accept-partial-fields --config configs/cnequity.futures.toml --apply
cne import shfe-archive /data/shfe-2025.zip --dataset option_bars --year 2025 --accept-partial-fields --config configs/cnequity.futures.toml --apply
```

导入没有源网络请求，会复制原 ZIP 到湖的 `meta/derivatives/annual_archives/`，记录哈希、导入时间、成员、拒绝行及缺失字段；如果知道官方原链接和下载时间，可加 `--archive-url https://www.shfe.com.cn/...` 与 `--archive-downloaded-at 2026-09-27T09:00:00+08:00` 作为来源证据。`--plan` 只说明导入范围和字段限制，不解析 ZIP。每个验证成功的工作簿独立暂存并自动发布；较晚成员损坏时，已验证成员仍可发布，同时返回非零状态和缺口。任何已有逐日行的主键都跳过，年度行不会覆盖完整日线。导入记录的 `coverage_status=partial_fields`，也不会生成“逐日完整”收据；需要日文件独有字段的研究或严格覆盖查询仍要补日文件。原始下载时间若没有独立证据，导入不会把本地文件修改时间冒充下载时间。

年度工作簿的格式会随年份与交易所变化：可能是 `.xls` 或 `.xlsx`、按合约分组，
也可能在后续行省略合约代码。解析器只接受已识别的表头和单位，遇到未知格式会拒绝，
不能把某一年的样本核验推断为所有历史年份的完整覆盖。INE 历史文件路由与 SHF
也可能不同；研究窗口仍须按交易所、合约和交易日检查缺口。

部分官方历史行本身会违反 OHLC 边界，早期日文件也可能缺成交额。
CLI 导入会把不合格行隔离到拒收台账并返回警告，不会把该交易日声明为完整；
缺少独立日文件对照时，年度包中的成交额也不能声称已逐日核实。

年度工作簿缺 `oi_change`，期权还缺行权量、Delta 和隐波字段，这些留空，不填 0。年度包可用于冷启动的价格/成交量历史，但不能作为完整日文件替代。导入后可按日文件补缺并重建合约与派生序列。年度包也不提供权威历史生命周期。
