# Changelog

本项目的重要变更都记在这里。格式遵循 [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)；
版本号遵循 [Semantic Versioning](https://semver.org/)。

## [0.11.0] — 2026-09-20

### 新增

- **`cne init` 在开跑前就说清楚这一轮覆盖什么。** 市场宽度、时间顺序，以及那句最容易被
  误读的「400 只」—— 它是最慢的历史 ST 扫描每轮先处理的证券数，不是 init 的全部取数
  范围。续跑不再用默认 profile 悄悄缩短一个更早的 run 的历史窗口：`--since` 是唯一
  有意的覆盖。

- **全市场的湖不再把沪深子集当成全市场发布。** 配置里包含北交所、却一只 active BJ
  证券都没取到时，`instruments` 直接失败。已建成的湖在板块接口临时不可用时仍可携带
  已有的 active BJ 行前进 —— 这条只拦新装的湖，因为那才是缺口会变成「事实」的地方。

- **`cne status --datasets` 证明最新交易日的标的截面。** 对每个按「当日 active 证券」
  建键的数据集（`daily_bars`、`trading_status`）比较 tip 分区与证券表：有数据、有明确
  停牌证据、或已记入待补账本的算覆盖。`index_bars` 按指数 universe 建键，不在其列 ——
  拿证券表去量它只会凭空造出缺口。同时报告尚未跑完的 init。

- **run 在跑的过程中就报告已写入的行数。** 以前只有 `finish_run` 写这两个数，于是
  正在跑的 run 显示 0、跑挂的 run 永远显示 0 —— 而「跑到哪了」恰恰是那时候要问的。

- **交易所每日读板成为 BJ 的 ST 证据来源。** 北交所行情板每个交易日都给出全部
  在交易 BJ 标的的 ST 标志（实测 344/344），行早就落在 `trading_status` 里，
  但门禁只认历史回补 sweep 写的回执，于是这些观测**对它不存在** —— 一个湖可以
  盯着交易所看一年，仍被告知 BJ 状态无法验证。现在每日观测在 compact 之后发布
  自己的回执，来源记为 `bse`，与 baostock / tushare 并列。<br>
  窗口取**从最新交易日往回、连续且完整**的那一段：某一天只答上 343/344 就在那里
  断开，而不是画条线穿过去 —— 那会替一个没人观测过的标的作结论。请求窗口早于
  观测起点时，BJ 仍按"无源可查"处理，深历史与近期两种答案因此不会混淆。
  Tushare 一旦配置就仍由它拥有 BJ（它能回到 2016）。<br>
  结果：不需要 token，湖跑得越久可背书的窗口越长；新装的湖从它自己第一次运行
  当天起算。

- **门禁改为回答「这个湖能背书到哪」，而不只是「全历史行不行」。** 后者对任何
  保留 BJ 又没买数据源的湖永远是否，于是新装的湖第一天就看到一盏永远红的灯，
  却不知道自己**现在**能做什么研究。`cne audit --full` 现在多打一行可背书窗口：
  各证据源能覆盖当前股票池的最新区间取交集，研究窗口落在里面就是被背书的。<br>
  交集为空时点名是谁挡着，并给出最接近的回执差多少只标的 —— 本机实测输出
  「baostock 尚无覆盖当前股票池的回执，最接近的回执差 10/5557 只」，那 10 只是
  sweep 之后新上市的，每日有界扫描会自己补上。这比「无窗口」多出的是可行动性。

### 变更

- **`cne status --datasets` 新增退出码 2**：表示「证明不了」（证据读不出来、
  `instruments` 缺失），与 1 的「已证明不全」分开。判 `!= 0` 的脚本不受影响；
  判 `== 1` 的会漏掉这一档。

- **`cne init --profile demo|sample` 不再静默覆盖 `--config-out`。** 已存在且内容不同时
  拒绝并保留原文件，需要 `--force`；内容相同可直接重跑。

- **`corporate_actions` 的进度按整轮报告，不再每 100 只重来一次。** 之前 reporter 建在
  每个分块里，ETA 的时间窗反复归零而计数持续上涨，算出来的值没有意义。分块 id 改为按
  符号集内容派生，跨尝试稳定。

### 文档

- **免费源对北交所 ST 历史到底能答什么，实测而非推测。** 同花顺 F10 对 BJ 代码
  没有「曾用名」字段；东财 F10 确实覆盖 BJ，返回 `FORMERNAME` 却不带任何日期；
  巨潮的公司概况里根本没有简称变更记录。连同湖里 2026-08 那次交易所公告采集
  —— 580 只全查、565 只零命中，而零命中的票不能被判为 normal —— 免费这条路就此
  封闭：这些源发布的是事件，而 ST 日历要的是状态。结论写进
  `docs/datasets/sources.md`，下一个人不必再花一天重新发现一遍。

### 修复

- **`cne init` 会在两个阶段之间把还在跑的自己判成孤儿。** 孤儿检测只看「还没结束」
  的批次来判断 run 是否活着，于是一个干净收尾的阶段等于没有任何存活证据 ——
  活跃时间退回 run 自己的起始时刻。`instruments` 跑 201 秒后，下一阶段开头用
  60 秒的宽限期去量一个 201 秒前的时间戳，父 run 当场被写成 `failed`，而进程还在
  正常执行 `corporate_actions`。批次**结束**和批次开始一样是主人还活着的证据，
  现在两者都算；`cne init` 另外全程持有自己的 run 锁，让检测器先看到锁。

- **Ctrl-C 之后取数还在继续，账本却说已经停了。** 线程池和进程池都把全部批次
  一次性提交，而退出时的 `shutdown(wait=True)` 没有 `cancel_futures`，于是整条
  队列会被排干：终端已经打印 `Aborted!`，`daily_bars` 的成功批次仍从 8 涨到 40，
  期间可能与新命令并发写同一数据集。现在中断会取消排队中的批次，只等已经进入
  源调用的那几条车道。

- **中断后立刻重跑无法接着上次的位置。** 三处叠在一起：刚中断的批次一小时内不进入
  可重试状态；只要某阶段有任何批次记录，整个阶段就不再执行；而 `cne run retry`
  从 `run_job` 的提前 return 出去，根本走不到关闭中断 run 的那段代码。现在中断会
  立即把这个 run 的活跃批次收尾（不看心跳年龄——操作员按下 Ctrl-C 本身就是证据），
  `cne backfill` 和 `cne run retry` 两条路径也一并纳入，不再需要等那一小时。

- **只跑完两个分块的数据集会被当成完整数据发布。** 闸门数的是「已经建了行」的批次，
  而未启动的批次根本没有行 —— 52 个分块只跑了 2 个，账本读出来是「没有未完成批次」，
  于是 441 条公司行为被 compact 发布、随后标成 `fresh`。现在 worker 池在第一个
  worker 启动前先把整个计划写成 `queued` 批次，`compact_allowed`、`step_succeeded`、
  `incomplete_batch_count` 这些既有闸门因此自动从「点名」变成「查覆盖」。

- **周末运行会期待一根不存在的周六 K 线。** 窗口截止日只按时钟推算，周日跑就落在
  周六。取数本身不丢数据，但完整性闸门把截止日当作一个必然有行的交易日
  （`_staged_daily_bar_symbols` 按 `trade_date == end` 精确过滤），于是全市场都判为
  未解决 —— 单标的回填明明取到了截至周五的 726 根 K 线，仍以
  `1 expected key(s) remain unknown` 退出 1。现在统一归一到最近已完成交易日，
  `daily_bars` 的三个读者（主回填、pre-2016 历史、退市补录）共用同一个含义。

- **`cne run retry` 会推进一个 `cne init` 拒绝推进的 init。** 后者自学会续跑起就不
  跨越未解决的阶段边界，前者走另一扇门、只看依赖图 —— 而没有任何声明说
  `index_bars` 依赖 `corporate_actions`。同一个 run 两套策略，现在合并成一条排序
  规则；判定 run 是否成功时仍然看完整的剩余步骤。

- **`cne status` 会给严重不完整的湖开绿灯。** 新鲜度只有日期一个维度：一次单标的
  修复和一次全市场 sweep 同样能把水位推上去。`--datasets` 现在额外证明最新交易日的
  标的截面 —— 有日线、有明确停牌证据、或已记入待补账本的才算覆盖 —— 并报告尚未跑完
  的 init。已被取数侧容忍并记账的 key 不会在这里被二次判失败：那是同一套系统的两半
  互相打架。「证明不了」（证据读不出来）退出 2，与「已证明不全」（1）分开。

- **`sample` 湖的健康输出不再冒认所有 error。** profile 豁免只覆盖合成源相关的
  检查，因此提示改成只为真正被降级的那些说话；仍有 error 时明说它们不在预期范围内。

- **`--groups` 的承诺不再被新门禁绕过。** 截面检查归 `daily_bars` 所属调度组管；
  未跑完的 init 不属于任何组，始终判失败，这一点写进了帮助文本和文档。

- **离线网络守卫把 forkserver 的 IPC 当成了出网。** Linux 3.14 默认用
  `forkserver` 拉起 `ProcessPoolExecutor`，父进程会连 `/tmp/pymp-*/sock-*`
  这种本机 Unix socket。守卫原先只放行 TCP loopback，于是六个跨进程测试在
  CI 为补上「可安装却没跑过」缺口而加入的解释器上红了。Unix socket 不出机器，
  现在和 loopback 一样放行。
- **Windows CI 无法 stub `launchctl`。** 过期补偿时间的测试把 `CNE_LAUNCHCTL`
  指到 `/usr/bin/true`，原生 Windows Python 执行不了。stub 改成 `.py` 空操作，
  由安装器用当前解释器跑。
- **`block_trades` 的交易所备份现在真能落盘。** 它带着
  `symbol / trade_date / price / volume / amount`，没有 `premium_ratio`，
  写盘时的 schema 检查整批拒绝。这条路由只在东财服务不了该会话时才会走到，
  于是它被请来补的第一天就失败了：2026-09-18 抓得到，一行都没落下。该列现在
  以 null 携带——两家交易所都不发溢价，若用本湖自己的收盘去推，就会把两个源
  写进同一行。
- **退市股的 Baostock 定向修复在默认配置下必然失败。** `query_dividend_data`
  在 socket 层就把响应解码成 `str`，结果对象上没有任何原始字节；而原始归档默认
  是开的，于是适配器在第一个有数据的年份就抛
  `response has no exact wire bytes`，一行都写不进去 —— 审计打印的正是这条修复
  命令。字节只在 socket 上存在过，现在就在那里录：请求前清空、收到多少记多少，
  归档拿到的是真的收到的那串字节，不是拼出来的。实测三只退市股补回 51 行
  分红送转（000022.SZ / 000043.SZ / 000760.SZ），落了 58 份原始载荷。

- **基金份额折算终于有了来源。** `unit_split`/`split_factor` 在 schema 和文档里
  写了很久，却没有任何适配器产出过一行：TDX `xdxr` 适配器只留 category 1，把
  category 11「扩缩股」连同它的 `suogu` 比例一起丢掉了。于是 159327.SZ 在
  2026-07-20 的 1 拆 3 无处安放 —— 当天原始收益 -69%、复权后 -6%，审计只能一直
  报「无除权记录的除权事件」。现在 category 11 落成 `unit_split`
  （`split_factor=suogu`），实测取到 3.0，与新浪因子序列同日从 1.0 跳到 3.0 完全
  一致。category 12「非流通股缩股」不动交易价，仍然排除；比例缺失或等于 1 的
  记录一律跳过，不替源头编比例。

- **东财 2015 年以前的除权行终于够得着了。** 回补路径的主源是 TDX 逐标的 xdxr，
  东财只当对端快照；而东财自己的报表其实回到 1991 年，只是回补下限
  `2015-09-29` 把更早的挡在外面 —— 日更那条等值过滤 `EX_DIVIDEND_DATE='D'`
  一直取得到。新增 `cne backfill corporate_actions --eastmoney-date-repair
  --ex-dates D1,D2,...`：按指定除权日逐日取，每个日期一个 capture scope
  （`begin_capture` 会重置所给的 scope，共用会让最后一天的回执替所有天背书）、
  一个 batch、一份 provenance；不带 `--symbols` 时不跑 TDX 全市场扫描。实测补回
  2001-2006 的 20 条现金分红，仲裁里的 `missing_recorded_action` 87 → 67。

- **仲裁的 8 个计数现在带下一步。** `adj_factor_arbitration` 只报「6,697 条矛盾、
  其中 1,294 条指向已记录的除权」，没有 `remediation` 字段，连配了对端 key 的湖
  也不知道接下来该跑什么。唯一背后真有路可走的是
  `missing_recorded_action`，而且只限除权日早于东财回补下限的那部分 —— 现在按
  这个下限切开，把可执行命令连同日期一并写进 finding 的 message（finding 只打印
  message，写在别的字段里等于没写）。措辞不打包票：报表逐日回答，也并非每个更早的
  事件都有，取不到的日期仍然留着。

- **基金份额折算不再靠人记得去补。** 折算只有 TDX `xdxr` category 11 这一条路，
  而它只在回补路径上；日更用的东财分红报表压根没有折算这个概念。于是折算发生后，
  除非有人想起来对那只票跑一次回补，记录就一直缺着。现在日更的
  `corporate_actions` 步骤会先问一句：最近 10 个交易日里，**有没有因子跳了、
  当天却没有除权记录的标的**——有就对这几只跑 xdxr，只写那几天的行。尽力而为：
  日更的抓取此时已经成功，对端不可达只记一条 warning，不会把好好的一天判失败；
  补不上的缺口十个交易日后自己滚出窗口，不会每天重问。
- **审计改用因子比值判除权，不再只看价格偏离。** `missing_corporate_action` 要求
  原始与复权收益偏离超过 11% 才出声 —— 对分红是合适的门槛，对份额折算是错的：
  1 拆 1.1 只让参考价动 9%，永远不会触发。新增 `unrecorded_ex_event`，直接读因子
  比值，窗口同样限制在最近 10 个交易日（全历史会把一千多条深历史跳变翻出来）。
  实测在本湖 2026-06-22 抓到 `000793.SZ`（×1.0478）和 `002762.SZ`（×1.0057）——
  两条都在 11% 之下，此前只有配了付费对端 key 的湖才看得见。价格检查已报过的
  日期会被排除，同一天不出两条。

- **合规矩阵开始为「只靠修复命令进来的源」说话。** `policies_for_dataset` 只读
  primary / backup / backfill 三个路由字段，于是 `cne ths-official resource-sectors`
  写进 `sector_bars` 的同花顺官方行没有任何条款覆盖 —— `unrouted_source` 报的正是
  这个盲区。把它声明成路由会是假话（没有任何调度会跑那条命令），所以新增
  `DatasetSpec.repair_sources`：矩阵认它（`DatasetPolicy.repair`，并计入 `.all`），
  韧性报告不认它，不会把一条手动命令当成可用的备源。顺带把
  `supplementary_sources` 也接进 `DatasetPolicy.supplementary` —— 那八条恢复链路
  此前同样不在矩阵里。

- **调度任务只有 256 个文件描述符。** launchd 给 agent 的是系统默认值，而
  compact 一个上千个日分区的数据集会把它用光 —— `trading_status` 在 2026-09-18
  两次因 `Too many open files (os error 24)` 失败。交互式 shell 里看不到这个问题
  （那边是 1,048,576），所以它只在调度上炸。三个 plist 模板都加上
  `SoftResourceLimits.NumberOfFiles = 8192`；重新生成用
  `python scripts/scheduler_config.py`。
- **`events-news` 这个 agent 现在也归仓库管。** 它是手写安装的，于是成了唯一没有
  描述符限额的那个 —— 偏偏就是它的 compact 用光了 256 个。补上模板并接进
  `scheduler_config.py`：已安装副本自己的间隔（900 秒）和 `CNE_CONFIG` 仍然保留，
  重装调度不会再把它漏掉。

- **`bse_code_migration` 跑完了也记成失败。** `_publish` 开了 run 却从不 finish，
  于是每次 apply 都挂在那里，等对账扫到再判 `failed: worker exited without
  finish_run` —— 2026-09-18 连着三条都是这样，迁移其实都跑完并发布了修订版，
  面板却分不出这和真崩溃的区别。现在成功记成功、异常按异常记，然后抛出。

- **改名血缘每天被日更抹掉。** `prev_symbol` 不是任何数据源提供的字段 —— 它由
  BJ 代码迁移按湖自己的老码表写入。而 instruments 每次都从实时源重建，compact
  又按 `symbol` 去重保留最新一行，于是新行那个 null 赢了：2026-09-18 的已发布
  修订版实测 248 → 2 → 248，最后那次是人工重跑了一遍迁移。也就是说，幸存者偏差
  相关查询依赖的改名对应关系，只在有人手动补的那天是真的。现在写盘前把湖已知
  的非空 `prev_symbol` 带上，且只补 null —— 源端真报了改名，以源端为准。
- **`bse_code_migration --apply` 每跑一次就空转发一版。** 只要文件里存在当前 BJ
  代码就无条件重写 parquet，分区 mtime 一变，`_publish` 就当成有活干，于是接连
  发出 revision 40、41，两次都没有任何行变化。内容没变就不再写盘。
- **`[universe].default` 是死配置。** loader 一直把它解析成 `Config.universe_default`，
  而整个 query 包没有任何消费者：`load()` 的 universe 来自调用参数和 profile。
  模板里删掉这个键，配置里仍然写着的会在加载时收到一条 warning 而不是静默失效，
  老配置照常能加载。

- **连着坏五个小时，湖里没有一处说得出来。** 2026-09-19 本地代理 00:51 挂掉，
  每 15 分钟一次的 news wire 连着 20 次回 `degraded` —— 每次都丢掉两个东财 step
  （`[Errno 61] Connection refused`），每次都记下自己那条 step finding 然后翻篇。
  五小时里审计一直是 HEALTHY，`stale_datasets` 是空的（涉及的数据集本来就没有
  日水位），最后是人翻日志发现的。新增 `job_run_streak`：同一个 job **末尾**连着
  3 次以上 `degraded`/`failed` 就报一条 warning，带上连败次数、起始时间和最后
  一条错误。只看末尾那一段 —— 断网前面当然是好的，要求「整个窗口无成功」就会
  在该出声的五小时里一直沉默（用真实 manifest 回放验证过：断网 1 小时报 16 连败，
  5 小时报 26 连败，恢复后自动消失）。

- **新增 `daily_bars_implied_price`：一行数据自己和自己对不上。** 已有的
  `daily_bars_volume_unit` 取的是每个源的中位数 —— 对「整个源换了单位」是对的
  形状，对「某几行错了」就不是：中位数稳在 1.0，坏行永远浮不上来。新检查按行问
  同一个问题：`amount / volume` 必须落在当天自己的 `low..high` 里（留 1% 容差），
  不需要第二个源。实测 2026-08-19..09-18：**522 个 ETF/LOF 日全部来自
  `tdx_protocol`**，另有 **8 个股票日全是北交所**，跨 bse / tdx / ths 三个源。
  典型如 160806.SZ 2026-09-11：`amount` 2,825.6 和分钟流分毫不差，区间
  2.016..2.032，而 `volume` 是 154,400，每股均价算出来 0.0183。原先那条跨源检查
  倒是看见了这一天，但把它读成「分钟数据有问题」—— 分钟侧每根 bar 的
  `amount/volume` 正好等于 close，错的是日线的 volume。按 asset_type 分开成条，
  几百个基金日不会把 8 个股票日埋掉。

- **资产负债恒等式按年代分层。** 245 条破坏里 **181 条是 2005 年及以前**，
  2021 年以后只有 9 条。一份 2003 年的年报不平是已公布的既成记录，不会被重述，
  和 2024 年的一条同级别报出来，只会把真正能动手的那几条埋掉。现在 2011 年起的
  报告期留 `warning`（本湖 36 条），更早的单独成一条 `info`，带
  `source_limited: True` 和逐年分布（本湖 209 条）。分界线放在集中度消失的地方，
  不是取个整数。对端差异那条（236/24,985）**保持 warning 不动** —— 对端快照只
  覆盖 2016Q1–2024Q4，全部落在近期，两个厂商在近几年的营收上不一致正是这条检查
  该说的话。

- **沪深的交易所读板改成每个交易日都跑一次（只写快照）。** 交易所读板器早就建好
  且够可信 —— 它就是 `trading_status` 的 failover 路径，实测 2026-09-15 对 5,219
  只标的与东财比对，ST 一致 100.000%、停牌 99.923%，四处不一致全是交易所对。
  但它只在东财失败时才跑，于是正常的一天里湖内**没有任何交易所出身的沪深状态
  读数**，而 ST 证据回执之所以认 `bse`，正是因为交易所板不是聚合商。现在每个
  交易日照常跑一次，写进 `meta/source_snapshots/trading_status/source=exchange`，
  **不碰 curated，权威归属不变** —— 先把可用性记录攒起来，再谈要不要让它发回执。
  2026-09-18 实跑：5,221 行、3.3 秒，与湖内东财行的 ST 标记一致率 **100.0000%**。

### 测试

- **两条按墙钟断言的测试改成按因果断言。** 它们是最近 CI 红灯里复发最多的一类：
  `test_file_lock` 里持锁线程在**离开 with 之后**才 `released.set()`，CI 的共享
  runner 只要在那个缝隙里把它调度出去，排队成功的 acquire 就先返回、断言反而失败；
  现在两边各在同一个 monotonic 时钟上打一个戳，比较先后 —— 排队的 acquire 不可能
  early 于它等的那次释放，没有余量可调。`test_rate_limit` 那条量的是线程**醒来**
  的间隔，而线程被调度晚就会把实测间隔压短（实测 0.0915s 对阈值 0.12s）；现在拆成
  两条：并发上限用计数器断言，间隔改为断言限流器写进状态文件的**预约槽位**——
  那是它自己算出来的精确算术，调度快慢影响不了。
- **新增 `test_toml_path_hygiene`：不许把裸路径写进 TOML。** `tmp_path` 直接插进
  f-string，在 Windows 上就是 `D:\a\_temp\...`，`tomllib` 把反斜杠当转义符
  （`Invalid hex value`）—— 三个平台绿、第四个红，2026-09-19 就这么挂了一次 CI，
  而挂掉的那条测试本身跟路径毫无关系。`path_for_toml` 早就存在，缺的只是强制；
  现在扫描整个测试树，`root/path/dir/file` 这类键的插值必须经过
  `path_for_toml()` 或 `.as_posix()`，故意测试解析失败的那一行用
  `raw-path-on-purpose` 标注豁免。

## [0.10.0] — 2026-09-18

覆盖与溯源。北交所成为一等公民；两处门禁不再因为错误的理由放行；四处每天都在
写坏行的缺陷已修，并配有迁移清理已经留下的脏数据。从干净安装把首次运行走了一遍：
研究 demo、demo 自己的日历、按名回补、空的 daily 计划、小湖上的健康检查，每一处
都曾对用户说过假话。

### 变更 — 破坏性（数据契约）

- **`corporate_actions` 新增 `split_factor`（schema v2）。** 基金拆分/合并既不是
  送股也不是分红，以前没有自己的字段：复权因子会跳一档，湖里却解释不了为什么。
  `cne contract diff contracts/v0.9.0.json contracts/v0.10.0.json` 报告 1 breaking、
  1 compatible——破坏的一半是单位契约新增 `split_factor: unit/unit`。不必重采：
  旧行带中性乘数；枚举单位映射的消费方需接受新键。

### 新增

- **CLI 改说中文。** 命令输出、错误信息和 `--help`——包括 Click 为 `--help` /
  `--version` 生成的文案——原先中英混杂：`cne doctor`、`cne verify` 用中文，
  `status`、`run`、`audit` 用英文，而数据集、文档和看板全程是中文。机器可读
  token 故意不动，因为有东西在解析它们：`HEALTHY` / `UNHEALTHY`，`fresh` /
  `STALE` / `empty` / `no source` 新鲜度词表，`[error]` / `[warning]`，每个
  JSON 字段和取值，以及 run 上报的状态词。流水线自己的 INFO 日志也仍是英文——
  它们是贴着发出它们的代码读的。Click 自己的壳（`Usage:`、`Options:`、
  `Error:`）走 gettext，不动。
- **`cne run daily --all-groups`** 按配置顺序逐个跑完全部调度组，某个组失败
  也继续，退出码取最差的那个。一天的摄入是六个组，以前能一次跑完六个的只有
  `scripts/daily_pipeline.sh`，而 PyPI 包并不安装它——`pip install` 用户文档上
  的答案是六条 cron，他们随手摸到的那条命令只跑核心脊柱，湖里 42 个数据集只
  有 15 个是新鲜的。数据集全部禁用的组（默认 `intraday`、`ticks`）会自己跳过。
- **CLI 现在会读 `CNE_CONFIG`**，不再只有 shell 流水线把它转成 `--config`。
  默认配置路径是相对的，cron 忘了 `cd` 就会报「找不到配置」，而环境里已经写了
  要的那份。显式 `--config` 仍然优先。

- **`block_trades` 和 `dragon_tiger` 有了第二条路由。** 两者原先只有一个厂商、
  没有后备。交易所自己发同样的披露——深交所 `CATALOGID=1265` 和
  `1842_xxpl_after`，上交所 `1902`——failover 放在抓取层，溯源、水位和 compact
  不必知道走了哪条。东财仍是主源，它能答就不问备份。两家交易所都不发北交所，
  记在 `backup_gaps` 里，而不是看起来像覆盖了。`share_unlock_schedule` 故意
  没有备份：深交所登记的是*已经发生*的解禁，而这个数据集是向前的日历。
- **北交所日线历史改走 TDX**（市场 id 2），不再按会话打一条新浪。OHLC 与湖里
  3,086 行完全一致，而且带着新浪在全部 505,518 行上留空的成交额；三个标的、
  十一个会话从 33 次新浪请求变成一次 TDX 批量。尖端仍走北交所快照，它发的是
  精确股数。

- 明确的基金拆分/合并事件（`unit_split`、`split_factor`）以及配套的复权因子
  检查。公司行动用 schema v2；旧事件保留中性单位乘数。下一个开发包是
  `0.10.0.dev0`；已发布的 v0.9.0 契约不变。
- 仅成交的日内重采样、APFS copy-on-write 修订拷贝，以及带查询截止时间、能挺过
  SDK handler 的有界 Baostock 登录/登出。

- **北交所自己的行情**现在提供 BJ 上市、停牌和 ST，与报价路径共用一次分页读取。
  TDX 只服务沪深，所以 BJ 此前只靠重放上一次手工代码空间扫描进目录；2026-09-15
  那天有 16 只正在交易、有真实成交量的名字，湖里一行都没有。北交所以*缺席*
  表示停牌，所以只有走完一圈才读成停牌，绝不从仍显示上一会话的行情板上读，
  而且只对在市名字——241 个退市代码否则会变成永久停牌。
- **上交所、深交所行情板**作为独立的 `trading_status` 备份：停牌是参考收盘旁
  开高低全零，ST 在证券简称里。对 2026-09-15 的 5,219 个标的实测，ST 一致
  100.000%，停牌 99.923%，四处不一致都是东财报停牌而交易所发了完整会话。
- `[universe].ingest` 约束所有隐式抓取范围（`all_a`、`all_a_sh_sz`、
  `all_instruments`）。
- `DatasetSpec.supplementary_sources`，写入行的源必须声明，而不是被报成未路由。
- `cne status --datasets --groups` 只按这台机器真正跑的调度组设门；
  `cne config diff` 对照随包模板报告漂移。
- **证明长任务还活着**（#37）。每个 step 开始时就报名，不只在结束时报；心跳在
  60 秒沉默后点名仍在跑的东西；K 线扫描在第一批落地前就说出范围——标的、窗口、
  批次、lanes。对着这些缺口测过：`instruments` 在一行日志后面跑了 183 秒，
  一个会话的 `backfill daily_bars` 等了 68 秒才有第一声。
- **`daily_bars` failover 链上的第四个厂商。** baostock 从写进 spec 起就声明为
  补充源，但线上从未调用——代码路径只给退市恢复用。于是只剩两个按标的厂商，
  东财历史主机不可达时（实测出口上每个 `push2his` 主机都断开，而 `push2` 仍在
  提供 clist）只剩一个，永远无法认证无数据键：认证需要两次独立的空。全市场会话
  里那些当天根本没成交的名字，会以「expected key(s) remain unknown」失败。
  baostock 走自己的故障域，对在市标的精确（失败那天与新浪分毫不差），停牌会话
  报成无行而不是错误。通过 `[sources.baostock]` 选择启用，和同花顺链接一样。
- **`adj_factors` 有了备份**，这是唯一只有一个厂商的核心数据集。新浪按账号而
  不是按端点封禁，所以一次无关期货扫描上的 HTTP 456 也会让湖里所有收益都算
  不出来。Baostock 发同一序列的未复权和后复权，二者之比就是本湖乘数约定下的
  这个因子；尖端缩放到 1.0 时也是新浪的水平约定。在 600519.SH 的 2026-06-26
  除权日实测：隐含台阶 0.976883，对新浪的 0.976880，差在 Baostock 两位小数
  复权收盘的舍入。行现在带着真正来的那个厂商，因为一次运行可以混用两者。
- **新浪期货主机的探针。** `commodity_bars`——国内主连和海外盘——由
  `stock2.finance.sina.com.cn` 提供，此前没有探针覆盖，而仍在声明这个数据集
  的唯一端点是东财历史主机，新浪正是因为不可靠才换掉它的。可达性按主机计：
  这是 `push2` 和 `push2his` 已经教过的同一课。
- **`cne sources substitutes`** 回答探针报告答不上的问题：这个端点挂了——哪个
  还够得着的源能扛它的数据集，是否独立于已经失败的那个。先排独立的，再排最快
  的；某个数据集什么都不剩时非零退出。数据集→源来自注册表，而不是探针表里手工
  维护、已经漂了的 `powers` 列表：照那张表读会让 `commodity_bars` 和
  `corporate_actions` 看起来无路可走，而后者声明的 TDX 备份其实是健康的。探针
  回答另一半——一个源的哪个*端点*可达——所以同一厂商一台主机上、另一台下，
  就报成那样。
- **原先没有进度的扫描现在会滚动报告。** baostock 估值回补、股东/十大股东窗口
  行走、新浪复权因子扇出和 `compact` 边走边报，共用从已经这样做的 TDX xdxr
  扫描抽出的 `sweep_progress`。
- **每次长运行一份日志**，`CNE_LOG_DIR`（默认 `{data.root}/logs`）下的
  `cne-<command>-<timestamp>.log`，启动时打印路径——跑了三小时失败的任务留下
  可读的东西。`CNE_LOG_DIR` 以前只有流水线脚本在读，CLI 不读。下面「变更」里
  的条目把这件事带到每个会花时间的命令。

### 变更

- **README 截图就是 0.10.0 实际打出来的。** `cne-demo.png` 还是旧命令名下的英文
  CLI 抄本，控制台主图仍画着看板已经换成左侧栏的顶部 pill。两张都重渲了；终端
  渲染器用 Menlo 画 ASCII 和框线、用 CJK 字体画中文，按终端格子推进，因为
  Pillow 没有字体回退、Menlo 没有汉字。数据集表截图对照正在跑的控制台核对过，
  没有变。点数过的说法按代码重数：88 项审计检查，「15 个上游源」改成「15 个
  上游端点」，那才是 `cne sources probe` 枚举的东西。

### 修复

- **`cne init --profile demo --research` 从未打出它存在就是为了展示的那份对比。**
  demo 湖有行情和新浪因子，没有 `corporate_actions`，除权日交叉检查左连接该表
  并把缺失条款填成零——空源被读成「哪里都没有除权日」，每一笔真实分红都变成
  无法解释的因子台阶。600519.SH（237 bps）和 000001.SZ（713 bps）因此失败，
  也就是窗口里每个发过股息的标的。没有对照源可仲裁的分歧现在是点名缺失数据集
  的警告，绝不是错误：没有证据不是因子坏了的证据。demo 也能在五个标的里一个
  厂商缺口时活下来，而不是五个全弃，并说出新浪没答的是哪几个。
- **demo 把抓到的交易日历真正发布了。** 日历跑在自己的波次里、没有 `compact`，
  step 记了 2,818 行，`cne status --datasets` 却报 `trading_calendar` 为空，
  `SELECT * FROM trading_calendar` 什么都不返回——demo 告诉第一次用的人它成功
  做成了空。
- **按名点名的标的不再被当成未上市占位符跳过。** 这条规则是全市场扫描的成本
  控制：没有 `list_date`、湖里也没有任何 K 线的代码还没上市，就不进按标的后备。
  套到指名范围上就变成什么都不抓、什么都不写、仍报 success——`cne backfill
  daily_bars --symbols 000001.SZ` 对着有数据的源返回 0 行，demo 还怪 TDX。指名
  范围现在完全不用占位 universe。
- **配置里没有 waves 的 `cne run daily` 是错误，不是成功。** 它报
  `planned_steps: []`、`status: success`、退出 0，同时什么都不抓，这是调度器
  最没法处理的答案。`Unknown group: core` 现在也会列出配置里实际有的组。
- **demo 湖不再按全市场验收。** `cne verify` 报 35 个缺口、`cne audit` 报 32
  个错误，都退出 1，而五标的湖已经做了它承诺的事——新用户第一次健康检查读起来
  像装坏了。`cne init --profile demo|sample` 在配置里打标（`data.profile`），
  两条命令随后只评判湖里实际有的数据集。sample 湖的合成日期不再被报成滞后，
  并附上会把真实 K 线抓进 `source=mock` 行的「修复」。
- **`economic_calendar` 读成 `no source` 而不是 `empty`。** 东财下线了这份报表，
  没有接替换源，永久的空和「有人忘了跑」分不清。`cne serve` 把它单独计数。
- **缺失的数据集会说出谁来建它。** 湖里没有因子时 `load(..., adjust="hfq")`
  只抛一条路径，读者要的是缺的那张表、不是调用方写下的名字时，这是答案的错误
  那一半。

- **整段窗口都在停牌时，扫描不再崩溃。** 认证这些标的把它们放进
  `expected_no_data`，报告再去 `empty_evidence` 里逐个查找——停牌名字没有条目，
  因为认证来自厂商交易状态，不是两个源都返回空。够不着 baostock 的机器上会
  `KeyError`；够得着的只是因为现场查询碰巧填了洞才通过。两种证据现在分开报，
  声称两个源一致的 finding 只列出真有两次空的标的。
- **重放的 `--trade-date` 再次约束回补窗口。** 默认结束日改成了最近*已结算*
  会话，修了盘中 `cne init`，却完全不再看 `trade_date`——`--trade-date
  2025-01-10` 会要到今天为止的一切，没人要这个窗口、也没人能复现。现在取二者
  较小的那个。
- **测试不声明就不能出网。** `-m 'not network'` 只跳过声明了的测试；八个误出
  网——两个问 baostock，四个问东财，两个问舆情端点。七个仍然通过，因为适配器
  回退了，唯一症状是时间：`test_exchange_trading_status.py` 19.2s，相对关 socket
  后的 0.09s，整次运行在 129s 和 169s 之间漂，还超时两次。第八个*因为*查询成功
  才通过，结论有一部分来自现场厂商。autouse fixture 现在拒绝连接——以及 send，
  因为 baostock 在 import 时连一次就不再拨——点名测试和地址。
- **限定范围的修复能到达给定的窗口。** 北交所托底把显式 `--symbols` 窗口截成
  日更回看，所以 `--outstanding` 一直拒绝被要求的 225 个会话。仍在交易的会话
  上的键不再让整月 pass 失败——它继续记欠。生产：403 个缺失标的-日降到 32，
  北交所缺口清零。
- **universe 认识一只证券之前它已经成交的会话是欠账，不是丢失。** 十三只北交所
  证券 07-22..09-04 上市，09-07 才有第一根 K 线，短了水位已经走过的 366 个会话；
  它们现在记到 outstanding 账本上，`--outstanding` 能修。
- **`cne backfill` 每过一趟就划掉键，而不是全部跑完才划。** 在 37 趟的第 30 趟
  被杀掉的运行什么都没结算，已经修好的键仍留在账本上。
- **新闻 dtype 由声明的 schema 决定，不是前一百行。** 某次会话前一百闪讯没点名
  任何证券，把 `related_symbols` 打成 Null，第一百零一行就炸；`news_headlines`
  和 `flash_news_wire` 在 2026-09-12 到 09-16 之间这样失败了 22 次。
- **`block_trades` 给一只证券一行，价格是加权价。** 交易所发的是逐笔成交，降级
  日会写成 29 行，厂商写 17 行——价格含义不同，而主键含价格。
- **`dragon_tiger` 读完整张名单，每个席位只计一次。** 深市名单分页，第一页只给
  当天 30 只里的 7 只；同时上买卖前五的席位被重复计数，两边各自的五名又丢掉
  其余。两边现在与东财在 2026-09-15 全部 37 行上完全一致。上交所缺科创板记在
  `backup_gaps`。

- **子命令上的 `--help` 不再打 traceback。** Click 的 `Exit` 继承 `RuntimeError`，
  记录失败的 catch-all 把每个用户先敲的那条命令记成未处理错误。`SystemExit`
  到不了那里，所以 `status --datasets` 的非零退出一直安静，这条却不。
- **第一次 `cne init` 不再在免费 API 后面干等十小时。** 历史 ST 扫描用湖里已有
  的 ST 信号收窄范围——新湖没有，于是继承全部 5,557 个标的，按 baostock 故意
  放慢的节奏：实测首次 init 花 1.8h 才走到这一步，里面还要再花 10.4h。节奏
  不变，因为超限会封 IP；变的是谁在等。`[orchestrator] st_history_symbols_per_run`
  （默认 400）限制单次运行，checkpoint 从停下的地方续，`cne backfill
  trading_status` 不设上限，因为按名点这个扫描就是要坐到底。暂停的扫描报成
  「尚未扫完」而不是「未解决」，免得去找并不存在的厂商故障。
- **被容忍的缺口能到达 `curated`，不再停在 `staging`。** step 级容忍被下一层
  废掉：引擎把 step 状态写到 batch 上，compact 跳过任何 batch 不是 `success`
  的数据集。实测 `cne init` 暂存了 3,894,608 根日线，带着 0.12% 缺口警告，然后
  一行都不发布。step 现在可以在仍报 warning 的同时结算 batch——选择启用，因为
  对多数 step 来说 warning 确实意味着还欠一次尝试：`trading_status` 只有部分
  ST 证据时继续挡住，否则湖会给从未扫过的 universe 开覆盖回执。
- **日更任务带着很小的尖端缺口继续，而不是丢掉整场会话。** 同一套容忍，旋钮
  更紧：三年回补的 1% 是散点，一场会话的 1% 是任何人交易都看着的最新 K 线缺
  55 只，日更重跑也够便宜，可以更严。`[orchestrator]
  daily_bars_tip_unresolved_tolerance` 默认 0.2%。一场会话什么都没暂存仍然
  拒绝——那是故障，不是残渣。
- **`cne backfill financial_statement_items --symbols` 存在了，限定范围的修复
  按自己的范围验收。** 以前直接拒绝，要修四只退市和一只北交所上市欠的 149 行
  资产负债表，只能做 36 趟全市场期间扫描。限定范围还暴露第二个 bug：全市场完整
  性检查对着五标的结果跑，正确的修复回来却是 `warning` 加
  `missing_statement_periods: 36`，这个 warning 让 compact 跳过数据集，3,120
  行修好的数据全搁浅。修完实测：149 个缺口降到 4。
- **`share_unlock_schedule` 按窗口问，而不是读完整份报告。** 东财曾经拒绝
  `FREE_DATE` 上的范围谓词（code=9501），适配器被改成每次调用都翻 2010..2035
  全部——63 页 × 500——所以回补的每一段都重走一遍，任意一页超时就整次失败。
  2026-09-17 再测，谓词被精确遵守：一年三页，2016 回补 76 秒，以前会超时。全量
  行走仍作为恰好那种拒绝的后备，因为这条上游以前这样断过。
- **`--outstanding` 修的是给它的散点，不是包围盒。** 欠账键不是一个区间：5,037
  个散在 833 个标的、692 个会话上，中位每个标的五个键，用一个窗口罩住它们会
  抓约 624,750 个键去修 5,037。按月分桶，37 趟大约 32,476。
- **`block_trades` 和 `dragon_tiger` 能挺过东财故障。** 两者只读一个厂商、没有
  第二条路，东财糟糕的一天就是湖里缺的一天。各交易所发自己的记录，厂商抛错或
  返回空时 step 现在去读——对 2026-09-15 实测，大宗成交金额与东财在全部 32 只
  沪深名字上中位偏差 0.0000%，000428.SZ 龙虎席位合计对得上东财的舍入。两家
  交易所都不发北交所，上交所龙虎榜从 2017-01-01 起，`DatasetSpec.backup_gaps`
  记下降级会话缺什么，而不是读成完整的一天。failover 在抓取层，每行仍带着自己
  的 `source`，厂商在答就不问备份。
- **`share_unlock_schedule` 故意没有备份。** 深交所解禁公告看起来像覆盖，直到
  问未来一个月：2026-08 有 28 行，九月其余 3 行，十一月 0 行。它登记的是已经
  发生的解禁，而这个数据集是向前的日历——835 行在未来。答错问题的源比没有更糟，
  因为它会在正好需要这份日程的那天读成已覆盖。
- **全市场扫描不再因为舍入误差把自己扔掉。** `daily_bars` 拒绝 checkpoint 缺键
  的快照，对真洞是对的，对瞬时洞是灾难：实测 `cne init` 花 1h43m 抓了 250 万行，
  然后在 5,037 个内部键上拒绝——窗口的 0.12%——于是 `compact` 没跑，`curated`
  仍空，整次运行作废。两道拒绝门现在都带容忍（`[orchestrator]
  daily_bars_unresolved_tolerance`，默认是扫描所要键的 1%，`0.0` 恢复旧的
  失败即关契约）。容忍内带警告继续；超出仍拒绝。
- **被容忍的缺口欠什么，写下来。** checkpoint 越过一个洞会把水位移过那些会话，
  之后没有任何增量运行会再问——不失败的代价本会是静默丢数。键进
  `meta/state/<dataset>.json` 的 `outstanding_keys`，按键合并，以免一夜失败把
  账本胀大；`cne status --datasets` 点名任何带着债的数据集；`cne backfill
  <dataset> --outstanding` 把它销掉，只划实际落地的键。整标的缺口只欠该标的
  上市过的会话——永远还不了的债若和真债同权，会把它淹死。什么都不过期：修了
  仍错过的记一次尝试，于是「昨夜毛刺丢掉的」和「配置的源都不服务这个」不再
  长得一样，而又不由存储替操作者退役键。
- **`cne init` 和 `cne init --profile demo` 能在交易时段跑。** 两者都把窗口解析
  成*今天*，K 线要到 15:05 才成形，然后死在 finality 门上——`init` 在 37 分钟
  参考和公司行动之后，阶段 3、4 从未跑；demo 是 README 第一条命令，却报了并不
  存在的 TDX 连通问题。未指定的结束日现在是最近已结算会话；显式 `--end` 仍然
  大声失败，因为修今天截断的 K 线是真请求，收盘前无法服务。
- **内部缺口拒绝会说出该怎么办。** 以前只报一个计数；现在点名 findings 文件、
  源探针、续跑的 retry 和限定范围的修复，和其他同类门一样。

- **运行日志会被清理。** `attach_log_file` 每次调用都在 `{data.root}/logs/`
  写一份带时间戳的文件，从来没人删——日更流水线每个组每天一份，加上每次
  retry 和 backfill，湖存在多久就积多久。`cne run clean --log-retention-days`
  （默认 30）丢掉过期的，只碰本 CLI 命名的文件。
- **`cne backfill` 拒绝颠倒的日期范围，而不是拿它去扫。**
  `--start 2026-01-02 --end 2026-01-01` 对着空窗口走了 24 秒真实请求，step
  抛错，引擎记下 traceback——命令仍打印 `status: success`，`rows_written: 0`。
  `derive`、`verify --bars` 和 `audit` 早就检查这个。
- **`cne verify --dataset` 拒绝它不认识的名字。** 扫整个注册表时库警告并跳过
  未知数据集是对的，调用者亲手敲的名字则不然：拼错的 `--dataset` 打印
  「覆盖完整：没有可修复的缺口」并退出 0，于是笔误读成湖没问题。现在失败，
  并给出 `cne backfill` 同款的近似建议。
- **`cne sources resilience` 不再忽略找不到的 `--config`。** 报告从注册表算，
  所以这个选项只在 `--with-availability` 时才读——拼错的路径被静默接受，命令
  退出 0。显式传入的 `--config` 现在两种路径都会解析。它的 `--help` 还声称
  一个域带着 29 个数据集；实测是 30。
- **进度心跳可以停，也不再骑在 `time.sleep` 上。** 它的线程在 `time.sleep(5)`
  上循环、没有停止信号，于是在测试进程里活过启动它的测试。后来有测试伪造
  `time.sleep` 来捕获参数——`module.time` 是共享的 `time` 模块，假货进程级
  生效——把那个循环变成全速空转：一次运行往无关断言里塞了 34,735,321 条。
  只在 CPU 争用时才对上，所以是 flaky 而不是稳定失败。线程现在等自己的
  `threading.Event`，`stop_heartbeat()` 结束它，测试 fixture 在测试之间清掉。
  间隔、轮询和日志输出不变。
- **`cne ths-official` 运行记进清单。** 三个会暂存行的命令铸了 run id、对着
  它暂存、告诉你把它传给 `cne run compact`——却从未打开一次运行，于是这个 id
  什么都不指。行到了 curated；缺的是父行。`cne status` 看不见这次运行，
  `cne run clean` 把无法证明已完成的暂存归到 *skipped*，永远不会像无清单孤儿
  那样按龄回收：三次 `ths-*` 运行就这样搁浅了 23MB。
- **`daily_bars` 尖端键失败会说出该怎么办。** 以前只报一个计数——不是哪些键、
  哪家厂商挂了、哪条命令续跑。现在点名 findings 文件、`cne sources probe`、
  精确的 `cne run retry --run-id`，以及只修那些键的 `--symbols`。
- **阻塞的锁等待有上界，并说出在等谁。** `compact` 和每次湖变更都在
  `blocking=True` 后面排队等同伴，那次等待没有截止、没有输出——和挂起无法
  区分，持有者还活着但卡住时就是挂起。崩溃的持有者从来不是问题：进程一死
  内核就放锁。等待现在会自我通报（「waiting for compact.lock — another
  process holds it」），`DEFAULT_LOCK_WAIT_SECONDS` 后放弃（1 小时，宽到合法
  的长 compact 仍能排队），错误点名锁文件、`lsof` 和 `cne status`。同一进程
  里的嵌套获取仍立即失败，而不是对自己超时。
- **崩溃的运行不再谎称还活着。** 存活从心跳年龄推断，于是中途被杀的运行在
  清单里保持 `status=running`，长达 `batch_stale_seconds`——默认一小时——那
  段时间每个读运行状态的命令都是错的：`cne run retry --failed-groups` 对着
  操作者正在看的那次崩溃回答「No failed daily group run to retry」，`cne
  status` 显示幽灵。每次运行现在在生命周期内持有 `meta/locks/{run_id}.lock`，
  进程一死内核立刻释放，于是未加锁的 `running` 行只需熬过一段短宽限期（60s，
  覆盖记录运行和拿锁之间的缝）而不是整个过期超时。更短的配置超时仍然赢——
  宽限期只封尸体能赖多久，从不让人等超过他们要求的。`retry --failed-groups`
  在挑选之前先对账，于是看见的是崩溃而不是幽灵。
- **被杀掉的 `cne init` 可以再启动。** 看起来挂了、被杀、再跑一次会被拒绝
  ——操作者手里只剩这一条命令，却对正想做的事说「不」。对*活着*的同伴拒绝
  是对的，对死的则错，而没有任何东西区分它们：init 不拿锁，于是 SIGKILL 留下
  的 `running` 看起来和正在进行的运行一模一样。现在在持续期间持有
  `init_job`，那把锁空闲时第二次 `cne init` 续上未完成的运行，只有另一个进程
  真的握着它才拒绝。仍然永远不会在一次未完成的全量 init 上再开第二次。
- **单会话回补会说窗口可以放宽。** TDX 每次请求最多返回 800 根 K 线，所以问
  一个会话和问有记录的每一年，扫描成本一样；按天填一个月要付三十次扫描的钱，
  拿到的却是一次就会返回的东西。只对回补说——日更任务的单会话窗口就是它的
  意义。
- **批次进度行不再夸大失败。** TDX 没有行的一个标的被报成
  `(100 symbols FAILED)`——那是批次大小，不是范围——随后扫描又通过 failover
  救回大多数。现在读 `(1/100 symbols failed)`。
- **批次 ETA 不再乱摆。** 用已完成批次的墙钟估计，会把扫描一次性启动成本算
  进去，而前 `lanes` 个批次会在一个批次延迟内一起落地：实测 53 批次的运行
  对着十分钟的工作打印 ~34m、~17m、~11m、~8m。估计现在从第一轮排空的 lanes
  开始计时，有数之前不给数字。
- **`trading_status` 不再断言源从未服务过的事实。** 东财 ST 板只选深沪，停牌
  源也到不了北交所，两者却都被存成 findings：全部 343 个在市 BJ 名字
  `risk_warning=False`，而交易所列着 \*ST康乐/\*ST田野/\*ST同辉；15,515 行
  BJ 每一行 `is_trading=True`，对照沪深 148,933 行里的 116 次停牌。未服务的
  交易所得 null，北交所的行来自北交所。没有简称的退市代码同样是未知而不是
  干净——253 个退市 BJ 代码在退市后每个会话都被发布成未受风险警示。
- **`valuation_metrics.ps_ttm` 读的是东财 `f45`**，那是元金额而不是比率：
  96.8% 的值超过 1000，中位 2.05e7，对照 baostock 的 3.2。字段是 `f130`。
  审计形态检查现在会让任何存着金额的比率列失败，按源计。
- **血缘不能再被默认。** `normalize_with_source` 默认 `"tdx_protocol"`，于是
  58,672 行 `trading_status` 点名一个并不服务状态源的厂商。参数现在强制，
  八个调用点都点名自己的厂商。
- **未上市代码不再挡住市场快照。** 没有 `list_date`、任何地方都没有成交 K
  线的代码还没开始交易，缺尖端是待上市，不是覆盖缺口；两个这样的代码每次
  运行都让 `daily_bars` 失败。
- **新鲜度门禁和调度对齐。** 只跑核心的主机有二十来个数据集没有任何任务去
  抓，于是未限定范围的门禁每天失败——2026-09-12/13/14 有 21 到 25 个过期
  ——三天真正 UNHEALTHY 淹没在噪声里。纯粹的新鲜度未命中也标题成 数据滞后，
  而不是 数据异常。
- 新浪期货扫描在 HTTP 456 上退避，而不是直接抛错让整个 `commodity_bars`
  step 失败。
- 颠倒的 `delist_date` 在合并后的帧上清除，而不只是传入的快照，因为同样的
  代码会再次从在市名单里丢掉。
- SLO 目标来自观测点类别（`cn` / `overseas`），从不来自地点，于是海外主机
  不会被拿它够不到的内地可用性来衡量。

### 变更

- **每条命令都有进程日志，每次失败都留记录。** 流水线的 INFO 记录在命令树
  根上接一次，所以加进去的东西不可能没有它们——以前，慢的 `cne verify` 或
  给数 GB 做哈希的 `snapshot export` 一声不吭，`status` 期间的库警告也无处
  可去。真正花时间的二十三条命令还会 tee 进 `{data.root}/logs/`。失败记在
  能看见每条命令的那一处：用法错误是 `WARNING`，其余是 `ERROR`，未预期异常
  带着 traceback，一次失败恰好一条记录，不论发生在哪一层。`cne mcp` 和
  `cne serve` 保留自己的日志——那里的 stdout 是 JSON-RPC 线和 uvicorn 的
  socket。根拒绝别人已经配好的 root logger，于是 import 这个 CLI 再调一次
  查询不再毁掉宿主应用的日志；组自己的选项（`cne --bogus-flag`，在任何命令
  分发之前就失败）也会被记下。
- **命令名大小写不敏感，`-h` 能用。** `cne STATUS` 以前是死胡同：Click 的
  建议按编辑距离跑，全大写离自己的小写太远，不会被提出来，于是错误里没有
  下一步——而 `cne Status` 会得到一条。一个 `token_normalize_func` 让命令、
  子命令和选项对齐；`cne config CREATE` 在命令体里正规化，因为自由形式参数
  会绕过那个钩子。前缀匹配仍关着，所以 `cne stat` 被拒绝而不是被猜。
- **已发布文档里过期的计数，以及一条已被取代的契约说法。** steps 模块页写
  着 40 个已注册 step，实际 47；`cne sources resilience` 的示例输出和旁边
  的散文仍是旧爆炸半径（eastmoney 29/4、tdx 9/6、exchange 2/1，实测 30/4、
  8/5、4/3）；契约页把 `pit_quality` 对非 PIT 表回退成字面量 `strict` 写成
  未来的破坏性变更——它已经发货，今天 29 个数据集带着它。现在有测试对照
  注册表核对这些计数。
- **文档里每一个 `bash` 块都是合法 shell。** 占位符写成 `<run_id>`、`<id>`、
  `<coverage_start>`，bash 会当成重定向，于是七个块过不了 `bash -n`，没法
  复制粘贴。现在用 Click 自己的大写约定（`RUN_ID`、`COVERAGE_START`）。
- **已发布文档覆盖代码实际有的东西。** `cne ths-official` 在 CLI 参考里根本
  没有章节；六个配置节（`[quality]`、`[incremental]`、`[raw_archive]`、
  `[exchange_audit]`、`[margin_trading]`、`[trade_ticks]`）没有文档；源列表
  写 7 个源，模板随包 14 个，还把已经成为 `margin_trading` 主源的
  `exchange` 写成仅审计；模块树漏了 `provenance`、`diagnostics`、
  `compliance` 和 `mcp_server`；`stale_pipeline.sh`——第三个带自己 launchd
  模板的调度任务——运维脚本页上完全没有。
- **顶层命令表面 19 条、分了组**（以前 25 条，扁平按字母排）。`cne --help`
  现在按湖的用法分组——搭建、运行、检查、消费、治理——而不是把 `audit`、
  `verify`、`verify-bars`、`stability` 和 `status` 并排列着，好像在它们之间
  选一个是显而易见的。敲一个搬走的名字会打印它去了哪里。
  - `cne verify-bars` → `cne verify --bars`；`cne stability` → `cne verify
    --runs`。三条都在问「该落地的落地了吗？」，三种粒度。选项按名字在各
    模式间拒绝，而不是被忽略。
  - `cne retry` / `cne compact` / `cne clean` → `cne run retry` / `run
    compact` / `run clean`。每个调度组已经以 `compact` step 收尾，所以这些
    是失败后的手工回头路，不是健康一天的一部分。
  - `cne demo` → `cne init --profile demo`（以及 `--sample` → `--profile
    sample`）。建多大一片市场是一条轴：demo、sample、quick、full。
  - `cne config init` → `cne config create`，和 `cne init` 只差一个词，而
    后者建一座湖，误跑代价大得多。
  - `cne ths-official snapshot` → `cne ths-official capture`，不再和无关的
    `cne snapshot` 撞名。

- 每日审计检查活跃分区；整湖扫描改到每周一次（`CNE_FULL_AUDIT_DOW`）。它
  读每一份历史 Parquet——25 GB 湖上墙钟 1h39m、CPU 7m——以前每个交易日都
  在跑。
- Step 持久化自己的 findings，失败的运行能自己解释。

### 移除

- 两份字节完全相同的图片副本：`docs/assets/architecture-overview.png`
  （与 `architecture-diagram-v2.png` 同字节）和 `og-image.png`（与
  `og-image-brand.png` 同字节），外加 `cne-serve-dataset.png`——没有东西嵌
  入它，上面印着 `cne retry --run-id`，这条命令已经不存在。
- `cne servers test`，已弃用，改用 `cne sources probe --only tdx_protocol`，
  原计划在 0.9.0 移除。`cne snapshot delta-create` 和 `cne status --run-id`，
  都是已有拼写的别名。

### 迁移

每条在编辑前先隔离，再通过修订存储重新发布，于是 curated 和已提交的读者
事后不会对不上。全部默认 dry-run；需要 `--apply`。

- `migrate_null_bad_eastmoney_ps_ttm.py` — 上面那些 `f45` 值
- `migrate_relabel_trading_status_provenance.py` — 58,672 行点名 TDX
- `migrate_drop_unsupported_suspensions.py` — 1,188 条停牌，依据只有
  「没有源回答」
- `migrate_drop_nav_series_bars.py` — 当成日线存的基金净值序列

## [Unreleased]

### Fixed

- **Bootstrap `cne init` deadlock on first-build lakes with dead funds.**
  The multi-source no-data certification inside `_gapfill_multiday_via_kline`
  required a symbol to have *no staged rows at all* and *every session
  missing*. A delisted/liquidated ETF or LOF that TDX and EastMoney cannot
  serve fails both: its tip fetch still stages one zero-volume pre-open
  placeholder, so the symbol kept its missing keys forever, the
  `daily_bars_interior_gap` gate refused to checkpoint, and `--resume`
  repeated the deadlock on every pass — the negative-evidence write point is
  unreachable behind that raise. Three changes, preserving the
  two-independent-sources-agreement principle:

  - A zero-volume placeholder no longer disqualifies certification: the
    rule now requires *no positive-volume staged row in the window*
    (placeholders are not trade evidence — the same semantics
    `load_bar_universe` already documents).
  - Partially-staged symbols (source retention starts mid-window) get their
    missing keys certified per contiguous segment: EastMoney kline and Sina
    are probed over each segment; rows either returns are staged first, and
    only a double-empty segment is certified (`daily_bars_segment_no_data`
    finding, `expected_no_data_keys` on the gapfill result, bounded
    negative evidence persisted per segment).
  - `_staged_daily_bar_missing_keys` now skips keys covered by live
    negative evidence — symmetric with its existing trading-status
    exclusion — so a run that has already certified a key passes the gate
    on the same invocation instead of raising first and recording later.

## [0.9.0] — 2026-09-13

一次破坏性数据契约变更，以及审计带来的存储/质量工作。
`cne contract diff contracts/v0.8.1.json contracts/v0.9.0.json`
报告 29 项破坏、0 项兼容差异，全部是同一个字段。

### 变更 — 破坏性（数据契约）

- **`pit_quality` 新增 `not_applicable`，29 个数据集改用它。**
  对任何不做时点声明的数据集，该值曾回退成字面量 `strict`，于是 42 个已注册
  数据集里有 29 个发布了 `pit_quality: "strict"`——包括 `daily_bars`、
  `trade_ticks` 和 `trading_calendar`——真正够格的只有 `announcement_index`。
  契约本身是完整的（同时发布 `pit: false` 和 `pit_grade: "none"`），但只读
  `pit_quality` 的消费方会把从未被考虑过的表当成最强声明。

  **迁移。** 不改数据、不必重采：只是元数据取值。把
  `pit_quality == "strict"` 当成「可安全做时点研究」的消费方，应改读 `pit`
  （布尔）或 `pit_grade`，这两项本来就是对的。枚举词表的消费方必须接受第四个
  值。`announcement_index` 不变。

  理由和被否决的替代方案见 [ADR-0011](docs/adr/0011-bitemporal-columns-are-carried-not-required.md)
  的命名债务一节。


### 新增

- **`cne run clean --keep-revision-generations N`（默认 5），以及
  [ADR-0010](docs/adr/0010-bounded-generation-retention.md)。** 每次提交都把
  整个数据集拷进新的不可变 generation，从未删过一个：307 个 generation、
  16 GB，对照 14 GB 精选数据，其中仅 `adj_factors` 就占 46 个 generation、
  9.5 GB。保留策略丢掉旧 generation 的字节，但留下每一张收据——收据是血缘
  记录，只要几 KB——且永不碰 `current.json` 解析到的那一代。在实测湖上从
  16 GB 收回 11.2 GB。ADR 记录了为何实现过对未改文件做硬链接、又为何撤回。

- **`[quality].audit_gate` 与流水线升级，以及
  [ADR-0012](docs/adr/0012-the-audit-gates-in-shadow-first.md)。** 每次加载后
  跑 84 项检查，却改不了一次运行上报什么：`step_audit` 无条件记 `success`。
  现在遇到 `error` 发现可以记 `failed`，默认仍是 `shadow`——运行照样成功，但
  每个受影响的运行把严重度分解追加到 `meta/quality/audit_gate.jsonl`，门禁
  可以按实测证据再武装。另外，`CNE_SOFT_FAIL_MAX_DAYS`（默认 3）会升级连续
  多日失败的软组：`CNE_SOFT_FAIL_OK=1` 曾让每次软失败都退出 0，于是一组挂了
  三天没人发现（`docs/operations/runbook.md`）。坏一天仍只告警；连坏三天退出
  1。

- **`scripts/sync_schema_docs.py`，由 CI 把门。** `docs/datasets/schema.md`
  手工维护已经漂了：42 个已注册数据集里有 11 个完全没有章节（`top_holders`、
  `share_structure`、`delisting_events`、`news_headlines` ……），而
  `domain/schemas.py` 里加的列也没有东西把它绑到那一页。脚本从注册表同步列名、
  顺序和类型，并按列名带走手写的 `说明` 单元格——包括代表一组的行
  （`open / high / low / close`），共享注释现在会落到每一列，而不是被丢掉。
  散文段落永不改动；两边不一致时 `--check` 让构建失败。

- **`PIT 双时态扩展列` 自成一节。** 它原先挂在 `#### instruments` 下面，而那
  不是 PIT 数据集，读起来像错表的属性。

### 修复

- **`pit_quality` 文档写成占位符，而不是声明。** 对任何不做时点声明的数据集，
  它回退成字面量 `strict`，于是 42 个数据集里有 29 个发布 `strict`——包括
  `daily_bars`——真正够格的只有 `announcement_index`。契约是完整的（同时发布
  `pit: false` 和 `pit_grade: "none"`），但只读 `pit_quality` 会把意思反过来。
  `docs/datasets/contract.md` 现在点名要读的字段是 `pit`。给 29 个数据集改名
  是破坏性的，留给有版本的发版；见
  [ADR-0011](docs/adr/0011-bitemporal-columns-are-carried-not-required.md)
  的命名债务一节。

- **双时态 PIT 列现在能落到磁盘。** `available_at`、`source_published_at`、
  `observed_at` 和 `revision_id` 在契约里声明、读入时正规化，但
  `validate_dataframe` 投影成恰好已注册的列，于是写盘路上丢掉全部四列——
  没有任何 PIT 数据集能存下它们，`reader.py` 只好拆出去、校验后再 `hstack`
  回来。校验现在在帧里已有这些列时把它们带过去（已注册 schema 不变；它们仍
  可选），compaction 物化它们，早于它们的文件会重写一次。读一个
  `financial_statement_items` 分区从 0.55 s 降到 0.003 s，整个数据集从 ~19 s
  降到 ~0.1 s，因为 `revision_id` 不再每次读取逐行重哈希。

  两件值得知道的后果。`observed_at` 和 `fetched_at` 一样排除在业务摘要之外
  ——同一事实的双时态名字，算进去会在每次对账时铸出一次修订。而且逐行存
  digest 不便宜，所以 `revision_id` 现在是截断的 96 位（24 个十六进制字符）
  SHA-256，而不是完整 64：当前 12.2M PIT 行上任意碰撞概率约 ~1e-15，完整
  宽度会给 190 MB 精选 parquet 再加 389 MB，而不是 73 MB——其中每一字节还会
  拷进每个已提交的 generation。旧宽度从未持久化，不必迁移。

### 移除

- **`ROADMAP.md` 和 `CONTRIBUTING.md`。** 它们描述的产品边界已经在
  [comparison](docs/comparison.md) 里，开发约定——包布局、提交约定、CI 门禁
  及其本地等价——现在住在
  [development/conventions](docs/development/conventions.md)，和其余开发者
  文档在一起。

### 变更

- **`CODE_OF_CONDUCT.md` 是三条规则加一句话。** 个人项目不需要它雇不起的举报
  机制。

## [0.8.1] — 2026-09-13

没有数据集契约变更：相对 0.8.0，`cne contract diff` 报告零破坏、零兼容差异。
没有 HiThink 密钥的湖不受「新增」一节任何内容影响——源保持惰性，对等检查保持
沉默，见 [ADR-0008](docs/adr/0008-optional-keyed-sources.md)。

### 新增

- **湖里已有的数据集多了一个可选的许可对等源。** 日线、财务报表、估值、板块
  K 线和公司行动可以从许可端点取数，用来仲裁分歧——有争议的值只有在第三源
  背书时才会改。包括 2016–2024 的资产负债表和现金流回补、ETF 代码路由到基金
  端点，以及估值快照按日累积，因为上游不保留历史。
- **`cne ths-official`——该源的命令组。** `backfill`、`repair-bars`、
  `resource-sectors` 和 `snapshot`，未配置密钥时各报 `skipped` 而不是失败。
- **审计以前做不到的三项检查。** 资产负债表必须平衡；估值列如果存的是金额而
  不是比率会被抓住；合规登记对照 `curated` 实际持有的东西，而不是对照它自己
  的路由表。最后一项找到两处真实缺口：`bse` 写入 `daily_bars` 和
  `trading_status` 却没有策略条目，六个已注册源写入并不路由给它们的数据集。
- **北交所已注册。** 它一直在写行，却不在 `sources/SOURCES.yml` 里，条款根本
  查不到。每个权限字段仍是 `unknown`——从这个出口访问站点返回 403。
- **ADR-0008（可选带密钥源）和 ADR-0009（股本稀释是一件事实）。**

### 变更

- **中文 README 现在介绍 serve 控制台。** 新增的 数据运维页面一节说明
  `cne serve` 展示什么，并嵌入带标注的看板插图。
- **社区文件遵循开源惯例。** `NOTICE` 只承载归属，不再第二份拷贝许可正文；
  `CODE_OF_CONDUCT.md` 改成中文以匹配项目其余第一语言文档，并补上范围、私下
  联系方式和所述后果；`SECURITY.md` 用表格声明支持的版本。`CONTRIBUTING.md`
  搬到 `.github/`，GitHub 仍会展示它，并记录提交约定和 CI 门禁及其本地等价。
- **`ROADMAP.md` 不再跟踪已发货的工作。** 它停在「Now · 0.6」和「Next · 0.7」，
  而项目已经发到 0.8。

### 修复

- **文档里每一条链接都 404。** GitHub Pages 区分大小写，13 条链接——包括
  `pyproject` 在 PyPI 上展示的 `Documentation` URL，以及 mkdocs 自己的
  `site_url`——指向 `rootsunc.github.io/cnequity` 而不是 `/CNEquity`。
- **Windows CI 解析不了测试配置。** 四个 `ths_official` 配置测试把原始
  `Path` 写进 TOML 字符串，于是 runner 的 `D:\a\…` 变成非法转义。这是 0.8.0
  最后一次提交上唯一失败的任务。
- **开放式基金被当成场内交易。** `51` 前缀把全部 188 个 `519xxx` 代码扫进可
  交易宇宙，TDX 答回净值序列：`daily_bars` 里 436,533 行，每行都有收盘、成交量
  和成交额全是零。
- **从未交易过的证券会被推断成退市。** 没有 K 线历史，看起来和历史停了一样；
  没有证据不再当作证据。
- **`.git-blame-ignore-revs` 什么都匹配不到。** 八月的历史改写给两次格式化
  提交换了新 id，GitHub 静默忽略该文件。
- **测试读了被 gitignore 的配置。**
  `test_the_shipped_configs_run_it_before_audit` 点名 `configs/cnequity.toml`
  ——用户自己的配置——所以开发者机器上通过，每次干净检出都失败。现在覆盖真正
  随包的两份模板。

## [0.8.0] — 2026-09-06

### 新增

- **`cne run events`——给 7x24 发布的源准备的任务。** 披露和新闻不因周末停下，
  但 `announcement_index`、`regulatory_events`、`news_headlines` 和
  `flash_news_wire` 却排在 `daily` 组里：交易日门禁在它们仍在发布的那些天跳过
  整次任务，而每个 `daily*` 组共享一把非阻塞的 `daily_ingestion` 锁，于是事件
  扫描只能挤在晚间重负载组留下的缝里。它们现在住在 `[job.events.groups]`，按
  自然日历跑，并拿自己的 `events_ingestion` 锁，两个任务既不等待也不跳过对方。
  `validate_config` 把这道分界守住：事件组只能调度日历范围的数据集，同一步骤
  不得同时排进两个任务。随包调度加上 `scripts/events_pipeline.sh` 和
  `com.cnequity.events` launchd agent（每个日历日，无工作日过滤）。
  `sentiment_scores` 留在 `research`，不受影响：它从来只读湖里已提交的公告和
  标题，从不读同一次运行的抓取。现有配置原样继续工作——这四步仍在各自 daily
  组里跑，直到你把它们挪进 `[job.events.groups]`，而这正是
  `configs/cnequity.example.toml` 现在随包的排法。`cne run daily --stale-only`
  跳过事件任务拥有的东西，两边不会在不同锁下重采同一份源。
- **`DatasetSpec.session_scope`** 声明数据集的日期轴是交易所会话还是自然日历
  日——一条注册表现在决定增量行走、空日守卫，以及事件任务可以调度什么，而不
  是三份分开的列表。
- **本地看板换成重建过的控制台壳。** 数据集状态、运行健康和运维动作现在共用
  一套响应式视觉层级，以及与 CLI 和清单相同的 success/degraded/failed 词表。
- **Python 3.14 是测过的解释器，而不只是能装上的。** `requires-python` 是
  `>=3.10` 且没有上限，所以 pip 从 3.14 发布起就把这个包装上去，而 CI 停在
  3.13——正是 httpx 下限任务要补上的同一种「可安装却没跑过」缺口，再高一个
  版本。3.14 现在在 Linux 上跑整套测试，也是 macOS 任务钉住的解释器，classifier
  也这么写。不必改源码：每个运行时依赖都能 import，完整测试套件在那里原样通过。
- **对照发布数字的机构检查，而不是第二家厂商。** 湖里每一个价格仲裁都是一个
  转发行和另一个比，只能说明两路源没有差，永远说不出哪边对。三项检查现在越过
  它们（ADR-0006）：
  - `daily_bars_vs_exchange` 把精选 OHLC 和成交额对照上交所、深交所自己发布的
    收盘。实测 2026-08-28：全部 5,212 个共享标的 OHLC **完全一致**，所以价格
    容差收紧（10 bps），越界是错误。成交额带着单向的定义缺口——交易所日合计
    折进了连续竞价 K 线排除的交易——所以按宇宙里分歧的份额判断，而不是按每个
    标的。停牌占位（深交所以零成交量发布、报价源省略）从缺 K 检查里排除。
  - `adj_factor_corporate_action_divergence` 用精选 `corporate_actions`（另一
    家厂商）按除权连续恒等式重算每档 hfq 因子台阶，报告存储序列不一致的地方。
    这能抓住台阶大小错了或日子错了；现有连续性绊线只看见 20 倍以上的断裂。配置
    在 `[adj_factors] crosscheck_*`。
  - `adj_factor_action_implies_nonpositive_price` 标记条款本身不可能对的行动行
    （分红超过整个前收）。
- **`margin_trading` 现在从汇编它的交易所读。** 上交所和深交所从会员报告汇总
  融资融券并发布逐券明细；东财只能拷那份文件。对照 2026-08-26 精选东财日验证：
  3,522 个共享标的上四个字段完全一致，证券数 4,100 对东财的 3,857。
  `[margin_trading] source` 选择归属者，仍接受 `"eastmoney"`；切换是运维的，
  从不自动。
- **机器可读的数据集契约。** `DatasetSpec` 现在携带 `schema_version`、
  `contract_level`、`pit_quality`、`availability_col`、`unit_contract` 和兼容
  策略，推断得出，现有位置构造不受影响。`cne contract show/export/validate/diff`
  （以及 `cnequity.domain.contracts`）把注册表、schema 和主键渲染成带确定性
  指纹的 JSON 文档；`diff` 把列删除、类型和主键变更，以及单位/PIT/历史变更
  分类为破坏。见 `docs/datasets/contract.md`。
- **时点查询模式。** `load(..., pit_mode="strict")` 只返回披露、发布/可得和湖
  观察都可证明不晚于 `as_of` 的版本，并排除重建的回补行；`"best_effort"` 保留
  它们并加上 `pit_is_exact` / `pit_quality`。四列可选双时态列（`available_at`、
  `source_published_at`、`observed_at`、`revision_id`）在读取时填上，
  `scripts/migrate_pit_vintages.py` 把它们就地写入旧文件。
- **版本化宇宙画像。** `cnequity.domain.universe_profiles` 是可复现研究范围的
  注册表（`cn_a_sh_sz_research_v1`、`cn_a_all_experimental_v1`，外加遗留记录），
  带稳定的 `scope_hash`。`load(profile=...)` 绑定交易所/板块、CDR/ETF、ST 和
  退市证据规则，并启用严格检查。`cne profile list/show`。见
  `docs/reference/universe-profiles.md`。
- **已提交的数据集修订和可移植快照。** Compaction 现在在精选文件变化时提交单调
  的 `revision` 外加内容摘要和逐文件哈希，通过 `StateStore` 和
  `cnequity.query.dataset_state` 暴露，以便一次从不移动水位的修复也能让缓存
  失效。`cne snapshot create/verify/restore` 产出带校验和、不可变的湖快照，
  同时携带契约指纹和运行血缘。
- **可移植快照归档和增量湖包。** `cne snapshot export/import` 把快照流到一份
  `tar.zst`（gzip 回退）再流回来，原子发布前先校验清单；每个 tar 成员先检查，
  绝对路径、`..`、重复、链接和设备节点被拒绝而不是解出来。`cne snapshot delta
  create/verify/apply` 只搬两个湖根之间变了的东西——整湖快照不是日常同步的
  工具。delta 带着增加/替换/删除前置条件（两根 delta 用字节哈希，
  `--from-revision` 用已提交修订号），套到错误基线上会失败，而不是静默腐蚀
  目标。`apply` 在完整变更集和事后指纹都通过之前备份每个被覆盖的文件，任何
  错误都回滚；delta 路径限制在 `curated/`、`derived/` 和 `meta/` 下的允许名单。
- **运行级数据集收据。** `dataset_results` 表为每个逻辑数据集和阶段
  （fetch/stage/compact/derive/audit/publish_revision）记一行，带 core/research/
  advisory 关键度，对手工拷贝的清单做加法迁移。`cne status --run <id|latest>`
  报告它。
- **源使用策略登记。** `sources/SOURCES.yml` 按源标签记录访问类型、条款审阅
  状态和保守的使用结论；未审阅权限是字面量 `unknown`，永远满足不了允许检查。
  `cnequity.compliance.source_policy` 和 `cne sources policy` 评估它并失败关闭。
  见 `docs/legal/source-matrix.md`。
- **源 SLO、韧性和稳定性门禁。** `cne sources slo` 把已存探针历史变成按源的
  可用性 SLO 和去重事件载荷；`cne sources resilience` 从注册表推导源集中度、
  故障域和核心数据集的失败关闭备份门，不发网络请求；`cne verify --runs` 检查
  连续干净交易日且不填缺口。各自支持 `--enforce`。
- **收据上的运行出处。** 修订和快照收据通过 `cnequity.provenance` 携带非秘密的
  代码和配置身份（包版本、git commit、配置指纹）。
- **供应链 CI。** 新的 `security.yml` 工作流在依赖变更和每周跑 `pip-audit`，
  并产出 CycloneDX SBOM。`docs/development/release-governance.md` 记录版本和
  数据契约策略。

### 变更

- **`regulatory_events` 从 `announcement_index` 派生，不再重新抓取。** 巨潮
  端点没有服务端过滤，于是该数据集重发 `announcement_index` 的同一请求，只留
  标题匹配关键词的几行——2026-01-01 为 6 条事件翻了 46 页、1,375 条公告，密集
  日则约 220 页翻两次，而源按每秒一次限速。两次抓取还隔一小时，同一天两边可以
  对不上；现在一个是另一个的投影。`cne backfill regulatory_events` 读已索引的
  公告，而不是再扫一遍巨潮，并且只派生窗口里它们实际覆盖的部分（见修复）。
- **调度摄入把正常日跑和迟到的陈旧数据恢复分开。** 日级 launchd agent 覆盖全部
  六个组，不再在线等待延迟源；第二个仅陈旧 agent 稍后跑。两个包装共享一把非
  阻塞、感知 PID 的锁，安装时用 XML 安全路径原子渲染两份 plist。

- **`trading_status` 把 `status` 从新的 `risk_warning` 列拆开（ADR-0007）。**
  `status` 以前同时承载交易状态和 ST / *ST 标识，由 `if/elif` 解析，停牌赢
  ——于是停牌的 ST 名字在已存历史里丢掉标识。线上见过：000711.SZ 在
  2026-08-27 是 `st`，2026-08-28 是 `suspended`，却没离开风险警示，
  `market_breadth` 因而按 ±10% 而不是 ±5% 给那一会话定价。`status` 现在只是
  交易状态（`normal`/`suspended`/`delisted`），`risk_warning` 是自己的可空
  布尔。读取接受两种编码，现有湖仍正确；
  `scripts/migrate_trading_status_risk_warning.py` 让物理 schema 一致（默认
  dry-run）。ST 和 *ST 仍不区分——喂这个数据集的源从未区分过——更细的标识
  仍在交易所简称里。
- **新默认下 `margin_trading` 大约落后一个会话。** 深交所比上交所晚一个业务日
  发布，一天只有两边都到才写——半市场日会推进水位，把另一半搁浅。
- **交易所源下 SH 行的 `short_balance` 为 null。** 上交所不发布融券余额（其
  `rqylje` 字段每行都是 null）。它可以由融券余量 × 收盘重建，但把本地算术盖上
  `source="exchange"` 会把它算到交易所头上。需要该字段时选
  `[margin_trading] source = "eastmoney"`。
- **运行状态契约。** 步骤级 `warning` 现在在运行级报成 `degraded`。`cne status`
  和运行命令（`run daily`、`run --stale-only`、`init`、`retry`）对 degraded
  运行退出 `2`——核心脊柱完成了，研究/咨询工作没有——只有核心失败才是 `1`；
  光秃的 `cne status` 以前总是退出 `0`。
- **研究源失败不再让整次运行失败。** `adj_factors` 或 `industry_index` 派生在
  源不可用时让运行 degraded，并保留已提交的原始 `daily_bars` 修订，而不是把整
  次运行标成失败。
- **`list_datasets()` 新增列** `pit_quality`、`pit_storage_columns`、
  `revision`、`revision_id`、`schema_version` 和 `contract_fingerprint`。
- **PyYAML 现在是直接运行时依赖**（用来解析 `sources/SOURCES.yml`），并作为
  wheel 数据文件随包。
- **CLI 表面从 43 条命令减到 33。** 审计发现其中 58% 既不在 README 也不在任何
  自动化里够得着——一次性工具被发布成 CLI 就变成永久兼容义务。没有丢掉能力：
    - `cne sources probe --only tdx_protocol` 在 0.8.x 线上仍作为弃用、隐藏的
      兼容别名保留。它保持 v0.7.3 的载荷探针语义，并警告计划在 0.9.0 移除；
      替换是 `cne sources probe --only tdx_protocol`。
    - `cne stats refresh` → `cne stats rebuild --if-stale`。它的 `--force` 本来
      就是光秃 `rebuild` 做的事。`--if-stale` 不能和 `--dataset` 组合：它按整湖
      水位决定。
    - `cne contract export` → `cne contract show --out PATH`。同一份文档，差别
      只在是否写到文件。
    - `cne catalog` → `cne stats show` 的无统计回退，输出用 `--json`。从未建过
      统计表的湖不该为了回答里面有什么而先走一遍构建。
    - `cne run catchup` → `scripts/run_catchup.py`；`cne repartition` →
      `scripts/repartition.py`；`cne delisted discover/reconcile/repair/coverage`
      → `scripts/delisted_ops.py`。组合和一次性迁移，这正是 `scripts/` 的用途。
      `cne delisted status` 和 `cne delisted backfill` 留下。
- **`cne sources` 现在是命令组；探针是 `cne sources probe`。** 这里唯一破坏性
  改名：`cne sources` 从源健康板落地起就随包，调用它的脚本需要多一个词。
  `slo`、`resilience` 和 `policy` 子命令在同一个复数名词下加入——它们在同一
  个未发布周期里更早以 `cne source <sub>` 加入，从未以单数随包，所以那个半边
  不影响任何已发布的东西。

  原因：`source` 和 `sources` 是只差一个字母的两个顶层入口，打错一个会跑另一
  条命令而不是报错。`cne source --help` 得花一句话说明谁是谁，命名缺陷就是这
  副样子。故意在 1.0 之前改名——之后就需要一轮弃用。
- **`cli/main.py` 按命令做什么拆开。** 一个 2,828 行模块变成 `setup_cmds`、
  `run_cmds`、`backfill_cmds`、`maintain_cmds`、`quality_cmds`、`govern_cmds`、
  `consume_cmds` 和 `delisted_cmds`，组在 `_root`，共享件在 `_shared`；`main`
  只 import 它们来注册。手写 34 次的 `--config` 现在是一个 `config_option`
  装饰器。测试 patch 绑定名字的那个模块——`main` 故意不再再导出内部，陈旧
  patch 目标会抛错，而不是静默 patch 一个没人读的名字。
- **vendored TDX 树按文件排除，而不是整批发配。** Ruff、`coverage` 和 Codecov
  跳过 `adapters/tdx_protocol/_wire` 下的一切，理由是上游代码保持字节接近以便
  再同步。其中两个文件是修了三处 tdxpy 缺陷并返回原始价格的移植，而
  `_wire/__init__.py`（`TdxWireClient`、页上限、心跳选择）上游根本没有——而
  tdxpy 自 2024 年起无人维护，正是树被 vendored 的原因，所以没有东西可再同步。
  那三个现在像包的其余部分一样被 lint 和度量；真正未动过的文件仍列出、仍跳过。

### 弃用

- **`cne sources probe --only tdx_protocol`** 作为兼容拼写再留一个次版本。它
  跑和 v0.7.3 相同的 `tdx_protocol` 载荷探针，同时发出 `DeprecationWarning` 和
  CLI 警告，计划在 0.9.0 移除。替换是 `cne sources probe --only tdx_protocol`。
- **查询层的 `universe="all_a"`。** 仍能解析并保持宽松的遗留语义，但发出
  `DeprecationWarning`；请选明确画像，例如 `cn_a_sh_sz_research_v1`。
- **PIT 数据集上不带显式 `pit_mode` 的 `load()`。** 省略情形保持旧的
  `fetched_at` 截止，不做精确性保证。研究代码应传入 `pit_mode` 并记下选择。

### 修复

- **没有披露的休市日不再算抓取失败。** `announcement_index` 按自然日走，窗口
  里含交易所从未开市的日子，全市场零行的周日（例如 2026-08-02）会抛
  `no rows returned`——step 失败、`capital` 组失败，依赖它的每一步都失败。
  按自然日逐日查询的源现在容忍空的非会话日；市场确实开过的那天，零行仍大声
  失败；实时页（`flash_news_wire`）任何一天空页都继续大声失败，因为空页对
  日历什么也说明不了。
- **DAG 中途被杀的运行，重试后不再变成 `success`。** 重试路径只在 `init`
  运行上检查从未启动的 step。日更任务被 OOM killer 杀掉后，没走到的 step
  连 batch 都没有，账本看起来干净：`cne run retry` 修好已失败的，把运行关成
  `success`，当天其余部分静默缺失。每次运行现在记下启动时的 step 列表
  （`planned_steps`），重试会跑从未启动的计划 step——跳过该次运行里输入仍
  失败的——并拒绝在计划 step 从未跑过时把运行标成 `success`。本版本之前创建
  的运行没有计划，保持旧行为，而不是按今天的配置去推断一份。
- **`scripts/delisted_ops.py repair` 不再把 `degraded` 当成成功。** 合并只列
  了 `failed` 和 `warning`，于是 `degraded` step 掉进成功分支——形状和分块
  回补一样：每一片都失败的扫描仍报成功。现在除了成功以外的任何东西，合并后
  都作为 warning 留下。

- **`scripts/health_notify.sh` 像跑它的流水线一样尊重 `CNE_BIN`。**
  `daily_pipeline.sh` 和 `stale_pipeline.sh` 都通过文档里的覆盖项 `CNE_BIN`
  解析 `cne`；它们调用的健康门禁却写死仓库自己的 `.venv`，于是设了覆盖项
  之后告警步骤探的是另一份——或者根本不存在的——二进制，各组用的却是真的。
  `china_egress_backfill.sh` 现在也接受 `CNE_BIN`，旧拼写 `CNE` 仍能用。日更
  流水线决定失败要不要喊人的门禁-对-软退出逻辑第一次有了测试，陈旧探针所依
  赖的 `STALE` 退出码也有了。

- **普通 CLI 失误不再打印 Python traceback。** 最常见的三种会直接穿过 Click
  抛出来：任意日期选项的笔误（`date.fromisoformat` 的裸 `ValueError`，来自
  十三个独立调用点）、`cne backfill` 敲错数据集名（注册表的 `KeyError`），
  以及 `cne query --sql` 的 SQL 错误。现在它们是 Click 对其他坏输入报的那种
  一行错误——日期会说是哪个选项、要什么形状，数据集名会建议近似命中，DuckDB
  自己那条已经点名行、列和近似命中的消息原样保留。

- **点名不存在的快照是错误，不是 traceback。** `cne snapshot
  verify|restore|create` 和 `snapshot delta verify|apply|create` 在快照缺失
  或不可读时打印 Python 栈——`export` 和 `import` 早就把它当操作者输入，一
  行说完。现在全都这样，消息点名操作者敲的快照，而不是他们从未写过的清单
  路径。

- **`cne status --datasets` 又能读了。** 这个旗标是 runbook 伸手去摸的新鲜度
  探针，但 `list_datasets` 已经长到二十列——契约指纹、修订 id、PIT 存储列表
  ——表格把它们全塞进终端，每个值撕成一两字符宽的竖条。现在打印旗标承诺的
  东西：数据集、层、新鲜度、覆盖范围和水位。完整清单挪到 `--datasets
  --all-columns`；脚本所依赖的 STALE 退出码不变。

- **分块的 `cne backfill` 不再给彻底失败的扫描报成功。**
  `aggregate_run_status` 故意把非核心 step 的失败叫成 *degraded* 运行而不是
  失败——日更任务里其他数据集仍落地了，湖还能用——但按日期和标的分块的扫
  描只传播 `failed` 和 `warning`，于是一片 `degraded` 让合计停在 `success`。
  46 个已注册 step 里 35 个是非核心，包括 `announcement_index`、
  `financial_statement_items`、`minute_bars` 和 `valuation_metrics`，所以每
  一片都抛错的扫描仍打印 `"status": "success"` 并退出 0——调度流水线那种什
  么都没写、什么都没说的失败模式。自己的收据失败的一片现在让扫描失败，并带
  `resume_from` 停下；仅仅是 degraded 的一片让扫描继续，但不再允许它声称成
  功。未分块路径本来就是这样。

- **`cne backfill regulatory_events` 不再死在第一片。** CLI 默认把这次扫描
  放到 2010 地板，按 31 天切片走，第一次失败就停；派生拒绝窗口里没有公告，
  覆盖守卫只夹尾巴。于是任何公告开始得更晚的湖——本项目自己的 init 从 2016
  起——第一片要 2010、什么都找不到，带着去抓历史上从未有过的披露的建议杀掉
  扫描。窗口现在两端都夹到 `announcement_index` 实际索引过的范围，范围外的
  一片报成 `before_source_coverage` 而不是失败。索引范围内的洞仍拒绝；增量
  运行的对账尾巴伸过第一个已索引日保持安静——那是固定回看，不是覆盖声明，
  报它会让比这条尾巴还年轻的湖永远 degraded。

- **一天不可读不再弄瞎整个对账窗口。** `announcement_index` 每次运行重读最
  近 30 天，源拒绝的单日会让 step 失败——丢掉窗口里其他每一天——直到坏日子
  滚出尾巴。现在按日独立抓取：完整的暂存，失败记成 `fetch_failed_days` 审计
  发现，step 报 `degraded`。什么都读不到的窗口仍大声失败；没有对账尾巴的数
  据集保持全有或全无契约，因为它的洞不会有任何东西回来。
- **密集披露日又能读了，而且每天都读到结尾。** `hisAnnouncement/query` 一次
  查询最多 100 页（3,000 行）：`pageNum>100` 静默重放第 1 页，而 `totalpages`
  继续报真实计数（实测 2026-08-28：6,537 行，`totalpages=217`，第 101 页 ==
  第 1 页）。同一次现场探针还带出另外两条契约事实：`totalpages` 是
  `floor(records / 30)`，所以会漏掉最后的不满页（2026-01-01：1,375 行，
  `totalpages=45`，第 46 页装着最后 25 条），而且 `column` 根本不是交易所选
  择器——`szse`、`sse` 和 `bj` 返回同一份全市场结果。
  - 行走现在跟记录总数而不是页数，于是普通一天的尾巴不再被丢掉，
    `totalpages=0` 读成「不足一页」而不是矛盾。
  - 超过上限的一天用真正能分区的过滤器拆开——`plate` 市场，然后板块，然后
    证监会行业，然后披露类别——每一刀只有在去重桶恰好对上源自己的
    `totalRecordNum` 时才接受。无法证明覆盖的罩子跳过、换下一刀；全都不能
    时，这一天大声失败，而不是把够得着的那部分发布出去。现场验证：
    2026-08-28 经两刀读到 6,537/6,537 行。
  - 每天走一次而不是按交易所 column 各走一次，公告和监管抓取的请求数减半。
- **派生停牌能到达已提交的读者。** `derive_suspension_history` 把
  `is_trading=false` 行直接写进可变的 curated 目录，于是已提交的读者从未见
  过它们，下一次 compact 从没有它们的已提交 generation 重建分区——这就是为
  什么 `daily_bars` 的内部缺口，不能被那条本可解释它的停牌给原谅。行现在
  像其他写入一样走 `staging → compact → commit`，作为日更任务里已注册的
  `trading_status_derive` step（90 天尾巴）、新的 `phase5_derive_and_publish`
  init 阶段（全部历史，在 K 线提交之后），以及 `cne derive trading_status`
  后面。
- **厂商重述的快照不再抹掉更好的证据。** `trading_status` 行现在先按证据等级
  再按新旧选择，polars 和 DuckDB 读路径共用：该会话收盘后的交易所记录或行
  情板读数，压过派生的 K 线缺口停牌，再压过盖到从未观察过的会话上的当前状态
  板。
- **巨潮公告和监管回补有界、可续、失败即关。** 历史请求按 31 天切片跑，带范
  围限定的 checkpoint 和唯一暂存身份。分页契约失败、过期 checkpoint、缺失
  日期、不完整的交易所 column 和部分拆分树，都不能再发布截断的历史。精确
  线路归档现在保留顶层调用身份，离线回放套和现场抓取相同的页、日期、总数
  和重试次数规则，不会把观察混在不同运行、重试或日期窗口之间。
- **稀疏事件日不再挡住 compaction。** 一次成功的有界巨潮响应，对没有公告或
  监管事件的自然日是有效的否定证据；那些日子与源失败区分开，而不是把本来
  完整的回补变成 warning。
- **重试和收据遥测在适配器与编排之间是真话。** worker/非 worker 重新入队和
  源请求重试有分开的持久计数器，多片失败保留累计请求总数，遗留清单做加性迁
  移，一次成功的自记录重试不能继承上一次尝试的失败收据。

- **Python 3.10 无法 import 巨潮适配器。** `datetime.UTC` 是 3.11+；公告和
  它的测试现在用 `timezone.utc`，和树的其余部分一样。那个 import 在包路径
  上，所以 3.10 CI 从未走到收集。
- **Python 3.10 无法收集 `test_contracts`。** 模块在顶层 import 了标准库
  `tomllib`；那个模块是 3.11+。配置加载器和其余 TOML 测试已经回退到
  `tomli`。
- **Windows 拒绝原始归档的符号链接边界夹具。** POSIX `rename()` 会替换空的
  目标目录；Windows 抛 `FileExistsError`。测试现在把数据集目录挪到还不存在
  的路径上。
- **Windows CI 在跨进程限速断言上 flaky。** `time.sleep` 在 50ms 间隔的 48%
  就返回了；地板现在是 100ms 的 30%——仍是空等的好几倍。源并发保持用同一
  个 30% 地板：Windows CI 测到 0.0527，对照 0.064 阈值。
- **Windows CI 把 Unix 调度包装当 PE 二进制执行。** `test_scheduler_scripts`
  用 `subprocess` 驱动 `.sh` 入口（`WinError 193`）。那些调用现在在
  Windows 上跳过，和 `backup_meta.sh` 一样。launchd 模板断言仍跑。
- **Windows CI 在进程池限速测试之后用 `KeyboardInterrupt` 中止单元套件。**
  Windows 上 worker 退出可能把 `CTRL_C_EVENT` 注入父进程的控制台组
  （CPython 33725）；会话现在忽略该信号。残留的 worker atexit 仍可能把全绿
  运行翻成退出码 1——CI 现在收割那些进程并保住 pytest 的状态。

- **没有列的帧被盖章时多出一行。** `pl.DataFrame()` 是代码库里「没什么可报」
  的值，polars 会把字面量对着零*列*帧广播成长度一——于是盖上血缘会造出一
  行，只带着那些字面量。那一行没有主键，严格校验会拒绝它，但几条路径里先
  跑的 `pl.concat(..., how="diagonal_*")` 会和真的一天的行拼在一起，键填
  null，再把它洗进湖。线上撞在 `step_trading_status`：厂商宇宙为空的会话
  产出没有 symbol、没有 `trade_date` 的一行，然后让*下一*会话的日期检查失
  败，还怪错了日子。`with_provenance`（28 个调用点）和
  `normalize_pit_storage_columns` 现在对这种帧 no-op，
  `cnequity.domain.frames.with_columns_unless_blank` 覆盖其余字面量盖章，
  `scripts/probe_blank_frame_broadcast.py` 是扫套件找新出现的 pytest 插件
  ——目前除了 polars 自己之外是零。
- **退市证券每个会话都被发布成正常交易。** 日更写入把既未停牌也不在风险板
  上的一律分类成 `normal` 且 `is_trading=True`，没有退市这个概念。在完整湖
  上对 2026-08-28 实测，那是 **611 个带着 `delist_date` 的标的**——最早退市
  1999-07-12——当天没有任何一根 K 线。厂商板无法报告已经离开市场的证券，
  所以这些行现在来自 `instruments`：`status=delisted`、`is_trading=False`、
  `source=derived_delisted`，`risk_warning` 从最后的简称读；这些标的完全从
  板请求里丢掉。迁移不回填历史——在窗口上重跑日更 step 来纠正。
- **`cne snapshot` 的三个子命令完全没有帮助文案。** `create`、`verify` 和
  `restore` 列成光秃名字，没有描述、没有选项帮助——CLI 里唯一没有的命令。
- **`cne verify --runs` 和 `cne sources policy` 没有 CLI 测试。** 两者都是
  失败即关的门禁，退出码就是全部意义，而 `stability` 每天跑在
  `scripts/daily_pipeline.sh` 里。一层不再抛错的包装会报告失败却仍退出 0。
- **两个 TDX 成交解析器没有测试。** `trade_ticks` 是单源数据集，价格按页差
  分编码，读错一个字段会腐蚀后面每一行——唯一验证是对照现场服务器的一次性
  字节比较，CI 无法重复。`tests/unit/test_tdx_tick_parsers.py` 现在钉住两个
  解析器假定的线路布局：字段顺序、只有历史响应才带的四个填充字节、缺失的
  逐笔成交计数、整数数量，以及未除权的价格。
- **vendored 树自称只保留「本项目用到的五个调用」**，两个成交命令把它带到
  七个之后。`test_tdx_decoupling` 现在钉住保留集合，于是计数是契约而不是
  注释。
- **ETF/LOF 复权因子和深历史现在完整。** 新浪基金载荷用 `s`（`f` 只是占位）；
  适配器现在直接从 `s` 映射基金 hfq，qfq 派生为 `1/s`。ETF/LOF 标的纳入因
  子自愈和覆盖审计，东财提供它们的上市日，THS 2016 年前原始历史计划包含它
  们，同时继续排除没有日期的申购占位。
- **TDX 证券发现排除未上市的交易所占位。** 沪深证券列表会用正的亚跳
  `pre_close` 哨兵宣传 IPO 和基金申购代码。那些行现在在进入证券宇宙、生成
  注定为空的日线批次之前就被排除。
- **瞬时 worker 失败消耗一份持久、有界的重试预算。** 批次重启不再重置
  `retry_count`；一次 retry/resume 调用会按配置节奏重复网络失败的 worker
  批次，直到恢复或到达 `max_retries`，并明确报告耗尽的批次。
- **重试分发跟逻辑任务身份。** 写进不同物理数据集的 step，包括历史 K 线回
  补，按 `task_id` 续跑，同时为更旧的辅助收据保留兼容回退。

## [0.7.3] — 2026-08-23

### 新增

- **质量和健康的研究宇宙范围。** 历史有效性检查接受显式研究宇宙，健康入口和
  `serve` API 按该范围而不是整湖报告就绪，于是从未覆盖某板块的源不再读成数据
  空洞。
- **查询层显式的 `all_a_sh_sz` 宇宙。** 调用方可以直接点名沪深宇宙，而不必用
  过滤器去近似。随之而来的拒绝行为见变更。
- **有范围的 Baostock 公司行动修复。** 公司行动的有界修复，包括历史北交所事件，
  替换以前需要全历史重抓的做法。复权因子对账发现还带着 Baostock 实际修得了的
  退市沪深标的样本（`baostock_repairable_sample`），于是审计输出点名候选，而不
  只计数。
- **可选的北交所 ST 证据回溯到 2016。** Baostock 的 ST 历史只覆盖沪深。显式配置
  Tushare Pro token 后，BJ 证据现在从 `stock_st`（2017-01-01 起）和 `bak_basic`
  名称历史（2016）收集。2016 年前的 BJ 作为已声明的源限制挡住，而不是读成干净；
  未配置 Tushare 仍明确挡住 BJ。
- **按需数据集的 `cne query --refresh`。** 强制重抓并覆盖匹配的缓存变体；按需
  缓存条目按请求参数键控，不同日期、行数和情绪模型不再互相复用结果。
- **北交所尖端成交额补全。** 北交所日线从次级快照补尖端成交额，而不是落下不完整。

### 修复

- **`cne run retry` 在自己的 trade_date 上恢复运行，而不是今天。** CLI 从未传入
  `trade_date`，重试默认落到今天。会话翻过后再试——普通情形——把每个失败批次
  对着今天的日期重放，而不是运行的日期，于是过去日期的回补静默向源要今天的数据，
  重试总是「成功」却从未合上原来的缺口。运行真正的 trade_date 早已记录、早已为
  其他用途读回；现在这里也用。`resume_init` 同一处修复。
- **`cne config validate` 拒绝畸形的 `[[failover.datasets]]` 条目。** 未注册的
  数据集名、点名没有真实源的 `primary`/`backup`，或同一数据集的重复条目，以前
  都静默通过——该数据集的 failover 只是什么都不做，真正宕机时才发现，而不是在
  配置时。
- **Baostock 查询的 Windows 截止回退现在是真正的超时。** 没有 `SIGALRM`（Windows
  上每次查询，不论线程），看门狗只触发关 socket 的副作用并指望它打断阻塞调用
  ——查询堵在别的东西上，或关闭没有传播，就可以无限超过声明的截止。抓取现在在
  守护 worker 线程里跑，调用方自己的等待才是有界的，所以无论查询在做什么，截止
  都站住。
- **东财列表请求在 httpx 0.28+ 上不再丢掉查询串。** httpx 0.27 及更早把 `params`
  字典合并进 URL 已有查询；0.28 则替换查询。push2 的 `ut` token 作为单键
  `params` 字典注入，于是 0.28 上每个 clist 请求塌成 `?ut=...`——丢掉 `fs`、
  `fields`、`pn`、`pz` 和 `fid`，静默少抓资金流向、ST 板、证券、轮动和估值。
  没有东西把 httpx 封在 `>=0.25` 之上，所以即使钉死的开发 lockfile（0.25.2）和
  离线测试套件都藏住了它，任何新装都受影响。token 现在合并进 URL，在每个支持的
  httpx 版本上行为相同，套件在 0.25.2 和 0.28.1 上都绿。
- **整湖一套规范的源优先级。** 存储、查询、视图、日历、板块映射、复权因子、日内
  和分笔检查，以及每个质量消费方，现在用同一方式解析多源行：带时间戳的观察赢，
  平局确定性打破，没有时间戳的遗留行排序一致。以前每个消费方可能对同一键挑不同
  行。
- **日线 failover 有范围、可恢复、无损。** 重试收窄到实际失败的批次，已验证批次
  复用而不是重抓，有效行活过部分失败的批次，请求窗口的边缘缺口被检出，源宕机只
  让那个源 degraded 而不是整次运行。过大的部分尖端快照直接拒绝。
- **卡住的上游请求被打断，而不再堵住一次运行。** TDX K 线请求和原生 socket 读取
  有界，卡住的 Baostock 查询被打断，长交易状态回补心跳，于是停滞看得见，而不再
  和慢进度分不清。
- **适配器对畸形载荷失败关闭。** TDX（未知价格系数、畸形数量和行动金额）、东财
  （行动值）、新浪（全畸形 kline）、中证指数（不支持的工作簿）、北交所（截断的
  行情分页）、Tushare（ST 证据行）和新闻索引（不稳定的文章身份）拒绝坏行，而不是
  写进去。同花顺增持计划被解析而不是丢掉。
- **预期缺口和真缺口区分开。** 退役数据集、按需备份缺口、已知指数源限制、新浪
  成交额缺口和不支持的交易所分类为信息性，并把原因带进审计产物和健康 API，于是
  被挡住的研究问题和带着已知源限制的健康湖分得清。
- **ST 证据收据做漂移检查且可组合。** 重叠覆盖收据合并，之后对同一宇宙的运行延
  伸更早的那份，重复标的被拒绝，进行中的检查点可见，不再匹配它们所声称数据的收据
  被检出。严格宇宙和 MCP 表面以真实全 A 证据为门。
- **占位和 stub 行留在湖外。** 填充的申购占位在摄入时剥掉、compaction 时清除，
  申购 stub 排除在标的宇宙外，退市占位尾巴按规范计数。
- **历史日历和休市建模。** 历史指数数据里的周末行被清洗，日历只留工作日，已验证
  的历史休市日排除，早市收市按证据建模而不是推断。
- **公司行动历史在边界上完整。** 源优先级对齐，历史下限匹配源实际发布的，2016
  年前回补行和最早快照覆盖被保留，可选快照失败不会让回补失败。
- **复权因子报告不完整覆盖。** 部分因子跨度被检出，未缓存的北交所因子被抓取而不是
  假定已有，退市标的不可用的因子被分类而不是静默缺失。
- **PIT 基本面以收集日期为界**，回补不能早于湖本可能知道它的时候把报告浮出来。
- **回补恢复反映实际落下的。** 孤儿 staging 被恢复，估值回补窗口被遵守，空的中证
  回补安全 degraded，派生覆盖修复路由到正确数据集，禁用的日内捕获不作为缺口审计，
  退市标的既不进日内扫描也不进活跃宇宙。

### 变更

- **严格宇宙查询现在在以前返回行或空帧的地方抛错。** 这是本发版唯一对调用方可见
  的破坏。不支持的宇宙名抛 `ValueError`，而不是把帧未过滤传过去（支持的名字是
  `all_a` 和 `all_a_sh_sz`）。在 `strict` 下，返回零行的 `daily_bars` 范围、缺失
  精选证券、缺失 ST 证据各自抛 `UniverseCoverageError`，而不是交出一份静默未经
  证明的总体，MCP 表面强制同一全 A 证据要求。依赖严格查询安静降级的代码现在必须
  捕获 `UniverseCoverageError` 或提供覆盖。这是故意的：无法证明覆盖的严格宇宙
  以前在返回幸存者偏差结果。
- **质量和查询扫描有界或流式，而不是整湖。** 跨数据集扫描、审计合计和估值覆盖
  流式或合并趟次；ST 宇宙扫描、复权审计、日覆盖边界、当日状态读取和 Baostock
  修复窗口剪到真正要紧的窗口；因子审计丢掉整次反连接；ST 收据再验证被缓存；重复
  的日内扫描和重复的日线 failover 请求消除。有界的新浪回退抓取并行跑。注意有界
  不只是速度变化：现在覆盖更窄窗口的扫描可以对同一湖合法产出不同的发现集，所以
  审计输出可能随运行时一起变。

### 文档

- 记录了有范围的研究有效性、公司行动修复范围、MCP 的显式宇宙语义、退役源语义、
  指数历史源限制，以及带 Baostock 节奏的 ST 覆盖。源覆盖计数不再写死会过期的数字。

## [0.7.2] — 2026-08-16

### 修复

- **源适配器对截断或畸形的上游载荷失败关闭。** 日历、巨潮、东财、新浪、TDX、同花顺
  和 Baostock 现在拒绝不完整页和破坏契约的行，而不是写进去。
- **数据集水位和新鲜度跟覆盖，而不是最新文件。** 会话密集的数据集在第一个日历缺口
  停下水位，稀疏尖端不能再看起来完整。
- **退市股回补覆盖准确且可恢复。** 收据和修复跟踪哪些标的真正落下，重试从未收据的
  剩余继续。
- **无成交占位不再漏进派生数据集。** 复权因子、行业指数、市场宽度、情绪和交易状态
  历史跳过不是真实会话的占位行。
- **摄入步骤拒绝部分或畸形快照。** K 线、资本、基本面、结构和相关步骤拒绝不完整
  批次，而不是把它们 compact 成成功。
- **质量审计不再把占位或部分报告当成覆盖。** 数据集检查、交叉检查、派生检查、PIT
  检查和历史有效性契约要求真实行和完整审计产物。
- **查询层感知占位且 PIT 正确。** 分区扫描、宇宙成员和 `load()` 丢掉无成交占位并
  遵守 as-of 语义。
- **CLI 回补恢复和报告匹配实际落下的。** 进度、收据和退出状态不再在只存下请求的
  一部分时声称窗口完成。
- **ST 覆盖检查点活过全 A 宇宙变大。** 之后以更大兼容标的集运行继承已完成标的，
  而不是从头再来。

## [0.7.1] — 2026-08-16

### 修复

- **跨进程限速不再握着文件锁睡觉。** 请求在短锁事务里预留共享的 `next_allowed_at`
  槽，释放锁后再等。锁获取现在有界超时，显式失败，而不是绕过限速器或永远挂起。
- **公司行动回补在标的批次粒度上可恢复。** 成功的块立即进 staging 并记入清单；重试
  跳过那些块，只抓未收据的标的。
- **加固整条摄入流水线的源和存储边界。** 畸形或不完整的适配器响应、非法 TDX 载荷、
  不安全的回补窗口、不完整的流水线结果、混用的分区布局和非原子已发布产物，现在被
  拒绝或显式处理。
- **对齐查询和派生数据语义。** 分片合并、主键去重、分区读取和行业/板块计算现在共用
  同一套规范行为。

### 变更

- 增加限速和公司行动恢复可观测性、清单心跳更新，以及新失败与重试行为的发版/配置
  文档。

## [0.7.0] — 2026-08-15

### 变更

- **项目从 `ashare-lake` 改名为 `CNEquity`。** 旧名与同空间已有项目撞车
  （mpquant/Ashare、AKShare），在「A股 数据湖」搜索里也浮不出来。包：
  `pip install cnequity`（PyPI 上的 `ashare-lake` 将不再更新）。CLI：`cne`
  （曾是 `asl`）。Import：`from cnequity...`（曾是 `from ashare_lake...`）。
  配置/数据默认：`cnequity.toml`、`data/cnequity/`（曾是 `ashare-lake.toml`、
  `data/ashare-lake/`）——现有本地配置和数据目录不会自动改名。

## [0.6.0] — 2026-08-10

### 修复

- **申万（`sw`）每次抓取都 TLS 校验失败。** `swsresearch.com` 只发叶子证书、不发
  中间证书——每次握手只有一张证——于是 certifi 建不出到它信任的根的路径。浏览
  器和 macOS curl 跟着叶子的 Authority Information Access 扩展把这藏起来；Python
  不会。实测 httpx 0/5，curl_cffi 0/6。公共 DigiCert 中间证书现在随包装上
  （`asl sources --only sw`：0/5 → **5/5**），在不削弱校验的前提下恢复路径——根
  仍必须被信任，主机名仍必须匹配。影响 `industry_members` 回补，这是服务器的属性，
  所以内地用户同样撞上。
- **`core` 无法在调度槽内完成，静默吞掉下一组。** 调度的 `daily*` 组共享一把非阻塞
  `daily_ingestion` 锁：下一组开火时上一组还在跑就不会排队，下一组中止。全市场
  `daily_bars` 实测 543ms/标的——约 5400 个标的约 49 分钟——对到 `capital` 只有
  30 分钟空隙。随包调度现在给 `core` 60 分钟，并按实测时长把其余错开，碰撞消息说
  的是一组正在被跳过，而不是点名一把内部锁。

- **`share_unlock_schedule` 每次运行都失败。** 东财 datacenter 现在直接拒绝日期列
  上的范围比较——`参数预处理错误:
  org.antlr.v4.runtime.InputMismatchException (code=9501)`——于是
  `(FREE_DATE>=…)(FREE_DATE<=…)` 过滤让步骤从能用变成抛错，这边没有任何改动。它
  现在按最新优先翻报告，停在第一页结束早于窗口的地方，然后在本地应用视界：63 页
  每页 500 降到约 7 页，实测 141.8s → **13.7s**，结果按行对照全量扫描验证过。
- **`commodity_bars` 每次运行烧掉 151s 却什么都不返回。** 快速失败谓词反了：它恰好
  重试 `is_transport_fail_fast` 说重试修不了的传输失败，瞬时的却立刻放弃。`clist`
  和 `datacenter` 都栽在同一谓词上。对着不可达的 push2his 实测 151.2s → **17.5s**。
- `fetch_datacenter` 接受可选的 `stop_after` 谓词，用于对已排序报告提前停下。它压
  制声明-`count` 完整性守卫，因为短读正是目的；没有它时守卫不变。

- **`northbound_flows` 从来不是北向。** 它读
  `push2his /stock/fflow/kline/get?secid=1.000001` 并把 f52 → 沪股通、f53 →
  深股通。那些字段是上证指数的主力净流入和小单净流入——零和分解的两腿，所以两条
  「通道」在 14 天里有 13 天符号相反，28 行里有 3 行超过交易所自己的 520 亿日额度
  上限，最高到 777.9 亿。那台主机不可达时，回退写入 `kamt` 的北向字段，而那路源
  退役后一直是硬零——于是好日子列里是错数，坏日子发明平坦会话。现在读沪深港通资金
  历史（`RPT_MUTUAL_DEAL_HISTORY`，`MUTUAL_TYPE` 001/003），同时填上
  `buy_amount` / `sell_amount`——以前写死成 0.0。

  **现有行是错的，必须替换**：丢掉该数据集的分区和它的水位，然后
  `asl backfill northbound_flows`。

- **`northbound_flows` 有了真正的历史：2014-11-17 → 2024-08-16。** 交易所在
  2024-08-16 之后停止发布每日北向净流入，所以 2024-08-19 起的行金额为 null。那些
  **丢掉，而不是填零**——零会声称没有数字的地方有平坦会话。水位因而冻在
  2024-08-16，`asl status` 永远报该数据集 STALE；那是源的状态，不是流水线故障。

  该步骤也把整个未结窗口一次请求抓完，而不是每个会话一次，因为冻结的水位否则会让
  每日缺口窗口无限长大。

### 变更

- **把 CLI 参考与 `asl demo --research` 同步。** 公开命令表现在记录新浪 hfq 对照，
  不再描述已移除的东财粘滞状态。

- **增加维护自动化。** 每周源健康探针发布明确标注的 GitHub Actions / 海外报告，版本
  标签现在要求匹配的包版本、`twine check` 和干净的发行构建，然后才是可选的 PyPI
  Trusted Publishing。

- **发布可搜索的文档站和可复制粘贴的研究 Recipes。** GitHub Pages 构建覆盖首次运行
  上手、复权语义、PIT 财务、DuckDB / Polars、MCP 和运维；拉取请求以严格模式构建，
  导航漂移在合并前可见。

- **现代化包许可证元数据。** 构建现在用 SPDX 许可证表达式和 `project.license-files`，
  避开弃用的 setuptools 表，同时保留 Apache-2.0 许可证和随包 NOTICE 文件。

- **东财客户端又是普通 `httpx`，`[sources.eastmoney].proxy` 是海外唯一杠杆。** 去掉
  curl_cffi 的 Chrome-JA3 伪装、带 DoH / `dig` / 写死种子 IP 梯子的 `CURLOPT_RESOLVE`
  CDN 钉死、粘滞上次好边缘文件（`meta/state/push2his_endpoint.json`），以及让那把
  梯子的失败模式付得起的出口断路器——大约 430 行，内地路由什么都买不到，东财轮换
  边缘或 TLS 指纹时就会坏。代理现在覆盖每个东财主机，而不只是 push2his kline，
  `httpx.ProxyError` 加入快速失败集合，死掉的代理停下批次而不是烧掉重试预算。
- **随包示例配置按内地路由定节奏**（`min_interval_seconds` 3.0 → 0.5，`batch_size`
  15 → 50，`batch_rest_seconds` 60 → 5），把整次约 991 个板块的 `sector_bars` 扫描
  从约 2h 降到约 10min。以前的值是按敌对海外出口定的，每个内地用户都在为它们付钱。

### 移除

- **`asl push2his remember` / `asl push2his probe`。** 两者只为驱动上面的粘滞 CDN
  边缘机械而存在。海外用户设 `[sources.eastmoney].proxy`，并用
  `asl sources --only eastmoney_push2,eastmoney_push2his` 验证。

### 文档

- **`sector_bars` 文档写成同花顺数据集，它已经是一段时间了。** 目录、步骤参考、CLI
  参考和东财适配器页仍把它写成东财 clist 日线外加 `backfill_source="eastmoney_kline"`
  下的 `push2his` kline 回补。注册表说 `ths`，`[sources.ths]` 禁用时步骤会抛，日线和
  历史故意是同一源——混用一次曾把两个指数基拼接进同一序列，在 439 个板块上造出假的
  +79% 中位跳。排障条目还引用了没有代码发出的日志行，并指向
  `[sources.eastmoney].proxy`，对该数据集什么都不做。
### 移除（配置）

- **`[sources.eastmoney].batch_size` / `.batch_rest_seconds` 没了。** 它们被解析进
  `Config` 却从未被任何东西读——批次冷却是 baostock 机制——于是读起来像已经接上
  的节奏，而每次东财扫描只靠 `min_interval_seconds`。未知键被忽略，所以仍带着它们
  的配置原样加载；方便时删掉那两行。

## [0.5.0] — 2026-08-03

### 变更

- **许可证：MIT → Apache License 2.0。** 项目源码现在在
  [Apache-2.0](LICENSE) 下；第三方声明（包括仍为 MIT 的 vendored tdxpy）住在
  [NOTICE](NOTICE)。落地行情数据仍在软件许可之外——见
  [legal](docs/legal-and-data-sources.md)。
- **README 主视觉改牌 ASL · cnequity。** 更短的卖点、幸存者图表和 demo 放前面；裁过
  的 serve 记分卡和架构图各自成节。现有湖没有升级步骤。
- **`asl servers test` 和 `asl push2his` 离开顶层命令列表。** 两者仍能用——它们在
  快速上手和 runbook 里——但 `servers test` 现在是 `asl sources --only tdx_protocol`
  的别名，断言回来的是真 K 线而不是 socket 开了，`push2his` 调试一台 CDN 主机。它们
  在和构成流水线真实状态机的命令抢注意力。

### 新增

- **标准引用元数据。** `CITATION.cff`、文档引用页和包元数据 URL，便于引用带版本的
  研究依赖，而不暗示数据再分发权。

- **`asl mcp`：湖作为 MCP 服务器，只读，走 stdio。** 六个工具按问题形状切，而不是
  每个数据集一个——`describe_lake`、`resolve_symbol`、`query_bars`、
  `query_fundamentals`、`query_dataset`、`run_sql`。智能体每轮从扁平列表挑，39 个
  数据集工具会把大部分上下文窗口花在它不会调用的名字上，却仍让它猜哪个回答问题。

  **查询契约走在响应里，而不是文档里**，因为模型不读 `docs/`。`describe_lake` 返回
  复权、PIT、`snapshot_only` 和 `universe` 规则；没有 `adjust` 的 K 线查询带着警告
  回来；带 `adjust` 的报告有多少行没有因子并静默用了 1.0；`query_fundamentals` 拒绝
  给 `as_of` 默认值并说明原因。每个载荷带着 `total` / `returned` / `truncated`，于是
  4,300 行里的 200 行一页不能被平均后报成市场的。

  `run_sql` 恰好接受一条 SELECT，由 DuckDB 自己的解析器而不是正则决定：湖摄入
  `news_headlines` 和 `flash_news_wire`，这里没人写的厂商文本，所以到达工具的 SQL
  可以被摄入内容塑造。只读连接仍会允许 `COPY ... TO`。

  **没有新依赖。** stdio JSON-RPC 循环约 200 行，而不是官方 `mcp` SDK，后者解析成
  另外 15 个包——本地服务器从不做的 OAuth 流程用的 cryptography、pyjwt 和
  truststore，什么都不导出的 tracing 用的 opentelemetry，以及钉死 httpx 旁边的第二
  套 HTTP 栈。不带 extras 的 `pip install cnequity` 保持原样。

- **`asl init --profile quick` / `--since`**——第一次回补*更浅，从不更窄*。`quick`
  抓全截面最近三个日历年；过滤标的反而会把本湖存在就是要修的幸存者偏差直接建进去，
  缺失的名字看起来和从未交易过的名字一模一样，而更少年份由 `coverage_start` 老实
  记录。窗口写进运行元数据，于是 `--resume` 复用它，而不是几天后静默退回全深度。

- **`asl sources`：本地 A 股源健康板。** 十四条探针覆盖本湖依赖的端点——TDX、三台
  东财主机、新浪、巨潮、两台同花顺主机、baostock、两家交易所、申万、央行、国家统计局。
  报告落在 `meta/source_health/<vantage>.json`，`asl serve` 在 `/source-health`
  渲染它。这些端点不是本项目的：AkShare、agent skill 文件和手写爬虫都依赖它们，却
  没有地方可查某一个变没变。

  探针是 CLI 动作，查看是看板。一次 GET 伸向十几台第三方主机，正是看板只读立场要
  阻止的——也是那里什么都不触发摄入的同一原因。

  **HTTP 200 不是「通」。** 东财用 200 答挑战页，新浪用空数组答未知标的，同花顺用
  200 且无行答限速页。因此每个探针都对载荷断言——`total`、`klines`、xlsx 的 `PK`
  魔数——而 `empty` 是自己的状态，因为源客气地答什么都没有，正是静默截断回补、却
  看起来比失败更健康的东西。

  **视角被记录，永不合并。** 其中若干在 WAF 拒绝非内地出口，所以同一主机同一秒一列
  可以绿、另一列红。`--vantage` 给每份报告打标签，页面并排渲染；合并会发明两边探针
  都没建立的事实。

  探针调用适配器自己的 URL 常量和客户端，于是脆弱的部分就是被测的部分。它们串行、
  每个源一次：会踩限速封禁的健康检查会造成它存在就是要观察的那次宕机。

- **`asl mcp --live`：没有湖的智能体用的 MCP。** 湖里没有的地方，标的查找和未复权
  日线按需从厂商抓、永不写入。其余按名字拒绝并带原因——基本面是因为厂商返回今天对
  重述数字的看法，没有老实的 `as_of`；`run_sql` 是因为它查询 live 模式并不产出的
  parquet。每个载荷带着 `origin: "lake" | "live"`，两边都标注，缺失字段不能默认成
  「lake」。除非问起否则关闭：湖坏了的用户必须得到「没有 parquet 数据」然后去修。
  每次调用上限 50 个标的、800 天，且必须有 `symbols`。

- **长抓取运行时的进度。** `asl init` 和 `asl run daily` 完全不设日志，于是数小时的
  回补直到收尾 JSON 才打印——和挂了分不清，看起来挂了的进程连同它已经攒下的小时
  一起被杀掉。步骤和 worker 池已经在记；没人在听。父进程现在每批打一行，带行数、
  已用时间和粗略估计。`--quiet` 退出。

- **`survivorship_gap.py --lang`**——图表标签本地化，于是中文 README 嵌入中文图。
  同一组数字，同一几何。

- **`scripts/survivorship_gap.py`**——在湖自己的 K 线上度量偏差，并产出无依赖 SVG。
  同一等权篮子、同一日期，唯一差别是退市名字是否仍在里面：2016–2021 读成完整 5.9%
  对仅幸存者 12.0%。这是下限而不是估计——退市名字带到它们最后打印的 K 线，只计
  精确复权名字，而湖自己的退市覆盖可能不完整，这些都会缩小测得的缺口。

- **`trade_ticks`：成交记录（分笔），选择启用且限定自选范围。** 两条新的 TDX 报文
  命令（`0x0fc5` 同会话，`0x0fb5` 历史），一个整会话要么全装要么不装的适配器，以及
  带着自己的 `[trade_ticks]` 配置块、`ticks` 步骤组和质量检查的数据集。默认关闭，
  不在任何调度上。

  **这些不是逐笔成交。** A 股 Level-1 是 3 秒快照，所以一行聚合了落在那一帧里的
  无论多少真实成交——实测，600519 平均 6.3，000001 平均 33.4。报文时间戳只有分钟
  精度（协议从未带秒），所以行按 `tick_seq` 键控，即它们在会话里的位置。`direction`
  是 TDX 自己的 tick-rule 推断，不是交易所字段，其 `after_hours` 值覆盖 15:05–15:30
  的固定价格交易，交易所日成交量不计这部分。

  历史对每个标的回到 2024-01-02——*固定下限*，不是分钟线那种滚动的逐标的 K 线计数，
  所以 `DatasetSpec` 新增 `history_floor_date`。成本约每个标的-会话 1.85 次请求、
  约 2,700 行，磁盘上约 8.4 字节一行：200 名字自选大约一分钟、4.5MB 一个会话。
  `[trade_ticks].scope = "all"` 在配置校验时拒绝，`max_symbols`（200）在第一次请求
  前拦住解析后的范围。

- **`DatasetSpec.history_floor_date`**——源边缘表达成日历日，而不是滚动交易日计数。
  `earliest_available()` 优先用它，回补守卫触发时丢掉「收窄你的范围」建议，因为没有
  范围伸得过固定下限。两个字段现在到达看板和 `/api/datasets/{name}`，以前无法表达
  哪种机制产出了 `earliest_available`——于是把有日期限制的源叫成无限。

- **`DatasetSpec.row_grain`**——一行覆盖什么（`1m` / `5m` / `tick`），仅描述。与
  `intraday_frequency` 分开，后者驱动抓取、检查和读取器，而 `trade_ticks` 故意不设；
  只有后者时，日内成交记录被显示成日级数据集。数据集面板的日内频率事实现在是行粒度。

### 修复

- **看板自己的测试依赖墙上时钟。** 新鲜度对照*今天*的上一交易日判断，于是带固定日期
  的夹具在写它的那天通过，之后每个数据集都报陈旧。

- **非默认起点的回补静默宽松。** K 线批次是否严格抓取——分页中途失败时抛而不是留下
  已到的页——从 `start == 2016-01-01` 推断，那是回补曾经唯一有过的起点。`asl init
  --since` 自己挑起点，所以 `_window_backfill` 现在问编排器的 `_backfill` 旗标，并把
  日期测试留作回退。没有这个，浅 init 路径会是丢掉某标的更早年份却不说的那条。

- **日内回补不再按日期切片尖端分页的 TDX 行走。** 分钟线源从今天向后翻页，于是靠近
  视界的 10 天块仍必须重抓每一页更新的页——CSI300 1m 付了约 8× 必要的报文流量。
  `minute_bars` / `minute_bars_5m` 现在用 `backfill_chunk_symbols=200`（每个标的一次
  尖端→视界行走，按批次 compact）。那条路径上 `resume_from_symbol` 替换日期
  `resume_from`。默认 `[minute_bars].fetch_workers` 提高到 4（仍被跨进程限速器封在约
  10 req/s）。

## [0.4.0] — 2026-08-01

<a id="upgrading-from-03x"></a>
### Upgrading from 0.3.x

1. **`daily_bars.volume` 始终是股（`data_version = v2`）。** 在 0.3.x 下写入的湖
   按源混用单位，信任成交额或流动性因子前需要一次性改写：

   ```bash
   scripts/migrate_daily_bars_volume_v2.py --config configs/cnequity.toml --dry-run
   scripts/migrate_daily_bars_volume_v2.py --config configs/cnequity.toml --apply
   ```

   先备份 curated；脚本幂等，不重盖 `fetched_at`。

2. **AkShare 没了。** 从任何手工改过的配置里删掉 `[sources.akshare]`（示例模板已经
   没有它）。为社融加上 `[sources.pboc]`，可选为 `asl audit` 里的发布方交叉检查加上
   `[sources.nbs]` / `[sources.exchange]`。pip/uv 留下的孤儿包：

   ```bash
   pip uninstall akshare mini-racer py-mini-racer
   ```

   `asl doctor --fix` 已移除——它只修 mini-racer 碰撞。

3. **宏观自愈。** 下一次 `macro_indicators` 运行改写错误的 `m2_yoy` 历史，并从央行
   回补 `social_financing`；没有单独迁移。行按 `(indicator_id, obs_date)` 保留最新
   `fetched_at`。

4. **日内是选择启用。** `[minute_bars].enabled` 默认 `false`，不在每日波次上。启用
   它，然后 `asl run daily --group intraday`（或 `asl demo --intraday` /
   `asl backfill minute_bars_5m …`）。TDX 保留约 95 个交易日的 1m 和约 491 个的 5m
   ——更早的窗口什么都不返回。

### 新增

- **`minute_bars` / `minute_bars_5m`——选择启用的日内 K 线。** 分开的数据集（各一种
  频率），因为源保留 95 个交易日的 1m 对 491 个的 5m，而一个数据集带着一个水位、一个
  `coverage_start` 和一个视界。注册了 schema、PK `(symbol, trade_date, bar_time,
  frequency)`、按日分区、`steps/intraday.py`（`group="intraday"`）、带
  `adjust="qfq"/"hfq"` 的 `load()`，以及四项审计检查。默认关闭
  （`[minute_bars].enabled = false`）；全市场 1m 约 35MB/天（8.4GB/年），对照
  2001–2026 整座日级湖的 468MB。`[minute_bars].scope` 默认 `index:000300.SH`
  （1m 约 2MB/天）。

  `bar_time` 是 K 线的**收盘**分钟（TDX 标注）：完整会话是 09:31–11:30 和
  13:01–15:00 上的 240 根；15:00 那根带着收盘集合竞价。价格未复权；复权在查询时
  加入当天因子。15m/30m/60m 不存——它们从 5m 精确聚合（`docs/datasets/catalog.md`
  有重采样片段）。

- `asl demo --intraday`、`[minute_bars].fetch_workers`（线程化并发 TDX 连接；不提高
  请求速率）、`asl backfill <intraday> --symbols`、`DatasetSpec.history_horizon_days`
  / `backfill_chunk_days`，以及日内审计检查（`minute_bars_off_session`、
  `minute_bars_trade_date_mismatch`、`minute_bars_session_coverage`、
  `minute_bars_daily_reconciliation`）。`asl backfill` 拒绝源视界之前的 `--start`。

- **发布方交叉检查（`quality/authority_checks.py`）。** 伸到发布方，而不只是湖的内部
  一致性：

  - `macro_pmi_vs_nbs`——制造业 PMI 对照国家统计局发布
  - `st_labels_vs_exchange`——ST 标识对照上交所 / 深交所列表

  由 `[sources.nbs]` / `[sources.exchange]` 把门，缺失时默认关闭。即使全部一致，结果
  也落在 `meta/quality/source_diffs/authority-{date}.json`。不用国家统计局查询 API
  （非内地出口 403）；改为解析发布文句。M2 不覆盖：央行只发水平，并从 2025-01 修订
  了 M1 口径。

- **`st_label_crosscheck`**——`trading_status` ST 标签对照证券交易所简称上的 ST 前缀
  （TDX 简称 × 东财风险板；无网络）。替换退役的 AkShare ST 并集，后者查询的就是东财
  适配器已经在查的同一 push2 端点，永远不可能不一致。

- **`macro_checks.py`**——月度宏观的新鲜度和修订跟踪（issue #10）：
  `macro_indicator_stale`、`macro_value_revised`。

- **`adapters/pboc/`**——央行 Excel 附件里的社会融资规模增量（双语表头，显式
  `单位：亿元人民币`）。示例配置里的 `[sources.pboc]`。覆盖到 2026-06；陈旧阈值
  75 天。

- `daily_bars_volume_unit` 审计检查（`quality/unit_checks.py`）：按源的中位
  `amount / close / volume` 落在 [0.8, 1.25] 之外则让运行失败。

### 变更

- **`daily_bars` 是 `data_version = v2`**——v2 保证 `volume` 是股；v1 意味着单位取决于
  `source`。通过 `domain.schemas.data_version_for` 按数据集解析；其余每个数据集仍在
  v1。`index_bars` 和 `sector_bars` 保留 TDX 自己的成交量单位（见
  `docs/datasets/schema.md`）。

- **`social_financing` 来自央行。** 社会融资规模是央行统计；中间商务部转发行路径
  （从未随包）落后两个发布周期，并端出被取代的版本。安静带着陈旧值的备份不是安全
  备份（ADR-0003）。年工作簿最新优先读，于是重述版本赢；堆在亿元表下面的百分比表
  由每张表自己的单位声明跳过。

- 宏观月度序列（`pmi`、`m2_yoy`）直接读东财 datacenter 报告（`RPT_ECONOMY_PMI` /
  `RPT_ECONOMY_CURRENCY_SUPPLY`），带上项目的重试、限速和 TLS 处理。每行盖上自己的
  `source`。

- README 架构图刷新（`docs/assets/architecture-overview.png`）：去掉 AkShare，在官方
  源下加上 `pboc`。

### 修复

- **`m2_yoy` 存的是 M0 环比增长，不是 M2 同比。** 旧的 AkShare 路径按中文子串匹配列，
  默认 `next(..., columns[-1])`；`"M2-同比增长"` 从未匹配
  `"货币和准货币(M2)-同比增长"`，于是每次抓取都落到 `流通中的现金(M0)-环比增长`。
  现在读成字段 `BASIC_CURRENCY_SAME`。下一次 `macro_indicators` 运行改写该序列（全
  历史重抓 + compact 保留最新 `fetched_at`）。

- **AkShare 时代路径下 `social_financing` 从未写出一行**（compact 的 `YYYYMM` 月份被
  当成不可解析丢掉）。央行适配器从 2015-01 回补。

- **`daily_bars.volume` 混用股和手，恰好差 100×。** 只有 `ths` 和 `baostock` 已经写
  股；`tdx_protocol` 把手透传，新浪除以 100。每个适配器现在在边界正规化成股
  （`cnequity.domain.units`）。

- **无成交 K 线存的是反规范化成交额而不是零。** TDX 的打包浮点解码器把原始零映射成
  `2**-127`；在 `adapters/tdx_protocol/_decode.py` 为日线和日内修好。现有行由成交量
  v2 迁移脚本清洗。

- **靠近历史边缘的回补日内切片静默返回零行。** `max_pages` 按切片宽度（`end - start`）
  定大小，而报文总是从今天向后翻页；靠近视界的切片在到达请求日期前耗尽 `max_pages`。
  现在按 `trade_date -> start`（真实行走深度）定大小。

- **一次重连失败就能中止整次全市场日内扫描**，丢掉已 staging 但未 compact 的批次。
  连接对重新探测的服务器重试一次；批次级失败被记录而不是中止步骤；批次大小 50 →
  200。

### 移除

- **AkShare 不再是依赖。** 以前两处调用都不是第二源：ST 板打的是树内已经查询的同一
  东财 push2 过滤，PMI / 货币供应包装打的是同一 datacenter 报告。丢掉它移除 15 个
  传递包，包括 `mini-racer`，外加 `asl doctor --fix` 和 `diagnostics/repair.py`
  （从来只关于那次碰撞）。示例配置里没有 `[sources.akshare]`。

## [0.3.1] — 2026-07-29

### 变更

- 把支持的 Python 下限从 3.11 降到 **3.10**（`requires-python = ">=3.10"`）。
  东财紧凑 `YYYYMMDD` kline 日期现在经 `strptime` 解析（3.10 的 `date.fromisoformat`
  只接受带横杠的 ISO 形式）。CI / classifier 覆盖 **3.10–3.13**。
- README 架构图是一张双语 JPG（`docs/assets/architecture-overview.jpg`）；Pillow
  渲染器和分开的 zh/en PNG 没了。

### 修复

- `asl config init` 始终写入**绝对** `data.root`（把模板的 `./data/cnequity` 相对
  当前工作目录解析），于是默认首次运行路径上 `asl doctor` 是绿的。
- 默认 `[on_demand].datasets` 只有 `stock_news` 和 `research_reports`。
  `announcement_body` / `financial_reports` 抛 `NotImplementedError`，而不是缓存空的
  占位 JSON。失败的 research_reports 抓取也不缓存。
- baostock / pandas 的 ImportError 提示不再推荐已移除的 extras（`[valuation]` /
  `[structure]`）；它们指向重装 `cnequity`。

## [0.3.0] — 2026-07-29

<a id="upgrading-from-02x"></a>
### Upgrading from 0.2.x

pip 和 uv 都不会卸掉仅仅不再是依赖的包，所以升级后的环境仍留着 `mootdx` 和它的
`py-mini-racer`。后者随后与 AkShare 带进来的 `mini-racer` 共享 `py_mini_racer`
import 包，一个静默覆盖另一个。本项目抓的东西不受影响——它调用的 AkShare 端点没有
一个会执行 JS——但如果你直接调用，AkShare 自己的 cninfo 和 sina API 会坏。

    asl doctor        # 报告它
    asl doctor --fix  # 解决它

`mootdx` 本身作为死重量留下，可以卸掉。全新环境没有这些。

### 变更

- `pip install cnequity` 就是完整安装。每个运行时源——AkShare、Baostock、SnowNLP，
  以及解析申万和中证成份电子表的 pandas/openpyxl/xlrd 三件套——都是硬依赖，于是没有
  daily 或回补步骤会因为忘了 extra 而静默丢掉一个源。相对以前的最小安装大约多 217MB。
- TDX 行情现在用 vendored 报文客户端（`adapters/tdx_protocol/_wire`，源自 tdxpy，
  MIT）而不是 `mootdx`。`mootdx` 和 `tdxpy` 都在 2024 年最后一次发布，无人维护。对照
  直播服务器验证与先前实现字节相同，包括完整 51478 行证券列表。
- `httpx` 不再封在 `<0.26`；那个上限来自 `mootdx`。安装现在解析到 0.28.x。
- 随包回退 TDX 主机列表现在在树内维护（`adapters/tdx_protocol/hosts.py`）。探测全部
  49 台已知主机发现 mootdx 的 38 台全死；提供真 K 线的四台排在最前。服务器选择从 16
  次探针全失败变成约 3s 解析出来。

### 新增

- 原生 Windows 10/11（64 位）支持：跨平台文件锁替换运行锁、水位写入、限速和 staging
  清理里 Unix 专用的 `fcntl.flock`（`cnequity.file_lock`）。
- CI `windows-latest` 任务跑离线单元套件。
- `asl config init` 在 Windows 上默认 `workers = 1`（与 macOS 相同）；之后提高
  workers 允许——Windows 用 spawn，不是不安全的 macOS fork 路径。
- 安装文档覆盖 PowerShell / cmd、路径形式，以及支持的 Windows 范围（x86-64；32 位 /
  ARM64 推迟）。
- `asl doctor`——检查 `asl config validate` 故意做不到的，因为那条命令对环境盲目：
  `data.root` 是否绝对、存在且可写，以及每个声明的依赖是否真正 import。`--fix` 跨
  平台修 `py_mini_racer` 发行碰撞。
- 守卫（`tests/unit/test_tdx_decoupling.py`）在 `mootdx`、`tdxpy` 或 racer 包被
  import 或重新声明为依赖时让构建失败。
- README（zh/en）以**到数据的最短路径**开头（demo 对 daily 湖），然后是数据集和对等
  比较表——给首次读者更少重复的安装/阅读章节。
- README 在最短路径上方展示分层架构 PNG（zh/en）；比较表下丢掉一行对等 punchline。
- README 截图去掉退役的 `mootdx` 探针行重渲；横幅文案跟上当前 `asl demo`。
- 文档枢纽重排（安装/快速上手在前）；`architecture.md` 和 `datasets/README.md` 折进
  overview/catalog 桩；模块 cli/query/config 页缩成源码地图。
- 架构 PNG 列出主源 + 补充适配器（`ths` / `sw` / `cni` / `macro`，外加日历种子注释）。

### 修复

- `bars()` 现在尊重从交易所后缀派生的市场。`mootdx` 没有这个参数，于是本项目算出的
  值被静默丢掉，再从代码前缀重派生。
- `asl demo` 不再看起来像挂了。它让探针客户端开着，心跳线程不是守护线程，于是六个
  步骤都已经打印完解释器仍活着。客户端现在关掉。
- vendored 客户端长回 `do_heartbeat`，裁到五个方法时把它丢掉了。`HeartBeatThread`
  每 10s 按名字调用它，于是每次 keepalive 都抛 AttributeError——短到够不到第一个
  间隔的测试看不见。
- DuckDB 视图 glob 现在用 POSIX 路径（`as_posix()`），于是 Windows 反斜杠不再打断
  `read_parquet(...)` SQL 字面量。
- Polars 递归扫描走 `parquet_glob()`（同一 POSIX 规则）；证券规划器用 `Path.rglob`
  而不是 `glob.glob(f"{Path}/…")`。
- `asl config init --data-root` 在路径是 Windows `C:\…` 形式时不再剥掉转义的反斜杠
  （可调用的 `re.sub` 替换）。
- `asl demo` 写入 TOML 安全的 `data.root`（转义的 POSIX 路径），于是后续
  `asl query --config configs/cnequity.demo.toml` 在 Windows 上能用。
- `asl doctor` 用真正的创建/删除探测可写性（不是 `os.access`），并在 Windows 上建议
  ACL 修复而不是 `chmod`。
- 东财粘滞 IP / CLI 粘滞读取始终用 UTF-8。
- 原子 parquet 替换在 `PermissionError` 上短暂重试（DuckDB / Explorer 仍握着目标时
  的 WinError 32）。
- TDX 心跳线程是守护线程，断开时 join，于是 spawn worker 在关闭后不逗留。
- 测试助手经 `path_for_toml()` 嵌入 `data.root`，于是 Windows CI 不再死在未转义
  `C:\Users\…` 造成的 `TOMLDecodeError: Invalid hex value`。
- Windows CI：`path_for_toml(Path("/tmp/…"))` 断言在 `win32` 上接受盘符 POSIX 形式
  （`D:/tmp/…`）。
- 离线单元覆盖扩到东财 / cninfo / failover / 情绪 / 板块助手，项目分支覆盖坐到 80%
  以上。
- 陈旧文档：去掉退役的 pip extras（`[macro]` / `[valuation]` / …）；澄清 `ASL_*`
  环境变量只给脚本用（`asl` CLI 不读它们）；修好东财适配器 CLI 相对链接和快速上手
  Init 锚点。

### 移除

- 全部 extras（`tdx`、`macro`、`nlp`、`valuation`、`structure`、`all`）。旧文档里的
  `pip install "cnequity[tdx]"` 仍能正确安装：pip 警告 extra 未提供并继续，uv 什么
  都不说。
- 贡献者工具从 `dev` extra 挪到 PEP 735 依赖组：`pip install -e . --group dev`
  （pip >= 25.1）或 `uv sync`。

## [0.2.0] — 2026-07-27

### 新增

- `asl config init` 写入随包的示例 TOML（不必检出仓库）；在 macOS 上强制
  `orchestrator.workers = 1`
- 随包模板在 `cnequity.config.templates`（与 `configs/cnequity.example.toml` 保持
  同步）

### 修复

- PyPI 项目页：随一份短的中文 `README.pypi.md`，用绝对 GitHub 链接（完整
  `README.md` 的相对路径在 pypi.org 上会断）

### 变更

- 把 `pip install "cnequity[tdx]"` 写成主安装路径
- `pyproject.toml` 的 `readme` 指向 `README.pypi.md` 而不是 `README.md`
- 入门文档用 `asl config init` 而不是 `git clone` + `cp`；快速上手把一分钟 demo 和
  全市场 init 分开

## [0.1.0] — 2026-07-19

自托管 A 股 Parquet 数据层的首次公开发布。

### 新增

- 多源摄入（TDX/mootdx、东财、新浪、巨潮、可选 Baostock/AkShare）进入
  staged → curated → derived 湖布局
- CLI（`asl`）用于 `init`、`run daily`、`backfill`、`compact`、`derive`、`audit`、
  `status`、`retry`、`query`、`catalog`
- Python `load()` API，带 `adjust` / `universe` / 时点 `as_of`
- 精选 Parquet 上的 DuckDB 视图
- 数据集覆盖参考、K 线、公司行动、基本面、资金流、板块/行业结构、宏观、新闻/情绪和
  风险事件
- 质量审计（PK、模拟源守卫、复权因子对账、交叉检查）
- 可选 extras：`tdx`、`valuation`、`macro`、`nlp`、`structure`、`dev`
- 日级流水线、健康通知和 meta 备份的运维脚本
- 文档：对照 AkShare/Tushare/Baostock、法律说明、schema 契约、按源限制、runbook

### 安全 / 卫生

- 忽略运行时日志和本地工具/编辑器目录
- HTTP 客户端默认开启 TLS 校验
- 项目 URL 指向 `rootSunc/cnequity`

[Unreleased]: https://github.com/rootSunc/CNEquity/compare/v0.10.0...main
[0.8.1]: https://github.com/rootSunc/CNEquity/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/rootSunc/CNEquity/compare/v0.7.3...v0.8.0
[0.7.3]: https://github.com/rootSunc/CNEquity/releases/tag/v0.7.3
[0.7.2]: https://github.com/rootSunc/CNEquity/releases/tag/v0.7.2
[0.7.1]: https://github.com/rootSunc/CNEquity/releases/tag/v0.7.1
[0.7.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.7.0
[0.6.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.6.0
[0.5.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.5.0
[0.4.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.4.0
[0.3.1]: https://github.com/rootSunc/CNEquity/releases/tag/v0.3.1
[0.3.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.3.0
[0.2.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.2.0
[0.1.0]: https://github.com/rootSunc/CNEquity/releases/tag/v0.1.0
