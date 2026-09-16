# CLI 参考

命令：`cne`（`cnequity.cli.main:cli`）

全局默认：`--config configs/cnequity.toml`

**命令名不区分大小写**：`cne STATUS`、`cne run DAILY`、`cne config CREATE` 与小写等价（Click 的 `token_normalize_func`）。`-h` 是 `--help` 的短写法。**不做前缀匹配**——`cne stat` 会被拒绝并提示 `stats` / `status`，而不是猜一个执行。

**六个命令不接受 `--config`**：`cne contract show|validate|diff`、`cne profile list|show`、`cne sources policy`。它们读的是随包发布的注册表而不是湖，所以指向哪个湖都给同一个答案。`cne doctor` 则相反——它接受 `--config`，但没有配置也能跑（这正是它存在的场景）。

---

## 命令一览

顶层 19 个入口，分五节——顺序就是一个湖被使用的顺序，和 `cne --help` 的分节一致。
分节定义在 `cnequity/cli/_root.py` 的 `SECTIONS`，漏掉任何一个命令都会让测试失败。

### 开始使用

| 命令 | 作用 |
|------|------|
| [`cne config`](#cne-config-create) | 校验、生成或 diff 配置（`create` / `validate` / `diff`） |
| [`cne doctor`](#cne-doctor) | 查环境、可选依赖与配置的静默故障；无配置无网络也能跑 |
| [`cne init`](#cne-init) | 建湖并跑 init phases。`--profile demo\|sample\|quick\|full` |

### 跑 pipeline

| 命令 | 作用 |
|------|------|
| [`cne run daily`](#cne-run-daily) | 日更采集（Wave DAG 或指定 schedule group） |
| [`cne run events`](#cne-run-events) | 7×24 事件流（公告、资讯），走自然日历而非交易日历 |
| [`cne run retry`](#cne-run-retry) | 重试一个 run，或每个 daily 分组最新的失败 run |
| [`cne run compact`](#cne-run-compact) | 把 staging 合进 curated |
| [`cne run clean`](#cne-run-clean) | 清理已 compact 的终态 run 的 staging 与过期孤儿 |
| [`cne backfill`](#cne-backfill-dataset) | 回填一个数据集 |
| [`cne derive`](#cne-derive-name) | 派生计算数据集 |

`compact` 已经是每个 schedule group 自带的 step，所以 `retry` / `compact` / `clean`
是**故障后的手动出口**，不属于正常的一天。

### 检查湖

| 命令 | 作用 |
|------|------|
| [`cne status`](#cne-status) | 最近 run 状态；`--datasets` 看逐数据集新鲜度 |
| [`cne verify`](#cne-verify) | **该落的有没有落**。默认按数据集×交易日，`--bars` 按证券×会话，[`--runs`](#cne-verify---runs) 按连续交易日运行证据 |
| [`cne audit`](#cne-audit) | **落下来的对不对**；`--full` 给全湖健康快照 |

`audit` 与 `verify` 问的不是同一件事——一个问正确性，一个问完整性。

### 消费湖

| 命令 | 作用 |
|------|------|
| [`cne query`](#cne-query) | 跑 DuckDB SQL，或按需拉取数据集 |
| [`cne serve`](#cne-serve) | 只读湖面板（默认 `127.0.0.1:8787`） |
| [`cne mcp`](#cne-mcp) | 以 MCP stdio 把湖提供给 AI agent |

### 治理与检视

| 命令 | 作用 | 子命令 |
|------|------|--------|
| [`cne snapshot`](#cne-snapshot) | 可移植快照的建立、校验与安全恢复 | `create` `verify` `restore` `export` `import` · `delta {create,verify,apply}` |
| [`cne contract`](#cne-contract) | 检视与校验已注册的数据契约 | `show` `diff` `validate` |
| [`cne profile`](#cne-profile) | 检视版本化的研究 universe 画像 | `list` `show` |
| [`cne stats`](#cne-stats) | `meta/stats` 下的度量表（行数、字节、源分布） | `rebuild` `show` |
| [`cne sources`](#cne-sources) | 探测依赖的数据源并检查证据 | `probe`（唯一联网）`slo` `resilience` `policy` `substitutes` |
| [`cne delisted`](#cne-delisted) | 读退市目录并抓它点名的历史 | `status` `backfill` |
| [`cne ths-official`](#cne-ths-official) | 对照/回填同花顺官方 API（需 key） | `capture` `backfill` `repair-bars` `resource-sectors` |

---

## 改名对照

输入旧名时 CLI 会直接给出新写法，不是 Click 默认的 "No such command"：

```
$ cne retry
Error: `cne retry` has moved. Use `cne run retry` instead.
```

| 旧 | 新 | 为什么 |
|----|----|--------|
| `cne demo` / `cne demo --sample` | `cne init --profile demo` / `--profile sample` | 建多大的湖是**一根轴**：demo / sample / quick / full。让第一次上手的人先在两个命令之间做选择，是多余的一次分叉 |
| `cne config init` | `cne config create` | 和 `cne init` 只差一个词，而后者会建整个湖，误敲的代价大得多 |
| `cne retry` / `cne compact` / `cne clean` | `cne run retry` / `run compact` / `run clean` | 它们只作用于 run，放在 `run` 下面才是会去找的地方 |
| `cne verify-bars` | `cne verify --bars` | 和 `cne verify` 问的是同一件事，只是粒度不同；两个顶层命令差一个连字符 |
| `cne stability` | `cne verify --runs` | 同上，第三种粒度：交易日 × run |
| `cne ths-official snapshot` | `cne ths-official capture` | 原来和顶层 `cne snapshot`（湖快照）同名不同义 |
| `cne servers test` | `cne sources probe --only tdx_protocol` | 早已标记废弃，声明 0.9.0 删除却一直留到 0.10 |

对照表定义在 `cnequity/cli/_root.py` 的 `MOVED`。它比隐藏别名更诚实——别名会烂在代码里，一个 dict 不会。

---

## cne init --profile demo | sample

一分钟试玩，是 `cne init` 的两档 profile（原 `cne demo`）：`demo` 拉少量流动性股票的真源近期日线，`sample` 在完全离线时生成明确标记为 `source=mock` 的合成小湖。两者都**不是**全市场——那是 `--profile quick|full`，见 [cne init](#cne-init)。

下列选项只对这两档生效；`--config` / `--resume` / `--layout-only` / `--since` 只对 `quick|full` 生效。用错一侧会被按名字拒绝。

| 选项 | 说明 |
|------|------|
| `--symbols` | 逗号分隔标的（默认茅台/平安银行/五粮液/宁德/中国平安） |
| `--days` | 约多少个交易日的 `daily_bars`（默认 30） |
| `--intraday` | 额外抓同一批标的的 1m 线（最多约 5 个交易日），打印一根完整会话 |
| `--research` | 额外从 Sina 派生 hfq 因子，并打印 raw / hfq 收益对照；会把窗口扩展到约 3 年 |
| `--data-root` | 独立湖根目录（默认 `data/cnequity-demo`） |
| `--profile sample` | 不访问网络，生成可用于验证安装、查询和 DuckDB 视图的合成样例；不可与 `--research` / `--intraday` 合用 |
| `--trade-date` | 截至日 YYYY-MM-DD（默认今天 / 最近交易日） |
| `--config-out` | 写出供后续 `cne query` 使用的小配置（默认 `configs/cnequity.demo.toml`） |

流程：建目录 → 探测 TDX → 拉 instruments 并裁成 demo 宇宙 → 交易日历 → `daily_bars` + compact → 打印样例表；加 `--research` 时再派生 Sina hfq 并校验 exact 覆盖，加 `--intraday` 时再跑 `minute_bars`。终端有分阶段进度与 INFO 日志。需要能访问 TDX；`allow_mock` 不会打开。

只想验证研究口径，不必初始化全市场：

```bash
cne init --profile demo --research --symbols 600519.SH
```

`--research` 需要额外访问 Sina；网络受限时先运行不带该选项的基础 demo。

完全无法访问 TDX 时，可先验证本地读写和查询链路：

```bash
cne init --profile sample
```

合成行会醒目标记为 `source=mock`，质量审计不会把它们视为真实数据；请勿复用该 demo 的 `data_root` 做研究或生产。

---

## cne init

初始化数据湖并执行 init phases。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径 |
| `--layout-only` | 仅建目录、manifest、DuckDB 视图 |
| `--trade-date YYYY-MM-DD` | init 截至交易日（默认今天） |
| `--resume` | 续跑最近未完成 init |
| `--run-id` | 续跑指定 init run（隐含 resume） |
| `--keep-going` | phase 失败后继续后续 phase |
| `--profile demo\|sample\|quick\|full` | 建多大的湖。`quick`（默认）= 全市场标的、最近 3 年；`full` = 全市场、各 step 自己的起点（`daily_bars` 为 2016-01-01，实测约 3 倍耗时）；`demo` / `sample` 是几只票的小湖，见 [上一节](#cne-init---profile-demo--sample) |
| `--since YYYY-MM-DD` | 显式指定历史起点，覆盖 `--profile` |
| `--quiet` | 只留 warning 及以上，不打逐批进度 |

**默认会打进度。** 全市场回填是几十个批次、可能跑几小时；之前它一声不吭直到最后吐 JSON，和卡死没法区分——而看起来卡死的进程会被 kill 掉，白扔已经跑完的几小时。现在每个批次一行：

```
14:22:07 INFO ...worker_pool: daily_bars 12/54 batches · 1,043,882 rows · 18m04s elapsed · ~1h03m left
```

**`quick` 是更浅，不是更窄。** 全市场标的一个不少，只是每只少几年。按标的裁剪会把这个湖本来要修掉的幸存者偏差直接建进去，而且一个缺席的标的看起来和「这只票从没交易过」一模一样；少几年的历史则由 `coverage_start` 如实记录。

窗口会写进 run metadata，`--resume` 自动沿用——否则几天后从新进程续跑会默认回到全深度，去抓你当初特意跳过的年份。

之后加深不必重跑 init：

```bash
cne backfill daily_bars --start 2016-01-01 --end COVERAGE_START
```

上表中 `--config` / `--layout-only` / `--resume` / `--run-id` / `--keep-going` / `--since` 只对 `quick|full` 生效；`--symbols` / `--days` / `--data-root` / `--config-out` / `--intraday` / `--research` 只对 `demo|sample` 生效。传错一侧会被按名字拒绝，不会被忽略。

退出：result `status != success` 时退出 1。

---

## cne config create

从包内模板写出用户配置（PyPI 安装后无需 clone 仓库）。

| 选项 | 说明 |
|------|------|
| `--config` | 输出路径（默认 `configs/cnequity.toml`） |
| `--data-root` | 写入 `[data].root` |
| `--force` | 覆盖已存在文件 |

macOS 上会把 `orchestrator.workers` 写成 `1`（与 `validate` 规则一致）。模板源：`cnequity.config.templates`（与仓库 `configs/cnequity.example.toml` 保持同步）。

---

## cne config validate

校验 TOML 与 step 引用。有错退出 1。

---

## cne config diff

对比当前配置与包内示例模板，列出**模板有而你没有**的部分。

用户配置由 `cne config create` 写一次，之后不再更新，而且是 gitignore 的。后续版本给调度组
新增的 step 不会自己出现在里面 —— 功能装上了，但从来不会被调度，`cne config validate`
依然回 `Configuration OK`。这条命令就是补这个信号。

报告分三类，按后果排序：

| 类别 | 后果 | 退出码影响 |
|------|------|-----------|
| 未被调度的 step | **会丢数据**：step 存在但不在任何 group / wave 里，永远不跑 | 有则退出 1 |
| 缺少的配置段 | 使用内置默认值 | 不影响 |
| 缺少的配置项 | 使用内置默认值 | 不影响 |

`[data].root` 和 `orchestrator.workers` 等本机相关取值不算漂移；配置里多出来的自定义内容也不报。

---

## cne sources resilience

按失败域展示来源集中度、爆炸半径与独立备源门禁。

| 选项 | 说明 |
|------|------|
| `--with-availability` | 把这个湖已积累的探针历史按失败域 join 上去（读湖，需 `--config`） |
| `--window-days` | 可用率统计窗口（默认 30 天） |
| `--enforce` | 关键数据集缺独立备源时退出 1 |
| `--out PATH` | 写文件而非打印 |

集中度本身不能决定主备源的选择：一个域背着 30 个数据集，其危险程度与它**从本机有多经常够不着**成正比，而后者是测出来的、不是声明出来的。`--with-availability` 把两者放进同一张表：

```
failure domain     datasets critical   measured  worst probe
eastmoney                30        4       0.0%  eastmoney_push2his
tdx                       8        5     100.0%  tdx_protocol
exchange                  4        3      88.2%  exchange_szse
```

一个域按它**最差**的探针计：需要两个端点的 feed，任一挂掉它就挂掉。没有观测的探针不贡献读数
（"从未测过"和"测出来是 0"是两个不同的答案），对应的域保持未标注。

`build_dependency_report` 本身仍是注册表的纯函数 —— 确定性、可测；join 只发生在 CLI 层。

---

## cne contract

查看和维护 42 个注册数据集的机器可读 JSON 契约。

| 子命令 | 说明 |
|--------|------|
| `show [DATASET]` | 输出一个数据集或完整 registry 契约；`--out PATH` 写文件而非打印 |
| `validate [PATH]` | 校验文件；省略 PATH 时校验当前 registry。文件加 `--against-registry` 做精确同步检查 |
| `diff OLD [NEW]` | 比较两个契约；省略 NEW 时比较当前 registry。默认发现 breaking 时退出 1，检查报告可加 `--allow-breaking` |

diff 会把删列、改类型、改主键、单位/PIT/历史语义变化识别为 breaking；新增
列和新增数据集为 compatible。

> `cne contract export` 已并入 `cne contract show --out`——两者本来就是同一份文档，
> 只差写不写文件。

---

## cne profile

查看版本化的研究 universe 画像（`cnequity.domain.universe_profiles` 注册表）。

| 子命令 | 说明 |
|--------|------|
| `list` | 输出注册表记录；`--official-only` 排除 legacy 兼容画像 |
| `show NAME` | 输出单个画像及其 `scope_hash`；`--symbol` 可重复，绑定具体标的并附 `concrete_scope_hash` |

画像绑定交易所/板块、CDR/ETF、ST/停牌、退市与 PIT 证据规则。研究读取用
`load(..., profile="cn_a_sh_sz_research_v1")`，并把 `name` / `version` / `scope_hash`
一起记进产出。详见 [universe 画像](universe-profiles.md)。

---

## cne doctor

环境与配置体检：`data.root` 是否绝对路径 / 可写、声明的依赖能否 import。不访问网络；无配置也能跑（新鲜安装）。有实质性风险时退出 1。

| 选项 | 说明 |
|------|------|
| `--json` | 机器可读输出 |

`cne doctor --fix` 已移除（只服务于已卸掉的 mini-racer 冲突修复）。

---

## cne run daily

| 选项 | 说明 |
|------|------|
| `--group` | `core` \| `capital` \| `signals` \| `fundamentals` \| `macro_risk` \| `research` \| `intraday` |
| `--backfill` | 强制 backfill 语义（慎用） |
| `--stale-only` | 只重抓仍落后于最后交易日的数据集（与 `--group` 互斥） |
| `--quiet` | 只留 warning 及以上，不打逐步进度 |

### --stale-only：当天的第二次机会

`snapshot` 数据集只抓 run 当天。**一次源端中断吃掉那个窗口，那天就永久没了**——`valuation_metrics` 就这样丢了 2026-07-30 和 07-31：per-host 重试和退避本来就有，只是全部耗尽了，而 snapshot 语义决定了后面任何一次 run 都补不回来。

缺的不是重试，是**当天的第二个窗口**。挂在主 pipeline 几小时之后：

```cron
# 主 pipeline
5 16 * * 1-5 /path/to/cnequity/scripts/daily_pipeline.sh

# 收尾补抓：只跑仍然落后的，没有就空转
5 20 * * 1-5 cd /path/to/cnequity && cne run daily --stale-only
```

新鲜度判据与 `cne status --datasets` 完全一致（含每数据集的 `max_staleness_days` 容忍），所以两者不会各说各话。没有落后的数据集时不建 run、直接退出 0，可以安全挂在定时器上。

派生数据集不在其中：它们由 curated 重算，该跑的是 `cne derive`，不是重抓。

无 `--group` 时跑完整 `[job.daily.waves]` DAG。`intraday` 组不在默认调度里：需先开 `[minute_bars].enabled`，再 `cne run daily --group intraday`。

成功或 `skipped_non_trading_day` 退出 0。

---

## cne run events

7x24 事件流：公告、监管事件、资讯。**不看交易日历**（这些源周末和节假日照发），
并且拿自己的 `events_ingestion` 锁，不与晚间批处理抢 `daily_ingestion`。

| 选项 | 说明 |
|------|------|
| `--group` | `[job.events.groups]` 中的一个组；默认按配置顺序跑完所有组 |
| `--trade-date` | 自然日 `YYYY-MM-DD`（默认今天），周末/节假日同样有效 |
| `--quiet` | 只留 warning 及以上 |

```cron
# 每天（含周末）一次全量事件流
0 14 * * *  /path/to/cnequity/scripts/events_pipeline.sh

# 只要资讯的日内新鲜度：单张实时页，代价很低
*/30 9-22 * * *  CNE_EVENTS_GROUP=news_wire /path/to/cnequity/scripts/events_pipeline.sh
```

`disclosures` 组每次都会重读 30 天对账尾窗，高频跑它是在重复付这份代价；
`news_wire` 没有尾窗，适合高频。组按配置顺序执行，所以 `regulatory` 能读到
`disclosures` 刚发布的公告。

---

## cne backfill DATASET

单数据集 backfill。snapshot 且无 `backfill_source` 时拒绝。

成功时自动 compact 当前 run。

| 选项 | 说明 |
|------|------|
| `--start` / `--end` | 窗口（日内数据集拒绝早于源端视野的 `--start`） |
| `--symbols` | 日内、`daily_bars`、`trading_status`、`corporate_actions` 的临时标的范围；其他数据集仍使用配置中的范围 |
| `--baostock-repair` | 仅 `corporate_actions`：显式补抓已退市 SH/SZ 标的的 Baostock 分红除权数据；建议与 `--symbols` 配合 |
| `--ths-repair` | 仅 `corporate_actions`：显式补抓已退市 BJ 标的的同花顺历史分红除权数据；建议与 `--symbols` 配合 |
| `--eastmoney-bj-repair` | 仅 `corporate_actions`：按北交所旧码→920 新码映射向 EastMoney 定向补抓历史分红除权数据；建议与 `--symbols` 配合 |
| `--bse-tip-repair` | 仅 `daily_bars`：读取已有 session 的 OHLCV，仅向 BSE 请求成交额并严格核对；必须同时指定相同的 `--start/--end` 与 `--symbols` |

```bash
cne backfill minute_bars_5m --start 2026-05-01 --end 2026-07-31 \
  --symbols 600519.SH,000001.SZ

# 已有 BJ 日线只补当前分区成交额，不重抓 Sina 历史
cne backfill daily_bars --start 2026-08-21 --end 2026-08-21 \
  --symbols 920000.BJ,920001.BJ --bse-tip-repair
```

### sector_bars

| 选项 | 说明 |
|------|------|
| `--retry-failed` | 跳过 checkpoint 中已完成的板块，只重试失败项 |
| `--force` | 清空 checkpoint 后全量重拉（与 `--retry-failed` 互斥） |

Checkpoint：`meta/state/sector_bars_backfill.json`。失败超过 50% 时 step 状态为 `warning` 但仍写入已成功部分。

**网络**：走同花顺 `d.10jqka.com.cn`（日更与历史同源），限速在 `[sources.ths]`。
与东财无关，`[sources.eastmoney] proxy` 对它不生效。

```bash
# 首次或换源后全量
cne backfill sector_bars --config configs/cnequity.toml --force

# 续跑失败板
cne backfill sector_bars --config configs/cnequity.toml --retry-failed
```

---

## cne run compact

| 选项 | 说明 |
|------|------|
| `--run-id` | 指定 run（默认最近 run） |

将 staging 合并入 curated。

---

## cne delisted

读退市目录，并拉它点名的那些行情。**重建**目录（扫码空间、核对终点、修 instruments、
覆盖门禁）是一次性工程，在 [`scripts/delisted_ops.py`](../operations/scripts.md#delisted_opspy)。

| 子命令 | 说明 |
|--------|------|
| `status [--since]` | 目录摘要：数量、年份、尚未 ingest |
| `backfill [--since]` | 对目录中尚未有行情的退市股拉 Sina 历史并 compact |

推荐顺序（跨 CLI 和脚本）：

```bash
cne delisted status                                 # 已知多少
python scripts/delisted_ops.py discover --limit 500 # 扫码空间，可续跑
cne delisted backfill --since 2016-01-01            # 拉扫到的行情
python scripts/delisted_ops.py repair               # bars 已在湖里时写 delist_date
python scripts/delisted_ops.py reconcile            # 先 dry-run
python scripts/delisted_ops.py reconcile --apply    # 仅在没有 active ingestion run 时
python scripts/delisted_ops.py coverage --start 2016-01-01 --universe all_a_sh_sz
```

`coverage` 的通过声明刻意很窄：它证明退市目录已扫完，且已知与窗口重叠的退市标的具备一致的末根有效成交和证券主数据；它不证明两端之间每个交易日都连续。数据源在停牌或正式摘牌前可能保留零成交占位行，门禁不会把它们误当成末次交易。目录末日晚于窗口、但窗口内又没有行情可证明已经上市的标的会进入 `unknown_overlap`，不会被静默排除。

`reconcile --apply` 不以单一供应商返回的“最后一条记录”为真相：必须有 curated
正成交量终点，且该终点不晚于 `instruments.delist_date`，同时旧目录日期还必须落在
正式退市日之后或非交易日，才允许自动修改。命令检测到任何 active ingestion run
都会拒绝执行；修改前的目录保存在 `meta/state/history/`，质量回执写入
`meta/quality/`，并记录修改前备份和修改后目录的 SHA-256。

---

## cne derive [name]

| name | 说明 |
|------|------|
| `adj_factors`（默认） | 计算 Sina hfq 因子 |
| `trading_status` | 派生历史停牌记录（`--start` / `--end` 按年分块重建） |
| `sector_routing` | 可选：EM 板块 × TDX 88xxxx 名称映射表（**不驱动** sector_bars 采集） |
| `sector_code_map` | BK* ↔ BOARD_CODE 身份映射（lake-only；推荐成分 join） |

```bash
cne derive trading_status --start 2001-01-01 --end 2001-12-31
```

---

## cne audit

| 选项 | 说明 |
|------|------|
| `--run-id` | 指定 run 的 findings（默认最近 run） |
| `--full` | 湖级健康快照（非 per-run 文件） |
| `--research-start YYYY-MM-DD` | 与 `--full` 合用；严格验证所选历史宇宙，未通过时退出 1 |
| `--research-end YYYY-MM-DD` | 研究窗口末日；默认取 `daily_bars` 最新分区 |
| `--research-universe all_a\|all_a_sh_sz` | 历史研究口径；默认 `all_a`，`all_a_sh_sz` 排除 BJ 的来源能力缺口 |

`--full` 且 UNHEALTHY 退出 1。显式传 `--research-start` 后，研究宇宙未通过也退出 1；此时末行会显示 `HEALTHY (operational; research BLOCKED)`，表示湖的运营健康与研究可用性是两个独立门禁。未显式传 `--research-start` 时，历史宇宙状态仍写入 health 与 `historical-validity-latest.json`，但不会改变运维健康的退出码。快照同时记录 `historical_universe`，避免把 scoped 结果误读成全 A。

---

## cne verify

三种粒度问同一个问题「该落的有没有落」。默认按数据集 × 交易日；`--bars` 按证券 ×
交易日；`--runs` 按交易日 × run。三者的选项互不通用，用错会被按名字拒绝而不是被忽略。

| 选项 | 模式 | 说明 |
|------|------|------|
| `--dataset` | 默认 | 只查这些数据集（逗号分隔）；默认全部已注册数据集 |
| `--repair` | 默认 | 对可修复的缺口跑回填，按数据集从新到旧 |
| `--kind` | 默认 | 只看这些缺口类型：`empty,stale,interior,shallow` |
| `--bars` | — | 改为逐证券检查覆盖，见下 |
| `--start` / `--end` | `--bars` | 覆盖窗口；`--start` 必填，`--end` 默认上一个完整交易日 |
| `--runs` | — | 改为检查连续交易日运行证据，见 [cne verify --runs](#cne-verify---runs) |
| `--days` / `--as-of` / `--enforce` | `--runs` | 见下方小节 |

（`--bars` 原为 `cne verify-bars`，`--runs` 原为 `cne stability`。）

**和 `cne audit`问的不是同一件事。** `audit` 问「落下来的数据对不对」，`verify` 问
「该落的有没有落」——后者是一个 step 一碰就抛异常时产生的故障。没有它，一个数据集可以
连续数周每次 run 都失败，而每次 run 只记录一个 failed batch，湖级看不出来。

**缺口按「能不能补」分开，而不是按大小。** `by_date` 数据集缺一个交易日是故障；
`snapshot` 数据集缺一个交易日是它本来的形状，任何回填都不可能诚实地补上它
（补了就是伪造行）。`--repair` 只跑前者。

```bash
cne verify                                  # 全表体检
cne verify --dataset daily_bars,adj_factors
cne verify --kind interior --repair   # 只补内部空洞
cne verify --bars --start 2026-09-07  # 逐证券 × 会话，含窗口内零行的证券
cne verify --runs --days 20 --enforce # 连续交易日运行证据
```

`--bars` 检查的是「证券 × 会话」这一格，包括窗口内一行都没有的证券——默认模式按数据集
聚合，看不见这种缺失。停牌等明确非交易状态不算缺口。不完整时退出 1。

---

## cne status

| 选项 | 说明 |
|------|------|
| `--datasets` | 逐数据集新鲜度表（dataset / layer / freshness / 覆盖区间 / watermark）；有 STALE 退出 1 |
| `--all-columns` | 配合 `--datasets`：打印 `list_datasets` 的全部列（契约指纹、revision、PIT 存储列等），而非仅新鲜度 |
| `--groups` | 配合 `--datasets`：只对这些调度组拥有的数据集判失败（空格或逗号分隔）。其它组的数据集照常列出、照常报为调度缺口，但不触发退出 1。只跑 `core` 的主机有二十多个数据集无人抓取，不加此项门禁天天失败（2026-09-12/13/14 为 21–25 个），告警就此失效。无人调度的数据集（`(unscheduled)`）仍然判失败——“不知道谁抓”不等于“别的主机在抓” |
| `--run <id\|latest>` | 指定 run（默认 `latest`）；摘要含每个数据集 stage 的 `dataset_results` 与聚合 `dataset_status`。别名 `--run-id` 已删除 |

无选项：输出最近 run 的 JSON 摘要。run 为 `degraded`（核心正常、研究/建议层降级）退出 2，
核心失败退出 1。

---

## cne run retry

重试失败 batch / 补 init 缺失 step。init run 走 `resume_init`。

| 选项 | 说明 |
|------|------|
| `--run-id <id>` | 重试指定 run |
| `--failed-groups` | 逐个独立进程重试每个 `daily:*` 分组最新的失败 run；若该分组已有更新的成功 run，则跳过旧失败 |

两项必须且只能选择一项。

成功退出 0；`RunLockError` 报错退出。

---

## cne run clean

删除已 compact 的终态 run staging，以及超龄 orphan。终态含 `success` / `warning` / `failed`（需 incomplete=0 且有成功 compact batch）。

| 选项 | 说明 |
|------|------|
| `--dry-run` | 只报告，不删任何东西 |
| `--orphan-retention-days` | 无 manifest 的 orphan staging 保留天数（默认 7） |
| `--snapshot-retention-days` | `meta/source_snapshots` 下 run_id 目录的保留天数（默认 14）。每个 dataset/source 的最新一份始终保留 |
| `--keep-revision-generations` | 每个数据集在 `meta/revisions/data` 下保留的已提交代数（默认 5）。receipt 永远保留，`current.json` 指向的那一代永不丢弃；`0` 关闭 |
| `--log-retention-days` | 删除 `logs/cne-*.log` 里超过这么多天的（默认 30）。每次调用写一个带时间戳的日志，此前没有任何东西清理它们；`0` 关闭。只处理本 CLI 命名的文件——`logs/` 下别人放的（如 launchd 的 stdout 重定向）不动 |
| `--reconcile-runs` | 清理前先把卡在 `running` 的 run（worker 崩溃）标记为 failed。`--reconcile-after-seconds` 可覆盖判定窗口，默认取 `[orchestrator].batch_stale_seconds` |
| `--force` | 也删尚未 cleanup-ready 的 staging（incomplete / 未 compact）；成功 fetch batch 会被 demote，`cne run retry` 全量重抓。**不要**对 success-without-compact 用 force——先 `cne run compact --run-id` |

---

## cne serve

只读湖面板：分层总览、逐数据集覆盖与新鲜度、溯源分布、覆盖热力图。

| 选项 | 默认 | 说明 |
|------|------|------|
| `--host` | `127.0.0.1` | 非回环地址**必须**配 `--token` |
| `--port` | `8787` | |
| `--token` | 无 | 要求 `Authorization: Bearer <token>` 或 `?token=` |

```bash
cne serve
```

页面在 `/`，单数据集在 `#/dataset/<name>`（状态 / 元数据 / 数据 三个 tab），跑批在 `#/runs`（含实时甘特），质量在 `#/quality`，OpenAPI 在 `/api/docs`（由 handler 生成，不会与实现漂移）。

**面板不写湖。** 没有端点会跑批、重试或清理——那些留给 CLI。唯一的例外是 `meta/stats` 会在后台按需重建，因为它是湖的缓存而不是湖的一部分。

数值全部来自已落盘的产物（注册表、目录布局、`meta/stats`、`meta/quality/health-latest.json`、manifest），**请求路径上不扫 curated**。所以：先 `cne stats rebuild` 才有行数与体积；findings 显示的是上次 `cne audit --full` 的快照，页面上标了日期。

端点与热力图语义见 [serve 模块](../modules/serve.md)。

---

## cne stats

湖的自我度量表，写到 `meta/stats/`。`list_datasets()` 只看目录名，答不了「这个分区有多少行、多大、谁写的」——那些在这里。

产物：

| 文件 | 粒度 | 列 |
|------|------|-----|
| `partition_stats.parquet` | dataset + partition | `granularity`、`period_start/end`、`row_count`、`file_count`、`bytes` |
| `provenance_stats.parquet` | dataset + partition + source + data_version | `row_count`、`fetched_at_min/max` |
| `stats-latest.json` | — | `generated_at`、`latest_run_id`、汇总数 |

两张表而不是一张：`bytes` / `file_count` 是目录的属性，`row_count` 按源拆分，把文件级数字挂到细粒度上会让它看起来可加，而加起来是重复计数。

不含 `tier` / `layer` / `history_mode`：那些在 `domain/datasets.py`，写进数据文件的副本只会过期。

用 parquet 而非 duckdb 文件：写入是「临时文件 + 原子 rename」，读端零阻塞；duckdb 文件要独占写锁，会让 `cne serve` 和夜间跑批互相挡路。

### cne stats rebuild

| 选项 | 说明 |
|------|------|
| `--dataset` | 只重建这些数据集（可重复）；**其余数据集保留原有行**，不会被删 |
| `--if-stale` | 只在「湖动过了」时才重建，否则空转返回。放定时器上用这个 |
| `--json` | 结果输出 JSON |

全量重建：参考湖（1.5GB / 6600 万行 / 21k 分区）约 6 秒——只读 `source`、`data_version`、`fetched_at` 三列。增量刷新是可行的（跑批动过的分区可以从 `ingestion_batches.window_start/window_end` 反推），但没到需要的规模。

**`--if-stale` 的判据是 run id，不是时钟。** 改变湖的是采集，所以建于最后一个 run 之后的表无论多旧都是当前的，建于之前的无论多新都是过期的——`stats-latest.json` 的 `latest_run_id` 和 manifest 的最新 run 比对即可，只读一个小 JSON 加一行 SQLite。

并发用非阻塞锁收敛：面板请求、cron、夜间跑批同时想重建时只有一个真做，抢不到锁的直接返回而不是排队——把 web 请求堵在一次全扫后面比多看一个 run 的旧数字更糟。

`--if-stale` 判的是全湖水位，所以不能和 `--dataset` 同用，命令会直接报错而不是二选一地猜。

刷新策略（`meta/stats` 不会自己刷新）：

```bash
# 兜底：定时器上跑，没变化就是空转
cne stats rebuild --if-stale
```

面板（M2）走 `stats_freshness()` 判过期 + 后台线程调 `refresh_stats_if_stale()`；线程策略留在调用方，模块本身是同步的。

> `cne stats refresh` 已并入 `cne stats rebuild --if-stale`；原 `--force` 就是不加 `--if-stale` 的默认行为。

### cne stats show

| 选项 | 说明 |
|------|------|
| `--dataset` | 单个数据集的逐分区明细 |
| `--by-source` | 改看 source / data_version 分布 |
| `--json` | 机器可读输出 |

**无 stats 表时直扫 curated 回退**（原 `cne catalog`）：只给 dataset / files / rows，没有字节数、
源分布和逐分区明细，但一个从没跑过 `stats rebuild` 的湖不该先做一次构建才能回答「里面有什么」。
`--dataset` / `--by-source` 是 stats 表独有的视图，回退时直接报错而不是降级回答另一个问题。

> `cne catalog` 已并入本命令的回退路径；`--json` 就是它原来的输出。

---

## cne query

使用 `--dataset X --symbol CODE` 时按需抓取并读取本地缓存；追加 `--refresh` 可强制重新抓取并覆盖对应的缓存变体。`--dataset`、`--symbol` 必须成对出现，否则命令会明确报错；未指定二者时才执行 DuckDB SQL 查询。

**DuckDB 模式**（默认）：

| 选项 | 默认 |
|------|------|
| `--sql` | `SELECT COUNT(*) AS n FROM daily_bars` |

**On-demand 模式**：

| 选项 | 说明 |
|------|------|
| `--dataset` | on-demand 数据集名 |
| `--symbol` | 如 `600519.SH` |

---

## cne mcp

把这个湖接给 AI agent（MCP over stdio）。只读，和 `cne serve` 同样的边界。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径，**建议绝对路径**（客户端从哪个目录拉起进程不确定） |
| `--live` | 湖里没有的，现拉现给、不落盘。只支持 `resolve_symbol` 与未复权日线，其余工具明确拒绝 |

不用手敲：由 MCP 客户端拉起并在管道上讲 JSON-RPC。三条路按手上有什么选：

```bash
cne init --profile demo                                   # 没湖想先试试：30 秒真数据
cne mcp --config /abs/path/cnequity.toml
cne mcp --config /abs/path/cnequity.toml --live
```

上面的 `cne mcp ...` 是标准 MCP stdio server 命令，Claude 只是其中一种
客户端。Codex、Cline、Cursor、Windsurf、Gemini CLI 或其它兼容客户端，均
使用相同的 `command` / `args`；客户端的注册入口不同，但不需要改 server。

`--live` **默认关，永不自动推断**：湖坏了的用户必须拿到 `no parquet data` 去修，而不是悄悄拿到一份来自别处、看起来差不多的答案。每次调用最多 50 个标的 / 800 天，且必须显式给 `symbols`。每条响应带 `origin: "lake" | "live"`。

6 个工具（`describe_lake` / `resolve_symbol` / `query_bars` / `query_fundamentals` / `query_dataset` / `run_sql`）、口径随响应返回、`run_sql` 只收单条 SELECT：见 [MCP 参考](mcp.md)。

---

## cne sources

数据源这一面的全部：`probe` 实时探测，其余三个从已存证据算派生结论、**不联网**。

| 子命令 | 说明 |
|--------|------|
| `probe` | 探测公开数据源，报告写进湖里 |
| `slo` | 把 `meta/source_health` 历史样本按 probe/vantage 聚成可用性 SLO，并写去重事故载荷。`--window-days`（默认 30）、`--minimum-observations`（默认 10）、`--enforce`（关键源不达标退出 1） |
| `resilience` | 从注册表算源集中度、failure-domain 爆炸半径和核心数据集独立备源门禁。`--out PATH` 落 JSON，`--enforce`（有核心表缺独立备源则退出 1） |
| `policy [SOURCE]` | 查 `sources/SOURCES.yml` 的来源使用策略。省略 SOURCE 输出全部；给 SOURCE 加 `--profile personal\|commercial\|cache\|redistribution` 做保守判断，未知权限一律 fail-closed（退出 1） |

> 原为 `cne sources`（探测）+ `cne source <sub>`（派生结论）——两个顶层条目差一个字母，
> 且 `cne source --help` 不得不用一句话把自己和邻居区分开。现在收敛成一个名词。

### cne sources probe

探测本湖依赖的公开数据源。每个源发**一个**请求，断言响应体（不是状态行），串行且尊重各源限速。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径（探测不读湖，但要用里面的限速与超时） |
| `--vantage` | 这次探测从哪个出口发出：`cn` / `overseas` / 任意标签（默认 `local`） |
| `--only` | 逗号分隔的 probe key，默认全部；传空串则一个都不测 |
| `--out` | JSON 报告路径。默认写进湖里的 `meta/source_health/<vantage>.json`，也就是 `cne serve` 读的位置 |

```bash
cne sources probe --vantage cn
cne serve                    # → http://127.0.0.1:8787/source-health
```

**探测在 CLI，展示在 serve。** 面板只读，不会替你去请求十几个第三方主机——和它不触发采集是同一个理由。多次探测（不同 `--vantage`）会并排显示，不合并。

**`--vantage` 要认真填。** 好几个源在 WAF 层拒绝非大陆出口，同一主机同一秒可以大陆绿、海外红，两个都是真的。没有这个标签的报告无法解读。

**源挂了不影响退出码。** 源变红是这条命令的输出而不是它的失败。

状态五档：`ok` 可用 · `empty` 空响应 · `blocked` 被拒 · `down` 不可达 · `skipped` 未探测。`empty` 单独一档是因为它看起来比失败健康、实际更危险（回填静默截断）。

口径、加新源的方法见 [数据源健康度](../operations/source-health.md)。

### cne sources substitutes

**哪个源挂了、还有谁能顶上。** `probe` 回答"谁活着"，这条回答"活着的里面谁能替死掉的那个干活"。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径 |
| `--vantage` | 读哪个出口的报告（默认 `local`） |
| `--probe` | 现测而不是读存档；请求量与 `cne sources probe` 相同 |
| `--json` | 机器可读输出 |

```bash
cne sources substitutes         # 读 meta/source_health/local.json
cne sources substitutes --probe # 现测
```

替代源按**独立优先、其次快**排序：和故障源同属一个风控面的端点不算第二意见——东财的历史主机挂了，东财的快照主机顶不上它。某个数据集"失败的端点存在且没有任何可用端点"时退出码为 1。

输出示例（本机实测，东财 `push2his` 全部主机不可达）：

```
daily_bars  —  有独立替代
    失败：eastmoney_push2his
    可用：sina                    1970ms  独立
    可用：ths_kline               2347ms  独立
    可用：baostock                5117ms  独立
    可用：eastmoney_push2         2807ms  同域 eastmoney
```

这条命令只读报告，**采集链路不读它**：一小时前从某个出口测到的可达性不是对下一个请求的承诺，所以故障切换仍然由采集链自己逐个源试过去。

---

## cne snapshot

把选定数据集复制成不可变、带校验和的可移植快照，用于冻结可复现实验依赖的 Parquet。

| 子命令 | 说明 |
|--------|------|
| `create NAME --dataset D [--dataset D ...]` | 建快照；manifest 固化每个 Parquet 的大小/SHA-256、数据集 state、契约指纹和运行 lineage |
| `verify NAME` | 逐文件校验大小与哈希；不通过退出 1 |
| `restore NAME TARGET` | 恢复到新目录或空目录（拒绝活动湖根、不覆盖已有文件）。恢复后对 TARGET 跑 `cne status --datasets` 再切换 |
| `export NAME [DEST]` | 打成一个可移植 tar 归档。`--compression auto` 有 zstd 就用 `tar.zst`，否则退到 `tar.gz`；先写同目录 `.part`，压缩器正常收尾后才原子改名 |
| `import ARCHIVE` | 先校验后落地：逐个 tar member 拒绝绝对路径、`..`、重名、软硬链接和设备节点，解出的目录先按 manifest 全量校验，再原子改目录名发布。`--name` 覆盖快照名（默认取归档文件名），`--overwrite` 只在校验通过后才替换同名快照 |

`--config` / `--snapshot-root` 各子命令通用；默认根为 `meta/snapshots`。

### 增量包 `cne snapshot delta`

整湖快照适合冻结实验依赖；**日常同步一个已有的湖用增量包**——只搬动变化的文件，
且带足够的前置条件让"应用到错误的基线上"变成一次失败而不是一次静默污染。

| 子命令 | 说明 |
|--------|------|
| `delta create NAME --from A --to B` | 把两个**数据根**（不是 `curated` 根）逐字节比对成不可变的 add/replace/delete 包。`--to` 默认当前配置的活动湖；`--dataset` 可重复，省略时取两根共有的数据集 |
| `delta create NAME --from-revision N` | 以已提交的 revision 号作前置条件。revision receipt 记的是变更文件、不是旧湖副本，所以这一模式发的是带 `allow_missing` 的 `replace`；需要严格的旧文件哈希前置就用上面的双根模式 |
| `delta verify NAME` | 校验每个 add/replace 载荷哈希与每条变更路径的语义 |
| `delta apply NAME TARGET` | 应用到**非空**的 TARGET 湖根。`--dry-run` 只验前置条件不落盘 |

`delta-create` 是 `delta create` 的兼容别名。

apply 的安全边界值得单独说：add/replace/delete 逐条对基线指纹（revision 增量则对
各数据集 revision）核验，写入走同目录临时文件，每个被覆盖的文件在整包变更与
应用后的目标指纹都通过之前一直留着备份；中途抛异常会把已做的逐条回滚——调用方
不会观察到一个已知的半成品状态。可写路径也被收窄到 `curated/`、`derived/` 和
`meta/` 下的白名单，增量包无法借此在目标根里写任意文件。

---

## cne ths-official

对照并回填 **同花顺官方 API**（需 key）。**没有 key 时这里每个命令都报 `skipped` 且什么都不改**——湖保持它已有的源不变。

按 [ADR-0008](../adr/0008-optional-keyed-sources.md)，keyed 源永远不拥有任何一行：`ths_official` 可以做 `backup_source` / `backfill_source`、可以进 failover 和 audit 配置，但永远不是 `primary_source`。

**验证与改写是两个开关。** `[sources.ths_official].verify` 默认开，只允许写 `meta/source_snapshots` 和 findings；`[sources.ths_official].backfill` 默认关，改动 curated 之前必须显式打开。持有凭证、启用源、允许它改数据，是三个决定而不是一个。

| 子命令 | 说明 |
|--------|------|
| `capture` | 抓对手源快照，喂给 `cne audit` 里的仲裁检查。**从不写 curated 行**。`--what corporate-actions\|daily-bars\|financials\|valuations\|all`（默认 all）、`--days`（bar 窗口，默认 45 天）、`--sample`（bar 抽样标的数，默认 400） |
| `backfill` | 补 2016–2024 的资产负债表与现金流缺口。需 `backfill = true`。`--start` / `--end`（默认 2016-01-01 ~ 2024-12-31）、`--chunk-size`（默认 200）、`--workers`（默认 4） |
| `repair-bars` | 把深度历史从无凭证爬取换到授权 API。**默认只报告，`--apply` 才写**。`--adjudicator FILE` 传入独立源的 (symbol, trade_date, close) parquet、`--diff-out FILE` 落有争议行、`--start` / `--end`（默认 2005-01-01 ~ 2015-12-31） |
| `resource-sectors` | 把 `sector_bars` 从爬取换到授权端点。同样 `--apply` 才写，且需 `backfill = true`。`--start`（服务底 2022-01-04）、`--end`、`--workers` |

**`capture` 不是可选的。** `cne audit` 里的 `adj_factor_arbitration` 与 `daily_bars_arbitration` 都读快照库，没跑过它这两个检查就**永久沉默**——最需要第二意见的湖恰恰一个都得不到。日更的 `finalize` wave 里有 `ths_official_snapshot` step，无 key 时自行跳过。

**`repair-bars` 和 `resource-sectors` 是"换源"而非"路由"**，所以按 ADR-0005 要求显式命令 + 默认 dry run。`repair-bars` 还应该配 `--adjudicator`：实测对照 baostock 的 1,418 条争议，对手源对 1,167 次、原有源对 251 次，没有三方分歧——盲目切换等于引入 251 个已知回归。仲裁源必须与两边都没有血缘关系（这里两个候选都是同花顺的，不能互相仲裁）。

`backfill` 与 `resource-sectors` 只 stage 行，之后要跑 `cne run compact --run-id <id>`。

集成背景与实测数据见 [同花顺官方 API 集成](../development/ths-official-integration.md)。

---

## cne verify --runs

（`cne verify` 的第三种模式，选项见上；原 `cne stability`。）

从权威 `trading_calendar` 取最近窗口，按 logical trade date 选最新 `daily:core` attempt，验证连续交易日运行证据。

| 选项 | 说明 |
|------|------|
| `--days` | 需连续通过的交易日数（默认 20） |
| `--as-of` | 含当日的 `YYYY-MM-DD` 截止 |
| `--enforce` | 门禁未过则退出 1 |

缺 run、核心 stage 失败、或只有旧 `warning` 且无 dataset receipt 都算失败。报告写
`meta/stability/latest.json` 与不可变历史目录。日更脚本每天跑但不 `--enforce`；
release 治理在第 20 天 enforce。

---

## cne --version

包版本号。

---

## 退出码汇总

| 码 | 场景 |
|----|------|
| 0 | 成功、非交易日跳过、健康检查通过 |
| 1 | 核心运行失败、UNHEALTHY、STALE、校验失败、门禁未过 |
| 2 | run 可用但降级（`degraded`）——核心 spine 正常，研究/建议层有失败（`run daily` / `run daily --stale-only` / `init` / `retry` / `status`） |

---

## 相关文档

- [快速开始](../getting-started/quickstart.md)
- [cli 模块](../modules/cli.md)
