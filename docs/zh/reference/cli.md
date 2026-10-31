# CLI 参考

参数与默认值由 Click 自动生成在[参数参考](cli-options.md)；所有命令的联网/写入边界见[完整副作用清单](cli-surface.md)；源选择和请求成本见[取数与源保护](../operations/fetch-policy.md)。

通用约定（`--config` 解析顺序、大小写、日志、显示语言、失败记录）集中在[通用约定](#通用约定)。

## 命令一览

顶层入口分五节——顺序就是一个湖被使用的顺序，和 `cne --help` 的分节一致。
分节定义在 `cnequity/cli/_root.py` 的 `SECTIONS`，漏掉任何一个命令都会让测试失败。

### 开始使用

| 命令 | 作用 |
|------|------|
| [`cne config`](#cne-config-create) | 生成、升级、校验或 diff 配置（`create` / `upgrade` / `validate` / `diff`） |
| [`cne doctor`](#cne-doctor) | 查环境、可选依赖与配置的静默故障；无配置无网络也能跑 |
| [`cne init`](#cne-init) | 建湖并跑 init phases。`--profile demo\|sample\|quick\|full` |

### 跑 pipeline

| 命令 | 作用 |
|------|------|
| [`cne run daily`](#cne-run-daily) | 跑一天的更新：全部日更调度组，再跑事件流 |
| [`cne run maintain`](#cne-run-maintain) | 日更后的维护：审计、门禁、源探针、运行证据、元数据备份 |
| [`cne schedule`](#cne-schedule) | 安装、查看、移除当前用户的定时任务 |
| [`cne run events`](#cne-run-events) | 7×24 事件流（公告、资讯），走自然日历而非交易日历 |
| [`cne run retry`](#cne-run-retry) | 重试一个 run，或每个 daily 分组最新的失败 run |
| [`cne run compact`](#cne-run-compact) | 把已结束、未发布的 staging 合进 curated |
| [`cne run clean`](#cne-run-clean) | 预览过期 staging、快照、日志与历史版本；交互终端可接着确认删除 |
| [`cne storage`](#cne-storage) | 版本保留登记、清理计划与原地待删除标记 |
| [`cne backfill`](#cne-backfill-dataset) | 回填一个数据集 |
| [`cne derive`](#cne-derive-name) | 派生计算数据集 |
| [`cne import shfe-archive`](#cne-import-shfe-archive) | 离线导入上期所官方年度包 |
| [`cne migrate`](#cne-migrate) | 预览或运行这个湖还欠的一次性迁移，并记录结果 |
| [`cne repair`](#cne-repair) | 离线修复已存数据的布局或字段口径；默认只预览，`--apply` 发布新版本 |

`compact` 已经是每个 schedule group 自带的 step，所以 `retry` / `compact` / `clean`
是**故障后的手动出口**，不属于正常的一天。

### 检查湖

| 命令 | 作用 |
|------|------|
| [`cne check`](#cne-check) | **这个湖能不能用**：新鲜度与覆盖、数据质量、规模一条命令验收 |
| [`cne status`](#cne-status) | 最近 run 状态；`--datasets` 看逐数据集新鲜度 |
| [`cne verify`](#cne-verify) | **该落的有没有落**。默认按数据集×交易日，`--bars` 按证券×会话，[`--runs`](#cne-verify---runs) 按连续交易日运行证据 |
| [`cne audit`](#cne-audit) | **落下来的对不对**；`--full` 给全湖健康快照 |
| [`cne decision-data payment-gaps`](#cne-decision-data-payment-gaps) | 按修订冻结现金分红到账日缺口，按可交易期与除权日规则分类 |
| [`cne decision-data stock-terms`](#cne-decision-data-stock-terms) | 按修订冻结同日送股与转增并存的待审事件 |
| [`cne decision-data cash-rights`](#cne-decision-data-cash-rights) | 导出经发行人原公告核实的持有人类别现金权利，并与当前修订逐项核对 |

`audit` 与 `verify` 问的不是同一件事——一个问正确性，一个问完整性。

`decision-data` 三个命令只读数据湖，按已提交修订冻结输入；可将结果写入输出目录，文件以内容 SHA-256 命名，重复运行不会覆盖原清单。
配置了 [`[research] holdout_start`](../getting-started/configuration.md#research) 时，触及该日期的窗口在读湖前就被拒绝。

### 消费湖

| 命令 | 作用 |
|------|------|
| [`cne query`](#cne-query) | 跑 DuckDB SQL，或按需拉取数据集 |
| [`cne serve`](#cne-serve) | 湖面板：浏览、操作页取数、网页确认清理（默认 `127.0.0.1:8787`） |
| [`cne mcp`](#cne-mcp) | 以 MCP（stdio 或 HTTP）把湖提供给 AI agent |

### 治理与检视

| 命令 | 作用 | 子命令 |
|------|------|--------|
| [`cne snapshot`](#cne-snapshot) | 可移植快照的建立、校验与安全恢复 | `create` `verify` `restore` `export` `import` · `delta {create,verify,apply}` |
| [`cne contract`](#cne-contract) | 检视与校验已注册的数据契约 | `show` `diff` `validate` |
| [`cne profile`](#cne-profile) | 检视版本化的研究 universe 画像 | `list` `show` |
| [`cne stats`](#cne-stats) | `meta/stats` 下的度量表（行数、字节、源分布） | `rebuild` `show` |
| [`cne sources`](#cne-sources) | 探测依赖的数据源并检查证据 | `probe`（联网）`limits` `slo` `resilience` `policy` `substitutes` |
| [`cne delisted`](#cne-delisted) | 读取退市目录 | `status` |
| [`cne ths-official`](#cne-ths-official) | 对照/回填同花顺官方 API（需 key） | `capture` `backfill` `repair-bars` `resource-sectors` |

## 改名对照

输入旧名时 CLI 会直接给出新写法，不是 Click 默认的 "No such command"：

```
$ cne retry
Error: `cne retry` 已改名，请改用 `cne run retry`。
```

| 旧 | 新 | 为什么 |
|----|----|--------|
| `cne demo` / `cne demo --sample` | `cne init --profile demo` / `--profile sample` | 建多大的湖是**一根轴**：demo / sample / quick / full。让第一次上手的人先在两个命令之间做选择，是多余的一次分叉 |
| `cne config init` | `cne config create` | 和 `cne init` 只差一个词，而后者会建整个湖，误敲的代价大得多 |
| `cne retry` / `cne compact` / `cne clean` | `cne run retry` / `run compact` / `run clean` | 它们只作用于 run，放在 `run` 下面才是会去找的地方 |
| `cne verify-bars` | `cne verify --bars` | 和 `cne verify` 问的是同一件事，只是粒度不同；两个顶层命令差一个连字符 |
| `cne stability` | `cne verify --runs` | 同上，第三种粒度：交易日 × run |
| `cne ths-official snapshot` | `cne ths-official capture` | 原来和顶层 `cne snapshot`（湖快照）同名不同义 |
| `cne delisted backfill --since DATE` | `cne backfill daily_bars --profile delisted --start DATE` | 退市行情使用同一个数据集补数入口，保留旧写法的迁移提示 |
| `cne servers test` | `cne sources probe --only tdx_protocol` | 早已标记废弃，声明 0.9.0 删除却一直留到 0.10 |

顶层对照定义在 `cnequity/cli/_root.py` 的 `MOVED`，分组内旧名由 `moved_hints` 提示。提示不执行旧命令。

## 通用约定

命令：`cne`（`cnequity.cli.main:cli`）

支持配置的叶命令接受 `--config PATH`；解析顺序为显式 `--config` → `CNE_CONFIG` → 当前目录的 `configs/cnequity.toml` → 用户级 `~/.config/cnequity/config.toml`（遵循 `XDG_CONFIG_HOME`；Windows 为 `%APPDATA%\cnequity\config.toml`）；`cne config path` 显示实际选中的配置和原因。例如 `cne status --config /abs/path/to/cnequity.toml`。

**命令名不区分大小写**：`cne STATUS`、`cne run DAILY`、`cne config CREATE` 与小写等价（Click 的 `token_normalize_func`）。`-h` 是 `--help` 的短写法。**不做前缀匹配**——`cne stat` 会被拒绝并提示 `stats` / `status`，而不是猜一个执行。

**每个命令都有过程日志。** 管线自己的 INFO 记录统一在命令树根部接到 stderr，所以 stdout 上的 JSON 契约不受影响。耗时的采集和维护命令另外把日志 tee 进 `{data.root}/logs/cne-<命令>-<时间戳>.log`（`cne run clean --log-retention-days` 可预览过期日志）。`cne mcp` 与 `cne serve` 自管日志——前者 stdout 是 JSON-RPC 线路，stderr 必须压在 WARNING；后者交给 uvicorn。交互终端上显示的是简短视图：只留时分秒，警告和错误前加标记，不带日志器名，步骤、心跳和进度按显示语言输出；输出被重定向（管道、调度器捕获、CI）时仍是完整格式，日志文件始终是完整的英文格式，方便检索和贴进 issue。

**显示语言。** 帮助、提示、报错和结束摘要支持中文和英文，依次取环境变量 `CNE_LANG`（`en` 或 `zh_CN`，也接受 `zh-CN` 等写法）、系统语言（`LC_ALL` / `LC_MESSAGES` / `LANG`，Windows 读界面语言），都没有时用英文；繁体中文系统落到英文。系统是英文、想看中文时：`export CNE_LANG=zh_CN`（PowerShell：`$env:CNE_LANG = "zh_CN"`）。不随语言变化的：`--json` 等 JSON 输出（其中的说明文字一律英文）、`HEALTHY` / `STALE` 等标记词、退出码、数据文件和日志文件。`cne serve` 控制台按页面所选语言显示，没选过时跟随浏览器；定时任务沿用安装时的语言。

**任何失败都会留下一条日志记录。** Click 只往 stderr 打一行 `Error:` 就结束，定时任务读的是日志文件而不是终端——失败若没成为日志记录，留下的就是一个最后一行停在半路的文件。记录写在命令树的唯一入口，所有已注册命令共用该入口，且每次失败只有一条：用法错误（参数写错、名字不认识）记 `WARNING`，其余记 `ERROR`，未预期的异常还带 traceback。退出码与 Click 的渲染都不变。

**六个命令不接受 `--config`**：`cne contract show|validate|diff`、`cne profile list|show`、`cne sources policy`。它们读的是随包发布的注册表而不是湖，所以指向哪个湖都给同一个答案。`cne doctor` 则相反——它接受 `--config`，但没有配置也能跑（这正是它存在的场景）。

## cne init --profile demo | sample

小范围体验，是 `cne init` 的两档 profile（原 `cne demo`）：`demo` 拉少量流动性股票的真源近期日线，`sample` 在完全离线时生成明确标记为 `source=mock` 的合成小湖。两者都**不是**全市场——那是 `--profile quick|full`，见 [cne init](#cne-init)。

下列选项只对这两档生效；`--config` / `--resume` / `--layout-only` / `--since` 只对 `quick|full` 生效。用错一侧会被按名字拒绝。

| 选项 | 说明 |
|------|------|
| `--symbols` | 逗号分隔标的（默认茅台/平安银行/五粮液/宁德/中国平安） |
| `--days` | 约多少个交易日的 `daily_bars`（默认 30） |
| `--intraday` | 额外抓同一批标的的 1m 线（最多约 5 个交易日），打印一根完整会话 |
| `--research` | 额外从 Sina 派生 hfq 因子，并打印 raw / hfq 收益对照；会把窗口扩展到约 3 年 |
| `--data-root` | 独立湖根目录（默认 `data/cnequity-demo`；sample 为 `data/cnequity-sample`） |
| `--profile sample` | 不访问网络，生成可用于验证安装、查询和 DuckDB 视图的合成样例；不可与 `--research` / `--intraday` 合用 |
| `--trade-date` | 截至日 YYYY-MM-DD（默认今天 / 最近交易日） |
| `--config-out` | 写出供后续 `cne query` 使用的小配置（默认 `configs/cnequity.demo.toml`；sample 为 `configs/cnequity.sample.toml`） |
| `--force` | 允许覆盖内容不同的已有 `--config-out`；默认保留原文件并报错，内容相同可直接重跑 |

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

## cne init

初始化数据湖并执行 init phases。quick/full 自动恢复配置市场与历史窗口内的已知退市日线，获取当前交易状态，并在日线发布后派生历史停牌。全市场历史 ST 扫描使用独立的 `cne backfill trading_status`，不作为 init 完成条件。

找不到配置时，`cne init` 先按随包示例生成用户级配置（同 `cne config create`）；显式 `--config` 或 `CNE_CONFIG` 指向的缺失文件不会被生成。中断、部分覆盖或旧版未完成的 init，重跑同一条 `cne init` 即自动续跑，已抓取数据保留。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径；默认路径缺失时自动生成 |
| `--layout-only` | 仅建目录、manifest、DuckDB 视图 |
| `--trade-date YYYY-MM-DD` | init 截至交易日（默认今天） |
| `--resume` | 续跑最近未完成 init |
| `--run-id` | 续跑指定 init run（隐含 resume） |
| `--keep-going` | 任一 phase 失败后都继续后续 phase（公司行为、指数与交易状态阶段，以及日线入湖后的派生和审计默认就会继续） |
| `--json` | 结束时输出完整的机器可读结果，而不是摘要 |
| `--no-preflight` | 跳过开跑前的网络预检；也可设 `CNE_SKIP_PREFLIGHT=1` |
| `--profile demo\|sample\|quick\|full` | 建多大的湖。`quick`（默认）= 全市场标的、最近 3 年；`full` = 全市场、各 step 自己的起点（`daily_bars` 为 2016-01-01），请求和落盘量更大；`demo` / `sample` 是几只票的小湖，见 [上一节](#cne-init---profile-demo--sample) |
| `--since YYYY-MM-DD` | 显式指定历史起点，覆盖 `--profile` |
| `--quiet` | 只留 warning 及以上，不打逐批进度 |

**默认会打进度。** 全市场回填可能运行较久；每批会报告已完成数、行数、耗时
和估计剩余时间。下列数值仅演示输出格式：

```
14:22:07 INFO ...worker_pool: daily_bars 1/2 batches · 240 rows · 1m24s elapsed · ~1m24s left
```

**`quick` 是更浅，不是更窄。** 全市场标的一个不少，只是每只少几年。按标的裁剪会把这个湖本来要修掉的幸存者偏差直接建进去，而且一个缺席的标的看起来和「这只票从没交易过」一模一样；少几年的历史则由 `coverage_start` 如实记录。

窗口会写进 run metadata，`--resume` 自动沿用——否则几天后从新进程续跑会默认回到全深度，去抓你当初特意跳过的年份。

之后加深不必重跑 init：

```bash
cne backfill daily_bars --start 2016-01-01 --end COVERAGE_START
```

上表中 `--config` / `--layout-only` / `--resume` / `--run-id` / `--keep-going` / `--since` 只对 `quick|full` 生效；`--symbols` / `--days` / `--data-root` / `--config-out` / `--intraday` / `--research` 只对 `demo|sample` 生效。传错一侧会被按名字拒绝，不会被忽略。

**公司行为、指数日线和交易状态阶段失败不阻断建湖。** 这些阶段出错时，init 照常抓日线、入湖并派生复权因子，结束时列出没完成的步骤；重跑同一条 `cne init` 只补这些。证券目录、交易日历或日线失败仍会停在原处，已下载的部分保留，恢复后重跑即续跑。

**结束时打印摘要。** 终端给出湖是否可用、还差哪些步骤、日志位置和下一步命令（`cne check`、`cne serve`、Python 读取、MCP 接入）。完整结果存到 `{data.root}/logs/cne-init-<run_id>.json`；需要在 stdout 上拿到它时加 `--json`。

退出：成功、来源受限（`degraded` / `warning`，包括本次 0 行），以及只差上述不阻断步骤、日线已入湖的情况返回 0；其他执行错误返回 1。

## cne config create

写出继承内置默认值的简短用户配置（`extends = "builtin"` 加 `[data].root`），见[继承内置默认值](../getting-started/configuration.md#extends-builtin)。

| 选项 | 说明 |
|------|------|
| `--config` | 输出路径（默认：当前目录已有 `configs/cnequity.toml` 时用它，否则用户级 `~/.config/cnequity/config.toml`） |
| `--data-root` | 写入 `[data].root` |
| `--force` | 覆盖已存在文件 |

平台相关的默认值（如 macOS 上 `orchestrator.workers = 1`）由内置默认值提供，不写进文件。默认值来源：`cnequity.config.templates`（与仓库 `configs/cnequity.example.toml` 保持同步）。

## cne config show

打印生效的完整配置（TOML），凭证打码；继承默认值的配置会标出哪些值来自文件、哪些组被 `steps_add` / `steps_remove` 修改过。`--defaults` 打印全部设置及其默认值和说明。

## cne config validate

校验 TOML 与 step 引用。有错退出 1。

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

调度组里缺少的 step 单独报出：示例配置的同名组有、而你的任何一个组都没有的 step，不带参数的 `cne run daily` 不会跑，即使某个 wave 里还列着它。

`[data].root` 和 `orchestrator.workers` 等本机相关取值不算漂移；配置里多出来的自定义内容也不报。继承默认值的配置不会缺键，只有整组替换的 `steps = [...]` 遮住新版本默认 step 时才报；`steps_remove` 和 `enabled = false` 是有意的退出，不算漂移。

## cne config upgrade

升级版本后运行这一条，把当前版本新增的调度 step 和调度组补进你的配置：

```bash
pip install --upgrade cnequity
cne config upgrade
```

- 示例配置同名组里有、你的任何组都没有的 step，追加到你的同名组；
- 你没有的日更 / 事件流调度组，连同注释整段追加到文件末尾；
- 只在示例 wave 里的 step，追加到你的同名 wave。

改动前把原文件备份为 `cnequity.toml.bak-时间戳`，写完后校验配置。其余设置（时间、并发、凭证、开关）不动；缺少的配置项本来就用内置默认值，不写入文件，以免把当前默认值固定下来。`--dry-run` 只列出改动。内联表或点号键写法的组无法自动编辑，命令会列出需要手动加入的 step 并返回 1。

继承默认值的配置升级后不需要这一步。`--minimize` 把完整配置改写成继承默认值的形式（规则见[继承内置默认值](../getting-started/configuration.md#extends-builtin)）：先列出保留的设置、调度组的 `steps_add` / `steps_remove`、被设为 `enabled = false` 的组，以及加载后行为会变化的设置；与 `--dry-run` 同用只预览。

## cne sources resilience

按失败域展示来源集中度、爆炸半径与独立备源门禁。

| 选项 | 说明 |
|------|------|
| `--with-availability` | 把这个湖已积累的探针历史按失败域 join 上去（读湖，需 `--config`） |
| `--window-days` | 可用率统计窗口（默认 30 天） |
| `--enforce` | 关键数据集缺独立备源时退出 1 |
| `--out PATH` | 写文件而非打印 |

集中度本身不能决定主备源的选择。`--with-availability` 将本湖已保存的探针样本
与故障域放在同一报告中；每个域展示所依赖探针中的最低可用率。没有观测的探针
不会被当作成功或失败，对应的域保持未标注。具体数值取决于自己的出口和观察窗口。

## cne contract

查看和维护全部注册数据集的机器可读 JSON 契约。

| 子命令 | 说明 |
|--------|------|
| `show [DATASET]` | 输出一个数据集或完整 registry 契约；`--out PATH` 写文件而非打印 |
| `validate [PATH]` | 校验文件；省略 PATH 时校验当前 registry。文件加 `--against-registry` 做精确同步检查 |
| `diff OLD [NEW]` | 比较两个契约；省略 NEW 时比较当前 registry。默认发现 breaking 时退出 1，检查报告可加 `--allow-breaking` |

diff 会把删列、改类型、改主键、单位/PIT/历史语义变化识别为 breaking；新增
列和新增数据集为 compatible。

> `cne contract export` 已并入 `cne contract show --out`——两者本来就是同一份文档，
> 只差写不写文件。

## cne profile

查看版本化的研究 universe 画像（`cnequity.domain.universe_profiles` 注册表）。

| 子命令 | 说明 |
|--------|------|
| `list` | 输出注册表记录。默认 `--include-compatibility`，含 legacy 兼容画像；`--official-only` 把它们排除 |
| `show NAME` | 输出单个画像及其 `scope_hash`；`--symbol` 可重复，绑定具体标的并附 `concrete_scope_hash` |

画像绑定交易所/板块、CDR/ETF、ST/停牌、退市与 PIT 证据规则。研究读取用
`load(..., profile="cn_a_sh_sz_research_v1")`，并把 `name` / `version` / `scope_hash`
一起记进产出。详见 [universe 画像](universe-profiles.md)。

## cne doctor

环境与配置体检：`data.root` 是否绝对路径 / 可写、声明的依赖能否 import。不访问网络；无配置也能跑（新鲜安装）。有实质性风险时退出 1。

| 选项 | 说明 |
|------|------|
| `--json` | 机器可读输出 |

`cne doctor --fix` 已移除（只服务于已卸掉的 mini-racer 冲突修复）。

## cne run daily

| 选项 | 说明 |
|------|------|
| `--group` | 只跑一个组：`core` \| `capital` \| `signals` \| `fundamentals` \| `macro_risk` \| `research` \| `intraday` |
| `--no-events` | 不带参数运行时不跑事件流 |
| `--core-only` | 只跑 `[[job.daily.waves]]` 核心骨架（旧版不带参数时的行为） |
| `--all-groups` | 只跑全部调度组、不含事件流；给已单独调度 `cne run events` 的旧定时任务保留 |
| `--backfill` | 强制 backfill 语义（慎用） |
| `--repair-gaps` | 在 daily/stale 跑之前，先修复已验证、且有诚实来源的历史缺口 |
| `--stale-only` | 只重抓仍落后于最后交易日的数据集（与 `--group` 互斥） |
| `--quiet` | 只留 warning 及以上，不打逐步进度 |

### --stale-only：当天的第二次机会

快照型日更只抓运行当天。错过当天窗口后，不能通过重放旧 `--trade-date` 把新观察
伪装成历史快照；能否从独立历史源补回，要看该数据集的 `history_mode` 与
`backfill_source`。`--stale-only` 给当天尚未补齐的数据第二个采集窗口。

把它挂在主 pipeline 几小时之后：

```cron
# 主 pipeline
5 16 * * 1-5 cne run daily && cne run maintain

# 收尾补抓：只跑仍然落后的，没有就空转
5 20 * * 1-5 cd /path/to/cnequity && cne run daily --stale-only
```

新鲜度判据与 `cne status --datasets` 完全一致（含每数据集的 `max_staleness_days` 容忍），所以两者不会各说各话。没有落后的数据集时不建 run、直接退出 0，可以安全挂在定时器上。

派生数据集不在其中：它们由 curated 重算，该跑的是 `cne derive`，不是重抓。

**不带参数就是完整的一天。** 按配置顺序串行跑完 `[job.daily.groups]` 的每个组，对整个湖跑一次 `audit`（它读全湖，所以放在所有组落盘之后；没有组实际运行时跳过），再跑 `[job.events.groups]` 事件流：一个组失败不中断后面的组，退出码取最差的一个，数据集全部关闭的组自动跳过。非交易日调度组自动跳过，事件流照常运行，所以一条定时任务每天跑一次即可；`cne schedule install` 安装的定时任务在它之后再跑 `cne run maintain`（审计、门禁与元数据备份）。事件流的锁已被单独的 `cne run events` 持有时，这一段记为 `skipped_locked`，不算失败；`--backfill` 补跑某个交易日时不跑事件流。配置比当前版本少了调度 step 时，会提示运行 `cne config upgrade`。

`--core-only` 跑 `[[job.daily.waves]]` DAG —— 只有核心骨架；配置里没有调度组时不带参数也走这条路，没有 waves 时直接报错，不会假装成功。`intraday` 组需先开 `[minute_bars].enabled`，它的数据集关闭时整组自动跳过。

成功、有效部分结果或 `skipped_non_trading_day` 退出 0；执行失败返回 1。

## cne run maintain

日更之后的维护，`cne schedule install` 安装的定时任务每个交易日在日更后运行一次：运行审计（`[job.maintenance] full_audit_weekday` 那天改为全湖审计）、新鲜度门禁、源探针与 SLO、最近 20 个交易日的运行证据、元数据备份（manifest 数据库、收据、质量结论，不含 curated Parquet，按 `[job.maintenance]` 的保留期轮换），以及 `cne run clean` 的预览。定时任务没有终端，不会进入删除确认。

| 选项 | 说明 |
|------|------|
| `--groups` | 新鲜度门禁判失败的调度组（空格或逗号分隔）；默认是定时日更实际跑的组 |
| `--notify` | 有问题时弹出桌面通知（macOS） |

审计、新鲜度门禁或备份失败时退出 1；源探针和运行证据的问题只报告。各步骤说明见[运行手册](../operations/runbook.md#maintain)。

## cne schedule

管理当前用户的定时任务（macOS launchd、Linux crontab、Windows 任务计划程序），与 `cne serve` 操作页是同一个任务。每分钟检查一次；每个交易日过了 `[job.daily] run_at`（北京时间）跑 `cne run daily`，然后 `cne run maintain`；过了 `[job.stale] run_at` 重试缺的快照。

| 子命令 | 说明 |
|------|------|
| `install` | 确认后注册或更新定时任务。`--pack` 只更新所选研究包的调度组；`--no-stale` 不装收尾补抓；`--events` 按配置的 `[[job.events.schedules]]` 跑事件流；配置没写计划时，`--events-group` / `--events-every` 另按间隔跑一个事件组；没给的选项保留现有设置；`--yes` 跳过确认 |
| `status` | 是否已安装、是否在运行、跑什么、最近一次检查；`--json` 输出机器可读结果；检测到旧的 `scripts/scheduler` launchd 任务时提示 |
| `remove` | 确认后停用并注销系统任务；正在运行的任务继续 |

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
0 14 * * *  cne run events --config /abs/path/to/cnequity.toml

# 只要资讯的日内新鲜度：单张实时页，代价很低
*/30 9-22 * * *  cne run events --group news_wire --config /abs/path/to/cnequity.toml
```

`disclosures` 组每次都会重读 30 天对账尾窗，高频跑它是在重复付这份代价；
`news_wire` 没有尾窗，适合高频。组按配置顺序执行，所以 `regulatory` 能读到
`disclosures` 刚发布的公告。

## cne backfill DATASET

所有可回填数据集支持 `--plan`：显示范围、登记源与生效开关、冷却、修复模式、欠账和切片，不联网或写湖。显式日线证券范围给冷缓存最低请求数；衍生品计划另给估算，其余未知请求数保留 `null`。显式 `--symbols` 不能为空，重复代码会去重。


单数据集 backfill。snapshot 且无 `backfill_source` 时正常结束并报告能力限制，保留已有快照，给出后续日更采集方案。历史请求超过来源的已知边界时获取仍可提供的范围，并分别记录请求范围与实际范围。

取数后自动 compact 当前 run 中经过封存校验的有效结果，部分来源失败不阻止独立事实发布。

**下游派生跟着输入走，不等第二天的日更。** 回填改变了哪些输入，就当场重算依赖它们的派生数据并核对：

| 回填 | 自动跟进 |
|---|---|
| `corporate_actions` | 对比回填前后已发布的公司行为，只取影响复权的条款（现金分红、送转、拆并、配股、参考价）有增删改的证券，重算它们的复权因子，再核对每只证券的因子覆盖到最新成交日线。只改到账日不触发重算 |
| `daily_bars`（含 `--profile delisted`） | 对比本次窗口内回填前后的日线（按证券×月份），对有变化的证券用已缓存的因子重新对齐复权因子（不重新抓取）并核对覆盖；再从第一个变化月份往前 90 天到最后一个变化月份，从日线缺口重建停牌。日线没有变化时两者都跳过 |

```
公司行为回填完成
  写入：            1,234 条
  受影响标的：      87

派生复权因子…
  处理：            87
  更新：            84
  跳过：            3（无日线 2, CDR 1）

✓ 公司行为与复权因子已同步
```

结果 JSON 另有 `adj_factors_sync` / `trading_status_derive` 字段。没有跟上的证券（抓取失败或未覆盖最新日线）会被列出，命令以 `degraded` 结束并给出重跑命令；下游派生出错同样记为 `degraded`，已发布的回填结果照常报告。

| 选项 | 说明 |
|------|------|
| `--start` / `--end` | 请求窗口；超出来源历史边界的部分报告覆盖不足，可用范围仍会获取 |
| `--symbols` | 日内、`daily_bars`、`trading_status`、`corporate_actions`、`share_structure` 的临时标的范围（`share_structure` 按证券一次取回全部股本变动，用于审计提示的股本滞后）；其他数据集仍使用配置中的范围 |
| `--outstanding` | 精确修复被容忍缺口记下的欠账键：作用域与窗口取自 ledger，而不是 `--symbols`/`--start`/`--end`。补上的键即刻销账，仍缺的继续欠着 |
| `--refetch` | 忽略完成收据和 checkpoint，重取整个范围（`sector_bars`、`futures_bars`、`option_bars`），仍遵守熔断。不加时回填从断点续跑 |
| `--source` | 本次改用的来源，不改配置；目前用于 `margin_trading`（`exchange` / `eastmoney`） |
| `--workers` | 仅 `margin_trading`：按日期推进的并发数，每个请求仍走共享限速 |
| `--exchange` | 衍生品：限定发布者，可重复 |
| `--profile delisted` | 仅 `daily_bars`：以已确认退市名录为 universe，`--start` 对应旧命令的 `--since`；`--plan` 离线 |
| `--plan` | 只读计划，不联网、不写湖 |

**数据集专属的修复和导入已移出 `backfill`。** 它们用同一套回填机制（窗口、范围、下游派生跟进、计划输出），但遵循修复命令的约定：默认只预览，`--apply` 才运行。旧开关已移除，请改用下表右列：

| 旧写法 | 新命令 |
|---|---|
| `backfill corporate_actions --payment-date-repair` / `--issuer-notice-repair` | [`cne repair payment-dates`](#cne-repair)（`--issuer-notices-only`） |
| `backfill corporate_actions --baostock-repair` / `--ths-repair` | `cne repair delisted-actions --source baostock\|ths` |
| `backfill corporate_actions --eastmoney-date-repair --ex-dates ...` | `cne repair ex-dates --ex-dates ...` |
| `backfill daily_bars --tdx-amount-repair` | `cne repair bar-amounts` |
| `backfill daily_bars --turnover-repair [--turnover-peer-review]` | `cne repair bar-turnover [--peer-review]` |
| `backfill daily_bars --bse-tip-repair --start D --end D` | `cne repair bse-tip-turnover --date D` |
| `backfill valuation_metrics --fill-em-outage` | `cne repair valuation-outage` |
| `backfill futures_bars --shfe-annual-archive ZIP --archive-year Y ...` | [`cne import shfe-archive ZIP --dataset futures_bars --year Y ...`](#cne-import-shfe-archive) |
| `--refresh`（衍生品）/ `--force`（`sector_bars`） | `--refetch` |
| `--retry-failed`（`sector_bars`） | 不再需要：回填默认续跑 |
| `--margin-source` | `--source` |

原来的 `--eastmoney-bj-repair`、`--bj-amount-repair` 和 `--tdx-volume-repair` 已移除：它们只修 0.17 及更早版本写入的数据（`--bj-amount-repair` 的替代是 `cne repair bar-amounts`）。日更不会用到这些修复，新数据由写入时的规则保证：

| 以前靠修复开关处理的问题 | 现在的处理 |
|---|---|
| BJ 当期成交额缺失 | 日更以北交所行情板为当期主源；交易所阶段缺成交额的行在合并前报 warning |
| 某个交易日缺一批 BJ 证券 | 合并前的完整性规则把该日记入缺口台账，并在下次运行时重取 |
| 无事件的超限涨跌 | 进入隔离区，保留已提交值，并记为待重取 |
| 新浪漏记的 BJ 复权台阶 | 因子由公司行为计算，`pre_close` 核对台阶，新浪只作对照 |

```bash
cne backfill minute_bars_5m --start 2026-05-01 --end 2026-07-31 \
  --symbols 600519.SH,000001.SZ

# 已有 BJ 日线只补当前分区成交额，不重抓 Sina 历史（先预览，再 --apply）
cne repair bse-tip-turnover --date 2026-08-21 --symbols 920000.BJ,920001.BJ --apply
```

### 衍生品回填

| 选项 | 说明 |
|------|------|
| `--plan` | 只读计划：来源、日期、冷缓存请求估算、限速与后续步骤；不联网、不写湖 |
| `--exchange` | 限定发布者，可重复；INE 归入 SHF 路由，2018 年期货另取能源中心日文件 |
| `--refetch` | futures_bars / option_bars 忽略收据与旧缓存重取，仍遵守熔断 |
| `--symbols` | futures_minute_bars 的显式合约列表，如 CU2611.SHF；日线不支持逐合约取文件 |

日线回填自动更新对应合约表及派生，输出 `followup`；分钟线只有滚动窗口，拒绝日期范围。连续期货全量重算，拒绝 `derive --start/--end`。上期所年度包用 [`cne import shfe-archive`](#cne-import-shfe-archive) 离线导入。完整流程、源端约束和验收见 [衍生品指南](../recipes/derivatives.md)。

衍生品合约回填的历史参考重放及请求上界见[衍生品研究指南](../recipes/derivatives.md)。合约表的 `--start/--end` 限制参考文件日期，不裁剪最终合约全集。

### sector_bars

默认从 checkpoint 续跑，跳过已完成的板块；`--refetch` 清空 checkpoint 后全量重拉。Checkpoint：`meta/state/sector_bars_backfill.json`。失败超过 50% 时 step 状态为 `warning` 但仍写入已成功部分。

**网络**：走同花顺 `d.10jqka.com.cn`（日更与历史同源），限速在 `[sources.ths]`。
与东财无关，`[sources.eastmoney] proxy` 对它不生效。

```bash
cne backfill sector_bars              # 续跑
cne backfill sector_bars --refetch    # 首次或换源后全量
```

## cne import shfe-archive

```bash
cne import shfe-archive ZIP --dataset futures_bars --year 2018 --accept-partial-fields           # 预览
cne import shfe-archive ZIP --dataset futures_bars --year 2018 --accept-partial-fields --apply   # 导入
```

离线导入上期所官方年度包：校验格式，保留缺失字段标记，跳过已有日线主键，不覆盖完整的日文件数据。`--accept-partial-fields` 必填，表示接受年度包缺少只有日文件才有的字段；`--url` / `--downloaded-at` 附上原始来源证据；`--start` / `--end` 限定在 `--year` 内。默认回填仍用完整日文件，细节见[衍生品指南](../recipes/derivatives.md#官方年度包已验证格式与使用限制)。

## cne run compact

| 选项 | 说明 |
|------|------|
| `--run-id` | 只发布这一次 run（默认处理所有待发布的 run） |

不带 `--run-id` 时，按时间顺序逐个发布所有「已结束、有暂存文件、还没有成功 compact」的 run，不用再逐个找出并指定 run_id。正在跑的 run 和 init run（由 `cne init` 续跑）不动。

将 staging 中经过封存校验的结果合并入 curated。活动批次、旧版未校验的部分 staging，以及完整性错误仍受门禁保护；失败请求保留供重试。

## cne delisted

读退市目录。退市行情使用 `cne backfill daily_bars --profile delisted` 补数。**重建**目录（扫码空间、核对终点、修 instruments、
覆盖门禁）是一次性工程，在 [`scripts/delisted_ops.py`](../operations/scripts.md#delisted_opspy)。

| 子命令 | 说明 |
|--------|------|
| `status [--since]` | 目录摘要：数量、年份、尚未 ingest |

推荐顺序（跨 CLI 和脚本）：

```bash
cne delisted status                                 # 已知多少
python scripts/delisted_ops.py discover --limit 500 # 扫码空间，可续跑
cne backfill daily_bars --profile delisted --start 2016-01-01  # 拉扫到的退市行情
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

## cne derive [name]

| name | 说明 |
|------|------|
| `adj_factors`（默认） | 计算 Sina hfq 因子 |
| `trading_status` | 派生历史停牌记录（`--start` / `--end` 按年分块重建）；`init` 和 `cne backfill daily_bars` 之后会自动按窗口运行 |
| `sector_routing` | 可选：EM 板块 × TDX 88xxxx 名称映射表（**不驱动** sector_bars 采集） |
| `sector_code_map` | BK* ↔ BOARD_CODE 身份映射（lake-only；推荐成分 join） |
| `futures_continuous` | 期货主力/次主力连续合约，按 T-1 持仓换月、只向后换；由 futures_bars 全量重建（需 `[futures] enabled`） |
| `option_greeks` | 期权隐含波动率与希腊字母（Black-76 / BAW），自动检测行情、合约、利率和模型依赖变化；`--full` 全量重算，`--start`/`--end` 限定窗口 |
| `minute_bars_15m` / `minute_bars_30m` / `minute_bars_60m` | 默认不计算。手动把 15m / 30m / 60m 算进湖：某只股票某天有 1m 用 1m，否则用 5m；缺组成 K 线的股票当天跳过并计数。不给窗口时只算还没算过或 1m / 5m 已更新的交易日；`--full` 全量重算，`--start`/`--end` 限定窗口。规则见 [15 / 30 / 60 分钟线](../recipes/minute-bars-15-30-60.md) |
| `adj_factor_source` | 用 Baostock 仲裁复权因子与公司行为的矛盾（新浪漏步、新浪虚步、湖缺事件、事件存疑）；证明新浪有误（除权日前一交易日在 10 天内、不在 2005-04-29 至 2007-12-31 股改期间，两家台阶相差超过 0.45%，且原生除权参考价支持 Baostock）且 Baostock 与其余事件一致的沪深股票，`--apply` 后整条因子改用 Baostock，证据写入 `meta/quality/evidence/`。Baostock 结果按批缓存 7 天，仅复用覆盖本次查询起止日期的完整结果；每只证券选取最新完整快照，失败请求的残片不参与仲裁。相同窗口下，中断重跑只取尚未完成的证券，预览后的 `--apply` 可复用预览数据；查询截止日推进或旧缓存缺少覆盖日期时会重新取证 |

```bash
cne derive trading_status --start 2001-01-01 --end 2001-12-31
```


## cne migrate

升级后修复旧版本写下的数据。新版本修好写入逻辑后，旧版本存下的行仍需一次性修正；每个修正是一个迁移。`meta/state/lake.json` 记录建湖的版本（早于它引入的迁移不需要运行）和这个湖已经运行过、或检查后无需运行的迁移。

```bash
cne migrate             # 预览：每个迁移会改多少行，不写任何东西
cne migrate --apply     # 按顺序运行并记录结果
```

| 选项 | 说明 |
|------|------|
| `--apply` | 运行还欠的迁移，并把结果写进 `meta/state/lake.json` |
| `--only` | 逗号分隔的迁移 id；即使已记录也重新运行 |
| `--json` | 机器可读输出 |

每个迁移都持有湖的变更锁，经修复门禁发布，旧版本保留可读。一个迁移失败时停止，后面的不运行，也不记录。`fund-listing-dates` 会读取交易所官方基金列表，`misdated-valuation` 在有需要替换的行时读取东财数据中心，其余只读湖。`cne doctor` 会提示还没检查的迁移。0.12 及更早版本写入的湖请先用 0.19 或更早版本迁移再升级。

## cne repair

默认基于湖中已有数据输出计划；加 `--apply` 后以新版本发布，旧版本继续保留，可按版本读取。`corporate-action-gaps --apply` 会按缺口向数据源补取证据。


`--apply` 发布前会比较已提交版本与候选版本的离线质量检查。历史因子/公司行为矛盾单独记录为非阻断诊断。无效因子数值、损坏文件、主键错误等结构问题新增或恶化时会阻止发布，候选版本移入 `_quarantine`，报告写入 `meta/quality/publication/`。这项修复门禁不受 `[quality].publication_gate` 的普通日更发布设置影响。

| 子命令 | 说明 |
|------|------|
| `layout DATASET` | 把分区数据集根目录或错误键目录中的文件并入登记分区。同一主键的多份观察只在非空值完全一致时互补填空；有冲突的键按规范规则整行保留一份。重复键的全部原始观察写入 `_quarantine`。发布新版本时若检测到这类布局，会提示运行此命令 |
| `repartition [DATASET] [--all]` | 按登记的分区粒度改写数据集分区；不带参数只列出需要改写的数据集，带数据集时预览文件、分区和体积变化，`--apply` 在湖锁下改写并整体换上。只回收空间和文件句柄，不修正确性；审计发现粒度混用时会指向这里 |
| `payment-dates` | `corporate_actions` 现金到账日：先用已审发行人公告，再用 Baostock 匹配；`--issuer-notices-only` 只用发行人公告并应用已审送转条款。需要 `--symbols`/`--start`/`--end` |
| `delisted-actions --source baostock\|ths` | 补已退市证券的历史分红除权：`baostock` 用于沪深，`ths` 用于北交所 |
| `ex-dates --ex-dates ...` | 按给定除权日逐日向东财请求历史除权行；只有它能取到 2015-09-29 之前的行 |
| `bar-amounts` | 新浪补上的沪深北历史日线与通达信核对，开高低收一致且成交量差小于一手时补成交额；价格与成交量不动 |
| `bar-turnover [--peer-review]` | 成交额缺失、为 0 或量额单位错位的沪深股票日线，用 Baostock 同日行整行替换；`--peer-review` 再用交易所历史价、通达信和东财复查冲突 |
| `bse-tip-turnover --date D` | 用北交所行情板补最近一个交易日的北交所成交额，不重抓新浪 |
| `valuation-outage` | 东财快照中断时，用东财数据中心估值报表补 `--start`/`--end` 窗口；只在取到全市场时写入 |
| `corporate-action-gaps` | 补上新浪与 Baostock 因子都有台阶、湖里却没有记录的除权事件，依据最近一次 `cne derive adj_factor_source` 的证据。附近 10 天内日期错开的记录，若条款能解释台阶就移到台阶日；其余按证券和年份向 Baostock 取分红，按生效交易日匹配，只收条款能解释台阶的行；Baostock 也解释不了的，再到巨潮查发行人的重整转增公告，公告写明的除权参考价能解释台阶才记为 `reorg_transfer`。此命令的计划仍离线，`--apply` 才请求 Baostock 和巨潮 |
| `status-traded --start --end` | 默认只读预览：用已提交日线的正成交量补已知上市期间、缺状态行的股票/ETF 键，来源 `derived_traded`，ST 保持 null。已有状态不覆盖；`--apply` 重新核对当前版本后发布并保留旧版本。建议按年或月分段 |
| `stock-listing-dates` | 用有成交但上市日期未知的股票首日定位巨潮 IPO 日历请求，也可用可重复的 `--event-date YYYY-MM-DD` 核验尚无日线的股票；只接受代码、日期匹配的明确上市事件，不能与更早成交矛盾。默认回放归档，`--refresh-sources` 联网采证，`--max-sessions` 分批；`--apply` 发布并保留已有日期和旧版本；若补入的官方上市日期晚于残留退市日期，同一修订清除该矛盾退市日期 |
| `index-snapshots` | 回放官方带日期的完整成分、权重文件；`--refresh-sources` 更新归档，`--indices` 选择指数。`--apply` 替换整个指数快照，百分数转比例，成分与权重按各自日期保存，不倒推历史 |
| `financial-originals` | `--symbols` 或 `--all-stocks` 选择股票，`--start-year/--end-year` 选择年份，`--quarter` 选择 Q1/Q2/Q3/Q4（默认年报），`--max-reports` 分批，`--after` 接续输出的 `next_cursor`。默认离线预览，`--refresh-sources` 归档巨潮完整查询和原始 PDF，`--apply` 追加可明确识别的合并报表当期数和累计数；`--reparse` 重放旧报告并保留未变行的首次观测时间；未识别布局或科目保留缺口，不回填采集时间 |
| `adj-factors` | 统一股票因子与 ETF/LOF 份额折算修复。默认离线预览并保存计划；`--refresh-sources` 显式联网采证，`--apply` 复查输入后发布确定的修正。支持 `--symbols`、事件窗口 `--start/--end`、`--max-symbols` 和 `--plan`；未解决事件保留原值和提示。详见 [历史因子修复](../datasets/sources.md) |
| `fund-volume` | 用上交所完整历史 K 线修复沪深场内基金零成交量、正成交额的行。`--refresh-sources` 归档来源，默认离线回放；`--apply` 仅替换上市窗口内、开高低收及舍入金额一致的行并保留旧版本。若湖中金额冲突，须有已归档的东财未复权日线同时证实价格和金额，才替换整行；东财整手成交量不参与补量。优先复用相符的原始响应归档，避免重复请求。不得用成交额倒算成交量。 后续重抓的零量正额行不会覆盖已恢复的有效成交量。 |
| `fund-no-print` | 用深交所明确的零成交额，或上交所按证券和日期返回的零竞价开高低及正参考价，核对日线欠账。默认离线预览；`--refresh-sources` 联网归档报表，`--max-sessions` 限制交易日数；`--apply` 保存证明并刷新覆盖水位。缺失响应不构成证明，正常可交易不等于当日有成交；舍入量额为零本身不足以证明无成交；上交所覆盖证明表示未形成竞价日线，不能据此否认汇总口径成交，不生成日线或停牌行。证据失效会重新打开欠账，实际成交记录优先 |

```bash
cne repair layout futures_bars
cne repair layout futures_bars --apply
```

## cne audit

| 选项 | 说明 |
|------|------|
| `--run-id` | 指定 run 的 findings（默认最近 run）；可重复指定，合并这些运行的步骤证据并执行一次湖审计 |
| `--full` | 湖级健康快照（非 per-run 文件） |
| `--quality-only` | 配合 `--full`：只对质量 error 设门禁；调度新鲜度另用 `cne status` 单独查 |
| `--research-start YYYY-MM-DD` | 与 `--full` 合用；严格验证所选历史宇宙，未通过时退出 1 |
| `--research-end YYYY-MM-DD` | 研究窗口末日；默认取 `daily_bars` 最新分区 |
| `--research-universe all_a\|all_a_sh_sz` | 历史研究口径；默认 `all_a`，`all_a_sh_sz` 排除 BJ 的来源能力缺口 |

`--full` 且 UNHEALTHY 退出 1。显式传 `--research-start` 后，研究宇宙未通过也退出 1；此时末行会显示 `HEALTHY (operational; research BLOCKED)`，表示湖的运营健康与研究可用性是两个独立门禁。未显式传 `--research-start` 时，历史宇宙状态仍写入 health 与 `historical-validity-latest.json`，但不会改变运维健康的退出码。快照同时记录 `historical_universe`，避免把 scoped 结果误读成全 A。

## cne verify

四种粒度检查「该落的有没有落」。默认按数据集 × 交易日；`--bars` 按证券 ×
交易日；`--runs` 按交易日 × run；`--derivatives` 检查衍生品研究窗口。模式互斥，不适用的选项会报错。

| 选项 | 模式 | 说明 |
|------|------|------|
| `--dataset` | 默认 / `--derivatives` | 只查这些数据集（逗号分隔）；默认全部已注册数据集 |
| `--repair` | 默认 | 对可修复的缺口跑回填，按数据集从新到旧；修完自动复查，仍有缺口时退出 1 |
| `--kind` | 默认 | 只看这些缺口类型：`empty,stale,interior,shallow` |
| `--bars` | — | 改为逐证券检查覆盖，见下 |
| `--start` / `--end` | `--bars` | 覆盖窗口；`--start` 必填，`--end` 默认上一个完整交易日 |
| `--derivatives` | — | 只读窗口检查；必须指定单个 `--dataset futures_bars` 或 `option_bars`，以及 `--start`、`--end` |
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
cne verify --kind interior --repair   # 只补内部空洞，修完自动复查
cne verify --bars --start 2026-09-07  # 逐证券 × 会话，含窗口内零行的证券
cne verify --runs --days 20 --enforce # 连续交易日运行证据
```

`--derivatives` 输出 JSON：交易所缺日、相邻交易日已知合约缺行、元数据缺失、窗口内欠账和各品种覆盖。退出 1 表示发现缺口，退出 2 表示证据不足；没有完整历史在市清单时不会退出 0。它不支持 `--repair`，不联网或改写数据。首日会读取前一交易日证据；权威生命周期还能检查整个窗口都没有行情的已知合约。DCE 新浪不以零成交合约缺行判定不完整，INE 的源端起始边界仍可能无法证明。此检查不能替代历史参数、派生有效性或交易可执行性验收。

```bash
cne verify --derivatives --dataset futures_bars --start 2026-09-01 --end 2026-09-24
```

`--bars` 检查的是「证券 × 会话」这一格，包括窗口内一行都没有的证券——默认模式按数据集
聚合，看不见这种缺失。停牌等明确非交易状态不算缺口。不完整时退出 1。

## cne decision-data payment-gaps

按当前 `corporate_actions` 已提交修订列出没有到账日的现金分红，每条标注：

- `bar_relation`：事件落在该代码已观察行情的 `within_observed_bar_span` / `before_first_observed_bar` / `after_last_observed_bar` / `no_observed_bar`。可交易期之外的事件不可能有持仓收到现金；这只是路由线索，不证明当日发行人身份。
- `ex_date_rule_eligible`：是否为沪深北 A 股股票，可按中登除权日划付规则以除权日代替（见 [corporate_actions 到账日](../datasets/sources.md#corporate_actions)）。

汇总里 `within_span_count` 是可能影响持仓的缺口，`within_span_rule_ineligible_count` 是其中规则也补不上、真正需要来源的部分。`unreviewed` 只表示尚未逐事件审查，不代表公告缺失。

| 选项 | 说明 |
|------|------|
| `--start` / `--end` | 起止日期；`--start` 默认 2016-01-01，`--end` 默认今天（配置留出期时为其前一天） |
| `--output-dir` | 不可变 JSON 目录，默认 `{data.root}/meta/decision_data_gaps` |
| `--config` | 选择数据湖配置 |

## cne decision-data stock-terms

列出同一除权日同时有正值 `bonus` 与 `transfer` 的事件。并存只是待审信号，真实方案可以两者都有；以发行人公告为准。选项同上，输出前缀 `stock-terms-`。

## cne decision-data cash-rights

把已审查的发行人原公告按持有人类别展开为不可变 JSON 证据视图。每条权利必须与当前 `corporate_actions` 修订的可交易 A 股金额、付款日和原公告 PDF 哈希一致；已标记差异化权利的公司行动若缺审查证据则失败。它不推定股权登记日，尚不是通用公司行动账本数据集。

| 选项 | 说明 |
|------|------|
| `--start` / `--end` | 同 `payment-gaps` |
| `--output-dir` | 内容寻址 JSON 目录，默认 `{data.root}/meta/decision_cash_rights` |
| `--config` | 选择数据湖配置 |

## 命令结果与退出码

取数、初始化、回填、重试和派生完成本次尝试后，分别报告执行、覆盖和发布情况。它们保留旧 `status` 字段，并采用版本 2 的独立结果字段：

| 字段 | 取值与含义 |
|---|---|
| `result_schema_version` | 新结果为 `2`；旧记录保留为 `1`，覆盖未知不被自动升级为完整 |
| `execution_status` | `queued` / `running` / `completed` / `skipped` / `failed` / `interrupted`，描述本次执行 |
| `coverage_status` | `complete` / `partial` / `unknown` / `not_applicable`，描述有证据支持的请求范围覆盖；行数或成功提示不能证明完整 |
| `publication_status` | `pending` / `published` / `partial` / `unchanged` / `none` / `rejected`，描述当前运行的版本发布 |
| `fallback` | 来源受限时列出已有可读数据、注册来源角色与开关、请求范围和重试命令；来源列表不代表已尝试或当前可达 |
| `reason_code` | 数据集回执和批次保留结构化原因，例如 `source_transient`、`source_unavailable`、`input_unavailable`、`storage_failure`、`execution_error` |
| `usable_result` | 有有效请求结果、已覆盖范围或明确无数据证据；来源受限且本次 0 行可以为 false。日更等不因此判定执行失败；`cne backfill` 在此时退出 1，见下文 |

`completed + partial` 可以是正常终态。已校验事实可以发布，缺失范围、未扫描证券、冷却与重试证据继续留在台账中。缺输入的派生会解释跳过；派生版本记录输入版本和覆盖证据，输入变化时要求重算。来源全部不可用且本次没有有效请求结果，也正常结束为覆盖不足，保留已有湖（`cne backfill` 的退出码见下文）；程序、配置、存储、完整性错误仍报告失败。

取数 / 写入命令：`success`、来源受限的 `warning` / `degraded`（包括 0 行）返回 0，`failed` 返回 1。例外是 `cne backfill`（含 `--outstanding`）：状态不是 `success`、`usable_result` 为 false 且本次写入 0 行时返回 1，因为回填就是为了补这段范围，一行都没补上即为失败；数据源不可用和数据源无法提供该范围（`capability_limit`）都属于这种情况，JSON 结果与 `fallback` 不变。显式质量检查（`audit`、`verify`、`status --gate`）继续按各自质量规则返回非零。只读报告成功读取并展示后返回 0，不要求报告中的数据完全健康。

**升级调度脚本：** 若以前依靠 `status --datasets` 的退出码触发告警，改为 `status --datasets --gate`（按组调度时同时加 `--groups`）。依赖日更 / 回填退出码 2 的脚本，应读取 `coverage_status`，或另行运行显式质量门禁。现有数据目录、manifest、检查点和成功批次不需要删除；旧元数据以增量方式迁移。旧部分 staging 没有校验封存时保持保守门禁，通过原范围重试后再发布。

## cne check

一条命令验收整个湖，依次给出：

1. **新鲜度与覆盖**：与 `cne status --datasets --gate` 相同，含最新交易日截面核对和未完成的 init；
2. **数据质量**：最近一次 run 的审计和最近的全湖审计快照及其年龄，两者的 error 都会列出并计入退出码；
3. **规模**：数据集、行数和体积，统计表过期时先自动重算。

| 选项 | 说明 |
|------|------|
| `--full` | 当场重跑全湖审计。它读每个历史分区，大湖可能要数小时；默认读最近一次的结果 |

退出码取最差的一项：0 可用；1 有缺口或质量 error；2 证明不了（没有审计记录、instruments 缺失等）。调度脚本仍可只用较轻的 `cne status --datasets --gate`。

## cne status

| 选项 | 说明 |
|------|------|
| `--datasets` | 逐数据集新鲜度表（dataset / layer / freshness / 覆盖区间 / watermark）；加 `--gate` 时有 STALE 退出 1。freshness 取值：`fresh` / `STALE` / `empty`（还没抓过）/ `no source`（源已下线且无替代）/ `retired`（源已下线但湖已抓到最后一天，如 `northbound_flows`）/ `removed`（配置用 `steps_remove` 或 `enabled = false` 有意移出调度、没有别的组再跑它；保留真实的最后日期，不判落后，`cne check`、审计和门禁都不计入）/ `n/a`（配置里关闭，或不按日判新鲜度） |
| `--gate` | 显式质量验收：失败 / 新鲜度不合格返回 1，覆盖不足或降级返回 2；普通只读查询成功返回 0 |
| `--all-columns` | 配合 `--datasets`：打印 `list_datasets` 的全部列（契约指纹、revision、PIT 存储列等），而非仅新鲜度 |
| `--groups` | 配合 `--datasets`：在 `--gate` 模式下只对这些调度组拥有的数据集判失败（空格或逗号分隔）。其它组的数据集照常列出、照常报为调度缺口，但不触发退出 1。只调度 `core` 的主机应指定这个范围，避免未安排采集的可选组持续触发门禁。无人调度的数据集（`(unscheduled)`）仍然判失败，须明确处理 |
| `--scope` / `--no-scope` | 配合 `--datasets`：是否做最新交易日的标的截面校验（默认开）。对每个按「当日 active 证券」建键的数据集（`daily_bars`、`trading_status`）比较 tip 分区与证券表：有数据、有明确停牌证据、已记入待补账本、或有覆盖当天的无数据证据（例如日线探测证实「未上市」的新股代码，两张表都认）的都算覆盖，其余判 INCOMPLETE。要读这些数据集的 tip 分区加 `instruments`，`--no-scope` 让这条命令回到纯元数据 |
| `--run <id\|latest>` | 指定 run（默认 `latest`）；摘要含每个数据集 stage 的 `dataset_results` 与聚合 `dataset_status`。别名 `--run-id` 已删除 |

`--datasets` 还会报告尚未跑完的 init：日期 fresh 只说明已有数据新鲜，不代表全市场覆盖完整。init 缺的步骤如果之后的 run 已经成功跑过（例如日更每天都跑的 `derive_industry_index`），就不再算未完成；`cne init` 续跑仍按严格口径判断。
init 不属于任何调度组，因此不受 `--groups` 豁免；截面校验则归 `daily_bars` 所属的组管。

无选项：输出最近 run 的 JSON 摘要，查询成功返回 0，包括被查询 run 本身失败的情形。`--gate` 对所选报告执行质量验收：失败返回 1，降级返回 2。

## cne run retry

重试失败 batch / 补 init 缺失 step。init run 走 `resume_init`。

| 选项 | 说明 |
|------|------|
| `--run-id <id>` | 重试指定 run |
| `--failed-groups` | 逐个独立进程重试每个 `daily:*` 分组最新的失败 run；若该分组已有更新的成功 run，则跳过旧失败 |

两项必须且只能选择一项。

成功或来源受限退出 0，并交付取得的有效部分与 fallback；真实执行错误或 `RunLockError` 报错退出。

新写入的回填 run 保存日期、标的、分钟频率、分笔范围、衍生品交易所与合约，以及一次性来源选择和修复参数。使用 fallback 中的 `cne run retry --run-id ...` 会恢复这些参数，避免换进程后跳过取数或取错范围。旧 run 未记录的参数沿用当前配置；凭据、代理和访问策略仍读取当前配置。板块 `--force` 只在首次执行时重置检查点，重试继续未完成部分。

## cne run clean

预览已 compact 的终态 run staging、超龄 orphan、来源快照、日志和历史版本。交互终端在预览后分两次确认：先是历史版本，再是已合并且不在使用中的 staging。正在使用的数据和 staging 不会自动删除。定时脚本没有终端，仍然只预览。

回补和日更结束时会按 [`[storage.retention].keep_generations`](../getting-started/configuration.md#storage-retention) 丢掉更老的代，默认 5。这个命令在确认后按 `--keep-revision-generations` 删，默认也是 5。设为 `0` 则不处理版本。

| 选项 | 说明 |
|------|------|
| `--dry-run` | 只打印预览，不提问；不能与会修改运行状态的 `--reconcile-runs` 同用 |
| `--orphan-retention-days` | 无 manifest 的 orphan staging 保留天数（默认 7） |
| `--snapshot-retention-days` | 来源快照保留天数（默认 14）；每个 dataset/source 的最新一份保留 |
| `--keep-revision-generations` | 确认删除时每个数据集保留最近 5 代，另保护 current 和 hold；`0` 跳过版本 |
| `--log-retention-days` | 预览超过指定天数的 `logs/cne-*.log`（默认 30）；`0` 跳过 |
| `--reconcile-runs` | 显式把符合静默条件的孤儿 running 跑次标记为 failed，仍会修改运行状态 |
| `--reconcile-after-seconds` | 覆盖对账静默窗口，默认取 `[orchestrator].batch_stale_seconds` |
| `--force` | 将未 cleanup-ready 的 staging 也列入预览，不删除、不降级批次 |

先输出预览：`dry_run: true`。实际删除字段仍为空或 0，`bytes_freed` 为 0；候选见 `candidate_run_ids`、`candidate_run_dirs`、`candidates`，账面大小见 `logical_bytes_selected`。交互终端在预览后会分两次提问，默认都不删。第一次只针对超出 `--keep-revision-generations` 的历史版本；正在使用的一代、hold 和收据不能用的代不删。第二次只针对已经成功 compact、没有未完成批次、也没有被锁住或保留的 staging。正在运行、被锁住、未合并、orphan 或 hold 的 staging 即使回答是也不删。来源快照和日志不删。回补和日更不会删 staging，也不会删当前正在使用的一代。`--dry-run`、管道和定时任务不提问。`confirmation_required` 为 `interactive` 或 `none`。删除前重新核验运行锁、引用保留、运行状态和逐文件指纹，并保留可恢复的删除收据。存储页仍可勾选最近 5 代里不是当前指针的版本。

预览另返回 `staging_inventory`、`staging_run_count` 和 `skipped_reasons`，逐运行列出状态、大小、年龄、未完成批次和候选/保留原因。正在重试的运行由运行锁保护；`--force` 不绕过运行锁或 hold。未完成批次不会仅因年老或存在更新的成功运行而自动淘汰。

## cne storage

版本生命周期包括引用登记、不可变计划、原地观察期和维护窗口下的物理回收。满 7 天不会自动删除。回补和日更会按 [`[storage.retention].keep_generations`](../getting-started/configuration.md#storage-retention)（默认 5）删掉没有保留的更老代，不碰 staging，也不删当前正在使用的一代。交互终端里的 `cne run clean` 在预览后分两次确认，同样不等观察期。这些删除都不经过网页确认。设为 `0` 则回补和日更不删。外部读取须由操作者停止。

| 命令 | 行为 |
|---|---|
| `inspect --keep 5` | 只读列出仍存在的版本和保留原因。`logical_bytes` 是文件大小合计；`exclusive_bytes` 是删掉这一代能腾出的字节，和其他代硬链接共享的文件不计。APFS 上的克隆是独立文件，仍会计入，不能当成腾出空间的承诺。未导入引用时仅为初筛 |
| `explain OBJECT_ID` | 显示版本的 current/recent/hold 等依据 |
| `import --manifest FILE` | 校验并合并已审核的本地引用清单；不会自动解除已有保护 |
| `hold OBJECT_ID --reason TEXT` | 新增版本或清理资源的保留理由，取消相应待删除标记 |
| `plan --keep 5 --phase mark` | 校验引用源后保存内容寻址的标记计划，返回 `plan_id` |
| `plan --keep 5 --phase purge` | 只选择观察期已满且内容未变的候选，逐文件核对 SHA-256；输出 `purge_ids` 和 `logical_bytes_to_purge` |
| `apply PLAN_ID --phase mark` | 再次校验指针、引用、登记和文件身份，开始至少 7 天的原地观察期 |
| `apply PLAN_ID --phase purge` | 已禁用；必须转到 serve 存储运维页重新检查并确认 |

每个子命令接受 `--config`。版本 `OBJECT_ID` 为当前湖内的 `revision/<dataset>/<revision_id>`；计划另外绑定湖身份及 metadata 根，不能直接在另一个湖使用。被引用但暂时缺失的版本也可通过清单登记保护，以便恢复后继续受到保护。

引用清单 schema 为 `schema_version: 1`，包含绝对 `meta_root`、`holds`（对象 ID 到非空理由列表）、`reference_roots`（绝对目录及可选的相对 `exclude` 列表）、`reference_fingerprint` 和可选 `experiments`。集成程序可用 `cnequity.storage.lifecycle.reference_files()` 枚举引用源，分类后生成 holds，并用 `reference_fingerprint()` 计算文件集合及 SHA-256 的指纹；两者都传 `meta_root=` 湖的 `meta` 目录。以该目录为根时，湖每次运行都会改写的内容（来源缓存、`state`、`checkpoints`、`quality`、`source_snapshots`、衍生品抓取记录等，完整列表见 `RUNTIME_META_PATHS`）不计入引用，否则任何一次运行都会让导入失效，清理随即拒绝执行。导入只校验清单是否仍对应这些文件，不代替引用语义审核；排除项和外部未登记消费者需由操作者核实。

新增、删除或修改引用文件都会阻止旧计划继续标记，须重新审核并导入。重复导入保留已有 hold。当前没有自动解除 hold 的命令；改变保留承诺需要单独审核。无收据、缺少完整文件清单或缺少当前指针的目录不会成为清理候选。

引用清单还可携带 `cases`：每项具有不可变 `case_id`、`roots`（对象 ID 列表）、`dependencies`（`from`、`to`、`kind`）。`requires_bytes` 从根递归产生 hold；`provenance` 仅记录来源，不递归保留字节。循环依赖只计算一次。通过 Python `LifecycleStore.register_case()` 可单独增加案例，需提供 `evidence`（绝对 `path` 及 `sha256`）；此操作只增加保护，取消受影响对象的待删除标记，不解除旧 hold，也不证明完整重放已可用。

**物理删除需要网页确认和维护窗口。** 操作者须在网页确认新的调度、其他服务及所有外部查询已停止，并等待延迟读取结束。面板只会暂停本实例的数据请求并等待后台扫描结束，不会自动停止外部进程。执行器还会尝试取得发布锁，并拒绝 manifest 中仍有 running 跑次的情况。只暂停采集但仍保留外部 LazyFrame 读取，不满足该条件。

所有候选在删除前重新验证。执行器先记录意图，再把单个 generation 原子移入同文件系统的 `meta/lifecycle/trash/<plan-id>/`，随后删除内容。receipt 保留；指定旧版本读取会明确失败。遇到错误不会静默跳过或扩大范围，操作记录位于 `meta/lifecycle/purges/<plan-id>.json`。在保护条件未变时，在网页“未完成的删除记录”重新审核、确认原清单后，会核验残留文件并继续原计划；已完成计划只返回原结果，不再次删除。若期间新增 hold、引用或发布，重试会停止，需先人工核查和恢复受影响的残留内容，不能用新计划绕过未完成操作。

`logical_bytes_deleted` 仅累计已完整删除的版本；部分失败的字节不计入，并报告 `partial_deletion_possible`。文件系统可用空间另记录前后值，可能受到其他系统活动影响。待删除标记和删除收据都不能替代备份。

`logical_bytes_selected` 来自版本文件清单，不是实际磁盘释放量；APFS 克隆和系统快照可使两者相差很大。标记阶段所有字节保持原位。

现有其他清理器共享 hold：`staging/<run_id>`、`source_snapshot/<dataset>/source=<source>/data_version=<version>/run_id=<run>`、`log/<cne-*.log文件名>`。可通过 `storage hold` 保护已存在对象，或由 case 的 `requires_bytes` 提供保护。年龄、compact/readiness 和每类最新快照规则继续有效；`--force` 不绕过 hold。登记后的引用源发生变化或不可读时，这些清理器也停止并要求刷新清单。

完整快照保存生命周期保留依据与外部对象清单，标记 `external_bytes_materialized: false`。这不会把外部工件和全部历史版本打包进快照。恢复后须重新绑定、审核引用源并导入；导入会继承保存的 holds/cases，创建新湖身份，不继承待删除状态或本机删除计划。含生命周期登记的湖暂不支持增量包，创建和应用会明确拒绝；使用完整快照保留这些依赖。

## cne serve

湖面板：分层总览、逐数据集覆盖与新鲜度、溯源分布、覆盖热力图、操作页，以及需明确确认的存储运维。

| 选项 | 默认 | 说明 |
|------|------|------|
| `--host` | `127.0.0.1` | 回环地址同时接受 `127.0.0.1` 和 `localhost`。非回环地址**必须**配 `--token` |
| `--port` | `8787` | |
| `--token` | 无 | 要求 `Authorization: Bearer <token>` 或 `?token=` |
| `--read-only` | 关 | 只浏览。不注册操作页和存储清理的写入口 |
| `--allow-remote-ops` | 关 | 非回环地址上也可以从操作页发起取数。令牌在网址里，局域网是明文，默认不打开 |

```bash
cne serve
```

页面在 `/`，单数据集在 `#/dataset/<name>`（状态 / 元数据 / 数据 三个 tab），操作在 `#/ops`，跑批在 `#/runs`（含实时甘特，每页 100 条，可翻到清单里的全部 run），质量在 `#/quality`，OpenAPI 在 `/api/docs`（由 handler 生成，不会与实现漂移）。侧栏可以在中文和 English 之间切换。数据集在页面上显示中文或英文名称，名称下保留注册标识；命令和接口仍使用该标识。选择会记在本机浏览器里。跑批列表和运行详情直接写出每个失败 batch 的原因，例如来源状态码或冷却说明；run 上的 “one or more core steps failed” 不再代替这些原因。降级 run 没有 run 级错误时，同样显示 batch 上的原因。数据集列表在有落后数据时直接去补抓；单个数据集页把可执行的回填、补抓或重算放在标题旁。初始化没跑完时，该 run 的主按钮是继续初始化。跑批页可以手跑会留下一条 run 的命令：今天的日更、一个调度组、补落后数据、事件流、回填一个数据集、重算一个派生。点开的是操作页同一张表单，先预览再确认，同一时间只跑一个。今天不是交易日且没有待跑会话时，不提供「跑今天」。列表按全部、运行中、需要处理和成功筛选，每页仍是 100 条。需要处理只计入仍要处理的 run：同一任务后来已经成功或正在重跑、更早的失败已经被新的一次替换，都不再计数；跳过的非交易日不会销掉前面的失败。初始化缺的步骤后来已经做成时也不再计入。已经重试成功或已过时的 batch 不再算进失败原因。列表和运行详情对这次 run 打开同一张表单：重试、重试失败的日更组、发布 staging，或继续没跑完的初始化。体检、备份、巡检和取数设置仍在操作页。

**操作页按任务分块，启动的仍是对应的 `cne` 命令。** 四块是初始化、日更、手动更新和常用命令。初始化在空湖或未完成时放在最上面，完成后收成一行状态，不再把「再初始化一次」当作主按钮。日更是平时的主操作，显示今天是否交易日、定时时间和这次会话有没有跑过。今天是交易日时，主按钮是现在跑今天；今天不是交易日时不提供跑今天，若还有未跑的定时会话则改为跑该交易日。事件流和收尾补抓仍然可用。手动更新、常用命令、定时设置、取数和备份默认收起。手动更新先选范围：研究包或一个调度组配一个交易日，事件流配一个自然日，一个数据集或一个派生配起止日期，或只补仍落后的数据。一次只启动一条命令，不能在一个任务里勾选多个数据集各回填一段。常用命令列出同一白名单的命令模板（不含本机配置路径），点「用这个」打开对应表单；`cne repair`、`cne query`、配置写入和退市工程脚本只在终端运行。每次都先预览命令行再确认。同一时间只跑一个；任务在单独的进程里，关掉浏览器或停掉 `cne serve` 都不会停掉它。进度写在 `{data.root}/logs/cne-serve-job-*.log`，关联的 run 在跑批页。取消在 POSIX 上相当于 Ctrl-C，Windows 上结束进程树。日更只有一个交易日。要补一段日线，把手动更新的范围改成一个数据集并选择 `daily_bars`，填写起点和终点；日线按标的请求，即使只补一天也会扫该范围的市场。交易状态这类快照漏掉当天，改一个旧日期补不回来。手跑一个调度组或一次回填，不会把那天记成定时日更已完成。

**取数设置写回当前配置，不启动命令。** 可改的只有：暂停 push2（`[sources.eastmoney] push2_paused`）、通达信、东财、新浪日线、Baostock、北交所官网、同花顺页面和巨潮的 `enabled`、分钟线、分笔、期货、期货分钟线，以及 `[universe] ingest` 和 `ingest_eligible_etfs`。页面先列出将改的键，确认后才写入，并在同目录留下带时间的备份。其余键、注释、间隔、预算和凭据不动。标的列表、频率和交易所只显示、不编辑。`CNE_PUSH2_PAUSED=1` 仍优先于配置。保存后，定时日更、收尾补抓和之后的命令都按新值执行。`--read-only` 不提供写入；远程修改同样需要 `--allow-remote-ops`。

**定时任务在日更块里管理。** 可选择自动日更、收尾补抓、每日数据备份及独立事件流，预览系统任务定义和范围后确认启用；取消勾选后预览并确认暂停。页面注册当前用户的 launchd（macOS）、crontab（Linux）或任务计划程序（Windows），每分钟检查一次。日更执行全部配置的日更组和事件流，收尾补抓只处理落后的快照并等待当日日更已执行，每个交易日各尝试一次。备份选择已发布数据集、目录及北京时间，每个自然日到点后尝试一次，不自动删除旧备份。独立事件流选择事件组，按 1–1440 分钟间隔运行，包含周末和节假日；失败也从尝试开始时计算间隔，占用时等待，恢复后不累积补跑。失败时从任务记录检查并手动重试。这里只管理页面创建的系统任务；已有脚本调度应先核对，避免重复运行。

**备份列表可读取默认目录或指定的外部目录。** 创建、校验和恢复复用 `cne snapshot`，只备份明确选择的数据集及对应状态、契约、修订与血缘，不保存原配置、凭据或完整运行数据库。列表读取清单后显示“待校验”；校验任务重新计算文件摘要。恢复预览显示范围与目标，执行时再核对清单并完整校验，目标必须是当前数据湖和备份目录之外的新目录或空目录。当前服务继续使用原数据湖，验收恢复结果并切换配置的步骤见[备份与恢复](../operations/runbook.md#备份与恢复)。

关闭网页或 `serve` 后定时任务仍生效，执行结果会出现在最近任务及日志中。Windows 和 macOS 使用当前用户的登录会话，用户退出登录后不执行；Linux 需要安装 crontab 且 cron 服务运行。休眠期间不执行，恢复后在下一次检查判断：日更与补抓超过下个交易日 09:15 的窗口不追溯执行，备份只补当天。页面显示最近一次检查，超过 20 分钟未触发会提示排查。安装使用当前 Python 环境及绝对配置路径，移动或删除环境后须重新配置；系统任务不会保存启动 serve 时的临时代理、凭据等环境变量。

修改时间只改配置中的 `[job.daily] run_at` 和 `[job.stale] run_at`，保留其他设置，第一次修改前生成同目录的 `.schedule.bak` 备份。非标准的内联 TOML 表会拒绝自动修改。系统注册失败时自动任务保持暂停，页面提供错误信息，可刷新状态后重试。暂停不终止正在执行的任务；要中断本次运行，进入该任务点击取消。`--read-only` 禁用定时设置，远程修改同样需要令牌和 `--allow-remote-ops`。

任务日志和回填预览统一使用 UTF-8，无需为 Windows 终端另外设置编码。Windows 取消会核对 PID 与进程创建时间；记录缺少身份信息（例如更新前已启动的任务）或无法核对时，页面拒绝发送终止信号，请通过系统任务管理器处理。强制取消后，可从关联 run 检查已发布数据并重试未完成的工作。

找不到配置、而且没有显式 `--config` / `CNE_CONFIG` 时，面板进入首次配置：填写数据目录、看 `cne doctor`、再启动 `cne init`。体检结束前不能开始初始化。页面先显示将执行的命令，确认后才启动。目录里已经有数据湖时，先说明会接管且不会清空。显式指向一个不存在的文件仍然报错，不会自动生成。湖里还没有 curated 数据时，日更和手动更新收在次要位置，主操作是初始化。没跑完、而且之后的运行还没成功做过所缺步骤的初始化，可以直接续跑。缺的步骤后来已经成功做过时，页面不再提示继续初始化。已到点但还没跑的定时日更会写明那个交易日。日更表单会说明今天是不是交易日、待跑会话，以及这次手跑算不算那次定时。初始化或日更结束后，页面给出总览或重试入口。

从别的机器打开（`--host` 不是回环地址）时，浏览和存储清理沿用 `--token`；操作页默认不能启动命令，需要同时给 `--allow-remote-ops`。首次配置始终只接受本机。

**存储运维在 `#/storage`。** 打开后先看到可以删除的历史版本，按数据集汇总，账面大的排在前面。正在使用和受保护的版本在另外两个计数里，不能勾选。所有大小均为账面值，APFS 克隆下不能据此承诺释放空间。浏览、刷新、观察期到期均不执行删除。

勾选数据集或单个版本后点「删除所选」。确认单列出将要删除的项目；勾选不可撤销和外部任务已停止，再点「永久删除」才会核验并删除。一次超过 200 项会分批核验。可以选择观察期内尚未到期的历史版本，以及最近 5 代里不是当前指针的版本。当前版本、仍被引用、人工保留、仍在使用的试验，以及缺少收据或归档的项目不能选。删除完成后，页面列出删掉的版本、账面大小，以及删除前后的磁盘可用空间；可用空间没有增加时会直接说明，不能把账面大小当成实际腾出的空间。

也可以按范围点击“检查到期项目”，只针对已经到期的项目生成清单。等待完整性与引用核验后核对清单。只有勾选不可撤销确认、外部读写停止确认，并点击“永久删除”才执行；确认凭据 10 分钟后失效，服务重启后也失效。执行前再次检查保护条件和文件摘要。未标记对象可先“检查待标记项目”，确认后开始至少 7 天观察期，标记不释放空间。失败可能留下部分已删内容，须重新审核未完成记录，不能自动重试删除。

操作和存储清理都只接受当前页面的同源请求和页面确认凭据。匿名访问使用 localhost / 回环 IP；远程访问须配置令牌。`meta/stats` 仍会在后台按需重建。健康页只展示已经写好的探测报告，不会因为打开页面去请求第三方主机。

数值全部来自已落盘的产物（注册表、目录布局、`meta/stats`、`meta/quality/health-latest.json`、manifest），**请求路径上不扫 curated**。所以：先 `cne stats rebuild` 才有行数与体积；findings 显示的是上次 `cne audit --full` 的快照，页面上标了日期。

控制台用于查看健康状态、覆盖和样例；缺口的处理见[排障指南](../operations/troubleshooting.md)。

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

全量重建会扫描已发布数据；耗时随分区数、文件数和存储性能变化。统计表不是
数据本身，重建只更新 `meta/stats/` 下的摘要。

**`--if-stale` 的判据是 run id，不是时钟。** 改变湖的是采集，所以建于最后一个 run 之后的表无论多旧都是当前的，建于之前的无论多新都是过期的——`stats-latest.json` 的 `latest_run_id` 和 manifest 的最新 run 比对即可，只读一个小 JSON 加一行 SQLite。

并发用非阻塞锁收敛：面板请求、cron、夜间跑批同时想重建时只有一个真做，抢不到锁的直接返回而不是排队——把 web 请求堵在一次全扫后面比多看一个 run 的旧数字更糟。

`--if-stale` 判的是全湖水位，所以不能和 `--dataset` 同用，命令会直接报错而不是二选一地猜。

刷新策略（`meta/stats` 不会自己刷新）：

```bash
# 兜底：定时器上跑，没变化就是空转
cne stats rebuild --if-stale
```

控制台会按需刷新过期统计；定时任务可用 `--if-stale` 避免无变化时重复扫描。

> `cne stats refresh` 已并入 `cne stats rebuild --if-stale`；原 `--force` 就是不加 `--if-stale` 的默认行为。

### cne stats show

| 选项 | 说明 |
|------|------|
| `--dataset` | 单个数据集的逐分区明细 |
| `--by-source` | 改看 source / data_version 分布 |
| `--json` | 机器可读输出 |

**统计表过期时先自动重算**（与 `cne stats rebuild --if-stale` 规则相同，另一个重算持锁时让路）；`--dataset` / `--by-source` 需要的统计表还没生成时也会先生成。

**无 stats 表、只看汇总时直扫 curated**（原 `cne catalog`）：只给 dataset / files / rows，没有字节数、
源分布和逐分区明细，但一个从没建过统计表的湖不该先做一次构建才能回答「里面有什么」。

> `cne catalog` 已并入本命令的回退路径；`--json` 就是它原来的输出。

## cne query

SQL 查询本地湖；`--dataset` 与 `--symbol` 成对使用，缓存缺失或 `--refresh` 才取数。`[on_demand].enabled = false` 禁止按需查询；源关闭时不能刷新。取数错误非零退出，已缓存内容需显式刷新以更新。

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

## cne mcp

把这个湖接给 AI agent（MCP，默认 stdio，`--http` 改用 Streamable HTTP）。只读，不提供 serve 的存储删除入口。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径；省略时按统一顺序查找（客户端从哪个目录拉起进程不确定，用户级配置在任何目录都能找到）。写明时**建议绝对路径** |
| `--live` | 湖里没有的，现拉现给、不落盘。只支持 `resolve_symbol` 与未复权日线，其余工具明确拒绝 |
| `--http` | 改用 Streamable HTTP，在 `/mcp` 上监听。给只接远程网址的客户端（ChatGPT）配合隧道使用 |
| `--host` / `--port` | `--http` 的监听地址与端口，默认 `127.0.0.1:8788`。非回环地址必须配 `--token` |
| `--token` | `--http` 下每个请求都要带的令牌：`Authorization: Bearer`、`/mcp/<令牌>` 路径或 `?token=`。经隧道公开时必须设置 |

不用手敲：由 MCP 客户端拉起并在管道上讲 JSON-RPC。三条路按手上有什么选：

```bash
cne init                                                  # 还没有湖：先建全市场主干
cne mcp --config /abs/path/cnequity.toml
cne mcp --config /abs/path/cnequity.toml --live
```

上面的 `cne mcp ...` 是标准 MCP stdio server 命令，Claude 只是其中一种
客户端。Codex、Cline、Cursor、Windsurf、Gemini CLI、VS Code 或其它兼容客户端，均
使用相同的 `command` / `args`；客户端的注册入口不同，但不需要改 server。
ChatGPT 只接公网 HTTPS 网址，用 `--http --token` 加隧道接入。各家的具体写法见
[MCP 参考](mcp.md#接入各家客户端)。

`--live` **默认关，永不自动推断**：湖坏了的用户必须拿到 `no parquet data` 去修，而不是悄悄拿到一份来自别处、看起来差不多的答案。每次调用最多 50 个标的 / 800 天，且必须显式给 `symbols`。每条响应带 `origin: "lake" | "live"`。

6 个工具（`describe_lake` / `resolve_symbol` / `query_bars` / `query_fundamentals` / `query_dataset` / `run_sql`）、口径随响应返回、`run_sql` 只收单条 SELECT：见 [MCP 参考](mcp.md)。

## cne sources

数据源这一面的全部：`probe` 可实时探测，`substitutes --probe` 也会探测；其他入口读取本地状态、策略或证据。

| 子命令 | 说明 |
|--------|------|
| `probe` | 探测公开数据源，报告写进湖里 |
| `slo` | 把 `meta/source_health` 历史样本按 probe/vantage 聚成可用性 SLO，并写去重事故载荷。`--window-days`（默认 30）、`--minimum-observations`（默认 10）、`--enforce`（关键源不达标退出 1） |
| `resilience` | 从注册表算源集中度、failure-domain 爆炸半径和核心数据集独立备源门禁。`--out PATH` 落 JSON，`--enforce`（有核心表缺独立备源则退出 1） |
| `policy [SOURCE]` | 查 `sources/SOURCES.yml` 的来源使用策略。省略 SOURCE 输出全部；给 SOURCE 加 `--profile personal\|commercial\|cache\|redistribution` 做保守判断，未知权限一律 fail-closed（退出 1）。`--redistribution` 是 `--profile redistribution` 的简写 |
| `limits` | 离线显示同一出口的共享冷却、EM 当日预算与熔断、本湖最近 run 的请求/重试遥测，以及欠账续跑命令；不创建湖、不探测源 |
| `substitutes` | 根据已保存的探测证据建议独立备源；仅加 `--probe` 才主动探测 |

> 原为 `cne sources`（探测）+ `cne source <sub>`（派生结论）——两个顶层条目差一个字母，
> 且 `cne source --help` 不得不用一句话把自己和邻居区分开。现在收敛成一个名词。

### cne sources probe

`cne sources probe --list` 离线列出合法源名，无需配置。`--only` 不接受空值或未知源名；探测共享采集限流与冷却。

探测本湖依赖的公开数据源。每个源做最小探测；握手、分页或下载可能形成多个请求。探测断言响应体，串行且尊重各源限速。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径（探测不读湖，但要用里面的限速与超时） |
| `--vantage` | 这次探测从哪个出口发出：`cn` / `overseas` / 任意标签（默认 `local`） |
| `--only` | 逗号分隔的 probe key，默认全部；空串或未知名称会报用法错误 |
| `--stale-only` | 若同一出口和探针有 12 小时内经校验的真实采集证据，则跳过主动探测；每日任务使用此模式 |
| `--out` | JSON 报告路径。默认写进湖里的 `meta/source_health/<vantage>.json`，也就是 `cne serve` 读的位置 |

```bash
cne sources probe --vantage cn
cne serve                    # → http://127.0.0.1:8787/source-health
```

**探测在 CLI，展示在 serve。** 健康页面只展示已有报告，不会替你去请求十几个第三方主机——和它不触发采集是同一个理由。多次探测（不同 `--vantage`）会并排显示，不合并。

**`--vantage` 标记实际出口。** 可达性受路由、凭证、服务状态与请求历史影响，不能单凭地区判定。标签允许字母、数字、点、下划线和连字符，须以字母或数字开头，最多 64 字符；报告只代表当次观测。

**源挂了不影响退出码。** 源变红是这条命令的输出而不是它的失败。

状态五档：`ok` 可用 · `empty` 空响应 · `blocked` 被拒 · `down` 不可达 · `skipped` 未探测。`empty` 单独一档是因为它看起来比失败健康、实际更危险（回填静默截断）。

口径、加新源的方法见 [数据源健康度](../operations/source-health.md)。

### cne sources substitutes

**哪个源挂了、还有谁能顶上。** `probe` 回答"谁活着"，这条回答"活着的里面谁能替死掉的那个干活"。

| 选项 | 说明 |
|------|------|
| `--config` | 配置文件路径 |
| `--vantage` | 读哪个出口的报告（默认 `local`） |
| `--probe` / `--no-probe` | 现测而不是读存档；请求量与 `cne sources probe` 相同。**默认 `--no-probe`**：这个命令的本职是解读已有证据，不是再打一轮请求 |
| `--json` | 机器可读输出 |

```bash
cne sources substitutes         # 读 meta/source_health/local.json
cne sources substitutes --probe # 现测
```

替代源按**独立优先、其次快**排序：和故障源同属一个风控面的端点不算第二意见——东财的历史主机挂了，东财的快照主机顶不上它。某个数据集"失败的端点存在且没有任何可用端点"时退出码为 1。

格式示例（假设东财 `push2his` 不可达；耗时为示意值）：

```
daily_bars  —  有独立替代
    失败：eastmoney_push2his
    可用：sina                    1970ms  独立
    可用：ths_kline               2347ms  独立
    可用：baostock                5117ms  独立
    可用：eastmoney_push2         2807ms  同域 eastmoney
```

这条命令只读报告，**采集链路不读它**：一小时前从某个出口测到的可达性不是对下一个请求的承诺，所以故障切换仍然由采集链自己逐个源试过去。

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


apply 的安全边界值得单独说：add/replace/delete 逐条对基线指纹（revision 增量则对
各数据集 revision）核验，写入走同目录临时文件，每个被覆盖的文件在整包变更与
应用后的目标指纹都通过之前一直留着备份；中途抛异常会把已做的逐条回滚——调用方
不会观察到一个已知的半成品状态。可写路径也被收窄到 `curated/`、`derived/` 和
`meta/` 下的白名单，增量包无法借此在目标根里写任意文件。

## cne ths-official

对照并回填 **同花顺官方 API**（需 key）。**没有 key 时这里每个命令都报 `skipped` 且什么都不改**——湖保持它已有的源不变。

可选 keyed 来源不作为默认主源：`ths_official` 可以做 `backup_source` / `backfill_source`、可以进 failover 和 audit 配置，但永远不是 `primary_source`。

**验证与改写是两个开关。** `[sources.ths_official].verify` 默认开，只允许写 `meta/source_snapshots` 和 findings；`[sources.ths_official].backfill` 默认关，改动 curated 之前必须显式打开。持有凭证、启用源、允许它改数据，是三个决定而不是一个。

| 子命令 | 说明 |
|--------|------|
| `capture` | 抓对手源快照，喂给 `cne audit` 里的仲裁检查。**从不写 curated 行**。`--what corporate-actions\|daily-bars\|financials\|valuations\|all`（默认 all）、`--days`（bar 窗口，默认 45 天）、`--sample`（bar 抽样标的数，默认 400） |
| `backfill` | 补 2016–2024 的资产负债表与现金流缺口。需 `backfill = true`。`--start` / `--end`（默认 2016-01-01 ~ 2024-12-31）、`--chunk-size`（默认 200）、`--workers`（默认 4） |
| `repair-bars` | 把深度历史从无凭证爬取换到授权 API。**默认只报告，`--apply` 才写**。`--adjudicator FILE` 传入独立源的 (symbol, trade_date, close) parquet、`--diff-out FILE` 落有争议行、`--start` / `--end`（默认 2005-01-01 ~ 2015-12-31） |
| `resource-sectors` | 把 `sector_bars` 从爬取换到授权端点。同样 `--apply` 才写，且需 `backfill = true`。`--start`（服务底 2022-01-04）、`--end`、`--workers` |

**`capture` 不是可选的。** `cne audit` 里的 `adj_factor_arbitration` 与 `daily_bars_arbitration` 都读快照库，没跑过它这两个检查就**永久沉默**——最需要第二意见的湖恰恰一个都得不到。日更的 `finalize` wave 里有 `ths_official_snapshot` step，无 key 时自行跳过。

**`repair-bars` 和 `resource-sectors` 会改变数据来源。** 默认预演，显式 `--apply` 才写入。`repair-bars` 可配独立的 `--adjudicator` 检查争议，不能把两个同源候选当作独立仲裁。

`backfill`、`resource-sectors --apply` 与 `repair-bars --apply` 写入 staging 后自动 compact 发布；结果里的 `compact` 字段是发布结果。

配置、使用和失败处理见 [THS 接入](../getting-started/configuration.md#ths-官方接口)。

## cne verify --runs

（`cne verify` 的第三种模式，选项见上；原 `cne stability`。）

从权威 `trading_calendar` 取最近窗口，每个交易日汇总所有 `daily:*` 运行、按数据集取最新 receipt，只判核心档数据集，验证连续交易日运行证据。

| 选项 | 说明 |
|------|------|
| `--days` | 需连续通过的交易日数（默认 20） |
| `--as-of` | 含当日的 `YYYY-MM-DD` 截止 |
| `--enforce` | 门禁未过则退出 1 |

缺 run、核心 stage 失败、或只有旧 `warning` 且无 dataset receipt 都算失败。报告写
`meta/stability/latest.json` 与不可变历史目录。日更脚本每天跑但不 `--enforce`；
release 治理在第 20 天 enforce。

## cne --version

包版本号。

## 退出码汇总

| 码 | 场景 |
|----|------|
| 0 | 成功、非交易日跳过、来源受限的正常取数终态（包括部分或 0 行）、成功读取只读报告、健康检查通过 |
| 1 | 真实执行错误、健康检查失败、校验失败或显式门禁未过 |
| 2 | 显式 `status --gate` 的 run 降级；`status --datasets --gate` 的覆盖无法证明（证据读不出来或 `instruments` 缺失），与“已证明不全”（1）分开 |

## 相关文档

- [快速开始](../getting-started/quickstart.md)
- [产品边界](../architecture/overview.md)

## 研究输入快照与发布前审计

```bash
cne snapshot create research-baseline --dataset daily_bars --research
cne snapshot verify research-baseline
```

`--research` 包含价格、hfq 因子、证券身份、交易状态和日历，缺少任一依赖时报错。快照清单还校验 ST/退市覆盖证据、目录、日历种子和非敏感读取配置，不包含 API token。恢复到独立目录后运行严格查询核验覆盖，保存查询参数并使用记录的软件版本。

```toml
[quality]
publication_gate = "block"  # off（默认）、shadow、block
```

这是发布前候选门禁：离线对比已提交湖与整批候选的全量审计结果。`block` 拒绝新增或恶化的 error（按检查项、数据集和范围识别同一问题）或审计异常，候选隔离、指针和水位保留；`shadow` 只记录，适合先测量误报及扫描成本。报告位于 `meta/quality/publication/`。它与发布后的 `audit_gate` 分开配置；配置含义见 [产品边界](../architecture/overview.md)。
