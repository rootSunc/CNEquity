# 仓库脚本

路径：`scripts/`

| 目录 | 内容 |
|---|---|
| `scripts/` | 运维工具：回填验收、退市名录重建、补跑追赶、幸存者偏差测量 |
| `scripts/dev/` | 仓库维护：文档同步（`sync_*`）、发布检查、离线基准与端到端检查、探针 |

这些脚本需要源码 checkout，不随 PyPI 包安装。定时更新用 `cne schedule install`（安装包自带，跨平台）；原来的 `scripts/scheduler/` 已移除，迁移见 [Runbook](runbook.md#legacy-scripts)。

> 以下脚本面向 **自托管本机/VPS**（含服务器回填）。开源贡献者只需
> CLI（`cne init` / `run`）即可；调度与告警按需选用，并非唯一部署方式。

## 生产脚本

### run_catchup.py

**用途**：漏跑 / 周末之后把门禁补齐——`daily:core`，再 `market_breadth` + `compact`
（`--core-only` 时跳过后者）。不传 `--backfill`（全量 CA 扫描在海外出口上很脆）。

```bash
python scripts/run_catchup.py                          # 最近一个交易日
python scripts/run_catchup.py --trade-date 2026-07-17
python scripts/run_catchup.py --trade-date 2026-07-17 --all-groups
```

**退出码看门禁，不看附加组**：core 或 market_breadth 失败 exit 1；`--extra-group` /
`--all-groups` 里的组失败只报告不改退出码——东财重的组在海外出口上本来就时好时坏。
水位已经到目标日的部分直接 `skipped_already_fresh`，不重跑。

原为 `cne run catchup`。它是编排而非能力（每一步都是 `cne run daily` 打一个 schedule
group），所以放在这里而不是发布的 CLI 里。

## Init 与验收

历史初始化与续跑直接使用公开 CLI：

```bash
cne init --profile full --since 2016-01-01   # 中断后重跑同一条命令即续跑
```

源能力诊断先用 `cne sources probe --list` 选最小探测；回填前先看
`cne backfill <dataset> --plan` 的范围与请求估算。

### accept_backfill.py

回填验收工具：

```bash
python scripts/accept_backfill.py snapshot --out /tmp/counts.json
python scripts/accept_backfill.py check --compare /tmp/counts.json
```

检查幂等性与 curated 行数稳定性。

### delisted_ops.py

重建退市宇宙（幸存者偏差修复）的四个子命令。**读**目录和拉行情留在 CLI
（`cne delisted status` / `cne backfill daily_bars --profile delisted`）——那两个有日常形态；这四个没有。

| 子命令 | 说明 |
|--------|------|
| `discover [--limit N]` | 扫 issued code space，分类为曾上市 / 从未发行；可续跑，探测失败的码保持 pending 而不会被记成「从未发行」 |
| `reconcile [--apply]` | 默认只读核对目录终点；`--apply` 只修正被正式退市日、交易日历和正成交量行情共同证伪的终点，并生成备份及 SHA-256 回执 |
| `repair [--since]` | **不重新拉行情**：用已有 `daily_bars` 跨度写 `instruments.delist_date`，并清掉 `认购款` 占位 |
| `coverage [--start] [--end]` | **只读严格门禁**：验证发现完整性、窗口重叠、末根有效成交和 instruments 身份；未通过退出 1 |

```bash
cne delisted status                                   # 已知多少
python scripts/delisted_ops.py discover --limit 500
cne backfill daily_bars --profile delisted --start 2016-01-01
python scripts/delisted_ops.py repair
python scripts/delisted_ops.py reconcile
python scripts/delisted_ops.py reconcile --apply
python scripts/delisted_ops.py coverage --start 2016-01-01 --universe all_a_sh_sz
```

只有 `coverage` 是门禁：未验证通过时退出 1，所以任何声称「幸存者安全」的流程都应该先过它。

`coverage` 的通过声明刻意很窄：它证明退市目录已扫完，且已知与窗口重叠的退市标的具备一致的
末根有效成交和证券主数据；它不证明两端之间每个交易日都连续。数据源在停牌或正式摘牌前可能保留
零成交占位行，门禁不会把它们误当成末次交易。目录末日晚于窗口、但窗口内又没有行情可证明已经
上市的标的默认进入 `unknown_overlap`。完整 Baostock 证券主档明确给出 IPO 晚于窗口时，列入 `not_yet_listed`；完整交易日历内每个应有交易日均有独立 Baostock 停牌证据时，列入 `verified_nontrading`。派生的行情缺口、缺少日期的日历或不完整停牌记录都不能作为通过依据；这两类均在报告中逐项保留。证券身份记录更新前保留内容寻址历史副本。

`reconcile --apply` 不以单一供应商返回的「最后一条记录」为真相：必须有 curated 正成交量终点，
且该终点不晚于 `instruments.delist_date`，同时旧目录日期还必须落在正式退市日之后或非交易日，
才允许自动修改。检测到任何 active ingestion run 都会拒绝执行；修改前的目录保存在
`meta/state/history/`，质量回执写入 `meta/quality/`。

原为 `cne delisted discover / reconcile / repair / coverage`。

## 分区重写（`cne repair repartition`）

把历史分区改写成 `DatasetSpec.partition_granularity` 配的周期
（见 [分区粒度](../architecture/lake-layout.md#日常管理)）。不带参数只列出待改写的数据集，
所以列表形式随时可跑。

```bash
cne repair repartition                          # 待改写的数据集
cne repair repartition --all                    # 先看影响
cne repair repartition trading_calendar --apply # 单个数据集
```

读路径按目录形状自解析，改粒度本身**不需要**迁移；这只是把碎文件收回来。写入是先建临时目录、
逐分区写完并核对总行数，再一次 rename 换上去，中途挂掉不动原数据；重复执行幂等。

以前是仓库脚本 `scripts/migrations/repartition.py`（更早是 `cne repartition`），现在随安装包提供，默认只预览，`--apply` 才改写。触发它的是 registry 粒度在既有湖之下变了，不是日常运维。旧版本湖的一次性数据迁移由 `cne migrate` 负责。

## 测试与冒烟

### smoke_daily_e2e.py

端到端冒烟：mock 或轻量配置下跑 miniature daily 路径，CI/本地回归用。

## 文档同步与翻译

### dev/sync_docs.py

从代码重新生成文档里由代码决定的部分，中英文两套都包括：字段表、衍生品能力表、CLI 参数表、命令副作用清单，以及 PyPI 用的 README。`--check` 只检查不写入，CI 会跑。CLI 帮助的中英文来自包内翻译目录（英文写在代码里，中文在 `src/cnequity/locale/zh_CN/LC_MESSAGES/cnequity.po`），缺中文时 `python scripts/dev/i18n_catalog.py` 检查失败；副作用说明的英文在 `scripts/dev/i18n/cli_surface.en.yml`，新增命令而没有对应英文时，检查失败。

### dev/i18n_catalog.py

维护 CLI、终端进度和控制台服务端文案的翻译目录。文案在代码里写英文，用 `cnequity.i18n` 的 `_()`（运行时翻译）或 `N_()`（只登记，显示时再翻译，如 Click 帮助和日志模板）标记；命令的 docstring 就是帮助，自动登记。中文在 `src/cnequity/locale/zh_CN/LC_MESSAGES/cnequity.po`，编译出的 `.mo` 一并提交，安装包不需要构建步骤：

```bash
python scripts/dev/i18n_catalog.py update    # 补新文案、删掉代码里已没有的
python scripts/dev/i18n_catalog.py compile   # 由 .po 重新生成 .mo
python scripts/dev/i18n_catalog.py           # 检查；有问题退出 1
```

检查覆盖：标记过的文案缺条目或缺中文、条目已不在代码里、模糊条目、占位符与英文不一致、首尾换行不一致、`.mo` 与 `.po` 不符。`tests/unit/test_i18n_catalog.py` 会跑它；`tests/unit/test_cli_untranslated.py` 另外拦下没走目录的中文字面量。Babel 只是开发依赖，安装后用标准库读取 `.mo`。

### dev/doc_translations.py

列出落后于中文原文的英文页。每个英文页在 `scripts/dev/i18n/doc_sources.json` 记着所依据中文页的摘要；中文改了、英文没跟上，`tests/unit/test_docs_i18n.py` 就会失败。改完英文后登记：

```bash
python scripts/dev/doc_translations.py --mark reference/mcp.md
```

代码生成的表格不计入摘要，重新生成表格不会要求重译。

## 相关文档

- [运维 Runbook](runbook.md)
- [快速开始](../getting-started/quickstart.md)
