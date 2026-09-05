# 脚本说明

路径：`scripts/`

运维与一次性工具脚本。生产日更以 `daily_pipeline.sh` 为主路径。

> 以下脚本面向 **自托管本机/VPS**（含 macOS launchd、大陆出口回填）。开源贡献者只需
> CLI（`cne init` / `run`）即可；调度与告警按需选用，并非唯一部署方式。

---

## 生产脚本

### daily_pipeline.sh

**用途**：交易日串行执行全部 schedule groups，末尾健康检查与备份。

**流程**：

```
for group in core capital signals fundamentals macro_risk research; do
  cne run daily --group $group
done
cne status --datasets            # 探针：有 STALE 才继续
  └─ sleep CNE_STALE_RETRY_DELAY_SEC
     cne run daily --stale-only  # 只重抓仍落后的
health_notify.sh
cne sources probe --vantage $CNE_SOURCE_VANTAGE
cne sources slo                    # 累积报告，日更不 enforce
cne stability --days 20           # 累积报告，日更不 enforce
backup_meta.sh
cne clean
```

**收尾补抓**排在健康检查之前，所以补抓成功不会误报。`snapshot` 数据集只抓 run 当天，源端在那一个窗口中断就永久丢那天（重放会伪造行）——而立刻重试大概率撞上同一场中断，所以先等再抓，**但只在真有 STALE 时才等**，干净的日子零成本。详见 [runbook · 收尾补抓](runbook.md#收尾补抓)。

**环境变量**（仅本脚本读取；`cne` CLI 不读）：`CNE_CONFIG`, `CNE_LOG_DIR`, `CNE_GROUPS`,
`CNE_GATE_GROUPS`（默认 `core`，失败标为 gate；其余组标 soft）、
`CNE_SOFT_FAIL_OK`（默认 `1`：gate OK 时 soft 失败 exit 0；`0`=仍 exit 1）、
`CNE_STALE_RETRY`（默认 `1`；`0` 关闭收尾补抓）、
`CNE_STALE_RETRY_DELAY_SEC`（默认 `1800`）、
`CNE_SOURCE_HEALTH`（默认 `1`；`0` 关闭每日源探测）、
`CNE_SOURCE_VANTAGE`（默认 `local`；应设为稳定且真实的出口标签，如 `cn` 或
`overseas`）、
`CNE_TRADE_DATE`、
`CNE_BIN`（覆盖 `cne` 路径；供用 stub 跑通控制流的测试用）。

结束时打印分组摘要（`group: OK|FAILED [gate|soft]`）外加 `stale-retry: OK|FAILED|not needed|skipped`，便于区分「门禁挂了」与「东财挂了」。补抓失败算 soft。

---

### events_pipeline.sh

**用途**：7x24 事件流（公告、监管事件、资讯）。`cne run events` 一次跑完
`[job.events.groups]` 里的所有组，或用 `CNE_EVENTS_GROUP` 指定一个。

**和 `daily_pipeline.sh` 分开的两个理由**，都不是风格问题：

- 这些源**周末和节假日照发**，而 `cne run daily` 在非交易日直接跳过整个任务；
- 它拿自己的 shell 锁（`events`）和自己的 ingestion 锁（`events_ingestion`），
  所以晚间批处理正在跑并不会让这一次事件抓取被跳过。两个任务写的数据集不相交，
  这一点由 `validate_config` 保证。

**环境变量**：`CNE_CONFIG`、`CNE_LOG_DIR`、`CNE_BIN`、`CNE_TRADE_DATE`、
`CNE_EVENTS_GROUP`（默认全部组）、`CNE_SCHEDULER_LOCK_DIR`。

上一次还在跑时直接跳过并 exit 0：每组下次都会重读自己的窗口，跳过一次不丢数据。

---

### install_scheduler.sh / uninstall_scheduler.sh

从 `scripts/launchd/com.cnequity.daily.plist.template` 生成用户 launchd plist，加载 `daily_pipeline.sh`。
同时安装 `com.cnequity.stale`（收尾补抓）与 `com.cnequity.events`（事件流，每个自然日 14:00 本机时区）。
安装时用 `CNE_SOURCE_VANTAGE=cn scripts/install_scheduler.sh` 固化真实出口标签；省略时
使用 `local`。标签仅允许字母、数字、点、下划线和连字符，防止生成无效 plist。

---

### health_notify.sh

```bash
cne audit --full
cne status --datasets
```

失败时 macOS `osascript` 通知，退出码非零。

---

### backup_meta.sh

打包 `meta/manifest.db`、`state/`、`quality/`、`revisions/`、`source_snapshots/`、
`source_health/` 和 `stability/` 为 `meta-YYYYMMDD-HHMMSS.tar.gz`，按保留天数清理旧包。
因此磁盘故障不会让 revision receipt、PIT 源快照或已积累的验收窗口归零。

参数：`backup_meta.sh [config_path] [backup_dir] [retention_days]`

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
group），和 `daily_pipeline.sh` 是同一类东西，所以放在这里而不是发布的 CLI 里。

---

## Init 与验收

### run_init_2016.py

辅助全量历史 init（2016 起）的包装脚本，封装推荐参数与环境检查。

### retry_init_finalize.py

init 完成后若 finalize（compact/derive/audit）失败，单独重试 finalize 步骤。

### accept_backfill.py

回填验收工具：

```bash
python scripts/accept_backfill.py snapshot --out /tmp/counts.json
python scripts/accept_backfill.py check --compare /tmp/counts.json
```

检查幂等性与 curated 行数稳定性。

---

### delisted_ops.py

重建退市宇宙（幸存者偏差修复）的四个子命令。**读**目录和拉行情留在 CLI
（`cne delisted status` / `cne delisted backfill`）——那两个有日常形态；这四个没有。

| 子命令 | 说明 |
|--------|------|
| `discover [--limit N]` | 扫 issued code space，分类为曾上市 / 从未发行；可续跑，探测失败的码保持 pending 而不会被记成「从未发行」 |
| `reconcile [--apply]` | 默认只读核对目录终点；`--apply` 只修正被正式退市日、交易日历和正成交量行情共同证伪的终点，并生成备份及 SHA-256 回执 |
| `repair [--since]` | **不重新拉行情**：用已有 `daily_bars` 跨度写 `instruments.delist_date`，并清掉 `认购款` 占位 |
| `coverage [--start] [--end]` | **只读严格门禁**：验证发现完整性、窗口重叠、末根有效成交和 instruments 身份；未通过退出 1 |

```bash
cne delisted status                                   # 已知多少
python scripts/delisted_ops.py discover --limit 500
cne delisted backfill --since 2016-01-01
python scripts/delisted_ops.py repair
python scripts/delisted_ops.py reconcile
python scripts/delisted_ops.py reconcile --apply
python scripts/delisted_ops.py coverage --start 2016-01-01 --universe all_a_sh_sz
```

只有 `coverage` 是门禁：未验证通过时退出 1，所以任何声称「幸存者安全」的流程都应该先过它。

`coverage` 的通过声明刻意很窄：它证明退市目录已扫完，且已知与窗口重叠的退市标的具备一致的
末根有效成交和证券主数据；它不证明两端之间每个交易日都连续。数据源在停牌或正式摘牌前可能保留
零成交占位行，门禁不会把它们误当成末次交易。目录末日晚于窗口、但窗口内又没有行情可证明已经
上市的标的会进入 `unknown_overlap`，不会被静默排除。

`reconcile --apply` 不以单一供应商返回的「最后一条记录」为真相：必须有 curated 正成交量终点，
且该终点不晚于 `instruments.delist_date`，同时旧目录日期还必须落在正式退市日之后或非交易日，
才允许自动修改。检测到任何 active ingestion run 都会拒绝执行；修改前的目录保存在
`meta/state/history/`，质量回执写入 `meta/quality/`。

原为 `cne delisted discover / reconcile / repair / coverage`。

---

## 一次性迁移

### repartition.py

把历史分区改写成 `DatasetSpec.partition_granularity` 配的周期
（见 [分区粒度](../architecture/lake-layout.md#分区粒度)）。不带参数只列出待改写的数据集，
所以列表形式随时可跑。

```bash
python scripts/repartition.py                        # 待改写的数据集
python scripts/repartition.py --all --dry-run        # 先看影响
python scripts/repartition.py trading_calendar       # 单个数据集
```

读路径按目录形状自解析，改粒度本身**不需要**迁移；这只是把碎文件收回来。写入是先建临时目录、
逐分区写完并核对总行数，再一次 rename 换上去，中途挂掉不动原数据；重复执行幂等。

原为 `cne repartition`。触发它的是 registry 粒度在既有湖之下变了——那是迁移，不是日常运维。

### migrate_daily_bars_volume_v2.py

把 curated `daily_bars.volume` 全部改写为「股」，并将 `data_version` 由 `v1` 提到 `v2`。

在修复之前，这一列混着两种单位：`tdx_protocol` / `sina` 写的是手，`ths` / `baostock`
写的是股，正好差 100 倍。存量行在任何一种口径下都是错的，只能重写。背景与各源实测证据见
[Schema 契约 · 成交量单位](../datasets/schema.md)。

```bash
scripts/migrate_daily_bars_volume_v2.py --config configs/cnequity.toml --dry-run
scripts/migrate_daily_bars_volume_v2.py --config configs/cnequity.toml --apply
```

- `--dry-run`（默认）只统计不落盘；`--apply` **就地改写 curated**，先跑 `backup_meta.sh` 并备份 curated。
- 幂等：已是 `v2` 的行跳过，中断后可直接续跑。
- `fetched_at` 不重新打戳——记录这次重新解释的列是 `data_version`。
- 跑完用 `cne audit` 确认 `daily_bars_volume_unit` 无 finding。

---

## 测试与冒烟

### smoke_daily_e2e.py

端到端冒烟：mock 或轻量配置下跑 miniature daily 路径，CI/本地回归用。

---

## launchd 模板

`scripts/launchd/com.cnequity.daily.plist.template`

- `ProgramArguments` 指向 `daily_pipeline.sh`
- `StartCalendarInterval`：Hour=11, Minute=15（本机时区；UTC+2/+3 机器约合 16:15/17:15 CST，均在收盘后）
- 标准输出/错误重定向到 `{data.root}/logs/launchd.*.log`

`scripts/launchd/com.cnequity.events.plist.template`

- `ProgramArguments` 指向 `events_pipeline.sh`
- `StartCalendarInterval`：Hour=14, Minute=0，**不带 `Weekday`**——事件流本来就要在
  周末和节假日跑（UTC+2/+3 机器约合 20:00 CST）
- 想要资讯的日内新鲜度就再加一个 `CNE_EVENTS_GROUP=news_wire` 的高频 agent/cron；
  `disclosures` 每次重读 30 天尾窗，不适合高频

---

## 相关文档

- [运维 Runbook](runbook.md)
- [快速开始](../getting-started/quickstart.md)
