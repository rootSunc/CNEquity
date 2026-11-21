# 日常更新

## 配置与日更 {#install-schedule}

用 `cne config path` 查看当前配置，`cne config validate` 校验。配置通过 `extends = "builtin"` 继承随包默认值时，可以只写需要覆盖的设置，例如 `[data].root`。各段的含义见[配置参考](../getting-started/configuration.md)。

```bash
cne run daily
cne status --datasets
cne check
```

无额外参数的 `cne run daily` 在交易日运行日更组，再运行已配置的事件流。非交易日跳过日更组，仍可运行事件流。

可用 `cne schedule install` 安装当前用户的系统定时任务，`cne schedule status` 查看状态。系统任务每分钟检查一次，日更与补抓的 `run_at` 按北京时间判断。用 `cne schedule install --help` 查看研究包、事件流和补抓选项。自定义调度器用自己的时区设置。

## 结果与覆盖 {#results}

| 信号 | 含义与下一步 |
|---|---|
| `success` 或 `degraded` | 这两种运行状态对应退出码 0。`degraded` 表示部分结果可用，仍需检查缺口。日更包含多阶段，整条命令取各阶段中最差的退出码。 |
| `skipped` | 结合 `reason_code` 判断，例如 `non_trading_day` 或 `locked`。 |
| `failed` 或 `blocked` | 查看 `cne status --run <运行 ID>` 和日志，处理阻断原因，再决定是否重试。 |

水位新鲜、目标范围覆盖完整和质量合格是三个检查项。用 `cne status --datasets` 看当前湖范围，用 `cne check` 查看验收结果。需要判断某次运行时，检查对应运行与数据集的结果。

`cne serve` 的「操作 → 今日」分别显示近期股票和指数日线覆盖、数据新鲜度及运行记录。「近期日线覆盖」按湖里的覆盖计划列出待补范围，支持从该范围打开回填预览。页面上的质量报告入口可查看已有检查结果。

## 补数与重试 {#backfill}

先查看计划，再执行需要的数据源请求。以下是缩小到一个标的的例子，日期应替换成实际缺口。

```bash
cne backfill daily_bars --symbols 600519.SH --start 2026-06-01 --end 2026-06-30 --plan
```

`--plan` 不取数、不写湖。确认计划后去掉它会发起采集。初次建湖中断可重跑相同的 `cne init`，日更失败可用 `cne run retry`，已有运行可用 `--run-id` 指定。只提供当前快照的数据集可能无法补回过去的缺口。

| 现象 | 检查 |
|---|---|
| 查询不到新数据 | 确认 `cne config path`、配置的 `data.root`、覆盖区间与发布状态。 |
| 源拒绝请求或超时 | 先运行 `cne doctor` 和 `cne sources limits`。需要验证连通性时用 `cne sources probe --list` 选源，再做有限探测，例如 `cne sources probe --only tdx_protocol`。 |
| 有活动写入或锁冲突 | 查运行状态和进程，等待持锁任务结束。锁文件存在本身不能证明仍有活动进程。 |
| 提示需要升级 | 按对应版本的[升级说明](../getting-started/installation.md)处理。 |

更多症状见[故障排查](troubleshooting.md)。参数以 `cne <命令> --help` 为准，执行前可查[联网与写入清单](../reference/cli-surface.md)。

## 快照、查看与恢复 {#backup-and-restore}

```bash
cne snapshot create baseline --dataset daily_bars
cne snapshot verify baseline
cne snapshot restore baseline /path/to/empty-restored-lake
```

快照包含选定数据集。恢复目标须为新目录或空目录，恢复后用独立配置指向该目录并验证查询。元数据备份只能恢复记录，完整的数据恢复还需数据文件。日更和维护可能按保留设置清理旧版本。需要长期重现研究结果时，提前保存对应快照。

本机 `cne serve` 的操作页可以发起支持的维护任务；只查看时可以使用 `--read-only`。升级前保留可独立恢复的数据副本，参见[安装与升级](../getting-started/installation.md)。
