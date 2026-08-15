# 脚本说明

路径：`scripts/`

运维与一次性工具脚本。生产日更以 `daily_pipeline.sh` 为主路径。

> 以下脚本面向 **自托管本机/VPS**（含 macOS launchd、大陆出口回填）。开源贡献者只需
> CLI（`cml init` / `run`）即可；调度与告警按需选用，并非唯一部署方式。

---

## 生产脚本

### daily_pipeline.sh

**用途**：交易日串行执行全部 schedule groups，末尾健康检查与备份。

**流程**：

```
for group in core capital signals fundamentals macro_risk research; do
  cml run daily --group $group
done
cml status --datasets            # 探针：有 STALE 才继续
  └─ sleep CML_STALE_RETRY_DELAY_SEC
     cml run daily --stale-only  # 只重抓仍落后的
health_notify.sh
backup_meta.sh
cml clean
```

**收尾补抓**排在健康检查之前，所以补抓成功不会误报。`snapshot` 数据集只抓 run 当天，源端在那一个窗口中断就永久丢那天（重放会伪造行）——而立刻重试大概率撞上同一场中断，所以先等再抓，**但只在真有 STALE 时才等**，干净的日子零成本。详见 [runbook · 收尾补抓](runbook.md#收尾补抓)。

**环境变量**（仅本脚本读取；`cml` CLI 不读）：`CML_CONFIG`, `CML_LOG_DIR`, `CML_GROUPS`,
`CML_GATE_GROUPS`（默认 `core`，失败标为 gate；其余组标 soft）、
`CML_SOFT_FAIL_OK`（默认 `1`：gate OK 时 soft 失败 exit 0；`0`=仍 exit 1）、
`CML_STALE_RETRY`（默认 `1`；`0` 关闭收尾补抓）、
`CML_STALE_RETRY_DELAY_SEC`（默认 `1800`）、
`CML_TRADE_DATE`、
`CML_BIN`（覆盖 `cml` 路径；供用 stub 跑通控制流的测试用）。

结束时打印分组摘要（`group: OK|FAILED [gate|soft]`）外加 `stale-retry: OK|FAILED|not needed|skipped`，便于区分「门禁挂了」与「东财挂了」。补抓失败算 soft。

---

### install_scheduler.sh / uninstall_scheduler.sh

从 `scripts/launchd/com.cnmarketlake.daily.plist.template` 生成用户 launchd plist，加载 `daily_pipeline.sh`。

---

### health_notify.sh

```bash
cml audit --full
cml status --datasets
```

失败时 macOS `osascript` 通知，退出码非零。

---

### backup_meta.sh

打包 `meta/manifest.db`、`meta/state/`、`meta/quality/` 为 `meta-YYYYMMDD-HHMMSS.tar.gz`，按保留天数清理旧包。

参数：`backup_meta.sh [config_path] [backup_dir] [retention_days]`

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

## 一次性迁移

### migrate_daily_bars_volume_v2.py

把 curated `daily_bars.volume` 全部改写为「股」，并将 `data_version` 由 `v1` 提到 `v2`。

在修复之前，这一列混着两种单位：`tdx_protocol` / `sina` 写的是手，`ths` / `baostock`
写的是股，正好差 100 倍。存量行在任何一种口径下都是错的，只能重写。背景与各源实测证据见
[Schema 契约 · 成交量单位](../datasets/schema.md)。

```bash
scripts/migrate_daily_bars_volume_v2.py --config configs/cn-market-lake.toml --dry-run
scripts/migrate_daily_bars_volume_v2.py --config configs/cn-market-lake.toml --apply
```

- `--dry-run`（默认）只统计不落盘；`--apply` **就地改写 curated**，先跑 `backup_meta.sh` 并备份 curated。
- 幂等：已是 `v2` 的行跳过，中断后可直接续跑。
- `fetched_at` 不重新打戳——记录这次重新解释的列是 `data_version`。
- 跑完用 `cml audit` 确认 `daily_bars_volume_unit` 无 finding。

---

## 测试与冒烟

### smoke_daily_e2e.py

端到端冒烟：mock 或轻量配置下跑 miniature daily 路径，CI/本地回归用。

---

## launchd 模板

`scripts/launchd/com.cnmarketlake.daily.plist.template`

- `ProgramArguments` 指向 `daily_pipeline.sh`
- `StartCalendarInterval`：Hour=11, Minute=15（Europe/Helsinki；夏令时 16:15 CST、冬令时 17:15 CST）
- 标准输出/错误重定向到 `{data.root}/logs/launchd.*.log`

---

## 相关文档

- [运维 Runbook](runbook.md)
- [快速开始](../getting-started/quickstart.md)
