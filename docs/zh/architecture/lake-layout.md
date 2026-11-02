# 数据湖目录

湖根目录由配置中的 `[data].root` 决定。不同演示湖和正式湖应使用独立路径。

| 路径 | 用途 |
|---|---|
| `staging/` | 尚待合并的批次数据；失败恢复可能依赖它 |
| `curated/`、`derived/` | 合并工作副本及兼容布局；直接扫描不固定已发布版本 |
| `meta/revisions/` | 数据版本收据、当前版本指针与不可变数据文件 |
| `meta/state/` | 更新水位与覆盖状态 |
| `meta/quality/` | 质量检查与来源差异报告 |
| `meta/raw/` | 原始响应归档 |
| `meta/raw_storage.json` | 可选的外置原文位置登记；来源记录仍在 `meta/raw/`，快照会包含外置原文而不依赖本机路径 |
| `meta/source_snapshots/` | 用于来源核验的快照 |
| `meta/locks/` | 单次运行、发布和写入协调所用的锁 |
| `locks/` | 调度脚本与操作页共用的调度锁。可用 `CNE_SCHEDULER_LOCK_DIR` 或 `CNE_LOCK_DIR` 改到别处 |
| `meta/snapshots/` | 默认可移植数据快照。含所选数据集及对应状态、契约和修订，不含原配置与凭据 |
| `meta/serve_jobs/` | 操作页任务记录与取消标记 |
| `duckdb/` | SQL 视图数据库 |
| `logs/` | 运行日志 |
| `backups/` | 湖内备份的可选位置。调度脚本默认把元数据 tar 放在这里；操作页也可以把数据快照放在这里。元数据 tar 不能恢复行情，数据快照也不能代替整盘复制 |

## 日常管理

通过 Python API 或 `cne query` 读取已发布数据。不要直接修改版本指针、generation 文件或把备份放进 `curated/`。长期保存使用[研究快照](../reference/python-api.md)，日志和报告留在自己的本地目录。

清理 staging 前先确认任务已完成并已合并。`meta/locks/` 和 `locks/` 里的锁文件存在，都不等于仍有进程持锁；不要靠删除锁文件恢复运行。见[排障](../operations/troubleshooting.md)。

一个分区可以有多个 Parquet 文件，各文件的主键互不重叠，读取时取全部文件。回补和日更只把新增的键写成新文件，修改只重写包含这些键的文件，不再重写整个分区；`cne run maintain` 每天把文件数达到 4 个的分区合并回一个文件（也可手动运行 `cne storage merge-files`）。超过 20 万行的分区整份写入时按代码分桶成 `part-bNNN-of-MMM.parquet`，修改只重写所在的桶。直接读 Parquet 的外部程序要读取分区目录下的所有文件，而不是只读 `part-merged.parquet`。

分区布局发生变化时，旧目录仍可能可读，但与新目录重叠会造成重复主键。按[迁移脚本说明](../operations/scripts.md)先预演、备份，再统一布局。
