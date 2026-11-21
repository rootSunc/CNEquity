# 安装与升级

需要 Python 3.11 或更高版本。安装包要求 DuckDB 2.0 或更高版本，随依赖安装。本项目在 macOS、Linux 和 Windows 上运行。真实采集需要访问配置的上游来源，[离线样例](quickstart.md#offline-sample)可用于检查安装和查询。

```bash
python -m pip install cnequity
cne --version
```

修改代码时可从[源码仓库](https://github.com/rootSunc/CNEquity)安装 `python -m pip install -e .`。发布包已包含控制台资源。

## 选择升级说明 {#升级与兼容性}

用 `cne --version` 确认当前软件版本，再阅读目标版本的[发布说明](https://github.com/rootSunc/CNEquity/releases)和[数据契约迁移说明](https://github.com/rootSunc/CNEquity/tree/main/contracts/migrations)。软件、数据集 schema 和已发布数据修订有各自的版本。

| 目标版本 | 数据湖升级入口 |
|---|---|
| 0.24.0 | 参见 [0.24.0 发布说明](https://github.com/rootSunc/CNEquity/releases/tag/v0.24.0)。本版没有新增必须运行的常规数据迁移。若旧湖仍欠迁移，可用 `cne migrate` 预览。 |
| 0.25.0 | 按下文使用 `cne upgrade`。已有旧湖的写入命令在升级完成前可能被拒绝。新建正式湖直接使用湖目录。 |
| 其他版本 | 使用对应 Release 的说明。跨版本升级时检查中间各版的兼容性变更。 |

## 升级到 0.25.0 {#cne-upgrade}

1. 停止使用此湖的旧版调度、写入任务和读取进程。保留升级前可独立恢复的配置与数据副本。仍直接扫描 `curated/` 或 `derived/` 的工具应在新版改用[查询接口或导出](../datasets/query-guide.md#parquet)。
2. 安装目标版本。在旧的完整配置上用 `cne config upgrade --dry-run` 检查配置变更，再按需运行 `cne config upgrade`。继承 `extends = "builtin"` 的配置随软件读取内置默认值。
3. 运行 `cne upgrade` 预览，检查将运行的迁移、磁盘空间和扩展要求。缺少 DuckDB 扩展时，预览也可能触发下载。
4. 核对预览及备份后运行 `cne upgrade --apply`。如升级中断，修正报告的问题后重跑同一条命令。
5. 运行 `cne status --datasets` 和 `cne check`，用原有查询验证关键数据，再恢复调度与服务。

!!! warning "旧布局清理"
    0.25.0 的升级会清理被湖目录取代的旧布局。默认升级备份保存的是湖的记录，回退软件或恢复元数据备份无法重建已删除的数据文件。需要回退时使用升级前保存的数据副本。

需要单独预览某项迁移时，用 `cne migrate --only <迁移 ID>`。加 `--apply` 会执行所选迁移，包括已经记录过的项目。
