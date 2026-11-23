# 安装与升级

需要 Python 3.11 或更高版本。安装包要求 DuckDB 2.0 或更高版本，随依赖安装。本项目在 macOS、Linux 和 Windows 上运行。真实采集需要访问配置的上游来源，[离线样例](quickstart.md#offline-sample)可用于检查安装和查询。内存或磁盘较小的机器先看[小内存和小磁盘的机器](configuration.md#small-machines)。

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
| 0.25.0 | 见[升级到 0.25.0](upgrade-0.25.md)，使用 `cne upgrade`。已有旧湖的写入命令在升级完成前可能被拒绝。新建正式湖直接使用湖目录。0.25.x 是能升级 0.24.0 及更早湖的最后一个版本系列。 |
| 其他版本 | 使用对应 Release 的说明。跨版本升级时检查中间各版的兼容性变更。 |

## 升级到 0.25.0 {#cne-upgrade}

0.25.0 改变了数据的存放方式，已有的湖要运行一次 `cne upgrade --apply`。0.25.x 是最后一个能升级 0.24.0 及更早湖的版本系列。准备、步骤、迁移内容和升级后的用法变化见[升级到 0.25.0](upgrade-0.25.md)。
