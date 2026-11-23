# 升级到 0.25.0

0.25.0 改变了数据的存放方式：已发布的数据和修订记录全部进入湖目录 `lake/`，不再使用 `curated/`、`derived/` 目录和 `meta/revisions/` 下的指针与回执。已有的湖需要运行一次 `cne upgrade --apply`。

## 是否需要升级 {#who}

- **0.25.0 新建的湖**：不需要。
- **0.24.0 及更早建立的湖**：需要，只做一次。
- **0.25.x 是最后一个能升级旧湖的版本系列**。更新的版本不再识别 0.25.0 之前的湖，必须先用 0.25.x 完成升级。
- **判断方法**：`cne upgrade` 列出还要做的步骤；`cne check` 在湖欠升级时会在结论处提示。
- **升级前**：读取照常；写入（日更、回补、派生、修复）会被拒绝，并提示运行 `cne upgrade`。

## 升级前准备 {#before}

- **停止使用这个湖的进程**：定时任务、`cne serve`、`cne mcp`、正在运行的查询和脚本。升级会等待正在进行的读取最多 30 秒；仍有进程在读写时，升级停止并说明原因，不改动任何内容。
- **备份整个数据目录（`data_root`）**：
    - 升级会删除旧布局（`curated/`、`derived/`、`meta/revisions/`），不可恢复。
    - 升级自带的备份（`backups/upgrade-<时间>/`）只含湖的记录，不含数据文件。
    - 回退需要这份完整副本。
- **磁盘空间**：文件系统支持硬链接时，数据文件链接进湖目录，几乎不占额外空间；不支持时需要再容纳一份数据。`cne upgrade` 的预览会给出估计。
- **DuckDB 扩展**：湖目录需要 DuckDB 扩展，首次使用时自动下载。连不上官方扩展站点时，设置 `EQUITYLAKE_EXTENSION_DIR`（本地目录）或 `EQUITYLAKE_EXTENSION_MIRROR`（镜像）。
- **旧快照**：0.25.0 之前用 `cne snapshot create` 做的快照，0.25.x 仍能校验和恢复，更新的版本不再读取。需要长期保留的，用 0.25.x 恢复出来（`cne snapshot restore`），或在升级后重新创建快照。

## 升级步骤 {#steps}

1. 安装 0.25.x：`python -m pip install -U "cnequity==0.25.*"`。
2. 检查配置：
    - 继承内置默认值（`extends = "builtin"`）的配置无需修改。
    - 完整展开的配置先运行 `cne config upgrade --dry-run` 查看变更，再运行 `cne config upgrade`。
3. 预览：`cne upgrade`。核对将运行的迁移、磁盘和扩展检查；出现 `BLOCKED` 时按提示处理，此时没有任何改动。
4. 升级：`cne upgrade --apply`。它先备份湖的记录，再按顺序运行迁移并显示进度。中途停止时，修正报告的问题后重跑同一命令，会从停下的地方继续。
5. 验收：运行 `cne status --datasets` 和 `cne check`，再用常用的查询核对关键数据。
6. 恢复使用：重启 `cne serve`、`cne mcp`，恢复定时任务。

## 升级做了什么 {#migrations}

`cne upgrade --apply` 按顺序执行以下迁移，每一步的结果记在 `meta/lake.json`：

- `lake-catalog`：把每个已发布的数据集登记进湖目录，逐行核对一致后才切换。
- `revision-records`：把修订记录导入湖目录。
- `legacy-datasets`：把旧样例湖、演示湖里没有修订号的数据集发布进湖目录。
- 0.18.0–0.23.0 的数据修复：只运行这个湖还没运行过的项目，部分项目需要联网。
- `attempts-ledger`：把 `meta/state/` 里的覆盖记录导入覆盖台账。
- `explicit-holds`：释放从文本里扫描出修订号而自动产生的保留；`cne storage hold` 和案例依赖的保留不变。
- `drop-generations`（不可恢复）：删除被湖目录取代的旧布局；被保留的修订暂时留下。
- `archive-held-revisions`：把仍被保留的旧修订转进湖目录，核对行一致后删除旧目录。转入后仍可按修订号读取。
- `meta-layout`：把 `meta/state/` 中剩余的记录移到各自的目录，`meta/state/` 退役。

单独预览某一项：`cne migrate --only <迁移 ID>`。

## 升级后需要调整的用法 {#changes}

- **直接读文件的外部工具**：不要再读 `curated/`、`derived/`，改用：
    - `cne storage export <数据集…> --out <目录>`，导出成普通 Parquet；
    - `cne query --sql …`、Python `load()` 或 MCP。
- **快照**：只能对已升级的湖创建。增量包命令 `cne snapshot delta create|verify|apply` 已删除。
- **已删除的命令和选项**：
    - `cne storage sync-catalog`、`cne storage merge-files`、`cne storage apply`；
    - `cne repair layout`、`cne repair repartition`；
    - `cne audit --research-universe`，改用 `--universe-profile`。
- **保留（hold）**：只来自 `cne storage hold`（可加 `--until 日期`）或案例依赖；文件里出现修订号不再自动保留该修订。
- **Python API**：
    - `load(universe="all_a")` 报错，改用 `profile="legacy_all_a"`。
    - 股票池函数的旧名（`get_universe_profile`、`list_profiles`、`show_profile`、`profile_json`）已删除，改用 `resolve_universe_profile`、`list_universe_profiles`、`show_universe_profile`。
    - `load_with_receipt()` 和 `dataset_state()` 返回的 `revision_receipt` 为 `None`；修订号、修订 id 和内容摘要不变。
- **`meta/state/` 退役**：直接读它的脚本改用 `cne status`、`dataset_state()`。
- **运行状态的取值**：`warning` 并入 `degraded`，`skipped_*` 改为 `skipped` 加原因代码。
- **不再起作用的配置**：`[storage]` 下的 `publish`，`[datasets]` 下的 `planned` 和 `planner_only`。保留在配置里只会告警。

## 出现问题时 {#troubleshooting}

- **预览出现 `BLOCKED`**：补齐扩展或磁盘空间后重跑，没有任何改动。
- **提示有其他进程在读写这个湖**：停止 `cne serve`、`cne mcp`、查询或定时任务后重跑，没有任何改动。
- **某个迁移失败**：之前的步骤已完成并记录。按报告修正原因后重跑 `cne upgrade --apply`。
- **需要回退**：安装原来的版本，并恢复升级前的完整数据副本。只恢复 `backups/upgrade-*` 不足以回退。
