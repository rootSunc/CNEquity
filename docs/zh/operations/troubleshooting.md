# 故障排查

先确定失败范围，再动手：`cne status` 看最近一次运行，`cne status --run <运行 ID>` 看各阶段，`cne status --datasets` 看各数据集的覆盖与新鲜度，`cne doctor` 检查安装、配置和湖的状态。检查结果写在 `meta/quality/findings/<运行 ID>.json`。任何一步失败都不会改动已发布的数据，所以不要通过删除湖目录、锁文件或手改数据文件来消除报错。

## 查询与发布 {#query-and-publish}

| 现象 | 原因与处理 |
|---|---|
| 查询不到新数据 | 先 `cne config path` 确认指向的湖。数据还在 `staging/` 没发布时，`cne run compact` 发布所有已结束但未发布的运行，或 `cne run retry --run-id <ID>` 续跑 |
| 写入命令被拒绝，提示运行 `cne upgrade` | 湖由较早的版本建立，按[升级到 0.25.0](../getting-started/upgrade-0.25.md)升级；读取不受影响 |
| 提交被拒绝，提示某个数据集的修订在这次运行期间变了 | 另一个写入先发布了这次读过或要发布的数据集。已发布数据没变，重跑同一条命令即可 |
| 发布门拦下了候选数据 | 已发布数据不变，候选分区移到 `_quarantine/`。按报告里新增或加重的检查项定位来源问题，修复后重跑 |
| `ReaderError`：缺复权因子、股票池证据不足 | 严格读取按设计拒绝返回。先补对应数据（如 `cne derive adj_factors`），或明确放宽口径 |
| `RevisionConsistencyError` | 固定的修订已被保留策略清理。需要长期复现的修订用 `cne storage hold` 登记，或保存研究快照 |

## 运行与锁 {#runs-and-locks}

| 现象 | 原因与处理 |
|---|---|
| `skipped`，`reason_code` 为 `locked` | 同类任务正在运行。等它结束；多个独立定时任务撞锁时只会跳过，应由一个调度入口顺序运行 |
| `RunLockError` 或等待湖锁超时 | 用 `cne status` 和进程列表确认持锁者。锁由操作系统持有，进程退出即释放；锁文件存在不代表仍有人持锁 |
| Windows 上报本地文件锁错误 | 检查湖目录的权限和占用；错误里有锁路径和原始系统错误 |
| `cne status` 显示有卡在 `running` 的运行 | 进程被杀或内存不足退出。下一次 `cne run daily` 或 `cne run retry` 会核对并恢复；要立即清理用 `cne run clean --reconcile-runs`，仍持锁的运行会被跳过 |
| 日志里 `worker pool broke` | 子进程退出（常见于内存不足），未完成的批次会串行重试 |
| 日志里 `no batch finished and no worker reported progress` | worker 挂住，超过 `[orchestrator].batch_stall_seconds` 后在当前进程重跑。反复出现时设 `tdx_daily_backend = "thread"` 并提交 Issue |
| 长时间只有 `still working` | 下一行写着哪个批次多久没进展。Linux 和 macOS 上按日志提示运行 `kill -USR1 <pid>`，所有线程的调用栈写到 stderr |
| 初始化中断 | 保留数据和配置，重跑同一条 `cne init`，已成功的批次不会重拉 |

## 来源与限流 {#sources}

原则：时间可以等，被封禁的代价远高于多等一天。不要为了加快速度调低请求间隔、开多进程请求同一个免费来源，或轮换出口继续已被拒绝的请求。

| 现象 | 原因与处理 |
|---|---|
| 请求被拒、超时或连接被重置 | `cne sources limits` 查看冷却与预算，冷却结束后再续跑。需要验证时用 `cne sources probe --only <来源>` 做有限探测 |
| 东方财富 push2 报 502 或连接被断 | 出口可能被限制。设 `[sources.eastmoney] push2_paused = true` 或 `CNE_PUSH2_PAUSED=1` 停止 push2 请求，其他来源照常；冷却与熔断到期后再解除并探测一次 |
| Baostock 黑名单或「今日请求已达上限」 | 每天 5 万次、单连接，黑名单冻结时长随当年次数增加。客户端会停在本地并冷却，已取到的标的会保留；冷却结束后重跑同一条命令 |
| 东方财富数据中心报 `rejected schema` | 来源的列与客户端契约不一致，该组不会发布。保留报表名与错误码，检查是否有修复版本或提交 Issue；不要把它当作空数据 |
| `daily_bars` 报 `remain absent`，水位不前进 | 窗口内有证券缺了某个交易日，主来源和备用来源都补不上。查 findings 里的 `daily_bars_interior_gap`：缺的是 ETF/LOF 时核对 `[universe].ingest`；只缺当天时等备用来源额度恢复后 `cne run retry`；用 `cne sources substitutes` 核对独立备源 |
| 国证指数报 `does not serve adjustment history` | 来源没有提供历史调样文件，不是空名单。当前成分不能替代历史成分，已有历史保留 |
| 同花顺板块回补大量失败 | 同花顺对密集请求限速，东方财富的代理设置对它无效。冷却后提高 `[sources.ths] min_interval_seconds`，再 `cne backfill sector_bars`，默认从断点续跑 |

## 覆盖与新鲜度 {#coverage}

| 现象 | 原因与处理 |
|---|---|
| `cne status --datasets` 显示落后 | 确认最近交易日跑过对应的组，用 `cne run daily --group <名称>` 重跑。季频数据集按自己的周期判断，每周组按上次应跑的交易日判断 |
| 非交易日 `cne run daily` 显示 `skipped` | `reason_code` 为 `non_trading_day`，正常。漏跑的交易日用 `cne run daily --trade-date <日期>` 补 |
| 历史有缺口 | `cne backfill <数据集> --start ... --end ... --plan` 先看计划，再去掉 `--plan` 执行。已确认为空的日子默认跳过，`--redo empty` 再问一次。只有快照的数据集补不回过去 |
| 某数据集的来源已经停止 | 用 `[datasets.history_until]` 写明历史到哪天为止，之后不再算欠着 |
| 磁盘不足 | 可用空间低于 `[storage.free_space].stop_gb` 时写入会拒绝开始并提示原因。先用 `cne storage usage` 看空间用在哪里、删除各部分至少能释放多少（快照和演练副本在 APFS 和支持 reflink 的 Linux 文件系统如 btrfs、XFS 上是克隆，`du` 会多算；ext4 不支持克隆，副本占满整份空间）。`cne run clean` 先列出可清理的旧修订、已合并的 staging、过期日志和来源快照，交互确认后删除；未完成的运行默认保留供续跑。小磁盘可以调小 `[storage.retention].keep_generations` |

## 审计结果 {#audit}

`cne audit --full` 读全部数据，大湖可能要几个小时。常见检查项：

| 检查项 | 处理 |
|---|---|
| `pk_unique` | 主键重复。查最近的发布，必要时回补该范围 |
| `mixed_partition_granularity` | 旧布局的细粒度分区与新分区重叠，运行 `cne upgrade --apply` |
| `unregistered_curated_dir` | `curated/` 下有不属于任何数据集的目录（如 `*.bak`），确认后移到备份目录 |
| `adj_close_discontinuity` | 复权收益异常，运行 `cne derive adj_factors` 并核对因子来源 |
| `missing_corporate_action` | 除权日没有公司行为记录，`cne backfill corporate_actions` |
| `trading_status_coverage_start` | 历史 ST 证据的起点晚于研究窗口，按市场回补并核对证据，不能只忽略这条警告 |

`warning` 和 `info` 不会让运行失败；`error` 是否让运行失败由 `[quality].audit_gate` 决定。

## 控制台 {#dashboard}

| 现象 | 处理 |
|---|---|
| `localhost:8787` 打不开而 `127.0.0.1:8787` 可以 | 升级到当前版本，回环地址同时接受两种写法 |
| 改了配置里的 `data.root` 后控制台仍显示旧湖 | `cne serve` 一直使用启动时的湖，重启后切换 |

问题无法定位时，附上 `cne doctor` 输出、命令、运行 ID 和脱敏后的日志提交 Issue。不要在 Issue 里贴凭证或个人数据路径。
