# CLI 参数与默认值

由 `scripts/dev/sync_docs.py` 从 Click 注册表生成。命令用途、副作用与操作场景分别见 [CLI 参考](cli.md) 和 [副作用清单](cli-surface.md)。

## `cne audit`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--run-id` | `—` | 运行 ID；可重复指定，合并运行证据并审计一次。 |
| `--full` | `False` | 整个湖的健康快照（当前状态 + 新鲜度），而不是某一次 run 的文件。 |
| `--research-start` | `—` | 严格校验从这一天开始的研究窗口（需要 --full）。 |
| `--quality-only` | `False` | 配合 --full：只按质量 error 判门禁；调度新鲜度请另外用 status 检查。 |
| `--research-end` | `—` | 研究窗口终点（默认取最新的 daily_bars；需要 --research-start）。 |
| `--research-universe` | `all_a` | --full 检查的历史研究 universe。 |

## `cne backfill`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `dataset` | `—` | 必填；位置参数 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--profile` | `default` | daily_bars 可选 delisted：只回填已确认退市名录，沿用独立发现证据。 |
| `--plan` | `False` | 只输出来源、范围和抓取方式；衍生品另给请求预算。不取数、不写湖。 |
| `--exchange` | `—` | 仅衍生品：限定交易所，可重复。INE 归入 SHF 路由；2018 期货另取 INE 日文件。 |
| `--source` | `—` | 本次回填改用的来源，不修改配置。支持 margin_trading（exchange 或 eastmoney）和 financial_statement_items（tdx_fin：只补湖中缺少的科目）。 |
| `--refetch` | `False` | 忽略完成收据和 checkpoint，重新获取整个范围（sector_bars、futures_bars、option_bars）；熔断仍然生效。不加时回填从断点续跑。 |
| `--start` | `—` | 回填区间起点（YYYY-MM-DD），包括 daily_bars、minute_bars、衍生品日线、日期/报告期推进及 sector_bars。sector_bars 默认往前 400 天；超过来源历史深度的范围保留为缺口，可取范围照常交付并提供补数指引。 |
| `--end` | `—` | 回填区间终点（YYYY-MM-DD，默认今天），与 --start 配合限定历史窗口。 |
| `--outstanding` | `False` | 只修复被容忍缺口欠下的那些 key，范围和窗口都取自欠账台账，不看 --symbols/--start/--end。补上的 key 会销账，仍然缺的继续欠着。 |
| `--symbols` | `—` | 限定范围的标的列表，逗号分隔：用于 intraday、trading_status、corporate_actions 的限定回填，以及 financial_statement_items、daily_bars、share_structure 的限定修复。trading_status 的 checkpoint 与覆盖证据会记下确切范围；daily_bars 会把这个显式范围写进 backfill 元数据。 |
| `--workers` | `1` | 仅 margin_trading 的日期推进并发数。每个请求仍然走配置里共享的源限流器；其它数据集必须为 1。 |

## `cne check`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--full` | `False` | 立即重跑全湖审计（读每个历史分区，大湖可能要数小时）；默认读最近一次的审计结果。 |
| `--pack` | `—` | 按研究包给出窗口、缺口和下一步。可重复。指定后，缺数据或历史 ST 未覆盖会使退出码变差。 |

## `cne config`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `action` | `—` | 必填；位置参数 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--force` | `False` | action=create 时覆盖已存在的配置文件。 |
| `--data-root` | `—` | action=create 时设置 [data].root（默认把 ./data/cnequity 解析成绝对路径）。 |
| `--dry-run` | `False` | action=upgrade 时只列出要补的 step 和调度组，不写文件。 |
| `--minimize` | `False` | 用于 action=upgrade：把完整配置改写为继承内置默认值，只保留不同的设置；会列出每项行为会变化的设置。 |
| `--defaults` | `False` | 用于 action=show：打印每项设置及其内置默认值和说明。 |

## `cne contract diff`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `old_contract` | `—` | 位置参数 |
| `new_contract` | `—` | 位置参数 |
| `--old` | `—` | 基线契约文件路径。 |
| `--new` | `—` | 候选契约文件路径。 |
| `--from` | `—` | --old 的别名。 |
| `--to` | `—` | --new 的别名。 |
| `--json` | `False` | 输出机器可读的 JSON。 |
| `--allow-breaking` | `False` | 即使发现破坏性变更也返回退出码 0。 |

## `cne contract show`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `dataset` | `—` | 位置参数 |
| `--dataset` | `—` | 数据集名（也可以直接作为参数传）。不给则输出完整契约。 |
| `--out, --output, --path` | `-` | 把 JSON 写到这个路径而不是标准输出；'-' 表示打印。 |
| `--json` | `False` | 输出机器可读的 JSON（默认）。 |

## `cne contract validate`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `contract_path` | `—` | 位置参数 |
| `--path` | `—` | 契约 JSON 路径（也可以直接作为参数传）。 |
| `--json` | `False` | 输出机器可读的 JSON。 |
| `--against-registry` | `False` | 要求文件里的契约与当前的 DATASETS / SCHEMAS / PRIMARY_KEYS 完全一致。 |

## `cne decision-data cash-rights`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--start` | `2016-01-01` | — |
| `--end` | `—` | 默认今天；配置了 [research] holdout_start 时默认其前一天。 |
| `--output-dir` | `—` | — |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |

## `cne decision-data payment-gaps`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--start` | `2016-01-01` | — |
| `--end` | `—` | 默认今天；配置了 [research] holdout_start 时默认其前一天。 |
| `--output-dir` | `—` | — |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |

## `cne decision-data stock-terms`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--start` | `2016-01-01` | — |
| `--end` | `—` | 默认今天；配置了 [research] holdout_start 时默认其前一天。 |
| `--output-dir` | `—` | — |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |

## `cne delisted status`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--since` | `2016-01-01` | 湖窗口起点。 |
| `--sample` | `15` | 打印多少行明细。 |

## `cne derive`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `name` | `adj_factors` | 位置参数 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--full` | `False` | 重写 adj_factors / industry_index / option_greeks / minute_bars_15m\|30m\|60m 的全部分区（默认只补增量）。 |
| `--start` | `—` | industry_index / trading_status / option_greeks / minute_bars_15m\|30m\|60m：只派生这个日期（YYYY-MM-DD）及之后的。 |
| `--end` | `—` | industry_index / trading_status / option_greeks / minute_bars_15m\|30m\|60m：只派生这个日期（YYYY-MM-DD）及之前的。 |
| `--apply` | `False` | adj_factor_source：写入逐证券来源覆盖并重算这些证券的因子（默认只报告）。 |

## `cne doctor`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--json` | `False` | 输出机器可读的 JSON。 |

## `cne import shfe-archive`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `archive` | `—` | 必填；位置参数 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--dataset` | `—` | 必填；年度包包含哪类日线。 |
| `--year` | `—` | 必填；它覆盖的交易年份。 |
| `--accept-partial-fields` | `False` | 接受年度包缺少只有日文件才有的字段；已有的完整日线不会被覆盖。必填。 |
| `--url` | `—` | 可选：它在上期所网站的原始 HTTPS 地址。 |
| `--downloaded-at` | `—` | 可选：下载时间，带时区的 ISO 8601。 |
| `--start` | `—` | 在 --year 之内：导入的第一天。 |
| `--end` | `—` | 在 --year 之内：导入的最后一天。 |
| `--apply` | `False` | 运行；不加只打印计划。 |

## `cne init`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--profile` | `quick` | 建多大。demo = 用真实数据源抓几只票，sample = 同样的形状但离线且确定 —— 两者都不是一个市场。quick = 全市场标的、最近 3 年；full = 全市场标的、从 2016-01-01 起（通常需要更多分页）。以后可以用 `cne backfill daily_bars` 补深。 |
| `--symbols` | `600519.SH,000001.SZ,000858.SZ,300750.SZ,601318.SH` | demo/sample：要抓的标的，逗号分隔（有意保持很少）。 |
| `--days` | `30` | demo/sample：daily_bars 大致抓最近多少个交易日。 |
| `--data-root` | `data/cnequity-demo; sample: data/cnequity-sample` | demo/sample：独立的湖根目录（不要拿去跑全市场 init）。 |
| `--config-out` | `configs/cnequity.demo.toml; sample: configs/cnequity.sample.toml` | demo/sample：把那份小配置写到哪，供后续 `cne query` 使用。 |
| `--force` | `False` | demo/sample：允许覆盖内容不同的已有 --config-out；默认拒绝并保留原文件。 |
| `--intraday` | `False` | demo/sample：同一批标的额外抓 1 分钟线（最多 5 个交易日）并打印一个交易日，让 bar_time 的口径看得见。 |
| `--research` | `False` | demo/sample：额外用 Sina 派生 hfq 复权因子，并打印未复权 / 复权收益对照（较慢；需要访问 Sina）。 |
| `--layout-only` | `False` | 只建目录、manifest 和 DuckDB 视图，跳过 init 各阶段。 |
| `--trade-date` | `—` | init 各阶段的 as-of 交易日（YYYY-MM-DD）；默认今天。 |
| `--resume` | `False` | 续跑最近一次没跑完的 init run（重试失败批次 + 补缺失阶段）。 |
| `--run-id` | `—` | 续跑指定的 init run_id（隐含 --resume）。 |
| `--keep-going` | `False` | 某个阶段失败后继续往下跑，而不是停下来。 |
| `--since` | `—` | 显式指定历史起点（YYYY-MM-DD）；覆盖 --profile。 |
| `--quiet` | `False` | 只留 warning 及以上，不打逐批进度。 |
| `--pack` | `—` | 研究包：market 行情、fundamentals 基本面、universe 历史 ST。不改变这次初始化下载的内容。决定结束后要保持更新的调度组，以及 `cne check --pack` 的结论。可重复。不写则沿用已保存的选择，第一次是 market。 |
| `--schedule` | `False` | 初始化成功结束后，为所选研究包安装定时日更和收尾补抓。不含公告和资讯。 |
| `--json` | `False` | 结束时输出完整的机器可读结果，而不是摘要；完整结果也总会存到日志目录。 |
| `--no-preflight` | `False` | 跳过开跑前最多 8 秒的网络预检（通达信、交易所、东财）。也可设环境变量 CNE_SKIP_PREFLIGHT=1。 |

## `cne mcp`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--live` | `False` | 湖里没有的数据就按需向源头取，并且不落盘。只支持标的查找和未复权日线；其它工具宁可拒绝，也不会在没有复权、universe 和 PIT 的情况下作答。 |
| `--http` | `False` | 改用 Streamable HTTP，在 /mcp 上监听。给只接远程 URL 的客户端（如 ChatGPT）配合隧道使用。 |
| `--host` | `127.0.0.1` | --http 的监听地址。非回环地址必须配 --token。 |
| `--port` | `8788` | --http 的端口。 |
| `--token` | `—` | --http：每个请求都要带这个令牌（Bearer 头、/mcp/<令牌> 路径或 ?token=）。经隧道公开时必须设置。 |

## `cne migrate`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--apply` | `False` | 运行欠下的迁移，并把每个结果记入 meta/state/lake.json。 |
| `--only` | `—` | 逗号分隔的迁移 id；即使已记录也重新运行。默认：这个湖还欠的全部迁移。 |
| `--json` | `False` | 输出机器可读的 JSON。 |

## `cne profile list`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--include-compatibility, --official-only` | `True` | 列表里包含历史遗留的 universe 别名。 |

## `cne profile show`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `name` | `—` | 必填；位置参数 |
| `--symbol` | `—` | 把 profile 绑定到具体标的，并附带 concrete_scope_hash。 |

## `cne query`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--sql` | `SELECT COUNT(*) AS n FROM daily_bars` | 只读 DuckDB SQL；每个数据集是一个视图，名称同 `cne status --datasets`。 |
| `--dataset` | `—` | 按需抓取的数据集名 |
| `--symbol` | `—` | 按需抓取的标的代码 |
| `--refresh` | `False` | 抓取前先刷新按需缓存（需要同时给 --dataset 和 --symbol）。 |

## `cne repair adj-factors`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--apply` | `False` | 应用可确定的因子修正并保留旧版本；未解决项只作提示。 |
| `--refresh-sources` | `False` | 联网补取独立因子证据；默认仅使用本地缓存。 |
| `--symbols` | `—` | 逗号分隔的证券代码；默认分批检查历史问题。 |
| `--start` | `—` | 待修事件起点（YYYY-MM-DD）；累计影响延续到其后。 |
| `--end` | `—` | 待修事件终点（YYYY-MM-DD）；累计影响延续到其后。 |
| `--max-symbols` | `100` | 每批最多检查的证券数；应用后保存历史进度。 |
| `--plan` | `—` | 应用指定预览的计划标识；输入变化时要求重新预览。 |

## `cne repair bar-amounts`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--start` | `—` | 必填；窗口起点（YYYY-MM-DD）。 |
| `--end` | `—` | 必填；窗口终点（YYYY-MM-DD）。 |
| `--symbols` | `—` | 逗号分隔的证券代码。 |
| `--apply` | `False` | 运行；不加只打印计划。 |

## `cne repair bar-turnover`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--start` | `—` | 必填；窗口起点（YYYY-MM-DD）。 |
| `--end` | `—` | 必填；窗口终点（YYYY-MM-DD）。 |
| `--symbols` | `—` | 逗号分隔的证券代码。 |
| `--peer-review` | `False` | 用交易所历史价格、通达信和东财复查价格冲突；只有证据一致时才整行替换。 |
| `--apply` | `False` | 运行；不加只打印计划。 |

## `cne repair bse-tip-turnover`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--symbols` | `—` | 必填；逗号分隔的证券代码。 |
| `--date` | `—` | 必填；交易日（YYYY-MM-DD）。 |
| `--apply` | `False` | 运行；不加只打印计划。 |

## `cne repair corporate-action-gaps`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--apply` | `False` | 向 Baostock 补取并发布新版本；默认只离线输出计划。 |

## `cne repair delisted-actions`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--source` | `—` | 必填；baostock 用于已退市的沪深证券；ths 用于已退市的北交所证券。 |
| `--symbols` | `—` | 逗号分隔的证券代码。 |
| `--start` | `—` | 窗口起点（YYYY-MM-DD）。 |
| `--end` | `—` | 窗口终点（YYYY-MM-DD）。 |
| `--apply` | `False` | 运行；不加只打印计划。 |

## `cne repair ex-dates`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--ex-dates` | `—` | 必填；逗号分隔的除权日，YYYY-MM-DD。 |
| `--symbols` | `—` | 逗号分隔的证券代码。 |
| `--apply` | `False` | 运行；不加只打印计划。 |

## `cne repair financial-originals`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--symbols` | `—` | 股票代码，逗号分隔。 |
| `--all-stocks` | `False` | 使用湖中股票目录；与 --symbols 二选一。 |
| `--after` | `—` | 接续上一批 next_cursor（YEAR:SYMBOL）；失败项可不带游标重试。 |
| `--quarter` | `Q4` | 报告期；Q2 为半年报，Q4 为年报，利润和现金流只恢复年初至期末累计数。 |
| `--start-year` | `2016` | — |
| `--end-year` | `2024` | — |
| `--max-reports` | `16` | — |
| `--reparse` | `False` | 重新解析已有原文报告；内容未变的行保留原始观测时间。 |
| `--refresh-sources` | `False` | 从巨潮取完整公告查询和原始定期报告；默认回放已归档来源。 |
| `--apply` | `False` | 追加可核实的原文当期数，保留既有版本及真实采集时间。 |

## `cne repair fund-no-print`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--apply` | `False` | 保存基金竞价日线覆盖证据并结清对应欠账；不生成日线或停牌行。 |
| `--refresh-sources` | `False` | 向沪深交易所取历史基金报表；默认离线回放已留存报表。 |
| `--max-sessions` | `32` | 每批最多核对的欠账交易日。 |

## `cne repair fund-volume`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--apply` | `False` | 核验后替换整行并发布新版本，保留旧版。 |
| `--refresh-sources` | `False` | 归档官方历史 K 线及必要的独立金额证据；默认离线回放。 |
| `--max-symbols` | `16` | — |

## `cne repair index-history`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--index-symbol` | `399001.SZ` | 选择已核验的历史成分目录。 |
| `--apply` | `False` | 替换已核验区间的完整成分历史，保留官方带日期权重及旧版本。 |
| `--refresh-sources` | `False` | 补采目录中未归档的官方历史附件；默认离线回放。 |

## `cne repair index-snapshots`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--indices` | `—` | 指数代码，逗号分隔；默认沪深300、科创50、深证成指、创业板指。 |
| `--apply` | `False` | 按文件实际日期替换完整指数快照，保留旧版本。 |
| `--refresh-sources` | `False` | 取官方成分和权重文件；默认离线回放。 |

## `cne repair layout`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `dataset` | `—` | 必填；位置参数 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--apply` | `False` | 写入并发布新版本；默认只输出计划。 |

## `cne repair payment-dates`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--symbols` | `—` | 必填；逗号分隔的证券代码。 |
| `--start` | `—` | 必填；窗口起点（YYYY-MM-DD）。 |
| `--end` | `—` | 必填；窗口终点（YYYY-MM-DD）。 |
| `--issuer-notices-only` | `False` | 只用发行人实施公告（已审清单、巨潮、北交所）；匹配不到的事件保留为缺口，不请求 Baostock。 |
| `--apply` | `False` | 运行；不加只打印计划。 |

## `cne repair repartition`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `dataset` | `—` | 位置参数 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--all` | `False` | 所有布局需要改写的数据集。 |
| `--apply` | `False` | 改写并换上分区；默认只预览。 |

## `cne repair status-traded`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--start` | `—` | 必填；成交状态补缺起点（YYYY-MM-DD）。 |
| `--end` | `—` | 必填；成交状态补缺终点（YYYY-MM-DD）。 |
| `--apply` | `False` | 核验后发布新版本；默认只输出计划。 |

## `cne repair stock-listing-dates`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--apply` | `False` | 发布官方 IPO 日历证实的股票上市日期；保留旧版本。 |
| `--refresh-sources` | `False` | 联网取巨潮 IPO 日历；默认回放已归档来源。 |
| `--max-sessions` | `32` | — |
| `--event-date` | `—` | 额外回放指定日的官方上市事件，可重复；不要求证券已有日线。 |

## `cne repair valuation-outage`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--start` | `—` | 必填；窗口起点（YYYY-MM-DD）。 |
| `--end` | `—` | 必填；窗口终点（YYYY-MM-DD）。 |
| `--apply` | `False` | 运行；不加只打印计划。 |

## `cne run clean`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--dry-run` | `False` | 兼容选项；现在所有清理均只预览。 |
| `--orphan-retention-days` | `7` | 预览超过这么多天、且 manifest 里没有记录的孤儿 staging。 |
| `--snapshot-retention-days` | `14` | 预览超过这么多天的 meta/source_snapshots run_id 目录（每个数据集 / 源的最新一份始终保留）。 |
| `--force` | `False` | 将未完成或未 compact 的 staging 也列入预览，不删除文件、不降级批次。 |
| `--keep-revision-generations` | `5` | 每个数据集保留最近这么多代及 current/hold；其他版本只列出候选，不标记、不释放字节。网页标记前须导入引用清单。0 表示跳过版本处理。 |
| `--log-retention-days` | `30` | 预览超过这么多天的 `logs/cne-*.log`，不删除；0 表示跳过。 |
| `--reconcile-runs` | `False` | 清理前，把卡在 'running'（worker 崩溃）的 run 标成 failed。 |
| `--reconcile-after-seconds` | `—` | 只对静默超过这么多秒的 run 做上面的对账（默认取 [orchestrator].batch_stale_seconds）。 |

## `cne run compact`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--run-id` | `—` | 只发布这一次 run；默认处理所有待发布的 run。 |

## `cne run daily`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--groups` | `—` | 把 --stale-only 限定在这些 daily 调度组内（逗号或空格分隔）。 |
| `--group` | `—` | 调度组：core、capital、signals、fundamentals、macro_risk、research、intraday、ticks |
| `--all-groups` | `False` | 只跑全部调度组，不含事件流。给已经单独调度 `cne run events` 的旧定时任务保留；不带参数的 `cne run daily` 已经包含全部调度组和事件流。 |
| `--core-only` | `False` | 只跑 [[job.daily.waves]] 核心骨架（旧版不带参数时的行为）。 |
| `--no-events` | `False` | 不带参数运行时不跑事件流（公告、监管事件、资讯）。 |
| `--trade-date` | `—` | as-of 交易日 YYYY-MM-DD（默认今天）。周末 / 节假日补跑时用。 |
| `--backfill` | `False` | 用 backfill 语义跑：跳过交易日门禁和每个 step 自己的增量窗口，改为抓配置里 backfill scope 指定的窗口。用于补跑调度漏掉的某一天；日常调度从不带这个参数。 |
| `--repair-gaps` | `False` | 在 daily / stale 跑之前，先修复已验证、且有诚实来源的历史缺口。 |
| `--quiet` | `False` | 只留 warning 及以上，不打逐步进度。 |
| `--stale-only` | `False` | 只重抓仍然落后于最后交易日的数据集。挂在主 pipeline 几小时之后跑：snapshot 类数据集一旦因源端中断丢掉当天窗口，第二天就补不回来了。 |
| `--snapshots-only` | `False` | 配合 --stale-only：只重抓 snapshot 类数据集；历史类留给下一次日更按日期补。 |
| `--pack` | `—` | 只跑这些研究包对应的日更调度组，不跑事件流。不写则仍跑全部调度组和事件流。可重复。 |

## `cne run events`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--group` | `—` | [job.events.groups] 里的某一个组（默认按顺序跑全部）。 |
| `--trade-date` | `—` | as-of 自然日 YYYY-MM-DD（默认今天），含周末与节假日。 |
| `--quiet` | `False` | 只留 warning 及以上，不打逐步进度。 |

## `cne run maintain`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--groups` | `—` | 新鲜度门禁判失败的调度组（空格或逗号分隔）。默认：定时日更实际运行的组。 |
| `--notify` | `False` | 有问题时同时弹出桌面通知（macOS）。 |

## `cne run retry`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--run-id` | `—` | 重试指定的一次 run。 |
| `--failed-groups` | `False` | 重试每个 daily 调度组最近一次失败的 run。 |

## `cne schedule install`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--pack` | `—` | 只更新这些研究包的调度组，不含事件流。可重复。默认保留当前选择（首次安装为全部调度组）。 |
| `--no-stale` | `False` | 不在 [job.stale].run_at 之后重试缺失的快照。 |
| `--events-group` | `—` | 另按自己的间隔运行这个事件流组（公告、资讯）。 |
| `--events-every` | `60` | 配合 --events-group：两次运行之间的分钟数。 |
| `--events, --no-events` | `—` | 运行配置里的 [[job.events.schedules]]（或关闭）。默认保留现有设置；配置定义了计划且尚无设置时开启。 |
| `--yes, -y` | `False` | 不询问确认直接安装。 |

## `cne schedule remove`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--yes, -y` | `False` | 不询问确认直接移除。 |

## `cne schedule status`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--json` | `False` | 输出机器可读的 JSON。 |

## `cne serve`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--host` | `127.0.0.1` | 回环地址会同时监听 127.0.0.1 和 localhost。非回环地址必须配 --token。 |
| `--port` | `8787` | — |
| `--token` | `—` | 要求这个 bearer token（或 ?token=）。--host 不是回环地址时必须设置。 |
| `--read-only` | `False` | 只浏览。不注册操作页和存储清理的写入口。 |
| `--allow-remote-ops` | `False` | 非回环地址上也可以从面板发起取数。默认远程只能浏览和做存储清理；令牌在网址里，局域网又是明文。 |

## `cne snapshot create`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `name` | `—` | 必填；位置参数 |
| `--research` | `False` | 包含价格、因子、身份、状态、日历及研究覆盖证据；缺少依赖时报错。 |
| `--dataset` | `—` | 必填；要包含的数据集（可重复）。快照永远是显式指定的，不会是整个湖。 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--snapshot-root` | `—` | 快照放在哪；默认是数据根目录下的 meta/snapshots。 |

## `cne snapshot delta apply`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `name` | `—` | 必填；位置参数 |
| `target` | `—` | 必填；位置参数 |
| `--dry-run` | `False` | 只校验前置条件，不改动 TARGET。 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--snapshot-root` | `—` | 增量包放在哪；默认是数据根目录下的 meta/snapshots。 |

## `cne snapshot delta create`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `name` | `—` | 必填；位置参数 |
| `--from` | `—` | 基线湖根目录。目标根目录会与它逐字节比对。 |
| `--to` | `—` | 目标湖根目录；默认取配置里当前生效的根目录。 |
| `--from-revision` | `—` | 用目标里已提交的 revision 作为基线前置条件。 |
| `--dataset` | `—` | 要包含的数据集（可重复）。不给则自动发现两个根目录里都有的数据集。 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--snapshot-root` | `—` | 增量包放在哪；默认是数据根目录下的 meta/snapshots。 |

## `cne snapshot delta verify`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `name` | `—` | 必填；位置参数 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--snapshot-root` | `—` | 增量包放在哪；默认是数据根目录下的 meta/snapshots。 |

## `cne snapshot export`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `name` | `—` | 必填；位置参数 |
| `destination` | `—` | 位置参数 |
| `--compression` | `auto` | 归档编码；auto 优先 tar.zst，不行则退回 tar.gz。 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--snapshot-root` | `—` | 快照放在哪；默认是数据根目录下的 meta/snapshots。 |

## `cne snapshot import`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `archive` | `—` | 必填；位置参数 |
| `--name` | `—` | 导入后的快照名；默认取归档文件名。 |
| `--overwrite` | `False` | 仅在归档校验通过之后，才替换已存在的同名快照。 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--snapshot-root` | `—` | 快照放在哪；默认是数据根目录下的 meta/snapshots。 |

## `cne snapshot restore`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `name` | `—` | 必填；位置参数 |
| `target` | `—` | 必填；位置参数 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--snapshot-root` | `—` | 快照放在哪；默认是数据根目录下的 meta/snapshots。 |

## `cne snapshot verify`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `name` | `—` | 必填；位置参数 |
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--snapshot-root` | `—` | 快照放在哪；默认是数据根目录下的 meta/snapshots。 |

## `cne sources limits`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |

## `cne sources policy`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `source` | `—` | 位置参数 |
| `--profile` | `—` | — |
| `--redistribution` | `False` | — |

## `cne sources probe`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--vantage` | `CNE_SOURCE_VANTAGE, else [source_health].vantage, else local` | 这次探测是从哪儿跑的 —— 'cn'、'overseas'，或你自己用的任何标签。有几个源拒绝非大陆出口，所以没有这个标签的结果没法解读。 |
| `--only` | `—` | 要探测的 key，逗号分隔；高成本/已受挑战的端点仅在显式列出时探测。 |
| `--stale-only` | `False` | 复用 12 小时内已校验的采集证据，只主动探测缺少新证据的端点。 |
| `--list` | `False` | 离线列出探测源名称，不需要配置。 |
| `--out` | `—` | JSON 报告写到哪。默认写到湖内的 meta/source_health/<vantage>.json，`cne serve` 也从那里读。 |

## `cne sources resilience`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--out` | `—` | — |
| `--enforce` | `False` | 核心数据集没有独立备份时退出 1。 |
| `--with-availability` | `False` | 把实测的探测可用率接到每个故障域上（会读湖）。 |
| `--window-days` | `30` | — |

## `cne sources slo`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--window-days` | `30` | — |
| `--minimum-observations` | `10` | — |
| `--enforce` | `False` | 关键源的 SLO 未达标时退出 1。 |

## `cne sources substitutes`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--vantage` | `CNE_SOURCE_VANTAGE, else [source_health].vantage, else local` | 读哪个出口位置（vantage）的报告。 |
| `--probe, --no-probe` | `False` | 现在实测，而不是读已存的报告。请求和 `sources probe` 相同。 |
| `--json` | `False` | 输出机器可读的 JSON。 |

## `cne stats rebuild`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--dataset` | `—` | 只重算这些数据集（可重复）。其它数据集的行保持不变。 |
| `--if-stale` | `False` | 除非统计建好之后又跑过采集，否则什么都不做。可以安全地挂定时器。 |
| `--json` | `False` | 以 JSON 打印结果。 |

## `cne stats show`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--dataset` | `—` | 某一个数据集的逐分区明细。 |
| `--by-source` | `False` | 改为按 source / data_version 分组。 |
| `--json` | `False` | 输出机器可读的 JSON。 |

## `cne status`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--run` | `—` | 要看的 run id，或 'latest'（默认）。包含各数据集 stage 的结果。 |
| `--datasets` | `False` | 逐数据集的新鲜度：覆盖区间、水位，以及相对最后交易日是否陈旧。 |
| `--all-columns` | `False` | 配合 --datasets：打印数据集清单的全部列，而不只是新鲜度。 |
| `--hide-empty` | `False` | 配合 --datasets：只列有数据或已陈旧的数据集，还没有数据的折成一行。 |
| `--groups` | `—` | 配合 --datasets：只对这些调度组拥有的数据集判失败（空格或逗号分隔）。其它组的数据集照常列出、照常报为调度缺口，但不会让门禁失败。截面检查同样受它约束——daily_bars 归哪个组，它的覆盖率就归谁判。未跑完的 init 不属于任何调度组，始终判失败。 |
| `--scope, --no-scope` | `True` | 配合 --datasets：是否做最新交易日的标的截面校验。它要读 daily_bars 的 tip 分区、instruments 和 trading_status，比单纯看水位贵；--no-scope 让这条命令回到纯元数据。 |
| `--gate` | `False` | 按所选范围和新鲜度执行门禁；默认只报告。 |

## `cne storage apply`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `plan_id` | `—` | 必填；位置参数 |
| `--phase` | `mark` | mark 原地标记；purge 已禁用，请转到 serve 存储运维页。 |
| `--maintenance-window` | `False` | 兼容选项；不能绕过网页删除确认。 |

## `cne storage explain`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `object_id` | `—` | 必填；位置参数 |

## `cne storage hold`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `object_id` | `—` | 必填；位置参数 |
| `--reason` | `—` | 必填 |

## `cne storage import`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--manifest` | `—` | 必填 |

## `cne storage inspect`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--keep` | `5` | — |

## `cne storage plan`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--keep` | `5` | — |
| `--phase` | `mark` | — |

## `cne storage raw-external`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--dataset` | `—` | 必填 |
| `--source` | `—` | 必填 |
| `--root` | `—` | 必填 |
| `--format` | `pdf` | — |

## `cne ths-official backfill`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--start` | `2016-01-01` | 起始报告期。 |
| `--end` | `2024-12-31` | 结束报告期。 |
| `--chunk-size` | `200` | 每个 staging 批次放多少只证券。 |
| `--workers` | `4` | 并发请求数。 |
| `--symbols` | `—` | 用逗号分隔的标的列表代替整个市场，定向重试失败证券；收尾 JSON 会在 `failed_symbols` 里列出仍未补齐的证券。 |

## `cne ths-official capture`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--what` | `all` | 抓取哪一种对端快照。 |
| `--days` | `45` | K 线窗口，按自然日算。 |
| `--sample` | `400` | K 线抽样多少只证券。 |

## `cne ths-official repair-bars`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--start` | `2005-01-01` | — |
| `--end` | `2015-12-31` | — |
| `--apply` | `False` | 真正写入。不加它只报告差异，不改任何东西。 |
| `--adjudicator` | `—` | 来自独立源的 (symbol, trade_date, close) parquet。 |
| `--diff-out` | `—` | 把有争议的行写到这里。 |
| `--workers` | `4` | — |

## `cne ths-official resource-sectors`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--start` | `2022-01-04` | 服务起点。 |
| `--end` | `—` | 默认今天。 |
| `--apply` | `False` | 真正写入。不加它只抓取并报告，不改任何东西。 |
| `--workers` | `4` | — |

## `cne verify`

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--config` | `configs/cnequity.toml, else ~/.config/cnequity/config.toml` | 配置文件路径。 |
| `--derivatives` | `False` | 只读验收衍生品研究窗口；需 --dataset、--start、--end。 |
| `--industry` | `False` | 只读检查 industry_members 按分类体系、层级、快照日的完整性；可用 --start/--end 缩小范围。 |
| `--bars` | `False` | 改为检查「证券 × 交易日」，含窗口内一行都没有的证券。需要 --start。 |
| `--runs` | `False` | 改为检查连续交易日的运行证据，不补任何缺口。 |
| `--dataset` | `—` | 只校验这些数据集（逗号分隔）；默认校验注册表里的全部。 |
| `--repair` | `False` | 把能补的缺口跑一遍回填，按数据集从新到旧。 |
| `--kind` | `—` | 只看这些缺口类型：empty,stale,interior,shallow。 |
| `--start` | `—` | 配合 --bars/--derivatives：覆盖窗口起点（含）。 |
| `--end` | `—` | 覆盖窗口终点；--bars 默认上一个完整交易日，--derivatives 必填。 |
| `--days` | `—` | 配合 --runs：要求连续多少个交易日（默认 20）。 |
| `--as-of` | `—` | 配合 --runs：截止日期 YYYY-MM-DD（含）。 |
| `--enforce` | `False` | 配合 --runs：连续天数门禁没过就退出 1。 |
