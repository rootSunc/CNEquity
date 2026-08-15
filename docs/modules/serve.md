# serve 模块

只读湖面板。`cml serve` 起 uvicorn，提供 `/api/*` JSON 与一个页面。

| 文件 | 职责 |
|------|------|
| `app.py` | FastAPI app 工厂、pydantic 响应模型、路由、token 中间件 |
| `lake.py` | `LakeView` — 把注册表、目录布局、`meta/stats`、`meta/quality`、manifest 投影成面板要的形状 |
| `static/index.html` | 页面外壳（HTML + CSS） |
| `static/bundle.js` | 行为与图表，由 `frontend/` 构建，**已提交** |

---

## 两条硬边界

**只读。** 没有任何端点会跑批、重试或清理，将来也不会：一个无鉴权的本地 HTTP 服务能触发采集就是负债，而 CLI 已经是那些操作的正确入口。页面显示该跑的命令并提供复制。`test_no_route_can_mutate_the_lake` 断言路由表里只有 `GET`/`HEAD`。

唯一的例外恰好证明规则——`meta/stats` 会在后台重建，因为它是**湖的缓存**而不是湖的一部分，而一个端着上周数字的面板比一个会刷新自己缓存的面板更糟。

**请求路径不扫 curated。** 覆盖、行数、体积、溯源全部来自已落盘的产物。一个会读 parquet 的请求，是一个随湖增长而变慢的请求——这正是度量表存在的理由。

例外是**数据 tab**：它的职责就是给你看真实的行，所以它读 parquet——但只读选中分区，并且行数有硬上限。

---

## 两个刻意不做的事

**不打开 `data/duckdb/cn-market-lake.duckdb`。** DuckDB 是「多读或单写」，面板持着读句柄会让夜间跑批里的 `ensure_duckdb_views()` 拿不到写锁——面板会搞挂它在报告的那条流水线。视图在进程私有的内存库里从注册表重建，是毫秒级的。

**不重算审计 findings。** `lake_health()` 要走一遍湖；面板读 `cml audit --full` 已经写好的 `meta/quality/health-latest.json`。一次页面访问不该花一次审计的代价——代价是显示的是上次审计的快照，页面上标了日期。

---

## 端点

| 端点 | 内容 |
|------|------|
| `GET /api/health` | 锚定交易日、fresh/stale/empty 计数、总行数与体积、findings 分级、度量表新鲜度 |
| `GET /api/tiers` | L0–L8 汇总（数据集数、各状态计数、行数、体积、成员） |
| `GET /api/datasets?tier=` | 逐数据集：注册表字段 + 覆盖 + 水位 + 度量 |
| `GET /api/datasets/{name}` | 详情：注册表契约 + schema + 主键 + 缺口 + findings + 建议命令 + 最近 batch |
| `GET /api/datasets/{name}/partitions` | 逐分区行数与体积序列 |
| `GET /api/datasets/{name}/provenance` | source × data_version 合计与 `fetched_at` 跨度 |
| `GET /api/datasets/{name}/provenance/series` | 同上但按时间分桶——source 分布**何时**变的 |
| `GET /api/datasets/{name}/dates` | 可选日期值 + `kind`（渲染层据此选控件） |
| `GET /api/datasets/{name}/rows` | 一页真实的行；`period` / `symbol` / `as_of` / `adjust` / `limit` / `offset` |
| `GET /api/heatmap?days=` | 数据集 × 交易日覆盖网格 |
| `GET /api/runs?limit=` | 最近 run + 各状态 batch 计数 |
| `GET /api/runs/{run_id}` | 该 run 的全部 batch（含 `stalled`） |
| `GET /api/stream/runs/{run_id}` | SSE：跑批中的 batch 实时变化 |
| `GET /api/quality?limit=` | findings 时间线、跨源差异、隔离区、on-demand 缓存 |
| `GET /api/quality/runs/{run_id}` | 该 run 的完整 findings 与跨源差异 |
| `GET /api/docs` | OpenAPI 页，由 handler 生成，不会与实现漂移 |

`empty` 拆成 `empty_optional` / `empty_required`：一个没人开启的可选数据集和一个失败的必需数据集在磁盘上长得一模一样，混在一起报会让人学会忽略它。

---

## 热力图的诚实性

格子回答的是「**存不存在一个覆盖这天的分区**」。对月/季/年分区的数据集，这比它画在上面的那一天要粗——目录覆盖的是整个周期，某一场具体交易日在里面有没有行，不读文件是不知道的。`granularity` 随每行返回，渲染层据此说明，而不是暗示一个布局并不具备的精度。

**缺口是不是故障，由 `gap_meaning` 说了算，规则在服务端。** 两种情况下空洞属于形态而非错误，而且都会让大半张图变红：

- **非日更**：`northbound_holdings` 是季频的，跨度内几乎每个交易日本来就没有分区。
- **snapshot 语义**：snapshot 数据集每个 run 落一份带戳的读数；没跑的那天没有快照，而且**给不出来**——重放会伪造行。这正是 `fetch_semantics` 存在的意义。

只有 `by_date` 且日更的数据集，才谈得上「少了本该有的一天」。判据读自 `fetch_semantics`，不是一份手工维护的名单，有测试遍历每个注册数据集守着这条。

单元格字母表：`#` 有覆盖、`.` 缺口、空格 覆盖区间外、`-` 无分区（单文件 merge）。一行一个字符串而不是一万个 JSON 对象。

概览页默认展示最近 **90 个交易日**，并隐藏当前窗口内完全没有分区的行；这些数据集仍会在「数据层」里显示。这样首屏关注的是已有覆盖的形状，而不是把尚未启用的可选数据集画成一大片空白。需要更长窗口时可以打开 `/?days=250` 等地址，API 仍支持最多 750 个交易日。

---

## 详情页的三个 tab

**状态**：覆盖条（含源端视野天花板）、缺口、溯源堆叠图、溯源合计表、审计 findings、最近 batch。
**元数据**：契约（分层 / 分区键 / 粒度 / 主键 / schema）、语义（`fetch_semantics` / `history_mode` / PIT / 水位）、来源（回填源 / 视野 / 最早可得）、运维（容忍天数 / required / 分块 / 行粒度）、可复制的命令。
**数据**：见下。

元数据全部来自 `domain/datasets.py` 与 `domain/schemas.py`——面板不复制一份。面板自己存一份契约，就是第二份会漂移的契约。

### 两个字段：为什么面板不能只看一个

这两处都是 `trade_ticks` 落地时暴露出来的——**一个 null 值不等于「没有限制」**，只等于「这个机制不适用」。

**源端视野**：`history_horizon_days`（滚动的每标的根数上限，分钟线）与 `history_floor_date`（固定日历底，分笔）是两种机制，都会产出 `earliest_available`。只读前者会把分笔渲染成「无上限」，而紧邻的「最早可得」写着 2024-01-02——面板自相矛盾。所以两个字段都进 payload，面板按机制分别措辞：

- `自 2024-01-02 起（固定底，不滚动）`
- `95 个交易日（滚动）`

区别是运营上真实的：滚动视野意味着窗口往前滑、老数据会掉出去；固定底意味着它只会变长。

**行粒度**：`intraday_frequency` 是**行为字段**——它的消费者（audit 的会话检查、reader 的复权集合、`cml backfill --symbols`）都假定存在 `bar_time` 列和「每交易日 N 根」。`trade_ticks` 故意不设它，正是为了不继承这些 bar 形状的代码路径。代价是面板显示 `—`，日内分笔看起来和日频数据集一样。

新增的 `row_grain` 纯描述、不带行为，三个日内数据集都设（`1m` / `5m` / `tick`），面板显示 `日` / `1m bar` / `5m bar` / `分笔（3 秒快照聚合，非 bar）`。两者同时存在时必须一致，`test_dataset_registry` 守着。

**缺口按数据集自己的周期计数**，不按天：一个年分区的数据集不会因为一个目录覆盖整年就「缺 364 天」，那样报会把真缺口淹掉。日粒度只算交易日——周末不是缺口。

### 两个不内联的东西

`partitions_detail` 不进详情响应：daily_bars 一个就有 6202 条，而详情在每次切 tab 时都要加载，逐分区序列只有一张图用得上。走 `/partitions`。

溯源序列**服务端分桶**：daily_bars 的 (日, source) 点有 11,324 个，一兆 JSON 画几百像素。桶宽逐级放大到序列装得下，并把选中的宽度（`bucket`）随响应返回——不告诉调用方它在看年度数据，坐标轴就没法诚实标注。

---

## 数据 tab（行浏览）

**日期控件由服务端的 `kind` 决定，不靠猜。** 注册表横跨 12 个日期列、4 种形态：

| kind | 数据集 | 控件 |
|------|--------|------|
| `trading_day` | `trade_date` 日分区 | 交易日下拉 |
| `event_day` | `announce_date` 等稀疏事件日 | 事件日下拉 |
| `period` | 月 / 年分区 | 周期下拉 |
| `report_period` | 3 个 `report_period` 数据集 | 报告期下拉（`2026Q2`） |
| `none` | `instruments` / `delisting_events` | 无控件——单文件 merge 没有「按日期取数」 |

日历控件套在 `report_period` 上会诱导一个该列答不了的查询；套在稀疏事件列上则大半是没数据的日子。**只提供真实存在的值**——这也是 `snapshot_only` 警告的诚实版本：那些数据集没跑的那天补不出来，所以它压根不在列表里，同时附一句说明。

**`report_period` 按值相等过滤，不按日期区间。** 它是 String 列，日期区间会拿文本去比日期。

**PIT 数据集必须给 `as_of`，面板不替它糊过去。** `load()` 一直是这个契约——没有 as_of 就没有「当前」视图，因为改述会存第二个 vintage 而不是覆盖第一个。页面把 as_of 默认成今天让 tab 打开时有东西看，并在 tooltip 里说明含义。

**复权控件只在 `adjustable` 的数据集上出现**，判据来自 `reader.ADJUSTABLE_DATASETS`。

**溯源列不会为了腾地方被丢掉。** 行级溯源是这个湖的卖点，藏起来等于教用户以为它不存在。

行读走 `query.reader.load()`——面板看到的和研究者 `load()` 拿到的是同一份东西（复权、PIT 收敛都一样），而不是第二条微妙不同的读路径。`limit` 硬上限 1000：这是个查看器，批量取数是 Python 里的 `load()`，不是一个翻页 URL。

---

## 跑批视图与实时流

`stalled` 不是 manifest 的列，是推出来的：**仍是 `running` 但静默超过 `batch_stale_seconds`**。引擎下次跑批会用同一个阈值把这种 batch 判为 failed，所以提前显示出来，等于让人先看到引擎即将得出的结论——否则一个死掉的 worker 和一个慢 worker 在界面上长得一样。

**SSE 是轮询出来的，不是被推送的。** 写 manifest 的是独立的 worker 进程，SQLite 没有通知通道。所以流每 2 秒比一次 `run_fingerprint`（一个把 batch 数、最大 `finished_at`/`heartbeat_at`、行数、重试数和状态串拼起来的廉价值）——没变就只发一个 keepalive 注释帧，闲置订阅者几乎不花钱。轮询在线程里做：SQLite 是阻塞的，放在事件循环上会卡住其它所有请求。

run 结束时流主动关闭，而不是让客户端挂着等永远不会来的事件。

**页面上的时钟是本地走的，不靠流。** 实测一个日内 backfill 在 70 秒里只产生 1 个变化帧——heartbeat 远不到每秒一次。只靠流的话，「● 实时」徽标下面的耗时和进行中那根 bar 的右端会是冻住的，那比不声称实时更糟。所以数据仍然只来自流，只有「现在」是本地推进的。

---

## 质量视图

四样东西，全部是已落盘的产物，面板不重算任何一项。

**findings 时间线** 读 `meta/quality/findings/*.json`。那个目录里也躺着别的检查写的产物（`authority-<date>.json`），结构完全不同——按 run findings 去读只会得到胡话。用「文件名是不是 UUID」区分，不是靠猜。

**跨源比对** 读 `source_diffs/*.json`。页面上专门写了一句：`no_overlap` 的意思是「两边没有共同主键可比」，**不是「一致」**——那是这张表最容易被读反的一行，而读反的方向恰好是让人放心的那个方向。

**隔离区** 是 `_quarantine/` 的目录清单，带文件数、体积和最后修改时间。文案明说它**不是垃圾桶**：里面每一样都是因为有问题而被撤出 curated 的数据，留着当证据；量出体积是为了让人能判断这份证据还值不值这些磁盘。参考湖里 `trading_calendar_superseded_day_parts` 就有 4,220 个文件——正是 `domain/partitions.py` 文档里讲的那个日分区退化案例的现场。

**on-demand 缓存** 读 `meta/on_demand/`。这些数据集不在 `DATASETS` 里、也不进 curated，所以面板别处一概看不见它们。空列表意味着还没人查过，页面直说这是正常状态而不是缺口——否则一个空表会被当成故障。

---

## 鉴权与绑定

默认绑 `127.0.0.1`。`--host` 指向非回环地址时**必须**给 `--token`，否则拒绝启动——这个检查排在读配置之前，免得 `--config` 打错字先失败、把绑定守卫盖掉。

token 支持 `Authorization: Bearer <token>` 与 `?token=`（页面用后者：浏览器发不了自定义头）。

---

## 缓存

`LakeView` 对目录遍历结果做 30 秒 TTL 缓存——总览页一次扇出好几个端点，否则每个都要重走一遍。缓存构建在锁外进行：两个并发 miss 各做一次，比让每个请求排在一次目录遍历后面便宜。

后台刷新的线程策略在这里，不在 `storage.stats`——那个模块保持同步、可测试。同一时刻只跑一个：stats 自己的锁本来就会收敛重复，但每个请求起一个线程再立刻丢锁是纯浪费。

---

## 前端

源码在 `frontend/`，用 esbuild 打成 `static/bundle.js`，**产物已提交**——`pip install` 不需要 node，只有改面板的贡献者需要，CI 会重建并 diff，两者不会漂移。细节见 [frontend/README.md](../../frontend/README.md)。

ECharts 走 `echarts/core` 显式注册而非预构建文件：1.1MB → 596KB（gzip 205KB），砍掉的一半是这个面板不画的图表类型。

**没有 CDN。** 页面唯一的 script 是同源的、和它一起打包的；有测试断言页面里不出现任何外链。湖经常跑在离线机器或需要代理的网络里，那里的一个外部资源就是一个打不开的页面。

**bundle 的 URL 带 mtime 戳。** `StaticFiles` 不发 `Cache-Control`，浏览器就按启发式新鲜度直接复用缓存里的 `/static/bundle.js`，不去 revalidate——升级之后就成了「新 API + 旧 JavaScript」，两边都解释不了的故障。用 mtime 而不是包版本：装 wheel 会刷新它，本地重建也会，而版本号只管前者，会让开发者拿昨天的 JS 测今天的代码（这个坑本人踩过）。

### 图表配色

分类色板取自 dataviz 参考实例的前 5 槽，两个模式都跑过校验器（最差相邻 CVD ΔE 9.1 light / 8.4 dark，正常视觉 19.6 / 19.3）。light 模式有三个槽低于 3:1 对比度，所以**图例与图下的合计表是必需的 relief**，不是装饰。

source 按名称字母序占槽，不按行数排名——一个源恰好长大了不该让整张图重新上色。超过 5 个折进中性色的「其他」，不循环取色。

---

## 相关文档

- [CLI 参考 · cml serve](../reference/cli.md#cml-serve)
- [storage 模块 · stats.py](storage.md#statspy)
- [frontend/README.md](../../frontend/README.md)
