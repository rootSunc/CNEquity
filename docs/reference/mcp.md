# MCP：把湖接给 AI agent

`cml serve` 把湖给人看，`cml mcp` 把湖给模型用。同样**只读**：这里没有任何触发采集、重试、清理的入口，采集仍然只在 CLI 上，由人来跑。

当前实现是标准 MCP over stdio，不绑定 Claude 或任何特定模型。客户端拉起
`cml mcp` 子进程，在 stdin/stdout 管道上交换 JSON-RPC；支持 stdio MCP 的
agent 都可以复用同一条命令和配置。

三条路，按你手上有什么选：

```bash
# ① 已经有湖 —— 完整口径
cml mcp --config /abs/path/to/cn-market-lake.toml

# ② 还没有湖，先试试 —— cml demo 给 30 秒真数据
cml demo
cml mcp --config /abs/path/to/configs/cn-market-lake.demo.toml

# ③ 不建湖 —— 现拉现给，不落盘
cml mcp --config /abs/path/to/cn-market-lake.toml --live
```

传输是 stdio：客户端拉起进程，在管道上讲 JSON-RPC，不需要手动执行。

**`--config` 和配置里的 `[data].root` 都要用绝对路径。** MCP 客户端从哪个目录启动进程是不确定的，而相对的 `data.root` 是相对**工作目录**解析的——于是湖解析到一个不存在的路径，每个工具都回「no parquet data」，agent 如实报告「没有数据」。这句话对那个路径是真的，对你的湖是假的。

`cml config init` 写出的配置本来就是绝对路径。启动时会检查 curated 下是否有 parquet，没有就直接退出并打印解析后的路径，而不是伺服一个空湖。

在客户端的 MCP 配置中，把上面的命令填成 `command` / `args`。下面是常见
的通用 JSON 形状；不同 agent 的文件位置和 UI 名称可能不同，但 server
参数不变：

```json
{
  "mcpServers": {
    "cn-market-lake": {
      "command": "cml",
      "args": ["mcp", "--config", "/abs/path/to/cn-market-lake.toml"]
    }
  }
}
```

兼容性边界：

| 客户端连接方式 | 当前支持 | 说明 |
|---|---|---|
| 本地 stdio 子进程 | ✅ | 任意支持 MCP stdio 的 agent，包括 Codex、Claude、Cline、Cursor、Windsurf、Gemini CLI 等 |
| MCP Streamable HTTP / URL | 尚未提供 | 当前 `cml mcp` 没有 HTTP listener；需要 URL 型远程部署时应使用反向代理/本地 stdio bridge，或后续启用 HTTP transport |

所以“是否支持某个 agent”取决于它是否支持 MCP stdio，而不是模型名称。若
目标 agent 只能接收 URL，需单独增加 Streamable HTTP 传输，不能把 stdio
命令伪装成 HTTP 服务。

---

## 为什么是 6 个工具，而不是 38 个

agent 每轮都要从一个平铺列表里选工具。按数据集给工具会让上下文里大半是它这次不会调的名字，而且它仍然不知道哪个能回答问题。这里按**问题形状**切：描述、找代码、读行情、读财报、读其它、聚合——数据集降级成参数。

| 工具 | 用途 |
|------|------|
| `describe_lake` | 湖里有什么、覆盖到哪、以及让答案正确的口径。每个会话先调它 |
| `resolve_symbol` | 「茅台」→ `600519.SH`。含退市股，带 `delist_date` 标记 |
| `query_bars` | 日线 / 指数 / 分钟线，带 `adjust` 与 `universe` |
| `query_fundamentals` | 财报科目，**必须**给 `as_of`（PIT） |
| `query_dataset` | 其余任意数据集，按日期与 symbol 过滤 |
| `run_sql` | 单条只读 DuckDB SELECT，跨数据集聚合 / 排名 / 分位数 |

### 口径写在响应里，不是写在文档里

模型不会去读 `docs/`。所以三条最容易产生「自信的错误答案」的规则直接放进返回值：

- `describe_lake` 的 `contract` 字段列出复权、PIT、`snapshot_only`、`history_horizon_days`、`universe` 的含义。
- `query_bars` 不带 `adjust` 时返回 `warning`；带了但有行缺因子时，报出「N/M 行 `adj_is_exact=false`」。
- `query_fundamentals` 不给 `as_of` 直接报错并解释为什么没有默认值——默认成今天，等于用今天的信息回答历史问题，而 agent 无从察觉。

### 分页永远说实话

每个响应都带 `total` / `returned` / `truncated`。只告诉模型这 200 行，它会把 200 行的均值当成全市场的均值报出去；`truncated` 与 `note` 就是让它改用 `run_sql` 的开关。

### 溯源是汇总的，不是逐行的

curated 每行都带 `source` / `data_version` / `fetched_at`，逐行返回会让行情载荷大出约三倍去重复同样的三个值。默认返回 `sources` 汇总；需要逐行时传 `include_provenance: true`。

---

## `--live`：没有湖也能接，但要知道少了什么

`--live` 让「湖里没有」的查询**现场去源上拉、不落盘**，直接给 agent。它是**入口，不是终点**。

**能力只有两项**：`resolve_symbol` 和**未复权**日线。其余全部明确拒绝——不是返回空，是报错并说明原因，因为空结果会被 agent 读成「这件事没发生过」。

| 工具 | live 下 | 为什么 |
|--|--|--|
| `resolve_symbol` | ✅ | 但用的是当前证券主数据，**退市股根本不在里面** |
| `query_bars`（daily_bars） | ✅ 未复权 | 带 `adjust` / `universe` 直接报错，见下 |
| `query_bars`（分钟线 / 指数） | ❌ | 行情协议这条路只给日线 |
| `query_fundamentals` | ❌ | 源返回的是「今天看到的」重述后数值，没有诚实的 `as_of` |
| `query_dataset` | ❌ | 每个数据集是各自的适配器 + 分页 + 质量检查，一次性拉是「披着同样列名的另一个序列」 |
| `run_sql` | ❌ | 它查的是磁盘上的 parquet，而 live 什么都不写 |

**为什么 `adjust` 会被拒绝而不是「尽力而为」**：复权因子是本项目从新浪单独派生、存进湖里的一个数据集，不是行情源挂在 bar 上的字段。把未复权价当复权价给出去，跨一次除权就是错的，而且**从数字上看不出来**。

**每条 live 响应都带 `origin: "live"` 和一段警告**，列明缺的是什么（复权、universe、PIT、写前校验、溯源）。湖里来的带 `origin: "lake"`——两边都标，所以「没有 origin 字段」不会被默认当成湖。

**默认关闭，永不自动推断。** 一个有湖的用户如果湖坏了，必须拿到「no parquet data」然后去修，而不是悄悄拿到一份来自别处的、看起来差不多的答案。

**每次调用有上限**：最多 50 个标的、800 天，且**必须显式给 `symbols`**——agent 会循环，而这些正是日更流水线依赖的主机；让模型自作主张扫全市场，是替用户挣一个他没问过的问题换来的封禁。

除了限速器自己的状态文件（那正是给请求限速用的），**磁盘上不会多出任何东西**，有测试守着。

---

## run_sql 的边界

只接受**一条 SELECT**，且用 DuckDB 自己的解析器判定，不是正则。

湖里有 `news_headlines`、`flash_news_wire` —— 供应商文本，不是我们写的内容，而 agent 会读它。也就是说到达这个工具的 SQL 可能被湖里摄入的内容影响。解析器能把 `SELECT ... -- ; DROP` 和两条语句分清楚，正则不能。连接本身也是 read-only，两者互补：read-only 拦不住 `COPY ... TO`（它写的是数据库文件之外的路径）。

SQL 连接还会被限制为只允许访问 `curated/` 和 `derived/` 两个 lake 目录，关闭 DuckDB external access 及扩展自动安装/加载，并锁定这些配置。因此 `read_text`、`read_csv`、`read_parquet`、HTTP URL 等指向 lake 外部的文件访问会失败；这不是操作系统沙箱，部署不可信 agent 时仍应使用进程/容器级隔离。

被拒绝的例子：多语句、`DROP`、`CREATE`、`ATTACH`、`COPY ... TO`。

`daily_bars_adj` 是现成视图，带 `hfq_*` / `qfq_*` 与 `adj_is_exact`，优先用它而不是自己 join `adj_factors`。

---

## 它能答、而「取数 skill」类项目答不了的问题

差异不在数据源，在于有没有历史序列：

- 「茅台过去五年 PE 的历史分位数，现在在什么位置」——需要 `valuation_metrics` 的多年日频序列
- 「2018 年这个财报因子的 IC 是多少，别用未来数据」——需要 `as_of` 的 PIT 切片
- 「过去三年退市的股票，退市前 60 天什么形态」——需要退市股仍在湖里

这些不是「还没做」，是**没有湖就做不到**：临时 HTTP 调用变不出历史。

---

## 依赖：没有新增

服务端是手写的 stdio JSON-RPC 循环（`cn_market_lake/mcp_server/protocol.py`），不用官方 `mcp` SDK。原因写在模块 docstring 里：`mcp` 2.0 会拉进 15 个包——OAuth 用的 cryptography / pyjwt / truststore、没人导出的 opentelemetry，以及本项目已锁 httpx 之外的**第二套 HTTP 栈**。而 `pip install cn-market-lake` 无 extras 装齐一切这条承诺，比这 200 行代码值钱。

需要 sampling / roots / elicitation / HTTP 传输时再换 SDK——那才是它挣回体积的地方。

---

## 排障

**客户端报 parse error，服务端看着正常。** stdout 就是 JSON-RPC 线路，任何多余输出都会污染它。`cml mcp` 已经把日志定向到 stderr（MCP 客户端会当作服务端日志收集）；如果你在 fork 里加了 `print`，那就是原因。

**工具报 "no parquet data for dataset X"。** 该数据集在这个湖里还是空的。调 `describe_lake --include_empty` 看注册了但没数据的清单，或跑对应的 step。

**读到的不是你以为的那个湖。** 检查 `--config` 是不是绝对路径；`describe_lake` 的返回里第一项就是 `data_root`。

---

## 相关文档

- [CLI](cli.md#cml-mcp) · [Python API](python-api.md)
- [数据集目录](../datasets/catalog.md) · [查询指南](../datasets/query-guide.md)
