# MCP：把湖接给 AI agent

`asl serve` 把湖给人看，`asl mcp` 把湖给模型用。同样**只读**：这里没有任何触发采集、重试、清理的入口，采集仍然只在 CLI 上，由人来跑。

```bash
claude mcp add ashare-lake -- asl mcp --config /abs/path/to/ashare-lake.toml
```

传输是 stdio：客户端拉起进程，在管道上讲 JSON-RPC，不需要手动执行。

**`--config` 和配置里的 `[data].root` 都要用绝对路径。** MCP 客户端从哪个目录启动进程是不确定的，而相对的 `data.root` 是相对**工作目录**解析的——于是湖解析到一个不存在的路径，每个工具都回「no parquet data」，agent 如实报告「没有数据」。这句话对那个路径是真的，对你的湖是假的。

`asl config init` 写出的配置本来就是绝对路径。启动时会检查 curated 下是否有 parquet，没有就直接退出并打印解析后的路径，而不是伺服一个空湖。

其它客户端（Codex、Cline、任何支持 MCP 的编辑器）填等价的配置即可：

```json
{
  "mcpServers": {
    "ashare-lake": {
      "command": "asl",
      "args": ["mcp", "--config", "/abs/path/to/ashare-lake.toml"]
    }
  }
}
```

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

## run_sql 的边界

只接受**一条 SELECT**，且用 DuckDB 自己的解析器判定，不是正则。

湖里有 `news_headlines`、`flash_news_wire` —— 供应商文本，不是我们写的内容，而 agent 会读它。也就是说到达这个工具的 SQL 可能被湖里摄入的内容影响。解析器能把 `SELECT ... -- ; DROP` 和两条语句分清楚，正则不能。连接本身也是 read-only，两者互补：read-only 拦不住 `COPY ... TO`（它写的是数据库文件之外的路径）。

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

服务端是手写的 stdio JSON-RPC 循环（`ashare_lake/mcp_server/protocol.py`），不用官方 `mcp` SDK。原因写在模块 docstring 里：`mcp` 2.0 会拉进 15 个包——OAuth 用的 cryptography / pyjwt / truststore、没人导出的 opentelemetry，以及本项目已锁 httpx 之外的**第二套 HTTP 栈**。而 `pip install ashare-lake` 无 extras 装齐一切这条承诺，比这 200 行代码值钱。

需要 sampling / roots / elicitation / HTTP 传输时再换 SDK——那才是它挣回体积的地方。

---

## 排障

**客户端报 parse error，服务端看着正常。** stdout 就是 JSON-RPC 线路，任何多余输出都会污染它。`asl mcp` 已经把日志定向到 stderr（MCP 客户端会当作服务端日志收集）；如果你在 fork 里加了 `print`，那就是原因。

**工具报 "no parquet data for dataset X"。** 该数据集在这个湖里还是空的。调 `describe_lake --include_empty` 看注册了但没数据的清单，或跑对应的 step。

**读到的不是你以为的那个湖。** 检查 `--config` 是不是绝对路径；`describe_lake` 的返回里第一项就是 `data_root`。

---

## 相关文档

- [CLI](cli.md#asl-mcp) · [Python API](python-api.md)
- [数据集目录](../datasets/catalog.md) · [查询指南](../datasets/query-guide.md)
