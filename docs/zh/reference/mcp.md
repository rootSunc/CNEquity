---
description: 把 A 股本地数据湖通过只读 MCP 接给 Claude、ChatGPT、Cursor 等 AI agent，直接用自然语言查询股票数据。
---

# MCP 接入：把湖接给 AI agent

`cne serve` 把湖给人看，也可以从操作页发起白名单里的取数和日更。`cne mcp` 把湖给模型用，并且**只读**：这里没有任何触发采集、重试、清理的入口。

当前实现是标准 MCP over stdio，不绑定 Claude 或任何特定模型。客户端拉起
`cne mcp` 子进程，在 stdin/stdout 管道上交换 JSON-RPC；支持 stdio MCP 的
agent 都可以复用同一条命令和配置。

三条路，按你手上有什么选：

```bash
# ① 已经有湖 —— 完整口径
cne mcp --config /abs/path/to/cnequity.toml

# ② 还没有湖，先试试 —— cne init --profile demo 抓小范围真数据
cne init --profile demo
cne mcp --config /abs/path/to/configs/cnequity.demo.toml

# ③ 不建湖 —— 现拉现给，不落盘
cne mcp --config /abs/path/to/cnequity.toml --live
```

默认传输是 stdio：客户端拉起进程，在管道上讲 JSON-RPC，不需要手动执行。只接远程 URL 的客户端（ChatGPT）用 `--http`，见下文[ChatGPT](#chatgpt)。

**`--config` 请写绝对路径。** MCP 客户端从哪个目录启动进程是不确定的，相对的 `--config` 会解析到别的位置。配置里的相对 `[data].root` 按配置文件所在目录解析，不受启动目录影响。省略 `--config` 时按统一顺序查找，用户级 `~/.config/cnequity/config.toml` 在任何目录都能找到；但写明绝对路径最不容易接错湖。

`cne config create` 写出的配置本来就是绝对路径。启动时会检查 curated 下是否有 parquet，没有就直接退出并打印解析后的路径，而不是伺服一个空湖。

## 接入各家客户端

下面的 `/abs/path/to/cnequity.toml` 换成你的配置绝对路径；只想先试试就用 `cne init --profile demo` 打印出的 demo 配置路径。

| 客户端 | 传输 | 接入方式 |
|---|---|---|
| Claude Code | stdio | 命令行一条 |
| Claude Desktop | stdio | `claude_desktop_config.json` |
| ChatGPT | HTTP | `cne mcp --http` + HTTPS 隧道 + 开发者模式连接器 |
| Codex（CLI / IDE 插件） | stdio | 命令行一条，或 `~/.codex/config.toml` |
| Gemini CLI | stdio | `~/.gemini/settings.json` |
| Cursor | stdio | `~/.cursor/mcp.json` |
| VS Code（Copilot agent 模式） | stdio | `.vscode/mcp.json` |
| Windsurf、Cline、Cherry Studio、Trae、LM Studio 等 | stdio | 在 MCP 设置里填同一条命令和参数 |

是否支持某个客户端，取决于它支持 stdio 还是只接远程 URL，与背后用哪家模型无关。

### Claude Code

```bash
claude mcp add cnequity -- cne mcp --config /abs/path/to/cnequity.toml
```

加 `-s user` 让所有项目都能用。

### Claude Desktop、Cursor、Gemini CLI、Windsurf

这几家用同一种 JSON，只是文件位置不同：Claude Desktop 在「设置 → 开发者 → 编辑配置」打开的 `claude_desktop_config.json`；Cursor 是 `~/.cursor/mcp.json`；Gemini CLI 是 `~/.gemini/settings.json`；Windsurf 是 `~/.codeium/windsurf/mcp_config.json`。

```json
{
  "mcpServers": {
    "cnequity": {
      "command": "cne",
      "args": ["mcp", "--config", "/abs/path/to/cnequity.toml"]
    }
  }
}
```

**桌面应用找不到 `cne` 时**，把 `command` 换成绝对路径（终端里 `which cne`，Windows 用 `where cne`）。从 Dock 或开始菜单启动的应用不继承 shell 的 `PATH`，装在虚拟环境里的 `cne` 尤其如此。

### Codex

```bash
codex mcp add cnequity -- cne mcp --config /abs/path/to/cnequity.toml
```

或者写进 `~/.codex/config.toml`：

```toml
[mcp_servers.cnequity]
command = "cne"
args = ["mcp", "--config", "/abs/path/to/cnequity.toml"]
```

### VS Code

VS Code 的键名是 `servers`，不是 `mcpServers`：

```json
{
  "servers": {
    "cnequity": {
      "type": "stdio",
      "command": "cne",
      "args": ["mcp", "--config", "/abs/path/to/cnequity.toml"]
    }
  }
}
```

### ChatGPT

ChatGPT 只连接公网 HTTPS 地址，不能拉起本地进程，认证也只有 OAuth 和「无认证」两种。所以要三步：用 HTTP 模式起服务，用隧道把它公开成 HTTPS，再把带令牌的网址交给 ChatGPT。需要付费套餐里的开发者模式。

```bash
# 1. 生成一个随机令牌，用 HTTP 模式起服务（默认只监听 127.0.0.1:8788）
TOKEN=$(python -c "import secrets; print(secrets.token_urlsafe(24))")
cne mcp --config /abs/path/to/cnequity.toml --http --token "$TOKEN"

# 2. 另开一个终端，用隧道公开成 HTTPS（cloudflared 或 ngrok 均可）
cloudflared tunnel --url http://localhost:8788
```

3. 在 ChatGPT 设置里打开开发者模式，新建连接器：网址填 `https://<隧道域名>/mcp/<令牌>`，认证选「无认证」。

令牌放在路径里，是因为 ChatGPT 不能为连接器加自定义请求头；服务端也接受 `Authorization: Bearer <令牌>` 和 `?token=<令牌>`。设了 `--token` 后，每个请求都必须带令牌，与 Host 头无关。不设令牌时，只接受本机直连：Host 不是回环地址、带转发头（隧道和反向代理都会加）或 Origin 来自其他站点的请求一律返回 403。

**这个网址就是钥匙。** 拿到它的人能对你的湖执行任意只读 SQL。别贴到公开的地方；不用时停掉隧道。cloudflared 的临时隧道每次启动域名都会变，换了之后要回 ChatGPT 改连接器网址。

六个工具都标了 `readOnlyHint`，ChatGPT 据此把它们当作只读操作。Claude 网页版的自定义连接器同样只接远程网址，也可以用这一套。

## 六个工具分别做什么

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
- `query_bars` 不带 `adjust` 时返回 `warning`；带了但有行缺因子时，报出「N/M 行 `adj_is_exact=false`」。响应同时回显 `adjust` 与 `universe`，使显式的 `all_a_sh_sz` 沪深子集口径不会在分页或下游处理时丢失。
- `query_fundamentals` 不给 `as_of` 直接报错并解释为什么没有默认值——默认成今天，等于用今天的信息回答历史问题，而 agent 无从察觉。
- `query_fundamentals` 默认 `pit_mode="best_effort"`：回填进湖的历史行也会返回，带 `pit_is_exact=false`，响应里的 `exact_rows` 给出精确行数。`pit_mode="strict"` 只返回湖在 `as_of` 当天已持有的行；结果为空而回填行存在时，`note` 会说明并提示改用 best_effort。

### 分页永远说实话

每个响应都带 `total` / `returned` / `truncated`。只告诉模型这 200 行，它会把 200 行的均值当成全市场的均值报出去；`truncated` 与 `note` 就是让它改用 `run_sql` 的开关。

### 溯源是汇总的，不是逐行的

curated 每行都带 `source` / `data_version` / `fetched_at`，逐行返回会让行情载荷大出约三倍去重复同样的三个值。默认返回 `sources` 汇总；需要逐行时传 `include_provenance: true`。

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

## run_sql 的边界

只接受**一条 SELECT**，且用 DuckDB 自己的解析器判定，不是正则。

湖里有 `news_headlines`、`flash_news_wire` —— 供应商文本，不是我们写的内容，而 agent 会读它。也就是说到达这个工具的 SQL 可能被湖里摄入的内容影响。解析器能把 `SELECT ... -- ; DROP` 和两条语句分清楚，正则不能。连接本身也是 read-only，两者互补：read-only 拦不住 `COPY ... TO`（它写的是数据库文件之外的路径）。

SQL 连接还会被限制为只允许访问 `curated/` 和 `derived/` 两个 lake 目录，关闭 DuckDB external access 及扩展自动安装/加载，并锁定这些配置。因此 `read_text`、`read_csv`、`read_parquet`、HTTP URL 等指向 lake 外部的文件访问会失败；这不是操作系统沙箱，部署不可信 agent 时仍应使用进程/容器级隔离。

被拒绝的例子：多语句、`DROP`、`CREATE`、`ATTACH`、`COPY ... TO`。

`daily_bars_adj` 是现成视图，带 `hfq_*` / qfq 与 `adj_is_exact`；它的 qfq anchor
是每个标的在整个湖内的最新 bar。做有明确历史窗口的 qfq 聚合时，使用
`daily_bars_qfq(DATE '2020-01-01', DATE '2020-12-31')`，该宏会按窗口 anchor，
优先于自己 join `adj_factors`。

## 适合交给 agent 的研究问题

差异不在数据源，在于有没有历史序列：

- 「茅台过去五年 PE 的历史分位数，现在在什么位置」——需要 `valuation_metrics` 的多年日频序列
- 「2018 年这个财报因子的 IC 是多少，别用未来数据」——需要 `as_of` 的 PIT 切片
- 「过去三年退市的股票，退市前 60 天什么形态」——需要退市股仍在湖里

回答前先通过 `describe_lake` 核对覆盖，并检查查询返回的口径与截断提示。数据未采集或证据不足时，agent 应保留限制，不能从空结果推出事件未发生。

## 传输与实现

服务端使用 `mcp_server/protocol.py` 的 JSON-RPC 处理，没有额外的 MCP SDK 依赖。默认传输是 stdio；`--http` 用 `mcp_server/http.py` 提供 Streamable HTTP：每个 `POST /mcp` 直接返回 JSON，不开 SSE 流，也不建会话，`GET` 返回 405。默认模式读取本地数据；`--live` 是显式联网模式，仍不写研究数据，限流账本可能更新。

## 排障

**客户端报 parse error，服务端看着正常。** stdout 就是 JSON-RPC 线路，任何多余输出都会污染它。`cne mcp` 已经把日志定向到 stderr（MCP 客户端会当作服务端日志收集）；如果你在 fork 里加了 `print`，那就是原因。

**工具报 "no parquet data for dataset X"。** 该数据集在这个湖里还是空的。调用 `describe_lake` 并传入 `include_empty: true` 看注册了但没数据的清单，或跑对应的 step。

**读到的不是你以为的那个湖。** 检查 `--config` 是不是绝对路径；`describe_lake` 的返回里第一项就是 `data_root`。

## 相关文档

- [CLI](cli.md#cne-mcp) · [Python API](python-api.md)
- [数据集目录](../datasets/catalog.md) · [查询指南](../datasets/query-guide.md)
