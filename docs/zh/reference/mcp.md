---
description: 把本地 A 股数据湖通过只读 MCP 接给 Claude、ChatGPT、Cursor 等 AI agent。
---

# MCP

`cne mcp` 把湖以只读工具提供给支持 MCP 的客户端。它没有采集、重试或清理入口；取数从 CLI 或 `cne serve` 的操作页发起。服务端是标准的 MCP JSON-RPC，不绑定特定模型。

```bash
cne mcp --config /abs/path/to/cnequity.toml            # 已有湖
cne init --profile demo                                 # 还没有湖：先建一个 5 只股票的小样例
cne mcp --config /abs/path/to/cnequity.toml --live      # 不建湖：现取现给，不落盘，能力有限
```

`--config` 写绝对路径。客户端从哪个目录拉起进程是不确定的；配置里相对的 `[data].root` 按配置文件所在目录解析。湖里没有已发布数据时，命令直接退出并打印解析出的配置和数据目录，而不是对着一个空湖提供服务。

## 接入客户端 {#clients}

支持 stdio 的客户端都用同一条命令和参数，区别只是配置文件的位置：

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

| 客户端 | 接入方式 |
|---|---|
| Claude Code | `claude mcp add cnequity -- cne mcp --config /abs/path/to/cnequity.toml`，加 `-s user` 对所有项目生效 |
| Codex | `codex mcp add cnequity -- cne mcp --config /abs/path/to/cnequity.toml`，或写进 `~/.codex/config.toml` 的 `[mcp_servers.cnequity]` |
| Claude Desktop、Cursor、Gemini CLI、Windsurf | 上面的 JSON，分别写进 `claude_desktop_config.json`、`~/.cursor/mcp.json`、`~/.gemini/settings.json`、`~/.codeium/windsurf/mcp_config.json` |
| VS Code | `.vscode/mcp.json`，顶层键是 `servers`，并加 `"type": "stdio"` |
| 只接受远程网址的客户端（如 ChatGPT） | 见下文 HTTP 模式 |

桌面应用找不到 `cne` 时，把 `command` 换成绝对路径（`which cne`，Windows 用 `where cne`）：从 Dock 或开始菜单启动的应用不继承 shell 的 `PATH`。

### HTTP 模式 {#http}

```bash
TOKEN=$(python -c "import secrets; print(secrets.token_urlsafe(24))")
cne mcp --config /abs/path/to/cnequity.toml --http --token "$TOKEN"   # 默认监听 127.0.0.1:8788
cloudflared tunnel --url http://localhost:8788                         # 另开终端，公开成 HTTPS
```

在客户端里填 `https://<隧道域名>/mcp/<令牌>`。服务端也接受 `Authorization: Bearer <令牌>` 和 `?token=<令牌>`。监听非回环地址必须设 `--token`；不设令牌时只接受本机直连，带转发头或其他站点 Origin 的请求返回 403。这个网址等于一把钥匙：拿到的人可以对湖执行任意只读 SQL。不用时停掉隧道。

HTTP 模式每个 `POST /mcp` 直接返回 JSON，不开 SSE 流，不建会话。

## 工具 {#tools}

| 工具 | 用途 |
|---|---|
| `describe_lake` | 湖里有哪些数据集、各自的覆盖范围、历史模式和新鲜度，以及让答案正确的口径说明。每个会话先调它；`include_empty` 列出已登记但还没数据的数据集 |
| `resolve_symbol` | 公司名或部分代码换成标准代码，如「茅台」→ `600519.SH`；含退市股 |
| `query_bars` | 日线、指数、分钟线，支持 `adjust` 和 `universe` |
| `query_fundamentals` | 财报科目，必须给 `as_of`；`pit_mode` 默认 `best_effort` |
| `query_dataset` | 其余数据集，按日期与代码过滤 |
| `run_sql` | 一条只读的 DuckDB `SELECT`，每个数据集是同名视图；用于排名、聚合、分位数和跨数据集关联 |

口径写在响应里，而不只写在文档里：

- `query_bars` 不带 `adjust` 时返回提示，带了但有行缺因子时报出 `adj_is_exact=false` 的行数；响应回显 `adjust` 与 `universe`。
- `query_fundamentals` 不给 `as_of` 直接报错：默认成今天等于用今天的信息回答历史问题。严格模式结果为空而有回填行时，`note` 会说明。
- 每个响应都带 `total`、`returned`、`truncated`。被截断时应改用 `run_sql` 聚合，而不是对截断的结果求均值。
- 溯源默认按来源汇总在 `sources` 里；要逐行溯源传 `include_provenance: true`。

`query_dataset` 和控制台的数据浏览共用一个行数上限 `[query].scan_budget_rows`（默认 400 万行，按分页前选中的行数算）。超过上限又没给 20 个以内代码的请求会被拒绝，并给出能装下的最近日期窗口。

## run_sql 的边界 {#run-sql}

只接受一条 `SELECT`，由 DuckDB 自己的解析器判定，所以注释里藏的第二条语句也会被识别。湖里有新闻等供应商文本，到达这个工具的 SQL 可能受这些内容影响，因此连接还做了这些限制：只读；只允许读已发布数据所在的目录；关闭外部访问和扩展的自动安装、加载，并锁定配置。指向湖外文件或网址的 `read_csv`、`read_parquet` 等会失败。这不是操作系统沙箱，部署给不可信的 agent 时仍应使用进程或容器隔离。

`daily_bars_adj` 视图带复权列；有明确窗口的前复权聚合用 `daily_bars_qfq(DATE '2020-01-01', DATE '2020-12-31')`。

## --live {#live}

`--live` 让湖里没有的数据现场从来源取、不落盘。它只支持 `resolve_symbol`（当前证券主数据，不含退市股）和未复权的 `daily_bars`；分钟线、指数、财报、其他数据集和 `run_sql` 都明确报错，因为空结果会被 agent 读成「这件事没发生过」。带 `adjust` 或 `universe` 也报错：复权因子是湖里单独派生的数据集，把未复权价当复权价跨一次除权就是错的。

每条响应都带 `origin`（`live` 或 `lake`），live 响应附带缺少的能力说明。每次调用最多 50 个代码、800 天，必须显式给 `symbols`。除了限速器自己的状态文件，不会写任何东西。`--live` 默认关闭，从不自动启用：湖坏了应该收到报错，而不是悄悄拿到另一份数据。

## 排障 {#troubleshooting}

| 现象 | 处理 |
|---|---|
| 客户端报 parse error | stdout 是 JSON-RPC 线路，日志已经写到 stderr；自己加的 `print` 会污染它 |
| 工具报某数据集没有数据 | 该数据集在这个湖里还是空的；用 `describe_lake` 加 `include_empty: true` 查看，或运行对应采集 |
| 读到的不是你的湖 | `describe_lake` 返回的第一项是 `data_root`；检查 `--config` 是不是绝对路径 |

回答研究问题前，先用 `describe_lake` 核对覆盖，并检查响应里的口径和截断提示。数据未采集或证据不足时，不能从空结果推出事件未发生。
