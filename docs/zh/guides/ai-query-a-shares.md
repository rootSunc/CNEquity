---
description: 用 Claude、ChatGPT、Cursor 等 AI 直接查询 A 股历史数据：通过只读 MCP 接入本地数据湖，三步完成，无需账号和 Token。
---

# 用 Claude、ChatGPT 直接查 A 股数据

想让 AI 回答「宁德时代最近 30 个交易日的最高收盘价是哪天」，需要它能读到可靠的历史数据，而不是临时搜网页。CNEquity 把本地 A 股数据湖通过 MCP 以只读方式接给 AI，支持 Claude、ChatGPT、Codex、Gemini CLI、Cursor、VS Code 等客户端。

## 三步接入

**① 安装并建一个小湖。** 一分钟内用真实数据源抓 5 只股票最近约 30 个交易日，不需要账号或 Token：

```bash
pip install cnequity
cne init --profile demo
```

**② 把湖接给 AI。** 以 Claude Code 为例，`--config` 写 demo 配置的绝对路径（上一步结束时会打印出来）：

```bash
claude mcp add cnequity -- cne mcp --config /abs/path/to/configs/cnequity.demo.toml
```

其他客户端的写法见 [MCP 接入](../reference/mcp.md#接入各家客户端)。ChatGPT 只接公网网址，需要用 `cne mcp --http` 加隧道。

**③ 直接提问。** 例如：

- 「宁德时代最近 30 个交易日的最高收盘价是哪天？」
- 「茅台过去五年 PE 的历史分位数，现在在什么位置？」（需要全市场湖里的估值历史）
- 「过去三年退市的股票，退市前 60 天走势如何？」（需要湖里保留退市股）

后两类问题依赖多年历史，demo 湖不够，要用 `cne init` 建全市场湖，见[快速开始](../getting-started/quickstart.md)。

## 为什么不直接让 AI 上网查

- **口径写在响应里。** AI 先调用 `describe_lake` 了解湖里有什么、覆盖到哪一天，查询结果会带上复权方式、截断和覆盖提示，而不是给出一个来源不明的数字。
- **财报不会用到未来数据。** 查财报必须给出时点（`as_of`），只返回当时已经披露的数据。
- **只读。** MCP 入口不能触发采集、重试或清理，AI 改不了你的数据。

## 相关页面

- [MCP 接入](../reference/mcp.md)：六个工具的用途、各客户端配置和排障
- [A股历史数据免费获取](free-a-share-data.md)：建全市场湖需要多久、能拿到多远的历史
