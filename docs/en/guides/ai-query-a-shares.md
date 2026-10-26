---
description: Query China A-share history directly from Claude, ChatGPT, Cursor and other AI tools - connect a local data lake through a read-only MCP server in three steps, no account or token needed.
---

# Ask Claude or ChatGPT about China A-share data

For an AI to answer "On which day in the last 30 trading days did CATL close highest?", it needs reliable historical data, not an ad-hoc web search. CNEquity connects a local China A-share data lake to AI clients through a read-only MCP server, and works with Claude, ChatGPT, Codex, Gemini CLI, Cursor, VS Code and others.

## Three steps

**① Install and build a small lake.** In about a minute, fetch the last ~30 trading days for 5 stocks from live sources; no account or token needed:

```bash
pip install cnequity
cne init --profile demo
```

**② Connect the lake to your AI client.** With Claude Code, pass the absolute path of the demo config (the previous step prints it):

```bash
claude mcp add cnequity -- cne mcp --config /abs/path/to/configs/cnequity.demo.toml
```

For other clients see [MCP integration](../reference/mcp.md#connecting-clients). ChatGPT only accepts public URLs, so it needs `cne mcp --http` plus a tunnel.

**③ Ask.** For example:

- "On which day in the last 30 trading days did CATL close highest?"
- "Where does Moutai's P/E sit within its five-year history?" (needs valuation history from a full-market lake)
- "How did stocks delisted in the past three years trade in their last 60 days?" (needs delisted stocks kept in the lake)

The last two depend on years of history, which the demo lake does not have; build a full-market lake with `cne init`, see the [Quickstart](../getting-started/quickstart.md).

## Why not let the AI search the web

- **Semantics come with the answer.** The AI first calls `describe_lake` to learn what the lake holds and how far it reaches; query results carry the price adjustment, truncation and coverage notes instead of a number of unknown origin.
- **No future data in financials.** Financial statement queries must give a point in time (`as_of`) and return only what had been disclosed by then.
- **Read-only.** The MCP server cannot trigger fetching, retries or cleanup, so the AI cannot change your data.

## Related pages

- [MCP integration](../reference/mcp.md): what the six tools do, client setup and troubleshooting
- [Free China A-share historical data](free-a-share-data.md): how long a full-market lake takes and how far back it goes
