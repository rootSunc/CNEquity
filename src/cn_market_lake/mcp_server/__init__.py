"""MCP server — the lake's agent-facing surface.

``cml serve`` shows the lake to a person; this shows it to a model. Same
read-only stance: nothing here ingests, retries or cleans, because a tool an
agent can call on its own initiative is the last place to put a write path.

The tools are cut by question shape rather than one per dataset — see
``catalog.py`` for the descriptors and ``tools.py`` for what they return.
"""

from cn_market_lake.mcp_server.protocol import serve_stdio

__all__ = ["serve_stdio"]
