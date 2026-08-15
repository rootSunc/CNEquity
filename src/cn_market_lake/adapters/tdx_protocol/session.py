"""Shared TDX session primitives (lock + teardown)."""

from __future__ import annotations

import threading

# The TDX wire client is not thread-safe (shared socket + heartbeat thread).
# All Quotes sessions must be serialized across orchestrator parallel steps.
TDX_SESSION_LOCK = threading.Lock()


def close_quotes_client(client: object) -> None:
    """Close a TDX client so its heartbeat thread dies (else the process
    can't exit — a serial daily run creates one client per fetch)."""
    if client is None:
        return
    inner = getattr(client, "client", None)
    close = getattr(inner, "close", None) or getattr(client, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass
