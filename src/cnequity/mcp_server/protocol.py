"""MCP over stdio: newline-delimited JSON-RPC 2.0, hand-rolled.

WHY NOT THE SDK. `mcp` 2.0 resolves to 15 additional packages — cryptography,
pyjwt and truststore for an OAuth flow a local stdio server never performs,
opentelemetry for tracing nothing exports, and a *second* HTTP stack beside the
httpx this project already pins. `pip install cnequity` currently brings
every runtime source with no extras, and that promise is worth more than the
~200 lines below. What we actually need is small and has been stable across
every protocol revision: three request methods over line-delimited JSON.

Revisit this if the server ever needs sampling, roots, elicitation or an HTTP
transport — those are where the SDK earns its footprint. Serving tools does not.

**stdout is the wire.** A stray `print` corrupts the stream and the client
reports a parse error with no hint where it came from, so `serve_stdio` points
logging at stderr before the loop starts and nothing here writes to stdout
except `_send`.

**The wire is UTF-8.** The stdio transport says so; the console code page
(GBK on a Chinese Windows) does not. `serve_stdio` therefore reads the real
stdin as bytes and decodes each frame itself, and writes the real stdout as
UTF-8 with bare `\n` line ends.
"""

from __future__ import annotations

import inspect
import io
import json
import logging
import sys
from collections.abc import Iterable
from typing import Any, TextIO

from cnequity.config import Config
from cnequity.mcp_server.catalog import DESCRIPTORS, HANDLERS
from cnequity.mcp_server.tools import ToolError

logger = logging.getLogger(__name__)

SERVER_NAME = "cnequity"

# The revision this server was written against. The client's requested version
# is echoed back when it sends one: the tools primitive is identical across
# every revision that has shipped, so refusing an older client would buy
# nothing and lock out editors that pin an earlier date.
PROTOCOL_VERSION = "2025-06-18"

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603


def _server_version() -> str:
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("cnequity")
    except PackageNotFoundError:  # pragma: no cover — editable checkout without install
        return "0"


def _text_result(payload: Any, *, is_error: bool = False) -> dict:
    return {
        "content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=False, default=str)}],
        "isError": is_error,
    }


def dispatch(config: Config, method: str, params: dict) -> dict:
    """Handle one request method. Raises ``_RpcError`` for protocol-level faults."""
    if method == "initialize":
        return {
            "protocolVersion": params.get("protocolVersion") or PROTOCOL_VERSION,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": SERVER_NAME, "version": _server_version()},
            "instructions": (
                "A local A-share Parquet lake. Call describe_lake first: it "
                "returns coverage and the rules that make an answer correct "
                "(price adjustment, point-in-time cutoffs, which series have no "
                "real history). Prefer run_sql for anything that aggregates."
            ),
        }
    if method == "ping":
        return {}
    if method == "tools/list":
        return {"tools": DESCRIPTORS}
    if method == "tools/call":
        return _call_tool(config, params)
    raise _RpcError(METHOD_NOT_FOUND, f"unknown method {method!r}")


def _call_tool(config: Config, params: dict) -> dict:
    name = params.get("name")
    handler = HANDLERS.get(name)
    if handler is None:
        raise _RpcError(INVALID_PARAMS, f"unknown tool {name!r}")

    arguments = params.get("arguments") or {}
    if not isinstance(arguments, dict):
        raise _RpcError(INVALID_PARAMS, "arguments must be an object")

    # Tool failures come back as a result with isError, not as a JSON-RPC error.
    # That is the difference between the agent seeing "as_of is required" and
    # being able to retry, and the client swallowing the fault as a transport
    # problem the model never learns about.
    # Bind first, so only a mismatch with the signature reads as bad arguments.
    # A TypeError raised inside the tool is a fault in the tool; calling it
    # "bad arguments" would send the agent off rewriting correct arguments.
    try:
        inspect.signature(handler).bind(config, **arguments)
    except TypeError as exc:
        return _text_result({"error": f"bad arguments for {name}: {exc}"}, is_error=True)
    try:
        return _text_result(handler(config, **arguments))
    except ToolError as exc:
        return _text_result({"error": str(exc)}, is_error=True)
    except Exception as exc:  # noqa: BLE001 — one bad call must not end the session
        logger.exception("tool %s failed", name)
        return _text_result({"error": f"{type(exc).__name__}: {exc}"}, is_error=True)


class _RpcError(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def handle_message(config: Config, message: object) -> dict | None:
    """Turn one decoded JSON-RPC message into a response, or None for a notification."""
    if not isinstance(message, dict):
        return {
            "jsonrpc": "2.0",
            "id": None,
            "error": {"code": INVALID_REQUEST, "message": "message must be an object"},
        }
    request_id = message.get("id")
    method = message.get("method")
    if not isinstance(method, str):
        if request_id is None:
            return None
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": INVALID_REQUEST, "message": "missing method"},
        }

    params = message.get("params") or {}
    if not isinstance(params, dict):
        params = {}

    # No id means a notification: `notifications/initialized` and `notifications/
    # cancelled` arrive this way, and the spec forbids replying to either.
    if request_id is None:
        return None

    try:
        return {"jsonrpc": "2.0", "id": request_id, "result": dispatch(config, method, params)}
    except _RpcError as exc:
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": exc.code, "message": exc.message},
        }
    except Exception as exc:  # noqa: BLE001 — keep the session alive
        logger.exception("dispatch failed for %s", method)
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": INTERNAL_ERROR, "message": f"{type(exc).__name__}: {exc}"},
        }


def handle_payload(config: Config, message: object) -> dict | list[dict] | None:
    """Handle a transport payload with identical batch rules on stdio and HTTP."""
    if isinstance(message, list) and message:
        replies = [r for r in (handle_message(config, m) for m in message) if r is not None]
        return replies or None
    return handle_message(config, message)


def _send(out: TextIO, message: dict | list[dict]) -> None:
    out.write(json.dumps(message, ensure_ascii=False, default=str) + "\n")
    out.flush()


def serve_stdio(
    config: Config,
    *,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
) -> None:
    """Read requests from stdin and write responses to stdout until EOF.

    Streams passed in are used as given. The process's own stdin and stdout
    are put on UTF-8 whatever the locale says, because the client on the other
    end of the pipe writes and expects UTF-8.
    """
    frames: Iterable[str | bytes]
    if stdin is not None:
        frames = stdin
    else:
        frames = getattr(sys.stdin, "buffer", sys.stdin)
    if stdout is None:
        stdout = sys.stdout
        if isinstance(stdout, io.TextIOWrapper):
            # backslashreplace: a lone surrogate echoed from a request can only
            # sit inside a JSON string, where `\udxxx` is a valid escape.
            stdout.reconfigure(encoding="utf-8", errors="backslashreplace", newline="\n")

    for line in frames:
        if isinstance(line, bytes):
            try:
                line = line.decode("utf-8")
            except UnicodeDecodeError as exc:
                _send(
                    stdout,
                    {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {"code": PARSE_ERROR, "message": f"frame is not UTF-8: {exc}"},
                    },
                )
                continue
        line = line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except json.JSONDecodeError as exc:
            # id is unknowable on a malformed frame; null is what the spec says.
            _send(
                stdout,
                {"jsonrpc": "2.0", "id": None, "error": {"code": PARSE_ERROR, "message": str(exc)}},
            )
            continue
        reply = handle_payload(config, message)
        if reply is not None:
            _send(stdout, reply)
