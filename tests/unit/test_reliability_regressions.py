"""Regressions for four reliability defects (D1-D4), fixed in 0.22.x.

D1  baostock ``fetch_per_symbol`` lost every fetched row when a relogin raised
    something other than ``RuntimeError`` (a connection reset, an OSError).
D2  ``error_kind`` classified ``socket.gaierror``/``herror`` and
    ``ssl.SSLError`` as ``storage_failure``: never auto-retried, later waves
    stopped, and ``walk_day_backfill`` re-raised before flushing fetched days.
D3  ``write_parquet_atomic``, ``write_json_atomic`` and
    ``StateStore._write_payload`` cleaned their temp file only on
    ``Exception``, so Ctrl-C left a hidden ``.*.tmp`` beside lake files.
D4  MCP tool results carried bare ``NaN``/``Infinity``, which strict JSON
    parsers reject; they are now ``null``.

``test_d2_real_disk_errors_stay_storage_failures`` guards the D2 fix from
reclassifying real I/O faults.
"""

from __future__ import annotations

import json
import socket
import ssl
from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.run_scope import set_backfill
from cnequity.storage import atomic as atomic_mod
from cnequity.storage.atomic import write_json_atomic, write_parquet_atomic
from cnequity.storage.state import StateStore


def _ok_login():
    return type("R", (), {"error_code": "0", "error_msg": ""})()


# =============================================================================
# D1  baostock relogin raising OSError discards the sweep
# =============================================================================


class _ResetOnRelogin:
    """First login succeeds; every later login hits a reset connection."""

    def __init__(self):
        self.logins = 0

    def login(self):
        self.logins += 1
        if self.logins == 1:
            return _ok_login()
        raise ConnectionResetError(104, "Connection reset by peer")

    def logout(self):
        return None


def test_d1_periodic_relogin_connection_reset_keeps_rows_already_fetched(monkeypatch):
    from cnequity.adapters.baostock import _session as sess

    monkeypatch.setattr(sess, "_RELOGIN_EVERY", 2)

    def fetch(_bs, symbol, _s, _e):
        return [{"symbol": symbol}]

    rows, failed = sess.fetch_per_symbol(
        ["A.SH", "B.SH", "C.SH", "D.SH"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        fetch,
        bs=_ResetOnRelogin(),
        sleep=lambda _s: None,
    )

    assert {r["symbol"] for r in rows} == {"A.SH", "B.SH"}
    assert failed == ["C.SH", "D.SH"]


def test_d1_retry_relogin_connection_reset_keeps_rows_already_fetched(monkeypatch):
    from cnequity.adapters.baostock import _session as sess

    def fetch(_bs, symbol, _s, _e):
        if symbol == "C.SH":
            return None  # retryable query error -> backoff -> relogin
        return [{"symbol": symbol}]

    rows, failed = sess.fetch_per_symbol(
        ["A.SH", "B.SH", "C.SH", "D.SH"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        fetch,
        bs=_ResetOnRelogin(),
        sleep=lambda _s: None,
    )

    assert {r["symbol"] for r in rows} == {"A.SH", "B.SH"}
    assert failed == ["C.SH", "D.SH"]


# =============================================================================
# D2  network OSErrors classified as storage failures
# =============================================================================

NETWORK_OSERRORS = [
    pytest.param(socket.gaierror(-2, "Name or service not known"), id="dns"),
    pytest.param(socket.herror(1, "Unknown host"), id="herror"),
    pytest.param(ssl.SSLError(1, "[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE]"), id="tls"),
    pytest.param(ssl.SSLEOFError(8, "EOF occurred in violation of protocol"), id="tls-eof"),
]


@pytest.mark.parametrize("exc", NETWORK_OSERRORS)
def test_d2_network_oserrors_are_source_transient_not_storage(exc):
    from cnequity.orchestrator.outcomes import error_kind

    assert error_kind(exc) == "source_transient"


def test_d2_real_disk_errors_stay_storage_failures():
    """Guard for the fix: narrowing must not reclassify real I/O faults."""
    import errno

    from cnequity.orchestrator.outcomes import error_kind

    assert error_kind(OSError(errno.ENOSPC, "No space left on device")) == "storage_failure"
    assert error_kind(PermissionError(errno.EACCES, "Permission denied")) == "storage_failure"


def test_d2_a_dns_failure_mid_backfill_keeps_fetched_days_and_owes_only_that_day(tmp_path):
    """Today the gaierror is re-raised before ``flush()``: days 1-2 are lost
    and day 3 is never recorded as owed."""
    from cnequity.steps.common import walk_day_backfill

    cfg = Config(data_root=tmp_path / "data")
    set_backfill(cfg, start=date(2026, 6, 1), end=date(2026, 6, 5))
    poison = date(2026, 6, 3)

    def fetch_one(day: date) -> pl.DataFrame:
        if day == poison:
            raise socket.gaierror(-3, "Temporary failure in name resolution")
        return pl.DataFrame([{"trade_date": day, "metric_id": "adv_pct", "value": 1.0}])

    result = walk_day_backfill(
        cfg, date(2026, 6, 5), "run-dns", "market_breadth", fetch_one, source="derived"
    )

    assert result["status"] == "warning"
    assert result["failed_days"] == 1
    assert StateStore(cfg.meta_root).get_missing_dates("market_breadth") == {poison}
    staged = {
        d
        for path in (cfg.staging_root / "market_breadth").rglob("*.parquet")
        for d in pl.read_parquet(path)["trade_date"].to_list()
    }
    assert staged == {date(2026, 6, d) for d in (1, 2, 4, 5)}


# =============================================================================
# D3  Ctrl-C during an atomic write leaves a hidden temp file in the lake
# =============================================================================


def _interrupt(*_args, **_kwargs):
    raise KeyboardInterrupt


def test_d3_ctrl_c_during_parquet_write_keeps_target_and_leaves_no_temp(tmp_path, monkeypatch):
    out = tmp_path / "part-merged.parquet"
    write_parquet_atomic(out, pl.DataFrame({"value": [1]}))
    monkeypatch.setattr(atomic_mod, "fsync_file", _interrupt)

    with pytest.raises(KeyboardInterrupt):
        write_parquet_atomic(out, pl.DataFrame({"value": [2]}))

    assert pl.read_parquet(out)["value"].to_list() == [1]  # passes today
    assert sorted(p.name for p in tmp_path.iterdir()) == [out.name]  # fails today


def test_d3_ctrl_c_during_json_write_keeps_target_and_leaves_no_temp(tmp_path):
    out = tmp_path / "receipt.json"
    write_json_atomic(out, {"ok": 1})

    with pytest.raises(KeyboardInterrupt):
        write_json_atomic(out, {"ok": object()}, default=_interrupt)

    assert json.loads(out.read_text(encoding="utf-8")) == {"ok": 1}
    assert sorted(p.name for p in tmp_path.iterdir()) == [out.name]


def test_d3_ctrl_c_during_a_watermark_write_leaves_no_temp(tmp_path, monkeypatch):
    from cnequity.storage import state as state_mod

    store = StateStore(tmp_path / "meta")
    store.set_date("daily_bars", date(2024, 6, 27))
    monkeypatch.setattr(state_mod.os, "fsync", _interrupt)

    with pytest.raises(KeyboardInterrupt):
        store.set_date("daily_bars", date(2024, 6, 28))

    monkeypatch.undo()
    assert store.get_date("daily_bars") == date(2024, 6, 27)
    assert not list(store.root.glob(".*.tmp"))


# =============================================================================
# D4  MCP text content is not strict JSON when data holds NaN / Infinity
# =============================================================================


@pytest.fixture
def sql_lake(tmp_path) -> Config:
    """Same dataset shapes as ``test_mcp_server.lake``, whose run_sql tests pass."""
    from datetime import datetime, timezone

    prov = {
        "source": "test",
        "data_version": "v1",
        "fetched_at": datetime(2024, 6, 28, tzinfo=timezone.utc),
    }
    root = tmp_path / "data"
    curated, derived = root / "curated", root / "derived"
    (curated / "instruments").mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "600001.SH"],
            "name": ["贵州茅台", "邯郸钢铁"],
            "exchange": ["SH", "SH"],
            "asset_type": ["stock", "stock"],
            "list_date": [date(2001, 8, 27), date(1997, 12, 8)],
            "delist_date": [None, date(2009, 12, 25)],
            "prev_symbol": [None, None],
            **prov,
        }
    ).write_parquet(curated / "instruments" / "part-merged.parquet")
    bars = curated / "daily_bars" / "trade_date=2024-06-27"
    bars.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 27)],
            "open": [10.0],
            "high": [11.0],
            "low": [9.0],
            "close": [10.5],
            "volume": [1000],
            "amount": [10500.0],
            **{**prov, "source": "tdx_protocol"},
        }
    ).write_parquet(bars / "part-0.parquet")
    adj = derived / "adj_factors" / "trade_date=2024-06-27"
    adj.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 27)],
            "adjust_type": ["hfq"],
            "factor": [2.0],
            **{**prov, "source": "sina"},
        }
    ).write_parquet(adj / "part-0.parquet")
    return Config(data_root=root)


def test_d4_tool_results_are_strict_json_even_with_nan(sql_lake):
    from cnequity.mcp_server import protocol

    reply = protocol.handle_message(
        sql_lake,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "run_sql",
                "arguments": {
                    "sql": "SELECT CAST('NaN' AS DOUBLE) AS pe, CAST('Infinity' AS DOUBLE) AS pb"
                },
            },
        },
    )
    assert reply["result"]["isError"] is False
    text = reply["result"]["content"][0]["text"]

    def reject(token: str):
        raise ValueError(f"non-standard JSON constant {token!r} in tool result")

    json.loads(text, parse_constant=reject)
