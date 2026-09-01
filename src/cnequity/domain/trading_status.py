"""What a ``trading_status`` row means, in one place.

The dataset used to carry two orthogonal facts in a single ``status`` string:
whether a security was **trading** that day, and whether it carried the
exchange's **risk-warning** designation (ST / *ST). One column cannot hold two
independent facts, and the writer resolved the conflict with an ``if/elif`` that
let suspension win:

    000711.SZ (ST京蓝)  2026-08-27  status=st
                        2026-08-28  status=suspended

The company did not leave risk warning that day — it halted. The stored history
lost the designation anyway, and every consumer that asked "was this ST" got the
wrong answer for the halt. `market_breadth` in particular reads it to pick the
±5% limit band, so a halted ST name was priced with the ±10% band.

So the two facts are now two columns:

* ``status`` — trading state: ``normal`` | ``suspended`` | ``delisted``
* ``risk_warning`` — the ST / *ST designation, independent of the above

``delisted`` is the second half of the same problem. The daily writer classified
everything that was neither halted nor on the ST board as ``normal`` with
``is_trading=True``, with no notion of delisting — so 611 symbols carrying a
``delist_date`` (one of them since 1999) were published as normally trading
every session. A dataset answering "was this security trading on day X" has to
be able to say "no, it was gone".

**ST vs *ST is not distinguished here.** No source that feeds this dataset ever
made the distinction — Baostock exposes a single ``isST`` flag, and the Tushare
adapter already collapsed its ``ST``/``*ST`` type to one value — so a boolean is
what the evidence actually supports. The finer designation lives in the
exchange 简称, reachable through ``instruments.name`` and
``adapters.exchange.st_lists.is_st_name``.

**Reading a lake that predates the split.** Old rows encode ST as
``status="st"``. :func:`risk_warning_expr` accepts both encodings, so queries
are correct before and after ``scripts/migrate_trading_status_risk_warning.py``
runs; the migration only makes the old rows say it in the new column.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

STATUS_NORMAL = "normal"
STATUS_SUSPENDED = "suspended"
STATUS_DELISTED = "delisted"

#: Trading states a row may carry. ``status`` no longer holds ST.
TRADING_STATES = frozenset({STATUS_NORMAL, STATUS_SUSPENDED, STATUS_DELISTED})

#: States in which the security was not trading.
NON_TRADING_STATES = frozenset({STATUS_SUSPENDED, STATUS_DELISTED})

#: How ST was encoded before ``risk_warning`` existed. Read-side only: nothing
#: writes these any more, and the migration rewrites them.
LEGACY_ST_STATUSES = frozenset({"st", "*st"})

#: Provenance for rows the lake derives from `instruments` rather than reading
#: from a vendor board: a delisted security appears on no daily board, so no
#: snapshot can say it stopped trading.
DELISTED_SOURCE = "derived_delisted"

#: Provenance for suspension rows reconstructed from a missing daily-bar
#: session. The daily feeds (EastMoney current snapshot, ST backfill) cannot
#: assert a symbol was halted, so the bar gap is the only evidence and it is
#: explicitly provisional: ``status_evidence_rank`` keeps it below a finalized
#: authority so a genuine source can correct it later.
DERIVED_BAR_GAP_SOURCE = "derived_bar_gap"

#: Sources whose snapshots are current-state observations. A snapshot fetched
#: on the same session after the 15:00 closing auction is point-in-time fact;
#: the same current-state answer stamped onto an older date is not.
_CURRENT_SNAPSHOT_SOURCES = frozenset({"eastmoney", "tdx_protocol"})

#: A snapshot fetched on ``trade_date`` at or after this time (Asia/Shanghai)
#: is treated as same-session evidence instead of a late current-state guess.
_SESSION_FINAL = time(15, 0)

_SHANGHAI = ZoneInfo("Asia/Shanghai")


def risk_warning_expr(columns: Iterable[str]) -> pl.Expr:
    """Whether a row carries the ST / *ST designation, in either encoding.

    Takes the frame's column names because a lake mid-migration has files both
    with and without ``risk_warning``; the legacy ``status`` encoding is always
    consulted so a partition that has not been rewritten still answers
    correctly rather than silently reporting every old ST day as clean.
    """
    legacy = pl.col("status").is_in(list(LEGACY_ST_STATUSES))
    if "risk_warning" in set(columns):
        return pl.col("risk_warning").fill_null(False) | legacy
    return legacy


def not_trading_expr(columns: Iterable[str]) -> pl.Expr:
    """Whether a row says the security was not trading that day."""
    expr = pl.col("status").is_in(list(NON_TRADING_STATES))
    if "is_trading" in set(columns):
        expr = expr | ~pl.col("is_trading").fill_null(False)
    return expr


def normalize_legacy(df: pl.DataFrame) -> pl.DataFrame:
    """Bring a frame onto the two-column encoding. Idempotent.

    A lake written before the split stores ST as ``status="st"`` and has no
    ``risk_warning`` column at all, which is a hard read error for anything
    that validates against the current schema. Rather than loosening that
    validation — which would let a genuinely malformed frame through — every
    read of stored ``trading_status`` passes through here, and any partition
    that gets rewritten afterwards is migrated as a side effect.
    """
    if "status" not in df.columns:
        return df
    return df.with_columns(
        risk_warning_expr(df.columns).alias("risk_warning"),
        pl.when(pl.col("status").is_in(list(LEGACY_ST_STATUSES)))
        .then(pl.lit(STATUS_NORMAL))
        .otherwise(pl.col("status"))
        .alias("status"),
    )


def is_risk_warning(status: str | None, risk_warning: bool | None = None) -> bool:
    """Row-wise form of :func:`risk_warning_expr` for non-polars callers."""
    if risk_warning:
        return True
    return str(status or "").strip().lower() in LEGACY_ST_STATUSES


def _as_shanghai_datetime(value: Any) -> datetime | None:
    """Normalize a provenance timestamp to an aware Asia/Shanghai datetime.

    Accepts ISO strings, naive datetimes (assumed UTC, matching stored
    ``fetched_at``), and aware datetimes.
    """
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=ZoneInfo("UTC"))
    return value.astimezone(_SHANGHAI)


def status_evidence_rank(row: Mapping[str, Any]) -> int:
    """Precedence for a collision with a derived bar-gap suspension.

    Historical Baostock evidence and a finalized same-session EastMoney
    snapshot are point-in-time facts. A later current-state snapshot stamped
    onto an older date is not. Unknown sources win conservatively so a newly
    introduced authority is never overwritten without an explicit policy.

    Lower rank wins (kept by ``keep="last"`` after an ascending sort); rank 0
    is the authority that may correct a derived row.
    """
    source = str(row.get("source") or "")
    if source == "baostock":
        return 0
    if source in _CURRENT_SNAPSHOT_SOURCES:
        fetched = _as_shanghai_datetime(row.get("fetched_at"))
        trade_date = row.get("trade_date")
        if (
            fetched is not None
            and isinstance(trade_date, date)
            and fetched.date() == trade_date
            and fetched.time() >= _SESSION_FINAL
        ):
            return 0
        return 2
    if source == DELISTED_SOURCE:
        # A formal delisting is a fact about the security, not an observation
        # of one session, so it outranks a current-state snapshot that simply
        # never learned the name is gone.
        return 0
    if source == DERIVED_BAR_GAP_SOURCE:
        return 1
    return 0


def evidence_rank_expr(fetched_at_timezone: str | None) -> pl.Expr:
    """Polars expression reproducing :func:`status_evidence_rank` row-wise.

    ``fetched_at_timezone`` is the stored column's time zone — ``None`` for a
    naive legacy timestamp, which is treated as UTC (``fetched_at`` is written
    timezone-aware elsewhere). Kept next to the row function so compact's
    columnar merge cannot drift from the derive writer's row-wise decision.
    """
    fetched = pl.col("fetched_at")
    if fetched_at_timezone:
        fetched = fetched.dt.convert_time_zone("Asia/Shanghai")
    else:
        fetched = fetched.dt.replace_time_zone("UTC").dt.convert_time_zone("Asia/Shanghai")
    same_session = (
        pl.col("trade_date").is_not_null()
        & (fetched.dt.date() == pl.col("trade_date"))
        & (fetched.dt.time() >= _SESSION_FINAL)
    )
    return (
        pl.when(pl.col("source") == "baostock")
        .then(pl.lit(0))
        .when(pl.col("source").is_in(list(_CURRENT_SNAPSHOT_SOURCES)))
        .then(pl.when(same_session).then(pl.lit(0)).otherwise(pl.lit(2)))
        .when(pl.col("source") == DELISTED_SOURCE)
        .then(pl.lit(0))
        .when(pl.col("source") == DERIVED_BAR_GAP_SOURCE)
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
    )
