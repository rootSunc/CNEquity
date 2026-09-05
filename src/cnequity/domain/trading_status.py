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
from datetime import date, datetime, time, timezone

import polars as pl

from cnequity.domain.market_time import SHANGHAI_TZ

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


# --- Which of two rows for the same (symbol, trade_date) is the better answer
#
# Everywhere else in the lake "freshest wins" is the right rule: a later fetch
# of the same key is a correction. ``trading_status`` breaks that rule, because
# two of its feeds are not observations of a session at all:
#
# * EastMoney's suspension / ST boards answer "is this name halted **now**".
#   Read at 09:12 on 2026-09-01 and stamped onto 2026-09-01, that is evidence.
#   Read on 2026-09-05 while a watermark catches up, the same answer stamped
#   onto 2026-09-01 is a guess about a session it never saw.
# * ``derived_bar_gap`` reconstructs a halt from the fact that a listed symbol
#   has no traded bar on a session its own bar history spans. That is weaker
#   than an exchange record but stronger than a restated current-state board,
#   and it is the only evidence that exists for most of the history.
#
# So the canonical row is chosen by evidence class first and recency second.
# Without this, one nightly EastMoney snapshot silently replaces every derived
# suspension it happens to touch — which is what made `derived_bar_gap` rows
# invisible to committed readers before they went through staging at all.

#: Provenance for suspension rows reconstructed from ``daily_bars`` gaps.
DERIVED_BAR_GAP_SOURCE = "derived_bar_gap"

#: Feeds that report current state rather than a session's record.
CURRENT_SNAPSHOT_SOURCES = frozenset({"eastmoney", "tdx_protocol"})

#: The closing auction ends at 15:00 Asia/Shanghai. A current-state board read
#: before then describes a session that had not finished happening.
SESSION_FINAL_AT = time(15, 0)

#: An exchange record, or a current-state board read after that same session
#: closed. Unknown sources land here too, so a newly introduced authority is
#: never silently overwritten before someone classifies it.
EVIDENCE_POINT_IN_TIME = 2
#: Reconstructed from other lake evidence rather than reported by a source.
EVIDENCE_DERIVED = 1
#: A current-state board stamped onto a session it did not observe.
EVIDENCE_RESTATED = 0

_EVIDENCE_INPUTS = ("source", "trade_date", "fetched_at")

#: ``fetched_at`` is stored as a UTC instant, and the exchange runs at a fixed
#: UTC+08:00 (see ``domain/market_time``). Shifting the instant is therefore
#: exact, and keeps both readers free of an IANA timezone database.
_SHANGHAI_OFFSET_HOURS = 8
_SHANGHAI_OFFSET_US = _SHANGHAI_OFFSET_HOURS * 3600 * 1_000_000


def _shanghai(value: object) -> datetime | None:
    """Read a stored ``fetched_at`` as Asia/Shanghai wall time."""
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(SHANGHAI_TZ)


def evidence_rank(row: Mapping[str, object]) -> int:
    """Evidence class of one status row. Higher wins a primary-key collision."""
    source = str(row.get("source") or "")
    if source in CURRENT_SNAPSHOT_SOURCES:
        fetched = _shanghai(row.get("fetched_at"))
        trade_date = row.get("trade_date")
        if (
            fetched is not None
            and isinstance(trade_date, date)
            and fetched.date() == trade_date
            and fetched.time() >= SESSION_FINAL_AT
        ):
            return EVIDENCE_POINT_IN_TIME
        return EVIDENCE_RESTATED
    if source == DERIVED_BAR_GAP_SOURCE:
        return EVIDENCE_DERIVED
    # baostock, DELISTED_SOURCE and anything not yet classified.
    return EVIDENCE_POINT_IN_TIME


def evidence_rank_expr(schema: Iterable[str] | Mapping[str, pl.DataType]) -> pl.Expr | None:
    """Columnar :func:`evidence_rank`, or ``None`` when the frame cannot say.

    A frame missing any of ``source`` / ``trade_date`` / ``fetched_at`` cannot
    distinguish a same-session board read from a restatement, so the caller
    falls back to its ordinary ordering instead of inventing a precedence.

    Pass a schema rather than bare column names where one is available: the
    lake still holds fragments that stored ``fetched_at`` as text or without a
    timezone, and the wall-clock comparison below has to read those the same
    way :func:`evidence_rank` does instead of failing the whole query.
    """
    if not set(_EVIDENCE_INPUTS).issubset(set(schema)):
        return None
    dtype = schema.get("fetched_at") if isinstance(schema, Mapping) else None
    fetched = pl.col("fetched_at")
    if dtype == pl.Utf8:
        fetched = fetched.str.to_datetime(time_zone="UTC", strict=False)
    elif isinstance(dtype, pl.Datetime):
        if dtype.time_zone is None:
            fetched = fetched.dt.replace_time_zone("UTC")
    elif dtype is not None:
        # A fragment whose ``fetched_at`` is not a timestamp at all cannot say
        # when it was observed. Schema validation is what reports that; here it
        # only means this frame gets the ordinary ordering rather than an
        # exception raised from inside somebody's query.
        return None
    fetched_local = (fetched + pl.duration(hours=_SHANGHAI_OFFSET_HOURS)).dt.replace_time_zone(None)
    same_session = (
        fetched.is_not_null()
        & (fetched_local.dt.date() == pl.col("trade_date"))
        & (fetched_local.dt.time() >= SESSION_FINAL_AT)
    )
    snapshot = pl.col("source").is_in(list(CURRENT_SNAPSHOT_SOURCES))
    return (
        pl.when(snapshot & same_session)
        .then(EVIDENCE_POINT_IN_TIME)
        .when(snapshot)
        .then(EVIDENCE_RESTATED)
        .when(pl.col("source") == DERIVED_BAR_GAP_SOURCE)
        .then(EVIDENCE_DERIVED)
        .otherwise(EVIDENCE_POINT_IN_TIME)
        .cast(pl.Int8)
    )


def evidence_rank_sql(columns: Iterable[str] | Mapping[str, str]) -> str | None:
    """SQL form of :func:`evidence_rank_expr` for the DuckDB read path.

    DuckDB is a separate reader from the polars one, and a lake that answers
    two different things depending on which one you asked is worse than a lake
    that answers the wrong thing consistently. ``tests`` pin the two forms to
    the same fixtures for that reason.

    Pass ``{column: type}`` where the reader knows the types. The polars form
    above declines a ``fetched_at`` that is not a timestamp and lets the caller
    fall back to its ordinary ordering; a name-only view would apply the CASE
    to the same fragment and order it differently — the exact divergence this
    pairing exists to prevent. Bare names keep the old behaviour, which is what
    :func:`evidence_rank_expr` does with a schema it cannot type either.
    """
    if not set(_EVIDENCE_INPUTS).issubset(set(columns)):
        return None
    if isinstance(columns, Mapping):
        fetched_type = str(columns.get("fetched_at") or "").strip().upper()
        if not fetched_type.startswith("TIMESTAMP"):
            return None
    boards = ", ".join(f"'{name}'" for name in sorted(CURRENT_SNAPSHOT_SOURCES))
    local = f"make_timestamp(epoch_us(fetched_at) + {_SHANGHAI_OFFSET_US})"
    final_at = SESSION_FINAL_AT.strftime("%H:%M:%S")
    return (
        "CASE "
        f"WHEN source IN ({boards}) THEN "
        f"CASE WHEN fetched_at IS NOT NULL AND CAST({local} AS DATE) = trade_date "
        f"AND CAST({local} AS TIME) >= TIME '{final_at}' "
        f"THEN {EVIDENCE_POINT_IN_TIME} ELSE {EVIDENCE_RESTATED} END "
        f"WHEN source = '{DERIVED_BAR_GAP_SOURCE}' THEN {EVIDENCE_DERIVED} "
        f"ELSE {EVIDENCE_POINT_IN_TIME} END"
    )
