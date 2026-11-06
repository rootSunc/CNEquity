"""EastMoney valuation metrics (PE/PB/PS/market cap)."""

from __future__ import annotations

import logging
from datetime import date

import polars as pl

from cnequity.adapters.eastmoney.clist import clist_rows_to_symbols, fetch_clist_pages
from cnequity.adapters.eastmoney.common import _to_float
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.domain.market_time import last_closed_session
from cnequity.domain.valuation import MV_VENDOR_REPORTED
from cnequity.domain.value_rules import apply_value_rules
from cnequity.orchestrator.outcomes import SourceUnavailableError

# f130 is 市销率 TTM. f45 — which this used to read as ps_ttm — is an amount in
# yuan, not a ratio: it put a median of 2.05e7 into `ps_ttm` for every EastMoney
# row while baostock's median for the same column was 3.2. Verified against the
# feed's own numbers, twice: total_mv / f132 (营业总收入 TTM) equals f130 exactly
# for 600519 (1.591e12 / 1.732e11 = 9.184) and for 000001 (2.294e11 / 1.327e11
# = 1.729).
_VALUATION_FIELDS = "f12,f13,f9,f23,f130,f20,f21"
logger = logging.getLogger(__name__)


def fetch_valuation_metrics(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config=None,
) -> pl.DataFrame:
    """datacenter's valuation for *trade_date*, or push2's snapshot as a fallback.

    datacenter (``RPT_VALUEANALYSIS_DET``, rows labelled ``eastmoney_datacenter``)
    is primary: its P/E is true TTM like baostock's history, it is keyed by
    date, covers Beijing, and its host is not the one that bans for volume.
    push2's clist snapshot is the fallback for a session datacenter has not
    published yet or cannot serve; its rows stay ``eastmoney`` because its f9
    is the dynamic P/E, not TTM (see ``valuation_datacenter``). The snapshot
    carries no date, so the fallback only serves *trade_date* while it is the
    last closed session; a catch-up run over older days used to stamp one live
    snapshot onto each of them.

    A bare call (no config) keeps the historical push2-only behaviour.
    """
    if config is None:
        return _fetch_valuation_push2(trade_date, client=client, config=config)
    from cnequity.adapters.eastmoney.valuation_datacenter import (
        SOURCE,
        fetch_valuation_datacenter,
    )

    try:
        primary = fetch_valuation_datacenter(trade_date, client=client, config=config)
        if not primary.is_empty():
            return primary.with_columns(pl.lit(SOURCE).alias("source"))
        failure: Exception = RuntimeError(
            f"datacenter has no valuation for {trade_date.isoformat()} yet"
        )
    except Exception as exc:  # noqa: BLE001 — every datacenter failure gets the fallback
        failure = exc
    if last_closed_session() != trade_date:
        raise SourceUnavailableError(
            f"valuation_metrics: datacenter has nothing usable for {trade_date.isoformat()} "
            f"({failure}); push2's clist snapshot is undated live data and describes that "
            "session only from its 15:30 close until the next session opens (Beijing)"
        ) from failure
    logger.warning(
        "valuation_metrics: datacenter failed (%s); trying push2 for %s",
        failure,
        trade_date.isoformat(),
    )
    try:
        fallback = _fetch_valuation_push2(trade_date, client=client, config=config)
    except Exception as exc:  # noqa: BLE001
        logger.warning("valuation_metrics: push2 fallback failed too: %s", exc)
        raise failure from exc
    if fallback.is_empty():
        raise failure
    return fallback


def _fetch_valuation_push2(
    trade_date: date,
    *,
    client: EastMoneyClient | None = None,
    config=None,
) -> pl.DataFrame:
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)
    try:
        rows_raw = fetch_clist_pages(client, fields=_VALUATION_FIELDS)
        mapped_rows = clist_rows_to_symbols(rows_raw)
        if len(mapped_rows) != len(rows_raw):
            logger.warning(
                "EastMoney valuation_metrics clist dropped %d non-security row(s)",
                len(rows_raw) - len(mapped_rows),
            )
        rows = []
        for sym, item in mapped_rows:
            rows.append(
                {
                    "symbol": sym,
                    "trade_date": trade_date,
                    # f9 is the dynamic P/E (latest report annualised), not TTM.
                    "pe_ttm": None,
                    "pe_dynamic": _to_float(item.get("f9")),
                    "total_mv_basis": MV_VENDOR_REPORTED,
                    "float_mv_basis": MV_VENDOR_REPORTED,
                    "pb": _to_float(item.get("f23")),
                    "ps_ttm": _to_float(item.get("f130")),
                    "total_mv": _to_float(item.get("f20")),
                    "float_mv": _to_float(item.get("f21")),
                }
            )
    finally:
        if owns:
            client.close()
    return apply_value_rules(
        pl.DataFrame(rows).unique(subset=["symbol", "trade_date"], keep="last")
        if rows
        else pl.DataFrame(),
        "valuation_metrics",
    )
