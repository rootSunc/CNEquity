"""Derivative coverage checks with explicit evidence limits.

A prior-session contract remains expected until an authoritative reference
ends its life. Missing rows cannot create their own expiry evidence. Calendar
sessions and exchange boundaries are checked independently of dense watermarks;
without a full live universe, no missing known keys means unverified, not full
market completeness (ADR-0017). The exchange's official contract file for the
session itself is that universe: an exchange with one, and no contract on it
missing, is proven complete.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.datasets import is_dataset_enabled
from cnequity.i18n import N_

__all__ = [
    "DERIVATIVE_BAR_CONTRACTS",
    "derivative_findings",
    "derivative_tip_scope",
    "exchange_session_gaps",
]

HEADING = N_("Ingest cross-section (futures/options, by exchange)")

#: Exchanges whose default route is a vendor series that omits no-trade
#: sessions (DCE via Sina); the tip proof cannot hold them to every contract.
VENDOR_ROUTED = frozenset({"DCE"})

#: Bars dataset → the contract table naming each contract's last trading day.
DERIVATIVE_BAR_CONTRACTS = {
    "futures_bars": ("futures_contracts", "last_trade_date"),
    "option_bars": ("option_contracts", "expiry_date"),
}


def _scan(config: Config, dataset: str) -> pl.LazyFrame | None:
    from cnequity.storage.published import published

    return published(config, dataset).scan()


def _last_trade_dates(config: Config, bars_dataset: str) -> dict[str, date]:
    table, column = DERIVATIVE_BAR_CONTRACTS[bars_dataset]
    contracts = _scan(config, table)
    if contracts is None:
        return {}
    frame = (
        contracts.filter(pl.col("dates_basis") == "exchange")
        .select("symbol", column)
        .filter(pl.col(column).is_not_null())
        .unique(subset=["symbol"], keep="last")
        .collect()
    )
    return dict(frame.iter_rows())


def derivative_tip_scope(config: Config, dataset: str) -> dict | None:
    """Cross-section proof for the tip session of a futures/option bars dataset.

    Same shape as the equity proof in ``cne status`` (complete / incomplete /
    unverified), so both print through one loop.
    """
    if dataset not in DERIVATIVE_BAR_CONTRACTS or not is_dataset_enabled(dataset, config):
        return None
    bars = _scan(config, dataset)
    if bars is None:
        return {
            "dataset": dataset,
            "heading": HEADING,
            "state": "unverified",
            "date": None,
            "message": f"{dataset} has no data yet, so coverage cannot be verified",
        }
    from cnequity.adapters.futures_exchange import members
    from cnequity.adapters.futures_exchange.registry import expected_exchanges

    kind = "futures" if dataset == "futures_bars" else "options"
    try:
        tip = bars.select(pl.col("trade_date").max()).collect().item()
        if tip is None:
            return None
        from datetime import timedelta

        from cnequity.query.calendar import list_trading_dates

        sessions = list_trading_dates(config, tip - timedelta(days=40), tip)
        previous = sessions[-2] if len(sessions) >= 2 else None
        if previous is None:
            return {
                "dataset": dataset,
                "heading": HEADING,
                "state": "unverified",
                "date": tip,
                "message": (
                    f"{dataset} has only one session, so there is no previous session "
                    "to derive the expected contracts from"
                ),
            }
        pairs = (
            bars.filter(pl.col("trade_date").is_in([previous, tip]))
            .select("trade_date", "exchange", "symbol")
            .unique()
            .collect()
        )
        ends = _last_trade_dates(config, dataset)
        reference_live: dict[str, set[str]] = {}
        # Exchanges whose official contract file for the tip session itself was
        # archived: that file names every contract live that day, which is the
        # complete universe the carried-forward set alone cannot give.
        referenced: set[str] = set()
        reference_root = config.meta_root / "derivatives" / "references"
        wanted_kind = "future" if kind == "futures" else "option"
        for path in reference_root.glob(f"*/{tip}*.parquet"):
            ref = pl.read_parquet(path)
            if {"kind", "list_date", "last_trade_date", "symbol"} <= set(ref.columns):
                of_kind = ref.filter(pl.col("kind") == wanted_kind)
                if not of_kind.is_empty():
                    referenced.add(path.parent.name)
                live = of_kind.filter(
                    (pl.col("list_date") <= tip) & (pl.col("last_trade_date") >= tip)
                )
                reference_live.setdefault(path.parent.name, set()).update(live["symbol"].to_list())
        before = pairs.filter(pl.col("trade_date") == previous)
        observed = set(pairs.filter(pl.col("trade_date") == tip)["symbol"].to_list())
        expected: set[str] = set()
        per_exchange: dict[str, list[int]] = {}
        unverifiable: list[str] = []
        unproven: list[str] = []
        missing_exchanges: list[str] = []
        for exchange in expected_exchanges(config, kind, tip):
            if exchange in VENDOR_ROUTED and getattr(config, "futures_dce_route", "sina") == "sina":
                # Sina drops sessions without a trade, so a contract missing
                # from the tip may simply not have traded: not provable.
                unverifiable.append(exchange)
                continue
            tip_rows = pairs.filter(
                (pl.col("trade_date") == tip) & pl.col("exchange").is_in(list(members(exchange)))
            )
            if tip_rows.is_empty():
                missing_exchanges.append(exchange)
            rows = before.filter(pl.col("exchange").is_in(list(members(exchange))))
            if exchange not in referenced:
                # Without the day's official list, contracts listed that day
                # are outside what the previous session can predict.
                unproven.append(exchange)
            live = {
                symbol
                for symbol in rows["symbol"].to_list()
                if symbol not in ends or ends[symbol] >= tip
            }
            live |= reference_live.get(exchange, set())
            expected |= live
            per_exchange[exchange] = [len(live & observed), len(live)]
    except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
        return {
            "dataset": dataset,
            "heading": HEADING,
            "state": "unverified",
            "date": tip if "tip" in locals() else None,
            "message": f"Failed to read derivative cross-section evidence: {exc}",
        }
    if not expected and not missing_exchanges:
        return {
            "dataset": dataset,
            "heading": HEADING,
            "state": "unverified",
            "date": tip,
            "message": (
                f"{previous} has no provable exchange contracts, so the contracts "
                f"expected on {tip} cannot be derived"
            ),
        }
    from cnequity.storage.derivative_evidence import owed_sessions

    owed = owed_sessions(config, dataset, tip)
    missing = sorted(expected - observed)
    covered = len(expected) - len(missing)
    if missing or missing_exchanges:
        state = "incomplete"
    elif unproven:
        state = "unverified"
    else:
        # Every exchange that can be proven has its official list for the day
        # and every contract on it has a bar. A vendor route that cannot be
        # proven is reported beside the result (`unverifiable_exchanges`) as a
        # property of the source, not counted as a failure of the data.
        state = "complete"
    return {
        "dataset": dataset,
        "heading": HEADING,
        "state": state,
        "date": tip,
        "covered": covered,
        "expected": len(expected),
        "ratio": covered / len(expected) if expected else 0.0,
        "missing": missing,
        "owed": len(owed),
        "owed_dates": [d.isoformat() for d in owed],
        "noun": N_("listed contracts"),
        "unverifiable_exchanges": unverifiable,
        "missing_exchanges": missing_exchanges,
        "unproven_exchanges": unproven,
        "message": (
            "previous-session contract coverage check; without the official list of "
            f"contracts live that day ({', '.join(unproven)}), newly listed contracts "
            "cannot be proven complete"
        ),
        "exchanges": per_exchange,
        "coverage": bars.group_by(["exchange", "product"])
        .agg(
            pl.col("trade_date").min().alias("first"),
            pl.col("trade_date").max().alias("last"),
            pl.col("trade_date").n_unique().alias("sessions"),
            pl.col("symbol").n_unique().alias("contracts"),
        )
        .collect()
        .sort("exchange", "product")
        .to_dicts(),
    }


def exchange_session_gaps(
    config: Config, dataset: str, *, start: date | None = None, end: date | None = None
) -> dict[str, list[date]]:
    """Sessions with no rows for an exchange, inside that exchange's own span.

    Sessions the exchange never published (``UNPUBLISHED_FUTURES_SESSIONS``)
    are not gaps: nothing can fill them.
    """
    if dataset not in DERIVATIVE_BAR_CONTRACTS:
        raise ValueError(f"Unsupported derivative bars dataset: {dataset}")
    if start and end and start > end:
        raise ValueError("start must not exceed end")
    bars = _scan(config, dataset)
    from cnequity.adapters.futures_exchange import members
    from cnequity.adapters.futures_exchange.registry import enabled_exchanges, reader
    from cnequity.domain.derivatives import UNPUBLISHED_FUTURES_SESSIONS
    from cnequity.query.calendar import list_trading_dates

    present = (
        bars.select("exchange", "trade_date").unique().collect()
        if bars is not None
        else pl.DataFrame(schema={"exchange": pl.String, "trade_date": pl.Date})
    )
    if present.is_empty() and (start is None or end is None):
        return {}
    start = start or present["trade_date"].min()
    end = end or present["trade_date"].max()
    kind = "futures" if dataset == "futures_bars" else "options"
    gaps: dict[str, list[date]] = {}
    for publisher in enabled_exchanges(config):
        floor = reader(config, publisher).first_session(kind)
        if floor is None:
            continue
        for name in members(publisher):
            days = set(present.filter(pl.col("exchange") == name)["trade_date"].to_list())
            # INE's publisher predates INE itself. Until a dated exchange
            # universe is available, its leading boundary remains unverified.
            member_floor = min(days) if name == "INE" and days else floor
            if name == "INE" and not days:
                continue
            excused = (
                UNPUBLISHED_FUTURES_SESSIONS.get(name, frozenset())
                if dataset == "futures_bars"
                else frozenset()
            )
            sessions = list_trading_dates(config, max(start, floor, member_floor), end)
            missing = [d for d in sessions if d not in days and d not in excused]
            if missing:
                gaps[name] = missing

    return gaps


def _unknown_product_findings(config: Config) -> list[dict]:
    findings: list[dict] = []
    for table in ("futures_contracts", "option_contracts"):
        contracts = _scan(config, table)
        if contracts is None:
            continue
        unknown = (
            contracts.filter(pl.col("multiplier").is_null())
            .select("exchange", "product")
            .unique()
            .collect()
        )
        if unknown.is_empty():
            continue
        names = sorted(f"{e}:{p}" for e, p in unknown.iter_rows())
        findings.append(
            {
                "dataset": table,
                "severity": "warning",
                "check": "futures_unknown_product",
                "message": (
                    f"{table}: {len(names)} products have no contract spec (multiplier/tick "
                    f"size empty); add them to domain/futures_products.py: {', '.join(names[:10])}"
                ),
                "products": names,
            }
        )
    return findings


#: Sessions of recent history the spec checks read; the table describes the
#: current contract rules, so older sessions would test old ones.
SPEC_CHECK_SESSIONS = 20


def _recent(bars: pl.LazyFrame, sessions: int) -> pl.DataFrame:
    days = (
        bars.select(pl.col("trade_date").unique().sort(descending=True).head(sessions))
        .collect()["trade_date"]
        .to_list()
    )
    return bars.filter(pl.col("trade_date").is_in(days) & (pl.col("volume") > 0)).collect()


def spec_findings(config: Config, dataset: str = "futures_bars") -> list[dict]:
    """Traded prices off the tick grid, and lot sizes the turnover contradicts.

    Both specs live in a hand-kept table (``domain/futures_products.py``), so
    both are checked against the data. A price not on the product's tick grid
    means the tick is wrong (or the parse is). ``amount / (volume × settle)``
    is the lot size to within the gap between settlement and the day's average
    price, so a ratio far from the table's multiplier means the multiplier is
    wrong. Futures only for the second: an option's settlement can sit far
    from where it traded.
    """
    from cnequity.domain.derivatives import parse_future_code, parse_option_code
    from cnequity.domain.futures_products import product_spec

    bars = _scan(config, dataset)
    if bars is None:
        return []
    recent = _recent(bars, SPEC_CHECK_SESSIONS)
    if recent.is_empty():
        return []
    kind = "future" if dataset == "futures_bars" else "option"
    parse = parse_future_code if kind == "future" else parse_option_code
    findings: list[dict] = []
    from cnequity.domain.derivatives import EXCHANGES

    for (exchange, product), group in recent.group_by(["exchange", "product"]):
        if exchange not in EXCHANGES:
            continue  # SSE/SZSE ETF options: terms come from the exchange list, not this table
        sample = group.row(0, named=True)
        contract = parse(sample["exchange_code"], exchange, sample["trade_date"])
        series = contract if kind == "future" else contract.series
        spec = product_spec(
            exchange, product, kind, on=group["trade_date"].min(), delivery=series.delivery_month
        )
        if spec is None:
            continue
        prices = pl.concat([group[c] for c in ("open", "high", "low", "close")]).drop_nulls()
        off = ((prices / spec.tick_size) - (prices / spec.tick_size).round(0)).abs() > 1e-6
        share = float(off.mean()) if prices.len() else 0.0
        if share > 0.01:
            findings.append(
                {
                    "dataset": dataset,
                    "severity": "warning",
                    "check": "futures_tick_grid",
                    "message": (
                        f"{dataset}: {exchange}:{product} over the last {SPEC_CHECK_SESSIONS} "
                        f"sessions, {share:.1%} of traded prices are not a multiple of the tick "
                        f"size {spec.tick_size}; check the tick in domain/futures_products.py"
                    ),
                    "exchange": exchange,
                    "product": product,
                }
            )
        if kind != "future":
            continue
        priced = group.filter(pl.col("amount").is_not_null() & (pl.col("amount") > 0))
        if priced.is_empty():
            continue
        implied = float((priced["amount"] / (priced["volume"] * priced["settle"])).median())
        if abs(implied / spec.multiplier - 1) > 0.15:
            findings.append(
                {
                    "dataset": dataset,
                    "severity": "warning",
                    "check": "futures_multiplier_mismatch",
                    "message": (
                        f"{dataset}: {exchange}:{product} turnover implies a contract "
                        f"multiplier of about {implied:.2f}, but the spec table says "
                        f"{spec.multiplier:g}; check domain/futures_products.py"
                    ),
                    "exchange": exchange,
                    "product": product,
                    "implied_multiplier": implied,
                }
            )
    return findings


def derivative_findings(config: Config) -> list[dict]:
    """Audit findings for the enabled futures/option datasets."""
    findings: list[dict] = []
    for dataset in DERIVATIVE_BAR_CONTRACTS:
        if not is_dataset_enabled(dataset, config):
            continue
        for exchange, missing in sorted(exchange_session_gaps(config, dataset).items()):
            findings.append(
                {
                    "dataset": dataset,
                    "severity": "warning",
                    "check": "futures_exchange_session_gap",
                    "message": (
                        f"{dataset}: {exchange} is missing {len(missing)} sessions inside the "
                        f"dataset's research window (e.g. "
                        f"{', '.join(d.isoformat() for d in missing[:5])}); "
                        f"`cne backfill {dataset} --start {missing[0].isoformat()} "
                        f"--end {missing[-1].isoformat()}` refetches only the sessions in the gap"
                    ),
                    "exchange": exchange,
                    "days_missing": len(missing),
                    "sample_dates": [d.isoformat() for d in missing[:8]],
                }
            )
    for dataset, (table, _) in DERIVATIVE_BAR_CONTRACTS.items():
        if is_dataset_enabled(dataset, config):
            findings.extend(spec_findings(config, dataset))
            bars, contracts = _scan(config, dataset), _scan(config, table)
            if bars is not None:
                symbols = set(bars.select("symbol").unique().collect()["symbol"].to_list())
                known = (
                    set(contracts.select("symbol").unique().collect()["symbol"].to_list())
                    if contracts is not None
                    else set()
                )
                missing = sorted(symbols - known)
                if missing:
                    findings.append(
                        {
                            "dataset": table,
                            "severity": "warning",
                            "check": "derivative_contracts_missing",
                            "message": (
                                f"{table}: {len(missing)} contracts in the bars have no "
                                "metadata; rebuild the contract table"
                            ),
                            "sample": missing[:10],
                        }
                    )
    if is_dataset_enabled("futures_contracts", config) or is_dataset_enabled(
        "option_contracts", config
    ):
        findings.extend(_unknown_product_findings(config))
    return findings
