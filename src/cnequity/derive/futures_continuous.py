"""Main and second-month continuous futures, built without looking ahead.

Rule ``oi_t-1_v2``, per product:

- **Main** on session T is chosen from session T-1's close: the eligible
  contract with the largest open interest. Nothing from T itself decides
  which contract T belongs to, so a backtest reading this series never sees
  the roll before the market could.
- **Eligible** means not about to stop trading. A contract whose last
  trading day is known (authoritative reference file) drops out five
  calendar days before it; one without a known end drops out when its
  delivery month begins. CFFEX index futures are the reason for the first
  form: their front month stays the main contract until the week it expires.
- **Rolls go forward only.** The main switches when another eligible contract
  of a *later* month out-holds it, or when the main stops being eligible.
- **Second** is the largest-open-interest eligible contract delivering after
  the main.

Each series carries two cumulative adjustment factors, updated on a roll from
the two contracts' settlements on T-1 (both known at that close):
``adj_ratio`` multiplies raw prices, ``adj_diff`` is added to them, and either
makes the series continuous across rolls while leaving the earliest prices as
published — the same direction ADR-0004 takes for equities. Divide by (or
subtract) the latest factor at query time for the back-adjusted view.

``roll_yield`` on the main series is ``ln(main / second)`` over the gap between
their delivery months, in years: positive in backwardation.
"""

from __future__ import annotations

import math
from datetime import date, timedelta

import polars as pl

RULE = "oi_t-1_v2"
#: Calendar days before a known last trading day at which a contract stops
#: being eligible to be the main.
EXPIRY_BUFFER_DAYS = 5

__all__ = ["RULE", "compute_futures_continuous", "derive_futures_continuous"]


def _price(row: dict) -> float | None:
    return row.get("settle") or row.get("close")


def _eligible(row: dict, on: date, ends: dict[str, date], deliveries: dict[str, date]) -> bool:
    end = ends.get(row["symbol"])
    if end is not None:
        return (end - on).days > EXPIRY_BUFFER_DAYS
    delivery = deliveries.get(row["symbol"])
    return delivery is None or (delivery.year, delivery.month) > (on.year, on.month)


def _pick(rows: list[dict], *, after: date | None, deliveries: dict[str, date]) -> dict | None:
    candidates = [
        r
        for r in rows
        if after is None
        or (deliveries.get(r["symbol"]) is not None and deliveries[r["symbol"]] > after)
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda r: (r.get("open_interest") or 0, -(deliveries[r["symbol"]].toordinal())),
    )


class _Series:
    def __init__(self) -> None:
        self.contract: str | None = None
        self.ratio = 1.0
        self.diff = 0.0

    def roll_to(self, new: str, prev_rows: dict[str, dict]) -> bool:
        if self.contract is None:
            self.contract = new
            return False
        old_price = _price(prev_rows.get(self.contract, {}))
        new_price = _price(prev_rows.get(new, {}))
        if old_price and new_price and self.ratio is not None:
            self.ratio *= old_price / new_price
            self.diff += old_price - new_price
        else:
            # Once the link is unobservable, no later roll can repair it.
            self.ratio = None
            self.diff = None
        self.contract = new
        return True


def _product_rows(
    frame: pl.DataFrame,
    ends: dict[str, date],
    deliveries: dict[str, date],
    previous_sessions: dict[date, date] | None = None,
) -> list[dict]:
    by_day: dict[date, dict[str, dict]] = {}
    for row in frame.iter_rows(named=True):
        by_day.setdefault(row["trade_date"], {})[row["symbol"]] = row
    days = sorted(by_day)
    series = {"main": _Series(), "second": _Series()}
    out: list[dict] = []
    for previous, day in zip(days, days[1:], strict=False):
        if previous_sessions is not None and previous_sessions.get(day) != previous:
            for state in series.values():
                state.ratio = None
                state.diff = None
            continue
        prev_rows = by_day[previous]
        eligible = [r for r in prev_rows.values() if _eligible(r, day, ends, deliveries)]
        main = series["main"]
        best = _pick(eligible, after=None, deliveries=deliveries)
        rolled = {"main": False, "second": False}
        if best is not None:
            current = prev_rows.get(main.contract) if main.contract else None
            current_ok = current is not None and _eligible(current, day, ends, deliveries)
            if main.contract is None or not current_ok:
                later = _pick(
                    eligible,
                    after=deliveries.get(main.contract) if main.contract else None,
                    deliveries=deliveries,
                )
                if later is not None:
                    rolled["main"] = main.roll_to(later["symbol"], prev_rows)
            elif (
                best["symbol"] != main.contract
                and deliveries[best["symbol"]] > deliveries[main.contract]
                and (best.get("open_interest") or 0) > (current.get("open_interest") or 0)
            ):
                rolled["main"] = main.roll_to(best["symbol"], prev_rows)
        second_best = None
        if main.contract is not None:
            second_best = _pick(
                eligible, after=deliveries.get(main.contract), deliveries=deliveries
            )
            if second_best is not None and second_best["symbol"] != series["second"].contract:
                rolled["second"] = series["second"].roll_to(second_best["symbol"], prev_rows)
        # Retain the previous second internally to preserve factor continuity,
        # but never emit it when no eligible contract follows today's main.
        today = by_day[day]
        second_row = today.get(second_best["symbol"]) if second_best is not None else None
        for name, state in series.items():
            if name == "second" and second_best is None:
                continue
            if state.contract not in {r["symbol"] for r in eligible}:
                continue
            row = today.get(state.contract or "")
            if row is None:
                continue
            roll_yield = None
            if name == "main" and second_row is not None:
                near, far = _price(row), _price(second_row)
                gap = (deliveries[second_row["symbol"]] - deliveries[row["symbol"]]).days / 365.25
                if near and far and gap > 0:
                    roll_yield = math.log(near / far) / gap
            out.append(
                {
                    "series": name,
                    "trade_date": day,
                    "contract_symbol": row["symbol"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "settle": row["settle"],
                    "volume": row["volume"],
                    "open_interest": row["open_interest"],
                    "rolled": rolled[name],
                    "adj_ratio": state.ratio,
                    "adj_diff": state.diff,
                    "roll_yield": roll_yield,
                    "rule": RULE,
                }
            )
    return out


def compute_futures_continuous(
    bars: pl.DataFrame,
    *,
    ends: dict[str, date] | None = None,
    deliveries: dict[str, date] | None = None,
    sessions: list[date] | None = None,
) -> pl.DataFrame:
    """Main and second series for every product in *bars*.

    ``deliveries`` maps a contract to its delivery month (first day);
    ``ends`` to its last trading day where known. Both come from
    ``futures_contracts``; a contract missing from ``deliveries`` is read off
    its symbol.
    """
    if bars.is_empty():
        return pl.DataFrame()
    ends = dict(ends or {})
    deliveries = dict(deliveries or {})
    for symbol in bars["symbol"].unique().to_list():
        if symbol not in deliveries:
            code = symbol.split(".")[0]
            digits = "".join(ch for ch in code if ch.isdigit())[-4:]
            deliveries[symbol] = date(2000 + int(digits[:2]), int(digits[2:]), 1)
    if sessions is None:
        from cnequity.domain.datasets import _is_exchange_session

        first, last = bars["trade_date"].min(), bars["trade_date"].max()
        sessions = [
            first + timedelta(days=i)
            for i in range((last - first).days + 1)
            if _is_exchange_session(first + timedelta(days=i))
        ]
    sessions = sorted(set(sessions))
    # The shared calendar can follow equity holidays while an exchange's
    # official futures file has a session (notably early SHFE history). Add
    # observed sessions for that exchange only; another exchange's special
    # session must not create a false gap here.
    observed: dict[str, set[date]] = {}
    for exchange, day in bars.select("exchange", "trade_date").unique().iter_rows():
        observed.setdefault(exchange, set()).add(day)
    previous_by_exchange: dict[str, dict[date, date]] = {}
    rows: list[dict] = []
    for (exchange, product), group in bars.group_by(["exchange", "product"]):
        if exchange not in previous_by_exchange:
            days = sorted(set(sessions) | observed[exchange])
            previous_by_exchange[exchange] = dict(zip(days[1:], days[:-1], strict=True))
        for row in _product_rows(group, ends, deliveries, previous_by_exchange[exchange]):
            rows.append({"symbol": f"{product}.{exchange}", **row})
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows, infer_schema_length=None).sort("symbol", "series", "trade_date")


def _curated(config, dataset: str) -> pl.DataFrame | None:
    from cnequity.storage.read_context import read_root

    root = read_root(config, dataset)
    files = list(root.glob("**/*.parquet")) if root.exists() else []
    if not files:
        return None
    from cnequity.query.canonical import dedupe_by_primary_key
    from cnequity.query.parquet_scan import scan_parquet_files

    return dedupe_by_primary_key(scan_parquet_files(files).collect(), dataset)


def derive_futures_continuous(config) -> dict:
    """Rebuild ``futures_continuous`` from curated bars and contracts.

    A full rebuild, not an increment: the adjustment factors are cumulative
    from each product's first session, so extending the tail alone would carry
    forward any history a backfill has since changed.
    """
    from cnequity.domain.datasets import DATASETS
    from cnequity.domain.schemas import validate_dataframe, with_provenance
    from cnequity.file_lock import lake_mutation_lock
    from cnequity.storage.parquet import CuratedWriter
    from cnequity.storage.state import StateStore

    bars = _curated(config, "futures_bars")
    if bars is None or bars.is_empty():
        return {"rows": 0, "note": "no futures_bars yet"}
    contracts = _curated(config, "futures_contracts")
    ends: dict[str, date] = {}
    deliveries: dict[str, date] = {}
    if contracts is not None and not contracts.is_empty():
        for symbol, delivery, last, basis in contracts.select(
            "symbol", "delivery_month", "last_trade_date", "dates_basis"
        ).iter_rows():
            if delivery is not None:
                deliveries[symbol] = delivery
            if last is not None and basis == "exchange":
                ends[symbol] = last
    from cnequity.query.calendar import list_trading_dates

    sessions = list_trading_dates(config, bars["trade_date"].min(), bars["trade_date"].max())
    frame = compute_futures_continuous(bars, ends=ends, deliveries=deliveries, sessions=sessions)
    if frame.is_empty():
        return {"rows": 0, "note": "no product has two sessions yet"}
    frame = validate_dataframe(
        with_provenance(frame, source="derived", data_version="v1"), "futures_continuous"
    )
    root = config.derived_root / "futures_continuous"
    writer = CuratedWriter(config.derived_root)
    with lake_mutation_lock(config.meta_root, blocking=True):
        keep: set[str] = set()
        for (period,), group in (
            frame.with_columns(
                pl.col("trade_date")
                .map_elements(DATASETS["futures_continuous"].partition_for, return_dtype=pl.Utf8)
                .alias("_p")
            )
            .partition_by("_p", as_dict=True)
            .items()
        ):
            keep.add(f"{DATASETS['futures_continuous'].partition_col}={period}")
            writer.write_partition(
                "futures_continuous", "trade_date", period, group.drop("_p"), "part-000.parquet"
            )
        # A product that lost history in a rebuild must not keep stale months.
        if root.exists():
            for stale in root.iterdir():
                if stale.is_dir() and stale.name not in keep:
                    for path in stale.rglob("*.parquet"):
                        path.unlink()
        StateStore(config.meta_root).set_date("futures_continuous", frame["trade_date"].max())
    return {
        "rows": frame.height,
        "products": frame["symbol"].n_unique(),
        "first": str(frame["trade_date"].min()),
        "last": str(frame["trade_date"].max()),
    }
