"""Compare the latest session in curated against Tushare, when a token is set.

Tushare is a peer here, not an authority: on 2026-11-04 it matched the lake
exactly wherever both carried the same data, but it also had wrong Shibor
values for 35 days of 2022-04 and listed one share unlock once per holder.
So every finding is at most a ``warning`` and nothing here changes curated
rows — a disagreement is a lead for the existing repair paths. The one bug
this would have caught early: on 2026-11-02 the daily factor derive stepped
45 symbols that Tushare (rightly) did not.

Each check costs one or two requests for one session; endpoints the account
cannot call are skipped after a cached permission probe. Without
``[sources.tushare]`` enabled and a token nothing runs and nothing is sent.
Results, including clean and skipped checks, are persisted to
``meta/quality/source_diffs/tushare-peer-<date>.json``.

Units: Tushare ``vol`` is lots (100 shares), ``amount`` thousand CNY, market
values and share counts 10k, money flow 10k CNY, index weights percent.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta

import polars as pl

from cnequity.adapters.tushare.client import (
    TushareClient,
    probe_permissions,
    tushare_enabled,
)
from cnequity.config import Config
from cnequity.domain.http_policy import SourceCoolingDown
from cnequity.query.canonical import dedupe_lazy_by_primary_key
from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
from cnequity.storage.atomic import write_json_atomic

logger = logging.getLogger(__name__)

_SAMPLE = 8
# A disagreement count up to this is reported as info: vendors legitimately
# differ on a handful of names on any day (a late restatement, a BJ volume
# definition). Above it, the share of the overlap decides.
_MIN_TOLERATED = 3
_TOLERATED_SHARE = 0.005
_STOCK_TYPES = ("stock", "cdr")


@dataclass(frozen=True)
class PeerOutcome:
    status: str
    findings: list[dict]


def _ymd(day: date) -> str:
    return day.strftime("%Y%m%d")


def _curated(
    config: Config, dataset: str, partition_col: str, start: date, end: date
) -> pl.DataFrame:
    root = (config.derived_root if dataset == "adj_factors" else config.curated_root) / dataset
    if not dataset_has_parquet(root):
        return pl.DataFrame()
    return (
        dedupe_lazy_by_primary_key(
            scan_parquet_root(root, partition_col=partition_col, start=start, end=end), dataset
        )
        .filter(pl.col(partition_col).is_between(pl.lit(start), pl.lit(end)))
        .collect()
    )


def _stock_symbols(config: Config) -> set[str]:
    root = config.curated_root / "instruments"
    if not dataset_has_parquet(root):
        return set()
    frame = dedupe_lazy_by_primary_key(scan_parquet_root(root), "instruments").collect()
    return set(frame.filter(pl.col("asset_type").is_in(_STOCK_TYPES))["symbol"].to_list())


def _previous_session(config: Config, day: date) -> date | None:
    from cnequity.query.calendar import list_trading_dates

    sessions = [d for d in list_trading_dates(config, day - timedelta(days=20), day) if d < day]
    return sessions[-1] if sessions else None


def _tolerated(overlap: int) -> int:
    return max(_MIN_TOLERATED, math.floor(overlap * _TOLERATED_SHARE))


def _value_finding(
    dataset: str,
    check: str,
    day: date,
    joined: pl.DataFrame,
    pairs: list[tuple[str, str]],
    *,
    rtol: float,
    atol: float = 1e-9,
) -> list[dict]:
    """``pairs`` are (lake column, peer column already in lake units)."""
    if joined.is_empty():
        return []
    bad = pl.any_horizontal(
        [
            (
                (pl.col(lake) - pl.col(peer)).abs() > (pl.col(peer).abs() * rtol).clip(atol)
            ).fill_null(False)
            for lake, peer in pairs
        ]
    )
    mismatched = joined.filter(bad)
    if mismatched.is_empty():
        return []
    columns = ["symbol", *[c for pair in pairs for c in pair]]
    return [
        {
            "dataset": dataset,
            "severity": "warning" if mismatched.height > _tolerated(joined.height) else "info",
            "check": check,
            "message": (
                f"{dataset}: {mismatched.height} of {joined.height} symbols disagree with Tushare "
                f"on {day.isoformat()} ({', '.join(lake for lake, _ in pairs)})"
            ),
            "trade_date": day.isoformat(),
            "overlap": joined.height,
            "disagreements": mismatched.height,
            "symbols": mismatched["symbol"].head(_SAMPLE).to_list(),
            "examples": mismatched.select(columns).head(3).to_dicts(),
        }
    ]


def _missing_finding(
    dataset: str, check: str, day: date, missing: list[str], peer_rows: int
) -> list[dict]:
    if not missing:
        return []
    return [
        {
            "dataset": dataset,
            "severity": "warning" if len(missing) > _tolerated(peer_rows) else "info",
            "check": check,
            "message": (
                f"{dataset}: Tushare has {len(missing)} of {peer_rows} rows on {day.isoformat()} "
                "that the lake does not"
            ),
            "trade_date": day.isoformat(),
            "peer_rows": peer_rows,
            "missing": len(missing),
            "symbols": sorted(missing)[:_SAMPLE],
        }
    ]


def _symbols(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.rename({"ts_code": "symbol"}) if "ts_code" in frame.columns else frame


# --- checks ---------------------------------------------------------------


def _daily_bars(config: Config, day: date, client: TushareClient) -> PeerOutcome:
    lake = _curated(config, "daily_bars", "trade_date", day, day).filter(pl.col("volume") > 0)
    if lake.is_empty():
        return PeerOutcome("skipped_no_curated", [])
    peer = _symbols(client.query("daily", {"trade_date": _ymd(day)})).filter(pl.col("vol") > 0)
    if peer.is_empty():
        return PeerOutcome("unavailable", [])
    peer = peer.select(
        "symbol",
        *[pl.col(c).cast(pl.Float64).alias(f"{c}_ts") for c in ("open", "high", "low", "close")],
        (pl.col("vol").cast(pl.Float64) * 100).alias("volume_ts"),
        (pl.col("amount").cast(pl.Float64) * 1000).alias("amount_ts"),
    )
    joined = lake.join(peer, on="symbol")
    findings = _value_finding(
        "daily_bars",
        "tushare_peer_daily_prices",
        day,
        joined,
        [(c, f"{c}_ts") for c in ("open", "high", "low", "close")],
        rtol=1e-4,
    )
    findings += _value_finding(
        "daily_bars",
        "tushare_peer_daily_turnover",
        day,
        joined,
        [("volume", "volume_ts"), ("amount", "amount_ts")],
        rtol=1e-2,
    )
    missing = set(peer["symbol"]) - set(lake["symbol"])
    findings += _missing_finding(
        "daily_bars", "tushare_peer_daily_missing", day, sorted(missing), peer.height
    )
    return PeerOutcome("disagreed" if findings else "agreed", findings)


def _adj_factor_steps(config: Config, day: date, client: TushareClient) -> PeerOutcome:
    prior = _previous_session(config, day)
    if prior is None:
        return PeerOutcome("skipped_no_calendar", [])
    lake = _curated(config, "adj_factors", "trade_date", prior, day).filter(
        pl.col("adjust_type") == "hfq"
    )
    if lake.is_empty():
        return PeerOutcome("skipped_no_curated", [])

    def step(frame: pl.DataFrame, col: str) -> pl.DataFrame:
        today = frame.filter(pl.col("trade_date") == day).select("symbol", pl.col(col).alias("_1"))
        before = frame.filter(pl.col("trade_date") == prior).select(
            "symbol", pl.col(col).alias("_0")
        )
        return today.join(before, on="symbol").select(
            "symbol", (pl.col("_1") / pl.col("_0")).alias(f"step_{col}")
        )

    peer = pl.concat(
        [
            _symbols(client.query("adj_factor", {"trade_date": _ymd(d)})).with_columns(
                pl.lit(d).alias("trade_date"), pl.col("adj_factor").cast(pl.Float64)
            )
            for d in (prior, day)
        ],
        how="diagonal_relaxed",
    )
    if peer.is_empty():
        return PeerOutcome("unavailable", [])
    joined = step(lake, "factor").join(step(peer, "adj_factor"), on="symbol")
    # Only a step on either side is evidence; flat factors agree trivially.
    joined = joined.filter(
        ((pl.col("step_factor") - 1).abs() > 1e-6) | ((pl.col("step_adj_factor") - 1).abs() > 1e-6)
    )
    findings = _value_finding(
        "adj_factors",
        "tushare_peer_factor_step",
        day,
        joined,
        [("step_factor", "step_adj_factor")],
        rtol=2e-3,
    )
    if findings:
        # Every disagreeing step is a candidate defect, however few.
        findings[0]["severity"] = "warning"
    return PeerOutcome("disagreed" if findings else "agreed", findings)


def _corporate_actions(config: Config, day: date, client: TushareClient) -> PeerOutcome:
    peer = _symbols(
        client.query(
            "dividend",
            {"ex_date": _ymd(day)},
            "ts_code,ex_date,div_proc,cash_div_tax,stk_div",
        )
    )
    if peer.is_empty():
        return PeerOutcome("agreed", [])
    peer = (
        peer.filter(pl.col("div_proc") == "实施")  # i18n-ignore: source value
        .group_by("symbol")
        .agg(
            pl.col("cash_div_tax").cast(pl.Float64).max().fill_null(0).alias("cash_ts"),
            pl.col("stk_div").cast(pl.Float64).max().fill_null(0).alias("shares_ts"),
        )
    )
    lake = (
        _curated(config, "corporate_actions", "ex_date", day, day)
        .group_by("symbol")
        .agg(
            pl.col("cash_dividend").fill_null(0).sum().alias("cash"),
            (pl.col("bonus_ratio").fill_null(0) + pl.col("transfer_ratio").fill_null(0))
            .sum()
            .alias("shares"),
        )
        if dataset_has_parquet(config.curated_root / "corporate_actions")
        else pl.DataFrame(schema={"symbol": pl.Utf8, "cash": pl.Float64, "shares": pl.Float64})
    )
    findings = _value_finding(
        "corporate_actions",
        "tushare_peer_dividend_terms",
        day,
        lake.join(peer, on="symbol"),
        [("cash", "cash_ts"), ("shares", "shares_ts")],
        rtol=1e-3,
    )
    missing = sorted(set(peer["symbol"]) - set(lake["symbol"]))
    findings += _missing_finding(
        "corporate_actions", "tushare_peer_dividend_missing", day, missing, peer.height
    )
    return PeerOutcome("disagreed" if findings else "agreed", findings)


def _trading_status(config: Config, day: date, client: TushareClient) -> PeerOutcome:
    from cnequity.domain.trading_status import STATUS_DELISTED, normalize_legacy

    lake = _curated(config, "trading_status", "trade_date", day, day)
    if lake.is_empty():
        return PeerOutcome("skipped_no_curated", [])
    lake = normalize_legacy(lake).filter(pl.col("status") != STATUS_DELISTED)
    covered = set(lake["symbol"])
    findings = []
    for check, api, params, lake_set in (
        (
            "tushare_peer_suspended",
            "suspend_d",
            {"trade_date": _ymd(day), "suspend_type": "S"},
            set(lake.filter(pl.col("status") == "suspended")["symbol"]),
        ),
        (
            "tushare_peer_st",
            "stock_st",
            {"trade_date": _ymd(day)},
            set(lake.filter(pl.col("risk_warning").fill_null(False))["symbol"]),
        ),
    ):
        peer_set = set(_symbols(client.query(api, params))["symbol"]) & covered
        only_peer, only_lake = sorted(peer_set - lake_set), sorted(lake_set - peer_set)
        total = len(only_peer) + len(only_lake)
        if not total:
            continue
        findings.append(
            {
                "dataset": "trading_status",
                "severity": "warning" if total > _MIN_TOLERATED else "info",
                "check": check,
                "message": (
                    f"trading_status: {len(only_peer)} flagged by Tushare but not the lake, "
                    f"{len(only_lake)} the other way, on {day.isoformat()} ({api})"
                ),
                "trade_date": day.isoformat(),
                "peer_only": only_peer[:_SAMPLE],
                "lake_only": only_lake[:_SAMPLE],
            }
        )
    return PeerOutcome("disagreed" if findings else "agreed", findings)


def _valuation_and_shares(config: Config, day: date, client: TushareClient) -> PeerOutcome:
    peer = _symbols(
        client.query(
            "daily_basic",
            {"trade_date": _ymd(day)},
            "ts_code,total_mv,circ_mv,total_share",
        )
    ).select(
        "symbol",
        (pl.col("total_mv").cast(pl.Float64) * 1e4).alias("total_mv_ts"),
        (pl.col("circ_mv").cast(pl.Float64) * 1e4).alias("float_mv_ts"),
        (pl.col("total_share").cast(pl.Float64) * 1e4).alias("total_shares_ts"),
    )
    if peer.is_empty():
        return PeerOutcome("unavailable", [])
    findings = []
    valuation = _curated(config, "valuation_metrics", "trade_date", day, day)
    if not valuation.is_empty():
        findings += _value_finding(
            "valuation_metrics",
            "tushare_peer_market_value",
            day,
            valuation.join(peer, on="symbol"),
            [("total_mv", "total_mv_ts"), ("float_mv", "float_mv_ts")],
            rtol=1e-2,
        )
    shares = _curated(config, "share_structure", "change_date", date(1990, 1, 1), day)
    if not shares.is_empty():
        latest = (
            shares.filter(pl.col("total_shares") > 0)
            .sort("change_date")
            .group_by("symbol")
            .agg(pl.col("total_shares").last())
        )
        findings += _value_finding(
            "share_structure",
            "tushare_peer_total_shares",
            day,
            latest.join(peer, on="symbol"),
            [("total_shares", "total_shares_ts")],
            rtol=1e-2,
        )
    return PeerOutcome("disagreed" if findings else "agreed", findings)


def _margin_trading(config: Config, day: date, client: TushareClient) -> PeerOutcome:
    # Margin balances publish a session late; judge the lake's newest day.
    lake = _curated(config, "margin_trading", "trade_date", day - timedelta(days=10), day)
    if lake.is_empty():
        return PeerOutcome("skipped_no_curated", [])
    newest = lake["trade_date"].max()
    lake = lake.filter(pl.col("trade_date") == newest)
    peer = _symbols(client.query("margin_detail", {"trade_date": _ymd(newest)}))
    if peer.is_empty():
        return PeerOutcome("unavailable", [])
    # The lake carries stocks only; Tushare also lists ETFs.
    stocks = _stock_symbols(config)
    peer = peer.filter(pl.col("symbol").is_in(list(stocks))).select(
        "symbol",
        *[
            pl.col(src).cast(pl.Float64).alias(dst)
            for src, dst in (
                ("rzye", "margin_balance_ts"),
                ("rzmre", "margin_buy_ts"),
                ("rqye", "short_balance_ts"),
                ("rqmcl", "short_sell_volume_ts"),
            )
        ],
    )
    findings = _value_finding(
        "margin_trading",
        "tushare_peer_margin",
        newest,
        lake.join(peer, on="symbol"),
        [
            ("margin_balance", "margin_balance_ts"),
            ("margin_buy", "margin_buy_ts"),
            ("short_balance", "short_balance_ts"),
            ("short_sell_volume", "short_sell_volume_ts"),
        ],
        rtol=1e-4,
    )
    missing = sorted(set(peer["symbol"]) - set(lake["symbol"]))
    findings += _missing_finding(
        "margin_trading", "tushare_peer_margin_missing", newest, missing, peer.height
    )
    return PeerOutcome("disagreed" if findings else "agreed", findings)


def _fund_flow(config: Config, day: date, client: TushareClient) -> PeerOutcome:
    lake = _curated(config, "fund_flow", "trade_date", day, day)
    if lake.is_empty():
        return PeerOutcome("skipped_no_curated", [])
    # moneyflow_dc is EastMoney's own definition, the same as the lake's.
    peer = _symbols(client.query("moneyflow_dc", {"trade_date": _ymd(day)})).select(
        "symbol", (pl.col("net_amount").cast(pl.Float64) * 1e4).alias("main_net_inflow_ts")
    )
    if peer.is_empty():
        return PeerOutcome("unavailable", [])
    findings = _value_finding(
        "fund_flow",
        "tushare_peer_fund_flow",
        day,
        lake.join(peer, on="symbol"),
        [("main_net_inflow", "main_net_inflow_ts")],
        rtol=1e-2,
        # The peer rounds to 100 CNY; small flows differ by that alone.
        atol=1e4,
    )
    # The peer also lists long-delisted codes with zero flow; only names that
    # traded that day can be owed a row.
    traded = set(
        _curated(config, "daily_bars", "trade_date", day, day).filter(pl.col("volume") > 0)[
            "symbol"
        ]
    )
    owed = set(peer["symbol"]) & traded
    missing = sorted(owed - set(lake["symbol"]))
    findings += _missing_finding(
        "fund_flow", "tushare_peer_fund_flow_missing", day, missing, len(owed)
    )
    return PeerOutcome("disagreed" if findings else "agreed", findings)


def _index_constituents(config: Config, day: date, client: TushareClient) -> PeerOutcome:
    lake = _curated(
        config, "index_constituents", "as_of_date", day.replace(day=1) - timedelta(days=40), day
    )
    if lake.is_empty():
        return PeerOutcome("skipped_no_curated", [])
    findings = []
    for index in sorted(set(lake["index_symbol"])):
        peer = client.query(
            "index_weight",
            {
                "index_code": index,
                "start_date": _ymd(day - timedelta(days=40)),
                "end_date": _ymd(day),
            },
        )
        if peer.is_empty():
            continue
        as_of = peer["trade_date"].max()
        peer_day = date(int(as_of[:4]), int(as_of[4:6]), int(as_of[6:]))
        mine = lake.filter(
            (pl.col("index_symbol") == index) & (pl.col("as_of_date") == pl.lit(peer_day))
        )
        if mine.is_empty():
            continue
        members = set(peer.filter(pl.col("trade_date") == as_of)["con_code"])
        held = set(mine["symbol"])
        only_peer, only_lake = sorted(members - held), sorted(held - members)
        if only_peer or only_lake:
            findings.append(
                {
                    "dataset": "index_constituents",
                    "severity": "warning",
                    "check": "tushare_peer_index_members",
                    "message": (
                        f"index_constituents {index} on {peer_day.isoformat()}: "
                        f"{len(only_peer)} members only in Tushare, {len(only_lake)} only in the lake"
                    ),
                    "trade_date": peer_day.isoformat(),
                    "index_symbol": index,
                    "peer_only": only_peer[:_SAMPLE],
                    "lake_only": only_lake[:_SAMPLE],
                }
            )
    return PeerOutcome("disagreed" if findings else "agreed", findings)


Check = Callable[[Config, date, TushareClient], PeerOutcome]

#: check name → (endpoints it calls, the check). Probed endpoints use the
#: smallest dated query; a denied endpoint skips its check.
CHECKS: dict[str, tuple[tuple[str, ...], Check]] = {
    "daily_bars": (("daily",), _daily_bars),
    "adj_factors": (("adj_factor",), _adj_factor_steps),
    "corporate_actions": (("dividend",), _corporate_actions),
    "trading_status": (("suspend_d", "stock_st"), _trading_status),
    "valuation_and_shares": (("daily_basic",), _valuation_and_shares),
    "margin_trading": (("margin_detail",), _margin_trading),
    "fund_flow": (("moneyflow_dc",), _fund_flow),
    "index_constituents": (("index_weight",), _index_constituents),
}


def _probe_params(api: str, day: date) -> dict:
    if api == "index_weight":
        return {"index_code": "000300.SH", "start_date": _ymd(day - timedelta(days=40))}
    if api == "dividend":
        return {"ex_date": _ymd(day)}
    return {"trade_date": _ymd(day)}


def run_tushare_peer_checks(
    config: Config, trade_date: date, *, client: TushareClient | None = None
) -> list[dict]:
    """Run every peer comparison for *trade_date* and persist the outcome."""
    if not tushare_enabled(config):
        return []
    own = client is None
    findings: list[dict] = []
    ran: dict[str, str] = {}
    try:
        client = client or TushareClient(config)
        apis = sorted({api for needed, _ in CHECKS.values() for api in needed})
        try:
            allowed = probe_permissions(
                config, {api: _probe_params(api, trade_date) for api in apis}, client=client
            )
        except SourceCoolingDown as exc:
            logger.warning("tushare peer checks skipped: %s", exc)
            allowed = {}
        for name, (needed, check) in CHECKS.items():
            if any(allowed.get(api) != "ok" for api in needed):
                ran[name] = "skipped_permission"
                continue
            try:
                outcome = check(config, trade_date, client)
            except SourceCoolingDown as exc:
                ran[name] = f"cooling_down: {exc}"
                break
            except Exception as exc:
                # A peer outage must not fail a data run.
                logger.warning("tushare peer check %s failed: %s", name, exc)
                ran[name] = f"error: {exc}"
                continue
            ran[name] = outcome.status
            findings.extend(outcome.findings)
    finally:
        if own and client is not None:
            client.close()
    out_dir = config.meta_root / "quality" / "source_diffs"
    try:
        out_dir.mkdir(parents=True, exist_ok=True)
        write_json_atomic(
            out_dir / f"tushare-peer-{trade_date.isoformat()}.json",
            {
                "trade_date": trade_date.isoformat(),
                "kind": "tushare_peer",
                "checks": ran,
                "findings": findings,
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    except OSError as exc:
        logger.warning("could not persist tushare peer checks: %s", exc)
    return findings
