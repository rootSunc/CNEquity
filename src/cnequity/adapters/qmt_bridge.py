"""QMT bridge bar adapter."""

from __future__ import annotations

import logging
import math
import sys
import threading
from datetime import date, datetime, timedelta, timezone
from datetime import time as datetime_time
from pathlib import Path
from typing import Any

import polars as pl

from cnequity.domain.schemas import data_version_for, with_provenance

logger = logging.getLogger(__name__)

SOURCE = "qmt_bridge"
_XTDATA_LOCK = threading.Lock()
_XTDATA_BY_KEY: dict[tuple[str | None, float | None], Any] = {}


class QmtBridgeSourceError(RuntimeError):
    """Raised when the local BigQMT bridge cannot deliver bars."""


QMT_BAR_DATASETS = frozenset({"daily_bars", "index_bars", "minute_bars", "minute_bars_5m"})
_CORPORATE_ACTIONS_SCHEMA = {
    "symbol": pl.Utf8,
    "ex_date": pl.Date,
    "action_type": pl.Utf8,
    "cash_dividend": pl.Float64,
    "bonus_ratio": pl.Float64,
    "transfer_ratio": pl.Float64,
    "split_factor": pl.Float64,
    "allotment_ratio": pl.Float64,
    "allotment_price": pl.Float64,
}
_FINANCIAL_PK = [
    "symbol",
    "report_period",
    "statement_type",
    "item_code",
    "announce_date",
]
_HOLDERNUM_ANNOUNCEMENT_LAG_DAYS = 500
_SHANGHAI_OFFSET = timedelta(hours=8)
_QMT_FINANCIAL_TABLES = {
    "ASHAREBALANCESHEET": (
        "balance",
        {
            "cash_equivalents": "monetary_funds",
            "account_receivable": "accounts_receivable",
            "inventories": "inventory",
            "fix_assets": "fixed_assets",
            "goodwill": "goodwill",
            "total_current_assets": "total_current_assets",
            "total_non_current_assets": "total_non_current_assets",
            "tot_assets": "total_assets",
            "shortterm_loan": "short_term_loans",
            "long_term_loans": "long_term_loans",
            "total_current_liability": "total_current_liabilities",
            "non_current_liabilities": "total_non_current_liabilities",
            "tot_liab": "total_liabilities",
            "cap_stk": "share_capital",
            "undistributed_profit": "retained_earnings",
            "tot_shrhldr_eqy_excl_min_int": "equity_excl_minority",
            "minority_int": "minority_interest",
            "total_equity": "total_equity",
        },
    ),
    "ASHAREINCOME": (
        "income",
        {
            "revenue": "revenue",
            "revenue_inc": "operating_revenue",
            "total_operating_cost": "total_operating_cost",
            "total_expense": "operating_cost",
            "less_taxes_surcharges_ops": "taxes_surcharges",
            "sale_expense": "sale_expense",
            "less_gerl_admin_exp": "manage_expense",
            "financial_expense": "finance_expense",
            "less_impair_loss_assets": "impairment_loss",
            "plus_net_invest_inc": "investment_income",
            "change_income_fair_value": "fair_value_change",
            "oper_profit": "operating_profit",
            "tot_profit": "total_profit",
            "inc_tax": "income_tax",
            "net_profit_incl_min_int_inc": "net_profit_incl_minority",
            "net_profit_excl_min_int_inc": "net_profit",
            "total_income": "comprehensive_income",
        },
    ),
    "ASHARECASHFLOW": (
        "cashflow",
        {
            "goods_sale_and_service_render_cash": "cash_from_sales",
            "stot_cash_inflows_oper_act": "cash_inflow_operate",
            "stot_cash_outflows_oper_act": "cash_outflow_operate",
            "net_cash_flows_oper_act": "net_cash_operate",
            "stot_cash_inflows_inv_act": "cash_inflow_invest",
            "stot_cash_outflows_inv_act": "cash_outflow_invest",
            "net_cash_flows_inv_act": "net_cash_invest",
            "cash_pay_acq_const_fiolta": "capex",
            "stot_cash_inflows_fnc_act": "cash_inflow_finance",
            "stot_cash_outflows_fnc_act": "cash_outflow_finance",
            "net_cash_flows_fnc_act": "net_cash_finance",
            "net_incr_cash_cash_equ": "net_cash_increase",
        },
    ),
    "PERSHAREINDEX": (
        "indicator",
        {
            "s_fa_ocfps": "ocf_per_share",
            "s_fa_bps": "bps",
            "s_fa_eps_basic": "eps",
            "s_fa_eps_diluted": "eps_diluted",
            "adjusted_earnings_per_share": "eps_deducted",
            "equity_roe": "roe",
            "net_roe": "roe_diluted",
            "total_roe": "roe_total_assets",
            "sales_gross_profit": "gross_margin",
            "inc_revenue_rate": "revenue_yoy",
            "du_profit_rate": "net_profit_yoy",
            "inc_net_profit_rate": "parent_net_profit_yoy",
            "adjusted_net_profit_rate": "net_profit_deducted_yoy",
            "adjusted_net_profit": "net_profit_deducted",
            "gear_ratio": "debt_to_asset_ratio",
            "inventory_turnover": "inventory_turnover",
        },
    ),
}
_QMT_PERIODS = {
    "daily_bars": "1d",
    "index_bars": "1d",
    "minute_bars": "1m",
    "minute_bars_5m": "5m",
}
_QMT_FREQUENCIES = {
    "index_bars": "1d",
    "minute_bars": "1m",
    "minute_bars_5m": "5m",
}
_QMT_INSTRUMENT_SECTORS = {
    "沪深A股": "stock",
    "沪深ETF": "etf",
}
_QMT_INDEX_SECTORS = {
    "上证50": "000016.SH",
    "沪深300": "000300.SH",
    "中证500": "000905.SH",
    "中证800": "000906.SH",
    "中证1000": "000852.SH",
    "创业板指": "399006.SZ",
    "创业板50": "399673.SZ",
    "科创50": "000688.SH",
    "科创100": "000698.SH",
    "上证180": "000010.SH",
    "上证380": "000009.SH",
    "上证100": "000132.SH",
    "中证全指": "000985.CSI",
}
_INSTRUMENTS_OUTPUT_SCHEMA = {
    "symbol": pl.Utf8,
    "name": pl.Utf8,
    "exchange": pl.Utf8,
    "asset_type": pl.Utf8,
    "list_date": pl.Date,
    "delist_date": pl.Date,
    "prev_symbol": pl.Utf8,
}


def _date_string(value: date) -> str:
    return value.strftime("%Y%m%d")


def _coerce_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    digits = "".join(char for char in str(value)[:14] if char.isdigit())
    if len(digits) < 8:
        return None
    try:
        return date.fromisoformat(f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}")
    except ValueError:
        return None


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        return datetime.combine(value, datetime_time())
    else:
        raw = str(value)
        if raw.endswith("Z"):
            raw = raw[:-1]
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            parsed = None
        if parsed is not None:
            if parsed.tzinfo is not None:
                parsed = parsed.replace(tzinfo=None)
            return parsed
        digits = "".join(char for char in raw[:14] if char.isdigit())
        try:
            if len(digits) == 8:
                parsed = datetime.strptime(digits, "%Y%m%d")
            elif len(digits) >= 12:
                parsed = datetime.strptime(digits[:14], "%Y%m%d%H%M%S")
            elif len(digits) == 10:
                parsed = datetime.fromtimestamp(float(value))
            else:
                return None
        except (ValueError, OSError, OverflowError):
            return None
    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


def _frame_index(frame: Any) -> list[Any]:
    try:
        return list(frame.index)
    except Exception:
        return []


def _values(frame: Any, column: str) -> list[Any] | None:
    try:
        if column not in set(map(str, frame.columns)):
            return None
        return list(frame[column].tolist())
    except Exception:
        return None


def _float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _volume_shares(value: float | None, unit: str, volume_in_lots: bool) -> int | None:
    if value is None or value < 0:
        return None
    shares = value * 100.0 if volume_in_lots else value
    if shares > 2**63 - 1:
        return None
    return int(round(shares))


def _infer_volume_in_lots(frames: list[Any]) -> bool:
    ratios: list[float] = []
    for frame in frames:
        closes = _values(frame, "close") or []
        volumes = _values(frame, "volume") or []
        amounts = _values(frame, "amount") or []
        for close, volume, amount in zip(closes, volumes, amounts, strict=False):
            close_value = _float(close)
            volume_value = _float(volume)
            amount_value = _float(amount)
            if not close_value or not volume_value or not amount_value:
                continue
            ratio = amount_value / (close_value * volume_value)
            if math.isfinite(ratio) and ratio > 0:
                ratios.append(ratio)
    if not ratios:
        return False
    ratios.sort()
    return ratios[len(ratios) // 2] > 10.0


def _frame_rows(
    symbol: str,
    frame: Any,
    start: date,
    end: date,
    *,
    dataset: str,
) -> list[dict[str, Any]]:
    if frame is None:
        return []
    try:
        indexes = list(frame.index)
    except Exception:
        return []

    opens = _values(frame, "open")
    highs = _values(frame, "high")
    lows = _values(frame, "low")
    closes = _values(frame, "close")
    volumes = _values(frame, "volume")
    amounts = _values(frame, "amount")

    rows: list[dict[str, Any]] = []
    for position, index_value in enumerate(indexes):
        minute_index = _coerce_datetime(index_value)
        trade_date = minute_index.date() if minute_index else None
        if trade_date is None or trade_date < start or trade_date > end:
            continue

        def _at(values: list[Any] | None, position: int) -> Any:
            if values is None or position >= len(values):
                return None
            return values[position]

        open_value = _float(_at(opens, position))
        high_value = _float(_at(highs, position))
        low_value = _float(_at(lows, position))
        close_value = _float(_at(closes, position))
        if not all(
            value and value > 0 for value in (open_value, high_value, low_value, close_value)
        ):
            continue
        volume_value = _float(_at(volumes, position))
        if volume_value is None or volume_value <= 0:
            continue
        row = {
            "symbol": symbol,
            "trade_date": trade_date,
            "open": open_value,
            "high": high_value,
            "low": low_value,
            "close": close_value,
            "volume": volume_value,
            "amount": _float(_at(amounts, position)) or 0.0,
        }
        if dataset == "index_bars":
            row["frequency"] = "1d"
        elif dataset in {"minute_bars", "minute_bars_5m"}:
            row["frequency"] = _QMT_FREQUENCIES[dataset]
            row["bar_time"] = minute_index
        rows.append(row)
    return rows


def _prepare_import_paths(config: Any) -> None:
    for field in ("qmt_bridge_src_path", "qmt_bridge_client_config_path"):
        raw_path = getattr(config, field, None)
        if not raw_path:
            continue
        path = Path(str(raw_path)).expanduser().resolve()
        if path.is_file():
            path = path.parent
        if not path.is_dir() or str(path) in sys.path:
            continue
        if field == "qmt_bridge_src_path":
            sys.path.insert(0, str(path))
        else:
            sys.path.append(str(path))


def _qmt_bridge_client(config: Any, xtdata: Any | None) -> Any:
    if xtdata is not None:
        return xtdata
    if config is None:
        raise QmtBridgeSourceError("QMT bridge fetch requires config or an xtdata client")
    return _xtdata(config)


def _qmt_timestamp_date(value: Any) -> date | None:
    """Decode QMT financial timestamps without shifting a Beijing day."""
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        return value
    else:
        raw = str(value).strip()
        if raw.lower() in {"", "nan", "nat", "none"}:
            return None
        try:
            number = float(raw)
        except (TypeError, ValueError):
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                return None
        else:
            seconds = number / 1000.0 if number > 1e11 else number
            parsed = datetime(1970, 1, 1) + timedelta(seconds=seconds + 8 * 3600)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone(_SHANGHAI_OFFSET))
    return parsed.date()


def _report_period(value: Any) -> str | None:
    report_date = _qmt_timestamp_date(value)
    if report_date is None:
        return None
    quarters = {(3, 31): "Q1", (6, 30): "Q2", (9, 30): "Q3", (12, 31): "Q4"}
    quarter = quarters.get((report_date.month, report_date.day))
    return f"{report_date.year}{quarter}" if quarter else None


def _financial_records(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    if isinstance(frame, dict):
        records: list[dict[str, Any]] = []
        for value in frame.values():
            records.extend(_financial_records(value))
        return records
    if isinstance(frame, list):
        return [value for value in frame if isinstance(value, dict)]
    if hasattr(frame, "to_dict"):
        try:
            values = frame.to_dict("records")
        except TypeError:
            values = frame.to_dict()
        if isinstance(values, list):
            return [value for value in values if isinstance(value, dict)]
    return []


def _financial_rows(
    symbol: str,
    table: str,
    frame: Any,
    start: date,
    end: date,
) -> list[dict[str, Any]]:
    statement_type, item_map = _QMT_FINANCIAL_TABLES[table]
    rows: list[dict[str, Any]] = []
    for record in _financial_records(frame):
        report_period = _report_period(record.get("m_timetag"))
        announce_date = _qmt_timestamp_date(record.get("m_anntime"))
        if report_period is None or announce_date is None:
            continue
        if announce_date < start or announce_date > end:
            continue
        for qmt_field, item_code in item_map.items():
            item_value = _float(record.get(qmt_field))
            if item_value is None:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "report_period": report_period,
                    "statement_type": statement_type,
                    "item_code": item_code,
                    "item_value": item_value,
                    "announce_date": announce_date,
                }
            )
    return rows


def fetch_financial_statement_items_qmt(
    symbols: list[str],
    start: date,
    end: date,
    *,
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Fetch statement items using QMT's first-disclosure dates.

    QMT only returns metadata rows for financial data that has actually been
    downloaded in the terminal. A metadata-free response is parsed as empty
    rather than being backfilled from ``m_timetag``, which is a report period
    and would introduce a look-ahead if treated as an announcement date.
    """
    client = _qmt_bridge_client(config, xtdata)
    if not symbols:
        return pl.DataFrame()

    rows: list[dict[str, Any]] = []
    failed = 0
    for symbol in symbols:
        for table in _QMT_FINANCIAL_TABLES:
            try:
                frame = client.get_financial_data(
                    [symbol],
                    [table],
                    start.strftime("%Y%m%d"),
                    end.strftime("%Y%m%d"),
                    report_type="announce_time",
                )
                rows.extend(_financial_rows(symbol, table, frame, start, end))
                if metrics is not None:
                    metrics["requests"] = int(metrics.get("requests", 0)) + 1
            except Exception:
                failed += 1
                if metrics is not None:
                    metrics["failed_requests"] = int(metrics.get("failed_requests", 0)) + 1
                logger.warning(
                    "QMT bridge financial data failed for %s/%s", symbol, table, exc_info=True
                )

    if failed == len(symbols) * len(_QMT_FINANCIAL_TABLES) and symbols:
        raise QmtBridgeSourceError("QMT bridge returned no financial-data responses")
    if not rows:
        return pl.DataFrame()

    frame = pl.DataFrame(rows).unique(subset=_FINANCIAL_PK, keep="last")
    if metrics is not None:
        metrics["rows_read"] = int(metrics.get("rows_read", 0)) + frame.height
    return with_provenance(
        frame.sort(["announce_date", "symbol", "statement_type", "item_code"]),
        source=SOURCE,
        data_version=data_version_for("financial_statement_items"),
    )


def _xtdata(config: Any) -> Any:
    _prepare_import_paths(config)
    try:
        from bigqmt_signal_trader.xtquant_compat import configure
    except Exception as exc:
        raise QmtBridgeSourceError(
            "BigQMT bridge package unavailable; set [qmt_bridge].bridge_src_path "
            "or install xtquant_big_convert"
        ) from exc

    timeout = getattr(config, "qmt_bridge_timeout_seconds", None)
    account_id = getattr(config, "qmt_bridge_account_id", None)
    key = (account_id, float(timeout) if timeout is not None else None)
    with _XTDATA_LOCK:
        client = _XTDATA_BY_KEY.get(key)
        if client is None:
            _, client = configure(
                account_id=account_id,
                redis_client=None,
                redis_config=None,
                timeout_seconds=timeout,
            )
            _XTDATA_BY_KEY[key] = client
    return client


def fetch_bars_qmt(
    symbols: list[str],
    start: date,
    end: date,
    *,
    dataset: str = "daily_bars",
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Download, then parse bars for *symbols* through the BigQMT bridge."""
    if dataset not in QMT_BAR_DATASETS:
        raise QmtBridgeSourceError(f"QMT bridge does not support dataset {dataset!r}")
    period = _QMT_PERIODS[dataset]
    if not symbols:
        return pl.DataFrame()
    if xtdata is None:
        if config is None:
            raise QmtBridgeSourceError("QMT bridge fetch requires config or an xtdata client")
        xtdata = _xtdata(config)

    chunk_size = int(getattr(config, "qmt_bridge_chunk_size", 0) or 0)
    download_timeout = getattr(config, "qmt_bridge_download_timeout_seconds", 180.0)
    data_wait = getattr(config, "qmt_bridge_data_wait_seconds", 10.0)
    volume_unit = str(getattr(config, "qmt_bridge_volume_unit", "auto")).lower()
    symbol_chunks = (
        [symbols]
        if chunk_size <= 0
        else [
            symbols[offset : offset + chunk_size] for offset in range(0, len(symbols), chunk_size)
        ]
    )

    frames: list[tuple[str, Any]] = []
    failed_chunks = 0
    for chunk in symbol_chunks:
        try:
            xtdata.download_history_data2(
                chunk,
                period,
                start_time=_date_string(start),
                end_time=_date_string(end),
                dividend_type="none",
                download_timeout_seconds=download_timeout,
                data_wait_seconds=data_wait,
            )
            local_data = xtdata.get_local_data(
                field_list=[],
                stock_list=chunk,
                period=period,
                start_time=_date_string(start),
                end_time=_date_string(end),
                count=-1,
                dividend_type="none",
                fill_data=False,
            )
            frames.extend((str(symbol), frame) for symbol, frame in (local_data or {}).items())
            if metrics is not None:
                metrics["requests"] = int(metrics.get("requests", 0)) + 1
        except Exception:
            failed_chunks += 1
            if metrics is not None:
                metrics["failed_requests"] = int(metrics.get("failed_requests", 0)) + 1
            logger.warning("QMT bridge %s chunk failed", dataset, exc_info=True)

    if not frames:
        raise QmtBridgeSourceError(f"QMT bridge returned no local {dataset} frames")
    if metrics is not None:
        metrics["fallback_requests"] = int(metrics.get("fallback_requests", 0)) + failed_chunks

    volume_in_lots = (
        _infer_volume_in_lots([frame for _, frame in frames])
        if volume_unit == "auto"
        else volume_unit == "lot"
    )
    if dataset == "index_bars":
        volume_in_lots = False
    rows: list[dict[str, Any]] = []
    for symbol, frame in frames:
        parsed = _frame_rows(symbol, frame, start, end, dataset=dataset)
        for row in parsed:
            row["volume"] = _volume_shares(row["volume"], volume_unit, volume_in_lots)
            if row["volume"] is None:
                continue
            rows.append(row)

    if not rows:
        return pl.DataFrame()
    primary_key = (
        ["symbol", "trade_date"]
        if dataset == "daily_bars"
        else ["symbol", "trade_date", "frequency"]
    )
    if dataset in {"minute_bars", "minute_bars_5m"}:
        primary_key = ["symbol", "trade_date", "bar_time", "frequency"]
    frame = pl.DataFrame(rows).unique(subset=primary_key, keep="last")
    if metrics is not None:
        metrics["rows_read"] = int(metrics.get("rows_read", 0)) + frame.height
        metrics["bytes_read"] = int(metrics.get("bytes_read", 0)) + frame.estimated_size()
    sort_columns = ["trade_date", "symbol"]
    if dataset in {"minute_bars", "minute_bars_5m"}:
        sort_columns.append("bar_time")
    return with_provenance(
        frame.sort(sort_columns),
        source=SOURCE,
        data_version=data_version_for(dataset),
    )


def fetch_daily_bars_qmt(
    symbols: list[str],
    start: date,
    end: date,
    *,
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Backwards-compatible daily-bars entry point."""
    return fetch_bars_qmt(
        symbols,
        start,
        end,
        dataset="daily_bars",
        config=config,
        xtdata=xtdata,
        metrics=metrics,
    )


def fetch_index_bars_qmt(
    symbols: list[str],
    start: date,
    end: date,
    *,
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Fetch index daily bars through the BigQMT bridge."""
    return fetch_bars_qmt(
        symbols,
        start,
        end,
        dataset="index_bars",
        config=config,
        xtdata=xtdata,
        metrics=metrics,
    )


def fetch_minute_bars_qmt(
    symbols: list[str],
    start: date,
    end: date,
    *,
    frequency: str,
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Fetch 1m or 5m bars through the BigQMT bridge."""
    dataset = {"1m": "minute_bars", "5m": "minute_bars_5m"}.get(frequency)
    if dataset is None:
        raise QmtBridgeSourceError(f"QMT bridge does not support frequency {frequency!r}")
    return fetch_bars_qmt(
        symbols,
        start,
        end,
        dataset=dataset,
        config=config,
        xtdata=xtdata,
        metrics=metrics,
    )


def _corporate_action_rows(symbol: str, frame: Any, start: date, end: date) -> list[dict[str, Any]]:
    if frame is None:
        return []
    indexes = _frame_index(frame)
    columns = {str(column) for column in getattr(frame, "columns", [])}

    def _column(name: str) -> list[Any]:
        if name not in columns:
            return []
        try:
            return list(frame[name].tolist())
        except Exception:
            return []

    interests = _column("interest")
    bonuses = _column("stockBonus")
    transfers = _column("stockGift")
    allotments = _column("allotNum")
    allot_prices = _column("allotPrice")
    rows: list[dict[str, Any]] = []
    for position, index_value in enumerate(indexes):
        ex_date = _coerce_date(index_value)
        if ex_date is None or ex_date < start or ex_date > end:
            continue

        def _value(values: list[Any], position: int, *, default: float = 0.0) -> float:
            if position >= len(values):
                return default
            parsed = _float(values[position])
            return parsed if parsed is not None and parsed > 0 else 0.0

        common = {
            "symbol": symbol,
            "ex_date": ex_date,
            "cash_dividend": 0.0,
            "bonus_ratio": 0.0,
            "transfer_ratio": 0.0,
            "split_factor": None,
            "allotment_ratio": None,
            "allotment_price": None,
        }
        cash_dividend = _value(interests, position)
        bonus_ratio = _value(bonuses, position)
        transfer_ratio = _value(transfers, position)
        allotment_ratio = _value(allotments, position)
        allotment_price = _value(allot_prices, position)
        if cash_dividend:
            rows.append({**common, "action_type": "cash_dividend", "cash_dividend": cash_dividend})
        if bonus_ratio:
            rows.append({**common, "action_type": "bonus", "bonus_ratio": bonus_ratio})
        if transfer_ratio:
            rows.append({**common, "action_type": "transfer", "transfer_ratio": transfer_ratio})
        if allotment_ratio:
            rows.append(
                {
                    **common,
                    "action_type": "allotment",
                    "allotment_ratio": allotment_ratio,
                    "allotment_price": allotment_price or None,
                }
            )
    return rows


def fetch_corporate_actions_qmt(
    symbols: list[str],
    start: date,
    end: date,
    *,
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Fetch the full QMT dividend/allotment history and filter it to *window*."""
    client = _qmt_bridge_client(config, xtdata)
    if not symbols:
        return pl.DataFrame(schema=_CORPORATE_ACTIONS_SCHEMA)

    rows: list[dict[str, Any]] = []
    failed = 0
    for symbol in symbols:
        try:
            # QMT's range form scans daily bars and silently degrades to an
            # empty answer; the no-date form returns the complete event history.
            frame = client.get_divid_factors(symbol)
            rows.extend(_corporate_action_rows(symbol, frame, start, end))
            if metrics is not None:
                metrics["requests"] = int(metrics.get("requests", 0)) + 1
        except Exception:
            failed += 1
            if metrics is not None:
                metrics["failed_requests"] = int(metrics.get("failed_requests", 0)) + 1
            logger.warning("QMT bridge corporate actions failed for %s", symbol, exc_info=True)

    if failed == len(symbols) and symbols:
        raise QmtBridgeSourceError("QMT bridge returned no corporate-action responses")
    if not rows:
        return pl.DataFrame(schema=_CORPORATE_ACTIONS_SCHEMA)
    frame = pl.DataFrame(rows, schema=_CORPORATE_ACTIONS_SCHEMA).unique(
        subset=["symbol", "ex_date", "action_type"],
        keep="last",
    )
    if metrics is not None:
        metrics["rows_read"] = int(metrics.get("rows_read", 0)) + frame.height
    return with_provenance(
        frame.sort(["ex_date", "symbol", "action_type"]),
        source=SOURCE,
        data_version=data_version_for("corporate_actions"),
    )


def fetch_trading_calendar_qmt(
    start: date,
    end: date,
    *,
    market: str = "SH",
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Fetch QMT sessions and materialise every calendar day in the window."""
    client = _qmt_bridge_client(config, xtdata)
    if start > end:
        return pl.DataFrame()
    try:
        sessions = client.get_trading_dates(
            market,
            start.strftime("%Y%m%d"),
            end.strftime("%Y%m%d"),
        )
    except Exception as exc:
        raise QmtBridgeSourceError(f"QMT bridge trading calendar failed: {exc}") from exc

    trading = set()
    for value in sessions or []:
        parsed = _coerce_date(value)
        if parsed is not None:
            trading.add(parsed)
    if metrics is not None:
        metrics["requests"] = int(metrics.get("requests", 0)) + 1
        metrics["rows_read"] = int(metrics.get("rows_read", 0)) + len(trading)
    rows = [
        {"trade_date": current, "is_trading": current in trading}
        for ordinal in range(start.toordinal(), end.toordinal() + 1)
        for current in [date.fromordinal(ordinal)]
    ]
    frame = pl.DataFrame(rows)
    return with_provenance(
        frame,
        source=SOURCE,
        data_version=data_version_for("trading_calendar"),
    )


def _instrument_detail_date(value: Any) -> date | None:
    """Parse QMT OpenDate/ExpireDate (int YYYYMMDD or 0/99999999)."""
    if value is None:
        return None
    raw = str(value).strip()
    if raw in {"0", "99999999", ""}:
        return None
    digits = "".join(char for char in raw[:8] if char.isdigit())
    if len(digits) != 8:
        return None
    year = int(digits[:4])
    if year < 1900:
        return None
    try:
        return date(year, int(digits[4:6]), int(digits[6:8]))
    except ValueError:
        return None


def fetch_instruments_qmt(
    *,
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Fetch the live SH/SZ instrument list through the BigQMT bridge.

    Returns stocks and ETFs with names and listing dates from the terminal's
    local instrument store. Beijing Exchange symbols are not available through
    this bridge (the QMT sector for 北证A股 returns empty), so the caller must
    still run the BSE merge for full-market coverage.
    """
    client = _qmt_bridge_client(config, xtdata)
    rows: list[dict[str, Any]] = []
    failed_sectors: list[str] = []

    for sector_name, asset_type in _QMT_INSTRUMENT_SECTORS.items():
        try:
            symbols = client.get_stock_list_in_sector(sector_name)
            if metrics is not None:
                metrics["requests"] = int(metrics.get("requests", 0)) + 1
        except Exception:
            failed_sectors.append(sector_name)
            if metrics is not None:
                metrics["failed_requests"] = int(metrics.get("failed_requests", 0)) + 1
            logger.warning("QMT bridge instrument sector %s failed", sector_name, exc_info=True)
            continue

        for symbol in symbols:
            normalized = str(symbol).strip()
            if not normalized or "." not in normalized:
                continue
            exchange = normalized.rsplit(".", 1)[-1].upper()
            name: str | None = None
            list_date: date | None = None
            delist_date: date | None = None
            try:
                detail = client.get_instrument_detail(normalized)
                if metrics is not None:
                    metrics["requests"] = int(metrics.get("requests", 0)) + 1
                if detail:
                    raw_name = detail.get("InstrumentName")
                    if raw_name:
                        name = str(raw_name).strip() or None
                    list_date = _instrument_detail_date(detail.get("OpenDate"))
                    delist_date = _instrument_detail_date(detail.get("ExpireDate"))
            except Exception:
                # Symbol is still valid from the sector list even if the
                # per-symbol detail lookup fails (e.g. newly listed).
                if metrics is not None:
                    metrics["failed_requests"] = int(metrics.get("failed_requests", 0)) + 1
            rows.append(
                {
                    "symbol": normalized,
                    "name": name,
                    "exchange": exchange,
                    "asset_type": asset_type,
                    "list_date": list_date,
                    "delist_date": delist_date,
                    "prev_symbol": None,
                }
            )

    if failed_sectors and len(failed_sectors) == len(_QMT_INSTRUMENT_SECTORS):
        raise QmtBridgeSourceError("QMT bridge returned no instrument-sector responses")
    if not rows:
        return pl.DataFrame()

    frame = pl.DataFrame(rows, schema=_INSTRUMENTS_OUTPUT_SCHEMA).unique(
        subset=["symbol"], keep="last"
    )
    if metrics is not None:
        metrics["rows_read"] = int(metrics.get("rows_read", 0)) + frame.height
    return with_provenance(
        frame.sort("symbol"),
        source=SOURCE,
        data_version=data_version_for("instruments"),
    )


def fetch_index_constituents_qmt(
    *,
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Fetch current index membership from QMT's built-in sector tags.

    QMT's ``get_stock_list_in_sector()`` covers the major broad-market indices
    as named sectors. Weight is not available through this API, so the output
    carries null weight — sufficient for membership screening, but not for
    weight-based rebalancing.
    """
    client = _qmt_bridge_client(config, xtdata)
    as_of = date.today()
    rows: list[dict[str, Any]] = []
    failed: list[str] = []

    for sector_name, index_symbol in _QMT_INDEX_SECTORS.items():
        try:
            members = client.get_stock_list_in_sector(sector_name)
            if metrics is not None:
                metrics["requests"] = int(metrics.get("requests", 0)) + 1
        except Exception:
            failed.append(sector_name)
            if metrics is not None:
                metrics["failed_requests"] = int(metrics.get("failed_requests", 0)) + 1
            logger.warning("QMT bridge index sector %s failed", sector_name, exc_info=True)
            continue
        for symbol in members:
            normalized = str(symbol).strip()
            if not normalized or "." not in normalized:
                continue
            rows.append(
                {
                    "index_symbol": index_symbol,
                    "symbol": normalized,
                    "as_of_date": as_of,
                    "weight": None,
                }
            )

    if failed and len(failed) == len(_QMT_INDEX_SECTORS):
        raise QmtBridgeSourceError("QMT bridge returned no index-sector responses")
    if not rows:
        return pl.DataFrame()

    frame = pl.DataFrame(rows).unique(subset=["index_symbol", "symbol"], keep="last")
    if metrics is not None:
        metrics["rows_read"] = int(metrics.get("rows_read", 0)) + frame.height
    return with_provenance(
        frame.sort(["index_symbol", "symbol"]),
        source=SOURCE,
        data_version=data_version_for("index_constituents"),
    )


_TRADE_TICKS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "tick_seq": pl.Int32,
    "trade_time": pl.Datetime(time_unit="us"),
    "price": pl.Float64,
    "volume": pl.Int64,
    "direction": pl.Utf8,
}


def fetch_trade_ticks_qmt(
    symbols: list[str],
    start: date,
    end: date,
    *,
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Fetch per-symbol tick snapshots through the BigQMT bridge.

    QMT's tick period returns 3-second snapshot frames with cumulative volume.
    This adapter diffs consecutive rows to derive per-tick volume and assigns
    ``direction="neutral"`` (QMT does not carry TDX's tick-rule inference).
    The user must download tick data in the QMT terminal first — local absence
    yields an empty DataFrame, not an error.
    """
    client = _qmt_bridge_client(config, xtdata)
    rows: list[dict[str, Any]] = []
    failed_symbols: list[str] = []

    for symbol in symbols:
        try:
            result = client.get_market_data_ex(
                [],
                [symbol],
                period="tick",
                start_time=start.strftime("%Y%m%d"),
                end_time=end.strftime("%Y%m%d"),
                count=-1,
            )
            if metrics is not None:
                metrics["requests"] = int(metrics.get("requests", 0)) + 1
        except Exception:
            failed_symbols.append(symbol)
            if metrics is not None:
                metrics["failed_requests"] = int(metrics.get("failed_requests", 0)) + 1
            logger.warning("QMT bridge tick data failed for %s", symbol, exc_info=True)
            continue

        frame = result.get(symbol) if isinstance(result, dict) else result
        if frame is None:
            continue
        try:
            records = frame.to_dict("records")
        except Exception:
            continue
        if not records:
            continue

        prev_cum_volume = 0.0
        tick_seq = 0
        for record in records:
            timestamp_ms = record.get("time")
            trade_datetime = _qmt_timestamp_datetime_ms(timestamp_ms)
            if trade_datetime is None:
                continue
            trade_date = trade_datetime.date()
            if trade_date < start or trade_date > end:
                continue
            price = _float(record.get("lastPrice"))
            if price is None or price <= 0:
                prev_cum_volume = _float(record.get("volume")) or 0.0
                continue
            cum_volume_lots = _float(record.get("volume")) or 0.0
            tick_volume = max(0, int(round(cum_volume_lots - prev_cum_volume)) * 100)
            prev_cum_volume = cum_volume_lots
            if tick_volume == 0:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "tick_seq": tick_seq,
                    "trade_time": trade_datetime,
                    "price": price,
                    "volume": tick_volume,
                    "direction": "neutral",
                }
            )
            tick_seq += 1

    if failed_symbols and len(failed_symbols) == len(symbols) and symbols:
        raise QmtBridgeSourceError("QMT bridge returned no tick-data responses")
    if not rows:
        return pl.DataFrame()

    frame = pl.DataFrame(rows, schema=_TRADE_TICKS_SCHEMA).unique(
        subset=["symbol", "trade_date", "tick_seq"], keep="last"
    )
    if metrics is not None:
        metrics["rows_read"] = int(metrics.get("rows_read", 0)) + frame.height
    return with_provenance(
        frame.sort(["symbol", "trade_date", "tick_seq"]),
        source=SOURCE,
        data_version=data_version_for("trade_ticks"),
    )


def _qmt_timestamp_datetime_ms(value: Any) -> datetime | None:
    """Decode a millisecond-epoch timestamp (QMT tick ``time`` column)."""
    if value is None:
        return None
    try:
        seconds = float(value) / 1000.0
        return datetime(1970, 1, 1) + timedelta(seconds=seconds + 8 * 3600)
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _coerce_lhb_nested_trader_frame(value: Any) -> list[dict[str, Any]]:
    """Decode QMT's nested buyTraderBooth / sellTraderBooth structures."""
    if isinstance(value, dict):
        records = value.get("records") or []
        return [row for row in records if isinstance(row, dict)]
    if isinstance(value, list):
        return [row for row in value if isinstance(row, dict)]
    if hasattr(value, "to_dict"):
        try:
            return [row for row in value.to_dict("records") if isinstance(row, dict)]
        except Exception:
            return []
    return []


def fetch_dragon_tiger_qmt(
    symbols: list[str],
    start: date,
    end: date,
    *,
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Fetch dragon-tiger list rows through QMT's ContextInfo.get_longhubang."""
    client = _qmt_bridge_client(config, xtdata)
    rows: list[dict[str, Any]] = []
    failed = 0
    for symbol in symbols:
        try:
            frame = client.get_longhubang(
                [symbol], start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
            )
            if metrics is not None:
                metrics["requests"] = int(metrics.get("requests", 0)) + 1
        except Exception:
            failed += 1
            if metrics is not None:
                metrics["failed_requests"] = int(metrics.get("failed_requests", 0)) + 1
            logger.warning("QMT bridge longhubang failed for %s", symbol, exc_info=True)
            continue
        if frame is None or not hasattr(frame, "to_dict"):
            continue
        for record in frame.to_dict("records"):
            trade_date = _coerce_date(record.get("date"))
            if trade_date is None or trade_date < start or trade_date > end:
                continue
            buy_total = 0.0
            sell_total = 0.0
            for booth_key, target in (
                ("buyTraderBooth", "buy"),
                ("sellTraderBooth", "sell"),
            ):
                for trader in _coerce_lhb_nested_trader_frame(record.get(booth_key)):
                    amount = _float(trader.get(f"{target}Amount")) or 0.0
                    if target == "buy":
                        buy_total += amount
                    else:
                        sell_total += amount
            rows.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "reason": str(record.get("reason") or ""),
                    "buy_amount": buy_total,
                    "sell_amount": sell_total,
                    "net_amount": buy_total - sell_total,
                }
            )
    if failed == len(symbols) and symbols:
        raise QmtBridgeSourceError("QMT bridge returned no longhubang responses")
    if not rows:
        return pl.DataFrame()
    frame = pl.DataFrame(rows).unique(subset=["symbol", "trade_date", "reason"], keep="last")
    if metrics is not None:
        metrics["rows_read"] = int(metrics.get("rows_read", 0)) + frame.height
    return with_provenance(
        frame.sort(["trade_date", "symbol"]),
        source=SOURCE,
        data_version=data_version_for("dragon_tiger"),
    )


def fetch_top_holders_qmt(
    symbols: list[str],
    start: date,
    end: date,
    *,
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Fetch top-10 holders through QMT's ContextInfo.get_top10_share_holder."""
    client = _qmt_bridge_client(config, xtdata)
    rows: list[dict[str, Any]] = []
    failed = 0
    for symbol in symbols:
        try:
            frame = client.get_top10_share_holder(
                [symbol], "holder", start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
            )
            if metrics is not None:
                metrics["requests"] = int(metrics.get("requests", 0)) + 1
        except Exception:
            failed += 1
            if metrics is not None:
                metrics["failed_requests"] = int(metrics.get("failed_requests", 0)) + 1
            logger.warning("QMT bridge top-holders failed for %s", symbol, exc_info=True)
            continue
        if frame is None or not hasattr(frame, "to_dict"):
            continue
        try:
            records = frame.to_dict("records")
        except Exception:
            continue
        for record in records:
            record_date = _coerce_date(record.get("index"))
            if record_date is None or record_date < start or record_date > end:
                continue
            holder_names = record.get("holdName") or []
            holder_types = record.get("holderType") or []
            hold_nums = record.get("holdNum") or []
            hold_ratios = record.get("holdRatio") or []
            for rank in range(min(len(holder_names), 10)):
                shares = _float(hold_nums[rank]) if rank < len(hold_nums) else None
                pct = _float(hold_ratios[rank]) if rank < len(hold_ratios) else None
                h_type = str(holder_types[rank]) if rank < len(holder_types) else ""
                rows.append(
                    {
                        "symbol": symbol,
                        "record_date": record_date,
                        "holder_scope": "total",
                        "holder_rank": rank + 1,
                        "holder_name": str(holder_names[rank]),
                        "holding_shares": shares,
                        "holding_pct": pct / 100.0 if pct is not None else None,
                        "is_institution": "机构" in h_type or "基金" in h_type or "券商" in h_type,
                        "holder_type": h_type or None,
                        "announce_date": None,
                    }
                )
    if failed == len(symbols) and symbols:
        raise QmtBridgeSourceError("QMT bridge returned no top-holder responses")
    if not rows:
        return pl.DataFrame()
    frame = pl.DataFrame(rows).unique(subset=["symbol", "record_date", "holder_rank"], keep="last")
    if metrics is not None:
        metrics["rows_read"] = int(metrics.get("rows_read", 0)) + frame.height
    return with_provenance(
        frame.sort(["record_date", "symbol", "holder_rank"]),
        source=SOURCE,
        data_version=data_version_for("top_holders"),
    )


def _holder_num_pairs(frame: Any) -> list[tuple[str, date, float]]:
    """Normalize one ``get_holder_num`` response into (symbol, date, count)."""
    if frame is None:
        return []
    try:
        records = frame.to_dict("records")
    except Exception:
        return []
    pairs: list[tuple[str, date, float]] = []
    for record in records:
        parsed = _coerce_datetime(record.get("timetag"))
        if parsed is None:
            continue
        count = _float(record.get("holdNum"))
        if count is None:
            count = _float(record.get("AHoldNum"))
        if count is None:
            continue
        symbol = str(record.get("stockCode") or "").strip().upper()
        if not symbol:
            continue
        pairs.append((symbol, parsed.date(), count))
    return pairs


def _paired_holder_num_dates(
    report_pairs: list[tuple[date, float]],
    announce_pairs: list[tuple[date, float]],
) -> list[tuple[date, date, float]]:
    """Pair QMT's report-time and announce-time holder counts.

    QMT returns one timeline keyed by report period and a second timeline keyed
    by disclosure date, but neither response carries both columns. Equal holder
    counts are the stable key; repeated counts are paired in chronological order
    so an unchanged count still maps to the correct filing.
    """
    reports_by_count: dict[float, list[date]] = {}
    announcements_by_count: dict[float, list[date]] = {}
    for count_date, count in report_pairs:
        reports_by_count.setdefault(count, []).append(count_date)
    for announce_date, count in announce_pairs:
        announcements_by_count.setdefault(count, []).append(announce_date)

    rows: list[tuple[date, date, float]] = []
    for count, count_dates in reports_by_count.items():
        announce_dates = announcements_by_count.get(count, [])
        if len(announce_dates) < len(count_dates):
            logger.warning(
                "QMT shareholder counts found only %d announcement date(s) for %d "
                "report date(s) at holder count %s; unmatched rows are dropped",
                len(announce_dates),
                len(count_dates),
                count,
            )
        for count_date, announce_date in zip(count_dates, announce_dates, strict=False):
            rows.append((count_date, announce_date, count))
    return rows


def fetch_shareholder_counts_qmt(
    symbols: list[str],
    start: date,
    end: date,
    *,
    by: str = "change_date",
    config: Any = None,
    xtdata: Any | None = None,
    metrics: dict[str, Any] | None = None,
) -> pl.DataFrame:
    """Fetch shareholder counts through QMT's report/announcement timelines.

    ``by="change_date"`` windows on the count date for backfills.
    ``by="notice_date"`` windows on the disclosure date for daily runs, which
    lets a count as of an older period end appear when it is disclosed now.
    QMT does not carry the EastMoney concentration columns, so those stay null.
    """
    if by not in {"change_date", "notice_date"}:
        raise ValueError(f"shareholder_counts cannot window on {by!r}")
    if start > end:
        raise ValueError("shareholder_counts start must not be after end")
    if not symbols:
        return pl.DataFrame()

    client = _qmt_bridge_client(config, xtdata)
    unique_symbols = list(dict.fromkeys(str(symbol).strip().upper() for symbol in symbols))
    chunk_size = max(1, int(getattr(config, "qmt_bridge_chunk_size", 100) or 100))
    chunks = [
        unique_symbols[offset : offset + chunk_size]
        for offset in range(0, len(unique_symbols), chunk_size)
    ]
    if by == "notice_date":
        report_start, report_end = start - timedelta(days=_HOLDERNUM_ANNOUNCEMENT_LAG_DAYS), end
        announce_start, announce_end = start, end
    else:
        report_start, report_end = start, end
        announce_start, announce_end = start, end + timedelta(days=_HOLDERNUM_ANNOUNCEMENT_LAG_DAYS)

    report_by_symbol: dict[str, list[tuple[date, float]]] = {}
    announcements_by_symbol: dict[str, list[tuple[date, float]]] = {}
    failed_chunks = 0
    for chunk in chunks:
        try:
            report_frame = client.get_holder_num(
                chunk,
                report_start.strftime("%Y%m%d"),
                report_end.strftime("%Y%m%d"),
                "report_time",
            )
            announce_frame = client.get_holder_num(
                chunk,
                announce_start.strftime("%Y%m%d"),
                announce_end.strftime("%Y%m%d"),
                "announce_time",
            )
            if metrics is not None:
                metrics["requests"] = int(metrics.get("requests", 0)) + 2
        except Exception:
            failed_chunks += 1
            if metrics is not None:
                metrics["failed_requests"] = int(metrics.get("failed_requests", 0)) + 2
            logger.warning("QMT bridge shareholder-counts chunk failed", exc_info=True)
            continue

        for symbol, count_date, count in _holder_num_pairs(report_frame):
            report_by_symbol.setdefault(symbol, []).append((count_date, count))
        for symbol, announce_date, count in _holder_num_pairs(announce_frame):
            announcements_by_symbol.setdefault(symbol, []).append((announce_date, count))

    if failed_chunks == len(chunks):
        raise QmtBridgeSourceError("QMT bridge returned no shareholder-count responses")

    rows: list[dict[str, Any]] = []
    for symbol in sorted(set(report_by_symbol) & set(announcements_by_symbol)):
        paired = _paired_holder_num_dates(report_by_symbol[symbol], announcements_by_symbol[symbol])
        for count_date, announce_date, count in paired:
            if by == "notice_date" and not start <= announce_date <= end:
                continue
            if by == "change_date" and not start <= count_date <= end:
                continue
            rows.append(
                {
                    "symbol": symbol,
                    "count_date": count_date,
                    "holder_count": count,
                    "holder_count_change_pct": None,
                    "avg_float_shares": None,
                    "avg_holding_value": None,
                    "announce_date": announce_date,
                }
            )

    if not rows:
        return pl.DataFrame()
    frame = pl.DataFrame(rows).unique(
        subset=["symbol", "count_date", "announce_date"],
        keep="last",
    )
    if metrics is not None:
        metrics["rows_read"] = int(metrics.get("rows_read", 0)) + frame.height
    return with_provenance(
        frame.sort(["count_date", "symbol", "announce_date"]),
        source=SOURCE,
        data_version=data_version_for("shareholder_counts"),
    )
