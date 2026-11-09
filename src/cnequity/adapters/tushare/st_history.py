"""Historical ST evidence for Beijing-exchange symbols via Tushare Pro.

Tushare's ``stock_st`` endpoint is a list of ST symbols, not a complete
per-symbol trading-status series.  The adapter therefore joins the returned
positive facts to the lake's own traded dates and emits explicit ``normal``
rows for every other traded date.  A missing or malformed source response is
retryable; it never becomes negative evidence.

The direct ``stock_st`` endpoint starts at 2017-01-01.  For the 2016 slice,
the adapter uses Tushare's historical ``bak_basic`` daily stock list and treats
the exchange's ``ST``/``*ST`` name prefix as the label; every requested symbol
must be present on every requested day or it is returned as failed.  Symbols
with persisted bars before 2016 are still returned as failed. Legacy
43/83/87xxx and current 920xxx BJ codes are queried as aliases because the
exchange migrated code identities.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from collections.abc import Callable, Iterable, Mapping
from datetime import date, datetime, timezone

import httpx
import polars as pl

from cnequity.config import Config
from cnequity.domain.bse_codes import current_to_legacy, legacy_to_current
from cnequity.domain.http_policy import (
    SourceCoolingDown,
    record_business_refusal,
    record_cache_reuse,
    record_http_response,
    record_request_event,
)
from cnequity.domain.rate_limit import source_request
from cnequity.domain.schemas import FETCHED_AT_DTYPE
from cnequity.domain.symbols import parse_symbol
from cnequity.domain.trading_status import STATUS_NORMAL
from cnequity.file_lock import exclusive_lock
from cnequity.query.parquet_scan import scan_parquet_root
from cnequity.storage.atomic import write_json_atomic

__all__ = ["TUSHARE_ST_DIRECT_FLOOR", "TUSHARE_ST_HISTORY_FLOOR", "fetch_st_history"]

logger = logging.getLogger(__name__)

TUSHARE_API_URL = "https://api.tushare.pro"
TUSHARE_ST_HISTORY_FLOOR = date(2016, 1, 1)
TUSHARE_ST_DIRECT_FLOOR = date(2017, 1, 1)
_PAGE_SIZE = 1000
_BAK_BASIC_PAGE_SIZE = 7000
_MAX_PAGES = 1000
_BAK_BASIC_CACHE_SECONDS = 7 * 86400
_BAK_BASIC_CACHE_VERSION = 1

_OUTPUT_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "is_trading": pl.Boolean,
    "status": pl.Utf8,
    "risk_warning": pl.Boolean,
    "fetched_at": FETCHED_AT_DTYPE,
}


def _post_json(
    client: httpx.Client,
    payload: dict,
    *,
    config: Config | None,
    sleep: Callable[[float], None],
) -> object:
    """POST once, retrying only transient transport/provider failures.

    Tushare errors in a successful HTTP response (for example insufficient
    points or an invalid token) are deliberately left to the endpoint parser;
    retrying those only delays a deterministic failure.  A timeout, malformed
    JSON response, HTTP 408, or HTTP 5xx is different: retrying the same
    request can recover without forcing the caller to replay the whole symbol
    batch.  Exhaustion still raises so the caller records the symbol/day as
    unresolved rather than manufacturing a normal row. A rate-limit refusal
    stops the source, including when it appears in a 200 business envelope.
    """
    attempts = max(1, int(config.max_retries)) if config is not None else 1
    backoff = float(config.retry_backoff_seconds) if config is not None else 0.0
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            with source_request(config, "tushare"):
                if attempt:
                    record_request_event(config, "tushare", "retry")
                response = client.post(TUSHARE_API_URL, json=payload)
                record_http_response(config, "tushare", response)
                response.raise_for_status()
                body = response.json()
                if isinstance(body, dict) and body.get("code") not in (0, "0"):
                    message = str(body.get("msg") or body.get("message") or "").lower()
                    if any(
                        marker in message
                        for marker in ("频繁", "限流", "每分钟", "rate limit", "too many requests")
                    ):
                        record_business_refusal(config, "tushare", kind="business_rate_limit")
                        raise SourceCoolingDown("tushare: provider rate-limit response")
                return body
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status == 429:
                raise SourceCoolingDown("tushare: HTTP 429 rate-limit response") from exc
            if status != 408 and not (status is not None and 500 <= status <= 599):
                raise
            last_error = exc
        except (httpx.RequestError, ValueError) as exc:
            last_error = exc
        if attempt + 1 < attempts:
            sleep(max(0.0, backoff) * (attempt + 1))
    assert last_error is not None
    raise last_error


def _source_codes(symbol: str) -> tuple[str, ...]:
    info = parse_symbol(symbol)
    if info.exchange != "BJ":
        return (f"{info.code}.{info.exchange}",)
    mapping = legacy_to_current()
    reverse = current_to_legacy()
    aliases = {info.code}
    if info.code in mapping:
        aliases.add(mapping[info.code])
    if info.code in reverse:
        aliases.add(reverse[info.code])
    return tuple(f"{code}.BJ" for code in sorted(aliases))


def _parse_date(value: object) -> date | None:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        try:
            return datetime.strptime(text, "%Y%m%d").date()
        except ValueError:
            return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _traded_dates_from_lake(
    config: Config,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, list[date]]:
    root = config.curated_root / "daily_bars"
    if not root.exists():
        return {}
    frame = (
        scan_parquet_root(
            root,
            partition_col="trade_date",
            start=start,
            end=end,
            traded_only=True,
        )
        .filter(pl.col("symbol").is_in(symbols))
        .select("symbol", "trade_date")
        .unique()
        .sort(["symbol", "trade_date"])
        .collect(engine="streaming")
    )
    return {
        (key[0] if isinstance(key, tuple) else key): [
            row["trade_date"] for row in rows.iter_rows(named=True)
        ]
        for key, rows in frame.partition_by("symbol", as_dict=True).items()
    }


def _request_rows(
    client: httpx.Client,
    token: str,
    ts_code: str,
    start: date,
    end: date,
    *,
    config: Config | None,
    sleep: Callable[[float], None],
) -> list[dict]:
    rows: list[dict] = []
    for page in range(_MAX_PAGES):
        payload = {
            "api_name": "stock_st",
            "token": token,
            "params": {
                "ts_code": ts_code,
                "start_date": start.strftime("%Y%m%d"),
                "end_date": end.strftime("%Y%m%d"),
                "limit": _PAGE_SIZE,
                "offset": page * _PAGE_SIZE,
            },
            "fields": "ts_code,name,trade_date,type,type_name",
        }
        response_payload = _post_json(client, payload, config=config, sleep=sleep)
        if not isinstance(response_payload, dict):
            raise RuntimeError("Tushare stock_st response is not an object")
        if response_payload.get("code") not in (0, "0"):
            raise RuntimeError(
                "Tushare stock_st failed: "
                f"{response_payload.get('msg') or response_payload.get('message') or response_payload.get('code')}"
            )
        data = response_payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Tushare stock_st response has no data object")
        fields = data.get("fields")
        items = data.get("items")
        if not isinstance(fields, list) or not isinstance(items, list):
            raise RuntimeError("Tushare stock_st response has invalid fields/items")
        page_rows: list[dict] = []
        for item in items:
            if not isinstance(item, list) or len(item) != len(fields):
                raise RuntimeError("Tushare stock_st response contains a malformed row")
            row = dict(zip((str(field) for field in fields), item, strict=True))
            page_rows.append(row)
        rows.extend(page_rows)
        if len(page_rows) < _PAGE_SIZE:
            break
    else:
        raise RuntimeError("Tushare stock_st pagination exceeded the safety limit")
    return rows


def _request_bak_basic_rows(
    client: httpx.Client,
    token: str,
    trade_date: date,
    *,
    config: Config | None,
    sleep: Callable[[float], None],
) -> list[dict]:
    """Fetch one complete historical daily stock-list page set."""
    rows: list[dict] = []
    for page in range(_MAX_PAGES):
        payload = {
            "api_name": "bak_basic",
            "token": token,
            "params": {
                "trade_date": trade_date.strftime("%Y%m%d"),
                "limit": _BAK_BASIC_PAGE_SIZE,
                "offset": page * _BAK_BASIC_PAGE_SIZE,
            },
            "fields": "ts_code,name,trade_date",
        }
        response_payload = _post_json(client, payload, config=config, sleep=sleep)
        if not isinstance(response_payload, dict) or response_payload.get("code") not in (0, "0"):
            message = response_payload.get("msg") if isinstance(response_payload, dict) else None
            raise RuntimeError(f"Tushare bak_basic failed: {message or response_payload!r}")
        data = response_payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Tushare bak_basic response has no data object")
        fields = data.get("fields")
        items = data.get("items")
        if not isinstance(fields, list) or not isinstance(items, list):
            raise RuntimeError("Tushare bak_basic response has invalid fields/items")
        page_rows: list[dict] = []
        for item in items:
            if not isinstance(item, list) or len(item) != len(fields):
                raise RuntimeError("Tushare bak_basic response contains a malformed row")
            page_rows.append(dict(zip((str(field) for field in fields), item, strict=True)))
        rows.extend(page_rows)
        if len(page_rows) < _BAK_BASIC_PAGE_SIZE:
            break
    else:
        raise RuntimeError("Tushare bak_basic pagination exceeded the safety limit")
    if not _bak_basic_rows_valid(rows, trade_date):
        raise RuntimeError(
            f"Tushare bak_basic {trade_date} is empty or has invalid identity/date rows"
        )
    return rows


def _bak_basic_rows_valid(rows: object, trade_date: date) -> bool:
    return (
        isinstance(rows, list)
        and bool(rows)
        and all(
            isinstance(row, dict)
            and str(row.get("ts_code") or "").strip()
            and _parse_date(row.get("trade_date")) == trade_date
            for row in rows
        )
    )


def _bak_basic_rows_hash(rows: list[dict]) -> str:
    content = json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _cached_bak_basic_rows(
    client: httpx.Client,
    token: str,
    trade_date: date,
    *,
    config: Config | None,
    sleep: Callable[[float], None],
    use_cache: bool,
) -> tuple[list[dict], datetime]:
    """Reuse only a complete positive day, partitioned by credential identity."""
    if config is None or not use_cache:
        rows = _request_bak_basic_rows(client, token, trade_date, config=config, sleep=sleep)
        return rows, datetime.now(timezone.utc)
    token_key = hashlib.sha256(token.encode("utf-8")).hexdigest()
    path = (
        config.meta_root
        / "source_cache"
        / "tushare"
        / "bak_basic"
        / token_key
        / f"{trade_date.isoformat()}.json"
    )
    with exclusive_lock(path.with_suffix(".lock")):
        if path.exists():
            try:
                saved = json.loads(path.read_text(encoding="utf-8"))
                age = datetime.now(timezone.utc) - datetime.fromisoformat(saved["captured_at"])
                rows = saved["rows"]
                if (
                    saved["trade_date"] == trade_date.isoformat()
                    and saved["parser_version"] == _BAK_BASIC_CACHE_VERSION
                    and 0 <= age.total_seconds() < _BAK_BASIC_CACHE_SECONDS
                    and _bak_basic_rows_valid(rows, trade_date)
                    and _bak_basic_rows_hash(rows) == saved["sha256"]
                ):
                    record_cache_reuse(config, "tushare", "bak_basic_day")
                    return rows, datetime.fromisoformat(saved["captured_at"])
            except (OSError, KeyError, TypeError, ValueError) as exc:
                logger.warning("Tushare bak_basic cache invalid for %s: %s", trade_date, exc)
        rows = _request_bak_basic_rows(client, token, trade_date, config=config, sleep=sleep)
        captured_at = datetime.now(timezone.utc)
        if _bak_basic_rows_valid(rows, trade_date):
            try:
                write_json_atomic(
                    path,
                    {
                        "trade_date": trade_date.isoformat(),
                        "captured_at": captured_at.isoformat(),
                        "parser_version": _BAK_BASIC_CACHE_VERSION,
                        "sha256": _bak_basic_rows_hash(rows),
                        "rows": rows,
                    },
                )
            except OSError as exc:
                logger.warning(
                    "Tushare valid bak_basic %s could not be cached: %s", trade_date, exc
                )
        return rows, captured_at


def _is_st_name(value: object) -> bool:
    """Recognize the exchange's ST/*ST prefix in the historical name field."""
    name = str(value or "").strip().upper()
    return name.startswith(("ST", "*ST", "S*ST", "SST"))


def _empty() -> pl.DataFrame:
    return pl.DataFrame(schema=_OUTPUT_SCHEMA)


def fetch_st_history(
    symbols: list[str],
    start: date,
    end: date,
    *,
    token: str | None = None,
    client: httpx.Client | None = None,
    config: Config | None = None,
    trading_dates: Mapping[str, Iterable[date]] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[pl.DataFrame, list[str]]:
    """Fetch complete traded-day ST/normal evidence for BJ symbols.

    ``trading_dates`` is injectable for tests and callers that already have a
    bar calendar.  In production it is read from ``daily_bars`` through
    *config*.  The source token is deliberately required; unlike a live ST
    snapshot, an unauthenticated empty response cannot be treated as normal.
    """
    token = (token or (config.tushare_token if config is not None else None) or "").strip()
    if not token:
        raise ValueError(
            "Tushare historical ST evidence requires TUSHARE_TOKEN or [sources.tushare].token"
        )
    if start > end:
        raise ValueError(f"Tushare ST history window is inverted: {start} > {end}")
    symbols = sorted(set(symbols))
    if any(parse_symbol(symbol).exchange != "BJ" for symbol in symbols):
        raise ValueError("Tushare ST history adapter only accepts BJ symbols")
    if trading_dates is None:
        if config is None:
            raise ValueError("Tushare ST history requires config or trading_dates")
        dates_by_symbol = _traded_dates_from_lake(config, symbols, start, end)
    else:
        dates_by_symbol = {
            symbol: sorted({d for d in dates if start <= d <= end})
            for symbol, dates in trading_dates.items()
        }

    owns_client = client is None
    if client is None:
        timeout = config.tushare_timeout_sec if config is not None else 30.0
        client = httpx.Client(timeout=timeout)

    rows: list[dict] = []
    failed: list[str] = []
    try:
        pre_floor_symbols = {
            symbol
            for symbol in symbols
            if any(d < TUSHARE_ST_HISTORY_FLOOR for d in dates_by_symbol.get(symbol, []))
        }
        pre_direct_dates = {
            symbol: [d for d in dates_by_symbol.get(symbol, []) if d < TUSHARE_ST_DIRECT_FLOOR]
            for symbol in symbols
            if symbol not in pre_floor_symbols
        }
        pre_direct_symbols = {symbol for symbol, dates in pre_direct_dates.items() if dates}
        pre_st_dates: dict[str, set[date]] = {symbol: set() for symbol in symbols}
        pre_captured_at: dict[date, datetime] = {}
        pre_failed: set[str] = set()
        if pre_direct_symbols:
            alias_to_symbol = {
                alias: symbol
                for symbol in sorted(pre_direct_symbols)
                for alias in _source_codes(symbol)
            }
            day_queue = sorted({d for dates in pre_direct_dates.values() for d in dates})
            for position, trade_date in enumerate(day_queue):
                required = {
                    symbol for symbol, dates in pre_direct_dates.items() if trade_date in dates
                }
                try:
                    day_rows, captured_at = _cached_bak_basic_rows(
                        client,
                        token,
                        trade_date,
                        config=config,
                        sleep=sleep,
                        use_cache=owns_client,
                    )
                except SourceCoolingDown:
                    return _empty(), symbols
                except Exception as exc:  # noqa: BLE001 — stop the failing source
                    logger.warning("tushare bak_basic: failed for %s: %s", trade_date, exc)
                    remaining = set(day_queue[position:])
                    pre_failed.update(
                        symbol
                        for symbol, dates in pre_direct_dates.items()
                        if remaining.intersection(dates)
                    )
                    break
                found: set[str] = set()
                pre_captured_at[trade_date] = captured_at
                for item in day_rows:
                    source_code = str(item.get("ts_code") or "").strip().upper()
                    symbol = alias_to_symbol.get(source_code)
                    if (
                        symbol is None
                        or _parse_date(item.get("trade_date")) != trade_date
                        or not str(item.get("name") or "").strip()
                    ):
                        continue
                    found.add(symbol)
                    if _is_st_name(item.get("name")):
                        pre_st_dates[symbol].add(trade_date)
                pre_failed.update(required - found)

        for index, symbol in enumerate(symbols):
            traded_dates = dates_by_symbol.get(symbol, [])
            if symbol in pre_floor_symbols:
                failed.append(symbol)
                continue
            if symbol in pre_failed:
                failed.append(symbol)
                continue
            try:
                st_dates = pre_st_dates[symbol]
                aliases = set(_source_codes(symbol))
                post_direct_dates = [d for d in traded_dates if d >= TUSHARE_ST_DIRECT_FLOOR]
                post_captured_at: datetime | None = None
                if post_direct_dates:
                    for source_code in sorted(aliases):
                        for item in _request_rows(
                            client,
                            token,
                            source_code,
                            max(start, TUSHARE_ST_DIRECT_FLOOR),
                            end,
                            config=config,
                            sleep=sleep,
                        ):
                            returned_code = str(item.get("ts_code") or "").strip().upper()
                            # The endpoint is queried per symbol/alias.  A
                            # missing code is just as unsafe as a different
                            # code: accepting it could attach another
                            # security's positive ST row to this symbol.
                            if returned_code not in aliases:
                                raise RuntimeError(
                                    "Tushare stock_st returned an invalid code "
                                    f"{returned_code!r} while querying {symbol}"
                                )
                            trade_date = _parse_date(item.get("trade_date"))
                            if trade_date is None:
                                raise RuntimeError(
                                    f"Tushare stock_st returned an invalid trade_date for {symbol}"
                                )
                            if not (start <= trade_date <= end):
                                continue
                            kind = str(item.get("type") or "").strip().upper()
                            if kind not in {"ST", "*ST"}:
                                raise RuntimeError(f"unknown Tushare ST type {kind!r} for {symbol}")
                            st_dates.add(trade_date)
                    post_captured_at = datetime.now(timezone.utc)
                rows.extend(
                    {
                        "symbol": symbol,
                        "trade_date": trade_date,
                        "is_trading": True,
                        "status": STATUS_NORMAL,
                        # `kind` above distinguishes ST from *ST, but no other
                        # source feeding this dataset does, so the finer
                        # designation has never survived into curated. It stays
                        # in the exchange 简称 via instruments.name.
                        "risk_warning": trade_date in st_dates,
                        "fetched_at": (
                            pre_captured_at[trade_date]
                            if trade_date < TUSHARE_ST_DIRECT_FLOOR
                            else post_captured_at
                        ),
                    }
                    for trade_date in traded_dates
                )
            except SourceCoolingDown:
                failed.extend(symbols[index:])
                break
            except Exception as exc:  # noqa: BLE001 — retry the symbol, not a false normal set
                failed.append(symbol)
                logger.warning("tushare ST history: failed for %s: %s", symbol, exc)
    finally:
        if owns_client:
            client.close()

    if not rows:
        return _empty(), failed
    return (
        pl.DataFrame(rows, schema=_OUTPUT_SCHEMA)
        .unique(subset=["symbol", "trade_date"], keep="last")
        .sort(["trade_date", "symbol"]),
        failed,
    )
