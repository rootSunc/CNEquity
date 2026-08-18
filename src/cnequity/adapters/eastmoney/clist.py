"""EastMoney push2 clist pagination."""

from __future__ import annotations

import json
import logging
import time
from urllib.parse import urlencode

from cnequity.adapters.eastmoney.common import (
    ALL_A_FS,
    PUSH2_CLIST_HOSTS,
    _to_int,
    symbol_from_clist,
)
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient

logger = logging.getLogger(__name__)


def _row_key(item: dict) -> tuple[str, str]:
    """Stable identity for one clist security row across page boundaries."""
    for field in ("f12", "SECURITY_CODE", "code"):
        value = item.get(field)
        if value is not None and str(value).strip():
            return field, str(value).strip()
    return "row", json.dumps(item, sort_keys=True, default=str, separators=(",", ":"))


def _fetch_clist_page(
    client: EastMoneyClient,
    *,
    host: str,
    fields: str,
    fs: str,
    page: int,
    page_size: int,
    max_retries: int = 3,
    retry_backoff_seconds: float = 1.0,
) -> tuple[list[dict], int | None]:
    params = urlencode(
        {
            "pn": page,
            "pz": page_size,
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f12",
            "fs": fs,
            "fields": fields,
        }
    )
    url = f"{host}/api/qt/clist/get?{params}"
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            resp = client.get(url)
            resp.raise_for_status()
            payload = resp.json()
            data = payload.get("data")
            if not isinstance(data, dict):
                raise RuntimeError("EastMoney clist response has no data object")
            raw_diff = data.get("diff")
            if raw_diff is None:
                diff: list[dict] = []
            elif isinstance(raw_diff, list):
                if any(not isinstance(item, dict) for item in raw_diff):
                    raise RuntimeError(
                        "EastMoney clist response data.diff contains a non-object row"
                    )
                diff = raw_diff
            else:
                raise RuntimeError("EastMoney clist response data.diff is not a list")
            raw_total = data.get("total")
            if raw_total is None or raw_total == "":
                total = None
            else:
                total = _to_int(raw_total, minimum=0)
                if total is None:
                    raise RuntimeError(
                        "EastMoney clist response data.total is not a non-negative integer"
                    )
            if total is None and diff:
                raise RuntimeError(
                    "EastMoney clist response data.diff contains rows without a reported total"
                )
            return diff, total
        except Exception as exc:
            last_exc = exc
            from cnequity.adapters.eastmoney.em_auth import is_transport_fail_fast

            if is_transport_fail_fast(exc):
                break
            if attempt + 1 < max_retries:
                time.sleep(retry_backoff_seconds * (attempt + 1))
    raise RuntimeError(f"EastMoney clist page {page} failed on {host}: {last_exc}") from last_exc


def fetch_clist_pages(
    client: EastMoneyClient,
    *,
    fields: str,
    fs: str = ALL_A_FS,
    # pz=5000 often trips push2 502 mid-universe (esp. overseas); 100 matches
    # rotation boards. Callers that need fewer round-trips may pass a larger pz.
    page_size: int = 100,
) -> list[dict]:
    rows_by_key: dict[tuple[str, str], dict] = {}
    active_host: str | None = None
    page = 1
    reported_total: int | None = None

    while True:
        if active_host is None:
            page_rows: list[dict] = []
            for host in PUSH2_CLIST_HOSTS:
                try:
                    page_rows, page_total = _fetch_clist_page(
                        client,
                        host=host,
                        fields=fields,
                        fs=fs,
                        page=page,
                        page_size=page_size,
                    )
                except Exception as exc:
                    logger.warning("EastMoney clist page %s failed on %s: %s", page, host, exc)
                    continue
                active_host = host
                break
            if active_host is None:
                # Full-universe snapshot: don't persist a truncated page.
                raise RuntimeError(
                    f"EastMoney clist page {page} failed on all hosts "
                    f"({len(rows_by_key)} rows fetched before failure)"
                )
        else:
            try:
                page_rows, page_total = _fetch_clist_page(
                    client,
                    host=active_host,
                    fields=fields,
                    fs=fs,
                    page=page,
                    page_size=page_size,
                )
            except Exception as exc:
                # Mid-pagination: try remaining hosts before fail-loud (push2
                # often 502s while push2delay still serves the same page).
                logger.warning(
                    "EastMoney clist page %s failed on %s: %s; trying failover hosts",
                    page,
                    active_host,
                    exc,
                )
                page_rows = []
                recovered = False
                for host in PUSH2_CLIST_HOSTS:
                    if host == active_host:
                        continue
                    try:
                        page_rows, page_total = _fetch_clist_page(
                            client,
                            host=host,
                            fields=fields,
                            fs=fs,
                            page=page,
                            page_size=page_size,
                        )
                        active_host = host
                        recovered = True
                        break
                    except Exception as host_exc:
                        logger.warning(
                            "EastMoney clist page %s failed on %s: %s",
                            page,
                            host,
                            host_exc,
                        )
                if not recovered:
                    raise RuntimeError(
                        f"EastMoney clist page {page} failed on all hosts "
                        f"({len(rows_by_key)} rows fetched before failure)"
                    ) from exc

        if page_total is not None:
            if reported_total is not None and page_total != reported_total:
                raise RuntimeError(
                    "EastMoney clist pagination changed reported total "
                    f"({reported_total} -> {page_total}) on page {page}"
                )
            reported_total = page_total

        if not page_rows:
            if reported_total is not None and reported_total > len(rows_by_key):
                raise RuntimeError(
                    f"EastMoney clist page {page} returned empty before reported "
                    f"total was reached ({len(rows_by_key)}/{reported_total} rows)"
                )
            break
        if reported_total is None:
            raise RuntimeError(
                f"EastMoney clist page {page} returned rows without a reported total"
            )
        previous_row_count = len(rows_by_key)
        for item in page_rows:
            rows_by_key[_row_key(item)] = item
        next_row_count = len(rows_by_key)
        if next_row_count > reported_total:
            raise RuntimeError(
                "EastMoney clist pagination exceeded reported total "
                f"({next_row_count}/{reported_total})"
            )
        if next_row_count == previous_row_count and next_row_count < reported_total:
            raise RuntimeError(
                f"EastMoney clist pagination did not advance on page {page} "
                f"({next_row_count}/{reported_total} unique rows)"
            )
        if len(page_rows) < page_size and next_row_count < reported_total:
            raise RuntimeError(
                f"EastMoney clist page {page} returned only {len(page_rows)} row(s) "
                f"before reported total was reached ({next_row_count}/{reported_total}); "
                "refusing a potentially truncated result"
            )
        if len(rows_by_key) >= reported_total:
            break
        page += 1

    return list(rows_by_key.values())


def clist_rows_to_symbols(
    rows: list[dict],
    *,
    symbol_resolver=symbol_from_clist,
) -> list[tuple[str, dict]]:
    out: list[tuple[str, dict]] = []
    for item in rows:
        market_id = _to_int(item.get("f13"), default=0, minimum=0)
        sym = symbol_resolver(str(item.get("f12", "")), market_id)
        if sym:
            out.append((sym, item))
    return out
