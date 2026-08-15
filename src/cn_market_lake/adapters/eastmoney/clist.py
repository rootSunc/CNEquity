"""EastMoney push2 clist pagination."""

from __future__ import annotations

import logging
import time
from urllib.parse import urlencode

from cn_market_lake.adapters.eastmoney.common import (
    ALL_A_FS,
    PUSH2_CLIST_HOSTS,
    symbol_from_clist,
)
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient

logger = logging.getLogger(__name__)


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
) -> tuple[list[dict], int]:
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
            data = payload.get("data") or {}
            diff = data.get("diff") or []
            total = int(data.get("total") or 0)
            return diff, total
        except Exception as exc:
            last_exc = exc
            from cn_market_lake.adapters.eastmoney.em_auth import is_transport_fail_fast

            if is_transport_fail_fast(exc):
                break
            if attempt + 1 < max_retries:
                time.sleep(retry_backoff_seconds * (attempt + 1))
    raise RuntimeError(f"EastMoney clist page {page} failed on {host}") from last_exc


def fetch_clist_pages(
    client: EastMoneyClient,
    *,
    fields: str,
    fs: str = ALL_A_FS,
    # pz=5000 often trips push2 502 mid-universe (esp. overseas); 100 matches
    # rotation boards. Callers that need fewer round-trips may pass a larger pz.
    page_size: int = 100,
) -> list[dict]:
    rows: list[dict] = []
    active_host: str | None = None
    page = 1
    total = 0

    while True:
        if active_host is None:
            page_rows: list[dict] = []
            for host in PUSH2_CLIST_HOSTS:
                try:
                    page_rows, total = _fetch_clist_page(
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
                    f"({len(rows)} rows fetched before failure)"
                )
        else:
            try:
                page_rows, total = _fetch_clist_page(
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
                        page_rows, total = _fetch_clist_page(
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
                        f"({len(rows)} rows fetched before failure)"
                    ) from exc

        if not page_rows:
            break
        rows.extend(page_rows)
        if len(rows) >= total:
            break
        page += 1

    return rows


def clist_rows_to_symbols(rows: list[dict]) -> list[tuple[str, dict]]:
    out: list[tuple[str, dict]] = []
    for item in rows:
        sym = symbol_from_clist(str(item.get("f12", "")), int(item.get("f13", 0)))
        if sym:
            out.append((sym, item))
    return out
