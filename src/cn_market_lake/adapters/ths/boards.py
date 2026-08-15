"""同花顺 (10jqka) sector board catalog and daily bars.

Why this source rather than EastMoney's ``push2his``: that host refuses any
egress outside mainland China at the WAF layer — TLS completes, non-API paths
answer normally, and only ``/api/...`` is dropped. Chrome TLS impersonation,
browser cookies, CDN-edge pinning and mainland *cloud* proxies were all measured
and all refused, so board history is simply not reachable from here.

``d.10jqka.com.cn`` serves the same shape of data unauthenticated from any
egress, with far deeper history (行业 back to 2007-08, 概念 to 2014) on a single
consistent index base. That last property is the important one: the previous lake
spliced TDX-based history against EastMoney-based daily rows under one
``sector_code``, which produced a fake +79% median jump on the splice date.

The payload is JSONP-flavoured::

    quotebridge_v6_line_bk_881121_01_2025({"data": "20250102,o,h,l,c,vol,amt,...;...", ...})

Board codes are ``881xxx`` for 行业 and ``885xxx`` for 概念. The concept catalog
page exposes most of the ``885xxx`` codes inline (a ``gnSection`` JSON blob); the
rest need one detail-page hit each, which is why the catalog is cached on disk —
it changes rarely and should not cost ~150 requests on every run.
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from collections.abc import Callable

    from cn_market_lake.config import Config

logger = logging.getLogger(__name__)

_KLINE_URL = "https://d.10jqka.com.cn/v6/line/bk_{code}/01/{part}.js"
_INDUSTRY_URL = "https://q.10jqka.com.cn/thshy/"
_CONCEPT_URL = "https://q.10jqka.com.cn/gn/"
_CONCEPT_DETAIL_URL = "https://q.10jqka.com.cn/gn/detail/code/{code}/"
_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
_HEADERS = {"User-Agent": _UA, "Referer": "https://q.10jqka.com.cn/"}

# `last.js` carries the most recent ~140 sessions; anything shorter than that
# needs no year files at all, which keeps the daily job to one request a board.
_LAST_WINDOW_SESSIONS = 140
_CATALOG_FILE = "ths_board_catalog.json"
_MAX_RETRIES = 3
_RETRY_BACKOFF_SECONDS = 2.0
_DEFAULT_MIN_INTERVAL = 1.0
# The listing host is much less tolerant than the data host:
# `d.10jqka.com.cn` served ~1300 sequential kline requests at 1 req/s without a
# single failure, while `q.10jqka.com.cn` started returning 401 after ~23 at the
# same pace. They therefore get separate limiters.
#
# The listing interval was 20.0 as an admitted guess. Measured 2026-08 at 25
# sequential requests per rung, 8s / 5s / 3s / 2s / 1.5s all came back 25/25
# clean, so 3.0 is twice the fastest rate observed safe and three times the one
# known to fail. The measurement was 25 requests of a single URL where a real
# rebuild is ~95 across several, so go lower only with a longer run behind it.
_PAGE_HOST = "q.10jqka.com.cn"
_PAGE_SOURCE = "ths_pages"
_DEFAULT_PAGE_MIN_INTERVAL = 3.0


class ThsError(RuntimeError):
    """A 同花顺 fetch failed in a way the caller must not paper over."""


def _throttle(url: str, config: Config | None) -> None:
    """Pace requests, per host — see the note on the two limiters above."""
    is_page = _PAGE_HOST in url
    if config is not None:
        config.rate_limit(_PAGE_SOURCE if is_page else "ths")
        return
    time.sleep(_DEFAULT_PAGE_MIN_INTERVAL if is_page else _DEFAULT_MIN_INTERVAL)


def _get(url: str, *, config: Config | None, timeout: float = 20.0) -> str:
    last_exc: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        _throttle(url, config)
        try:
            resp = httpx.get(url, headers=_HEADERS, timeout=timeout, follow_redirects=True)
        except Exception as exc:  # noqa: BLE001 — retried below, raised as ThsError
            last_exc = exc
        else:
            if resp.status_code == 200:
                return resp.text
            # 401/403 mean the endpoint wants the JS-derived `hexin-v` token;
            # retrying cannot produce one, so fail immediately and loudly.
            if resp.status_code in (401, 403):
                raise ThsError(f"{url} -> HTTP {resp.status_code} (token-gated endpoint)")
            # 404 means the year file itself does not exist — routine for a
            # board younger than the year being asked for (see
            # fetch_board_bars, which already treats a per-year miss as
            # normal and moves on). Retrying cannot make a missing file
            # appear. Measured cost of not doing this: a deep sweep (2010
            # onward) against a young thematic board like 芬太尼/华为概念 hits
            # a decade of guaranteed 404s, each retried 3x with backoff —
            # ~12s wasted per missing year, compounding into whole boards
            # that take 15-20 minutes apiece and make a 432-board sweep look
            # hung.
            if resp.status_code == 404:
                raise ThsError(f"{url} -> HTTP 404 (no data for this period)")
            last_exc = ThsError(f"{url} -> HTTP {resp.status_code}")
        if attempt + 1 < _MAX_RETRIES:
            time.sleep(_RETRY_BACKOFF_SECONDS * (attempt + 1))
    raise ThsError(f"{url} failed after {_MAX_RETRIES} attempts") from last_exc


def _unwrap_jsonp(text: str) -> dict[str, Any]:
    start, end = text.find("("), text.rfind(")")
    if start < 0 or end <= start:
        raise ThsError("unrecognised JSONP payload")
    return json.loads(text[start + 1 : end])


def _catalog_path(config: Config) -> Path:
    return config.meta_root / "state" / _CATALOG_FILE


_SEED_CATALOG = Path(__file__).with_name("seeds") / "board_catalog_seed.json"


def _read_catalog(path: Path) -> list[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8")).get("boards", [])
    except (OSError, ValueError) as exc:
        logger.warning("THS catalog at %s unreadable (%s)", path, exc)
        return []


def load_cached_catalog(config: Config) -> list[dict]:
    """Cached board catalog, falling back to the seed shipped with the package.

    Rebuilding the catalog means ~95 requests to the listing host, and that host
    starts answering 401 partway through: a sweep at 1 req/s got 23 boards in
    before it tripped, and kept refusing while the requests continued. How long
    it stays tripped is not known — it had cleared by the time it was checked
    again, so treat a rebuild as something that may or may not finish today.

    That is enough to want a fallback: without one, losing the workspace cache
    would strand board bars behind a host that may not cooperate on demand. The
    seed only goes stale as boards are added, which costs coverage of the new
    board rather than correctness of the existing ones.
    """
    boards = _read_catalog(_catalog_path(config))
    if boards:
        return boards
    if _SEED_CATALOG.exists():
        boards = _read_catalog(_SEED_CATALOG)
        if boards:
            logger.warning(
                "THS catalog cache missing; using the %d-board seed shipped with the "
                "package. Run `fetch_board_catalog(refresh=True)` when the listing "
                "host is willing to pick up boards added since.",
                len(boards),
            )
    return boards


def _save_catalog(config: Config, boards: list[dict]) -> None:
    path = _catalog_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {"fetched_at": date.today().isoformat(), "boards": boards},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _parse_industry(html: str) -> list[dict]:
    out: list[dict] = []
    seen: set[str] = set()
    for code, name in re.findall(r"/code/(88\d{4})/[^>]*>([^<]{1,30})</a>", html):
        if code in seen:
            continue
        seen.add(code)
        out.append(
            {
                "sector_code": code,
                "sector_name": name.strip(),
                "board_type": "industry",
                # Industry boards are listed under the same 881xxx code they
                # trade under, so the two keys coincide here.
                "detail_code": code,
            }
        )
    return out


def _parse_concept_inline(html: str) -> dict[str, str]:
    """``gnSection`` maps most concepts to their 885xxx index code in-page."""
    m = re.search(r"id=\"gnSection\"\s+value='(\{.*?\})'", html, re.S)
    if not m:
        return {}
    try:
        blob = json.loads(m.group(1))
    except ValueError:
        return {}
    return {
        str(v.get("platename") or "").strip(): str(v.get("platecode") or "").strip()
        for v in blob.values()
        if v.get("platename") and v.get("platecode")
    }


def _parse_concept_links(html: str) -> list[tuple[str, str]]:
    """``(detail_code, name)`` for every concept listed on the catalog page."""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for code, name in re.findall(r"/gn/detail/code/(\d{6})/[^>]*>([^<]{1,30})</a>", html):
        if code in seen:
            continue
        seen.add(code)
        out.append((code, name.strip()))
    return out


def _concept_index_code(detail_code: str, *, config: Config | None) -> str | None:
    html = _get(_CONCEPT_DETAIL_URL.format(code=detail_code), config=config)
    m = re.search(r"\b(885\d{3})\b", html)
    return m.group(1) if m else None


def fetch_board_catalog(*, config: Config | None = None, refresh: bool = False) -> list[dict]:
    """All 同花顺 boards as ``sector_code`` / ``sector_name`` / ``board_type``.

    Cached on disk: resolving the concept codes the catalog page does not inline
    costs one request each, and the board universe changes far more slowly than
    the bars do.
    """
    if config is not None and not refresh:
        cached = load_cached_catalog(config)
        if cached:
            logger.info("THS catalog: %d boards from cache", len(cached))
            return cached

    boards = _parse_industry(_get(_INDUSTRY_URL, config=config))
    logger.info("THS catalog: %d industry boards", len(boards))

    concept_html = _get(_CONCEPT_URL, config=config)
    inline = _parse_concept_inline(concept_html)
    links = _parse_concept_links(concept_html)
    logger.info(
        "THS catalog: %d concepts listed, %d index codes inline; resolving the rest",
        len(links),
        len(inline),
    )

    unresolved = 0
    for detail_code, name in links:
        code = inline.get(name)
        if not code:
            code = _concept_index_code(detail_code, config=config)
            if not code:
                unresolved += 1
                logger.debug("THS catalog: no index code for concept %s (%s)", name, detail_code)
                continue
        boards.append(
            {
                "sector_code": code,
                "sector_name": name,
                "board_type": "concept",
                # Kline and membership are keyed differently: bars want the
                # 885xxx index code, the constituent pages want the 30xxxx
                # listing code. Both are in hand here, so keep both — recovering
                # the listing code later costs a request per concept.
                "detail_code": detail_code,
            }
        )

    # Same index can back two listings; keep one row per code.
    deduped: dict[str, dict] = {}
    for b in boards:
        deduped.setdefault(b["sector_code"], b)
    out = sorted(deduped.values(), key=lambda b: b["sector_code"])
    logger.info("THS catalog: %d boards total (%d concepts unresolved)", len(out), unresolved)
    if config is not None and out:
        _save_catalog(config, out)
    return out


def _parse_kline(payload: dict[str, Any], board: dict) -> list[dict]:
    rows: list[dict] = []
    raw = str(payload.get("data") or "")
    if not raw:
        return rows
    for line in raw.split(";"):
        parts = line.split(",")
        if len(parts) < 7 or len(parts[0]) != 8:
            continue
        stamp = parts[0]
        try:
            rows.append(
                {
                    "sector_code": board["sector_code"],
                    "sector_name": board["sector_name"],
                    "board_type": board["board_type"],
                    "trade_date": date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:8])),
                    "open": float(parts[1]),
                    "high": float(parts[2]),
                    "low": float(parts[3]),
                    "close": float(parts[4]),
                    "volume": int(float(parts[5])),
                    "amount": float(parts[6]),
                }
            )
        except ValueError:
            continue
    return rows


def _with_change_pct(rows: list[dict]) -> list[dict]:
    """同花顺 ships no change_pct; derive it so the column keeps its meaning.

    Computed on the fetched series, so the first bar of a window has no prior
    close and is left null rather than guessed at.
    """
    rows.sort(key=lambda r: r["trade_date"])
    prev: float | None = None
    for row in rows:
        row["change_pct"] = (
            round((row["close"] / prev - 1.0) * 100.0, 4) if prev not in (None, 0) else None
        )
        prev = row["close"]
    return rows


def fetch_board_bars(
    board: dict,
    start: date,
    end: date,
    *,
    config: Config | None = None,
) -> list[dict]:
    """Daily bars for one board over ``[start, end]``.

    Uses ``last.js`` (one request, ~140 sessions) when the window fits inside it,
    which is the daily and short-gap case; longer windows pull the per-year files
    the source already splits history into.
    """
    code = board["sector_code"]
    span_days = (end - start).days
    parts: list[str]
    if span_days <= _LAST_WINDOW_SESSIONS:
        parts = ["last"]
    else:
        parts = [str(y) for y in range(start.year, end.year + 1)]
        # The current year's file lags intraday; `last` carries the freshest bars.
        if end.year == date.today().year:
            parts.append("last")

    rows: list[dict] = []
    seen: set[date] = set()
    for part in parts:
        try:
            payload = _unwrap_jsonp(_get(_KLINE_URL.format(code=code, part=part), config=config))
        except ThsError as exc:
            # A missing year file is normal for a board younger than the window;
            # `last` failing is not, and neither is a transport error.
            if part == "last":
                raise
            logger.debug("THS %s %s unavailable: %s", code, part, exc)
            continue
        for row in _parse_kline(payload, board):
            if row["trade_date"] in seen or not (start <= row["trade_date"] <= end):
                continue
            seen.add(row["trade_date"])
            rows.append(row)
    return _with_change_pct(rows)


# A sweep is ~451 boards; these bound how long a broken source can burn.
_DEAD_SOURCE_STREAK = 12
_SWEEP_BATCH = 40


def sweep_board_bars(
    start: date,
    end: date,
    *,
    config: Config | None = None,
    boards: list[dict] | None = None,
    skip_sectors: set[str] | None = None,
    on_batch: Callable[[list[dict], list[str]], None] | None = None,
) -> tuple[list[dict], list[str], list[str]]:
    """Bars for every board over ``[start, end]``.

    Returns ``(rows, failed_codes, succeeded_codes)``. ``on_batch(rows, codes)``
    makes progress durable mid-sweep — a several-hundred-board pass that dies at
    board 400 must not discard the first 399 — and clears the accumulator, so the
    returned rows are empty for a batched caller.

    Aborts after a run of consecutive failures: once the source stops answering,
    grinding through the remaining boards only deepens a rate-limit penalty.
    """
    if boards is None:
        boards = fetch_board_catalog(config=config)
    skip = skip_sectors or set()
    todo = [b for b in boards if b["sector_code"] not in skip]

    rows: list[dict] = []
    pending: list[str] = []
    failed: list[str] = []
    succeeded: list[str] = []

    def flush() -> None:
        nonlocal rows, pending
        if on_batch is None or not pending:
            return
        on_batch(rows, list(pending))
        rows = []
        pending = []

    dead_streak = 0
    for i, board in enumerate(todo):
        if i > 0 and i % _SWEEP_BATCH == 0:
            flush()
            logger.info(
                "THS sweep: %d/%d boards (ok=%d failed=%d)",
                i,
                len(todo),
                len(succeeded),
                len(failed),
            )
        code = board["sector_code"]
        try:
            rows.extend(fetch_board_bars(board, start, end, config=config))
            succeeded.append(code)
            pending.append(code)
            dead_streak = 0
        except Exception as exc:  # noqa: BLE001 — recorded, sweep continues
            logger.warning("THS sweep: %s (%s) failed: %s", code, board["sector_name"], exc)
            failed.append(code)
            dead_streak += 1
            if dead_streak >= _DEAD_SOURCE_STREAK:
                logger.error(
                    "THS sweep: aborting after %d consecutive failures; "
                    "%d ok, %d boards left unmarked for retry",
                    dead_streak,
                    len(succeeded),
                    len(todo) - i - 1,
                )
                break
    flush()
    return rows, failed, succeeded
