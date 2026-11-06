"""Probe selected public source routes this lake depends on, from one vantage point.

WHY THIS EXISTS. Anyone pulling A-share data — through AkShare, through a
skill file, through their own scraper — hits many of the same endpoints, and
when one of them changes there is no place to look it up. Validated ingest
can provide passive evidence; the scheduled stale-only pass probes routine
endpoints only when there is no recent matching evidence.

**HTTP 200 is not "up".** EastMoney answers a challenge page with 200, Sina
answers an unknown symbol with an empty array, and THS answers a rate-limited
page with 200 and no data. So every probe asserts on the *payload*, not the
status line, and a source that answers politely with nothing is reported as
``empty`` rather than ``ok`` — that state is the one that silently truncates a
backfill, and it is invisible from the outside.

**Where you probe from changes the answer.** Several of these refuse non-mainland
egress at the WAF, so the same probe is honestly ``ok`` in Shanghai and
``blocked`` in Virginia. Neither reading is wrong and neither generalises, which
is why a report carries the vantage it was taken from and the page shows them
side by side rather than merging them into one verdict.

**One probe is not an SLA.** It is a bounded observation at a single moment,
potentially involving more than one wire request. A green row means that
observation worked; it does not promise the next thousand will, which
for the rate-limited sources here is a genuinely different question.

Previously challenged or cumulative-quota endpoints are manual-only. Routine
and scheduled sweeps report them as skipped unless explicitly selected.

Probes reuse the adapters' own URL constants and clients, so the fragile part —
EastMoney's headers, THS's pacing, the TDX wire — is the part being tested, and
an adapter that moves takes its probe with it.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from urllib.parse import urlparse

from cnequity.config import Config
from cnequity.domain.http_policy import SourceCoolingDown, record_http_response
from cnequity.domain.market_time import shanghai_today
from cnequity.domain.rate_limit import source_request
from cnequity.i18n import N_, Message, _
from cnequity.storage.atomic import write_json_atomic

logger = logging.getLogger(__name__)

# Each probe is bounded, so a slow source costs the sweep its own timeout
# and nothing else. Short enough that a hung host cannot stall a scheduled run,
# long enough that a merely sluggish one is not reported as down.
TIMEOUT_SECONDS = 20.0


class ProbeStatus(str, Enum):
    OK = "ok"
    EMPTY = "empty"
    BLOCKED = "blocked"
    DOWN = "down"
    SKIPPED = "skipped"


STATUS_LABELS: dict[ProbeStatus, str] = {
    ProbeStatus.OK: N_("available"),
    ProbeStatus.EMPTY: N_("empty response"),
    ProbeStatus.BLOCKED: N_("refused"),
    ProbeStatus.DOWN: N_("unreachable"),
    ProbeStatus.SKIPPED: N_("not probed"),
}

STATUS_MEANING: dict[ProbeStatus, str] = {
    ProbeStatus.OK: N_("Returned real data."),
    ProbeStatus.EMPTY: N_(
        "Connected and HTTP was fine, but no data — the most dangerous state: a backfill silently truncates."
    ),
    ProbeStatus.BLOCKED: N_(
        "Reached but refused (403 / risk-control page / CAPTCHA). Common from outside mainland China."
    ),
    ProbeStatus.DOWN: N_("Could not connect, or timed out."),
    ProbeStatus.SKIPPED: N_(
        "Not probed this time (disabled in the config, excluded by --only, or a costly endpoint that must be selected explicitly)."
    ),
}


class ProbeBlocked(RuntimeError):
    """Reached the host and was refused — a different fact from unreachable."""


class ProbeEmpty(RuntimeError):
    """Answered without error and without data."""


@dataclass(frozen=True)
class SourceProbe:
    key: str
    label: str
    host: str
    powers: tuple[str, ...]
    run: Callable[[Config], str]
    note: str = ""
    # Sources sharing a WAF fail together, so a page that groups by it can say
    # "all of EastMoney is out" instead of listing six independent-looking rows.
    blast_radius: str = ""
    config_key: str = ""
    # Expensive or previously challenged endpoints require an explicit --only.
    manual_only: bool = False


@dataclass
class ProbeResult:
    key: str
    label: str
    host: str
    powers: list[str]
    status: str
    latency_ms: int | None
    detail: str
    note: str = ""
    blast_radius: str = ""
    sample_kind: str = "active"


@dataclass
class HealthReport:
    vantage: str
    generated_at: str
    version: str
    results: list[ProbeResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "vantage": self.vantage,
            "generated_at": self.generated_at,
            "version": self.version,
            "results": [asdict(r) for r in self.results],
        }

    @classmethod
    def from_dict(cls, raw: dict) -> HealthReport:
        return cls(
            vantage=str(raw.get("vantage") or "unknown"),
            generated_at=str(raw.get("generated_at") or ""),
            version=str(raw.get("version") or ""),
            results=[ProbeResult(**r) for r in raw.get("results") or []],
        )


# --- helpers ---------------------------------------------------------------


def _recent_weekday(days_back: int = 3) -> date:
    """A date recent enough to be served, old enough to have closed.

    Deliberately not "today": several of these endpoints hold nothing for the
    current session until after the close, and a probe that goes red every
    morning teaches people to ignore it.
    """
    day = shanghai_today() - timedelta(days=days_back)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


_BLOCKED_STATUS_CODES = frozenset({401, 403, 412, 429, 451, 456})


def _detail(exc: Exception) -> str:
    """The exception's text, kept as a `Message` when it is one.

    A `Message` is its English text, which is what the stored report gets; the
    CLI shows the same detail in the reader's language.
    """
    return exc.args[0] if len(exc.args) == 1 and isinstance(exc.args[0], Message) else str(exc)


def _classify(exc: Exception) -> tuple[ProbeStatus, str]:
    """Blocked, empty or down — the distinction is the whole point of the page."""
    import httpx

    from cnequity.adapters.eastmoney.host_guard import EastMoneyHostBlockedError

    if isinstance(exc, (ProbeBlocked, SourceCoolingDown, EastMoneyHostBlockedError)):
        return ProbeStatus.BLOCKED, _detail(exc)
    if isinstance(exc, ProbeEmpty):
        return ProbeStatus.EMPTY, _detail(exc)
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        if code in _BLOCKED_STATUS_CODES:
            return ProbeStatus.BLOCKED, f"HTTP {code}"
        return ProbeStatus.DOWN, f"HTTP {code}"
    message = f"{type(exc).__name__}: {exc}".strip()
    return ProbeStatus.DOWN, message[:300]


# --- the probes ------------------------------------------------------------


def _probe_tdx(config: Config) -> str:
    from cnequity.adapters.tdx_protocol.client import fetch_daily_bars

    end = _recent_weekday()
    df = fetch_daily_bars(
        ["600519.SH"],
        end - timedelta(days=10),
        end,
        rate_limit=config.tdx_rate_limit_spec(),
        allow_mock=False,
        config=config,
    )
    if df.is_empty():
        raise ProbeEmpty(Message(N_("The quote host connected but returned no bars")))
    return Message(N_("%d daily bars"), df.height)


def _eastmoney_json(config: Config, url: str) -> dict:
    """One EastMoney request, with the status line checked before the body.

    ``raise_for_status`` first because these hosts answer a 502 or a challenge
    with an HTML body: parsing that reports a JSON decode error and classifies
    as ``down``, hiding the status code that says which of the two it was.
    """
    from cnequity.adapters.eastmoney.em_auth import EastMoneyClient

    resp = EastMoneyClient(config=config).get(url)
    resp.raise_for_status()
    return resp.json()


def _probe_em_push2(config: Config) -> str:
    """Walk the clist host list, exactly as ``fetch_clist_pages`` does.

    Probing only the first host reported this source down while every clist
    sweep in the pipeline was succeeding on a later one: push2 answers 502
    while push2delay serves the same query. A probe that is redder than the
    pipeline sends people chasing an outage that is not there.
    """
    from cnequity.adapters.eastmoney.common import ALL_A_FS, PUSH2_CLIST_HOSTS

    last_exc: Exception | None = None
    for host in PUSH2_CLIST_HOSTS:
        url = (
            f"{host}/api/qt/clist/get"
            f"?pn=1&pz=1&po=1&np=1&fltt=2&invt=2&fid=f12&fs={ALL_A_FS}&fields=f12,f14"
        )
        try:
            payload = _eastmoney_json(config, url)
        except Exception as exc:  # noqa: BLE001 — alternate host only for non-refusals
            if _classify(exc)[0] == ProbeStatus.BLOCKED:
                raise
            last_exc = exc
            continue
        total = int((payload.get("data") or {}).get("total") or 0)
        if not total:
            last_exc = ProbeEmpty(Message(N_("clist returned total=0")))
            continue
        suffix = (
            ""
            if host == PUSH2_CLIST_HOSTS[0]
            else Message(N_(" (via %s)"), urlparse(host).hostname)
        )
        return Message(N_("%d symbols market-wide%s"), total, suffix)
    raise last_exc if last_exc else ProbeEmpty(Message(N_("clist has no usable host")))


def _probe_em_push2his(config: Config) -> str:
    from cnequity.adapters.eastmoney.common import PUSH2HIS_KLINE_HOSTS

    url = (
        f"{PUSH2HIS_KLINE_HOSTS[0]}/api/qt/stock/kline/get"
        "?secid=1.600519&klt=101&fqt=0&beg=0&end=20500101&lmt=5"
        "&fields1=f1,f2,f3&fields2=f51,f52,f53,f54,f55"
    )
    payload = _eastmoney_json(config, url)
    klines = (payload.get("data") or {}).get("klines") or []
    if not klines:
        raise ProbeEmpty(Message(N_("kline returned nothing")))
    return Message(N_("%d K-lines"), len(klines))


def _probe_em_datacenter(config: Config) -> str:
    from cnequity.adapters.eastmoney.common import DATACENTER_BASE

    url = (
        f"{DATACENTER_BASE}?reportName=RPT_SHAREBONUS_DET"
        "&columns=SECURITY_CODE,EX_DIVIDEND_DATE&pageSize=1&pageNumber=1"
    )
    payload = _eastmoney_json(config, url)
    rows = (payload.get("result") or {}).get("data") or []
    if not rows:
        raise ProbeEmpty(
            Message(N_("datacenter has no data: %s"), payload.get("message") or "no result")
        )
    return Message(N_("1 row of the dividend report"))


def _probe_sina(config: Config) -> str:
    from cnequity.adapters.sina.bars import symbol_exists

    last = symbol_exists("600519.SH", config=config)
    if last is None:
        raise ProbeEmpty(Message(N_("No bars found for a known symbol")))
    return Message(N_("latest bar %s"), last.isoformat())


def _probe_sina_futures(config: Config) -> str:
    """Sina's futures host, which is not the host `_probe_sina` measures.

    `commodity_bars` — domestic main-continuous and offshore alike — is served
    by `stock2.finance.sina.com.cn`, while the daily-bar path uses
    `money.finance.sina.com.cn`. Reachability is per host, not per vendor: this
    project already learned that from EastMoney, where `push2his` was dropping
    every connection while `push2` kept serving. Until this probe existed the
    only endpoint declaring `commodity_bars` was EastMoney's history host —
    the very one Sina replaced for being unreliable — so a substitution report
    called the dataset stranded while its actual source answered in 2.9s.
    """
    from datetime import timedelta

    from cnequity.adapters.sina.domestic_futures import (
        DOMESTIC_CONTRACTS,
        fetch_domestic_commodity_bars_range,
    )

    end = _recent_weekday()
    # One contract, one request: the vendor bans by account, not by endpoint,
    # so a health check must not spend the budget the sweep needs.
    frame = fetch_domestic_commodity_bars_range(
        end - timedelta(days=10),
        end,
        contracts=DOMESTIC_CONTRACTS[:1],
        config=config,
    )
    if frame.is_empty():
        raise ProbeEmpty(Message(N_("The continuous contract returned no bars")))
    return Message(N_("%s: latest %d daily bars"), DOMESTIC_CONTRACTS[0][1], frame.height)


def _probe_futures_exchange(exchange: str, reader=None) -> Callable[[Config], str]:
    """One exchange's futures daily file, with at most two session attempts.

    A probe must not download option and reference files as a side effect.
    Two recent weekdays tolerate a single unpublished session; longer holiday
    closures are reported as empty rather than spending more requests.
    """

    def probe(config: Config) -> str:
        from datetime import timedelta

        from cnequity.adapters.futures_exchange.common import (
            FuturesDayUnavailable,
            FuturesSourceBlocked,
        )
        from cnequity.steps.derivatives import READERS

        day = _recent_weekday()
        for _attempt in range(2):
            try:
                parsed = (reader or READERS[exchange]).fetch(day, kind="futures", config=config)
            except FuturesSourceBlocked as exc:
                raise ProbeBlocked(str(exc)) from exc
            except FuturesDayUnavailable:
                day -= timedelta(days=1)
                while day.weekday() >= 5:
                    day -= timedelta(days=1)
                continue
            if parsed.futures.is_empty():
                raise ProbeEmpty(Message(N_("%s futures file has no contracts"), day.isoformat()))
            extra = (
                Message(N_(", %d option contracts"), parsed.options.height)
                if not parsed.options.is_empty()
                else ""
            )
            return Message(
                N_("%s: %d futures contracts%s"), day.isoformat(), parsed.futures.height, extra
            )
        raise ProbeEmpty(
            Message(
                N_("No futures daily quote file was fetched for either of the last two weekdays")
            )
        )

    return probe


def _probe_dce_official(config: Config) -> str:
    """DCE's own endpoint, which the default route avoids because it is blocked.

    Kept as a probe so the answer is measured on each machine rather than
    assumed: `[futures] dce_route = "official"` is only worth choosing where
    this is green; a green probe still does not validate its field mapping.
    """
    from cnequity.steps.derivatives import DCE_OFFICIAL

    probe = _probe_futures_exchange("DCE", reader=DCE_OFFICIAL)
    return probe(config)


def _probe_cninfo(config: Config) -> str:
    import httpx

    from cnequity.adapters.cninfo.announcements import _CNINFO_URL

    day = _recent_weekday()
    with httpx.Client(timeout=TIMEOUT_SECONDS, headers={"User-Agent": "Mozilla/5.0"}) as client:
        with source_request(config, "cninfo"):
            resp = client.post(
                _CNINFO_URL,
                data={
                    "pageNum": 1,
                    "pageSize": 5,
                    "column": "szse",
                    "tabName": "fulltext",
                    "seDate": f"{day.isoformat()}~{day.isoformat()}",
                },
            )
            record_http_response(config, "cninfo", resp)
        resp.raise_for_status()
        payload = resp.json()
    total = int(payload.get("totalAnnouncement") or 0)
    if not total:
        raise ProbeEmpty(Message(N_("%s returned no announcements"), day.isoformat()))
    return Message(N_("%s: %d in total"), day.isoformat(), total)


def _probe_ths_kline(config: Config) -> str:
    from cnequity.adapters.ths.boards import _get
    from cnequity.adapters.ths.stock_bars import _STOCK_KLINE_URL

    text = _get(
        _STOCK_KLINE_URL.format(code="600519", part="last"),
        config=config,
        timeout=TIMEOUT_SECONDS,
    )
    if '"data"' not in text:
        raise ProbeEmpty(Message(N_("The kline response has no data field")))
    return Message(N_("%d bytes of K-lines"), len(text))


def _probe_ths_pages(config: Config) -> str:
    from cnequity.adapters.ths.boards import _INDUSTRY_URL, _get

    text = _get(_INDUSTRY_URL, config=config, timeout=TIMEOUT_SECONDS)
    if "thshy" not in text:
        raise ProbeBlocked(
            Message(
                N_(
                    "The industry directory page is not a directory (most likely a risk-control page)"
                )
            )
        )
    return Message(N_("%d-byte directory page"), len(text))


def _probe_baostock(config: Config) -> str:
    from cnequity.adapters.baostock._session import (
        _finish_session,
        _login,
        check_result,
        import_baostock,
    )
    from cnequity.adapters.baostock.access import hold_baostock_connection
    from cnequity.domain.http_policy import SourceCoolingDown

    bs = import_baostock()
    blacklisted = False
    with hold_baostock_connection(config):
        _login(bs, config=config)
        try:
            day = _recent_weekday()
            with source_request(config, "baostock"):
                rs = check_result(
                    bs.query_history_k_data_plus(
                        "sh.600519",
                        "date,close",
                        start_date=day.isoformat(),
                        end_date=day.isoformat(),
                        frequency="d",
                    ),
                    config=config,
                )
            if rs.error_code != "0":
                raise ProbeBlocked(f"error_code={rs.error_code} {rs.error_msg}")
            rows = 0
            while rs.next():
                rs.get_row_data()
                rows += 1
        except SourceCoolingDown:
            blacklisted = True
            raise
        finally:
            _finish_session(bs, config=config, blacklisted=blacklisted)
    if not rows:
        raise ProbeEmpty(Message(N_("%s returned no quotes"), day.isoformat()))
    return Message(N_("%d rows"), rows)


def _probe_bse(config: Config) -> str:
    """Check the first board page without consuming a full ~30-page sweep."""
    import httpx

    from cnequity.adapters.bse.daily_quotes import (
        _HEADERS,
        _QUOTATION_API,
        _QUOTATION_PAGE,
        _parse_page,
        _request_data,
    )

    with httpx.Client(timeout=TIMEOUT_SECONDS, follow_redirects=False, headers=_HEADERS) as client:
        with source_request(config, "bse"):
            landing = client.get(_QUOTATION_PAGE)
            record_http_response(config, "bse", landing)
        if landing.status_code not in (301, 302, 307, 308):
            landing.raise_for_status()
        with source_request(config, "bse"):
            response = client.post(_QUOTATION_API, data=_request_data(0))
            record_http_response(config, "bse", response, expected_json=True)
        response.raise_for_status()
        rows, total = _parse_page(response.text)
    if not rows or total <= 0:
        raise ProbeEmpty(Message(N_("The first page has no quote rows")))
    return Message(
        N_(
            "%d rows on the first page, %d claimed for the whole board (remaining pages not checked)"
        ),
        len(rows),
        total,
    )


def _probe_sse(config: Config) -> str:
    from cnequity.adapters.exchange.st_lists import _SSE_HEADERS, SSE_URL, _client

    # Chrome impersonation, exactly as the adapter does it: the exchange serves
    # a plain httpx request an interstitial, so a probe without it would report
    # an outage that the real fetch does not have.
    with source_request(config, "exchange"):
        resp = _client().get(
            SSE_URL, headers=_SSE_HEADERS, impersonate="chrome", timeout=TIMEOUT_SECONDS
        )
        record_http_response(config, "exchange", resp)
    resp.raise_for_status()
    text = resp.content.decode("gbk", "ignore")
    rows = [ln for ln in text.splitlines()[1:] if ln.split("\t")[0].strip().isdigit()]
    if not rows:
        raise ProbeBlocked(Message(N_("The SSE listing did not return a code table")))
    return Message(N_("%d symbols"), len(rows))


def _probe_szse(config: Config) -> str:
    from cnequity.adapters.exchange.st_lists import _SZSE_HEADERS, SZSE_URL, _client

    with source_request(config, "exchange"):
        resp = _client().get(
            SZSE_URL, headers=_SZSE_HEADERS, impersonate="chrome", timeout=TIMEOUT_SECONDS
        )
        record_http_response(config, "exchange", resp)
    resp.raise_for_status()
    payload = resp.content
    # An xlsx is a zip; a WAF page is HTML. Length alone would pass either.
    if payload[:2] != b"PK":
        raise ProbeBlocked(Message(N_("The SZSE export did not return an xlsx")))
    return Message(N_("%d bytes of xlsx"), len(payload))


def _probe_pboc(config: Config) -> str:
    from cnequity.adapters.pboc._tables import STATS_INDEX, get_text

    html = get_text(STATS_INDEX, config=config)
    if "社会融资" not in html:  # i18n-ignore — text on the PBOC page
        raise ProbeBlocked(
            Message(
                N_(
                    "No total-social-financing entry on the Survey and Statistics Department index page"
                )
            )
        )
    return Message(N_("%d-byte index page"), len(html))


def _probe_nbs(config: Config) -> str:
    from cnequity.adapters.nbs.pmi_release import find_latest_release

    found = find_latest_release(config=config)
    if not found:
        raise ProbeEmpty(Message(N_("No PMI in the latest releases list")))
    released, _url = found
    return Message(N_("latest PMI release %s"), released.isoformat())


def _probe_sw(config: Config) -> str:
    # Same client as the adapter, so the probe carries the intermediate cert
    # swsresearch.com omits. Building a bare httpx.Client here reported the
    # source down while the backfill would have worked.
    from cnequity.adapters.sw.industry_history import (
        _HEADERS,
        SW_INDUSTRY_XLS_URL,
        sw_client,
    )

    with sw_client(timeout=TIMEOUT_SECONDS) as client:
        with source_request(config, "sw"):
            resp = client.get(SW_INDUSTRY_XLS_URL, headers=_HEADERS)
            record_http_response(config, "sw", resp)
        resp.raise_for_status()
        body = resp.content
    # An XLS starts with the OLE2 magic; a WAF page starts with '<'.
    if body[:2] not in (b"\xd0\xcf", b"PK"):
        raise ProbeBlocked(Message(N_("The SW constituent download did not return a spreadsheet")))
    return Message(N_("%d-byte spreadsheet"), len(body))


def _probe_cni(config: Config) -> str:
    from cnequity.adapters.cni.index_constituents_history import fetch_cni_index_adjustments

    rows = fetch_cni_index_adjustments("399001.SZ", config=config)
    if rows.is_empty():
        raise ProbeEmpty(Message(N_("The CNI historical constituent file has no valid rows")))
    return Message(N_("399001.SZ historical constituents: %d rows"), rows.height)


PROBES: tuple[SourceProbe, ...] = (
    SourceProbe(
        key="tdx_protocol",
        label=N_("TDX quote hosts (TCP 7709)"),
        host="tdx (TCP)",
        powers=(
            "daily_bars",
            "index_bars",
            "minute_bars",
            "minute_bars_5m",
            "trade_ticks",
            "instruments",
            "corporate_actions",
            "trading_calendar",
        ),
        run=_probe_tdx,
        note=N_(
            "Binary TCP protocol; shares no risk-control surface with any HTTP source. Direct connections from abroad usually time out."
        ),
        blast_radius="tdx",
        config_key="tdx_protocol",
    ),
    SourceProbe(
        key="eastmoney_push2",
        label=N_("EastMoney push2 (live snapshot / clist)"),
        host="push2.eastmoney.com",
        powers=("daily_bars", "valuation_metrics", "trading_status", "hot_rank"),
        run=_probe_em_push2,
        note=N_("EastMoney hosts share one risk control; a ban takes them out together."),
        blast_radius="eastmoney",
        config_key="eastmoney",
    ),
    SourceProbe(
        key="eastmoney_push2his",
        label=N_("EastMoney push2his (historical K-lines)"),
        host="push2his.eastmoney.com",
        powers=("daily_bars", "commodity_bars", "sector_bars"),
        run=_probe_em_push2his,
        note=N_(
            "Shares egress protection with push2; on a refusal the affected sources pause, keep their state, and are re-verified on a small scale after the cooldown."
        ),
        blast_radius="eastmoney",
        config_key="eastmoney",
    ),
    SourceProbe(
        key="eastmoney_datacenter",
        label=N_("EastMoney datacenter (report API)"),
        host="datacenter-web.eastmoney.com",
        powers=(
            "corporate_actions",
            "margin_trading",
            "dragon_tiger",
            "block_trades",
            "share_unlock_schedule",
        ),
        run=_probe_em_datacenter,
        note=N_(
            "Paginated, and an empty page looks just like the last page — this project reconciles by pages/count."
        ),
        blast_radius="eastmoney",
        config_key="eastmoney",
    ),
    SourceProbe(
        key="sina",
        label=N_("Sina Finance (daily bars / adjustment factors)"),
        host="finance.sina.com.cn",
        powers=("daily_bars", "adj_factors", "delisting_events"),
        run=_probe_sina,
        note=N_(
            "The only source of adjustment factors, covering every stock back to its listing date."
        ),
        blast_radius="sina",
        config_key="sina",
    ),
    SourceProbe(
        key="sina_futures",
        label=N_("Sina futures (continuous / overseas daily bars)"),
        host="stock2.finance.sina.com.cn",
        powers=("commodity_bars", "futures_bars", "futures_contracts"),
        run=_probe_sina_futures,
        note=N_("Not the same host as Sina daily bars, but shares the same per-account 456 quota."),
        blast_radius="sina",
        config_key="sina",
    ),
    SourceProbe(
        key="shfe",
        label=N_("SHFE website (per-contract daily quotes, including INE)"),
        host="www.shfe.com.cn",
        powers=("futures_bars", "futures_contracts"),
        run=_probe_futures_exchange("SHF"),
        note=N_(
            "One request covers both SHFE and INE products; a non-trading day returns a 404 page."
        ),
        blast_radius="shfe",
        config_key="futures_exchange",
    ),
    SourceProbe(
        key="dce",
        label=N_("DCE website (per-contract daily quotes, dce_route = official)"),
        host="www.dce.com.cn",
        powers=("futures_bars", "futures_contracts"),
        run=_probe_dce_official,
        note=N_(
            "A 412 challenge was observed before, so the default route goes through Sina; explicit diagnosis only. Green proves only that the futures file is reachable, not that the official field mapping has been accepted."
        ),
        blast_radius="dce",
        config_key="futures_exchange",
        manual_only=True,
    ),
    SourceProbe(
        key="czce",
        label=N_("CZCE website (per-contract daily quotes)"),
        host="www.czce.com.cn",
        powers=("futures_bars", "futures_contracts"),
        run=_probe_futures_exchange("CZC"),
        note=N_(
            'Before 2015-10 it reads the old comma-separated archive; a non-trading day returns a "no data today" page.'
        ),
        blast_radius="czce",
        config_key="futures_exchange",
    ),
    SourceProbe(
        key="gfex",
        label=N_("GFEX website (per-contract daily quotes)"),
        host="www.gfex.com.cn",
        powers=("futures_bars", "futures_contracts"),
        run=_probe_futures_exchange("GFE"),
        note=N_('POST API; a non-trading day returns a 200 with only an all-zero "total" row.'),
        blast_radius="gfex",
        config_key="futures_exchange",
    ),
    SourceProbe(
        key="cffex",
        label=N_("CFFEX website (per-contract daily quotes)"),
        host="www.cffex.com.cn",
        powers=("futures_bars", "option_bars", "futures_contracts", "option_contracts"),
        run=_probe_futures_exchange("CFE"),
        note=N_(
            "Files the exchange publishes itself; a non-trading day returns a 302 rather than an empty file, so the probe walks back to the latest trading day."
        ),
        blast_radius="cffex",
        config_key="futures_exchange",
    ),
    SourceProbe(
        key="cninfo",
        label=N_("CNINFO (full-text announcement index)"),
        host="www.cninfo.com.cn",
        powers=("announcement_index", "regulatory_events"),
        run=_probe_cninfo,
        note="",
        blast_radius="cninfo",
        config_key="cninfo",
    ),
    SourceProbe(
        key="ths_kline",
        label=N_("THS d.10jqka (K-lines)"),
        host="d.10jqka.com.cn",
        powers=("daily_bars", "sector_bars", "index_bars"),
        run=_probe_ths_kline,
        note=N_(
            "Deep history and sector quotes depend on this endpoint; the configured interval is client-side protection, not the source's quota."
        ),
        blast_radius="ths",
        config_key="ths",
    ),
    SourceProbe(
        key="ths_pages",
        label=N_("THS q.10jqka (sector directory page)"),
        host="q.10jqka.com.cn",
        powers=("sector_bars",),
        run=_probe_ths_pages,
        note=N_(
            "The directory page has returned 401 after consecutive requests; explicit diagnosis only, and daily runs prefer the directory cache."
        ),
        blast_radius="ths_pages",
        config_key="ths_pages",
        manual_only=True,
    ),
    SourceProbe(
        key="baostock",
        label=N_("baostock (valuation / ST / delisted-stock history)"),
        host="baostock.com",
        powers=("valuation_metrics", "trading_status", "daily_bars"),
        run=_probe_baostock,
        note=N_(
            "Login, query and logout each cost a request; it has blacklisted IPs before, so by default only passive ingest evidence is used."
        ),
        blast_radius="baostock",
        config_key="baostock",
        manual_only=True,
    ),
    SourceProbe(
        key="bse",
        label=N_("BSE official quote board (first page)"),
        host="www.bse.cn",
        powers=("instruments", "trading_status", "daily_bars"),
        run=_probe_bse,
        note=N_(
            "One page handshake plus one page of quotes; proves only that the current API is reachable, not that the whole board paginates completely or any past day."
        ),
        blast_radius="bse",
        config_key="bse",
    ),
    SourceProbe(
        key="exchange_sse",
        label=N_("SSE official listing"),
        host="query.sse.com.cn",
        # Also the first link of the daily_bars failover chain: one whole-board
        # request answers for a session's worth of symbols at once.
        powers=("trading_status", "daily_bars"),
        run=_probe_sse,
        note=N_(
            "The exchange's official definition, on a different risk-control surface from EastMoney; usable as a backup source."
        ),
        blast_radius="exchange",
        config_key="exchange",
    ),
    SourceProbe(
        key="exchange_szse",
        label=N_("SZSE official listing"),
        host="www.szse.cn",
        powers=("trading_status", "daily_bars"),
        run=_probe_szse,
        note=N_("Same as above."),
        blast_radius="exchange",
        config_key="exchange",
    ),
    SourceProbe(
        key="sw",
        label=N_("SWS Research (industry classification history)"),
        host="www.swsresearch.com",
        powers=("industry_members", "industry_index"),
        run=_probe_sw,
        note=N_("XLS download; an HTML answer is a risk-control page."),
        blast_radius="sw",
        config_key="sw",
    ),
    SourceProbe(
        key="cni",
        label=N_("CNI index historical constituent files"),
        host="www.cnindex.com.cn",
        powers=("index_constituents",),
        run=_probe_cni,
        note=N_(
            "Downloads and parses the full history file; explicit diagnosis only, and daily runs prefer ingest evidence."
        ),
        blast_radius="cni",
        config_key="cni",
        manual_only=True,
    ),
    SourceProbe(
        key="pboc",
        label=N_("PBOC Survey and Statistics Department (total social financing)"),
        host="www.pbc.gov.cn",
        powers=("macro_indicators",),
        run=_probe_pboc,
        note="",
        blast_radius="pboc",
        config_key="pboc",
    ),
    SourceProbe(
        key="nbs",
        label=N_("NBS (PMI release)"),
        host="www.stats.gov.cn",
        powers=("macro_indicators",),
        run=_probe_nbs,
        note=N_("Used to cross-check EastMoney's PMI values."),
        blast_radius="nbs",
        config_key="nbs",
    ),
)

PROBES_BY_KEY = {probe.key: probe for probe in PROBES}

# Only map a validated dataset to a probe when both use the same source and
# request family. A successful EastMoney clist, for example, does not prove
# that the datacenter endpoint worked, so it cannot suppress that probe.
_PASSIVE_PROBES: dict[tuple[str, str], str] = {
    ("bse", "instruments"): "bse",
    ("bse", "trading_status"): "bse",
    ("bse", "daily_bars"): "bse",
    ("sw", "industry_members"): "sw",
    ("cni", "index_constituents"): "cni",
    ("cninfo", "announcement_index"): "cninfo",
    ("pboc", "macro_indicators"): "pboc",
    ("nbs", "macro_indicators"): "nbs",
    ("baostock", "valuation_metrics"): "baostock",
    ("baostock", "trading_status"): "baostock",
    ("sina", "daily_bars"): "sina",
    ("ths", "sector_bars"): "ths_kline",
    ("eastmoney", "financial_statement_items"): "eastmoney_datacenter",
    ("eastmoney_backfill", "financial_statement_items"): "eastmoney_datacenter",
    ("eastmoney", "share_structure"): "eastmoney_datacenter",
    ("eastmoney", "shareholder_counts"): "eastmoney_datacenter",
    ("eastmoney", "top_holders"): "eastmoney_datacenter",
    ("eastmoney_backfill", "share_structure"): "eastmoney_datacenter",
    ("eastmoney_backfill", "shareholder_counts"): "eastmoney_datacenter",
    ("eastmoney_backfill", "top_holders"): "eastmoney_datacenter",
}


def record_validated_ingest(
    config: Config, *, dataset: str, source: str, rows: int, run_id: str
) -> None:
    """Save a parsed, validated fetch as operational evidence for its endpoint."""
    key = _PASSIVE_PROBES.get((source, dataset))
    if key is None or rows <= 0:
        return
    vantage = effective_vantage(config)
    validate_vantage(vantage)
    path = config.meta_root / "source_health" / "passive" / vantage / f"{key}.json"
    write_json_atomic(
        path,
        {
            "key": key,
            "source": source,
            "dataset": dataset,
            "run_id": run_id,
            "rows": rows,
            "validated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "sample_kind": "passive",
        },
        indent=2,
    )


def _recent_passive_result(
    config: Config, probe: SourceProbe, vantage: str, *, max_age: timedelta
) -> ProbeResult | None:
    if _probe_disabled(probe, config):
        return None
    path = config.meta_root / "source_health" / "passive" / vantage / f"{probe.key}.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        when = datetime.fromisoformat(payload["validated_at"])
        if when.tzinfo is None:
            return None
        if (
            payload.get("key") != probe.key
            or payload.get("sample_kind") != "passive"
            or int(payload.get("rows", 0)) <= 0
            or not timedelta(0) <= datetime.now(timezone.utc) - when <= max_age
        ):
            return None
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None
    return ProbeResult(
        key=probe.key,
        label=probe.label,
        host=probe.host,
        powers=list(probe.powers),
        status=ProbeStatus.OK.value,
        latency_ms=None,
        detail=Message(
            N_("Ingest validation passed: %s %s rows (%s)"),
            payload["dataset"],
            payload["rows"],
            when.isoformat(),
        ),
        note=probe.note,
        blast_radius=probe.blast_radius,
        sample_kind="passive",
    )


def _probe_disabled(probe: SourceProbe, config: Config) -> bool:
    return (probe.key == "tdx_protocol" and not config.tdx_enabled) or bool(
        probe.config_key and not config.sources.get(probe.config_key, True)
    )


def run_probe(probe: SourceProbe, config: Config) -> ProbeResult:
    """Run one probe. Never raises: a failure *is* the measurement."""
    base = {
        "key": probe.key,
        "label": probe.label,
        "host": probe.host,
        "powers": list(probe.powers),
        "note": probe.note,
        "blast_radius": probe.blast_radius,
    }
    if _probe_disabled(probe, config):
        return ProbeResult(
            **base,
            status=ProbeStatus.SKIPPED.value,
            latency_ms=None,
            detail=Message(N_("Disabled in the config")),
        )

    started = time.monotonic()
    try:
        detail = probe.run(config)
        status = ProbeStatus.OK
    except Exception as exc:  # noqa: BLE001 — the failure is the result
        status, detail = _classify(exc)
        logger.info("probe %s -> %s (%s)", probe.key, status.value, detail)
    elapsed_ms = int((time.monotonic() - started) * 1000)
    return ProbeResult(**base, status=status.value, latency_ms=elapsed_ms, detail=detail)


def effective_vantage(config=None, explicit: str | None = None) -> str:
    """The egress label: ``--vantage``, then ``CNE_SOURCE_VANTAGE``, then the config, then ``local``.

    Set ``[source_health] vantage`` once and every probe, passive health record
    and scheduled maintenance run carries it; the environment variable stays for
    a one-off run from another network.
    """
    if explicit:
        return explicit
    return (
        os.environ.get("CNE_SOURCE_VANTAGE") or getattr(config, "source_vantage", None) or "local"
    )


def validate_vantage(vantage: str) -> None:
    import re

    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", vantage):
        raise ValueError(
            _(
                "vantage must be 1–64 letters, digits, dots, underscores or hyphens, "
                "starting with a letter or digit"
            )
        )


def run_probes(
    config: Config,
    *,
    vantage: str,
    only: list[str] | None = None,
    stale_only: bool = False,
    passive_max_age: timedelta = timedelta(hours=12),
) -> HealthReport:
    """Probe routine sources once and assemble a report for *vantage*.

    Serial rather than concurrent. These are the same hosts the daily pipeline
    depends on, and firing many requests at once is how a health check earns
    the lake a rate-limit ban — which would be the check causing the outage it
    is meant to observe.
    """
    from importlib.metadata import PackageNotFoundError, version

    # `only is None` means "everything"; an empty list means "nothing". Treating
    # the two alike would turn `--only ""` into a full sweep of every source,
    # which is the opposite of what anyone typing it wants.
    validate_vantage(vantage)
    unknown = sorted(set(only or []) - set(PROBES_BY_KEY))
    if unknown:
        raise ValueError(
            _("Unknown probe sources: {unknown}; see cne sources probe --list").format(
                unknown=", ".join(unknown)
            )
        )
    selected = PROBES if only is None else tuple(PROBES_BY_KEY[k] for k in dict.fromkeys(only))
    try:
        pkg_version = version("cnequity")
    except PackageNotFoundError:  # pragma: no cover — source checkout
        pkg_version = "unknown"

    def measure(probe: SourceProbe) -> ProbeResult:
        passive = (
            _recent_passive_result(config, probe, vantage, max_age=passive_max_age)
            if stale_only
            else None
        )
        if passive is not None:
            return passive
        if only is None and probe.manual_only and not _probe_disabled(probe, config):
            return ProbeResult(
                key=probe.key,
                label=probe.label,
                host=probe.host,
                powers=list(probe.powers),
                status=ProbeStatus.SKIPPED.value,
                latency_ms=None,
                detail=Message(N_("High-risk/costly endpoint; probe it explicitly with --only")),
                note=probe.note,
                blast_radius=probe.blast_radius,
            )
        return run_probe(probe, config)

    return HealthReport(
        vantage=vantage,
        # UTC with an explicit offset: the page is read from several timezones
        # and a bare local timestamp is unreadable in all but one of them.
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        version=pkg_version,
        results=[measure(probe) for probe in selected],
    )
