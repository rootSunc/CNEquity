"""Probe every public source this lake depends on, from one vantage point.

WHY THIS EXISTS. Anyone pulling A-share data — through AkShare, through a
skill file, through their own scraper — hits the same dozen endpoints, and when
one of them changes there is no place to look it up. Finding out costs an
afternoon of debugging your own code first. This lake already runs the full
sweep every trading day, so it knows; publishing what it knows costs one extra
request per source.

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

**One probe is not an SLA.** It is a single request at a single moment. A green
row means that request worked; it does not promise the next thousand will, which
for the rate-limited sources here is a genuinely different question.

Probes reuse the adapters' own URL constants and clients, so the fragile part —
EastMoney's headers, THS's pacing, the TDX wire — is the part being tested, and
an adapter that moves takes its probe with it.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from enum import Enum

from ashare_lake.config import Config

logger = logging.getLogger(__name__)

# Every probe is one request, so a slow source costs the sweep its own timeout
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
    ProbeStatus.OK: "可用",
    ProbeStatus.EMPTY: "空响应",
    ProbeStatus.BLOCKED: "被拒",
    ProbeStatus.DOWN: "不可达",
    ProbeStatus.SKIPPED: "未探测",
}

STATUS_MEANING: dict[ProbeStatus, str] = {
    ProbeStatus.OK: "返回了真实数据。",
    ProbeStatus.EMPTY: "连上了、HTTP 也正常，但没有数据——最危险的一档，回填会静默截断。",
    ProbeStatus.BLOCKED: "到达了但被拒绝（403 / 风控页 / 人机验证）。常见于非大陆出口。",
    ProbeStatus.DOWN: "连不上或超时。",
    ProbeStatus.SKIPPED: "本次未探测（配置里关掉了，或用 --only 排除）。",
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
    day = date.today() - timedelta(days=days_back)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


_BLOCKED_STATUS_CODES = frozenset({401, 403, 429, 451})


def _classify(exc: Exception) -> tuple[ProbeStatus, str]:
    """Blocked, empty or down — the distinction is the whole point of the page."""
    import httpx

    if isinstance(exc, ProbeBlocked):
        return ProbeStatus.BLOCKED, str(exc)
    if isinstance(exc, ProbeEmpty):
        return ProbeStatus.EMPTY, str(exc)
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        if code in _BLOCKED_STATUS_CODES:
            return ProbeStatus.BLOCKED, f"HTTP {code}"
        return ProbeStatus.DOWN, f"HTTP {code}"
    message = f"{type(exc).__name__}: {exc}".strip()
    return ProbeStatus.DOWN, message[:300]


# --- the probes ------------------------------------------------------------


def _probe_tdx(config: Config) -> str:
    from ashare_lake.adapters.tdx_protocol.client import fetch_daily_bars

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
        raise ProbeEmpty("行情主机连上了但没有返回 bar")
    return f"{df.height} 根日线"


def _eastmoney_json(config: Config, url: str) -> dict:
    """One EastMoney request, with the status line checked before the body.

    ``raise_for_status`` first because these hosts answer a 502 or a challenge
    with an HTML body: parsing that reports a JSON decode error and classifies
    as ``down``, hiding the status code that says which of the two it was.
    """
    from ashare_lake.adapters.eastmoney.em_auth import EastMoneyClient

    resp = EastMoneyClient(config=config).get(url)
    resp.raise_for_status()
    return resp.json()


def _probe_em_push2(config: Config) -> str:
    from ashare_lake.adapters.eastmoney.common import ALL_A_FS, PUSH2_CLIST_HOSTS

    url = (
        f"{PUSH2_CLIST_HOSTS[0]}/api/qt/clist/get"
        f"?pn=1&pz=1&po=1&np=1&fltt=2&invt=2&fid=f12&fs={ALL_A_FS}&fields=f12,f14"
    )
    payload = _eastmoney_json(config, url)
    total = int((payload.get("data") or {}).get("total") or 0)
    if not total:
        raise ProbeEmpty("clist 返回 total=0")
    return f"全市场 {total} 只"


def _probe_em_push2his(config: Config) -> str:
    from ashare_lake.adapters.eastmoney.common import PUSH2HIS_KLINE_HOSTS

    url = (
        f"{PUSH2HIS_KLINE_HOSTS[0]}/api/qt/stock/kline/get"
        "?secid=1.600519&klt=101&fqt=0&beg=0&end=20500101&lmt=5"
        "&fields1=f1,f2,f3&fields2=f51,f52,f53,f54,f55"
    )
    payload = _eastmoney_json(config, url)
    klines = (payload.get("data") or {}).get("klines") or []
    if not klines:
        raise ProbeEmpty("kline 返回空")
    return f"{len(klines)} 根 K 线"


def _probe_em_datacenter(config: Config) -> str:
    from ashare_lake.adapters.eastmoney.common import DATACENTER_BASE

    url = (
        f"{DATACENTER_BASE}?reportName=RPT_SHAREBONUS_DET"
        "&columns=SECURITY_CODE,EX_DIVIDEND_DATE&pageSize=1&pageNumber=1"
    )
    payload = _eastmoney_json(config, url)
    rows = (payload.get("result") or {}).get("data") or []
    if not rows:
        raise ProbeEmpty(f"datacenter 无数据：{payload.get('message') or 'no result'}")
    return "分红报表 1 行"


def _probe_sina(config: Config) -> str:
    from ashare_lake.adapters.sina.bars import symbol_exists

    last = symbol_exists("600519.SH")
    if last is None:
        raise ProbeEmpty("已知标的查不到任何 bar")
    return f"最新 bar {last.isoformat()}"


def _probe_cninfo(config: Config) -> str:
    import httpx

    from ashare_lake.adapters.cninfo.announcements import _CNINFO_URL

    day = _recent_weekday()
    with httpx.Client(timeout=TIMEOUT_SECONDS, headers={"User-Agent": "Mozilla/5.0"}) as client:
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
        resp.raise_for_status()
        payload = resp.json()
    total = int(payload.get("totalAnnouncement") or 0)
    if not total:
        raise ProbeEmpty(f"{day.isoformat()} 无公告返回")
    return f"{day.isoformat()} 共 {total} 条"


def _probe_ths_kline(config: Config) -> str:
    from ashare_lake.adapters.ths.boards import _get
    from ashare_lake.adapters.ths.stock_bars import _STOCK_KLINE_URL

    text = _get(
        _STOCK_KLINE_URL.format(code="600519", part="last"),
        config=config,
        timeout=TIMEOUT_SECONDS,
    )
    if '"data"' not in text:
        raise ProbeEmpty("kline 响应里没有 data 字段")
    return f"{len(text)} 字节 K 线"


def _probe_ths_pages(config: Config) -> str:
    from ashare_lake.adapters.ths.boards import _INDUSTRY_URL, _get

    text = _get(_INDUSTRY_URL, config=config, timeout=TIMEOUT_SECONDS)
    if "thshy" not in text:
        raise ProbeBlocked("行业目录页返回的不是目录（多半是风控页）")
    return f"{len(text)} 字节目录页"


def _probe_baostock(config: Config) -> str:
    from ashare_lake.adapters.baostock._session import _login, import_baostock

    bs = import_baostock()
    _login(bs)
    try:
        day = _recent_weekday()
        rs = bs.query_history_k_data_plus(
            "sh.600519",
            "date,close",
            start_date=day.isoformat(),
            end_date=day.isoformat(),
            frequency="d",
        )
        if rs.error_code != "0":
            raise ProbeBlocked(f"error_code={rs.error_code} {rs.error_msg}")
        rows = 0
        while rs.next():
            rs.get_row_data()
            rows += 1
    finally:
        bs.logout()
    if not rows:
        raise ProbeEmpty(f"{day.isoformat()} 无行情返回")
    return f"{rows} 行"


def _probe_sse(config: Config) -> str:
    from ashare_lake.adapters.exchange.st_lists import _SSE_HEADERS, SSE_URL, _client

    # Chrome impersonation, exactly as the adapter does it: the exchange serves
    # a plain httpx request an interstitial, so a probe without it would report
    # an outage that the real fetch does not have.
    resp = _client().get(
        SSE_URL, headers=_SSE_HEADERS, impersonate="chrome", timeout=TIMEOUT_SECONDS
    )
    resp.raise_for_status()
    text = resp.content.decode("gbk", "ignore")
    rows = [ln for ln in text.splitlines()[1:] if ln.split("\t")[0].strip().isdigit()]
    if not rows:
        raise ProbeBlocked("上交所清单返回的不是代码表")
    return f"{len(rows)} 只"


def _probe_szse(config: Config) -> str:
    from ashare_lake.adapters.exchange.st_lists import _SZSE_HEADERS, SZSE_URL, _client

    resp = _client().get(
        SZSE_URL, headers=_SZSE_HEADERS, impersonate="chrome", timeout=TIMEOUT_SECONDS
    )
    resp.raise_for_status()
    payload = resp.content
    # An xlsx is a zip; a WAF page is HTML. Length alone would pass either.
    if payload[:2] != b"PK":
        raise ProbeBlocked("深交所导出返回的不是 xlsx")
    return f"{len(payload)} 字节 xlsx"


def _probe_pboc(config: Config) -> str:
    from ashare_lake.adapters.pboc._tables import STATS_INDEX, get_text

    html = get_text(STATS_INDEX)
    if "社会融资" not in html:
        raise ProbeBlocked("调查统计司索引页里找不到社会融资条目")
    return f"{len(html)} 字节索引页"


def _probe_nbs(config: Config) -> str:
    from ashare_lake.adapters.nbs.pmi_release import find_latest_release

    found = find_latest_release()
    if not found:
        raise ProbeEmpty("最新发布列表里没有 PMI")
    released, _url = found
    return f"最新 PMI 发布 {released.isoformat()}"


def _probe_sw(config: Config) -> str:
    import httpx

    from ashare_lake.adapters.sw.industry_history import _HEADERS, SW_INDUSTRY_XLS_URL

    with httpx.Client(timeout=TIMEOUT_SECONDS, follow_redirects=True) as client:
        resp = client.get(SW_INDUSTRY_XLS_URL, headers=_HEADERS)
        resp.raise_for_status()
        body = resp.content
    # An XLS starts with the OLE2 magic; a WAF page starts with '<'.
    if body[:2] not in (b"\xd0\xcf", b"PK"):
        raise ProbeBlocked("申万成分下载返回的不是表格文件")
    return f"{len(body)} 字节表格"


PROBES: tuple[SourceProbe, ...] = (
    SourceProbe(
        key="tdx_protocol",
        label="通达信行情主机（TCP 7709）",
        host="tdx (TCP)",
        powers=("daily_bars", "index_bars", "minute_bars", "trade_ticks", "instruments"),
        run=_probe_tdx,
        note="二进制 TCP 协议，与所有 HTTP 源不共享风控面。海外直连通常超时。",
        blast_radius="tdx",
        config_key="tdx_protocol",
    ),
    SourceProbe(
        key="eastmoney_push2",
        label="东财 push2（实时快照 / clist）",
        host="push2.eastmoney.com",
        powers=("daily_bars", "valuation_metrics", "trading_status", "hot_rank"),
        run=_probe_em_push2,
        note="东财系共用一套风控，被封会成片失联。",
        blast_radius="eastmoney",
        config_key="eastmoney",
    ),
    SourceProbe(
        key="eastmoney_push2his",
        label="东财 push2his（历史 K 线）",
        host="push2his.eastmoney.com",
        powers=("daily_bars", "commodity_bars", "sector_bars"),
        run=_probe_em_push2his,
        note="对非大陆出口最敏感的一个；本项目为它内置了 Chrome TLS 伪装与 CDN sticky。",
        blast_radius="eastmoney",
        config_key="eastmoney",
    ),
    SourceProbe(
        key="eastmoney_datacenter",
        label="东财 datacenter（报表接口）",
        host="datacenter-web.eastmoney.com",
        powers=(
            "corporate_actions",
            "margin_trading",
            "dragon_tiger",
            "block_trades",
            "share_unlock_schedule",
        ),
        run=_probe_em_datacenter,
        note="分页返回，空页与结束页长得一样——本项目按 pages/count 对账。",
        blast_radius="eastmoney",
        config_key="eastmoney",
    ),
    SourceProbe(
        key="sina",
        label="新浪财经（日线 / 复权因子）",
        host="finance.sina.com.cn",
        powers=("daily_bars", "adj_factors", "delisting_events"),
        run=_probe_sina,
        note="复权因子唯一来源，覆盖到每只票的上市日。",
        blast_radius="sina",
        config_key="sina",
    ),
    SourceProbe(
        key="cninfo",
        label="巨潮 cninfo（公告全文索引）",
        host="www.cninfo.com.cn",
        powers=("announcement_index", "regulatory_events"),
        run=_probe_cninfo,
        note="",
        blast_radius="cninfo",
        config_key="cninfo",
    ),
    SourceProbe(
        key="ths_kline",
        label="同花顺 d.10jqka（K 线）",
        host="d.10jqka.com.cn",
        powers=("daily_bars", "sector_bars", "index_bars"),
        run=_probe_ths_kline,
        note="pre-2016 深历史与板块行情靠它；1 req/s 实测安全。",
        blast_radius="ths",
        config_key="ths",
    ),
    SourceProbe(
        key="ths_pages",
        label="同花顺 q.10jqka（板块目录页）",
        host="q.10jqka.com.cn",
        powers=("sector_bars",),
        run=_probe_ths_pages,
        note="比 K 线主机脆弱得多：实测 1 req/s 到第 23 个请求就 401。仅目录构建会碰它。",
        blast_radius="ths_pages",
        config_key="ths_pages",
    ),
    SourceProbe(
        key="baostock",
        label="baostock（估值 / ST / 退市股历史）",
        host="baostock.com",
        powers=("valuation_metrics", "trading_status", "daily_bars"),
        run=_probe_baostock,
        note="免费额度紧：实测一个会话约 43 次查询后进黑名单，冷却约 40 分钟。探测只发一次。",
        blast_radius="baostock",
        config_key="baostock",
    ),
    SourceProbe(
        key="exchange_sse",
        label="上交所官方清单",
        host="query.sse.com.cn",
        powers=("trading_status",),
        run=_probe_sse,
        note="交易所官方口径，与东财不同风控面，可作备源。",
        blast_radius="exchange",
        config_key="exchange",
    ),
    SourceProbe(
        key="exchange_szse",
        label="深交所官方清单",
        host="www.szse.cn",
        powers=("trading_status",),
        run=_probe_szse,
        note="同上。",
        blast_radius="exchange",
        config_key="exchange",
    ),
    SourceProbe(
        key="sw",
        label="申万研究（行业分类历史）",
        host="www.swsresearch.com",
        powers=("industry_members", "industry_index"),
        run=_probe_sw,
        note="XLS 下载；返回 HTML 即为风控页。",
        blast_radius="sw",
        config_key="sw",
    ),
    SourceProbe(
        key="pboc",
        label="人民银行 调查统计司（社融）",
        host="www.pbc.gov.cn",
        powers=("macro_indicators",),
        run=_probe_pboc,
        note="",
        blast_radius="pboc",
        config_key="pboc",
    ),
    SourceProbe(
        key="nbs",
        label="国家统计局（PMI 发布）",
        host="www.stats.gov.cn",
        powers=("macro_indicators",),
        run=_probe_nbs,
        note="用于交叉核验东财的 PMI 取值。",
        blast_radius="nbs",
        config_key="nbs",
    ),
)

PROBES_BY_KEY = {probe.key: probe for probe in PROBES}


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
    if probe.config_key and not config.sources.get(probe.config_key, True):
        return ProbeResult(
            **base,
            status=ProbeStatus.SKIPPED.value,
            latency_ms=None,
            detail="配置里已关闭",
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


def run_probes(
    config: Config,
    *,
    vantage: str,
    only: list[str] | None = None,
) -> HealthReport:
    """Probe every source once and assemble a report for *vantage*.

    Serial rather than concurrent. These are the same hosts the daily pipeline
    depends on, and firing fourteen requests at once is how a health check earns
    the lake a rate-limit ban — which would be the check causing the outage it
    is meant to observe.
    """
    from importlib.metadata import PackageNotFoundError, version

    # `only is None` means "everything"; an empty list means "nothing". Treating
    # the two alike would turn `--only ""` into a full sweep of every source,
    # which is the opposite of what anyone typing it wants.
    selected = (
        PROBES if only is None else tuple(PROBES_BY_KEY[k] for k in only if k in PROBES_BY_KEY)
    )
    try:
        pkg_version = version("ashare-lake")
    except PackageNotFoundError:  # pragma: no cover — source checkout
        pkg_version = "unknown"

    return HealthReport(
        vantage=vantage,
        # UTC with an explicit offset: the page is read from several timezones
        # and a bare local timestamp is unreadable in all but one of them.
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        version=pkg_version,
        results=[run_probe(probe, config) for probe in selected],
    )
