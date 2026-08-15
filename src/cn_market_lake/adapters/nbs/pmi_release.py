"""国家统计局 — 制造业 PMI from the monthly 采购经理指数 release.

The NBS query API (``data.stats.gov.cn/easyquery.htm``) would be the obvious
source and is deliberately *not* used: it answers 403 with a WAF ``UrlACL`` body
from non-mainland egress, while the site root and the release pages under
``www.stats.gov.cn`` answer normally. Building the check on the blocked path
would leave it silently dead for exactly the users least able to diagnose it —
the same trap the ths adapter documents for EastMoney ``push2his``.

So this reads the release instead. The number lives in one sentence the NBS has
phrased the same way for years::

    7月份，制造业采购经理指数（PMI）为49.2%，比上月下降1.1个百分点

which is enough for what this is for: confirming that the value already in
curated matches what the statistics bureau published. It reads the newest
release only — the point is to catch a vendor drifting from the publisher, and a
vendor that is right this month having been wrong two years ago is not the
failure being guarded against.
"""

from __future__ import annotations

import logging
import re
from datetime import date

logger = logging.getLogger(__name__)

BASE = "https://www.stats.gov.cn"
RELEASE_INDEX = f"{BASE}/sj/zxfb/"
_TIMEOUT_SECONDS = 45.0

_RELEASE_LINK_RE = re.compile(
    r"""href=["'](\.?/?[^"']*?/t\d{8}_\d+\.html)["'][^>]*>\s*([^<]{6,80}采购经理指数[^<]{0,40})</a>"""
)
_TITLE_MONTH_RE = re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月")

# `非制造业采购经理指数` contains the manufacturing phrase as a substring, so the
# lookbehind is what keeps this from reading the services index instead.
_PMI_RE = re.compile(r"(?<!非)制造业采购经理指数（PMI）为([\d.]+)%")
_TAG_RE = re.compile(r"<[^>]+>")
_SCRIPT_RE = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S)
_SPACE_RE = re.compile(r"[\s　\xa0]+")


def _get(url: str) -> str:
    from curl_cffi import requests as cr

    resp = cr.get(url, impersonate="chrome", timeout=_TIMEOUT_SECONDS)
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp.text


def _plain_text(html: str) -> str:
    """Tag-stripped text with *all* whitespace removed.

    Markup splits the sentence into fragments (``制造业采购经理指数（ PMI ）为
    49.2%``), so collapsing spaces is not enough — they have to go entirely for
    the pattern to match.
    """
    return _SPACE_RE.sub("", _TAG_RE.sub(" ", _SCRIPT_RE.sub("", html)))


def find_latest_release(index_html: str | None = None) -> tuple[date, str] | None:
    """Newest 采购经理指数 release as ``(observation month end, absolute url)``."""
    html = index_html if index_html is not None else _get(RELEASE_INDEX)
    best: tuple[date, str] | None = None
    for href, title in _RELEASE_LINK_RE.findall(html):
        match = _TITLE_MONTH_RE.search(_SPACE_RE.sub("", title))
        if not match:
            continue
        year, month = int(match.group(1)), int(match.group(2))
        if not 1 <= month <= 12:
            continue
        import calendar

        obs = date(year, month, calendar.monthrange(year, month)[1])
        url = href if href.startswith("http") else f"{RELEASE_INDEX}{href.lstrip('./')}"
        if best is None or obs > best[0]:
            best = (obs, url)
    return best


def parse_pmi(release_html: str) -> float | None:
    match = _PMI_RE.search(_plain_text(release_html))
    return float(match.group(1)) if match else None


def fetch_latest_pmi(*, config=None) -> dict | None:
    """``{"obs_date", "value", "url"}`` for the newest published month, or None.

    Never raises: this backs a cross-check, and a statistics-bureau page being
    unreachable is not a reason to fail a data run.
    """
    if config is not None:
        config.rate_limit("nbs")
    try:
        latest = find_latest_release()
        if latest is None:
            logger.warning("NBS: no 采购经理指数 release found on %s", RELEASE_INDEX)
            return None
        obs_date, url = latest
        value = parse_pmi(_get(url))
    except Exception as exc:
        logger.warning("NBS PMI release unavailable: %s", exc)
        return None

    if value is None:
        logger.warning("NBS PMI release %s parsed to no value; wording may have changed", url)
        return None
    return {"obs_date": obs_date, "value": value, "url": url}
