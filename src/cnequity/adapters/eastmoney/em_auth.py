"""EastMoney HTTP client — NID cookie auth, browser headers, optional proxy.

EastMoney serves every endpoint this lake uses over ordinary TLS to anyone
with a mainland route, which is what almost every user of this project has.
The client is therefore plain ``httpx``: one connection pool, the headers the
quote page sends, and the ``nid`` cookie the datacenter endpoints want.

Users without a mainland route (overseas, some cloud egress) set
``[sources.eastmoney].proxy`` to an HTTP(S) proxy that has one. That is the
single supported lever, and it applies to every EastMoney host rather than to
one CDN. What used to live here instead — Chrome JA3 impersonation via
curl_cffi, ``CURLOPT_RESOLVE`` pinning against a ladder of DoH/dig/hardcoded
edge IPs, a sticky last-good IP persisted across runs, and a circuit breaker
to make that ladder's failure mode affordable — solved the same problem for
one person at the cost of ~500 lines that every mainland user paid to
maintain and that broke whenever EastMoney rotated an edge or a fingerprint.
"""

from __future__ import annotations

import logging
import random
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpx

if TYPE_CHECKING:
    from cnequity.config import Config

logger = logging.getLogger(__name__)

_NID_CACHE: dict = {"nid": None, "expires": 0.0}
# How long a fetched nid is reused, and how long a *failed* fetch is remembered.
# The failure side is the load-bearing one: before it existed, only a successful
# fetch set an expiry, so a route where anonflow2 refuses (403 from a non-mainland
# IP) re-attempted the handshake ahead of every single request — a 220-page sweep
# paid 220 pointless round trips. The datacenter answers fine without the cookie,
# so backing off costs nothing.
_NID_TTL_SECONDS = 300.0
_NID_FAILURE_COOLDOWN_SECONDS = 600.0

_EASTMONEY_DOMAINS = (
    "eastmoney.com",
    "datacenter-web.eastmoney.com",
    "reportapi.eastmoney.com",
    "search-api-web.eastmoney.com",
    "np-weblist.eastmoney.com",
    "anonflow2.eastmoney.com",
)
_PUSH2_DOMAINS = (
    "push2.eastmoney.com",
    "push2delay.eastmoney.com",
    "push2his.eastmoney.com",
    "91.push2his.eastmoney.com",
    "40.push2.eastmoney.com",
)
_QUOTE_REFERER = "https://quote.eastmoney.com/"
# Front-end token the push2 quote APIs require; public, stable across sessions.
# Requests without it are rejected, so it is injected for every push2 host
# rather than repeated at each of the dozen call sites.
_PUSH2_UT = "fa5fd1943c7b386f172d6893dbfba10b"
_CHROME_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)


def fetch_nid(client: httpx.Client | None = None) -> str:
    """Handshake for the ``nid`` cookie the datacenter endpoints like to see.

    Pass the caller's client whenever there is one: anonflow2 is an EastMoney
    host like any other, so a bare client here would leave the proxy that makes
    the rest of the traffic reachable and get refused on its own.
    """
    url = "https://anonflow2.eastmoney.com/backend/api/webreport"
    payload = {
        "deviceType": "web",
        "browser": "Chrome",
        "os": "Windows",
        "screen": "1920x1080",
        "canvasKey": hex(random.getrandbits(64)),
        "webglKey": hex(random.getrandbits(64)),
        "fontKey": hex(random.getrandbits(64)),
        "audioKey": hex(random.getrandbits(64)),
    }
    own = client is None
    if own:
        client = httpx.Client(timeout=10.0)
    try:
        resp = client.post(url, json=payload)
        for cookie in resp.cookies.jar:
            if cookie.name == "nid":
                return cookie.value or ""
    except Exception as exc:
        logger.debug("NID fetch failed: %s", exc)
    finally:
        if own:
            client.close()
    return ""


def get_nid(client: httpx.Client | None = None) -> str:
    """Cached nid cookie. *client* should be the caller's own — see fetch_nid."""
    now = time.time()
    if now > _NID_CACHE["expires"]:
        nid = fetch_nid(client)
        if nid:
            _NID_CACHE["nid"] = nid
            _NID_CACHE["expires"] = now + _NID_TTL_SECONDS
        else:
            # Back off on failure too, or every request retries the handshake.
            _NID_CACHE["expires"] = now + _NID_FAILURE_COOLDOWN_SECONDS
    return _NID_CACHE["nid"] or ""


def build_eastmoney_headers(url: str, client: httpx.Client | None = None) -> dict[str, str]:
    headers = {
        "User-Agent": _CHROME_UA,
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }
    if any(d in url for d in _PUSH2_DOMAINS):
        headers["Referer"] = _QUOTE_REFERER
        return headers
    if any(d in url for d in _EASTMONEY_DOMAINS):
        # Don't recurse: the nid handshake is itself an EastMoney URL.
        if "anonflow2.eastmoney.com" in url:
            return headers
        nid = get_nid(client)
        if nid:
            headers["Cookie"] = f"nid={nid}"
    return headers


def is_push2_url(url: str) -> bool:
    return any(d in url for d in _PUSH2_DOMAINS)


def apply_push2_token(url: str, params) -> dict | None:
    """Return the ``params`` to send so a push2 request carries ``ut``.

    Callers build push2 URLs both ways — a fully-formed query string and a
    ``params`` dict — so both are handled. httpx merges ``params`` into an
    existing query rather than replacing it, which is what makes returning a
    one-key dict safe for the query-string callers.
    """
    if not is_push2_url(url):
        return params
    if "ut=" in urlparse(url).query:
        return params
    if params is None:
        return {"ut": _PUSH2_UT}
    if isinstance(params, dict):
        if "ut" in params:
            return params
        return {**params, "ut": _PUSH2_UT}
    return params


def rate_limit_if_unconfigured(client: object, config: Config | None) -> None:
    """Keep caller-owned clients paced without double-throttling configured ones."""
    if config is not None and getattr(client, "config", None) is None:
        config.rate_limit("eastmoney")


def is_transport_fail_fast(exc: BaseException) -> bool:
    """True for failures that retrying the same request will not fix.

    A connect refusal, a TLS/protocol drop or a dead proxy is a property of
    the route, not of this request: retrying spends the backoff and fails
    identically. Read timeouts are excluded on purpose — those do come back
    on a second try when EastMoney is merely busy.
    """
    return isinstance(
        exc,
        (
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.ProxyError,
            httpx.RemoteProtocolError,
        ),
    )


class EastMoneyClient:
    """HTTP client with automatic EastMoney auth headers."""

    def __init__(
        self,
        min_interval: float | None = None,
        *,
        config: Config | None = None,
    ):
        # Prefer Config pacing; bare clients default to 1.0s in-process spacing.
        self.config = config
        if config is not None:
            self.min_interval = 0.0
        elif min_interval is None:
            self.min_interval = 1.0
        else:
            self.min_interval = float(min_interval)
        self._last_request = 0.0
        self._proxy = None
        timeout = 15.0
        if config is not None:
            if getattr(config, "eastmoney_proxy", None):
                self._proxy = config.eastmoney_proxy
            timeout = float(getattr(config, "eastmoney_timeout_sec", 15.0) or 15.0)
        self._timeout = timeout
        # httpx>=0.28 removed ``proxies``; older httpx still needs it. The mootdx
        # pin that forced <0.26 is gone, but the floor is still 0.25.
        client_kwargs: dict = {"timeout": timeout, "follow_redirects": True}
        if self._proxy is not None:
            client_kwargs["proxy"] = self._proxy
        try:
            self._client = httpx.Client(**client_kwargs)
        except TypeError:
            if self._proxy is not None:
                client_kwargs.pop("proxy", None)
                client_kwargs["proxies"] = self._proxy
            self._client = httpx.Client(**client_kwargs)

    def _throttle(self) -> None:
        if self.config is not None:
            self.config.rate_limit("eastmoney")
            return
        if self.min_interval <= 0:
            return
        elapsed = time.time() - self._last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request = time.time()

    def get(self, url: str, **kwargs) -> httpx.Response:
        self._throttle()
        headers = kwargs.pop("headers", {})
        headers.update(build_eastmoney_headers(url, self._client))
        params = apply_push2_token(url, kwargs.pop("params", None))
        if params is not None:
            # Merge into the URL's own query string. httpx >= 0.28 replaced the
            # URL query with `params` instead of merging, which dropped every
            # push2 clist field (clist.py builds the full query string and
            # relies on this call to add `ut`). Merge ourselves so the request
            # is identical on every httpx version.
            url = str(httpx.URL(url).copy_merge_params(params))
        return self._client.get(url, headers=headers, **kwargs)

    def post(self, url: str, **kwargs) -> httpx.Response:
        self._throttle()
        headers = kwargs.pop("headers", {})
        headers.update(build_eastmoney_headers(url, self._client))
        return self._client.post(url, headers=headers, **kwargs)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> EastMoneyClient:
        return self

    def __exit__(self, *args) -> None:
        self.close()
