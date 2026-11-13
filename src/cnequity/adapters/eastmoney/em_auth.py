"""EastMoney HTTP client — NID cookie auth, browser headers, optional proxy.

EastMoney serves every endpoint this lake uses over ordinary TLS to anyone
with a mainland route, which is what almost every user of this project has.
The client is therefore plain ``httpx``: one connection pool, the headers the
quote page sends, and the ``nid`` cookie the datacenter endpoints want.

Users without a mainland route (overseas, some cloud egress) set
``[sources.eastmoney].proxy`` to an HTTP(S) proxy that has one. It applies to
every EastMoney host unless ``proxy_hosts`` names the ones that need it; the
others then connect directly (environment proxies ignored), so a datacenter
that answers directly does not fail with a proxy that is down. Deployments
where the proxy itself is less reliable for
historical K-lines may explicitly enable ``direct_fallback``; that retries only
``push2his`` once, and stays off by default so mandatory proxy policy is never
silently bypassed. What used to live here instead — Chrome JA3 impersonation via
curl_cffi, ``CURLOPT_RESOLVE`` pinning against a ladder of DoH/dig/hardcoded
edge IPs, a sticky last-good IP persisted across runs, and a circuit breaker
to make that ladder's failure mode affordable — solved the same problem for
one person at the cost of ~500 lines that every mainland user paid to
maintain and that broke whenever EastMoney rotated an edge or a fingerprint.
"""

from __future__ import annotations

import logging
import random
import socket
import threading
import time
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import httpx

from cnequity.adapters.eastmoney import host_guard
from cnequity.adapters.eastmoney.host_guard import (  # noqa: F401 — re-exported
    DatacenterBlockedError,
    DatacenterBreakerOpenError,
    DatacenterBudgetExhaustedError,
    EastMoneyHostBlockedError,
    Push2BlockedError,
    Push2BreakerOpenError,
    Push2BudgetExhaustedError,
    Push2PausedError,
)
from cnequity.domain.http_policy import record_http_response, record_request_event
from cnequity.domain.rate_limit import source_request
from cnequity.domain.request_meter import count_request

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
_DIRECT_FALLBACK_STATUS_CODES = frozenset({502, 503, 504})


NID_URL = "https://anonflow2.eastmoney.com/backend/api/webreport"


def fetch_nid(client: httpx.Client | None = None, *, config: Config | None = None) -> str:
    """Handshake for the ``nid`` cookie the datacenter endpoints like to see.

    Pass the caller's client whenever there is one: anonflow2 is an EastMoney
    host like any other, so a bare client here would leave the proxy that makes
    the rest of the traffic reachable and get refused on its own.
    """
    url = NID_URL
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
        with source_request(config, "eastmoney"):
            resp = client.post(url, json=payload)
            record_http_response(config, "eastmoney", resp, expected_json=True)
        for cookie in resp.cookies.jar:
            if cookie.name == "nid":
                return cookie.value or ""
    except Exception as exc:
        logger.debug("NID fetch failed: %s", exc)
    finally:
        if own:
            client.close()
    return ""


def get_nid(client: httpx.Client | None = None, *, config: Config | None = None) -> str:
    """Cached nid cookie. *client* should be the caller's own — see fetch_nid."""
    now = time.time()
    if now > _NID_CACHE["expires"]:
        if config is None:
            # Keep the small public helper compatible with callers that inject
            # a one-argument fetcher in tests/integrations.
            nid = fetch_nid(client)
        else:
            nid = fetch_nid(client, config=config)
        if nid:
            _NID_CACHE["nid"] = nid
            _NID_CACHE["expires"] = now + _NID_TTL_SECONDS
        else:
            # Back off on failure too, or every request retries the handshake.
            _NID_CACHE["expires"] = now + _NID_FAILURE_COOLDOWN_SECONDS
    return _NID_CACHE["nid"] or ""


def build_eastmoney_headers(
    url: str, client: httpx.Client | None = None, *, config: Config | None = None
) -> dict[str, str]:
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
        if config is None:
            # Preserve the helper's historical call shape for lightweight
            # monkeypatches while configured clients pass their limiter below.
            nid = get_nid(client)
        else:
            nid = get_nid(client, config=config)
        if nid:
            headers["Cookie"] = f"nid={nid}"
    return headers


def is_push2_url(url: str) -> bool:
    return any(d in url for d in _PUSH2_DOMAINS)


def _request_source(url: str) -> str:
    """push2 and datacenter each pace on their own lane; other hosts share one."""
    if host_guard.is_push2_url(url):
        return "eastmoney_push2"
    if host_guard.is_datacenter_url(url):
        return "eastmoney_dc"
    return "eastmoney"


def apply_push2_token(url: str, params) -> dict | None:
    """Return the ``params`` to send so a push2 request carries ``ut``.

    Callers build push2 URLs both ways — a fully-formed query string and a
    ``params`` dict — so both are handled. Returning a one-key dict is only
    safe for the query-string callers because :meth:`EastMoneyClient.get`
    merges the result into the URL itself; do not pass this straight to httpx,
    where 0.28 would replace the caller's query instead of merging into it.
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


def _proxy_accepts_connections(proxy: str) -> bool:
    """Whether a TCP connection to the proxy itself succeeds."""
    parsed = urlparse(proxy)
    default_port = {"https": 443, "socks5": 1080, "socks5h": 1080}.get(parsed.scheme, 80)
    try:
        with socket.create_connection(
            (parsed.hostname or "127.0.0.1", parsed.port or default_port), timeout=2.0
        ):
            return True
    except OSError:
        return False


# A proxy that refused a probe stays "dead" this long: every request through a
# dead proxy would otherwise pay its own probe, and a proxy that comes back
# mid-window must not have its first failures blamed on EastMoney.
_PROXY_DEAD_TTL_SECONDS = 30.0
_proxy_dead_until: dict[str, float] = {}
_proxy_state_lock = threading.Lock()


def _proxy_is_dead(proxy: str) -> bool:
    """Whether *proxy* refused a probe now or within the last TTL window."""
    with _proxy_state_lock:
        if _proxy_dead_until.get(proxy, 0.0) > time.monotonic():
            return True
    if _proxy_accepts_connections(proxy):
        return False
    with _proxy_state_lock:
        _proxy_dead_until[proxy] = time.monotonic() + _PROXY_DEAD_TTL_SECONDS
    return True


def _proxy_answered(proxy: str | None) -> None:
    """A response came back through *proxy*: it is running again."""
    if proxy is not None:
        with _proxy_state_lock:
            _proxy_dead_until.pop(proxy, None)


def is_transport_fail_fast(exc: BaseException) -> bool:
    """True for failures that retrying the same request will not fix.

    A connect refusal, a TLS/protocol drop, a dead proxy or an edge 5xx is a
    property of the route, not of this request: retrying spends the backoff
    and fails identically.

    Timeouts are included deliberately, and this is the counter-intuitive
    part. A blocked or throttled egress to EastMoney surfaces as a *timeout*,
    not a refusal, and it stays blocked for the whole run — `commodity_bars`
    measured 151 s per daily run (15 contracts x 5 attempts x backoff) to
    return nothing before the timeout case was made fail-fast. `bars.py` also
    feeds this predicate into a consecutive-failure circuit breaker, which
    only works if a dead route is reported as dead on the first request.

    This is NOT a "retry transient errors" predicate, and EastMoney's own
    transient conditions do not reach it: throttling arrives as an
    application-level busy message in the response body and is raised as
    ``_ServerBusy`` (see ``datacenter.py``), which falls through to False here
    and *is* retried with backoff. Making timeouts retryable here would
    re-open the 151 s regression; change it only with a measurement that says
    otherwise.
    """
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response is not None and exc.response.status_code >= 500
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
        timeout_sec: float | None = None,
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
        self._proxy_hosts: tuple[str, ...] = ()
        timeout = 15.0 if timeout_sec is None else float(timeout_sec)
        if config is not None:
            if getattr(config, "eastmoney_proxy", None):
                self._proxy = config.eastmoney_proxy
                self._proxy_hosts = tuple(getattr(config, "eastmoney_proxy_hosts", ()) or ())
            if timeout_sec is None:
                timeout = float(getattr(config, "eastmoney_timeout_sec", 15.0) or 15.0)
        self._timeout = timeout
        self.last_route_outcome: dict[str, object] = {}
        self._direct_client: httpx.Client | None = None
        self._direct_client_lock = threading.Lock()
        self._client = self._make_client(proxy=self._proxy)

    def _make_client(self, *, proxy: str | None, direct: bool = False) -> httpx.Client:
        # httpx>=0.28 removed ``proxies``; older httpx still needs it. The mootdx
        # pin that forced <0.26 is gone, but the floor is still 0.25.
        client_kwargs: dict = {"timeout": self._timeout, "follow_redirects": True}
        if direct:
            # A real direct retry must not inherit HTTPS_PROXY/HTTP_PROXY from
            # the process after the explicit proxy route has failed.
            client_kwargs["trust_env"] = False
        if proxy is not None:
            client_kwargs["proxy"] = proxy
        try:
            return httpx.Client(**client_kwargs)
        except TypeError:
            if proxy is not None:
                client_kwargs.pop("proxy", None)
                client_kwargs["proxies"] = proxy
            return httpx.Client(**client_kwargs)

    def _proxied(self, url: str) -> bool:
        """Whether *url* goes through the configured proxy."""
        if self._proxy is None:
            return False
        if not self._proxy_hosts:
            return True
        host = (httpx.URL(url).host or "").lower()
        return any(host == name or host.endswith("." + name) for name in self._proxy_hosts)

    def _route(self, url: str) -> httpx.Client:
        """The proxy client, or a direct one for a host left off ``proxy_hosts``.

        Without any proxy the main client keeps honouring HTTPS_PROXY; a host
        the configuration sends direct ignores it, as the fallback route does.
        """
        if self._proxy is not None and not self._proxied(url):
            return self._get_direct_client()
        return self._client

    def _can_fallback_direct(self, url: str) -> bool:
        host = (httpx.URL(url).host or "").lower()
        return bool(
            self._proxied(url)
            and getattr(self.config, "eastmoney_direct_fallback", False)
            and (host == "push2his.eastmoney.com" or host.endswith(".push2his.eastmoney.com"))
        )

    def _get_direct_client(self) -> httpx.Client:
        if self._direct_client is None:
            with self._direct_client_lock:
                if self._direct_client is None:
                    self._direct_client = self._make_client(proxy=None, direct=True)
        return self._direct_client

    def _get_direct(self, url: str, *, headers: dict[str, str], **kwargs) -> httpx.Response:
        # A second route is a second request: the breaker and the budget apply.
        host_guard.ensure_open(self.config, url)
        logger.warning("EastMoney push2his proxy route failed; retrying once via direct route")
        try:
            with source_request(self.config, _request_source(url)):
                host_guard.admit(self.config, url)
                direct_client = self._get_direct_client()
                record_request_event(self.config, _request_source(url), "fallback")
                count_request()
                response = direct_client.get(url, headers=headers, **kwargs)
                record_http_response(
                    self.config, _request_source(url), response, expected_json=True
                )
        except Exception as exc:
            self._note_outcome(url, exc=exc)
            raise
        self._note_outcome(url, status_code=response.status_code)
        return response

    def _proxy_failure(self, exc: BaseException, url: str) -> BaseException:
        """Name a refused connection to the proxy itself.

        A proxy that is not running (Clash gone after a logout) raises a plain
        ``ConnectError``, which the breaker would read as EastMoney refusing
        this IP and close every host until midnight. The proxy is probed
        directly so a genuine refusal from EastMoney is never relabelled; a
        failed probe is remembered briefly (see ``_proxy_is_dead``).
        """
        if (
            not self._proxied(url)
            or not isinstance(exc, httpx.ConnectError)
            or isinstance(exc, host_guard.EastMoneyHostBlockedError)
            or not _proxy_is_dead(self._proxy)
        ):
            return exc
        return host_guard.EastMoneyProxyUnreachableError(
            f"EastMoney proxy {self._proxy} is unreachable (is it running?): {exc}"
        )

    def _note_outcome(
        self, url: str, *, status_code: int | None = None, exc: BaseException | None = None
    ) -> None:
        host_guard.note_outcome(self.config, url, status_code=status_code, exc=exc)

    def _throttle(self) -> None:
        if self.config is not None:
            # Kept as a public compatibility hook for callers that explicitly
            # invoke it. ``get``/``post`` use source_request directly when a
            # Config is present, avoiding a duplicate pacing reservation.
            self.config.rate_limit("eastmoney")
            return
        if self.min_interval <= 0:
            return
        elapsed = time.time() - self._last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self._last_request = time.time()

    def get(self, url: str, **kwargs) -> httpx.Response:
        host_guard.ensure_open(self.config, url)
        request_event = kwargs.pop("_request_event", None)
        if self.config is None:
            # Bare clients retain their historical per-instance pacing. A
            # configured client gets both pacing and the shared lease from the
            # request context below, so calling _throttle there would reserve
            # every slot twice.
            self._throttle()
        headers = kwargs.pop("headers", {})
        headers.update(build_eastmoney_headers(url, self._route(NID_URL), config=self.config))
        params = apply_push2_token(url, kwargs.pop("params", None))
        if params is not None:
            # Merge into the URL ourselves rather than handing `params` to httpx.
            # httpx <= 0.27 merged it into an existing query; 0.28 replaced the
            # query outright. clist.py builds a fully-formed query string and
            # relies on this call only to add `ut`, so under 0.28 every clist
            # request degenerated to `?ut=...` — losing fs, fields, pn, pz and
            # fid, and silently under-fetching fund_flow, the ST board,
            # instruments, rotation and valuation. pyproject caps nothing above
            # httpx>=0.25, so a fresh `pip install cnequity` hit exactly this.
            # copy_merge_params behaves identically on both versions.
            url = str(httpx.URL(url).copy_merge_params(params))
        self.last_route_outcome = {
            "primary_route": "proxy" if self._proxied(url) else "direct",
            "proxy_failed": False,
            "direct_failed": False,
            "direct_succeeded": False,
        }
        try:
            with source_request(self.config, _request_source(url)):
                host_guard.admit(self.config, url)
                if request_event is not None:
                    record_request_event(self.config, _request_source(url), request_event)
                count_request(retry=request_event == "retry")
                response = self._route(url).get(url, headers=headers, **kwargs)
                record_http_response(
                    self.config, _request_source(url), response, expected_json=True
                )
        except (
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.ProxyError,
            httpx.RemoteProtocolError,
        ) as exc:
            failure = self._proxy_failure(exc, url)
            self.last_route_outcome["primary_error"] = type(failure).__name__
            self._note_outcome(url, exc=failure)
            if not self._can_fallback_direct(url):
                if failure is exc:
                    raise
                raise failure from exc
            self.last_route_outcome["proxy_failed"] = True
            try:
                direct = self._get_direct(url, headers=headers, **kwargs)
            except Exception as direct_exc:
                self.last_route_outcome["direct_failed"] = True
                self.last_route_outcome["direct_error"] = type(direct_exc).__name__
                raise
            self.last_route_outcome["direct_succeeded"] = True
            self.last_route_outcome["status_code"] = direct.status_code
            return direct
        except httpx.TransportError as exc:
            # ReadError / WriteError and friends: still push2 dropping us.
            self._note_outcome(url, exc=exc)
            raise
        self._note_outcome(url, status_code=response.status_code)
        if self._proxied(url):
            _proxy_answered(self._proxy)
        if response.status_code in _DIRECT_FALLBACK_STATUS_CODES and self._can_fallback_direct(url):
            self.last_route_outcome["proxy_failed"] = True
            self.last_route_outcome["primary_status_code"] = response.status_code
            try:
                direct = self._get_direct(url, headers=headers, **kwargs)
            except Exception as direct_exc:
                self.last_route_outcome["direct_failed"] = True
                self.last_route_outcome["direct_error"] = type(direct_exc).__name__
                raise
            self.last_route_outcome["direct_succeeded"] = True
            self.last_route_outcome["status_code"] = direct.status_code
            return direct
        self.last_route_outcome["status_code"] = response.status_code
        return response

    def post(self, url: str, **kwargs) -> httpx.Response:
        host_guard.ensure_open(self.config, url)
        if self.config is None:
            self._throttle()
        headers = kwargs.pop("headers", {})
        headers.update(build_eastmoney_headers(url, self._route(NID_URL), config=self.config))
        try:
            with source_request(self.config, _request_source(url)):
                host_guard.admit(self.config, url)
                response = self._route(url).post(url, headers=headers, **kwargs)
                record_http_response(
                    self.config, _request_source(url), response, expected_json=True
                )
        except Exception as exc:
            failure = self._proxy_failure(exc, url)
            self._note_outcome(url, exc=failure)
            if failure is exc:
                raise
            raise failure from exc
        self._note_outcome(url, status_code=response.status_code)
        if self._proxied(url):
            _proxy_answered(self._proxy)
        return response

    def close(self) -> None:
        self._client.close()
        if self._direct_client is not None:
            self._direct_client.close()

    def __enter__(self) -> EastMoneyClient:
        return self

    def __exit__(self, *args) -> None:
        self.close()
