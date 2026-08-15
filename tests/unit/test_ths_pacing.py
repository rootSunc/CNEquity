"""同花顺 requests must be paced per host.

`d.10jqka.com.cn` served ~1300 sequential kline requests at 1 req/s without a
failure; `q.10jqka.com.cn` started returning 401 after ~23 at the same pace.
Routing a listing request through the kline limiter is how the catalog build
gets the source to block us, so the choice is worth pinning down.
"""

from __future__ import annotations

from cn_market_lake.adapters.ths import boards


def _record_sources(monkeypatch) -> list[str]:
    seen: list[str] = []

    class Cfg:
        def rate_limit(self, source: str) -> None:
            seen.append(source)

    monkeypatch.setattr(boards, "_DEFAULT_MIN_INTERVAL", 0.0)
    monkeypatch.setattr(boards, "_DEFAULT_PAGE_MIN_INTERVAL", 0.0)
    return seen, Cfg()


def test_kline_host_uses_the_fast_limiter(monkeypatch):
    seen, cfg = _record_sources(monkeypatch)
    boards._throttle("https://d.10jqka.com.cn/v6/line/bk_881121/01/last.js", cfg)
    assert seen == ["ths"]


def test_listing_hosts_use_the_slow_limiter(monkeypatch):
    seen, cfg = _record_sources(monkeypatch)
    for url in (
        "https://q.10jqka.com.cn/thshy/",
        "https://q.10jqka.com.cn/gn/",
        "https://q.10jqka.com.cn/gn/detail/code/301558/",
    ):
        boards._throttle(url, cfg)
    assert seen == ["ths_pages"] * 3


def test_without_config_the_two_hosts_still_differ(monkeypatch):
    slept: list[float] = []
    monkeypatch.setattr(boards.time, "sleep", lambda s: slept.append(s))
    boards._throttle("https://d.10jqka.com.cn/v6/line/bk_881121/01/last.js", None)
    boards._throttle("https://q.10jqka.com.cn/thshy/", None)
    assert slept[0] == boards._DEFAULT_MIN_INTERVAL
    assert slept[1] == boards._DEFAULT_PAGE_MIN_INTERVAL
    assert slept[1] > slept[0]


def test_seed_catalog_covers_the_boards_bars_need(tmp_path):
    """A cache loss must not strand bars behind an uncooperative host.

    The seed only has to carry `sector_code` (which drives the kline URL) and
    `detail_code` (listing pages); it goes stale as boards are added, which
    costs coverage of the new board rather than correctness of the rest.
    """

    class Cfg:
        meta_root = tmp_path  # deliberately empty: no cached catalog

    seeded = boards.load_cached_catalog(Cfg())
    assert len(seeded) > 400
    assert all(b["sector_code"] and b["detail_code"] for b in seeded)
    kinds = {b["board_type"] for b in seeded}
    assert kinds == {"industry", "concept"}
    # Industry boards trade and list under the same code; concepts do not.
    industry = [b for b in seeded if b["board_type"] == "industry"]
    concept = [b for b in seeded if b["board_type"] == "concept"]
    assert all(b["sector_code"] == b["detail_code"] for b in industry)
    assert all(b["sector_code"].startswith("88") for b in seeded)
    assert any(b["sector_code"] != b["detail_code"] for b in concept)
