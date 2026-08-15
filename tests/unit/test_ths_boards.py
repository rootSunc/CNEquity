"""Offline coverage for 同花顺 board catalog / kline parsers."""

from __future__ import annotations

from datetime import date

import pytest

from cn_market_lake.adapters.ths import boards as ths
from cn_market_lake.config import Config


def test_get_fails_fast_on_404_without_retrying(monkeypatch):
    """A missing year file for a young thematic board (芬太尼, 华为概念, ...)
    is routine — fetch_board_bars already treats it as normal and moves on —
    but retrying a 404 three times with backoff cannot make the file appear.
    Measured cost of not fixing this: a deep sweep (2010 onward) against a
    board created in, say, 2022 hits ~12 guaranteed-404 years, each costing
    ~12s to retry-and-give-up, turning one board into 15-20 minutes and
    making a 432-board sweep look hung."""
    calls = {"n": 0}
    sleeps: list[float] = []

    class Resp:
        status_code = 404

    def fake_get(url, **kwargs):
        calls["n"] += 1
        return Resp()

    monkeypatch.setattr(ths.httpx, "get", fake_get)
    monkeypatch.setattr(ths.time, "sleep", lambda s: sleeps.append(s))
    monkeypatch.setattr(ths, "_throttle", lambda url, config: None)

    with pytest.raises(ths.ThsError, match="404"):
        ths._get("https://d.10jqka.com.cn/v6/line/zs_885805/00/2010.js", config=None)

    assert calls["n"] == 1, "404 must not be retried like a transient failure"
    assert sleeps == [], "no retry means no backoff sleep either"


def test_get_still_retries_a_genuinely_transient_status(monkeypatch):
    """The 404 fix must not swallow real transient failures — a 500 (or any
    non-401/403/404 status) keeps the existing retry-with-backoff behavior."""

    class Resp:
        status_code = 500

    calls = {"n": 0}

    def fake_get(url, **kwargs):
        calls["n"] += 1
        return Resp()

    monkeypatch.setattr(ths.httpx, "get", fake_get)
    monkeypatch.setattr(ths.time, "sleep", lambda s: None)
    monkeypatch.setattr(ths, "_throttle", lambda url, config: None)

    with pytest.raises(ths.ThsError):
        ths._get("https://d.10jqka.com.cn/v6/line/zs_881121/00/2010.js", config=None)

    assert calls["n"] == ths._MAX_RETRIES


def test_unwrap_jsonp_and_errors():
    payload = ths._unwrap_jsonp('quotebridge_v6_line_bk_881121_01_2025({"data":"x"});')
    assert payload == {"data": "x"}
    with pytest.raises(ths.ThsError):
        ths._unwrap_jsonp("not-jsonp")


def test_parse_industry_dedupes():
    html = """
    <a href="/code/881121/foo">煤炭</a>
    <a href="/code/881121/bar">煤炭复制</a>
    <a href="/code/881122/baz">钢铁</a>
    """
    boards = ths._parse_industry(html)
    assert [b["sector_code"] for b in boards] == ["881121", "881122"]
    assert boards[0]["board_type"] == "industry"
    assert boards[0]["detail_code"] == "881121"


def test_parse_concept_inline_and_links():
    html = """
    <input id="gnSection" value='{"1":{"platename":"AI","platecode":"885001"},"2":{"platename":"","platecode":"885002"}}'/>
    <a href="/gn/detail/code/308814/x">人形机器人</a>
    <a href="/gn/detail/code/308814/y">人形机器人</a>
    """
    inline = ths._parse_concept_inline(html)
    assert inline == {"AI": "885001"}
    assert ths._parse_concept_inline("<div/>") == {}
    assert ths._parse_concept_inline("id=\"gnSection\" value='{bad}'") == {}
    links = ths._parse_concept_links(html)
    assert links == [("308814", "人形机器人")]


def test_parse_kline_and_change_pct():
    board = {
        "sector_code": "881121",
        "sector_name": "煤炭",
        "board_type": "industry",
    }
    rows = ths._parse_kline(
        {"data": ("20250102,10,12,9,11,1000,1500000000;bad;20250103,11,13,10,12,1100,1600000000")},
        board,
    )
    assert len(rows) == 2
    assert rows[0]["trade_date"] == date(2025, 1, 2)
    assert rows[1]["close"] == 12.0

    # Feed unsorted; first bar null change_pct, second derived.
    shuffled = [rows[1], rows[0]]
    out = ths._with_change_pct(shuffled)
    assert out[0]["change_pct"] is None
    assert out[1]["change_pct"] == pytest.approx((12 / 11 - 1) * 100, rel=1e-3)


def test_catalog_cache_roundtrip_and_seed_fallback(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    boards = [
        {
            "sector_code": "881121",
            "sector_name": "煤炭",
            "board_type": "industry",
            "detail_code": "881121",
        }
    ]
    ths._save_catalog(cfg, boards)
    assert ths.load_cached_catalog(cfg) == boards

    # Corrupt cache → seed fallback (package ships a seed).
    path = ths._catalog_path(cfg)
    path.write_text("{not-json", encoding="utf-8")
    seeded = ths.load_cached_catalog(cfg)
    assert seeded  # non-empty seed
    assert "sector_code" in seeded[0]


def test_fetch_board_bars_uses_last_window(monkeypatch):
    board = {
        "sector_code": "881121",
        "sector_name": "煤炭",
        "board_type": "industry",
        "detail_code": "881121",
    }
    calls: list[str] = []

    def fake_get(url, *, config=None, timeout=20.0):
        calls.append(url)
        return 'cb({"data":"20250102,10,12,9,11,1000,1e9;20250103,11,13,10,12,1100,1.1e9"});'

    monkeypatch.setattr(ths, "_get", fake_get)
    rows = ths.fetch_board_bars(board, date(2025, 1, 2), date(2025, 1, 3))
    assert any("/last.js" in u for u in calls)
    assert len(rows) == 2
    assert rows[0]["change_pct"] is None
    assert rows[1]["change_pct"] is not None


def test_sweep_board_bars_skip_and_abort(monkeypatch):
    boards = [
        {"sector_code": "A", "sector_name": "a", "board_type": "industry"},
        {"sector_code": "B", "sector_name": "b", "board_type": "industry"},
        {"sector_code": "C", "sector_name": "c", "board_type": "industry"},
    ]
    attempts: list[str] = []

    def fake_fetch(board, start, end, *, config=None):
        attempts.append(board["sector_code"])
        if board["sector_code"] == "B":
            raise ths.ThsError("nope")
        return [
            {
                **board,
                "trade_date": start,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
                "amount": 1.0,
                "change_pct": None,
            }
        ]

    monkeypatch.setattr(ths, "fetch_board_bars", fake_fetch)
    batches: list = []
    rows, failed, done = ths.sweep_board_bars(
        date(2025, 1, 2),
        date(2025, 1, 3),
        boards=boards,
        skip_sectors={"C"},
        on_batch=lambda batch_rows, codes: batches.append((list(codes), list(batch_rows))),
    )
    assert "A" in done
    assert "B" in failed
    assert "C" not in attempts
    # Batched callers get rows drained into on_batch; returned rows empty.
    assert rows == []
    assert batches


def test_fetch_board_catalog_refresh_and_cache(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    pages = {
        "industry": '<a href="/code/881121/x">煤炭</a>',
        "concept": (
            '<input id="gnSection" value=\'{"1":{"platename":"AI","platecode":"885001"}}\'/>'
            '<a href="/gn/detail/code/308814/x">AI</a>'
            '<a href="/gn/detail/code/308815/y">机器人</a>'
        ),
        "detail": "board index 885999 elsewhere",
    }

    def fake_get(url, *, config=None, timeout=20.0):
        if "bk_" in url or "hy" in url or "industry" in url.lower() or "881" in url:
            # Industry listing URL
            if "gn" in url or "concept" in url:
                return pages["concept"]
        if "gn" in url and "detail" in url:
            return pages["detail"]
        if "gn" in url:
            return pages["concept"]
        return pages["industry"]

    monkeypatch.setattr(ths, "_get", fake_get)
    # Point module URLs through our fake by matching substrings more simply:
    monkeypatch.setattr(ths, "_INDUSTRY_URL", "http://ths/industry")
    monkeypatch.setattr(ths, "_CONCEPT_URL", "http://ths/gn")
    monkeypatch.setattr(ths, "_CONCEPT_DETAIL_URL", "http://ths/gn/detail/code/{code}/")

    def get2(url, *, config=None, timeout=20.0):
        if url == "http://ths/industry":
            return pages["industry"]
        if url == "http://ths/gn":
            return pages["concept"]
        if "308815" in url:
            return pages["detail"]
        return ""

    monkeypatch.setattr(ths, "_get", get2)
    boards = ths.fetch_board_catalog(config=cfg, refresh=True)
    codes = {b["sector_code"] for b in boards}
    assert "881121" in codes
    assert "885001" in codes  # inline
    assert "885999" in codes  # resolved from detail page

    cached = ths.fetch_board_catalog(config=cfg, refresh=False)
    assert cached == boards


def test_sweep_aborts_on_dead_source_streak(monkeypatch):
    boards = [
        {"sector_code": f"B{i}", "sector_name": "x", "board_type": "industry"} for i in range(15)
    ]
    monkeypatch.setattr(
        ths,
        "fetch_board_bars",
        lambda *a, **k: (_ for _ in ()).throw(ths.ThsError("dead")),
    )
    monkeypatch.setattr(ths, "_DEAD_SOURCE_STREAK", 3)
    rows, failed, done = ths.sweep_board_bars(
        date(2025, 1, 2),
        date(2025, 1, 3),
        boards=boards,
    )
    assert done == []
    assert len(failed) == 3
    assert rows == []
