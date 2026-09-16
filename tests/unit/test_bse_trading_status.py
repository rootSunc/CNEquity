"""Beijing encodes a halt as an absence, so this adapter's job is mostly to
refuse to read one when it has not earned the right to."""

import json
from datetime import date

import polars as pl
import pytest

from cnequity.adapters.bse.instruments import fetch_bse_instruments
from cnequity.adapters.bse.trading_status import fetch_trading_status_bse
from cnequity.config import Config
from cnequity.domain.trading_status import STATUS_NORMAL, STATUS_SUSPENDED


class _Response:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        return None


class _Client:
    def __init__(self, pages: dict[int, str]):
        self.pages = pages
        self.posted: list[int] = []

    def get(self, url):
        return _Response("")

    def post(self, url, *, data):
        page = int(data["page"])
        self.posted.append(page)
        return _Response(self.pages[page])

    def close(self):
        return None


def _jsonp(rows, total: int):
    return "null(" + json.dumps([{"content": rows, "totalElements": total}]) + ")"


def _row(code: str, name: str, trade_date: str = "20260915"):
    return {"hqzqdm": code, "hqzqjc": name, "hqjsrq": trade_date}


def _config(tmp_path):
    return Config(data_root=tmp_path / "data", source_intervals={"bse": 0.0})


def test_a_complete_board_makes_absence_a_halt_and_reads_st_from_the_name(tmp_path):
    client = _Client({0: _jsonp([_row("920023", "*ST田野"), _row("920571", "中裕科技")], total=2)})

    result = fetch_trading_status_bse(
        ["920023.BJ", "920571.BJ", "920999.BJ"],
        date(2026, 9, 15),
        client=client,
        config=_config(tmp_path),
    )

    assert result.complete is True
    rows = {r["symbol"]: r for r in result.rows.to_dicts()}
    assert rows["920571.BJ"]["is_trading"] is True
    assert rows["920571.BJ"]["status"] == STATUS_NORMAL
    assert rows["920571.BJ"]["risk_warning"] is False
    assert rows["920023.BJ"]["risk_warning"] is True
    # Off the board: halted, and its ST designation is unknown rather than clean.
    assert rows["920999.BJ"]["is_trading"] is False
    assert rows["920999.BJ"]["status"] == STATUS_SUSPENDED
    assert rows["920999.BJ"]["risk_warning"] is None


def test_an_incomplete_walk_judges_nobody_absent(tmp_path):
    """A short read looks exactly like a board full of halts.

    The walk paginates by page offset, so an under-filled page ends it cleanly
    at the right page having collected far too few rows — no error to catch,
    just a board that is quietly missing most of the exchange. Only counting
    the rows against the advertised total sees it.
    """
    client = _Client(
        {
            0: _jsonp([_row("920571", "中裕科技")], total=21),
            1: _jsonp([_row("920572", "国义招标")], total=21),
        }
    )

    result = fetch_trading_status_bse(
        ["920571.BJ", "920999.BJ"], date(2026, 9, 15), client=client, config=_config(tmp_path)
    )

    assert client.posted == [0, 1]
    assert result.complete is False
    assert result.rows["symbol"].to_list() == ["920571.BJ"]


def test_a_board_still_showing_the_previous_session_halts_nobody(tmp_path):
    """The board is a snapshot of the latest session, not of a session we name.

    Run before Beijing publishes and the walk completes perfectly over
    yesterday's rows: every row is dropped as off-session, every name in scope
    becomes "absent", and without this guard the exchange is declared suspended
    in full.
    """
    client = _Client({0: _jsonp([_row("920571", "中裕科技", trade_date="20260914")], total=1)})

    result = fetch_trading_status_bse(
        ["920571.BJ", "920999.BJ"], date(2026, 9, 15), client=client, config=_config(tmp_path)
    )

    assert result.complete is True
    assert result.listed == frozenset()
    assert result.rows.is_empty()


def test_scope_is_what_gets_judged_not_the_whole_board(tmp_path):
    """Retired Beijing codes are absent from the board for good — 241 of them
    after the 2025 renumbering — so a caller that hands this the catalogued
    universe instead of the live one would manufacture permanent halts."""
    client = _Client({0: _jsonp([_row("920571", "中裕科技")], total=1)})

    result = fetch_trading_status_bse(
        ["920571.BJ"], date(2026, 9, 15), client=client, config=_config(tmp_path)
    )

    assert result.rows.height == 1
    assert result.rows["is_trading"].to_list() == [True]


def test_instruments_come_off_the_same_board_read_with_their_names(tmp_path):
    client = _Client({0: _jsonp([_row("920038", "森合高科"), _row("920023", "*ST田野")], total=2)})

    out = fetch_bse_instruments(date(2026, 9, 15), client=client, config=_config(tmp_path))

    assert out["symbol"].to_list() == ["920023.BJ", "920038.BJ"]
    assert out["name"].to_list() == ["*ST田野", "森合高科"]
    assert out["exchange"].unique().to_list() == ["BJ"]
    # Absent from the board payload, and the compact coalesces them from prior.
    assert out["list_date"].null_count() == out.height
    assert out.schema["list_date"] == pl.Date


@pytest.mark.parametrize("payload", [_jsonp([], total=0)])
def test_an_empty_board_yields_no_instruments(tmp_path, payload):
    out = fetch_bse_instruments(
        date(2026, 9, 15), client=_Client({0: payload}), config=_config(tmp_path)
    )

    assert out.is_empty()
    assert out.columns[:3] == ["symbol", "name", "exchange"]
