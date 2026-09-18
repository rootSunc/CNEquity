"""Offline tests for the explicitly scoped Baostock corporate-action repair."""

from datetime import date

import polars as pl

from cnequity.adapters.baostock.corporate_actions import (
    fetch_corporate_actions_baostock,
)

_FIELDS = [
    "code",
    "dividPreNoticeDate",
    "dividAgmPumDate",
    "dividPlanAnnounceDate",
    "dividPlanDate",
    "dividRegistDate",
    "dividOperateDate",
    "dividPayDate",
    "dividStockMarketDate",
    "dividCashPsBeforeTax",
    "dividCashPsAfterTax",
    "dividStocksPs",
    "dividCashStock",
    "dividReserveToStockPs",
]


class _Result:
    def __init__(self, rows, *, error_code="0", fields=None):
        self.error_code = error_code
        self.error_msg = "" if error_code == "0" else "unsupported"
        self.fields = fields or _FIELDS
        self._rows = rows
        self._index = -1

    def next(self):
        self._index += 1
        return self._index < len(self._rows)

    def get_row_data(self):
        return self._rows[self._index]


class _Baostock:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []
        self.logged_out = False

    def login(self):
        return _Result([])

    def query_dividend_data(self, code, year, **kwargs):
        self.calls.append((code, year, kwargs))
        return _Result(self.rows.get((code, year), []))

    def logout(self):
        self.logged_out = True


def _row(
    code="sz.300114",
    operate="2020-08-11",
    cash="0.049538",
    bonus="0.100000",
    transfer="0.200000",
):
    return [
        code,
        "",
        "2020-06-29",
        "2020-04-01",
        "2020-08-04",
        "2020-08-10",
        operate,
        operate,
        "",
        cash,
        "0.0445842",
        bonus,
        "",
        transfer,
    ]


def test_fetch_corporate_actions_maps_baostock_per_share_fields():
    bs = _Baostock({("sz.300114", 2020): [_row()]})

    df, failed = fetch_corporate_actions_baostock(
        ["300114.SZ"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        bs=bs,
        sleep=lambda _: None,
    )

    assert failed == []
    assert bs.logged_out is True
    assert bs.calls == [("sz.300114", 2020, {"yearType": "operate"})]
    assert df.select("symbol", "ex_date", "action_type").to_dicts() == [
        {"symbol": "300114.SZ", "ex_date": date(2020, 8, 11), "action_type": "bonus"},
        {
            "symbol": "300114.SZ",
            "ex_date": date(2020, 8, 11),
            "action_type": "cash_dividend",
        },
        {"symbol": "300114.SZ", "ex_date": date(2020, 8, 11), "action_type": "transfer"},
    ]
    assert df.filter(pl.col("action_type") == "cash_dividend")["cash_dividend"].item() == 0.049538
    assert df.filter(pl.col("action_type") == "bonus")["bonus_ratio"].item() == 0.1
    assert df.filter(pl.col("action_type") == "transfer")["transfer_ratio"].item() == 0.2


def test_fetch_corporate_actions_queries_each_year_and_bounds_ex_date():
    bs = _Baostock(
        {
            ("sh.600000", 2019): [_row(code="sh.600000", operate="2019-12-31")],
            ("sh.600000", 2020): [_row(code="sh.600000", operate="2020-01-02")],
        }
    )

    df, failed = fetch_corporate_actions_baostock(
        ["600000.SH"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        bs=bs,
        sleep=lambda _: None,
    )

    assert failed == []
    assert bs.calls == [("sh.600000", 2020, {"yearType": "operate"})]
    assert df.height == 3
    assert df["ex_date"].unique().to_list() == [date(2020, 1, 2)]


def test_fetch_corporate_actions_honors_symbol_specific_window():
    bs = _Baostock({("sh.600000", 2020): [_row(code="sh.600000")]})

    df, failed = fetch_corporate_actions_baostock(
        ["600000.SH"],
        date(2016, 1, 1),
        date(2025, 12, 31),
        bs=bs,
        sleep=lambda _: None,
        symbol_windows={"600000.SH": (date(2020, 1, 1), date(2020, 12, 31))},
    )

    assert failed == []
    assert df.height == 3
    assert bs.calls == [("sh.600000", 2020, {"yearType": "operate"})]


def test_fetch_corporate_actions_paces_each_annual_query():
    bs = _Baostock({})
    sleeps = []

    df, failed = fetch_corporate_actions_baostock(
        ["600000.SH"],
        date(2018, 1, 1),
        date(2020, 12, 31),
        bs=bs,
        sleep=sleeps.append,
    )

    assert df.is_empty()
    assert failed == []
    # One symbol-level pace plus one pace for each of the three year queries.
    assert sleeps == [1.0, 1.0, 1.0, 1.0]
    assert [year for _code, year, _kwargs in bs.calls] == [2018, 2019, 2020]


def test_fetch_corporate_actions_skips_bj_without_querying_vendor():
    bs = _Baostock({})

    df, failed = fetch_corporate_actions_baostock(
        ["430090.BJ"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        bs=bs,
        sleep=lambda _: None,
    )

    assert failed == []
    assert df.is_empty()
    assert bs.calls == []


def test_fetch_corporate_actions_rejects_invalid_numeric_row():
    bs = _Baostock({("sz.300114", 2020): [_row(cash="not-a-number")]})

    df, failed = fetch_corporate_actions_baostock(
        ["300114.SZ"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        bs=bs,
        sleep=lambda _: None,
    )

    assert df.is_empty()
    assert failed == []


class _FakeSocket:
    """A socket that answers one request with fixed bytes, like baostock's."""

    def __init__(self, response: bytes):
        self._response = response
        self._pending = b""

    def send(self, data):
        self._pending = self._response
        return len(data)

    def recv(self, _size=8192):
        chunk, self._pending = self._pending, b""
        return chunk

    def settimeout(self, _timeout):
        return None


class _SocketBaostock(_Baostock):
    """A session whose query goes over the socket, as the real SDK's does."""

    def __init__(self, rows, response: bytes):
        super().__init__(rows)
        self.response = response

    def query_dividend_data(self, code, year, **kwargs):
        import baostock.common.context as bctx

        sock = bctx.default_socket
        sock.send(b"query_dividend_data")
        sock.recv(8192)
        return super().query_dividend_data(code, year, **kwargs)


def test_the_archive_captures_the_bytes_the_sdk_read(tmp_path, monkeypatch):
    """The repair must be archivable, and the SDK hands out no bytes.

    ``query_dividend_data`` decodes the response inside the socket helper and
    keeps only parsed fields, so a lake with raw archiving on — the default —
    had no wire to archive and every repair died on the first answered year.
    The bytes still exist at the socket; record them there.
    """
    import json

    import baostock.common.context as bctx

    from cnequity.config import Config
    from cnequity.storage.layout import init_data_layout

    wire = b"response-header<![CDATA[]]>\n"
    monkeypatch.setattr(bctx, "default_socket", _FakeSocket(wire), raising=False)
    config = Config(data_root=tmp_path / "data")
    init_data_layout(config)
    bs = _SocketBaostock({("sz.300114", 2020): [_row()]}, wire)

    df, failed = fetch_corporate_actions_baostock(
        ["300114.SZ"],
        date(2020, 1, 1),
        date(2020, 12, 31),
        bs=bs,
        sleep=lambda _: None,
        config=config,
        run_id="repair-test",
    )

    assert failed == []
    assert not df.is_empty()
    sidecars = list((config.meta_root / "raw").rglob("*.json"))
    archived = [json.loads(path.read_text(encoding="utf-8")) for path in sidecars]
    payloads = [p for p in (config.meta_root / "raw").rglob("*.gz")]
    assert payloads, "the response bytes must reach the archive"
    assert any(record.get("request_params", {}).get("year") == 2020 for record in archived), (
        archived
    )
