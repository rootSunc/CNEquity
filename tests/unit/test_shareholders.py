"""Shareholder structure — 股本结构 / 股东户数 / 前十大股东.

The load-bearing decision here is PIT. A 半年报 shareholder list is dated 06-30
and disclosed in late August; keyed by period alone, a July backtest reads
August's filing. `RPT_F10_EH_HOLDERS` is the awkward case — it carries no
disclosure date at all, so its rows borrow one from the float-holder report,
which is the other half of the same filing.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney import shareholders as sh

PERIOD = date(2025, 6, 30)
WIN_START = date(2025, 1, 1)
WIN_END = date(2025, 12, 31)
NOTICE = "2025-08-23 00:00:00"


class _Client:
    def close(self):
        return None


def _patch(monkeypatch, by_report: dict[str, list[dict]], seen: dict | None = None):
    """Route fetch_datacenter by report name so both holder reports differ."""

    def _fake(client, report, columns, **kwargs):
        if seen is not None:
            seen[report] = kwargs.get("filter_expr", "")
        return by_report.get(report, [])

    monkeypatch.setattr(sh, "fetch_datacenter", _fake)


def _holder(code: str, rank: int, *, name: str, pct_field: str, pct: float, notice=None):
    row = {
        "SECUCODE": f"{code}.SH",
        "END_DATE": "2025-06-30 00:00:00",
        "HOLDER_NAME": name,
        "HOLD_NUM": 1000 * rank,
        "HOLDER_RANK": rank,
        "IS_HOLDORG": "1",
        pct_field: pct,
    }
    if notice is not None:
        row["NOTICE_DATE"] = notice
    return row


# --- share_structure ---------------------------------------------------------


def test_share_structure_maps_the_four_share_counts(monkeypatch):
    _patch(
        monkeypatch,
        {
            sh._EQUITY_REPORT: [
                {
                    "SECUCODE": "000001.SZ",
                    "END_DATE": "2025-06-30 00:00:00",
                    "TOTAL_SHARES": 19405918198,
                    "UNLIMITED_SHARES": 19405600653,
                    "LIMITED_SHARES": 317545,
                    "FREELIQCI_SHARES": 8160481215,
                    "CHANGE_REASON": "高管股份变动",
                    "NOTICE_DATE": NOTICE,
                }
            ]
        },
    )
    df = sh.fetch_share_structure(WIN_START, WIN_END, client=_Client())
    row = df.row(0, named=True)
    assert row["symbol"] == "000001.SZ"
    assert row["change_date"] == PERIOD
    assert row["total_shares"] == 19405918198
    assert row["float_shares"] == 19405600653
    assert row["restricted_shares"] == 317545
    # free float is not the same number as float — an index weights on this one.
    assert row["free_float_shares"] == 8160481215
    assert row["change_reason"] == "高管股份变动"
    assert row["announce_date"] == date(2025, 8, 23)


def test_share_structure_is_swept_by_date_window_never_by_quarter_end(monkeypatch):
    """END_DATE here is the date the share count changed, not a report period.

    600519 has 16 rows in its whole history (送股上市, 回购, …). A quarter-end
    filter returns 2,446 rows market-wide — enough to look right — while
    silently dropping the ~50/day dated to every other date.
    """
    seen: dict[str, str] = {}
    _patch(monkeypatch, {sh._EQUITY_REPORT: []}, seen)
    sh.fetch_share_structure(WIN_START, WIN_END, client=_Client())
    expr = seen[sh._EQUITY_REPORT]
    assert "END_DATE>='2025-01-01'" in expr
    assert "END_DATE<='2025-12-31'" in expr
    assert "END_DATE=2025" not in expr, "an equality filter would be the period-sweep bug"


def test_share_structure_can_window_on_the_announcement_date_instead(monkeypatch):
    """Daily runs ask what was newly *disclosed* — a change effective weeks ago
    can be announced today, and a change-date window would never see it."""
    seen: dict[str, str] = {}
    _patch(monkeypatch, {sh._EQUITY_REPORT: []}, seen)
    sh.fetch_share_structure(
        date(2025, 8, 1), date(2025, 8, 31), by=sh.NOTICE_DATE, client=_Client()
    )
    assert "NOTICE_DATE>='2025-08-01'" in seen[sh._EQUITY_REPORT]
    assert "END_DATE" not in seen[sh._EQUITY_REPORT]


# --- shareholder_counts ------------------------------------------------------


def test_shareholder_counts_maps_the_concentration_inputs(monkeypatch):
    _patch(
        monkeypatch,
        {
            sh._HOLDERNUM_REPORT: [
                {
                    "SECUCODE": "000001.SZ",
                    "END_DATE": "2025-06-30 00:00:00",
                    "HOLDER_TOTAL_NUM": 443583,
                    "TOTAL_NUM_RATIO": -12.0341,
                    "AVG_FREE_SHARES": 43747,
                    "AVG_HOLD_AMT": 517077.94,
                    "NOTICE_DATE": NOTICE,
                }
            ]
        },
    )
    row = sh.fetch_shareholder_counts(WIN_START, WIN_END, client=_Client()).row(0, named=True)
    assert row["holder_count"] == 443583
    assert row["holder_count_change_pct"] == -12.0341
    assert row["count_date"] == PERIOD
    assert row["announce_date"] == date(2025, 8, 23)


# --- top_holders -------------------------------------------------------------


def test_both_scopes_land_in_one_frame_with_their_own_pct_field(monkeypatch):
    """holding_pct means share-of-total for one scope and share-of-float for the
    other. They come from different source columns and are not comparable."""
    _patch(
        monkeypatch,
        {
            sh._FREEHOLDERS_REPORT: [
                _holder(
                    "600519",
                    1,
                    name="香港中央结算",
                    pct_field="FREE_HOLDNUM_RATIO",
                    pct=8.5,
                    notice=NOTICE,
                )
            ],
            sh._HOLDERS_REPORT: [
                _holder("600519", 1, name="茅台集团", pct_field="HOLD_NUM_RATIO", pct=54.0)
            ],
        },
    )
    df = sh.fetch_top_holders(WIN_START, WIN_END, client=_Client())
    assert set(df["holder_scope"].to_list()) == {sh.SCOPE_FLOAT, sh.SCOPE_TOTAL}

    float_row = df.filter(pl.col("holder_scope") == sh.SCOPE_FLOAT).row(0, named=True)
    total_row = df.filter(pl.col("holder_scope") == sh.SCOPE_TOTAL).row(0, named=True)
    assert float_row["holding_pct"] == 8.5
    assert total_row["holding_pct"] == 54.0
    assert float_row["is_institution"] is True


def test_total_scope_borrows_its_disclosure_date_from_the_float_report(monkeypatch):
    """RPT_F10_EH_HOLDERS carries no NOTICE_DATE; without the borrow it could not
    be served point-in-time at all."""
    _patch(
        monkeypatch,
        {
            sh._FREEHOLDERS_REPORT: [
                _holder(
                    "600519", 1, name="A", pct_field="FREE_HOLDNUM_RATIO", pct=1.0, notice=NOTICE
                )
            ],
            sh._HOLDERS_REPORT: [
                _holder("600519", 1, name="B", pct_field="HOLD_NUM_RATIO", pct=2.0),
                _holder("600519", 2, name="C", pct_field="HOLD_NUM_RATIO", pct=1.0),
            ],
        },
    )
    df = sh.fetch_top_holders(WIN_START, WIN_END, client=_Client())
    total = df.filter(pl.col("holder_scope") == sh.SCOPE_TOTAL)
    assert total.height == 2
    assert total["announce_date"].to_list() == [date(2025, 8, 23)] * 2


def test_undated_total_rows_are_dropped_not_stamped_with_the_period(monkeypatch):
    """Dating them 06-30 would assert the list was known on 06-30 — the exact
    lookahead this dataset exists to prevent."""
    _patch(
        monkeypatch,
        {
            # No float filing for this symbol, so nothing to borrow from.
            sh._FREEHOLDERS_REPORT: [
                _holder(
                    "600519", 1, name="A", pct_field="FREE_HOLDNUM_RATIO", pct=1.0, notice=NOTICE
                )
            ],
            sh._HOLDERS_REPORT: [
                _holder("000001", 1, name="orphan", pct_field="HOLD_NUM_RATIO", pct=2.0)
            ],
        },
    )
    df = sh.fetch_top_holders(WIN_START, WIN_END, client=_Client())
    assert "000001.SH" not in df["symbol"].to_list()
    assert df["announce_date"].null_count() == 0


def test_rows_without_a_rank_or_name_are_skipped(monkeypatch):
    """Both are in the primary key; a row missing either cannot be identified."""
    for field in ("HOLDER_RANK", "HOLDER_NAME"):
        bad = _holder("600519", 1, name="A", pct_field="FREE_HOLDNUM_RATIO", pct=1.0, notice=NOTICE)
        bad[field] = None
        _patch(monkeypatch, {sh._FREEHOLDERS_REPORT: [bad]})
        assert sh.fetch_top_holders(WIN_START, WIN_END, client=_Client()).is_empty(), field


def test_holders_tied_on_share_count_share_a_rank_and_both_survive(monkeypatch):
    """holder_rank is NOT unique. 600010.SH 2025-06-30 rank 9 is both 博时基金
    and 易方达基金 at 167,831,580 shares each — the 中证金融 vehicles hold
    identical allocations, so ties are routine and land on the holders most
    worth seeing. Keying without holder_name silently deletes one of them."""
    tied = [
        _holder("600010", 9, name=n, pct_field="FREE_HOLDNUM_RATIO", pct=1.2, notice=NOTICE)
        for n in (
            "博时基金-农业银行-博时中证金融资产管理计划",
            "易方达基金-农业银行-易方达中证金融资产管理计划",
        )
    ]
    for row in tied:
        row["HOLD_NUM"] = 167831580
    _patch(monkeypatch, {sh._FREEHOLDERS_REPORT: tied})
    df = sh.fetch_top_holders(WIN_START, WIN_END, client=_Client())
    assert df.height == 2, "a tied holder was dropped"
    assert df["holder_name"].n_unique() == 2


def test_is_org_leaves_unknown_as_null_rather_than_false():
    assert sh._is_org("1") is True
    assert sh._is_org("0") is False
    assert sh._is_org("") is None
    assert sh._is_org(None) is None


def test_registered_with_pit_and_the_rank_in_the_key():
    from cn_market_lake.domain.datasets import DATASETS
    from cn_market_lake.domain.schemas import PRIMARY_KEYS

    for name in ("share_structure", "shareholder_counts", "top_holders"):
        assert DATASETS[name].pit is True, f"{name} must be point-in-time"
        assert "announce_date" in PRIMARY_KEYS[name]
    assert "holder_rank" in PRIMARY_KEYS["top_holders"]
    assert "holder_name" in PRIMARY_KEYS["top_holders"]
    assert "holder_scope" in PRIMARY_KEYS["top_holders"]
