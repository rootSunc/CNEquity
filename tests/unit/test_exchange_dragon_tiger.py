"""The exchange route for `dragon_tiger`, against measured EastMoney figures.

Every case here is a real discrepancy found by comparing the adapter with the
lake for 2026-09-15 across all thirty SZ securities. An earlier check on one
symbol reported an exact match and hid all three.
"""

from __future__ import annotations

from datetime import date

from cnequity.adapters.exchange import dragon_tiger as dt


def _desk(mmlb: str, name: str, buy: str, sell: str) -> dict:
    return {"mmlb": mmlb, "zsmc": name, "mrje": buy, "mcje": sell}


def test_a_desk_on_both_lists_is_counted_once():
    """000823.SZ: 深股通专用 topped both sides, and summing the ten rows put
    buying 54% above the vendor's."""
    desks = [
        _desk("买1", "深股通专用", "323,082,002", "267,785,012"),
        _desk("买2", "甲营业部", "159,355,141", "0"),
        _desk("卖1", "深股通专用", "323,082,002", "267,785,012"),
        _desk("卖2", "乙营业部", "0", "117,617,815"),
    ]
    buy, sell = dt._desk_totals(desks)
    assert buy == 323_082_002 + 159_355_141
    assert sell == 267_785_012 + 117_617_815


def test_the_sell_side_desks_still_contribute_their_buying():
    """000428.SZ: restricting each side to its own five dropped 1,805,945 of
    buying and came in 2.3% under. The vendor counts every listed desk."""
    desks = [
        _desk("买1", "机构专用", "28,177,171", "2,074,965"),
        _desk("卖1", "中信证券北京中关村", "419,003", "25,874,617"),
        _desk("卖2", "国泰海通南京胜利路", "2,580", "7,867,267"),
    ]
    buy, _ = dt._desk_totals(desks)
    assert buy == 28_177_171 + 419_003 + 2_580


def test_a_placeholder_name_is_not_one_desk():
    """机构专用 stood four times on 000428.SZ's buy list with different figures,
    so the repeat test cannot be the name alone."""
    desks = [
        _desk("买1", "机构专用", "28,177,171", "2,074,965"),
        _desk("买3", "机构专用", "12,810,050", "376,022"),
        _desk("买4", "机构专用", "11,919,210", "2,483,604"),
    ]
    buy, sell = dt._desk_totals(desks)
    assert buy == 28_177_171 + 12_810_050 + 11_919_210
    assert sell == 2_074_965 + 376_022 + 2_483_604


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _Session:
    """Serves the SZSE list ten rows a page, as the endpoint does."""

    def __init__(self, pages: list[list[dict]]):
        self.pages = pages
        self.asked: list[str] = []

    def get(self, url, **_):
        self.asked.append(url)
        page = int(url.split("PAGENO=")[1].split("&")[0])
        return _Resp(
            [
                {
                    "metadata": {"pagecount": len(self.pages)},
                    "data": self.pages[page - 1],
                }
            ]
        )

    def close(self):
        return None


def test_the_list_is_read_past_its_first_page():
    """The day's thirty securities arrived as four pages of ten; reading page
    one alone returned seven and looked like a complete session."""
    pages = [[{"zqdm": f"{i:06d}"} for i in range(n, n + 10)] for n in (1, 11, 21)]
    session = _Session(pages)
    rows = dt._szse_listed(session, date(2026, 9, 15), None)
    assert len(rows) == 30
    assert [u.split("PAGENO=")[1] for u in session.asked] == ["1", "2", "3"]


def test_a_failed_page_is_not_a_short_day():
    class _Broken(_Session):
        def get(self, url, **_):
            if "PAGENO=2" in url:
                raise RuntimeError("boom")
            return super().get(url, **_)

    session = _Broken([[{"zqdm": "000001"}] * 10, [{"zqdm": "000002"}] * 10])
    try:
        dt._szse_listed(session, date(2026, 9, 15), None)
    except RuntimeError:
        pass
    else:  # pragma: no cover
        raise AssertionError("a dropped page must not read as the end of the list")
