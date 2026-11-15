"""Issuer halt notices as bar-coverage evidence (domain/announced_halts.py)."""

from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.announced_halts import announced_halt_keys, classify_title
from cnequity.domain.trading_status import independent_suspension_keys
from cnequity.storage.instrument_catalog import load_curated_trading_status

# 2025-11-17 .. 2025-12-31 weekdays; no holidays fall in this stretch.
SESSIONS = [d for d in pl.date_range(date(2025, 11, 17), date(2025, 12, 31), eager=True)]
SESSIONS = [d for d in SESSIONS if d.weekday() < 5]


def _notices(*rows):
    return pl.DataFrame(
        [{"symbol": s, "announce_date": d, "title": t} for s, d, t in rows],
        schema={"symbol": pl.Utf8, "announce_date": pl.Date, "title": pl.Utf8},
    )


@pytest.mark.parametrize(
    ("title", "kind"),
    [
        ("东兴证券股份有限公司关于筹划重大资产重组的停牌公告", "open"),
        ("关于筹划重大资产重组的停牌进展公告", "continue"),
        ("重大资产重组停牌期间进展公告", "continue"),
        ("关于披露重大资产重组预案的一般风险提示暨公司股票复牌的公告", "resume"),
        # Advance notices: the halt starts later, often weeks later.
        ("关于公司A股股票连续停牌直至终止上市、实施换股吸收合并的提示性公告", None),
        ("关于公司A股股票连续停牌的提示性公告", None),
        ("关于实施2024年年度权益分派时可转债转股价格调整暨转股连续停牌的提示性公告", None),
        ("关于停牌前一个交易日前十大股东和前十大流通股东持股情况的公告", None),
        ("关于实施其他风险警示暨停牌的公告", None),
        ("关于控股子公司停牌的提示性公告", None),
        ("股票交易异常波动风险提示暨停牌核查的公告", None),
    ],
)
def test_titles_are_classified_conservatively(title, kind):
    assert classify_title(title) == kind


def test_a_bracketed_halt_excludes_the_notice_days():
    keys = announced_halt_keys(
        _notices(
            ("601198.SH", date(2025, 11, 20), "关于筹划重大资产重组的停牌公告"),
            ("601198.SH", date(2025, 11, 27), "关于筹划重大资产重组的停牌进展公告"),
            ("601198.SH", date(2025, 12, 4), "关于股票复牌的公告"),
        ),
        SESSIONS,
    )
    days = sorted(d for _s, d in keys)
    assert days[0] == date(2025, 11, 21) and days[-1] == date(2025, 12, 3)
    assert date(2025, 11, 27) in days  # a progress day sits inside the span
    assert date(2025, 11, 20) not in days and date(2025, 12, 4) not in days


def test_an_unconfirmed_open_notice_excuses_nothing():
    assert not announced_halt_keys(
        _notices(("600000.SH", date(2025, 11, 20), "关于重大事项的停牌公告")), SESSIONS
    )


def test_a_confirmation_after_the_gap_does_not_bridge():
    keys = announced_halt_keys(
        _notices(
            ("600000.SH", date(2025, 11, 17), "关于重大事项的停牌公告"),
            ("600000.SH", date(2025, 12, 15), "关于重大事项停牌进展公告"),
        ),
        SESSIONS,
    )
    assert not keys


def _write(cfg, dataset, column, day, rows, schema):
    part = cfg.curated_root / dataset / f"{column}={day}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows, schema=schema).write_parquet(part / "part-0.parquet")


STATUS_SCHEMA = {
    "symbol": pl.Utf8,
    "trade_date": pl.Date,
    "is_trading": pl.Boolean,
    "status": pl.Utf8,
    "source": pl.Utf8,
    "fetched_at": pl.Datetime("us", "UTC"),
}


def test_the_loader_adds_announced_halts_without_overriding_first_hand_rows(tmp_path):
    cfg = Config(data_root=tmp_path)
    for day, title in [
        (date(2025, 11, 20), "关于筹划重大资产重组的停牌公告"),
        (date(2025, 11, 27), "关于筹划重大资产重组的停牌进展公告"),
    ]:
        _write(
            cfg,
            "announcement_index",
            "announce_date",
            day,
            [
                {"symbol": symbol, "announce_date": day, "title": title}
                for symbol in ("601059.SH", "601198.SH")
            ],
            {"symbol": pl.Utf8, "announce_date": pl.Date, "title": pl.Utf8},
        )
    day = date(2025, 11, 24)
    _write(
        cfg,
        "trading_status",
        "trade_date",
        day,
        [
            # Carried board: cannot prove the halt, so the notice replaces it.
            {
                "symbol": "601059.SH",
                "trade_date": day,
                "is_trading": False,
                "status": "suspended",
                "source": "eastmoney_cached",
                "fetched_at": datetime(2025, 11, 24, 8, tzinfo=timezone.utc),
            },
            # A first-hand trading row vetoes the notice.
            {
                "symbol": "601198.SH",
                "trade_date": day,
                "is_trading": True,
                "status": "normal",
                "source": "baostock",
                "fetched_at": datetime(2025, 12, 1, tzinfo=timezone.utc),
            },
        ],
        STATUS_SCHEMA,
    )

    status = load_curated_trading_status(cfg, start=day, end=day)
    by_symbol = {r["symbol"]: r for r in status.iter_rows(named=True)}
    assert by_symbol["601059.SH"]["source"] == "announcement"
    assert by_symbol["601198.SH"]["source"] == "baostock"
    assert independent_suspension_keys(status).rows() == [("601059.SH", day)]

    plain = load_curated_trading_status(cfg, start=day, end=day, include_announced=False)
    assert set(plain["source"]) == {"eastmoney_cached", "baostock"}


def test_notices_of_one_day_are_read_in_a_fixed_order_whatever_order_they_arrive_in():
    # A halt opens 11-17 and is confirmed 11-20; on 11-24 a progress notice of
    # that halt and the opening notice of the next one arrive together; on
    # 11-27 the stock resumes and halts again the same day; 12-01 resumes.
    rows = [
        ("600000.SH", date(2025, 11, 17), "关于筹划重大资产重组的停牌公告"),
        ("600000.SH", date(2025, 11, 20), "关于筹划重大资产重组的停牌进展公告"),
        ("600000.SH", date(2025, 11, 24), "关于筹划重大资产重组的停牌进展公告"),
        ("600000.SH", date(2025, 11, 24), "关于筹划发行股份购买资产的停牌公告"),
        ("600000.SH", date(2025, 11, 27), "关于公司股票复牌的公告"),
        ("600000.SH", date(2025, 11, 27), "关于筹划控制权变更的停牌公告"),
        ("600000.SH", date(2025, 12, 1), "关于公司股票复牌的公告"),
    ]
    found = {
        frozenset(announced_halt_keys(_notices(*order), SESSIONS)) for order in (rows, rows[::-1])
    }
    assert len(found) == 1
    (keys,) = found
    days = sorted(day for _, day in keys)
    # The first halt holds 11-18 to 11-21 (its 11-24 progress notice closes the bracket
    # before the second halt opens), the second 11-25 and 11-26, the third 11-28.
    assert days == [
        date(2025, 11, 18),
        date(2025, 11, 19),
        date(2025, 11, 20),
        date(2025, 11, 21),
        date(2025, 11, 25),
        date(2025, 11, 26),
        date(2025, 11, 28),
    ]
