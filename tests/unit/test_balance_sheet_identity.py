"""Which broken balance sheets are worth chasing, and which are just old.

Measured 2026-09-19 over 286,689 periods: 245 breaks, 181 of them 2005 or
earlier, 9 from 2021 on. A 2003 annual report that does not foot is the
published record and will never be restated; reporting it at the same severity
as a 2024 one buries the handful somebody could actually act on.
"""

from datetime import date, datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.quality.cross_checks import MODERN_STATEMENT_YEAR, balance_sheet_identity_findings
from cnequity.storage.layout import init_data_layout

FETCHED = datetime(2026, 9, 19, tzinfo=timezone.utc)


def _rows(symbol: str, period: str, assets: float, liabilities: float, equity: float) -> list[dict]:
    return [
        {
            "symbol": symbol,
            "report_period": period,
            "statement_type": "balance",
            "item_code": code,
            "item_value": value,
            "announce_date": date(int(period[:4]) + 1, 4, 20),
            "source": "eastmoney",
            "data_version": "v1",
            "fetched_at": FETCHED,
        }
        for code, value in (
            ("total_assets", assets),
            ("total_liabilities", liabilities),
            ("total_equity", equity),
        )
    ]


def _lake(tmp_path, rows: list[dict]) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    init_data_layout(cfg)
    frame = pl.DataFrame(rows)
    for period in frame.get_column("report_period").unique().to_list():
        part = cfg.curated_root / "financial_statement_items" / f"report_period={period}"
        part.mkdir(parents=True, exist_ok=True)
        frame.filter(pl.col("report_period") == period).write_parquet(part / "part-0.parquet")
    return cfg


def test_a_sheet_that_foots_says_nothing(tmp_path):
    cfg = _lake(tmp_path, _rows("600519.SH", "2024Q4", 100.0, 40.0, 60.0))

    assert balance_sheet_identity_findings(cfg) == []


def test_a_recent_break_is_a_warning_worth_chasing(tmp_path):
    cfg = _lake(tmp_path, _rows("600519.SH", "2024Q4", 100.0, 40.0, 50.0))

    (finding,) = balance_sheet_identity_findings(cfg)

    assert finding["check"] == "balance_sheet_identity"
    assert finding["severity"] == "warning"
    assert finding["rows"] == 1
    assert finding["since_year"] == MODERN_STATEMENT_YEAR


def test_an_old_break_is_counted_not_chased(tmp_path):
    cfg = _lake(tmp_path, _rows("600519.SH", "2003Q4", 100.0, 40.0, 50.0))

    (finding,) = balance_sheet_identity_findings(cfg)

    assert finding["check"] == "balance_sheet_identity_historical"
    assert finding["severity"] == "info"
    assert finding["source_limited"] is True
    assert finding["by_report_year"] == {"2003": 1}


def test_the_old_ones_cannot_bury_the_recent_one(tmp_path):
    cfg = _lake(
        tmp_path,
        _rows("000001.SZ", "2002Q4", 100.0, 40.0, 50.0)
        + _rows("000002.SZ", "2003Q4", 100.0, 40.0, 50.0)
        + _rows("600519.SH", "2024Q4", 100.0, 40.0, 50.0),
    )

    findings = {f["severity"]: f["rows"] for f in balance_sheet_identity_findings(cfg)}

    assert findings == {"warning": 1, "info": 2}
