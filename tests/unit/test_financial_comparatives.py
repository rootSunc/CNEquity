"""Statement items republished as comparatives in later filings."""

from __future__ import annotations

from datetime import date

import polars as pl

from cnequity.domain.financial_statements import (
    reconstruct_comparatives,
    unchanged_republications,
)

SCHEMA = {
    "symbol": pl.Utf8,
    "report_period": pl.Utf8,
    "statement_type": pl.Utf8,
    "item_code": pl.Utf8,
    "item_value": pl.Float64,
    "announce_date": pl.Date,
    "source": pl.Utf8,
}


def _frame(rows):
    return pl.DataFrame(rows, schema=SCHEMA, orient="row")


NO_DATES = pl.DataFrame(
    schema={"symbol": pl.Utf8, "report_period": pl.Utf8, "original_date": pl.Date}
)


def test_an_item_first_seen_as_a_comparative_gets_its_own_filing_date():
    frame = _frame(
        [
            # 2024Q2's own filing: the balance sheet arrived, the income item did not.
            (
                "600001.SH",
                "2024Q2",
                "balance",
                "total_assets",
                10.0,
                date(2024, 8, 20),
                "eastmoney",
            ),
            # 2025Q2's filing, which republishes 2024Q2 income as prior-year figures.
            (
                "600001.SH",
                "2025Q2",
                "balance",
                "total_assets",
                12.0,
                date(2025, 8, 21),
                "eastmoney",
            ),
            ("600001.SH", "2024Q2", "income", "net_profit", 3.0, date(2025, 8, 21), "eastmoney"),
        ]
    )

    out = reconstruct_comparatives(frame, NO_DATES)

    assert out.select("report_period", "item_code", "announce_date", "source").rows() == [
        ("2024Q2", "net_profit", date(2024, 8, 20), "eastmoney_backfill")
    ]


def test_a_period_the_results_table_dates_is_left_to_it():
    frame = _frame(
        [
            (
                "600001.SH",
                "2024Q2",
                "balance",
                "total_assets",
                10.0,
                date(2024, 8, 20),
                "eastmoney",
            ),
            (
                "600001.SH",
                "2025Q2",
                "balance",
                "total_assets",
                12.0,
                date(2025, 8, 21),
                "eastmoney",
            ),
            ("600001.SH", "2024Q2", "income", "net_profit", 3.0, date(2025, 8, 21), "eastmoney"),
        ]
    )
    dates = pl.DataFrame(
        {"symbol": ["600001.SH"], "report_period": ["2024Q2"], "original_date": [date(2024, 8, 20)]}
    )

    assert reconstruct_comparatives(frame, dates).is_empty()


def test_an_annual_item_after_a_results_flash_is_not_moved_to_the_flash():
    """601098.SH 2021Q4: the 业绩快报 on 2022-01-20 dates the period, and the
    annual report with its cash-flow statement came on 2022-04-26 — the same day
    as 2022Q1's report, so the item looks like a comparative. It is not: moving
    it to the flash date would show a figure nobody had published yet."""
    from cnequity.domain.financial_statements import reconstruct_disclosures

    frame = _frame(
        [
            ("601098.SH", "2021Q4", "income", "net_profit", 9.0, date(2022, 1, 20), "eastmoney"),
            ("601098.SH", "2022Q1", "income", "net_profit", 2.0, date(2022, 4, 26), "eastmoney"),
            (
                "601098.SH",
                "2021Q4",
                "cashflow",
                "end_cash",
                5.0,
                date(2022, 4, 26),
                "cninfo_original",
            ),
        ]
    )
    dates = pl.DataFrame(
        {"symbol": ["601098.SH"], "report_period": ["2021Q4"], "original_date": [date(2022, 1, 20)]}
    )

    assert reconstruct_comparatives(frame, dates).is_empty()
    assert (
        reconstruct_disclosures(frame, dates).filter(pl.col("item_code") == "end_cash").is_empty()
    )


def test_a_late_row_that_is_not_a_later_filing_is_not_reconstructed():
    frame = _frame(
        [
            (
                "600001.SH",
                "2024Q2",
                "balance",
                "total_assets",
                10.0,
                date(2024, 8, 20),
                "eastmoney",
            ),
            ("600001.SH", "2024Q2", "income", "net_profit", 3.0, date(2025, 1, 9), "eastmoney"),
        ]
    )

    assert reconstruct_comparatives(frame, NO_DATES).is_empty()


def test_only_unchanged_later_republications_are_repeats():
    held = _frame(
        [
            ("600001.SH", "2024Q2", "income", "net_profit", 3.0, date(2024, 8, 20), "eastmoney"),
            ("600001.SH", "2024Q2", "income", "revenue", 9.0, date(2024, 8, 20), "eastmoney"),
        ]
    )
    incoming = _frame(
        [
            ("600001.SH", "2024Q2", "income", "net_profit", 3.0, date(2025, 8, 21), "eastmoney"),
            ("600001.SH", "2024Q2", "income", "revenue", 9.5, date(2025, 8, 21), "eastmoney"),
            ("600001.SH", "2025Q2", "income", "net_profit", 4.0, date(2025, 8, 21), "eastmoney"),
        ]
    )

    repeats = unchanged_republications(incoming, held)

    assert repeats.select("report_period", "item_code").rows() == [("2024Q2", "net_profit")]
