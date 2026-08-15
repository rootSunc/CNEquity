"""EastMoney financial statement items (batch, PIT via announce_date).

Four EastMoney datacenter reports feed one long-format dataset:

* ``RPT_LICO_FN_CPD`` (业绩报表) — headline per-share and ratio items, and the
  only report carrying the **original** announcement date.
* ``RPT_DMSK_FN_BALANCE`` / ``_INCOME`` / ``_CASHFLOW`` (财务报表) — the
  statement levels a factor library actually needs: book equity, total assets,
  operating cost, operating cash flow, capex.

**Why announce_date comes from LICO.** The DMSK reports' ``NOTICE_DATE`` is not
the original announcement — it is the date the figure was last *republished*,
typically as a prior-period comparative in a later filing. EastMoney dates the
FY2016 balance sheet 2018-03-15 and the FY2024 cash flow 2026-03-21, one to two
years after the fact. Taking those at face value would push every statement item
1-2 years into the future and leave a PIT query with almost no fundamentals in
its usable window. LICO's ``NOTICE_DATE`` is the real first-disclosure date
(FY2024 → 2025-03-15), so backfill resolves announce_date from LICO and falls
back to the report's own NOTICE_DATE only when LICO has no matching row — a
fallback that can only make data arrive *late*, never early.

**Known limitation — restated values, original dates.** A backfill fetch returns
whichever version of a figure EastMoney serves today, which for older periods is
the restated one, paired with the original announcement date. That is the usual
compromise for a free source, but it is a real (small) look-ahead: the restated
number was not knowable on the original date. Only vintages accumulated by daily
runs going forward are true point-in-time.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import polars as pl

from cn_market_lake.adapters.eastmoney.common import (
    report_period_from_date as _report_period,
)
from cn_market_lake.adapters.eastmoney.common import symbol_from_secucode
from cn_market_lake.adapters.eastmoney.datacenter import fetch_datacenter
from cn_market_lake.adapters.eastmoney.em_auth import EastMoneyClient
from cn_market_lake.config import Config

logger = logging.getLogger(__name__)

# Align with daily_bars research window; CLI --start/--end can clip further.
_BACKFILL_START_YEAR = 2001
_QUARTER_END_MMDD = (("03", "31"), ("06", "30"), ("09", "30"), ("12", "31"))

# Mirrors PRIMARY_KEYS["financial_statement_items"]: announce_date is part of the
# key so a restatement adds a vintage instead of erasing the original.
_PK = ["symbol", "report_period", "statement_type", "item_code", "announce_date"]


@dataclass(frozen=True)
class _Report:
    """One EastMoney datacenter report contributing items to the dataset.

    ``report_date_field`` differs between the two families (``REPORTDATE`` on
    LICO, ``REPORT_DATE`` on DMSK) and is used for both the column list and the
    backfill filter, so a wrong name fails loudly at fetch instead of silently
    returning everything.
    """

    name: str
    report_date_field: str
    items: dict[tuple[str, str], str]
    # True for the report whose NOTICE_DATE is the original announcement.
    authoritative_announce_date: bool = False

    @property
    def columns(self) -> str:
        fields = dict.fromkeys(self.items.values())
        return f"SECURITY_CODE,SECUCODE,{self.report_date_field},NOTICE_DATE," + ",".join(fields)


# Amount items are yuan; *_yoy, roe and gross_margin are percent; eps/bps/
# ocf_per_share are per-share yuan. Units follow EastMoney as served.
_REPORTS: tuple[_Report, ...] = (
    _Report(
        "RPT_LICO_FN_CPD",
        "REPORTDATE",
        {
            ("income", "revenue"): "TOTAL_OPERATE_INCOME",
            ("income", "net_profit"): "PARENT_NETPROFIT",
            ("indicator", "roe"): "WEIGHTAVG_ROE",
            ("indicator", "eps"): "BASIC_EPS",
            ("indicator", "eps_deducted"): "DEDUCT_BASIC_EPS",
            # Book value per share — the cheapest route to a B/P factor, and a
            # cross-check on balance/total_equity divided by share count.
            ("indicator", "bps"): "BPS",
            ("indicator", "gross_margin"): "XSMLL",
            ("indicator", "ocf_per_share"): "MGJYXJJE",
            ("indicator", "revenue_yoy"): "YSTZ",
            ("indicator", "net_profit_yoy"): "SJLTZ",
        },
        authoritative_announce_date=True,
    ),
    _Report(
        "RPT_DMSK_FN_BALANCE",
        "REPORT_DATE",
        {
            ("balance", "total_assets"): "TOTAL_ASSETS",
            # Total equity including minority interest (股东权益合计), not the
            # parent-only figure; divide with that in mind when forming B/P.
            ("balance", "total_equity"): "TOTAL_EQUITY",
            ("balance", "total_liabilities"): "TOTAL_LIABILITIES",
            ("balance", "inventory"): "INVENTORY",
            ("balance", "accounts_receivable"): "ACCOUNTS_RECE",
            ("balance", "monetary_funds"): "MONETARYFUNDS",
            ("balance", "fixed_assets"): "FIXED_ASSET",
        },
    ),
    _Report(
        "RPT_DMSK_FN_INCOME",
        "REPORT_DATE",
        {
            ("income", "operating_cost"): "OPERATE_COST",
            ("income", "operating_profit"): "OPERATE_PROFIT",
            ("income", "total_profit"): "TOTAL_PROFIT",
            ("income", "net_profit_deducted"): "DEDUCT_PARENT_NETPROFIT",
            ("income", "income_tax"): "INCOME_TAX",
            ("income", "sale_expense"): "SALE_EXPENSE",
            ("income", "manage_expense"): "MANAGE_EXPENSE",
            ("income", "finance_expense"): "FINANCE_EXPENSE",
        },
    ),
    _Report(
        "RPT_DMSK_FN_CASHFLOW",
        "REPORT_DATE",
        {
            ("cashflow", "net_cash_operate"): "NETCASH_OPERATE",
            ("cashflow", "net_cash_invest"): "NETCASH_INVEST",
            ("cashflow", "net_cash_finance"): "NETCASH_FINANCE",
            # 购建固定资产/无形资产等支付的现金 — the standard capex proxy.
            ("cashflow", "capex"): "CONSTRUCT_LONG_ASSET",
            ("cashflow", "end_cash"): "END_CCE",
        },
    ),
)

_ANNOUNCE_SOURCE = next(r for r in _REPORTS if r.authoritative_announce_date)


def _report_period_dates(
    trade_date: date,
    *,
    start: date | None = None,
    end: date | None = None,
) -> list[str]:
    """Quarter-end report dates from the backfill floor through *trade_date*.

    Optional *start* / *end* clip the walk (CLI ``cml backfill … --start/--end``)
    so ops can chunk multi-year sweeps. Bounds are inclusive on the period date.
    """
    lower = date(_BACKFILL_START_YEAR, 1, 1)
    if start is not None:
        lower = max(lower, start)
    upper = trade_date
    if end is not None:
        upper = min(upper, end)
    if lower > upper:
        return []

    out: list[str] = []
    for year in range(lower.year, upper.year + 1):
        for mm, dd in _QUARTER_END_MMDD:
            ds = f"{year}-{mm}-{dd}"
            period = date.fromisoformat(ds)
            if lower <= period <= upper:
                out.append(ds)
    return sorted(out, reverse=True)


def _parse_rows(
    raw: list[dict],
    report: _Report,
    *,
    default_notice: str,
    announce_dates: dict[tuple[str, str], date] | None = None,
) -> tuple[list[dict], int]:
    """Long-format rows for one report page-set, plus a fallback-date count."""
    rows: list[dict] = []
    fallbacks = 0
    for item in raw:
        # SECUCODE (e.g. 600519.SH) filters to A-share and drops NEEQ (.NQ),
        # which dominate same-day announcements and would otherwise be empty.
        sym = symbol_from_secucode(item.get("SECUCODE"))
        if not sym:
            continue
        report_period = _report_period(item.get(report.report_date_field))
        if not report_period:
            continue

        announce_date = (announce_dates or {}).get((sym, report_period))
        if announce_date is None:
            if announce_dates is not None:
                fallbacks += 1
            notice_raw = item.get("NOTICE_DATE") or default_notice
            announce_date = date.fromisoformat(str(notice_raw)[:10])

        for (statement_type, item_code), field in report.items.items():
            val = item.get(field)
            if val is None:
                continue
            try:
                item_value = float(val)
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "symbol": sym,
                    "report_period": report_period,
                    "statement_type": statement_type,
                    "item_code": item_code,
                    "item_value": item_value,
                    "announce_date": announce_date,
                }
            )
    return rows, fallbacks


def _fetch_report(
    client: EastMoneyClient,
    report: _Report,
    filter_expr: str,
    *,
    config: Config | None,
) -> list[dict]:
    if config is not None:
        config.rate_limit("eastmoney")
    return fetch_datacenter(
        client,
        report.name,
        report.columns,
        filter_expr=filter_expr,
        page_size=500,
    )


def _announce_date_map(raw: list[dict]) -> dict[tuple[str, str], date]:
    """(symbol, report_period) -> original announcement date, from LICO rows."""
    out: dict[tuple[str, str], date] = {}
    for item in raw:
        sym = symbol_from_secucode(item.get("SECUCODE"))
        period = _report_period(item.get(_ANNOUNCE_SOURCE.report_date_field))
        notice = item.get("NOTICE_DATE")
        if not sym or not period or not notice:
            continue
        try:
            out[(sym, period)] = date.fromisoformat(str(notice)[:10])
        except ValueError:
            continue
    return out


def fetch_financial_statement_items(
    trade_date: date,
    *,
    backfill: bool = False,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
) -> pl.DataFrame:
    """Fetch financial statement items with PIT ``announce_date``.

    ``backfill=False`` (daily): rows whose ``NOTICE_DATE`` equals *trade_date* —
    newly announced reports *and* restatements republished today, both of which
    genuinely became knowable on that date.

    ``backfill=True``: every A-share report for each quarter-end period from
    2001 (or ``config._backfill_start``) through *trade_date* (or
    ``config._backfill_end``). announce_date is resolved from ``RPT_LICO_FN_CPD``
    (see module docstring — the statement reports' own NOTICE_DATE is a
    republication timestamp and lands 1-2 years late).
    """
    owns = client is None
    if client is None:
        client = EastMoneyClient(config=config)

    ds = trade_date.isoformat()
    rows: list[dict] = []
    try:
        if not backfill:
            for report in _REPORTS:
                raw = _fetch_report(client, report, f"(NOTICE_DATE='{ds}')", config=config)
                parsed, _ = _parse_rows(raw, report, default_notice=ds)
                rows.extend(parsed)
        else:
            range_start = getattr(config, "_backfill_start", None) if config else None
            range_end = getattr(config, "_backfill_end", None) if config else None
            for period in _report_period_dates(trade_date, start=range_start, end=range_end):
                announce_raw = _fetch_report(
                    client,
                    _ANNOUNCE_SOURCE,
                    f"({_ANNOUNCE_SOURCE.report_date_field}='{period}')",
                    config=config,
                )
                announce_dates = _announce_date_map(announce_raw)
                parsed, _ = _parse_rows(announce_raw, _ANNOUNCE_SOURCE, default_notice=ds)
                rows.extend(parsed)

                for report in _REPORTS:
                    if report is _ANNOUNCE_SOURCE:
                        continue
                    raw = _fetch_report(
                        client,
                        report,
                        f"({report.report_date_field}='{period}')",
                        config=config,
                    )
                    parsed, fallbacks = _parse_rows(
                        raw,
                        report,
                        default_notice=ds,
                        announce_dates=announce_dates,
                    )
                    if fallbacks:
                        logger.info(
                            "%s %s: %d row(s) had no LICO announcement date; "
                            "using the report's own (later) NOTICE_DATE",
                            report.name,
                            period,
                            fallbacks,
                        )
                    rows.extend(parsed)
    finally:
        if owns:
            client.close()

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).unique(subset=_PK, keep="last")
