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

**Backfill PIT boundary.** A backfill fetch returns whichever version of a figure
EastMoney serves at collection time, which for older periods may be restated.
The step marks those rows as ``eastmoney_backfill``; the reader permits them only
for ``as_of`` dates on or after their actual ``fetched_at`` date. This keeps the
historical table useful after collection without claiming that a current
restatement was knowable on its original announcement date. Vintages accumulated
by daily runs keep the ordinary announcement-date semantics.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta

import polars as pl

from cnequity.adapters.eastmoney.common import (
    _to_float,
    symbol_from_secucode,
)
from cnequity.adapters.eastmoney.common import (
    report_period_from_date as _report_period,
)
from cnequity.adapters.eastmoney.datacenter import fetch_datacenter
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient, rate_limit_if_unconfigured
from cnequity.config import Config
from cnequity.storage.raw_archive import RawPayloadArchive, begin_capture

logger = logging.getLogger(__name__)

# Align with daily_bars research window; CLI --start/--end can clip further.
_BACKFILL_START_YEAR = 2001
_QUARTER_END_MMDD = (("03", "31"), ("06", "30"), ("09", "30"), ("12", "31"))
_MIN_ANNOUNCE_DATE = date(_BACKFILL_START_YEAR, 1, 1)

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

    Optional *start* / *end* clip the walk (CLI ``cne backfill … --start/--end``)
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
        used_fallback = False
        if announce_date is None:
            if announce_dates is not None:
                used_fallback = True
            notice_raw = item.get("NOTICE_DATE") or default_notice
            announce_date = _source_date(notice_raw)
            if announce_date is None or announce_date < _MIN_ANNOUNCE_DATE:
                logger.warning(
                    "financial_statement_items: skipping row with invalid announce date %r",
                    notice_raw,
                )
                continue
        if used_fallback:
            fallbacks += 1

        for (statement_type, item_code), field in report.items.items():
            val = item.get(field)
            if val is None:
                continue
            item_value = _to_float(val)
            if item_value is None:
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
    run_id: str | None = None,
    request_scope: str | None = None,
    archive_source: str = "eastmoney",
    capture_nonce: str | None = None,
) -> list[dict]:
    rate_limit_if_unconfigured(client, config)
    archive = None
    if config is not None and hasattr(config, "meta_root"):
        archive_dataset = "financial_statement_items"
        should_archive = getattr(config, "should_archive_raw", None)
        if should_archive is None or should_archive(archive_dataset):
            archive = RawPayloadArchive(
                config.meta_root,
                enabled=getattr(config, "raw_archive_enabled", False),
                datasets=[archive_dataset],
                compression=getattr(config, "raw_archive_compression", "gzip"),
                max_payload_bytes=getattr(config, "raw_archive_max_payload_bytes", None),
                capture_owner=config,
                capture_run_id=run_id,
                capture_source=archive_source,
                capture_scope=request_scope,
                capture_nonce=capture_nonce,
            )
    return fetch_datacenter(
        client,
        report.name,
        report.columns,
        filter_expr=filter_expr,
        page_size=500,
        # Financial announcements can be inserted while a paged daily report
        # is being read. Keep the truncation/short-page guards, but accept a
        # small upward count drift once all declared pages are read.
        allow_count_overrun=True,
        archive=archive,
        archive_dataset="financial_statement_items",
        archive_run_id=run_id,
        archive_source=archive_source,
        archive_request_scope=request_scope,
    )


def _source_date(value: object) -> date | None:
    text = str(value or "").strip()
    if len(text) < 10:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _rows_for_notice_date(raw: list[dict], expected: date) -> list[dict]:
    rows = [item for item in raw if _source_date(item.get("NOTICE_DATE")) == expected]
    dropped = len(raw) - len(rows)
    if dropped:
        logger.warning(
            "EastMoney financial statements dropped %d row(s) with invalid or "
            "unexpected NOTICE_DATE for %s",
            dropped,
            expected.isoformat(),
        )
    if raw and not rows:
        raise RuntimeError(
            "EastMoney financial statement response contains no NOTICE_DATE row "
            f"for {expected.isoformat()}"
        )
    return rows


def _rows_for_notice_window(raw: list[dict], start: date, end: date) -> list[dict]:
    """Keep only rows disclosed in the requested rolling reconciliation window."""
    rows = [
        item
        for item in raw
        if (notice := _source_date(item.get("NOTICE_DATE"))) is not None and start <= notice <= end
    ]
    if raw and not rows:
        raise RuntimeError(
            "EastMoney financial statement response contains no NOTICE_DATE row for "
            f"{end.isoformat()} within {start.isoformat()}..{end.isoformat()}"
        )
    dropped = len(raw) - len(rows)
    if dropped:
        logger.warning(
            "EastMoney financial statements dropped %d row(s) outside NOTICE_DATE window %s..%s",
            dropped,
            start.isoformat(),
            end.isoformat(),
        )
    return rows


def _rows_for_report_period(raw: list[dict], report: _Report, expected_period: str) -> list[dict]:
    rows = [
        item
        for item in raw
        if _report_period(item.get(report.report_date_field)) == _report_period(expected_period)
    ]
    dropped = len(raw) - len(rows)
    if dropped:
        logger.warning(
            "%s dropped %d row(s) with invalid or unexpected %s for %s",
            report.name,
            dropped,
            report.report_date_field,
            expected_period,
        )
    if raw and not rows:
        raise RuntimeError(
            f"EastMoney {report.name} response contains no "
            f"{report.report_date_field} row for {expected_period}"
        )
    return rows


def _announce_date_map(raw: list[dict]) -> dict[tuple[str, str], date]:
    """(symbol, report_period) -> original announcement date, from LICO rows."""
    out: dict[tuple[str, str], date] = {}
    for item in raw:
        sym = symbol_from_secucode(item.get("SECUCODE"))
        period = _report_period(item.get(_ANNOUNCE_SOURCE.report_date_field))
        notice = item.get("NOTICE_DATE")
        if not sym or not period or not notice:
            continue
        notice_date = _source_date(notice)
        if notice_date is None or notice_date < _MIN_ANNOUNCE_DATE:
            continue
        key = (sym, period)
        # LICO can return more than one row for a period after a restatement.
        # Keep the first disclosure date regardless of source row ordering;
        # using the last row would make PIT results depend on pagination order.
        previous = out.get(key)
        if previous is None or notice_date < previous:
            out[key] = notice_date
    return out


def fetch_financial_statement_items(
    trade_date: date,
    *,
    backfill: bool = False,
    client: EastMoneyClient | None = None,
    config: Config | None = None,
    run_id: str | None = None,
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
    request_scope = f"{'backfill' if backfill else 'daily'}:{trade_date.isoformat()}"
    archive_source = "eastmoney_backfill" if backfill else "eastmoney"
    capture_nonce: str | None = None
    if config is not None and hasattr(config, "meta_root"):
        should_archive = getattr(config, "should_archive_raw", None)
        if should_archive is None or should_archive("financial_statement_items"):
            if bool(getattr(config, "raw_archive_enabled", False)):
                capture_nonce = begin_capture(
                    config,
                    "financial_statement_items",
                    run_id,
                    source=archive_source,
                    request_scope=request_scope,
                )
    rows: list[dict] = []
    try:
        if not backfill:
            from cnequity.domain.datasets import DATASETS

            # A configured pipeline run opts into the registry's rolling
            # reconciliation window.  Keep the lightweight adapter API's
            # historical exact-date behaviour when callers omit ``config``;
            # this is useful for one-off probes and preserves its fail-loud
            # date validation contract.
            lookback = (
                int(
                    getattr(
                        DATASETS.get("financial_statement_items"), "reconciliation_lookback_days", 0
                    )
                    or 0
                )
                if config is not None
                else 0
            )
            notice_start = trade_date - timedelta(days=max(lookback - 1, 0))
            for report in _REPORTS:
                raw = _rows_for_notice_window(
                    _fetch_report(
                        client,
                        report,
                        f"(NOTICE_DATE>='{notice_start.isoformat()}') AND (NOTICE_DATE<='{ds}')",
                        config=config,
                        run_id=run_id,
                        request_scope=request_scope,
                        archive_source=archive_source,
                        capture_nonce=capture_nonce,
                    ),
                    notice_start,
                    trade_date,
                )
                parsed, _ = _parse_rows(raw, report, default_notice=ds)
                rows.extend(parsed)
        else:
            range_start = getattr(config, "_backfill_start", None) if config else None
            range_end = getattr(config, "_backfill_end", None) if config else None
            # A scoped repair asks for a handful of securities, not the market.
            # Four delisted names and one BSE listing owed 149 (symbol, period)
            # balance rows that the licensed peer refuses by code; without this
            # the only way to reach them was 36 whole-market period sweeps.
            scope = list(getattr(config, "_backfill_symbols", None) or []) if config else []
            scope_expr = (
                "(SECUCODE in (" + ",".join(f'"{sym}"' for sym in sorted(scope)) + "))"
                if scope
                else ""
            )
            for period in _report_period_dates(trade_date, start=range_start, end=range_end):
                announce_raw = _fetch_report(
                    client,
                    _ANNOUNCE_SOURCE,
                    f"({_ANNOUNCE_SOURCE.report_date_field}='{period}'){scope_expr}",
                    config=config,
                    run_id=run_id,
                    request_scope=request_scope,
                    archive_source=archive_source,
                    capture_nonce=capture_nonce,
                )
                announce_raw = _rows_for_report_period(announce_raw, _ANNOUNCE_SOURCE, period)
                announce_dates = _announce_date_map(announce_raw)
                parsed, _ = _parse_rows(announce_raw, _ANNOUNCE_SOURCE, default_notice=ds)
                rows.extend(parsed)

                for report in _REPORTS:
                    if report is _ANNOUNCE_SOURCE:
                        continue
                    raw = _fetch_report(
                        client,
                        report,
                        f"({report.report_date_field}='{period}'){scope_expr}",
                        config=config,
                        run_id=run_id,
                        request_scope=request_scope,
                        archive_source=archive_source,
                        capture_nonce=capture_nonce,
                    )
                    raw = _rows_for_report_period(raw, report, period)
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
