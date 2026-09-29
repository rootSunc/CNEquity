"""`backfill` and the chunking, scoping and recovery it needs.

The helpers are the bulk of it: a backfill is one command with several failure
modes that each need their own repair path (symbol-chunked, day-chunked, and
recovering staging an interrupted terminal run left behind).
"""

from __future__ import annotations

import difflib
import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    _run_status_exit_code,
    attach_log_file,
    comma_values,
    config_option,
    parse_date_option,
)
from cnequity.domain.datasets import get_dataset
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine


@cli.command()
@click.argument("dataset")
@config_option
@click.option(
    "--profile",
    type=click.Choice(["default", "delisted"]),
    default="default",
    show_default=True,
    help="daily_bars 可选 delisted：只回填已确认退市名录，沿用独立发现证据。",
)
@click.option(
    "--shfe-annual-archive",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="仅期货/期权日线：从已核验格式的本地上期所年度 ZIP 离线导入。",
)
@click.option("--archive-year", type=click.IntRange(2000, 2100), help="年度 ZIP 对应的交易年份。")
@click.option(
    "--accept-partial-fields",
    is_flag=True,
    help="明确接受年度包缺少日文件独有字段；已有完整日线不会被覆盖。",
)
@click.option("--archive-url", help="可选：该 ZIP 在上期所网站上的原始 HTTPS 链接，仅写证据。")
@click.option("--archive-downloaded-at", help="可选：已知的原始下载时间，带时区 ISO 8601。")
@click.option(
    "--plan", is_flag=True, help="只输出来源、范围和抓取方式；衍生品另给请求预算。不取数、不写湖。"
)
@click.option(
    "--exchange",
    "exchanges",
    multiple=True,
    type=click.Choice(["SHF", "INE", "CZC", "GFE", "DCE", "CFE"], case_sensitive=False),
    help="仅衍生品：限定交易所，可重复。INE 归入 SHF 路由；2018 期货另取 INE 日文件。",
)
@click.option(
    "--refresh",
    is_flag=True,
    help="仅衍生品日线：忽略完成收据与响应缓存，重新核对指定区间；不绕过熔断。",
)
@click.option(
    "--retry-failed",
    is_flag=True,
    help="续跑 sector_bars 回填（跳过 checkpoint 里已写过的板块）。",
)
@click.option(
    "--force",
    is_flag=True,
    help="清掉 sector_bars 回填 checkpoint，重抓全部板块。",
)
@click.option(
    "--start",
    "start_str",
    default=None,
    help=(
        "回填区间起点（YYYY-MM-DD），包括 daily_bars、minute_bars、衍生品日线、"
        "日期/报告期推进及 sector_bars。sector_bars 默认往前 400 天；"
        "超过来源历史深度的范围保留为缺口，可取范围照常交付并提供补数指引。"
    ),
)
@click.option(
    "--end",
    "end_str",
    default=None,
    help=("回填区间终点（YYYY-MM-DD，默认今天），与 --start 配合限定历史窗口。"),
)
@click.option(
    "--outstanding",
    is_flag=True,
    help=(
        "只修复被容忍缺口欠下的那些 key，范围和窗口都取自欠账台账，不看 --symbols/--start/--end。补上的 "
        "key 会销账，仍然缺的继续欠着。"
    ),
)
@click.option(
    "--max-attempts",
    default=10,
    show_default=True,
    help=(
        "仅 --outstanding：单个 key 累计未补齐达到该次数后进入 parked 状态。"
        "后续 --outstanding 默认跳过，但 key 仍留在台账里。"
    ),
)
@click.option(
    "--retry-parked",
    is_flag=True,
    help="仅 --outstanding：忽略尝试上限，把已 parked 的 key 也纳入本次修复。",
)
@click.option(
    "--symbols",
    "symbols_str",
    default=None,
    help=(
        "限定范围的标的列表，逗号分隔：用于 intraday、trading_status、corporate_actions "
        "的限定回填，以及 financial_statement_items、daily_bars、share_structure 的限定修复。trading_status "
        "的 checkpoint 与覆盖证据会记下确切范围；daily_bars 会把这个显式范围写进 backfill 元数据。"
    ),
)
@click.option(
    "--workers",
    default=1,
    show_default=True,
    help="仅 margin_trading 的日期推进并发数。每个请求仍然走配置里共享的源限流器；其它数据集必须为 1。",
)
@click.option(
    "--margin-source",
    type=click.Choice(["exchange", "eastmoney"]),
    default=None,
    help="仅 margin_trading：本次回填使用的来源，不修改配置文件或来源限速。",
)
@click.option(
    "--payment-date-repair",
    is_flag=True,
    help=(
        "仅 corporate_actions：先应用已审发行人公告，再用 Baostock 匹配真实派息日；"
        "早于除息日的付款日视为未知。"
    ),
)
@click.option(
    "--issuer-notice-repair",
    is_flag=True,
    help=(
        "仅 corporate_actions：只用发行人实施公告（已审清单、巨潮、北交所）修复付款日和已审送转条款；"
        "未匹配事件保留缺口，不请求 Baostock。"
    ),
)
@click.option(
    "--baostock-repair",
    is_flag=True,
    help="仅 corporate_actions：用 Baostock 显式修复已退市的沪深标的。",
)
@click.option(
    "--ths-repair",
    is_flag=True,
    help="仅 corporate_actions，历史迁移用：用同花顺补已退市北交所标的的历史分红除权。",
)
@click.option(
    "--eastmoney-bj-repair",
    is_flag=True,
    help="仅 corporate_actions，历史迁移用：通过现行的 920xxx 东财代码补北交所老代码的历史分红除权。",
)
@click.option(
    "--eastmoney-date-repair",
    is_flag=True,
    help=(
        "仅 corporate_actions：按 --ex-dates 指定的除权日向东财逐日要历史除权行。"
        "回补路径的主源是 TDX，东财只有日更的等值过滤能取到 2015-09-29 以前的行。"
    ),
)
@click.option(
    "--ex-dates",
    "ex_dates_str",
    default=None,
    help="配合 --eastmoney-date-repair：逗号分隔的除权日 YYYY-MM-DD。",
)
@click.option(
    "--bse-tip-repair",
    is_flag=True,
    help=(
        "仅 daily_bars，历史迁移用：用北交所官网补已有当期交易日的 BJ 成交额，不重抓 Sina。"
        "日更已以北交所行情板为 BJ 当期主源。"
    ),
)
@click.option(
    "--bj-amount-repair",
    is_flag=True,
    help=(
        "仅 daily_bars，已由 --tdx-amount-repair 取代：从 TDX 补 Sina 从未发布过的北交所成交额，"
        "已存的价格和成交量一律不动。需要 --start/--end。"
    ),
)
@click.option(
    "--tdx-amount-repair",
    is_flag=True,
    help=(
        "仅 daily_bars：新浪补上的沪深北历史行与通达信一起核对，只在开高低收一致且成交量差小于一手时补成交额；"
        "通达信没有的代码保留新浪行。已存的价格和成交量一律不动。需要 --start/--end。"
    ),
)
@click.option(
    "--tdx-volume-repair",
    is_flag=True,
    help=(
        "仅 daily_bars：重读 TDX，只改写已存 TDX 行的成交量（修 2026-09-17 前的解码错误）；"
        "价格须一致，64.5 元以下被放大的成交额一并改写；不新增行。需要 --symbols 和 --start/--end。"
    ),
)
@click.option(
    "--turnover-repair",
    is_flag=True,
    help=(
        "仅 daily_bars：成交额缺失、为 0 或量额单位错位的沪深股票行，用 Baostock 同日行整行替换；"
        "开高低收须在半分钱内一致，不一致或未提供的保留原值。需要 --start/--end。"
    ),
)
@click.option(
    "--fill-em-outage",
    is_flag=True,
    help=(
        "仅 valuation_metrics：东财快照中断时，用东财 datacenter 估值报表补东财最后一个完整日之后、"
        "今天之前的 --start/--end 窗口；全市场取全才写入。"
    ),
)
def backfill(
    dataset: str,
    config_path: str,
    profile: str,
    shfe_annual_archive: Path | None,
    archive_year: int | None,
    accept_partial_fields: bool,
    archive_url: str | None,
    archive_downloaded_at: str | None,
    plan: bool,
    exchanges: tuple[str, ...],
    refresh: bool,
    retry_failed: bool,
    force: bool,
    start_str: str | None,
    end_str: str | None,
    symbols_str: str | None,
    outstanding: bool,
    workers: int,
    margin_source: str | None,
    baostock_repair: bool,
    payment_date_repair: bool,
    issuer_notice_repair: bool,
    ths_repair: bool,
    eastmoney_bj_repair: bool,
    eastmoney_date_repair: bool,
    ex_dates_str: str | None,
    bse_tip_repair: bool,
    bj_amount_repair: bool,
    tdx_amount_repair: bool,
    tdx_volume_repair: bool,
    turnover_repair: bool,
    fill_em_outage: bool,
    max_attempts: int,
    retry_parked: bool,
):
    """回填一个数据集。

    \b
    成本按源的计费单位算，不按窗口算。daily_bars 是逐标的抓取，所以 `--start D --end D`
    和多年窗口一样要扫一遍全市场 —— 一个交易日不等于一个请求。
    只想快速验证而不是跑全市场时，用 `--symbols` 缩小范围。
    """
    dataset = _require_known_dataset(dataset)
    if profile == "delisted":
        if dataset != "daily_bars":
            raise click.ClickException("--profile delisted 只适用于 daily_bars")
        if (
            end_str
            or symbols_str
            or outstanding
            or exchanges
            or refresh
            or retry_failed
            or force
            or workers != 1
            or margin_source
            or baostock_repair
            or payment_date_repair
            or issuer_notice_repair
            or ths_repair
            or eastmoney_bj_repair
            or eastmoney_date_repair
            or ex_dates_str
            or bse_tip_repair
            or bj_amount_repair
            or tdx_amount_repair
            or tdx_volume_repair
            or turnover_repair
            or fill_em_outage
            or shfe_annual_archive
            or archive_year
            or accept_partial_fields
            or archive_url
            or archive_downloaded_at
        ):
            raise click.ClickException("--profile delisted 只接受 --start、--plan 与 --config")
        since = parse_date_option(start_str or "2016-01-01", "--start")
        if plan:
            click.echo(
                json.dumps(
                    {
                        "dataset": dataset,
                        "profile": profile,
                        "since": since.isoformat(),
                        "source": "sina",
                        "mode": "confirmed_delisted_catalog",
                        "note": "执行时按退市名录的待补标的续跑；请求数取决于名录和已完成收据",
                    },
                    indent=2,
                )
            )
            return
        cfg = _cfg(config_path)
        attach_log_file(cfg, "delisted-backfill")
        window = _bar_lineage_window(since, None)
        bars_before = _bar_fingerprint(cfg, window)
        result = _run_delisted_profile(cfg, since)
        if result.get("status") != "failed":
            _follow_lineage(result, lambda: _after_daily_bars(cfg, bars_before, window, result))
        click.echo(json.dumps(result, indent=2, default=str))
        if _run_status_exit_code(result["status"]):
            raise click.ClickException("delisted recovery execution or publication failed")
        return
    if shfe_annual_archive is not None:
        if dataset not in {"futures_bars", "option_bars"}:
            raise click.ClickException("--shfe-annual-archive 只适用于 futures_bars/option_bars")
        if archive_year is None or not accept_partial_fields:
            raise click.ClickException(
                "年度包缺少日文件字段；必须指定 --archive-year 和 --accept-partial-fields"
            )
        if archive_url:
            parsed_url = urlsplit(archive_url)
            host = parsed_url.hostname or ""
            if parsed_url.scheme != "https" or not (
                host == "shfe.com.cn" or host.endswith(".shfe.com.cn")
            ):
                raise click.ClickException("--archive-url 必须是上期所的 HTTPS 链接")
        if archive_downloaded_at:
            try:
                downloaded = datetime.fromisoformat(archive_downloaded_at.replace("Z", "+00:00"))
            except ValueError as exc:
                raise click.ClickException("--archive-downloaded-at 需要 ISO 8601 时间") from exc
            if downloaded.tzinfo is None:
                raise click.ClickException("--archive-downloaded-at 必须带时区")
            archive_downloaded_at = downloaded.isoformat()
        if (
            profile != "default"
            or symbols_str
            or outstanding
            or exchanges
            or refresh
            or retry_failed
            or force
            or workers != 1
            or margin_source
            or baostock_repair
            or payment_date_repair
            or issuer_notice_repair
            or ths_repair
            or eastmoney_bj_repair
            or eastmoney_date_repair
            or ex_dates_str
            or bse_tip_repair
            or bj_amount_repair
            or tdx_amount_repair
            or tdx_volume_repair
            or turnover_repair
            or fill_em_outage
        ):
            raise click.ClickException("年度包导入只接受 --start/--end/--plan 和归档参数")
        start = parse_date_option(start_str, "--start") if start_str else None
        end = parse_date_option(end_str, "--end") if end_str else None
        first, last = date(archive_year, 1, 1), date(archive_year, 12, 31)
        if (start and not first <= start <= last) or (end and not first <= end <= last):
            raise click.ClickException("--start/--end 必须落在 --archive-year 内")
        if start and end and start > end:
            raise click.ClickException("--start 不得晚于 --end")
        if plan:
            click.echo(
                json.dumps(
                    {
                        "dataset": dataset,
                        "archive": str(shfe_annual_archive),
                        "year": archive_year,
                        "start": str(start or first),
                        "end": str(end or last),
                        "mode": "offline_partial_fields",
                        "existing_daily_rows": "protected",
                        "network_requests": 0,
                    },
                    indent=2,
                )
            )
            return
        cfg = _cfg(config_path)
        attach_log_file(cfg, "shfe-annual-import")
        result = _run_shfe_annual_archive(
            cfg,
            dataset,
            shfe_annual_archive,
            archive_year,
            start=start,
            end=end,
            source_url=archive_url,
            downloaded_at=archive_downloaded_at,
        )
        click.echo(json.dumps(result, indent=2, default=str))
        if result["status"] != "success":
            raise click.ClickException("年度包有未验证成员或行；已保存有效切片与缺口")
        return
    if archive_year is not None or accept_partial_fields or archive_url or archive_downloaded_at:
        raise click.ClickException("年度归档选项需要 --shfe-annual-archive ZIP")
    symbols = comma_values(symbols_str, "--symbols")
    if symbols is not None:
        symbols = list(dict.fromkeys(s.upper() for s in symbols))
        symbols_str = ",".join(symbols)
    cfg = _cfg(config_path)
    derivatives = {
        "futures_bars",
        "option_bars",
        "futures_contracts",
        "option_contracts",
        "futures_minute_bars",
    }
    if (exchanges or refresh) and dataset not in derivatives:
        raise click.ClickException("--exchange / --refresh 只适用于衍生品采集数据集")
    if (force or retry_failed) and dataset != "sector_bars":
        raise click.ClickException(
            "--force / --retry-failed 只适用于 sector_bars；衍生品重取请用 --refresh"
        )
    if dataset in derivatives:
        cfg.futures_enabled = True
        if dataset.startswith("option_"):
            cfg.futures_options = True
        if exchanges:
            cfg.futures_exchanges = [e.upper() for e in exchanges]
        if refresh and dataset not in {"futures_bars", "option_bars"}:
            raise click.ClickException("--refresh 只适用于 futures_bars / option_bars")
        if outstanding:
            raise click.ClickException(
                "衍生品欠账由日更持久重试；指定 --start / --end 可修复历史区间"
            )
        if dataset == "futures_minute_bars" and (start_str or end_str):
            raise click.ClickException(
                "futures_minute_bars 只能采集近期窗口，不能按 --start / --end 请求历史"
            )
        if symbols_str and dataset != "futures_minute_bars":
            raise click.ClickException(
                "衍生品日线按交易所文件取数；使用 --exchange，不支持 --symbols"
            )
        import uuid

        cfg._derivative_batch = uuid.uuid4().hex
        cfg._derivatives_refresh = refresh
    if margin_source:
        if dataset != "margin_trading":
            raise click.ClickException("--margin-source 只适用于 margin_trading")
        cfg.margin_trading_source = margin_source
    if payment_date_repair or issuer_notice_repair:
        if dataset != "corporate_actions" or not symbols_str or not start_str or not end_str:
            raise click.ClickException(
                "付款日修复需要 corporate_actions、--symbols、--start 和 --end"
            )
        if payment_date_repair and issuer_notice_repair:
            raise click.ClickException("两种付款日修复模式不能同时使用")
        if any(
            (baostock_repair, ths_repair, eastmoney_bj_repair, eastmoney_date_repair, outstanding)
        ):
            raise click.ClickException("付款日修复必须独立运行")
        cfg._corporate_actions_payment_repair = True
        cfg._corporate_actions_issuer_notice_only = issuer_notice_repair
    if workers < 1:
        raise click.ClickException("--workers 至少为 1")
    if workers > 1 and dataset != "margin_trading":
        raise click.ClickException(
            "--workers > 1 目前只支持 margin_trading；其它回填只用一条日期推进通道"
        )
    if baostock_repair and dataset != "corporate_actions":
        raise click.ClickException("--baostock-repair 只适用于 corporate_actions")
    if ths_repair and dataset != "corporate_actions":
        raise click.ClickException("--ths-repair 只适用于 corporate_actions")
    if eastmoney_bj_repair and dataset != "corporate_actions":
        raise click.ClickException("--eastmoney-bj-repair 只适用于 corporate_actions")
    if eastmoney_date_repair and dataset != "corporate_actions":
        raise click.ClickException("--eastmoney-date-repair 只适用于 corporate_actions")
    if ex_dates_str and not eastmoney_date_repair:
        raise click.ClickException("--ex-dates 需要配合 --eastmoney-date-repair")
    if bse_tip_repair and dataset != "daily_bars":
        raise click.ClickException("--bse-tip-repair 只适用于 daily_bars")
    if bj_amount_repair and dataset != "daily_bars":
        raise click.ClickException("--bj-amount-repair 只适用于 daily_bars")
    if tdx_amount_repair and dataset != "daily_bars":
        raise click.ClickException("--tdx-amount-repair 只适用于 daily_bars")
    if bj_amount_repair and tdx_amount_repair:
        raise click.ClickException("--bj-amount-repair 与 --tdx-amount-repair 只能用一个")
    if bj_amount_repair:
        click.echo(
            "提示：--bj-amount-repair 已由 --tdx-amount-repair 取代，"
            "后者按开高低收与成交量核对后才补成交额，并覆盖沪深北。",
            err=True,
        )
    if baostock_repair:
        cfg._corporate_actions_baostock_repair = True
    if ths_repair:
        cfg._corporate_actions_ths_repair = True
    if eastmoney_bj_repair:
        cfg._corporate_actions_eastmoney_bj_repair = True
    if eastmoney_date_repair:
        raw_dates = [d.strip() for d in (ex_dates_str or "").split(",") if d.strip()]
        if not raw_dates:
            raise click.ClickException("--eastmoney-date-repair 需要 --ex-dates")
        seen = {parse_date_option(value, "--ex-dates") for value in raw_dates}
        cfg._corporate_actions_eastmoney_date_repair = sorted(seen)
    if dataset == "sector_bars":
        if retry_failed and force:
            raise click.ClickException("--retry-failed 和 --force 只能用一个。")
        cfg._sector_bars_force = force
    start_d = parse_date_option(start_str, "--start")
    end_d = parse_date_option(end_str, "--end")
    if start_d and end_d and start_d > end_d:
        # Transposing the two used to cost a full network sweep: the walk had no
        # days in it, the step raised, the engine logged the traceback, and the
        # command still printed status=success with rows_written=0. `derive`,
        # `verify --bars` and `audit` all refuse this up front; so does this now.
        raise click.ClickException("--start 必须早于或等于 --end")
    if bj_amount_repair:
        if start_d is None or end_d is None:
            raise click.ClickException("--bj-amount-repair 需要同时给 --start 和 --end")
        cfg._bj_amount_repair = True
    if tdx_amount_repair:
        if start_d is None or end_d is None:
            raise click.ClickException("--tdx-amount-repair 需要同时给 --start 和 --end")
        cfg._tdx_amount_repair = True
    if tdx_volume_repair:
        if dataset != "daily_bars":
            raise click.ClickException("--tdx-volume-repair 只适用于 daily_bars")
        if not symbols_str or start_d is None or end_d is None:
            raise click.ClickException("--tdx-volume-repair 需要 --symbols 和 --start/--end")
        cfg._tdx_volume_repair = True
    if turnover_repair:
        if dataset != "daily_bars":
            raise click.ClickException("--turnover-repair 只适用于 daily_bars")
        if start_d is None or end_d is None:
            raise click.ClickException("--turnover-repair 需要同时给 --start 和 --end")
        cfg._turnover_repair = True
    if fill_em_outage:
        if dataset != "valuation_metrics":
            raise click.ClickException("--fill-em-outage 只适用于 valuation_metrics")
        if start_d is None or end_d is None:
            raise click.ClickException("--fill-em-outage 需要同时给 --start 和 --end")
        cfg._valuation_fill_em_outage = True
    if bse_tip_repair:
        if not symbols_str:
            raise click.ClickException("--bse-tip-repair 需要 --symbols")
        if start_d is None or end_d is None or start_d != end_d:
            raise click.ClickException("--bse-tip-repair 需要显式给出同一天的 --start 和 --end")
        cfg._bse_tip_repair = True
    if outstanding:
        if plan:
            raise click.ClickException(
                "--plan 请指定数据集范围；--outstanding 使用欠账台账执行修复"
            )
        if symbols_str or start_d or end_d:
            raise click.ClickException(
                "--outstanding 的范围取自欠账台账；请去掉 --symbols/--start/--end"
            )
        attach_log_file(cfg, f"backfill-{dataset}")
        if max_attempts < 1:
            raise click.ClickException("--max-attempts 至少为 1")
        result = _repair_outstanding(
            cfg,
            dataset,
            workers,
            max_attempts=max_attempts,
            retry_parked=retry_parked,
        )
        click.echo(json.dumps(result, indent=2, default=str))
        if code := _run_status_exit_code(result["status"]):
            raise SystemExit(code)
        return
    if symbols is not None:
        if dataset in (
            "daily_bars",
            "trading_status",
            "corporate_actions",
            "financial_statement_items",
            "share_structure",
        ):
            cfg._backfill_symbols = symbols
        else:
            _override_scope(cfg, dataset, symbols)
        if not plan:
            click.echo(f"[{dataset}] 本次 run 的范围被覆盖为 {len(symbols)} 只标的", err=True)
    if start_d:
        cfg._backfill_start = start_d
    if end_d:
        cfg._backfill_end = end_d
    cfg._backfill_workers = workers
    if plan:
        repair_modes = [
            flag
            for enabled, flag in (
                (payment_date_repair, "payment-date-repair"),
                (issuer_notice_repair, "issuer-notice-repair"),
                (baostock_repair, "baostock-repair"),
                (ths_repair, "ths-repair"),
                (eastmoney_bj_repair, "eastmoney-bj-repair"),
                (eastmoney_date_repair, "eastmoney-date-repair"),
                (bse_tip_repair, "bse-tip-repair"),
                (bj_amount_repair, "bj-amount-repair"),
                (tdx_amount_repair, "tdx-amount-repair"),
                (tdx_volume_repair, "tdx-volume-repair"),
                (turnover_repair, "turnover-repair"),
                (fill_em_outage, "fill-em-outage"),
                (force, "force"),
                (retry_failed, "retry-failed"),
            )
            if enabled
        ]
        payload = (
            _derivatives_plan(cfg, dataset, start_str, end_str, symbols_str)
            if dataset in derivatives
            else _backfill_plan(cfg, dataset, start_d, end_d, symbols, workers, repair_modes)
        )
        click.echo(json.dumps(payload, indent=2, default=str, ensure_ascii=False))
        return
    attach_log_file(cfg, f"backfill-{dataset}")
    if dataset == "trading_status":
        # The per-run bound exists so `cne init` is not held behind baostock's
        # pacing for ten hours. Asking for this backfill by name *is* asking to
        # sit through it, and silently stopping at 400 symbols would look like
        # the command had finished the job.
        cfg.st_history_symbols_per_run = 0

    spec = get_dataset(dataset)
    # Explicit lineage: fingerprint the inputs a derive reads before fetching,
    # so the follow-up below derives exactly what this backfill changed.
    actions_before = bars_before = bar_window = None
    if dataset == "corporate_actions":
        from cnequity.derive.lineage import corporate_action_fingerprint

        actions_before = corporate_action_fingerprint(cfg)
    elif dataset == "daily_bars":
        bar_window = _bar_lineage_window(start_d, end_d)
        bars_before = _bar_fingerprint(cfg, bar_window)
    # Offset-paged sources (intraday) chunk by symbol, not by date, so each
    # symbol pays for locating the window only once.
    if spec.backfill_chunk_symbols and start_d and end_d:
        result = _backfill_symbol_chunked(cfg, dataset, start_d, end_d, spec.backfill_chunk_symbols)
    elif spec.backfill_chunk_days and start_d and end_d:
        result = _backfill_chunked(cfg, dataset, start_d, end_d, spec.backfill_chunk_days)
    else:
        result = _backfill_once(cfg, dataset)
    if (
        dataset in {"futures_bars", "option_bars", "futures_contracts", "option_contracts"}
        and result.get("status") != "failed"
    ):
        steps = _derivatives_followup(dataset)
        followup = JobEngine(cfg).run_job("derivatives-rebuild", steps=steps, backfill=True)
        result["followup"] = followup
        if followup.get("status") != "success":
            result["status"] = followup.get("status", "degraded")
    if actions_before is not None and result.get("status") != "failed":
        result["adj_factors_sync"] = _follow_lineage(
            result, lambda: _after_corporate_actions(cfg, actions_before, result)
        )
    if bars_before is not None and result.get("status") != "failed":
        _follow_lineage(result, lambda: _after_daily_bars(cfg, bars_before, bar_window, result))
    if outstanding:
        result["outstanding"] = _settle_outstanding(cfg, dataset)
    click.echo(json.dumps(result, indent=2, default=str))
    code = _run_status_exit_code(result["status"])
    if code:
        raise SystemExit(code)


def _follow_lineage(result: dict, derive) -> dict:
    """Run a downstream derive; its failure degrades the backfill, never hides it.

    The fetched rows are already published by this point, so the command's own
    output must still reach the caller with the derive's failure recorded.
    """
    try:
        summary = derive()
    except Exception as exc:  # noqa: BLE001 — report it alongside the published fetch
        logging.getLogger(__name__).exception("downstream derive failed")
        click.echo(f"下游派生失败：{type(exc).__name__}: {exc}", err=True)
        summary = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
    if (
        summary.get("status") in {"failed", "degraded", "warning"}
        or summary.get("synchronized") is False
    ) and result.get("status") == "success":
        result["status"] = "degraded"
    return summary


def _after_corporate_actions(cfg, before, result: dict) -> dict:
    """Refetch and check factors for the symbols whose action terms changed."""
    from cnequity.derive.lineage import changed_symbols, corporate_action_fingerprint

    affected = changed_symbols(before, corporate_action_fingerprint(cfg))
    click.echo("\n公司行为回填完成", err=True)
    click.echo(f"  写入：            {int(result.get('rows_written') or 0):,} 条", err=True)
    click.echo(f"  受影响标的：      {len(affected):,}", err=True)
    if not affected:
        click.echo("✓ 公司行为没有改变任何复权因子输入，无需重算", err=True)
        return {"affected_symbols": 0, "synchronized": True}
    return _sync_adj_factors(
        cfg,
        affected,
        realign=False,
        input_label="公司行为",
        rerun="cne backfill corporate_actions --symbols",
    )


def _bar_lineage_window(start: date | None, end: date | None) -> tuple[date, date]:
    """The bar range a daily_bars backfill can touch: its own default when unset."""
    from cnequity.steps.common import BACKFILL_START

    return start or BACKFILL_START, end or shanghai_today()


def _bar_fingerprint(cfg, window: tuple[date, date]):
    from cnequity.derive.lineage import daily_bar_fingerprint

    return daily_bar_fingerprint(cfg, *window)


def _after_daily_bars(cfg, before, window: tuple[date, date], result: dict) -> dict:
    """Realign factors and re-derive suspensions over the bars that changed.

    Same lineage as corporate actions: the next daily run would only realign a
    capped batch of symbols with history the factor table does not reach, and
    no daily step re-derives suspensions at all.
    """
    from cnequity.derive.lineage import changed_bar_scope
    from cnequity.steps.reference import DERIVE_TAIL_DAYS

    affected, first, last = changed_bar_scope(before, _bar_fingerprint(cfg, window))
    click.echo("\n日线回填完成", err=True)
    click.echo(f"  写入：            {int(result.get('rows_written') or 0):,} 行", err=True)
    click.echo(f"  受影响标的：      {len(affected):,}", err=True)
    if not affected:
        click.echo("✓ 日线没有变化，复权因子与停牌无需重算", err=True)
        summary = {"affected_symbols": 0, "synchronized": True}
        result["adj_factors_sync"] = summary
        return summary
    click.echo(f"  变化月份：        {first:%Y-%m} .. {last:%Y-%m}", err=True)
    result["adj_factors_sync"] = _follow_lineage(
        result,
        lambda: _sync_adj_factors(
            cfg,
            affected,
            realign=True,
            input_label="日线",
            rerun="cne backfill daily_bars --symbols",
        ),
    )
    # A halt shows only as a gap between two traded bars, so the window reaches
    # back past the first changed month by the derive's own tail.
    month_end = (last.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    start = first - timedelta(days=DERIVE_TAIL_DAYS)
    end = min(month_end, window[1])
    result["trading_status_derive"] = _follow_lineage(
        result, lambda: _derive_suspensions(cfg, start, end)
    )
    return {"affected_symbols": len(affected), "status": result.get("status")}


def _sync_adj_factors(
    cfg, affected: list[str], *, realign: bool, input_label: str, rerun: str
) -> dict:
    """Derive factors for *affected*, then check each reaches its latest traded bar.

    Explicit lineage instead of hoping the next daily run notices: see
    `cnequity.derive.lineage`. *realign* keeps cached factors (bars changed,
    factors did not); otherwise the factors are refetched. A symbol left
    unsynchronized degrades the command.
    """
    from cnequity.cli.maintain_cmds import _published_derive
    from cnequity.derive.adj_factors import compute_adj_factors
    from cnequity.derive.lineage import verify_factor_sync

    click.echo("\n派生复权因子…", err=True)
    with _published_derive(cfg, "adj_factors") as outcome:
        derived = (
            compute_adj_factors(cfg, realign_symbols=affected)
            if realign
            else compute_adj_factors(cfg, refresh_symbols=affected)
        )
        outcome["rows_written"] = derived.rows
        if derived.failed:
            outcome["status"] = "degraded"
    sync = verify_factor_sync(cfg, affected, failed=derived.failed, rows=derived.rows)
    reasons = {"no_bars": "无日线", "cdr": "CDR"}
    skipped = ", ".join(
        f"{reasons[r]} {sum(v == r for v in sync.skipped.values())}"
        for r in reasons
        if r in sync.skipped.values()
    )
    click.echo(f"  处理：            {len(affected):,}", err=True)
    click.echo(f"  更新：            {len(sync.updated):,}", err=True)
    click.echo(
        f"  跳过：            {len(sync.skipped):,}" + (f"（{skipped}）" if skipped else ""),
        err=True,
    )
    if sync.synchronized:
        click.echo(f"\n✓ {input_label}与复权因子已同步", err=True)
    else:
        behind = sorted(sync.failed + sync.lagging)
        preview = ",".join(behind[:20])
        click.echo(
            f"  失败：            {len(sync.failed):,}\n"
            f"  未覆盖最新日线：  {len(sync.lagging):,}\n"
            f"\n✗ {len(behind)} 只标的的复权因子没有跟上{input_label}；"
            f"重跑：cne derive adj_factors 或 {rerun} {preview}",
            err=True,
        )
    return sync.as_dict()


def _derive_suspensions(cfg, start: date, end: date) -> dict:
    """Reconstruct suspensions over the bar window a backfill changed.

    Bar-gap suspensions are not part of the lean daily core: the daily
    `trading_status` snapshot records each session's halts. Only a change to
    bar history can reveal suspensions nobody snapshotted, so the derivation
    follows the command that changes it.
    """
    from cnequity.cli.maintain_cmds import _derive_trading_status

    summary = _derive_trading_status(cfg, start=start, end=end)
    click.echo(
        f"\n已按 {start.isoformat()}..{end.isoformat()} 的日线重算停牌："
        f"{int(summary.get('rows_staged') or 0):,} 行（{summary.get('status')}）",
        err=True,
    )
    return summary


def _backfill_plan(cfg, dataset, start, end, symbols, workers, repair_modes) -> dict:
    from cnequity.diagnostics.source_limits import effective_source_policy
    from cnequity.domain.datasets import history_mode_for
    from cnequity.domain.http_policy import cooldown_status, source_family
    from cnequity.domain.rate_limit import _read_json

    spec = get_dataset(dataset)
    registered = {
        "primary": spec.primary_source,
        "backup": spec.backup_source,
        "backfill": spec.backfill_source,
        "supplementary": list(spec.supplementary_sources),
        "repair_only": list(spec.repair_sources),
    }
    names = {
        name
        for name in (
            spec.primary_source,
            spec.backup_source,
            spec.backfill_source,
            *spec.supplementary_sources,
            *spec.repair_sources,
        )
        if name
    }
    statuses = {}
    for name in sorted(names):
        family = source_family(name)
        statuses[name] = {
            "enabled": cfg.sources.get(name, True)
            and cfg.sources.get(family, True)
            and (not family.startswith("eastmoney") or cfg.sources.get("eastmoney", True))
            and (family != "tdx_protocol" or cfg.tdx_enabled),
            **cooldown_status(cfg.rate_limit_root, name),
            **effective_source_policy(cfg, name, names),
        }
    if dataset == "margin_trading":
        routing = {
            "selected": cfg.margin_trading_source,
            "source_enabled": statuses.get(cfg.margin_trading_source, {}).get("enabled"),
            "confidence": "configured",
        }
    elif dataset == "daily_bars" and symbols:
        routing = {
            "markets": {
                market: sorted(s for s in symbols if s.endswith(f".{market}"))
                for market in ("SH", "SZ", "BJ")
                if any(s.endswith(f".{market}") for s in symbols)
            },
            "confidence": "market-scope-only; actual fallback depends on gaps and source responses",
        }
    else:
        routing = {"confidence": "actual source depends on gaps, route policy and responses"}
    state = _read_json(cfg.meta_root / "state" / f"{dataset}.json")
    outstanding = state.get("outstanding_keys")
    cold_minimum = None
    broad_tip = None
    if dataset == "daily_bars" and symbols:
        cold_minimum = sum(not s.endswith(".BJ") for s in symbols) + int(
            any(s.endswith(".BJ") for s in symbols)
        )
        scoped_history = start is not None and end is not None and start < end
        broad_tip = {
            "exchange": "skip"
            if scoped_history or sum(not s.endswith(".BJ") for s in symbols) <= 4
            else "eligible",
            "bse": "skip"
            if scoped_history or sum(s.endswith(".BJ") for s in symbols) <= 4
            else "eligible",
            "reason": "显式历史或最多 4 只标的时跳过全市场快照；按标的请求仍受共享限流约束",
        }
    return {
        "dataset": dataset,
        "data_root": str(cfg.data_root),
        "start": start,
        "end": end,
        "window_note": "未指定的边界由数据集的增量水位和历史能力决定；计划不创建水位或请求网络",
        "symbols": symbols,
        "scope": f"显式 {len(symbols)} 只" if symbols else "数据集默认范围，可能是全市场",
        "fetch_semantics": spec.fetch_semantics,
        "history_mode": history_mode_for(spec),
        "history_note": (
            "来源只有当前快照；历史请求正常报告能力限制，保留已有数据并给出日更采集方案"
            if history_mode_for(spec) == "snapshot_only"
            else "超出来源能力的范围记为覆盖缺口；执行时获取来源仍可提供的范围"
        ),
        "registered_sources": registered,
        "source_status": statuses,
        "routing": routing,
        "repair_modes": repair_modes,
        "workers": workers,
        "checkpoint": {
            "watermark": state.get("watermark"),
            "outstanding_keys": len(outstanding) if isinstance(outstanding, list) else 0,
        },
        "margin_source": cfg.margin_trading_source if dataset == "margin_trading" else None,
        "source_note": "登记源用于说明能力；逐市场路由和显式修复模式见取数指南，实际来源写入数据行",
        "chunk_symbols": spec.backfill_chunk_symbols,
        "chunk_days": spec.backfill_chunk_days,
        "earliest_available": spec.earliest_available(shanghai_today()),
        "requests": None,
        "cold_request_lower_bound": cold_minimum,
        "broad_tip_snapshots": broad_tip,
        "tdx_history_window": {
            "strategy": "exponential-bracket-binary-seek-then-scan",
            "applies_when": "TDX K 线回填且 end 早于最新完整页；其他来源沿用各自策略",
            "cost": "每标的：对数级定位页 + 目标窗口页；跳页后额外校验一次最新页",
            "limits": "仅限源仍保留的历史；未知日期退回顺序读取，失败不提交该标的的部分结果",
        }
        if dataset in {"daily_bars", "index_bars", "minute_bars", "minute_bars_5m"}
        else None,
        "cost_note": "分页、缺口和缓存决定请求数；单日窗口不等于单个请求。先用少量 --symbols 验证",
        "pacing_seconds": cfg.source_intervals,
        "rate_limit_root": str(cfg.rate_limit_root),
        "writes": False,
    }


def _derivatives_followup(dataset: str) -> list[str]:
    if dataset in {"futures_bars", "futures_contracts"}:
        return ["futures_contracts", "compact", "derive_futures_continuous", "derive_option_greeks"]
    return ["option_contracts", "compact", "derive_option_greeks"]


def _derivatives_plan(cfg, dataset, start_str, end_str, symbols_str) -> dict:
    from cnequity.adapters.futures_exchange import shfe
    from cnequity.adapters.futures_exchange.registry import capabilities, enabled_exchanges, reader
    from cnequity.adapters.sina.dce_futures import candidate_codes
    from cnequity.query.calendar import list_trading_dates
    from cnequity.steps.derivatives import minute_scope

    start = parse_date_option(start_str, "--start")
    end = parse_date_option(end_str, "--end") or shanghai_today()
    if start and start > end:
        raise click.ClickException("--start 必须早于或等于 --end")
    if symbols_str:
        _override_scope(
            cfg, dataset, [s.strip().upper() for s in symbols_str.split(",") if s.strip()]
        )
    if dataset == "futures_minute_bars":
        symbols = minute_scope(cfg)
        return {
            "dataset": dataset,
            "contracts": symbols,
            "requests_cold": len(symbols),
            "limit": "每合约近期约 1023 根；仅启用后积累，不能历史回填",
            "writes": False,
        }
    kind = "options" if dataset.startswith("option_") else "futures"
    routes = []
    for exchange in enabled_exchanges(cfg):
        route = reader(cfg, exchange)
        floor = route.first_session(kind)
        if floor is None:
            routes.append({"exchange": exchange, "supported": False})
            continue
        lo = max(start or floor, floor)
        days = list_trading_dates(cfg, lo, end) if lo <= end else []
        reference_cost = (
            len(days) if route.fetch_reference is not None and route.reference_history else 0
        )
        if dataset.endswith("_contracts"):
            cost = (
                reference_cost
                if (start_str or end_str)
                else (1 if route.fetch_reference is not None else 0)
            )
        elif exchange == "DCE" and cfg.futures_dce_route == "sina":
            cost = len({code for day in days for code in candidate_codes(day)}) + 3
        else:
            cost = len(days) * (17 if exchange == "DCE" and kind == "options" else 1)
            if exchange == "SHF" and dataset == "futures_bars":
                cost += sum(
                    shfe.INE_FIRST_SESSION <= day <= shfe.INE_DIRECT_LAST_SESSION for day in days
                )
        if dataset.endswith("_contracts") and exchange == "GFE" and not (start_str or end_str):
            cost = None
        routes.append(
            {
                "exchange": exchange,
                "supported": True,
                "start": lo,
                "end": end,
                "sessions": len(days),
                "cold_request_estimate": cost,
                "reference_requests_upper_bound": reference_cost
                if (start_str or end_str)
                else (None if exchange == "GFE" else (1 if route.fetch_reference else 0)),
                "reference_budget_note": "GFEX current snapshot needs one product-list request plus one request per product; no fixed upper bound"
                if exchange == "GFE"
                else "one reference file per observed session",
            }
        )
    return {
        "dataset": dataset,
        "routes": routes,
        "source_capabilities": capabilities(),
        "refresh": bool(getattr(cfg, "_derivatives_refresh", False)),
        "cache": "持久响应缓存；收据匹配才跳过。行情预算不含重试；参考文件另列上界（仅有行情的日期读取）。实际缓存命中需按完整请求键检查",
        "pacing_seconds": {"futures_exchange": 1.0, "sina": 0.3, **cfg.source_intervals},
        "followup_steps": _derivatives_followup(dataset),
        "quality": "续跑完成不代表全市场完整；DCE 新浪缺零成交日，期权套利缺同步盘口",
        "writes": False,
    }


def _repair_outstanding(
    cfg,
    dataset: str,
    workers: int,
    *,
    max_attempts: int = 10,
    retry_parked: bool = False,
) -> dict:
    """Refetch exactly what the ledger says is owed, a month at a time.

    Owed keys are scatter, not a range: measured on a real init, 5,037 keys sat
    across 833 symbols and 692 sessions, a median of 5 keys and 11 days per
    symbol. Asking for one window spanning all of them would fetch ~624,750
    keys to repair 5,037 — the same disproportion the tolerance exists to
    avoid, in the command meant to undo it. Bucketing by month costs ~32,476 in
    37 calls; per-session would be exact but 692 engine runs to save 27k
    fetches, which is the wrong trade.
    """
    from collections import defaultdict

    from cnequity.orchestrator.manifest import Manifest
    from cnequity.steps.bars import _last_final_session
    from cnequity.storage import state as state_module

    # A later ordinary run may already have published many owed keys. Reconcile
    # against committed rows before grouping months, or those keys would drive
    # unnecessary source requests. This is not a repair attempt for missing keys.
    _settle_outstanding(cfg, dataset, note_missing_attempt=False)
    owed = state_module.StateStore(cfg.meta_root).get_outstanding_keys(dataset)
    if not owed:
        return {"dataset": dataset, "status": "success", "outstanding": 0, "note": "nothing owed"}
    limit = max(1, int(max_attempts))
    parked = [] if retry_parked else [
        row
        for row in owed
        if int(row.get("attempts", 0) or 0) >= limit
    ]
    active = owed if retry_parked else [
        row
        for row in owed
        if int(row.get("attempts", 0) or 0) < limit
    ]
    if parked:
        click.echo(
            f"[{dataset}] {len(parked)} 个 key 已累计 {limit} 次未补齐；"
            "本次跳过，key 仍在台账中（--retry-parked 可强制重试）",
            err=True,
        )
    if not active:
        return {
            "dataset": dataset,
            "status": "success",
            "outstanding": len(owed),
            "parked": len(parked),
            "parked_after_attempts": limit,
            "note": "all outstanding keys are parked; use --retry-parked to retry them",
        }

    # A key for a session that has not closed yet would make its whole monthly
    # pass fail the finality guard, and every other key in that month with it:
    # defer those to their own later repair instead.
    final = _last_final_session().isoformat() if dataset == "daily_bars" else None
    buckets: dict[str, set[str]] = defaultdict(set)
    days_in: dict[str, list[str]] = defaultdict(list)
    deferred = 0
    for row in active:
        symbol, day = row.get("symbol"), row.get("trade_date")
        if not symbol or not day:
            continue
        if final and day > final:
            deferred += 1
            continue
        buckets[day[:7]].add(symbol)
        days_in[day[:7]].append(day)
    if deferred:
        click.echo(
            f"[{dataset}] 有 {deferred} 个 key 属于尚未收定的交易日；继续欠着，留给后面的 run",
            err=True,
        )
    if not buckets:
        return {
            "dataset": dataset,
            "status": "success",
            "outstanding": len(owed),
            "note": "every owed key is for a session that is not final yet",
        }

    click.echo(
        f"[{dataset}] 待试 {len(active)} 个 key，涉及 "
        f"{len({r['symbol'] for r in active})} 只标的；分 {len(buckets)} 个月度批次修复",
        err=True,
    )
    failures: list[str] = []
    passes = 0
    filled = 0
    fatal = False
    for index, month in enumerate(sorted(buckets), start=1):
        symbols = sorted(buckets[month])
        lo, hi = min(days_in[month]), max(days_in[month])
        click.echo(
            f"[{dataset}] {index}/{len(buckets)} {month}：{len(symbols)} 只标的 {lo}..{hi}",
            err=True,
        )
        cfg._backfill_symbols = symbols
        cfg._backfill_start = date.fromisoformat(lo)
        cfg._backfill_end = date.fromisoformat(hi)
        cfg._backfill_workers = workers
        out = _backfill_once(cfg, dataset)
        passes += 1
        if out.get("status") not in ("success", "warning", "degraded"):
            failures.append(f"{month}: {out.get('status')}")
        # Settle after each pass, not once at the end. A repair of a real
        # backlog runs for hours — the first version reached pass 30 of 37 over
        # three hours and, killed there, had struck nothing off: every row it
        # had fetched was still owed. Interrupting this now costs the pass in
        # flight, not the run.
        settled = _settle_outstanding(cfg, dataset, attempted_scope=(set(symbols), lo, hi))
        filled += settled["filled"]
        if out.get("status") == "failed" and out.get("run_id"):
            aggregate = Manifest(cfg.manifest_path).aggregate_run_status(out["run_id"])
            if aggregate["core_failures"] or not aggregate["source_failures"]:
                fatal = True
                break

    settled = settled if buckets else _settle_outstanding(cfg, dataset)
    return {
        "dataset": dataset,
        "status": "failed"
        if fatal or (failures and not filled)
        else "warning"
        if settled["still_owed"]
        else "success",
        "passes": passes,
        "failed_passes": failures,
        "outstanding": settled,
        "result_schema_version": 2,
        "execution_status": "failed" if fatal or (failures and not filled) else "completed",
        "coverage_status": "partial" if settled["still_owed"] else "complete",
        "publication_status": "partial"
        if filled and settled["still_owed"]
        else "published"
        if filled
        else "unchanged",
        "usable_result": bool(filled or not settled["still_owed"]),
        "parked": len(parked),
        "parked_after_attempts": limit,
    }


def _settle_outstanding(
    cfg,
    dataset: str,
    *,
    note_missing_attempt: bool = True,
    attempted_scope: tuple[set[str], str, str] | None = None,
) -> dict:
    """Strike off the owed keys that are now in the lake, and report the rest.

    Checked against what actually landed rather than against the run's exit
    status: a repair that reaches some of the keys should shrink the debt by
    exactly those, and a key the vendor still does not serve must stay owed
    rather than be quietly forgotten by a successful-looking run.
    """
    import polars as pl

    from cnequity.domain.datasets import get_dataset as _spec
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root
    from cnequity.storage.state import StateStore

    store = StateStore(cfg.meta_root)
    owed = store.get_outstanding_keys(dataset)
    if not owed:
        return {"before": 0, "filled": 0, "still_owed": 0}
    root = cfg.curated_root / dataset
    if not dataset_has_parquet(root):
        return {"before": len(owed), "filled": 0, "still_owed": len(owed)}
    date_col = _spec(dataset).partition_col
    # Pushed down to the owed scope. Collecting the whole dataset to check a
    # handful of keys reads 14GB to answer a question about 5,000 rows, and a
    # settle that costs more than the repair will not get run.
    wanted_symbols = sorted({row["symbol"] for row in owed if row.get("symbol")})
    wanted_days = sorted({row["trade_date"] for row in owed if row.get("trade_date")})
    present = set(
        scan_parquet_root(root, partition_col=date_col)
        .select("symbol", pl.col(date_col).cast(pl.Utf8).alias("_d"))
        .filter(
            pl.col("symbol").is_in(wanted_symbols)
            & pl.col("_d").is_between(pl.lit(wanted_days[0]), pl.lit(wanted_days[-1]))
        )
        .unique()
        .collect()
        .iter_rows()
    )
    filled = [
        (row["symbol"], row["trade_date"])
        for row in owed
        if (row.get("symbol"), row.get("trade_date")) in present
    ]
    missed = [
        (row["symbol"], row["trade_date"])
        for row in owed
        if (row.get("symbol"), row.get("trade_date")) not in present
    ]
    left = store.clear_outstanding_keys(dataset, filled) if filled else len(owed)
    # A key this repair reached for and still did not get is worth counting:
    # nothing here expires, so the attempt count is the only thing that will
    # ever distinguish last night's blip from a vendor that has stopped
    # serving the symbol at all.
    if note_missing_attempt:
        attempted = missed
        if attempted_scope is not None:
            symbols, start, end = attempted_scope
            attempted = [
                (symbol, day) for symbol, day in missed if symbol in symbols and start <= day <= end
            ]
        store.note_repair_attempt(dataset, attempted)
    stubborn = sum(
        1 for row in store.get_outstanding_keys(dataset) if int(row.get("attempts", 0) or 0) >= 3
    )
    out = {"before": len(owed), "filled": len(filled), "still_owed": left}
    if stubborn:
        out["unfilled_after_3_attempts"] = stubborn
    return out


# Datasets whose universe comes from a config block rather than from
# `instruments`, and the block that holds it. `cne backfill --symbols` and the
# horizon guard both need to name the right one — telling a trade_ticks user to
# narrow `[minute_bars].scope` sends them to edit a setting that does nothing.
SCOPED_DATASETS: dict[str, str] = {
    "minute_bars": "minute_bars",
    "minute_bars_5m": "minute_bars",
    "trade_ticks": "trade_ticks",
}


def _override_scope(cfg, dataset: str, symbols: list[str]) -> None:
    """Point *dataset* at exactly *symbols* for this run only.

    Enabling as well as scoping: a one-off `--symbols` pull should not also
    require flipping the config's `enabled` flag first, and the capture steps
    return early when it is false.
    """
    if dataset == "futures_minute_bars":
        from cnequity.domain.derivatives import parse_future_code

        for symbol in symbols:
            code, _, exchange = symbol.partition(".")
            try:
                contract = parse_future_code(code, exchange, shanghai_today())
            except ValueError as exc:
                raise click.ClickException(f"非法期货合约 {symbol}: {exc}") from exc
            if contract.symbol != symbol:
                raise click.ClickException(f"请使用标准期货合约代码：{contract.symbol}")
        cfg.futures_enabled = cfg.futures_minute_enabled = True
        cfg.futures_minute_contracts = symbols
        cfg.futures_minute_products = []
        cfg.futures_minute_max_contracts = max(cfg.futures_minute_max_contracts, len(symbols))
        return
    block = SCOPED_DATASETS.get(dataset)
    if block is None:
        raise click.ClickException(
            f"--symbols 只适用于配置里有 scope 的数据集"
            f"（{', '.join(sorted(SCOPED_DATASETS))}）；{dataset} 不支持按标的覆盖范围。"
        )
    setattr(cfg, f"{block}_enabled", True)
    setattr(cfg, f"{block}_scope", "watchlist")
    setattr(cfg, f"{block}_symbols", symbols)
    # The ceiling exists to stop an unnoticed full-market sweep, not to second
    # guess a list the user just typed out by hand.
    if block == "trade_ticks":
        cfg.trade_ticks_max_symbols = max(cfg.trade_ticks_max_symbols, len(symbols))
    frequency = get_dataset(dataset).intraday_frequency
    if frequency and frequency not in cfg.minute_bars_frequencies:
        cfg.minute_bars_frequencies = [*cfg.minute_bars_frequencies, frequency]


def _guard_history_horizon(dataset: str, start: date | None) -> None:
    """Refuse a window the source cannot serve, instead of sweeping into nothing.

    A horizon-limited source does not return *less* data for an older window,
    it returns none — so without this an ``cne backfill minute_bars --start
    2016-01-01`` spends hours producing an empty lake and reads as a bug in the
    lake rather than a limit of the vendor.
    """
    spec = get_dataset(dataset)
    earliest = spec.earliest_available(shanghai_today())
    if earliest is None or start is None or start >= earliest:
        return
    if spec.history_floor_date is not None:
        # A fixed floor, not a per-symbol budget: no symbol reaches further
        # back, so there is no narrower scope that would help.
        raise click.ClickException(
            f"{dataset}：--start {start} 早于源的历史下限。"
            f"对任何标的，上游都不提供早于 {earliest} 的数据，"
            f"也没有任何回填源能延长它。请改用 --start {earliest} 或更晚的日期。"
        )
    block = SCOPED_DATASETS.get(dataset, "minute_bars")
    raise click.ClickException(
        f"{dataset}：--start {start} 早于源能提供的历史深度。"
        f"对每个交易日都有报价的标的，上游每个标的大约只保留 {spec.history_horizon_days} "
        f"个交易日（大致回到 {earliest}），并且没有任何回填源能延长它。"
        f"请改用 --start {earliest} 或更晚的日期。"
        "（成交稀疏的标的有 K 线的天数更少，因此能回溯得更远。"
        f"要取那些，请先把 [{block}].scope 收窄成一个观察列表 —— "
        "用那个起点扫全市场，会在根本没有数据的标的上耗掉好几个小时。）"
    )


def _finish_backfill_run(engine, result: dict) -> dict:
    """Compact this run's staging, then close the run out."""
    run_id = result["run_id"]
    # Compact partial sweeps too, including failed ones. `compact` only ever
    # drains the *current* run's staging, so skipping it here would strand
    # every row the sweep did fetch before the failure — measured in
    # production: a walk_day_backfill window that flushed 21 clean days to
    # staging before an exception on day 22 still lost all 21, because this
    # used to skip compact on status=="failed". A run with nothing staged
    # compacts to a no-op (`step_compact` only touches datasets with files
    # under this run_id), so there is no cost to always trying.
    # Through the engine, not step_compact directly: the recorded compact
    # batch is what later lets `cne run clean` release this run's staging.
    result["compact"] = engine.run_step("compact", shanghai_today(), run_id)
    compact_status = result["compact"].get("status", "success")
    aggregate = (
        engine.manifest.aggregate_run_status(run_id)
        if hasattr(engine.manifest, "aggregate_run_status")
        else {}
    )
    if aggregate.get("results"):
        result["status"] = aggregate["status"]
    if compact_status == "failed" or result["status"] == "failed":
        result["status"] = "failed"
    elif compact_status == "warning" or result["status"] == "warning":
        result["status"] = "warning"
    engine.manifest.finish_run(
        run_id,
        result["status"],
        rows_read=result.get("rows_read", 0),
        rows_written=result.get("rows_written", 0),
        error_message="one or more steps failed" if result["status"] == "failed" else None,
    )
    if hasattr(engine, "_public_outcome"):
        result.update(engine._public_outcome(run_id))
    persisted = engine.manifest.get_run(run_id) if hasattr(engine.manifest, "get_run") else None
    if persisted is not None:
        result["status"] = persisted["status"]
    return result


def _run_had_step_failure(engine: JobEngine, run_id: str) -> bool:
    """Whether a step in *run_id* actually failed, whatever tier softened it.

    ``aggregate_run_status`` deliberately reports a *run* as degraded rather
    than failed when the step that raised was not core: in the daily job the
    other datasets still landed and the lake stays usable. A single-dataset
    sweep has no such consolation — that one dataset is the entire job — and
    35 of the registered steps are non-core, so reading the run tier here let
    `cne backfill` print ``"status": "success"`` and exit 0 for a sweep whose
    every slice had raised.
    """
    aggregate = engine.manifest.aggregate_run_status(run_id)
    if "status" in aggregate:
        return aggregate["status"] == "failed"
    return bool(aggregate["core_failures"]) or any(
        item["status"] in {"failed", "blocked"} for item in aggregate["degraded_results"]
    )


def _source_limited_slice(engine, run_id: str) -> bool:
    """An unusable source slice does not cancel independent later slices."""
    result = (
        engine.manifest.aggregate_run_status(run_id)
        if hasattr(engine.manifest, "aggregate_run_status")
        else {}
    )
    return bool(result.get("source_failures")) and not result["core_failures"]


def _recover_compactable_backfill_staging(engine: JobEngine, dataset: str) -> list[str]:
    """Compact staged rows left by an interrupted terminal backfill run.

    A process killed after a step flushed a batch has no chance to execute the
    normal ``_finish_backfill_run`` path. The next invocation used to start a
    fresh run while leaving those rows invisible in staging, so checkpointed
    positive facts were fetched again and the old run became a permanent
    staging leak. Terminal runs with staged files are safe to compact here: the
    regular compact gate still protects incomplete worker batches, and coverage
    receipts remain gated by their versioned checkpoint.
    """
    from cnequity.orchestrator.compact_gate import compact_allowed
    from cnequity.storage import StagingWriter

    config = getattr(engine, "config", None)
    if config is None:  # lightweight engine doubles in CLI/unit tests
        return []
    # A hard-killed worker leaves its manifest row as ``running``. Reconcile
    # stale rows before selecting recovery candidates; otherwise their staged
    # facts stay invisible and the next retry fetches already checkpointed
    # symbols again. Active runs remain protected by the per-run lock.
    reconciled = engine.manifest.reconcile_orphaned_runs(
        stale_after_seconds=config.batch_stale_seconds,
        locks_root=config.meta_root,
    )
    if reconciled.get("runs_closed"):
        logging.getLogger(__name__).warning(
            "Reconciled %d orphaned backfill run(s) before staging recovery",
            reconciled["runs_closed"],
        )
    writer = StagingWriter(config.staging_root)
    recovered: list[str] = []
    for run in engine.manifest.list_runs("backfill"):
        run_id = str(run["run_id"])
        # Name the in-flight states, not the terminal ones. Listing the
        # terminal spellings is how `degraded` — a status this same release
        # taught the engine to return — came to be skipped here, leaving the
        # staged rows of exactly the runs most likely to have some.
        if run["status"] in ("running", "stale"):
            continue
        batches = engine.manifest.get_batches_for_run(run_id)
        if any(batch["dataset"] == "compact" and batch["status"] == "success" for batch in batches):
            continue
        if not writer.list_run_files(dataset, run_id):
            continue
        # A failed blocking batch cannot be published by compact. Replaying
        # compact on every later backfill only rescans the same immutable
        # partitions before the gate rejects it; leave staging and failure
        # evidence untouched until its batch is actually resolved.
        allowed, incomplete = compact_allowed(
            engine.manifest,
            run_id,
            dataset,
            stale_after_seconds=config.batch_stale_seconds,
        )
        if not allowed:
            logging.getLogger(__name__).info(
                "Skipped staged %s from run %s: %d blocking batch(es) remain",
                dataset,
                run_id,
                incomplete,
            )
            continue
        result = engine.run_step("compact", shanghai_today(), run_id)
        if result.get("status") == "success":
            recovered.append(run_id)
            logging.getLogger(__name__).info(
                "Recovered staged %s from interrupted backfill run %s before retry",
                dataset,
                run_id,
            )
    return recovered


def _run_delisted_profile(cfg, since: date) -> dict:
    """Run the delisted profile through one manifest and compact path."""
    from cnequity.orchestrator.outcomes import step_outcome
    from cnequity.orchestrator.registry import get_step
    from cnequity.steps.delisted import backfill_delisted_bars

    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run(
        "delisted_backfill", {"since": since.isoformat(), "profile": "delisted"}
    )
    try:
        result = backfill_delisted_bars(cfg, run_id, since)
        engine._record_step_result(
            name="daily_bars",
            entry=get_step("daily_bars"),
            run_id=run_id,
            status=result.get("status", "success"),
            out=result,
        )
        compact_out = engine.run_step("compact", shanghai_today(), run_id)
    except (KeyboardInterrupt, SystemExit):
        engine.manifest.interrupt_run(run_id, error_message="delisted recovery interrupted")
        raise
    except Exception as exc:
        engine.manifest.finish_run(run_id, "failed", error_message=str(exc))
        raise
    complete = (
        result.get("status", "success") == "success"
        and compact_out.get("status", "success") == "success"
    )
    run_status = "success" if complete else "warning"
    if compact_out.get("status") in {"failed", "blocked"} or step_outcome(
        result.get("status", "success"), result
    ).execution_status in {"failed", "interrupted"}:
        run_status = "failed"
    error_message = None if complete else "delisted recovery has unresolved targets"
    engine.manifest.finish_run(
        run_id,
        run_status,
        rows_read=result.get("rows_read", 0),
        rows_written=result.get("rows_written", 0),
        error_message=error_message,
    )
    return {
        "run_id": run_id,
        **result,
        "status": run_status,
        "compact": compact_out,
        **engine._public_outcome(run_id),
    }


def _run_shfe_annual_archive(
    cfg,
    dataset: str,
    path: Path,
    year: int,
    *,
    start: date | None,
    end: date | None,
    source_url: str | None,
    downloaded_at: str | None = None,
) -> dict:
    """Keep an explicit partial-field import in the normal run/compact ledger."""
    from cnequity.steps.derivative_archive import import_shfe_annual

    engine = JobEngine(cfg)
    run_id = engine.manifest.start_run(
        "shfe_annual_import",
        {
            "dataset": dataset,
            "year": year,
            "archive": str(path),
            "start": str(start) if start else None,
            "end": str(end) if end else None,
            "partial_fields_accepted": True,
        },
    )
    try:
        result = import_shfe_annual(
            cfg,
            run_id,
            dataset,
            path,
            year=year,
            start=start,
            end=end,
            source_url=source_url,
            downloaded_at=downloaded_at,
        )
        compact = engine.run_step("compact", shanghai_today(), run_id)
        status = result["status"]
        if compact.get("status") == "failed":
            status = "failed"
        elif compact.get("status") == "warning" and status == "success":
            status = "warning"
    except Exception as exc:
        engine.manifest.finish_run(run_id, "failed", error_message=str(exc))
        raise
    engine.manifest.finish_run(
        run_id,
        status,
        rows_read=result["rows_read"],
        rows_written=result["rows_written"],
        error_message="annual archive has unresolved members or rows"
        if status != "success"
        else None,
    )
    return {"run_id": run_id, **result, "status": status, "compact": compact}


def _require_known_dataset(dataset: str) -> str:
    """Reject a mistyped name with the near misses, not a ``KeyError`` dump.

    `cne backfill` takes a dataset, and the registry lookup that rejects an
    unknown one raised straight through the CLI — so a typo printed a Python
    traceback instead of telling the operator what to type.
    """
    from cnequity.domain.datasets import DATASETS

    # Registry names are lower case, and command names are already
    # case-insensitive, so a dataset typed in caps should resolve the same way.
    # Returns the canonical spelling for the caller to use from here on.
    canonical = dataset.lower()
    if canonical in DATASETS:
        return canonical
    close = difflib.get_close_matches(canonical, sorted(DATASETS), n=3)
    hint = f"是不是想找：{', '.join(close)}？" if close else ""
    raise click.ClickException(
        f"未知数据集 {dataset!r}。{hint}`cne status --datasets` 会列出全部数据集。"
    )


def _run_backfill(cfg, dataset: str, start: date | None, end: date | None) -> dict:
    """Backfill one window, dispatching exactly as `cne backfill` does.

    Shared so `cne verify --repair` cannot drift into a second, subtly
    different backfill path — the chunking rules below are not incidental
    (see `_backfill_symbol_chunked`).
    """
    if start is not None:
        cfg._backfill_start = start
    if end is not None:
        cfg._backfill_end = end
    spec = get_dataset(dataset)
    if spec.backfill_chunk_symbols and start and end:
        return _backfill_symbol_chunked(cfg, dataset, start, end, spec.backfill_chunk_symbols)
    if spec.backfill_chunk_days and start and end:
        return _backfill_chunked(cfg, dataset, start, end, spec.backfill_chunk_days)
    return _backfill_once(cfg, dataset)


def _backfill_once(cfg, dataset: str) -> dict:
    # CNINFO range steps also protect direct step invocations with an internal
    # 31-day window, but the CLI must make each window a separate run so the
    # compact boundary drains staging before the next window is fetched.  If
    # no explicit range was supplied, an omitted --end means today.
    # `regulatory_events` is chunked alongside it for the same compact bound,
    # though it no longer fetches: it derives from the announcements already
    # indexed and clamps each slice to their range, so a floor that predates
    # the lake's own history costs a skipped slice, not a failed sweep.
    if dataset in {"announcement_index", "regulatory_events"}:
        start = getattr(cfg, "_backfill_start", None) or date(2010, 1, 1)
        end = getattr(cfg, "_backfill_end", None) or shanghai_today()
        return _backfill_chunked(cfg, dataset, start, end, get_dataset(dataset).backfill_chunk_days)
    engine = JobEngine(cfg)
    _recover_compactable_backfill_staging(engine, dataset)
    # Do not finish_run until after compact — otherwise a kill between the two
    # leaves status=success with no compact batch, and `cne run clean` cannot reclaim
    # staging that never reached curated (same ordering as delisted CLI).
    result = engine.run_job("backfill", steps=[dataset], backfill=True, finalize_run=False)
    return _finish_backfill_run(engine, result)


def _backfill_symbol_chunked(cfg, dataset: str, start: date, end: date, chunk_symbols: int) -> dict:
    """Backfill a tip-paged dataset as compacted symbol slices over [start, end].

    TDX intraday locates old windows by offset before scanning their pages.
    Chunking by symbol pays that positioning cost once per name, bounds compact
    memory, and makes an interruption cost only the current symbol batch.
    """
    from cnequity.steps.intraday import (
        _filter_all_scope_to_listed_symbols,
        resolve_scope,
    )

    symbols = resolve_scope(cfg)
    if (cfg.minute_bars_scope or "").strip() == "all":
        symbols = _filter_all_scope_to_listed_symbols(cfg, symbols, start, end)
    if not symbols:
        raise click.ClickException(
            f"{dataset}：范围解析出来是 0 只标的 —— 检查 [minute_bars].scope"
        )

    engine = JobEngine(cfg)
    _recover_compactable_backfill_staging(engine, dataset)
    chunks: list[dict] = []
    failed_scopes: list[dict] = []
    usable = False
    status = "success"
    rows_read = rows_written = 0
    original_scope = cfg.minute_bars_scope
    original_symbols = list(cfg.minute_bars_symbols)
    cfg._backfill_start, cfg._backfill_end = start, end
    try:
        for index in range(0, len(symbols), chunk_symbols):
            chunk = symbols[index : index + chunk_symbols]
            cfg.minute_bars_scope = "watchlist"
            cfg.minute_bars_symbols = chunk
            click.echo(
                f"[{dataset}] 标的 {index + 1}..{index + len(chunk)}/"
                f"{len(symbols)}（{chunk[0]}..{chunk[-1]}）窗口 {start}..{end}",
                err=True,
            )
            result = engine.run_job("backfill", steps=[dataset], backfill=True, finalize_run=False)
            if _run_had_step_failure(engine, result["run_id"]):
                result["status"] = "failed"
            result = _finish_backfill_run(engine, result)
            usable |= bool(
                result.get("usable_result", result["status"] == "success")
                or result.get("rows_written")
            )
            rows_read += int(result.get("rows_read", 0))
            rows_written += int(result.get("rows_written", 0))
            chunks.append(
                {
                    "symbols_from": index + 1,
                    "run_id": result["run_id"],
                    "symbols_to": index + len(chunk),
                    "first_symbol": chunk[0],
                    "last_symbol": chunk[-1],
                    "start": start,
                    "end": end,
                    "status": result["status"],
                    "rows_written": result.get("rows_written", 0),
                    "fallback": result.get("fallback", []),
                }
            )
            if result["status"] == "failed":
                if _source_limited_slice(engine, result["run_id"]):
                    failed_scopes.append(chunks[-1])
                    status = "warning"
                    continue
                status = "failed"
                break
            if result["status"] in {"warning", "degraded"} and status == "success":
                status = result["status"]
            if result["status"] in {"warning", "degraded"}:
                failed_scopes.append(chunks[-1])
    finally:
        cfg.minute_bars_scope = original_scope
        cfg.minute_bars_symbols = original_symbols

    return {
        "dataset": dataset,
        "status": status,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "chunks": chunks,
        "failed_scopes": failed_scopes,
        "usable_result": usable,
        "fallback": [option for chunk in chunks for option in chunk["fallback"]],
        "result_schema_version": 2,
        "execution_status": "failed" if status == "failed" else "completed",
        "coverage_status": "unknown" if status == "success" else "partial",
        "publication_status": "partial"
        if status != "success" and usable
        else "published"
        if rows_written
        else "unchanged",
        "resume_from_symbol": (
            chunks[-1]["first_symbol"] if status == "failed" and chunks else None
        ),
    }


def _backfill_chunked(cfg, dataset: str, start: date, end: date, chunk_days: int) -> dict:
    """Run the backfill as a sequence of compacted date slices.

    One run for the whole window would stage more than compact can hold in
    memory (it reads every staging file of a run into one frame). Slicing also
    means a kill costs the current slice rather than the whole sweep: every
    earlier slice is already in curated.

    Do **not** use this for tip-paged intraday sources — see
    ``_backfill_symbol_chunked``.
    """
    engine = JobEngine(cfg)
    _recover_compactable_backfill_staging(engine, dataset)
    slices: list[dict] = []
    failed_scopes: list[dict] = []
    usable = False
    status = "success"
    rows_read = rows_written = 0
    cursor = start
    while cursor <= end:
        slice_end = min(cursor + timedelta(days=chunk_days - 1), end)
        cfg._backfill_start, cfg._backfill_end = cursor, slice_end
        click.echo(f"[{dataset}] 分片 {cursor}..{slice_end}", err=True)
        result = engine.run_job("backfill", steps=[dataset], backfill=True, finalize_run=False)
        if _run_had_step_failure(engine, result["run_id"]):
            result["status"] = "failed"
        result = _finish_backfill_run(engine, result)
        usable |= bool(
            result.get("usable_result", result["status"] == "success") or result.get("rows_written")
        )
        rows_read += int(result.get("rows_read", 0))
        rows_written += int(result.get("rows_written", 0))
        slices.append(
            {
                "start": cursor,
                "run_id": result["run_id"],
                "end": slice_end,
                "status": result["status"],
                "rows_written": result.get("rows_written", 0),
                "fallback": result.get("fallback", []),
            }
        )
        if result["status"] == "failed":
            if _source_limited_slice(engine, result["run_id"]):
                failed_scopes.append(slices[-1])
                status = "warning"
                cursor = slice_end + timedelta(days=1)
                continue
            status = "failed"
            break
        # `degraded` is an outcome, not a synonym for success: a slice the
        # source could not supply must not leave the sweep claiming it did.
        if result["status"] in {"warning", "degraded"} and status == "success":
            status = result["status"]
        if result["status"] in {"warning", "degraded"}:
            failed_scopes.append(slices[-1])
        cursor = slice_end + timedelta(days=1)
    return {
        "dataset": dataset,
        "status": status,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "slices": slices,
        "failed_scopes": failed_scopes,
        "usable_result": usable,
        "fallback": [option for part in slices for option in part["fallback"]],
        "result_schema_version": 2,
        "execution_status": "failed" if status == "failed" else "completed",
        "coverage_status": "unknown" if status == "success" else "partial",
        "publication_status": "partial"
        if status != "success" and usable
        else "published"
        if rows_written
        else "unchanged",
        "resume_from": slices[-1]["start"] if status == "failed" and slices else None,
    }
