"""`backfill` and the chunking, scoping and recovery it needs.

The helpers are the bulk of it: a backfill is one command with several failure
modes that each need their own repair path (symbol-chunked, day-chunked, and
recovering staging an interrupted terminal run left behind).
"""

from __future__ import annotations

import difflib
import json
import logging
from datetime import date, timedelta

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    attach_log_file,
    config_option,
    parse_date_option,
)
from cnequity.domain.datasets import fetch_semantics, get_dataset
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine


@cli.command()
@click.argument("dataset")
@config_option
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
        "按日期推进的回填（margin_trading、financial_statement_items 报告期推进、minute_bars）"
        "的区间起点（YYYY-MM-DD），也用来收窄 sector_bars 的 K 线窗口（默认往前 400 天）。有历史深度限制的数据集会拒绝比源仍能提供的范围更早的起点。"
    ),
)
@click.option(
    "--end",
    "end_str",
    default=None,
    help=(
        "按日期推进的回填（margin_trading、financial_statement_items 报告期推进）与 sector_bars "
        "的区间终点（YYYY-MM-DD，默认今天）。"
    ),
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
    "--symbols",
    "symbols_str",
    default=None,
    help=(
        "限定范围的标的列表，逗号分隔：用于 intraday、trading_status、corporate_actions "
        "的限定回填，以及 financial_statement_items、daily_bars 的限定修复。trading_status "
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
    "--baostock-repair",
    is_flag=True,
    help="仅 corporate_actions：用 Baostock 显式修复已退市的沪深标的。",
)
@click.option(
    "--ths-repair",
    is_flag=True,
    help="仅 corporate_actions：用同花顺显式修复已退市的北交所标的。",
)
@click.option(
    "--eastmoney-bj-repair",
    is_flag=True,
    help="仅 corporate_actions：通过现行的 920xxx 东财代码修复北交所老代码。",
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
    help="仅 daily_bars：用北交所官网补已有交易日的 BJ 成交额，不重抓 Sina。",
)
@click.option(
    "--bj-amount-repair",
    is_flag=True,
    help="仅 daily_bars：从 TDX 补 Sina 从未发布过的北交所成交额，已存的价格和成交量一律不动。需要 --start/--end。",
)
def backfill(
    dataset: str,
    config_path: str,
    retry_failed: bool,
    force: bool,
    start_str: str | None,
    end_str: str | None,
    symbols_str: str | None,
    outstanding: bool,
    workers: int,
    baostock_repair: bool,
    ths_repair: bool,
    eastmoney_bj_repair: bool,
    eastmoney_date_repair: bool,
    ex_dates_str: str | None,
    bse_tip_repair: bool,
    bj_amount_repair: bool,
):
    """回填一个数据集。

    \b
    成本按源的计费单位算，不按窗口算。daily_bars 是逐标的抓取，所以 `--start D --end D`
    和多年窗口一样要扫一遍全市场 —— 一个交易日不等于一个请求。
    只想快速验证而不是跑全市场时，用 `--symbols` 缩小范围。
    """
    dataset = _require_known_dataset(dataset)
    if fetch_semantics(dataset) == "snapshot" and not get_dataset(dataset).backfill_source:
        raise click.ClickException(
            f"{dataset}：不支持回填 —— 它的采集语义是 snapshot"
            "（实时页面盖上 trade_date；历史值拿不到）。"
            "请改为在交易日跑日更采集。"
        )
    cfg = _cfg(config_path)
    attach_log_file(cfg, f"backfill-{dataset}")
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
    if bse_tip_repair:
        if not symbols_str:
            raise click.ClickException("--bse-tip-repair 需要 --symbols")
        if start_d is None or end_d is None or start_d != end_d:
            raise click.ClickException("--bse-tip-repair 需要显式给出同一天的 --start 和 --end")
        cfg._bse_tip_repair = True
    if outstanding:
        if symbols_str or start_d or end_d:
            raise click.ClickException(
                "--outstanding 的范围取自欠账台账；请去掉 --symbols/--start/--end"
            )
        result = _repair_outstanding(cfg, dataset, workers)
        click.echo(json.dumps(result, indent=2, default=str))
        if result["status"] != "success":
            raise SystemExit(1)
        return
    _guard_history_horizon(dataset, start_d)
    if symbols_str:
        symbols = [s.strip().upper() for s in symbols_str.split(",") if s.strip()]
        if dataset in (
            "daily_bars",
            "trading_status",
            "corporate_actions",
            "financial_statement_items",
        ):
            cfg._backfill_symbols = symbols
        else:
            _override_scope(cfg, dataset, symbols)
        click.echo(f"[{dataset}] 本次 run 的范围被覆盖为 {len(symbols)} 只标的", err=True)
    if start_d:
        cfg._backfill_start = start_d
    if end_d:
        cfg._backfill_end = end_d
    cfg._backfill_workers = workers
    if dataset == "trading_status":
        # The per-run bound exists so `cne init` is not held behind baostock's
        # pacing for ten hours. Asking for this backfill by name *is* asking to
        # sit through it, and silently stopping at 400 symbols would look like
        # the command had finished the job.
        cfg.st_history_symbols_per_run = 0

    spec = get_dataset(dataset)
    # Tip-paged sources (intraday) must chunk by symbol, not by date: the wire
    # always walks tip → start, so date slices re-fetch every newer page.
    if spec.backfill_chunk_symbols and start_d and end_d:
        result = _backfill_symbol_chunked(cfg, dataset, start_d, end_d, spec.backfill_chunk_symbols)
    elif spec.backfill_chunk_days and start_d and end_d:
        result = _backfill_chunked(cfg, dataset, start_d, end_d, spec.backfill_chunk_days)
    else:
        result = _backfill_once(cfg, dataset)
    if outstanding:
        result["outstanding"] = _settle_outstanding(cfg, dataset)
    click.echo(json.dumps(result, indent=2, default=str))
    if result["status"] != "success":
        raise SystemExit(1)


def _repair_outstanding(cfg, dataset: str, workers: int) -> dict:
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

    from cnequity.steps.bars import _last_final_session
    from cnequity.storage.state import StateStore

    owed = StateStore(cfg.meta_root).get_outstanding_keys(dataset)
    if not owed:
        return {"dataset": dataset, "status": "success", "outstanding": 0, "note": "nothing owed"}

    # A key for a session that has not closed yet would make its whole monthly
    # pass fail the finality guard, and every other key in that month with it:
    # one 2026-09-18 key held back 242 owed sessions at 03:36 Shanghai. It stays
    # on the ledger for a later run rather than blocking today's repair.
    final = _last_final_session().isoformat() if dataset == "daily_bars" else None
    buckets: dict[str, set[str]] = defaultdict(set)
    days_in: dict[str, list[str]] = defaultdict(list)
    deferred = 0
    for row in owed:
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
        f"[{dataset}] 欠着 {len(owed)} 个 key，涉及 "
        f"{len({r['symbol'] for r in owed})} 只标的；分 {len(buckets)} 个月度批次修复",
        err=True,
    )
    failures: list[str] = []
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
        if out.get("status") not in ("success", "warning", "degraded"):
            failures.append(f"{month}: {out.get('status')}")
        # Settle after each pass, not once at the end. A repair of a real
        # backlog runs for hours — the first version reached pass 30 of 37 over
        # three hours and, killed there, had struck nothing off: every row it
        # had fetched was still owed. Interrupting this now costs the pass in
        # flight, not the run.
        settled = _settle_outstanding(cfg, dataset)

    settled = settled if buckets else _settle_outstanding(cfg, dataset)
    return {
        "dataset": dataset,
        "status": "success" if not failures else "failed",
        "passes": len(buckets),
        "failed_passes": failures,
        "outstanding": settled,
    }


def _settle_outstanding(cfg, dataset: str) -> dict:
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
    store.note_repair_attempt(dataset, missed)
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
    block = SCOPED_DATASETS.get(dataset)
    if block is None:
        raise click.ClickException(
            f"--symbols 只适用于配置里有 scope 的数据集"
            f"（{', '.join(sorted(SCOPED_DATASETS))}）；{dataset} 的标的范围来自 instruments。"
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
    return bool(aggregate["core_failures"]) or any(
        str(item["status"]) in {"failed", "blocked"} for item in aggregate["degraded_results"]
    )


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
        result = engine.run_step("compact", shanghai_today(), run_id)
        if result.get("status") == "success":
            recovered.append(run_id)
            logging.getLogger(__name__).info(
                "Recovered staged %s from interrupted backfill run %s before retry",
                dataset,
                run_id,
            )
    return recovered


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

    TDX intraday pages backwards from the live tip. A date-sliced sweep of the
    same window therefore re-walks tip → each slice_start for every symbol —
    measured ~8× the wire traffic of one tip→horizon walk on CSI300 1m. Chunking
    by symbol keeps one walk per name, bounds compact memory, and makes a kill
    cost only the current symbol batch.
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
            rows_read += int(result.get("rows_read", 0))
            rows_written += int(result.get("rows_written", 0))
            chunks.append(
                {
                    "symbols_from": index + 1,
                    "symbols_to": index + len(chunk),
                    "first_symbol": chunk[0],
                    "last_symbol": chunk[-1],
                    "start": start,
                    "end": end,
                    "status": result["status"],
                    "rows_written": result.get("rows_written", 0),
                }
            )
            if result["status"] == "failed":
                status = "failed"
                break
            if result["status"] in {"warning", "degraded"} and status == "success":
                status = result["status"]
    finally:
        cfg.minute_bars_scope = original_scope
        cfg.minute_bars_symbols = original_symbols

    return {
        "dataset": dataset,
        "status": status,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "chunks": chunks,
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
        rows_read += int(result.get("rows_read", 0))
        rows_written += int(result.get("rows_written", 0))
        slices.append(
            {
                "start": cursor,
                "end": slice_end,
                "status": result["status"],
                "rows_written": result.get("rows_written", 0),
            }
        )
        if result["status"] == "failed":
            # Stop rather than press on: the slices already compacted are kept,
            # and the window to resume from is the one printed here.
            status = "failed"
            break
        # `degraded` is an outcome, not a synonym for success: a slice the
        # source could not supply must not leave the sweep claiming it did.
        if result["status"] in {"warning", "degraded"} and status == "success":
            status = result["status"]
        cursor = slice_end + timedelta(days=1)
    return {
        "dataset": dataset,
        "status": status,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "slices": slices,
        "resume_from": slices[-1]["start"] if status == "failed" and slices else None,
    }
