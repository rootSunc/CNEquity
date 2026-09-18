"""Lake maintenance: `run compact`, `run clean`, `derive`, `stats`.

What you run against a lake that already exists, to keep its shape rather than
to change what it holds.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date

import click
import polars as pl

from cnequity.cli._root import cli, run
from cnequity.cli._shared import (
    _cfg,
    _run_status_exit_code,
    attach_log_file,
    config_option,
    parse_date_option,
)
from cnequity.derive.adj_factors import compute_adj_factors
from cnequity.domain.market_time import shanghai_today
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.manifest import Manifest
from cnequity.query.parquet_scan import scan_parquet_files
from cnequity.storage.revisions import prune_revision_generations
from cnequity.storage.source_snapshots import (
    DEFAULT_SNAPSHOT_RETENTION_DAYS,
    clean_source_snapshots,
)
from cnequity.storage.staging_cleanup import (
    DEFAULT_LOG_RETENTION_DAYS,
    clean_run_logs,
    clean_staging,
)


@run.command("compact")
@config_option
@click.option("--run-id", default=None)
def compact(config_path: str, run_id: str | None):
    """把这次 run 里 staging 的所有数据集 compact 进 curated。"""
    cfg = _cfg(config_path)
    manifest = Manifest(cfg.manifest_path)
    if not run_id:
        latest = manifest.latest_run()
        if not latest:
            raise click.ClickException("没有找到任何 run")
        run_id = latest["run_id"]

    attach_log_file(cfg, "run-compact")
    out = JobEngine(cfg).run_step("compact", shanghai_today(), run_id)
    click.echo(
        json.dumps(
            {"run_id": run_id, "rows_written": out.get("rows_written", 0), **out},
            indent=2,
            default=str,
        )
    )


def _derive_trading_status(cfg, *, start: date | None, end: date | None) -> dict:
    """Run the derive step and publish it, as the daily job would.

    The rows have to reach a committed revision to be worth anything: a
    consumer reading the lake — including `daily_bars`'s own interior-gap
    check — reads the committed generation, not the mutable curated directory
    a direct write would land in. So this is a one-step run through the
    engine, followed by the same compact the daily job ends with.
    """
    engine = JobEngine(cfg)
    trade_date = shanghai_today()
    run_id = engine.manifest.start_run(
        "derive_trading_status",
        {
            "trade_date": trade_date.isoformat(),
            "derive_start": start.isoformat() if start else None,
            "derive_end": end.isoformat() if end else None,
        },
    )
    context = {"derive_start": start, "derive_end": end, "derive_full": True}
    summary: dict = {"run_id": run_id}
    try:
        derived = engine.run_step("trading_status_derive", trade_date, run_id, context)
        summary["rows_staged"] = derived.get("rows_written", 0)
        summary["compact"] = engine.run_step("compact", trade_date, run_id)
    except Exception as exc:
        engine.manifest.finish_run(run_id, "failed", error_message=str(exc))
        raise
    step_statuses = {
        str(derived.get("status", "success")),
        str(summary["compact"].get("status", "success")),
    }
    if step_statuses.intersection({"failed", "blocked"}):
        status = "failed"
    elif step_statuses.intersection({"warning", "degraded"}):
        status = "degraded"
    else:
        status = "success"
    engine.manifest.finish_run(
        run_id,
        status,
        rows_written=summary["rows_staged"],
    )
    persisted = engine.manifest.get_run(run_id)
    summary["status"] = str(persisted["status"]) if persisted is not None else status
    return summary


@contextmanager
def _published_derive(cfg, dataset: str):
    """Make CLI derives visible to revision-aware readers under the writer lock."""
    from cnequity.file_lock import lake_mutation_lock
    from cnequity.orchestrator.run_lock import run_lock
    from cnequity.steps.finalize import (
        _layer_file_identity,
        _publish_derived_revision,
        _record_dataset_result,
    )
    from cnequity.storage.revisions import RevisionStore

    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run(f"derive:{dataset}", {"dataset": dataset})
    outcome = {"status": "success", "rows_written": 0}
    try:
        with run_lock(cfg.meta_root, run_id), lake_mutation_lock(cfg.meta_root):
            store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
            store.ensure_current(dataset)
            store.materialize_current(dataset)
            before = _layer_file_identity(cfg.derived_root / dataset)
            yield outcome
            revision = _publish_derived_revision(cfg, dataset, run_id, shanghai_today(), before)
            _record_dataset_result(
                cfg,
                run_id,
                dataset,
                "derive",
                outcome["status"],
                criticality="research",
                revision_id=revision["revision_id"] if revision else None,
                rows_written=outcome["rows_written"],
            )
            manifest.finish_run(run_id, outcome["status"], rows_written=outcome["rows_written"])
    except Exception as exc:
        manifest.finish_run(run_id, "failed", error_message=str(exc))
        raise


@cli.command()
@click.argument("name", default="adj_factors")
@config_option
@click.option(
    "--full",
    is_flag=True,
    default=False,
    help="重写 adj_factors 的全部分区（默认只从水位往后追加）。",
)
@click.option(
    "--start",
    "start_str",
    default=None,
    help="industry_index / trading_status：只派生这个日期（YYYY-MM-DD）及之后的。",
)
@click.option(
    "--end",
    "end_str",
    default=None,
    help="industry_index / trading_status：只派生这个日期（YYYY-MM-DD）及之前的。",
)
@click.option(
    "--apply",
    "apply_changes",
    is_flag=True,
    help="bse_code_migration：真正重写分区（默认只报告）。",
)
def derive(
    name: str,
    config_path: str,
    full: bool,
    start_str: str | None,
    end_str: str | None,
    apply_changes: bool,
):
    """派生计算类数据集。

    \b
    `adj_factors`、`industry_index` 和 `trading_status` 本来就是日更里的 step
    （`derive_adj_factors`、`derive_industry_index`、`trading_status_derive`），
    所以在这里跑它们属于修复或补更早的窗口，不是正常一天的一部分。
    `sector_routing`、`sector_code_map` 和 `valuation_orphans` 没有任何调度会跑，只能手动执行。
    """
    # Derive targets are lower case in the registry, and command names are
    # already case-insensitive; a target typed in caps should resolve the same.
    name = name.lower()
    cfg = _cfg(config_path)
    attach_log_file(cfg, "derive")
    start = parse_date_option(start_str, "--start")
    end = parse_date_option(end_str, "--end")
    if start and end and start > end:
        raise click.ClickException("--start 必须早于或等于 --end")
    if name == "adj_factors":
        with _published_derive(cfg, name) as outcome:
            result = compute_adj_factors(cfg, full=full)
            outcome["rows_written"] = result.rows
            if result.failed:
                outcome["status"] = "degraded"
        click.echo(f"已派生 {name}：{result.rows} 行")
        if result.failed:
            click.echo(
                f"警告：{len(result.failed)} 个 标的×类型 抓取失败（{result.fail_ratio:.1%}）",
                err=True,
            )
            raise SystemExit(1)
    elif name == "industry_index":
        from cnequity.derive.industry_index import derive_industry_index

        with _published_derive(cfg, name) as outcome:
            summary = derive_industry_index(cfg, start=start, end=end, full=full)
            outcome["rows_written"] = summary.get("rows", 0)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "trading_status":
        summary = _derive_trading_status(cfg, start=start, end=end)
        click.echo(json.dumps(summary, indent=2, default=str))
        exit_code = _run_status_exit_code(str(summary.get("status", "failed")))
        if exit_code:
            raise SystemExit(exit_code)
    elif name == "sector_routing":
        from cnequity.derive.sector_routing import derive_sector_routing

        summary = derive_sector_routing(cfg)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "sector_code_map":
        from cnequity.derive.sector_code_map import derive_sector_code_map

        summary = derive_sector_code_map(cfg)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "valuation_orphans":
        from cnequity.storage.valuation_orphans import purge_valuation_orphan_symbols

        summary = purge_valuation_orphan_symbols(cfg)
        click.echo(json.dumps(summary, indent=2, default=str))
    elif name == "bse_code_migration":
        from cnequity.storage.bse_code_migration import migrate_bse_legacy_codes

        summary = migrate_bse_legacy_codes(cfg, apply=apply_changes)
        if not apply_changes:
            summary["note"] = "report only; re-run with --apply to rewrite the partitions"
        click.echo(json.dumps(summary, indent=2, default=str))
    else:
        raise click.ClickException(f"未知的 derive 目标：{name}")


@run.command("clean")
@config_option
@click.option("--dry-run", is_flag=True, help="只报告可以删的 staging，不真删。")
@click.option(
    "--orphan-retention-days",
    default=7,
    show_default=True,
    help="删掉超过这么多天、且 manifest 里没有记录的孤儿 staging。",
)
@click.option(
    "--snapshot-retention-days",
    default=DEFAULT_SNAPSHOT_RETENTION_DAYS,
    show_default=True,
    help=(
        "删掉超过这么多天的 meta/source_snapshots run_id 目录（每个数据集 / 源的最新一份始终保留）"
        "。"
    ),
)
@click.option(
    "--force",
    is_flag=True,
    help=(
        "连还不满足清理条件的 staging 也删（批次没跑完、和/或没 compact 过）。成功的抓取批次会被降级为 failed，"
        "好让 `cne run retry` 重抓（数据是重抓不是丢失，但重试会变成整段重跑）。成功但没 compact 的 run "
        "不要用它 —— 先跑 `cne run compact --run-id`。"
    ),
)
@click.option(
    "--keep-revision-generations",
    default=5,
    show_default=True,
    type=int,
    help=(
        "meta/revisions/data 下每个数据集保留这么多代已提交版本，更老的代只删存储字节。receipt 始终保留，"
        "current.json 指向的那一代永远不删。0 表示不清理。"
    ),
)
@click.option(
    "--log-retention-days",
    default=DEFAULT_LOG_RETENTION_DAYS,
    show_default=True,
    type=int,
    help=(
        "删掉超过这么多天的 `logs/cne-*.log`。每次调用都会写一份，没有别的东西会清理它们。0 表示不清理。"
    ),
)
@click.option(
    "--reconcile-runs",
    is_flag=True,
    help="清理前，把卡在 'running'（worker 崩溃）的 run 标成 failed。",
)
@click.option(
    "--reconcile-after-seconds",
    default=None,
    type=float,
    help=("只对静默超过这么多秒的 run 做上面的对账（默认取 [orchestrator].batch_stale_seconds）。"),
)
def clean(
    config_path: str,
    dry_run: bool,
    orphan_retention_days: int,
    snapshot_retention_days: int,
    keep_revision_generations: int,
    log_retention_days: int,
    force: bool,
    reconcile_runs: bool,
    reconcile_after_seconds: float | None,
):
    """清掉已 compact 的终态 run 的 staging，以及过期的孤儿目录。

    \b
    「可清理」是指：run 处于终态（success/warning/failed）、所有批次都已落定，
    并且记录过一次成功的 compact。没跑完或从没 compact 过的 staging 会留着等重试，
    除非加了 --force。同时清理过期的 `meta/source_snapshots` run_id 目录。
    """
    cfg = _cfg(config_path)
    attach_log_file(cfg, "run-clean")
    reconciled: dict[str, int] | None = None
    if reconcile_runs:
        manifest = Manifest(cfg.manifest_path)
        stale_after = (
            float(reconcile_after_seconds)
            if reconcile_after_seconds is not None
            else cfg.batch_stale_seconds
        )
        reconciled = manifest.reconcile_orphaned_runs(
            stale_after_seconds=stale_after,
            locks_root=cfg.meta_root,
        )
    result = clean_staging(
        cfg,
        dry_run=dry_run,
        orphan_retention_days=orphan_retention_days,
        force=force,
    )
    snaps = clean_source_snapshots(
        cfg.meta_root,
        retention_days=snapshot_retention_days,
        dry_run=dry_run,
    )
    # Each commit copies the whole dataset into a new immutable generation and
    # nothing removed one, so meta/revisions grew past curated/ itself.
    generations = (
        prune_revision_generations(cfg.meta_root, keep=keep_revision_generations, dry_run=dry_run)
        if keep_revision_generations > 0
        else []
    )
    logs = clean_run_logs(cfg.data_root, retention_days=log_retention_days, dry_run=dry_run)
    click.echo(
        json.dumps(
            {
                "dry_run": dry_run,
                "reconciled": reconciled,
                "removed_run_ids": result.removed_run_ids,
                "orphan_run_ids": result.orphan_run_ids,
                "force_removed_run_ids": result.force_removed_run_ids,
                "skipped_run_ids": result.skipped_run_ids,
                "bytes_freed": (
                    result.bytes_freed
                    + snaps.bytes_freed
                    + logs.bytes_freed
                    + sum(item.freed_bytes for item in generations)
                ),
                "source_snapshots": {
                    "removed_run_dirs": snaps.removed_run_dirs,
                    "kept_run_dirs": snaps.kept_run_dirs,
                    "bytes_freed": snaps.bytes_freed,
                },
                "run_logs": {
                    "removed": len(logs.removed),
                    "kept": logs.kept,
                    "bytes_freed": logs.bytes_freed,
                },
                "revision_generations": [
                    {
                        "dataset": item.dataset,
                        "removed": len(item.removed_revision_ids),
                        "kept": len(item.kept_revision_ids),
                        "bytes_freed": item.freed_bytes,
                    }
                    for item in generations
                ],
            },
            indent=2,
        )
    )


@cli.group()
def stats():
    """meta/stats 下的湖度量表（行数、字节数、来源构成）。"""


def _stats_rebuild_if_stale(cfg, *, as_json: bool) -> None:
    """The former `cne stats refresh`: rebuild only when the lake has moved on."""
    from cnequity.storage.stats import refresh_stats_if_stale, stats_freshness

    freshness = stats_freshness(cfg)
    result = refresh_stats_if_stale(cfg)
    if result is None:
        reason = (
            "stale, but another rebuild holds the lock — nothing to do"
            if freshness.stale
            else f"current as of run {freshness.latest_run_id} — nothing to do"
        )
        click.echo(json.dumps({"rebuilt": False, "reason": reason}) if as_json else reason)
        return
    if as_json:
        click.echo(json.dumps({"rebuilt": True, **result.as_dict()}, indent=2, default=str))
        return
    click.echo(
        f"已重算（{freshness.reason or 'stale'}）："
        f"{len(result.datasets)} 个数据集、{result.rows:,} 行，"
        f"耗时 {result.elapsed_seconds:.1f}s"
    )


@stats.command("rebuild")
@config_option
@click.option(
    "--dataset",
    "dataset_names",
    multiple=True,
    help="只重算这些数据集（可重复）。其它数据集的行保持不变。",
)
@click.option(
    "--if-stale",
    is_flag=True,
    help="除非统计建好之后又跑过采集，否则什么都不做。可以安全地挂定时器。",
)
@click.option("--json", "as_json", is_flag=True, help="以 JSON 打印结果。")
def stats_rebuild(config_path: str, dataset_names: tuple[str, ...], if_stale: bool, as_json: bool):
    """从 curated 和 derived 重算 partition_stats / provenance_stats。

    \b
    默认无条件重算。要挂定时器请用 `--if-stale`：没有任何变化时它直接返回，
    并且在已有并发重算持锁时选择让路而不是排队 ——
    一个卡在全量扫描后面的面板请求，比落后一次 run 的数字更糟。

    \b
    是否过期按 run id 判断，不看时钟。只有采集会改变这个湖，
    所以在最后一次 run 之后建的统计就是新的，无论看上去多旧。
    """
    # Registry names are lower case; `--dataset` should not care about case.
    dataset_names = tuple(n.lower() for n in dataset_names)
    from cnequity.storage.stats import rebuild_stats

    cfg = _cfg(config_path)
    attach_log_file(cfg, "stats-rebuild")

    if if_stale:
        if dataset_names:
            raise click.UsageError("--if-stale 会重算整个湖；请去掉 --dataset，或者去掉 --if-stale")
        _stats_rebuild_if_stale(cfg, as_json=as_json)
        return

    try:
        result = rebuild_stats(cfg, datasets=list(dataset_names) or None)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    if as_json:
        click.echo(json.dumps(result.as_dict(), indent=2, default=str))
        return
    click.echo(
        f"{len(result.datasets)} 个数据集、{result.partitions} 个分区、"
        f"{result.rows:,} 行、{result.files} 个文件、"
        f"{result.bytes / 1e6:.1f}MB，耗时 {result.elapsed_seconds:.1f}s"
    )
    if result.empty:
        click.echo(f"还没有 parquet：{', '.join(sorted(result.empty))}")


def _scan_curated_datasets(cfg) -> list[dict]:
    """Count curated Parquet on the spot — the former `cne catalog`.

    Every call walks the whole tree, which is why `cne stats rebuild` exists.
    It stays as the answer for a lake that has never built its stats tables:
    "what is in here" should not require a build step first.
    """
    entries = []
    curated = cfg.curated_root
    if not curated.exists():
        return entries
    for ds_dir in sorted(curated.iterdir()):
        if not ds_dir.is_dir():
            continue
        files = list(ds_dir.glob("**/*.parquet"))
        # lazy count(*) resolves from parquet metadata without decoding data
        # pages — cheap even on a 10-year lake.
        rows = int(scan_parquet_files(files).select(pl.len()).collect().item()) if files else 0
        entries.append({"dataset": ds_dir.name, "files": len(files), "rows": rows})
    return entries


@stats.command("show")
@config_option
@click.option("--dataset", default=None, help="某一个数据集的逐分区明细。")
@click.option("--by-source", is_flag=True, help="改为按 source / data_version 分组。")
@click.option("--json", "as_json", is_flag=True, help="输出机器可读的 JSON。")
def stats_show(config_path: str, dataset: str | None, by_source: bool, as_json: bool):
    """汇总统计表；如果还没有统计表，就直接扫 curated。

    \b
    这条读的表由 `cne stats rebuild` 生成。没有它们时，命令退回到现场数 curated 的 Parquet：
    更慢，也更薄 —— 没有字节总量、没有来源构成、没有逐分区明细 ——
    但它能在一个从没建过任何东西的克隆上回答「这个湖里有什么」。
    这个退路就是从前的 `cne catalog`，`--json` 是它的输出。
    """
    dataset = dataset.lower() if dataset else None
    from cnequity.storage.stats import (
        load_partition_stats,
        load_provenance_stats,
        load_summary,
        stats_freshness,
    )

    cfg = _cfg(config_path)
    summary = load_summary(cfg)
    if summary is None:
        if dataset or by_source:
            raise click.ClickException(
                "还没有统计表 —— `--dataset` / `--by-source` 需要先跑 `cne stats rebuild`"
            )
        entries = _scan_curated_datasets(cfg)
        if as_json:
            click.echo(json.dumps(entries, indent=2))
            return
        click.echo("没有统计表 —— 已直接扫 curated；其余内容请先跑 `cne stats rebuild`")
        click.echo(
            pl.DataFrame(
                entries, schema={"dataset": pl.String, "files": pl.Int64, "rows": pl.Int64}
            )
        )
        return
    freshness = stats_freshness(cfg)

    df = load_provenance_stats(cfg) if by_source else load_partition_stats(cfg)
    if dataset:
        df = df.filter(pl.col("dataset") == dataset)
        if df.is_empty():
            raise click.ClickException(f"数据集 {dataset!r} 没有统计行")
    elif by_source:
        df = df.group_by(["dataset", "source", "data_version"]).agg(
            pl.col("row_count").sum(),
            pl.col("fetched_at_min").min(),
            pl.col("fetched_at_max").max(),
        )
    else:
        df = df.group_by("dataset").agg(
            pl.len().alias("partitions"),
            pl.col("row_count").sum(),
            pl.col("file_count").sum().alias("files"),
            pl.col("bytes").sum(),
            pl.col("period_start").min(),
            pl.col("period_end").max(),
        )

    stale_note = (
        f"  STALE — {freshness.reason}; run `cne stats rebuild --if-stale`"
        if freshness.stale
        else ""
    )
    click.echo(
        f"生成于：{summary.get('generated_at')}  run：{summary.get('latest_run_id')}{stale_note}"
    )
    if as_json:
        click.echo(json.dumps(df.sort(df.columns[:2]).to_dicts(), indent=2, default=str))
        return
    with pl.Config(tbl_rows=-1, tbl_cols=-1, fmt_str_lengths=32):
        click.echo(df.sort(df.columns[:2]))
