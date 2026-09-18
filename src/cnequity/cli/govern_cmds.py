"""Contracts, profiles and snapshots — the reproducibility surface.

These are what let a published result name exactly the data it used: a
fingerprinted dataset contract, a versioned research universe, and an immutable
checksummed copy of the bytes.
"""

from __future__ import annotations

import json
import tarfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import click

from cnequity.cli._root import cli
from cnequity.cli._shared import (
    _cfg,
    attach_log_file,
    config_option,
)


def _profile_list_payload(include_compatibility: bool) -> list[dict]:
    from cnequity.domain.universe_profiles import list_universe_profiles

    return list_universe_profiles(include_compatibility=include_compatibility)


def _profile_show_payload(name: str, symbols: tuple[str, ...]) -> dict:
    from cnequity.domain.universe_profiles import (
        resolve_universe_profile,
        show_universe_profile,
    )

    try:
        payload = show_universe_profile(name)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    if symbols:
        payload["concrete_scope_hash"] = resolve_universe_profile(name).symbol_scope_hash(symbols)
        payload["symbols"] = sorted(
            {symbol.strip().upper() for symbol in symbols if symbol.strip()}
        )
    return payload


@cli.group("profile")
def profile_grp():
    """查看带版本的研究 universe profile。"""


@profile_grp.command("list")
@click.option(
    "--include-compatibility/--official-only",
    default=True,
    show_default=True,
    help="列表里包含历史遗留的 universe 别名。",
)
def profile_list(include_compatibility: bool):
    """列出 profile 注册表记录（机器可读）。"""

    click.echo(
        json.dumps(_profile_list_payload(include_compatibility), ensure_ascii=False, indent=2)
    )


@profile_grp.command("show")
@click.argument("name")
@click.option(
    "--symbol",
    "symbols",
    multiple=True,
    help="把 profile 绑定到具体标的，并附带 concrete_scope_hash。",
)
def profile_show(name: str, symbols: tuple[str, ...]):
    """展示某个带版本的 profile 及其稳定的 scope hash。"""

    click.echo(json.dumps(_profile_show_payload(name, symbols), ensure_ascii=False, indent=2))


@cli.group("contract")
def contract_grp():
    """查看并校验已注册的数据集数据契约。"""


@contract_grp.command("show")
@click.argument("dataset", required=False)
@click.option(
    "--dataset",
    "dataset_option",
    default=None,
    help="数据集名（也可以直接作为参数传）。不给则输出完整契约。",
)
@click.option(
    "--out",
    "--output",
    "--path",
    "output_path",
    default="-",
    show_default=True,
    help="把 JSON 写到这个路径而不是标准输出；'-' 表示打印。",
)
@click.option("--json", "as_json", is_flag=True, help="输出机器可读的 JSON（默认）。")
def contract_show(dataset: str | None, dataset_option: str | None, output_path: str, as_json: bool):
    """展示某一个数据集的契约，或整个注册表的契约。

    \b
    `--out PATH` 表示写文件而不是打印：那份文件就是随发布一起提交的契约快照，
    也正是 `cne contract diff` 回读、用来判断后来的注册表是兼容还是破坏性变更的东西。
    """
    from cnequity.domain.contracts import (
        build_contract,
        contract_json,
        dataset_contract,
        export_contract,
    )

    # Registry names are lower case, and command names are already
    # case-insensitive; a dataset typed in caps should resolve the same way.
    name = (dataset_option or dataset or "").lower() or None
    try:
        payload = dataset_contract(name) if name else build_contract()
    except KeyError as exc:
        raise click.ClickException(str(exc)) from exc
    # ``--json`` is intentionally a no-op today: JSON is the stable output
    # shape for this command. Keeping the option makes scripts explicit and
    # leaves room for a future human table without changing their invocation.
    del as_json

    if output_path == "-":
        click.echo(contract_json(payload))
        return
    if name:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        Path(output_path).write_text(contract_json(payload) + "\n", encoding="utf-8")
    else:
        export_contract(output_path)
    click.echo(f"已写入 {output_path}")


@contract_grp.command("diff")
@click.argument("old_contract", required=False)
@click.argument("new_contract", required=False)
@click.option("--old", "old_option", default=None, help="基线契约文件路径。")
@click.option("--new", "new_option", default=None, help="候选契约文件路径。")
@click.option("--from", "from_option", default=None, help="--old 的别名。")
@click.option("--to", "to_option", default=None, help="--new 的别名。")
@click.option("--json", "as_json", is_flag=True, help="输出机器可读的 JSON。")
@click.option(
    "--allow-breaking",
    is_flag=True,
    help="即使发现破坏性变更也返回退出码 0。",
)
def contract_diff(
    old_contract: str | None,
    new_contract: str | None,
    old_option: str | None,
    new_option: str | None,
    from_option: str | None,
    to_option: str | None,
    as_json: bool,
    allow_breaking: bool,
):
    """比较 OLD_CONTRACT 与 NEW_CONTRACT（默认用当前注册表）。"""
    from cnequity.domain.contracts import contract_json, diff_contracts, format_contract_diff

    old_path = old_option or from_option or old_contract
    new_path = new_option or to_option or new_contract
    if old_path is None:
        raise click.UsageError("请给出 OLD_CONTRACT 或 --old/--from")
    try:
        diff = diff_contracts(old_path, new_path)
    except (OSError, TypeError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    if as_json:
        click.echo(contract_json(diff))
    else:
        click.echo(format_contract_diff(diff))
    if diff["is_breaking"] and not allow_breaking:
        raise SystemExit(1)


@contract_grp.command("validate")
@click.argument("contract_path", required=False)
@click.option(
    "--path", "path_option", default=None, help="契约 JSON 路径（也可以直接作为参数传）。"
)
@click.option("--json", "as_json", is_flag=True, help="输出机器可读的 JSON。")
@click.option(
    "--against-registry",
    is_flag=True,
    help="要求文件里的契约与当前的 DATASETS / SCHEMAS / PRIMARY_KEYS 完全一致。",
)
def contract_validate(
    contract_path: str | None,
    path_option: str | None,
    as_json: bool,
    against_registry: bool,
):
    """校验一份契约文件；不给则校验当前注册表。"""
    from cnequity.domain.contracts import contract_json, validate_contract

    contract_path = path_option or contract_path
    errors = validate_contract(
        contract_path,
        against_registry=True if (contract_path is None or against_registry) else False,
    )
    if as_json:
        click.echo(contract_json({"valid": not errors, "errors": errors}))
    elif errors:
        for error in errors:
            click.echo(f"ERROR: {error}", err=True)
    else:
        click.echo("契约检查通过")
    if errors:
        raise SystemExit(1)


@contextmanager
def _snapshot_operator_errors() -> Iterator[None]:
    """Surface snapshot input/data problems as Click errors, not tracebacks.

    A snapshot that is missing, corrupt or fails verification is operator
    input, and the store signals it with a plain ``FileNotFoundError`` or
    ``ValueError``. ``export`` and ``import`` already made this call; the rest
    of the group printed a Python traceback for the identical class of
    failure, so naming a snapshot that does not exist looked like a crash.
    """
    try:
        yield
    except (OSError, ValueError, RuntimeError, KeyError, tarfile.TarError) as exc:
        raise click.ClickException(str(exc)) from exc


@cli.group("snapshot")
def snapshot_grp():
    """创建、校验并安全恢复可移植的湖快照。"""


@snapshot_grp.command("create")
@click.argument("name")
@click.option(
    "--dataset",
    "datasets",
    multiple=True,
    required=True,
    help="要包含的数据集（可重复）。快照永远是显式指定的，不会是整个湖。",
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="快照放在哪；默认是数据根目录下的 meta/snapshots。",
)
def snapshot_create(
    name: str, datasets: tuple[str, ...], config_path: str, snapshot_root: Path | None
):
    """把指定的数据集冻结成一份新的、不可变的快照。

    \b
    manifest 会记下每个 Parquet 文件的大小和 SHA-256，连同数据集状态、契约指纹和 run 血缘 ——
    足够让后来的人证明某个已发布的结果用的正是这些字节。命令会打印 manifest 路径。
    """
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-create")
    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        manifest = SnapshotStore(cfg, snapshot_root).create(name, list(datasets))
    click.echo(str(manifest))


@snapshot_grp.command("verify")
@click.argument("name")
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="快照放在哪；默认是数据根目录下的 meta/snapshots。",
)
def snapshot_verify(name: str, config_path: str, snapshot_root: Path | None):
    """把快照里每个文件重新哈希，与 manifest 对账。

    \b
    一旦发现大小或摘要不符就退出 1，所以它可以直接当定时任务里的门禁。
    在信任一份不是你刚刚创建的快照之前先跑它 ——
    位腐烂和被截断的拷贝，在哈希对不上之前长得一模一样。
    """
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-verify")
    from dataclasses import asdict

    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        result = SnapshotStore(cfg, snapshot_root).verify(name)
    click.echo(json.dumps(asdict(result), indent=2, ensure_ascii=False))
    if not result.passed:
        raise SystemExit(1)


@snapshot_grp.command("restore")
@click.argument("name")
@click.argument("target", type=click.Path(path_type=Path))
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="快照放在哪；默认是数据根目录下的 meta/snapshots。",
)
def snapshot_restore(name: str, target: Path, config_path: str, snapshot_root: Path | None):
    """把快照恢复到 TARGET，TARGET 必须是新建的或空目录。

    \b
    活跃的湖根目录会被拒绝，已存在的文件永远不会被覆盖：
    恢复是为了把旧版本摆在当前版本旁边看，不是把线上的湖回滚。
    在把任何东西指向它之前，先对 TARGET 跑一次 `cne status --datasets` 看看结果。
    """
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-restore")
    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        restored = SnapshotStore(cfg, snapshot_root).restore(name, target)
    click.echo(str(restored))


@snapshot_grp.command("export")
@click.argument("name")
@click.argument("destination", type=click.Path(path_type=Path), required=False)
@click.option(
    "--compression",
    type=click.Choice(["auto", "zstd", "gzip", "none"]),
    default="auto",
    show_default=True,
    help="归档编码；auto 优先 tar.zst，不行则退回 tar.gz。",
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="快照放在哪；默认是数据根目录下的 meta/snapshots。",
)
def snapshot_export(
    name: str,
    destination: Path | None,
    compression: str,
    config_path: str,
    snapshot_root: Path | None,
):
    """把快照 NAME 流式打包成一个可移植的 tar 归档。"""
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-export")
    from cnequity.storage.snapshots import SnapshotStore

    try:
        archive = SnapshotStore(cfg, snapshot_root).export_archive(
            name,
            destination,
            compression=compression,
        )
    except (OSError, ValueError, RuntimeError, KeyError, tarfile.TarError) as exc:
        # Snapshot validation failures are operator input/data errors.  Keep
        # Click's normal one-line error surface; a Python traceback is not
        # useful when an archive is missing, corrupt or fails verification.
        raise click.ClickException(str(exc)) from exc
    click.echo(str(archive))


@snapshot_grp.command("import")
@click.argument("archive", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option("--name", default=None, help="导入后的快照名；默认取归档文件名。")
@click.option(
    "--overwrite",
    is_flag=True,
    help="仅在归档校验通过之后，才替换已存在的同名快照。",
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="快照放在哪；默认是数据根目录下的 meta/snapshots。",
)
def snapshot_import(
    archive: Path,
    name: str | None,
    overwrite: bool,
    config_path: str,
    snapshot_root: Path | None,
):
    """校验 ARCHIVE 并原子地导入快照库。"""
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-import")
    from cnequity.storage.snapshots import SnapshotStore

    try:
        restored = SnapshotStore(cfg, snapshot_root).import_archive(
            archive,
            name=name,
            overwrite=overwrite,
        )
    except (OSError, ValueError, RuntimeError, KeyError, tarfile.TarError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(str(restored))


@snapshot_grp.group("delta")
def snapshot_delta_grp():
    """创建、校验并应用可移植的增量包。"""


def _snapshot_delta_create(
    name: str,
    baseline: Path | None,
    target: Path | None,
    from_revision: int | None,
    datasets: tuple[str, ...],
    config_path: str,
    snapshot_root: Path | None,
) -> None:
    from cnequity.storage.snapshots import SnapshotStore

    # ``--from 12`` was used by an early command draft before the explicit
    # ``--from-revision`` spelling existed.  Accept it when it is not a real
    # path, while preserving normal two-root paths named with digits.
    if from_revision is None and baseline is not None and not baseline.exists():
        raw = str(baseline)
        if raw.isdigit():
            from_revision = int(raw)
            baseline = None
    if from_revision is None and baseline is None:
        raise click.UsageError("请给出 --from BASELINE 或 --from-revision REVISION")
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-delta-create")
    with _snapshot_operator_errors():
        if from_revision is not None:
            manifest = SnapshotStore(cfg, snapshot_root).create_delta(
                name,
                datasets=list(datasets),
                target=target,
                from_revision=from_revision,
            )
        else:
            manifest = SnapshotStore(cfg, snapshot_root).create_delta(
                name,
                baseline=baseline,
                target=target,
                datasets=list(datasets) if datasets else None,
            )
    click.echo(str(manifest))


@snapshot_delta_grp.command("create")
@click.argument("name")
@click.option(
    "--from",
    "baseline",
    type=click.Path(path_type=Path),
    default=None,
    help="基线湖根目录。目标根目录会与它逐字节比对。",
)
@click.option(
    "--to",
    "target",
    type=click.Path(path_type=Path),
    default=None,
    help="目标湖根目录；默认取配置里当前生效的根目录。",
)
@click.option(
    "--from-revision",
    type=int,
    default=None,
    help="用目标里已提交的 revision 作为基线前置条件。",
)
@click.option(
    "--dataset",
    "datasets",
    multiple=True,
    help="要包含的数据集（可重复）。不给则自动发现两个根目录里都有的数据集。",
)
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="增量包放在哪；默认是数据根目录下的 meta/snapshots。",
)
def snapshot_delta_create(
    name: str,
    baseline: Path | None,
    target: Path | None,
    from_revision: int | None,
    datasets: tuple[str, ...],
    config_path: str,
    snapshot_root: Path | None,
):
    """把 NAME 创建成一个不可变的 增加/替换/删除 包。"""

    _snapshot_delta_create(
        name, baseline, target, from_revision, datasets, config_path, snapshot_root
    )


@snapshot_delta_grp.command("verify")
@click.argument("name")
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="增量包放在哪；默认是数据根目录下的 meta/snapshots。",
)
def snapshot_delta_verify(name: str, config_path: str, snapshot_root: Path | None):
    """校验全部 增加/替换 载荷的哈希与变更语义。"""
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-delta-verify")

    from dataclasses import asdict

    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        result = SnapshotStore(cfg, snapshot_root).verify_delta(name)
    click.echo(json.dumps(asdict(result), indent=2, ensure_ascii=False))
    if not result.passed:
        raise SystemExit(1)


@snapshot_delta_grp.command("apply")
@click.argument("name")
@click.argument("target", type=click.Path(path_type=Path))
@click.option("--dry-run", is_flag=True, help="只校验前置条件，不改动 TARGET。")
@config_option
@click.option(
    "--snapshot-root",
    type=click.Path(path_type=Path),
    default=None,
    help="增量包放在哪；默认是数据根目录下的 meta/snapshots。",
)
def snapshot_delta_apply(
    name: str,
    target: Path,
    dry_run: bool,
    config_path: str,
    snapshot_root: Path | None,
):
    """把 NAME 安全地应用到非空的 TARGET 湖根目录上。"""
    cfg = _cfg(config_path)
    attach_log_file(cfg, "snapshot-delta-apply")

    from cnequity.storage.snapshots import SnapshotStore

    with _snapshot_operator_errors():
        applied = SnapshotStore(cfg, snapshot_root).apply_delta(name, target, dry_run=dry_run)
    click.echo(str(applied))
