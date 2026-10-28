"""Regenerate every documentation page that mirrors code, or check they still do.

One entry point for what used to be five scripts. Each target renders the text
its file should hold from the code it mirrors; ``--check`` fails instead of
writing, which is what CI runs.

    python scripts/dev/sync_docs.py                 # rewrite every target
    python scripts/dev/sync_docs.py --check         # CI: fail on any drift
    python scripts/dev/sync_docs.py cli-options     # one target

Targets:

* ``schema`` — the column tables in ``docs/zh/datasets/schema.md`` (synced, not
  generated: the hand-written 说明 cells survive, see below);
* ``config`` — ``configs/cnequity.example.toml`` mirrors the packaged template;
* ``derivatives`` — the capability table in ``docs/zh/datasets/sources.md``;
* ``cli-options`` — ``docs/zh/reference/cli-options.md`` from Click's registry;
* ``cli-surface`` — ``docs/zh/reference/cli-surface.md``, the reviewed side-effect
  inventory; a new command fails until its row is written below;
* ``pypi-readme`` — ``README.pypi.md`` is ``README.md`` with every relative link
  made absolute, since PyPI resolves none of them.

The English site (``docs/en``) has its own copy of each generated page:
``schema-en``, ``derivatives-en``, ``cli-options-en`` and ``cli-surface-en``. Click
help is English in the code and Chinese in the package catalog, so each page
renders its own language from there (``scripts/dev/i18n_catalog.py`` fails on a
help text with no Chinese). The side-effect inventory is written below in
Chinese and translated through ``scripts/dev/i18n/cli_surface.en.yml``; a
command without an entry there fails the check.

Schema page notes:

The page is not generated. Its prose carries rationale that no schema can hold
— why ``trading_status`` splits ST from suspension, what the volume unit is —
and regenerating it would throw that away. What it cannot maintain by hand is
the mechanical half: 11 of 42 datasets had no section at all, and a column
added in ``domain/schemas.py`` reached the docs only if someone remembered.

So this syncs rather than generates. Column names, order and types come from
``DATASET_SCHEMAS``; the ``说明`` cell is hand-written and is carried across by
column name. A column that leaves the schema loses its row (and its
description) — that is the point.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable
from pathlib import Path

import click
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from cnequity.adapters.futures_exchange.registry import capabilities  # noqa: E402
from cnequity.cli.main import cli  # noqa: E402
from cnequity.i18n import translate_to  # noqa: E402

DOC_PATH = REPO_ROOT / "docs" / "zh" / "datasets" / "schema.md"
DOC_PATH_EN = REPO_ROOT / "docs" / "en" / "datasets" / "schema.md"
I18N = Path(__file__).resolve().parent / "i18n"

# --- schema ------------------------------------------------------------------

# The page's own vocabulary, not Polars'. Keep the spellings already in use so
# a sync does not rewrite every row the first time it runs.
_TYPE_NAMES = {
    "String": "string",
    "Date": "date",
    "Boolean": "bool",
    "Float64": "float64",
    "Int8": "int8",
    "Int32": "int32",
    "Int64": "int64",
    "Datetime(time_unit='us', time_zone='UTC')": "timestamp",
    "Datetime(time_unit='us', time_zone=None)": "timestamp（naive）",
}

_TABLE_DIVIDER = "|--------|------|-------|"
# Per language: the column-table header, and the heading new sections go before.
_SCHEMA_LABELS = {
    "zh": {"header": "| 列 | 类型 | 说明 |", "anchor": "### Compact 去重"},
    "en": {"header": "| Column | Type | Description |", "anchor": "### Compact deduplication"},
}


def _type_name(dtype: object, lang: str = "zh") -> str:
    text = str(dtype)
    name = _TYPE_NAMES.get(text, text.lower())
    return name.replace("（naive）", " (naive)") if lang == "en" else name


def _schemas() -> dict[str, dict[str, object]]:
    from cnequity.domain.schemas import DATASET_SCHEMAS

    return DATASET_SCHEMAS


def _heading_datasets(heading: str, datasets: list[str]) -> list[str]:
    """Return every dataset a ``####`` heading documents.

    Headings are not always one dataset (``minute_bars / minute_bars_5m``) and
    not always bare (``stock_news（按需缓存）``).
    """
    return [
        name for name in datasets if re.search(rf"(?<![\w_]){re.escape(name)}(?![\w_])", heading)
    ]


def _split_table(
    lines: list[str], start: int, header: str = _SCHEMA_LABELS["zh"]["header"]
) -> tuple[int, int, dict[str, str]]:
    """Locate the first table after *start*; return its bounds and descriptions."""
    first_cell = header.split("|")[1].strip()
    index = start
    while index < len(lines) and not lines[index].startswith(f"| {first_cell} "):
        if lines[index].startswith("#### "):
            return -1, -1, {}
        index += 1
    if index >= len(lines):
        return -1, -1, {}
    table_start = index
    index += 2  # header + divider
    descriptions: dict[str, str] = {}
    while index < len(lines) and lines[index].startswith("|"):
        cells = [cell.strip() for cell in lines[index].strip().strip("|").split("|")]
        if cells:
            description = cells[2] if len(cells) > 2 else ""
            # One row often stands for several columns that share a note --
            # "open / high / low / close" or "source / data_version /
            # fetched_at". Expanding those to one row per column must carry the
            # note to each of them, or the sync quietly deletes the sentence
            # that explained the group.
            for name in (part.strip() for part in cells[0].split("/")):
                if name and (name not in descriptions or not descriptions[name]):
                    descriptions[name] = description
        index += 1
    return table_start, index, descriptions


def _render_table(
    schema: dict[str, object], descriptions: dict[str, str], lang: str = "zh"
) -> list[str]:
    rows = [_SCHEMA_LABELS[lang]["header"], _TABLE_DIVIDER]
    for column, dtype in schema.items():
        rows.append(f"| {column} | {_type_name(dtype, lang)} | {descriptions.get(column, '')} |")
    return rows


def sync(text: str, lang: str = "zh") -> tuple[str, list[str]]:
    labels = _SCHEMA_LABELS[lang]
    schemas = _schemas()
    datasets = list(schemas)
    lines = text.split("\n")
    notes: list[str] = []
    documented: set[str] = set()

    index = 0
    while index < len(lines):
        if not lines[index].startswith("#### "):
            index += 1
            continue
        heading = lines[index][5:].strip()
        names = _heading_datasets(heading, datasets)
        if not names:
            index += 1
            continue
        documented.update(names)
        table_start, table_end, descriptions = _split_table(lines, index + 1, labels["header"])
        if table_start < 0:
            notes.append(f"{heading}: no column table found, left alone")
            index += 1
            continue
        rendered = _render_table(schemas[names[0]], descriptions, lang)
        if lines[table_start:table_end] != rendered:
            notes.append(f"{heading}: column table synced")
        lines[table_start:table_end] = rendered
        index = table_start + len(rendered)

    missing = [name for name in datasets if name not in documented]
    if missing:
        anchor = next(
            (i for i, line in enumerate(lines) if line.startswith(labels["anchor"])),
            len(lines),
        )
        block: list[str] = []
        for name in missing:
            block.append(f"#### {name}")
            block.append("")
            block.extend(_render_table(schemas[name], {}, lang))
            block.append("")
            notes.append(f"{name}: section added")
        lines[anchor:anchor] = block

    return "\n".join(lines), notes


# --- cli-options -------------------------------------------------------------


def _option_leaves(group: click.Group, prefix: str = ""):
    for name, command in sorted(group.commands.items()):
        path = f"{prefix} {name}".strip()
        if isinstance(command, click.Group):
            yield from _option_leaves(command, path)
        else:
            yield path, command


def _cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ").strip()


_OPTION_WORDS = {
    "zh": {
        "title": "# CLI 参数与默认值",
        "intro": "由 `scripts/dev/sync_docs.py` 从 Click 注册表生成。命令用途、副作用与操作场景分别见 [CLI 参考](cli.md) 和 [副作用清单](cli-surface.md)。",
        "header": "| 参数 | 默认值 | 说明 |",
        "runtime": "运行时计算",
        "empty_list": "空列表",
        "positional": "位置参数",
        "required": "必填",
        "required_prefix": "必填；",
        "no_params": "无参数",
    },
    "en": {
        "title": "# CLI options and defaults",
        "intro": "Generated by `scripts/dev/sync_docs.py` from the Click registry. For what each command is for, its side effects and when to use it, see the [CLI reference](cli.md) and the [side-effect inventory](cli-surface.md). `cne <command> --help` prints the same options in Chinese.",
        "header": "| Option | Default | Description |",
        "runtime": "computed at runtime",
        "empty_list": "empty list",
        "positional": "positional argument",
        "required": "Required",
        "required_prefix": "Required; ",
        "no_params": "No options",
    },
}


def _default(param: click.Parameter, ctx: click.Context, lang: str = "zh") -> str:
    words = _OPTION_WORDS[lang]
    # A string `show_default` is what `--help` prints in place of the value,
    # e.g. a default that depends on another option.
    shown = getattr(param, "show_default", None)
    if isinstance(shown, str):
        return shown
    value = param.get_default(ctx, call=False)
    if value.__class__.__name__ == "Sentinel":
        return "—"
    if callable(value):
        return words["runtime"]
    if value is None:
        return "—"
    if isinstance(value, (tuple, list)):
        return ", ".join(map(str, value)) or words["empty_list"]
    return str(value)


def _load_catalog(name: str) -> dict:
    return yaml.safe_load((I18N / name).read_text(encoding="utf-8")) or {}


def _help_in(lang: str, param: click.Parameter) -> str:
    """An option's help in a docs language, from the package catalog.

    The written help is the English msgid; `docs/zh` renders its Chinese.
    """
    written = param.__dict__.get("help") or ""
    return translate_to("zh_CN" if lang == "zh" else "en", written) if written else ""


def render_cli_options(lang: str = "zh") -> str:
    words = _OPTION_WORDS[lang]
    lines = [words["title"], "", words["intro"], ""]
    for path, command in _option_leaves(cli):
        ctx = click.Context(command)
        lines.extend([f"## `cne {path}`", "", words["header"], "|---|---|---|"])
        for param in command.params:
            if isinstance(param, click.Option):
                label = ", ".join([*param.opts, *param.secondary_opts])
                description = _help_in(lang, param)
            else:
                label = param.name or ""
                description = words["positional"]
            if param.required:
                description = (
                    f"{words['required_prefix']}{description}" if description else words["required"]
                )
            description = description or "—"
            lines.append(
                f"| `{_cell(label)}` | `{_cell(_default(param, ctx, lang))}` | {_cell(description)} |"
            )
        if not command.params:
            lines.append(f"| — | — | {words['no_params']} |")
        lines.append("")
    return "\n".join(lines)


# --- cli-surface -------------------------------------------------------------

# Each leaf has one concise user-visible side-effect classification. Adding a
# command fails --check until someone reviews its request and write boundary.
EFFECTS: dict[str, tuple[str, str]] = {
    "audit": ("条件", "读取湖、输出审计；启用外部对照时访问源"),
    "backfill": ("执行时", "补历史并写暂存与发布数据；--plan 仅读配置和状态"),
    "check": ("无", "读取状态、新鲜度、审计结果与统计；统计过期时重算，--full 重跑全湖审计"),
    "config": (
        "无",
        "create 写继承内置默认值的个人配置；upgrade 备份后补调度 step，--minimize 备份后改写为继承默认值；validate/diff/show/path 仅读",
    ),
    "contract diff": ("无", "读取契约并输出差异"),
    "contract show": ("无", "读取契约；指定输出路径时写文件"),
    "contract validate": ("无", "校验契约"),
    "decision-data cash-rights": ("无", "读取湖并输出决策资料"),
    "decision-data payment-gaps": ("无", "读取湖并输出决策资料"),
    "decision-data stock-terms": ("无", "读取湖并输出决策资料"),
    "delisted status": ("无", "读取退市状态"),
    "derive": ("条件", "写派生数据；adj_factors 等模式可访问源"),
    "doctor": ("无", "离线检查配置与环境"),
    "init": ("条件", "sample 离线；demo/quick/full 取数；layout-only 创建目录"),
    "mcp": ("条件", "默认读湖；--live 可取源数据；--http 监听本地端口"),
    "profile list": ("无", "列出内置范围"),
    "profile show": ("无", "显示内置范围"),
    "query": ("条件", "SQL 读湖；按需数据缓存未命中或刷新时取数"),
    "repair corporate-action-gaps": (
        "--apply 时",
        "默认离线预览；--apply 按缺口查询 Baostock 与巨潮资讯，核验后发布新版本并保留旧版本",
    ),
    "repair layout": ("无", "默认只预览；--apply 发布新版本，保留旧版本和重复观察证据"),
    "repair status-traded": (
        "无",
        "默认只读预览；--apply 用已提交成交日线补缺失状态键，ST 保持未知，发布新版本并保留旧版本",
    ),
    "import shfe-archive": (
        "无",
        "默认只读预览；--apply 校验本地上期所年度包后写入缺字段的日线行，不覆盖已有主键，保存原包证据",
    ),
    "migrate": (
        "条件",
        "默认只读预览；--apply 按顺序运行还欠的迁移，经修复门禁发布并保留旧版本，结果写入 meta/state/lake.json；fund-listing-dates 读交易所基金列表，misdated-valuation 有待替换行时读东财数据中心",
    ),
    "repair payment-dates": (
        "仅 --apply 时",
        "默认只读预览；--apply 用已审发行人公告（巨潮、北交所）与 Baostock 修复现金到账日，经回填门禁发布",
    ),
    "repair delisted-actions": (
        "仅 --apply 时",
        "默认只读预览；--apply 从 Baostock（沪深）或同花顺（北交所）补已退市证券的历史分红除权并写湖",
    ),
    "repair ex-dates": (
        "仅 --apply 时",
        "默认只读预览；--apply 按给定除权日逐日向东财请求历史除权行并写湖",
    ),
    "repair bar-amounts": (
        "仅 --apply 时",
        "默认只读预览；--apply 请求通达信核对新浪补的日线，只补成交额，不改价格与成交量",
    ),
    "repair bar-turnover": (
        "仅 --apply 时",
        "默认只读预览；--apply 请求 Baostock（--peer-review 另请求交易所历史、通达信与东财）整行替换成交额异常的沪深股票日线",
    ),
    "repair bse-tip-turnover": (
        "仅 --apply 时",
        "默认只读预览；--apply 请求北交所行情板，只补最近一个交易日的成交额",
    ),
    "repair valuation-outage": (
        "仅 --apply 时",
        "默认只读预览；--apply 请求东财数据中心估值报表补断档窗口，取到全市场才写入",
    ),
    "repair adj-factors": (
        "仅 --refresh-sources 联网",
        "默认离线预览并保存计划；--apply 发布有证据支持的修正、保留旧版本和未解决提示，分批保存历史进度",
    ),
    "repair stock-listing-dates": (
        "仅 --refresh-sources 联网",
        "默认离线回放 IPO 日历；--apply 补官方上市事件证实的股票上市日期，经修复门禁发布并保留旧版本",
    ),
    "repair index-snapshots": (
        "仅 --refresh-sources 联网",
        "默认离线回放官方文件；--apply 按实际日期替换完整指数成分快照及真实权重，经修复门禁发布并保留旧版本",
    ),
    "repair index-history": (
        "仅 --refresh-sources 联网",
        "默认离线回放所选深证成指或科创50的已核验官方附件目录；--apply 按生效日替换完整历史名单，保留官方实际日期权重及旧版本；未知历史权重为空",
    ),
    "repair financial-originals": (
        "仅 --refresh-sources 联网",
        "默认离线回放原始定期报告；--apply 追加能明确识别的合并报表当期数，经修复门禁发布并保留已有版本和真实采集时间",
    ),
    "repair fund-no-print": (
        "仅 --refresh-sources 联网",
        "默认离线预览；联网采证会归档报表并更新缓存；--apply 保存基金竞价日线覆盖证明、结清对应欠账并刷新水位，不生成日线或停牌行",
    ),
    "repair fund-volume": (
        "仅 --refresh-sources 联网",
        "默认离线预览；联网归档上交所行情服务的历史 K 线，金额冲突时复用或采集东财证据；--apply 核验后修复沪深场内基金成交量，经修复门禁发布并保留旧版",
    ),
    "repair repartition": (
        "无",
        "默认只读列出或预览；--apply 在湖锁下按登记粒度改写分区并整体换上",
    ),
    "run clean": ("无", "仅预览过期文件；显式 --reconcile-runs 可修改运行状态"),
    "run compact": ("无", "将已结束、未发布的暂存数据发布到湖"),
    "run daily": ("执行时", "增量取数并写湖；默认含全部调度组与事件流"),
    "run events": ("执行时", "事件取数并写湖"),
    "run maintain": (
        "执行时",
        "日更后的维护：审计、新鲜度门禁、源探针与 SLO、运行证据核验（只读），写元数据备份并按保留期轮换旧备份，过期文件仅预览",
    ),
    "run retry": ("执行时", "重试失败范围并写湖"),
    "serve": (
        "条件",
        "启动湖面板。回环地址可从操作页发起白名单内的取数与日更；--read-only 关闭写入口；远程取数须 --allow-remote-ops；存储清理仍须网页确认；取数开关经预览确认后写回配置并备份，不因此启动取数",
    ),
    "schedule install": (
        "无",
        "确认后为当前用户注册系统定时任务（launchd/cron/任务计划），写定时设置；保留面板设置的备份与事件流；--events 按配置的 [[job.events.schedules]] 运行事件流",
    ),
    "schedule remove": ("无", "确认后停用定时并注销系统任务；正在运行的任务继续"),
    "schedule status": ("无", "只读：系统定时任务状态与最近一次检查"),
    "snapshot create": ("无", "创建本地快照"),
    "snapshot delta apply": ("无", "应用本地增量包；--dry-run 仅校验"),
    "snapshot delta create": ("无", "创建本地增量包"),
    "snapshot delta verify": ("无", "校验本地增量包"),
    "snapshot export": ("无", "导出本地快照"),
    "snapshot import": ("无", "导入本地快照"),
    "snapshot restore": ("无", "恢复本地快照到目标目录"),
    "snapshot verify": ("无", "校验本地快照"),
    "sources limits": ("无", "读取出口预算、冷却及本地欠账；不探测源"),
    "sources policy": ("无", "读取源政策"),
    "sources probe": ("条件", "--list 离线；探测时访问源并写报告"),
    "sources resilience": ("无", "读取登记源与探测证据；可写报告"),
    "sources slo": ("无", "读取探测历史并写统计"),
    "sources substitutes": ("条件", "默认读报告；--probe 访问源"),
    "stats rebuild": ("无", "重建本地统计"),
    "stats show": ("无", "读取本地统计；过期或明细视图需要时自动重算"),
    "status": ("无", "读取运行状态与数据集覆盖"),
    "storage apply": ("无", "复核版本计划并 mark；CLI purge 禁用，删除须在 serve 网页确认"),
    "storage explain": ("无", "读取版本保留依据"),
    "storage hold": ("无", "增加对象保护并取消待删除标记"),
    "storage import": ("无", "校验并登记引用清单；只增加保护"),
    "storage inspect": ("无", "读取版本与保留原因"),
    "storage plan": ("无", "校验保留依赖并保存版本计划；不删除数据"),
    "storage raw-external": ("无", "登记外置原文目录；本地保留来源记录，不移动或删除文件"),
    "ths-official backfill": ("执行时", "调用凭证源并补缺"),
    "ths-official capture": ("执行时", "调用凭证源并写对照证据"),
    "ths-official repair-bars": ("执行时", "预演也取数；--apply 写修复"),
    "ths-official resource-sectors": ("执行时", "预演也取数；--apply 写暂存"),
    "verify": ("条件", "默认本地校验；--repair 访问源并修复"),
}


def _surface_leaves(group: click.Group, prefix: str = "") -> list[str]:
    result: list[str] = []
    for name, command in group.commands.items():
        path = f"{prefix} {name}".strip()
        result.extend(
            _surface_leaves(command, path) if isinstance(command, click.Group) else [path]
        )
    return result


_SURFACE_HEAD = {
    "zh": [
        "# 联网与写入清单",
        "",
        "由 `scripts/dev/sync_docs.py` 从 Click 命令注册表核对。‘执行时’指运行命令主体；`--help` 不执行。详细源选择与限制见[取数策略](../operations/fetch-policy.md)。",
        "",
        "| 命令 | 第三方取数 | 本地效果 |",
        "|---|---|---|",
    ],
    "en": [
        "# Network and write inventory",
        "",
        "Checked against the Click command registry by `scripts/dev/sync_docs.py`. \u201cWhen run\u201d means when the command body executes; `--help` executes nothing. For source selection and limits, see [Fetching and source protection](../operations/fetch-policy.md).",
        "",
        "| Command | Third-party fetching | Local effects |",
        "|---|---|---|",
    ],
}


def render_cli_surface(lang: str = "zh") -> str:
    actual = set(_surface_leaves(cli))
    declared = set(EFFECTS)
    if actual != declared:
        raise ValueError(
            f"CLI side-effect inventory drift: missing={sorted(actual - declared)}, "
            f"removed={sorted(declared - actual)}"
        )
    effects = EFFECTS
    if lang == "en":
        effects = {key: tuple(value) for key, value in _load_catalog("cli_surface.en.yml").items()}
        if set(effects) != declared:
            raise SystemExit(
                "scripts/dev/i18n/cli_surface.en.yml does not cover the same commands as EFFECTS: "
                f"missing={sorted(declared - set(effects))}, extra={sorted(set(effects) - declared)}"
            )
    rows = list(_SURFACE_HEAD[lang])
    for path in sorted(actual):
        network, local = effects[path]
        rows.append(f"| `cne {path}` | {network} | {local} |")
    return "\n".join([*rows, ""])


# --- derivatives -------------------------------------------------------------

DERIVATIVE_START = "<!-- derivative-capabilities:start -->"
DERIVATIVE_END = "<!-- derivative-capabilities:end -->"


_DERIVATIVE_WORDS = {
    "zh": {
        "header": "| 发布者 | 路由 | 期货起点 | 期权起点 | 生命周期参考 | 历史参考 | 状态 |",
        "unsupported": "不支持",
        "yes": "有",
        "no": "无",
        "note": "此表由 `scripts/dev/sync_docs.py` 从读取器注册表生成。起点是适配器路由边界，不证明源端或本湖连续完整；`experimental` 尚未通过真实载荷验收。INE 2018 年期货使用能源中心独立日文件，2019 年起随 SHF 路由合并发布；其独立起点不能套用 SHF 日期。",
    },
    "en": {
        "header": "| Publisher | Route | Futures since | Options since | Lifecycle reference | Historical reference | Status |",
        "unsupported": "not supported",
        "yes": "yes",
        "no": "no",
        "note": "Generated by `scripts/dev/sync_docs.py` from the reader registry. Start dates are adapter route boundaries; they do not prove that the source or this lake is continuous and complete. `experimental` routes have not yet passed acceptance on real payloads. INE futures for 2018 come from the energy center's own daily files; from 2019 they are published together with the SHF route, so the SHF dates do not apply to INE's separate start.",
    },
}


def render_derivative_table(lang: str = "zh") -> str:
    words = _DERIVATIVE_WORDS[lang]
    lines = [DERIVATIVE_START, "", words["header"], "|---|---|---|---|---|---|---|"]
    for row in capabilities():
        lines.append(
            f"| {row['exchange']} | {row['route']} | {row['futures_since']} | "
            f"{row['options_since'] or words['unsupported']} | "
            f"{words['yes'] if row['reference'] else words['no']} | "
            f"{words['yes'] if row['reference_history'] else words['no']} | {row['status']} |"
        )
    lines.extend(["", words["note"], "", DERIVATIVE_END])
    return "\n".join(lines)


# --- pypi-readme -------------------------------------------------------------

SITE_URL = "https://rootsunc.github.io/CNEquity/"
REPO_URL = "https://github.com/rootSunc/CNEquity"
RAW_URL = "https://raw.githubusercontent.com/rootSunc/CNEquity/main/"
_RELATIVE = re.compile(r'(\]\(|src=")(?!https?:|#|mailto:)([^)"\s]+)')


def _absolute(target: str) -> str:
    """Where a README-relative link points once the page is rendered on PyPI."""
    path, _, anchor = target.partition("#")
    suffix = f"#{anchor}" if anchor else ""
    for tree, site in (("docs/zh/", SITE_URL), ("docs/en/", SITE_URL + "en/")):
        if path.startswith(tree) and path.endswith(".md"):
            # mkdocs directory URLs: docs/zh/a/b.md -> a/b/, docs/zh/a/README.md -> a/
            page = path.removeprefix(tree).removesuffix(".md")
            page = page.removesuffix("README").removesuffix("index")
            return site + (page.rstrip("/") + "/" if page else "") + suffix
    if path.startswith("docs/assets/"):
        return RAW_URL + path
    return f"{REPO_URL}/blob/main/{path}{suffix}"


def render_pypi_readme() -> str:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    body = _RELATIVE.sub(lambda m: m.group(1) + _absolute(m.group(2)), readme)
    return (
        "<!-- Generated from README.md by scripts/dev/sync_docs.py; edit README.md. -->\n\n" + body
    )


# --- driver ------------------------------------------------------------------


def _schema() -> tuple[Path, str, list[str]]:
    updated, notes = sync(DOC_PATH.read_text(encoding="utf-8"))
    return DOC_PATH, updated, notes


def _schema_en() -> tuple[Path, str, list[str]]:
    updated, notes = sync(DOC_PATH_EN.read_text(encoding="utf-8"), lang="en")
    return DOC_PATH_EN, updated, notes


def _config() -> tuple[Path, str, list[str]]:
    source = REPO_ROOT / "src/cnequity/config/templates/cnequity.example.toml"
    return REPO_ROOT / "configs/cnequity.example.toml", source.read_text(encoding="utf-8"), []


def _derivatives(lang: str = "zh") -> tuple[Path, str, list[str]]:
    page = REPO_ROOT / f"docs/{lang}/datasets/sources.md"
    text = page.read_text(encoding="utf-8")
    if DERIVATIVE_START not in text or DERIVATIVE_END not in text:
        raise SystemExit(f"{page.relative_to(REPO_ROOT)} is missing capability markers")
    before, rest = text.split(DERIVATIVE_START, 1)
    _, after = rest.split(DERIVATIVE_END, 1)
    return page, before + render_derivative_table(lang) + after, []


def _cli_options(lang: str = "zh") -> tuple[Path, str, list[str]]:
    return REPO_ROOT / f"docs/{lang}/reference/cli-options.md", render_cli_options(lang), []


def _cli_surface(lang: str = "zh") -> tuple[Path, str, list[str]]:
    return REPO_ROOT / f"docs/{lang}/reference/cli-surface.md", render_cli_surface(lang), []


def _pypi_readme() -> tuple[Path, str, list[str]]:
    return REPO_ROOT / "README.pypi.md", render_pypi_readme(), []


TARGETS: dict[str, Callable[[], tuple[Path, str, list[str]]]] = {
    "schema": _schema,
    "schema-en": _schema_en,
    "config": _config,
    "derivatives": _derivatives,
    "derivatives-en": lambda: _derivatives("en"),
    "cli-options": _cli_options,
    "cli-options-en": lambda: _cli_options("en"),
    "cli-surface": _cli_surface,
    "cli-surface-en": lambda: _cli_surface("en"),
    "pypi-readme": _pypi_readme,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="fail on drift instead of writing")
    parser.add_argument(
        "targets", nargs="*", metavar="TARGET", help=f"subset ({', '.join(TARGETS)}); default all"
    )
    args = parser.parse_args()
    unknown = sorted(set(args.targets) - set(TARGETS))
    if unknown:
        parser.error(f"unknown target(s): {', '.join(unknown)}")
    stale = 0
    for name in args.targets or TARGETS:
        path, expected, notes = TARGETS[name]()
        current = path.read_bytes() if path.is_file() else None
        if current == expected.encode("utf-8"):
            print(f"{name}: in sync")
            continue
        if args.check:
            stale += 1
            print(f"{name}: OUT OF SYNC — {path.relative_to(REPO_ROOT)}")
            for note in notes:
                print(f"  - {note}")
            continue
        path.write_bytes(expected.encode("utf-8"))
        for note in notes:
            print(f"  - {note}")
        print(f"{name}: wrote {path.relative_to(REPO_ROOT)}")
    if stale:
        print("\nRun: python scripts/dev/sync_docs.py")
    return int(bool(stale))


if __name__ == "__main__":
    raise SystemExit(main())
