"""Regenerate every documentation page that mirrors code, or check they still do.

One entry point for what used to be five scripts. Each target renders the text
its file should hold from the code it mirrors; ``--check`` fails instead of
writing, which is what CI runs.

    python scripts/dev/sync_docs.py                 # rewrite every target
    python scripts/dev/sync_docs.py --check         # CI: fail on any drift
    python scripts/dev/sync_docs.py catalog         # one target

Dataset and derivatives catalogs follow the registries. Field tables keep
their hand-written explanations while syncing column names and types.
The side-effect inventory must cover every registered CLI command. Options
and defaults are available through Click help.

The driver also mirrors the bundled config template and builds the PyPI
description from the repository README.
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
sys.path.insert(0, str(Path(__file__).resolve().parent))

from cnequity.adapters.futures_exchange.registry import capabilities  # noqa: E402
from cnequity.cli._root import MOVED  # noqa: E402
from cnequity.cli.main import cli  # noqa: E402
from cnequity.domain.datasets import (  # noqa: E402
    DATASETS,
    TIER_LABELS,
    datasets_by_tier,
    history_mode_for,
)
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
    "zh": {"header": "| 列 | 类型 | 说明 |", "anchor": "### 导出 Parquet"},
    "en": {"header": "| Column | Type | Description |", "anchor": "### Export Parquet"},
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


SCHEMA_INDEX_START = "<!-- schema-tier-index:start -->"
SCHEMA_INDEX_END = "<!-- schema-tier-index:end -->"


def _heading_anchor(heading: str) -> str:
    if match := re.search(r"\{#([^}]+)\}", heading):
        return match.group(1)
    return "-".join(re.findall(r"[a-z0-9_]+", heading.lower()))


def _render_schema_tier_index(anchors: dict[str, str], lang: str = "zh") -> str:
    locale = "zh_CN" if lang == "zh" else "en"
    header = "| 分层 | 数据集 |" if lang == "zh" else "| Tier | Datasets |"
    rows = [SCHEMA_INDEX_START, "", header, "|---|---|"]
    for tier, names in datasets_by_tier().items():
        if not names:
            continue
        label = translate_to(locale, TIER_LABELS[tier])
        links = " · ".join(f"[`{name}`](#{anchors.get(name, name)})" for name in names)
        rows.append(f"| **{tier} {label}** | {links} |")
    rows.extend(["", SCHEMA_INDEX_END])
    return "\n".join(rows)


def sync(text: str, lang: str = "zh") -> tuple[str, list[str]]:
    labels = _SCHEMA_LABELS[lang]
    schemas = _schemas()
    datasets = list(schemas)
    lines = text.split("\n")
    notes: list[str] = []
    documented: set[str] = set()
    anchors: dict[str, str] = {}

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
        anchor = _heading_anchor(heading)
        for name in names:
            anchors[name] = anchor
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
        anchor_idx = next(
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
            anchors[name] = name
        lines[anchor_idx:anchor_idx] = block

    result = "\n".join(lines)
    if SCHEMA_INDEX_START in result and SCHEMA_INDEX_END in result:
        before, rest = result.split(SCHEMA_INDEX_START, 1)
        _, after = rest.split(SCHEMA_INDEX_END, 1)
        result = before + _render_schema_tier_index(anchors, lang) + after
    return result, notes


def _load_catalog(name: str) -> dict:
    return yaml.safe_load((I18N / name).read_text(encoding="utf-8")) or {}


# --- cli-surface -------------------------------------------------------------

# Each leaf has one concise user-visible side-effect classification. Adding a
# command fails --check until someone reviews its request and write boundary.
EFFECTS: dict[str, tuple[str, str]] = {
    "audit": ("条件", "读取湖、输出审计；启用外部对照时访问源"),
    "backfill": ("执行时", "补历史并写暂存与发布数据；--plan 仅读配置和状态"),
    "check": ("无", "读取状态、新鲜度、审计结果与统计；统计过期时重算，--full 重跑全湖审计"),
    "config create": ("无", "写一个继承内置默认值的个人配置；已存在时需 --force"),
    "config diff": ("无", "只读：报告配置相对内置默认值缺少的步骤和设置"),
    "config path": ("无", "只读：显示生效的配置、来源和指向的湖"),
    "config show": ("无", "只读：打印生效配置，--defaults 列出全部默认值"),
    "config upgrade": (
        "无",
        "先备份再加入新版本的调度步骤；--minimize 改写为继承默认值；--dry-run 不写文件",
    ),
    "config validate": ("无", "只读：校验配置"),
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
    "repair status-traded": (
        "无",
        "默认只读预览；--apply 用已提交成交日线补缺失状态键，ST 保持未知，发布新版本并保留旧版本",
    ),
    "import shfe-archive": (
        "无",
        "默认只读预览；--apply 校验本地上期所年度包后写入缺字段的日线行，不覆盖已有主键，保存原包证据",
    ),
    "upgrade": (
        "条件",
        "预览检查扩展和磁盘，可能下载缺少的扩展；--apply 默认先备份湖的记录，再运行所欠迁移并清理已被取代的旧文件。迁移可能取数",
    ),
    "migrate": (
        "条件",
        "默认预览待执行的迁移；--only 指定迁移；--apply 执行并记录结果。所选迁移可访问来源或清理被取代的数据文件",
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
    "run clean": (
        "无",
        "先预览过期文件；交互终端分两次确认历史版本和已合并且不在使用中的 staging；正在使用的数据和 staging 不自动删除；定时和非交互只预览；显式 --reconcile-runs 可修改运行状态",
    ),
    "run compact": ("无", "将已结束、未发布的暂存数据发布到湖"),
    "run daily": ("执行时", "增量取数并写湖；默认含全部调度组与事件流"),
    "run events": ("执行时", "事件取数并写湖"),
    "run maintain": (
        "执行时",
        "日更后的维护：重试旧欠账、检查审计与新鲜度、探测源、应用湖目录保留策略、轮换元数据备份，并预览常规清理",
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
    "snapshot delete": ("无", "删除本地快照，并解除它带来的失效保留引用"),
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
    "storage explain": ("无", "读取版本保留依据"),
    "storage hold": ("无", "增加对象保护；自动清理不再删除它"),
    "storage release": ("无", "解除指定版本的手动保留与失效引用；案例依赖和仍有效的引用保留"),
    "storage refresh": ("无", "重新扫描引用目录，为其中提到的版本增加保留；不会解除保留"),
    "storage import": ("无", "校验并登记引用清单；只增加保护"),
    "storage inspect": ("无", "读取版本与保留原因"),
    "storage plan": ("无", "预览自动清理现在会删除哪些代；不删除数据"),
    "storage raw-external": ("无", "登记外置原文目录；本地保留来源记录，不移动或删除文件"),
    "storage catalog-retention": (
        "无",
        "预演只读；--apply 让湖目录过期旧快照，并删除超过宽限期的文件",
    ),
    "storage export": (
        "无",
        "只读湖；把湖目录里已发布的数据集导出成普通 Parquet，只写 --out 指定的目录",
    ),
    "storage verify-catalog": (
        "无",
        "只读；核对发布进湖目录的每个修订，其快照里的行与发布时记录的摘要是否一致",
    ),
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
        "由 `scripts/dev/sync_docs.py` 从 Click 命令注册表核对。‘执行时’指运行命令主体；`--help` 不执行。参数以 `cne <命令> --help` 为准，常见操作见[更新与排障](../operations/runbook.md)。",
        "",
        "| 命令 | 第三方取数 | 本地效果 |",
        "|---|---|---|",
    ],
    "en": [
        "# Network and write inventory",
        "",
        "Checked against the Click command registry by `scripts/dev/sync_docs.py`. \u201cWhen run\u201d means when the command body executes; `--help` executes nothing. Run `cne <command> --help` for options. See [Update and troubleshoot](../operations/runbook.md) for common tasks.",
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
    rename_header = {
        "zh": ["## 改名对照", "", "| 旧命令 | 当前命令 |"],
        "en": ["## Renamed commands", "", "| Previous command | Current command |"],
    }
    rows.extend(["", *rename_header[lang], "|---|---|"])
    rows.extend(f"| `cne {old}` | `{new}` |" for old, new in sorted(MOVED.items()))
    return "\n".join([*rows, ""])


# --- dataset catalog ---------------------------------------------------------

CATALOG_START = "<!-- dataset-catalog:start -->"
CATALOG_END = "<!-- dataset-catalog:end -->"

_CATALOG_HEADER = {
    "zh": "| 数据集 | 支持 | 主来源 | 历史类型 | 回填来源 | 目录请求边界 |",
    "en": "| Dataset | Support | Primary source | History mode | Backfill source | Registry request limit |",
}
_SESSION_UNIT = {"zh": "个交易日", "en": "trading sessions"}


def render_dataset_catalog(lang: str = "zh") -> str:
    locale = "zh_CN" if lang == "zh" else "en"
    lines = [CATALOG_START, ""]
    for tier, names in datasets_by_tier().items():
        if not names:
            continue
        label = translate_to(locale, TIER_LABELS[tier])
        lines.extend([f"## {tier} {label}", "", _CATALOG_HEADER[lang], "|---|---|---|---|---|---|"])
        for name in names:
            spec = DATASETS[name]
            source = spec.backfill_source
            if spec.keyed_backfill_source:
                source = f"{spec.keyed_backfill_source} (API key)"
            limit = str(spec.history_floor_date or spec.keyed_backfill_floor or "—")
            if spec.history_horizon_days is not None:
                limit = f"{spec.history_horizon_days} {_SESSION_UNIT[lang]}"
            lines.append(
                f"| {name} | {spec.support} | {spec.primary_source} | "
                f"{history_mode_for(spec)} | {source or '—'} | {limit} |"
            )
        lines.append("")
    return "\n".join([*lines, CATALOG_END])


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


def _catalog(lang: str = "zh") -> tuple[Path, str, list[str]]:
    page = REPO_ROOT / f"docs/{lang}/datasets/catalog.md"
    text = page.read_text(encoding="utf-8")
    for start, end, render in (
        (CATALOG_START, CATALOG_END, render_dataset_catalog),
        (DERIVATIVE_START, DERIVATIVE_END, render_derivative_table),
    ):
        before, rest = text.split(start, 1)
        _, after = rest.split(end, 1)
        text = before + render(lang) + after
    return page, text, []


def _cli_surface(lang: str = "zh") -> tuple[Path, str, list[str]]:
    return REPO_ROOT / f"docs/{lang}/reference/cli-surface.md", render_cli_surface(lang), []


def _pypi_readme() -> tuple[Path, str, list[str]]:
    return REPO_ROOT / "README.pypi.md", render_pypi_readme(), []


TARGETS: dict[str, Callable[[], tuple[Path, str, list[str]]]] = {
    "schema": _schema,
    "schema-en": _schema_en,
    "config": _config,
    "catalog": _catalog,
    "catalog-en": lambda: _catalog("en"),
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
