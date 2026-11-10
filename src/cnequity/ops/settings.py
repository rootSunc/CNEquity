"""Durable lake switches the operations page may write.

The page previews a diff, then writes only the whitelisted keys back into the
existing TOML. Comments, secrets, intervals and every other key stay. A
timestamped backup is kept beside the file, the same way ``cne config upgrade``
does. Saving settings does not start a fetch.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import secrets
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from cnequity.config import load_config
from cnequity.config.loader import push2_paused_by_env
from cnequity.config.upgrade import _table_span, tomllib
from cnequity.file_lock import LockUnavailable, exclusive_lock
from cnequity.i18n import N_, _, use_language
from cnequity.ops.catalog import OpsError, config_file
from cnequity.ops.scheduler import replace_config, state_dir
from cnequity.serve.display import en, zh

HOME_NOTE = N_(
    "These switches affect all future fetching, including scheduled daily updates and closing catch-up. Saving does not start fetching. Instrument lists, frequencies and exchanges stay in the config file."
)
NOTE_CHANGED = N_(
    "Writes to the current config after backing up the original file. Does not start fetching."
)
NOTE_UNCHANGED = N_("No changes to save.")
ACKNOWLEDGEMENT = N_(
    "I confirm saving these settings. Scheduled daily updates, closing catch-up and later commands will use the new values."
)
PUSH2_ENV_NOTE = N_(
    "The local environment variable CNE_PUSH2_PAUSED is on, so turning off the pause in config has no effect; push2 still sends no requests."
)
STRUCTURE = N_(
    "The config structure does not support changing these switches automatically; use standard TOML tables."
)

_PREVIEW_TTL = 600
_VALUE = (
    r"(?:true|false|\"(?:[^\"\\]|\\.)*\"|'(?:[^'\\]|\\.)*'|"
    r"[+-]?\d+(?:\.\d+)?|[A-Za-z_][\w-]*)"
)


@dataclass(frozen=True)
class Setting:
    id: str
    group: str
    table: str
    key: str
    kind: str  # bool, choice
    label: str
    help: str
    default: bool | str
    choices: tuple[tuple[str, str], ...] = ()
    scope: Callable[[Any], str] | None = None


def _listed(items, *, empty: str) -> str:
    names = [str(item) for item in items if str(item).strip()]
    if not names:
        return empty
    shown = _(", ").join(names[:8])
    if len(names) > 8:
        shown += _(" and others ({total} in all)").format(total=len(names))
    return shown


# The scope notes are computed from the config, so the settings page renders
# each once per language (see `SettingsService.home`).
def _minute_scope(config) -> str:
    freqs = _listed(config.minute_bars_frequencies, empty=_("no frequency set"))
    symbols = _listed(config.minute_bars_symbols, empty="")
    extra = _(", symbols {symbols}").format(symbols=symbols) if symbols else ""
    return _("The scope stays in the config file: {scope}, frequencies {freqs}{extra}.").format(
        scope=config.minute_bars_scope, freqs=freqs, extra=extra
    )


def _ticks_scope(config) -> str:
    symbols = _listed(config.trade_ticks_symbols, empty=_("no symbols listed"))
    return _(
        "The scope stays in the config file: {scope}, limit {limit}, symbols {symbols}."
    ).format(scope=config.trade_ticks_scope, limit=config.trade_ticks_max_symbols, symbols=symbols)


def _futures_scope(config) -> str:
    exchanges = _listed(config.futures_exchanges, empty=_("every supported route"))
    options = _("with options") if config.futures_options else _("without options")
    return _(
        "The scope stays in the config file: exchanges {exchanges}, {options}, DCE route {route}."
    ).format(exchanges=exchanges, options=options, route=config.futures_dce_route)


def _futures_minute_scope(config) -> str:
    products = _listed(config.futures_minute_products, empty=_("no products listed"))
    contracts = _listed(config.futures_minute_contracts, empty=_("no contracts listed"))
    return _(
        "The scope stays in the config file: products {products}, contracts {contracts}, "
        "limit {limit}."
    ).format(products=products, contracts=contracts, limit=config.futures_minute_max_contracts)


_INGEST = (
    ("all_a", N_("All A-shares")),
    ("all_a_sh_sz", N_("Excluding BSE")),
    ("all_instruments", N_("All codes (incl. ETF/LOF)")),
)

SETTINGS: tuple[Setting, ...] = (
    Setting(
        "push2_paused",
        "sources",
        "sources.eastmoney",
        "push2_paused",
        "bool",
        N_("Pause push2"),
        N_(
            "Stops push2, push2his and push2delay; no requests are sent. datacenter and other EastMoney hosts continue. This is separate from the EastMoney master switch below."
        ),
        False,
    ),
    Setting(
        "tdx",
        "sources",
        "tdx_protocol",
        "enabled",
        "bool",
        N_("TDX"),
        N_("Primary daily bar source. When off, daily bars are no longer fetched from TDX."),
        True,
    ),
    Setting(
        "eastmoney",
        "sources",
        "sources.eastmoney",
        "enabled",
        "bool",
        N_("EastMoney"),
        N_("Announcements, financials and fund flows. When off, EastMoney is no longer requested."),
        True,
    ),
    Setting(
        "sina_bars",
        "sources",
        "sources.sina_bars",
        "enabled",
        "bool",
        N_("Sina daily bars"),
        N_("Daily bar fallback. When off, Sina is no longer used to fill daily bars."),
        True,
    ),
    Setting(
        "baostock",
        "sources",
        "sources.baostock",
        "enabled",
        "bool",
        "Baostock",
        N_("Historical quotes fallback."),
        True,
    ),
    Setting(
        "bse",
        "sources",
        "sources.bse",
        "enabled",
        "bool",
        N_("BSE website"),
        N_("Same-day BSE quotes. Not a historical source."),
        True,
    ),
    Setting(
        "ths",
        "sources",
        "sources.ths",
        "enabled",
        "bool",
        N_("THS pages"),
        N_("Sector quotes. When off, THS public pages are no longer requested."),
        True,
    ),
    Setting(
        "cninfo",
        "sources",
        "sources.cninfo",
        "enabled",
        "bool",
        N_("CNINFO"),
        N_("Announcements and regulatory filings. When off, CNINFO is no longer requested."),
        True,
    ),
    Setting(
        "tushare",
        "licensed",
        "sources.tushare",
        "enabled",
        "bool",
        N_("Tushare"),
        N_(
            "Off by default. Needs your own token; the token stays in the config file. When off, no Tushare request is sent, and its datasets are not listed as stale."
        ),
        False,
    ),
    Setting(
        "ths_official",
        "licensed",
        "sources.ths_official",
        "enabled",
        "bool",
        N_("THS official API"),
        N_(
            "Off by default. A licensed API; the key stays in the config file. When off, no request is sent."
        ),
        False,
    ),
    Setting(
        "minute_bars",
        "capture",
        "minute_bars",
        "enabled",
        "bool",
        N_("Minute bars"),
        N_("Off by default. When on, the daily update keeps fetching for the current scope."),
        False,
        scope=_minute_scope,
    ),
    Setting(
        "trade_ticks",
        "capture",
        "trade_ticks",
        "enabled",
        "bool",
        N_("Trade ticks"),
        N_("Off by default. When on, the daily update keeps fetching for the current scope."),
        False,
        scope=_ticks_scope,
    ),
    Setting(
        "futures",
        "capture",
        "futures",
        "enabled",
        "bool",
        N_("Futures and options"),
        N_("Off by default. When on, the daily update fetches futures and options."),
        False,
        scope=_futures_scope,
    ),
    Setting(
        "futures_minute",
        "capture",
        "futures",
        "minute_enabled",
        "bool",
        N_("Futures minute bars"),
        N_("Off by default. When on, the daily update fetches futures minute bars."),
        False,
        scope=_futures_minute_scope,
    ),
    Setting(
        "ingest",
        "universe",
        "universe",
        "ingest",
        "choice",
        N_("Securities in the daily update"),
        N_(
            "Choose which securities the daily update fetches. ST, suspended and delisted names are kept."
        ),
        "all_a",
        choices=_INGEST,
    ),
    Setting(
        "ingest_listed_funds",
        "universe",
        "universe",
        "ingest_listed_funds",
        "bool",
        N_("Also fetch listed ETF/LOF daily bars"),
        N_(
            "Fetch fund quotes from the security master independently of research eligibility; unknown listing dates are reported separately. Off by default."
        ),
        False,
    ),
    Setting(
        "ingest_eligible_etfs",
        "universe",
        "universe",
        "ingest_eligible_etfs",
        "bool",
        N_("Also fetch eligible ETF daily bars"),
        N_(
            "Also fetch daily bars for ETFs that are recently replayable and qualify per the exchange directory. Off by default."
        ),
        False,
    ),
)

_BY_ID = {spec.id: spec for spec in SETTINGS}
GROUPS = (
    ("sources", N_("Data sources")),
    ("licensed", N_("Paid sources")),
    ("capture", N_("Optional collection")),
    ("universe", N_("Daily update coverage")),
)


def _read(path: Path) -> str:
    try:
        return path.read_bytes().decode("utf-8")
    except UnicodeError as exc:
        raise OpsError(_("The config file is not UTF-8 and cannot be edited on the page.")) from exc


def _lookup(raw: dict, spec: Setting):
    node: Any = raw
    for part in spec.table.split("."):
        if not isinstance(node, dict) or part not in node:
            return spec.default
        node = node[part]
    if isinstance(node, dict):
        if spec.key not in node:
            return spec.default
        return node[spec.key]
    # ``eastmoney = false`` is the enabled flag, not a table.
    if isinstance(node, bool) and spec.key == "enabled":
        return node
    return spec.default


def read_values(text: str) -> dict[str, Any]:
    from cnequity.config.layers import effective_raw

    try:
        # What the lake runs with: an `extends = "builtin"` config leaves most
        # values to the defaults, and the page must show those, not its own.
        raw = effective_raw(tomllib.loads(text))
    except ValueError as exc:
        raise OpsError(_("The config file cannot be parsed; fix the TOML first.")) from exc
    found: dict[str, Any] = {}
    for spec in SETTINGS:
        value = _lookup(raw, spec)
        if spec.kind == "bool" and not isinstance(value, bool):
            raise OpsError(
                _("The config of {value} is not true or false.").format(value=_(spec.label))
            )
        if spec.kind == "choice" and value not in {item[0] for item in spec.choices}:
            raise OpsError(
                _("The current value of {value} is not among the choices.").format(
                    value=_(spec.label)
                )
            )
        found[spec.id] = value
    return found


def normalize(raw: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise OpsError(_("The settings are incomplete."))
    unknown = set(raw) - set(_BY_ID)
    if unknown:
        raise OpsError(_("Some settings are not accepted by the page."))
    missing = [_(spec.label) for spec in SETTINGS if spec.id not in raw]
    if missing:
        raise OpsError(_("The settings are incomplete."))
    out: dict[str, Any] = {}
    for spec in SETTINGS:
        value = raw[spec.id]
        if spec.kind == "bool":
            if not isinstance(value, bool):
                raise OpsError(_("{value} must be a switch.").format(value=_(spec.label)))
        elif not isinstance(value, str) or value not in {item[0] for item in spec.choices}:
            raise OpsError(_("The value of {value} is invalid.").format(value=_(spec.label)))
        out[spec.id] = value
    return out


def _display(spec: Setting, value: Any) -> str:
    if spec.kind == "bool":
        return N_("On") if value else N_("Off")
    for choice, label in spec.choices:
        if choice == value:
            return label
    return str(value)


def _literal(spec: Setting, value: Any) -> str:
    if spec.kind == "bool":
        return "true" if value else "false"
    return json.dumps(value, ensure_ascii=False)


def _assignment(key: str) -> re.Pattern[str]:
    ident = rf'(?:{re.escape(key)}|"{re.escape(key)}"|\'{re.escape(key)}\')'
    return re.compile(rf"^(\s*{ident}\s*=\s*)({_VALUE})(\s*(?:#.*)?)(\r?\n)?$")


def _assign_expected(tree: dict, spec: Setting, value: Any) -> None:
    node = tree
    parts = spec.table.split(".")
    for part in parts[:-1]:
        current = node.get(part)
        if current is None:
            node[part] = {}
            current = node[part]
        if not isinstance(current, dict):
            raise OpsError(_(STRUCTURE))
        node = current
    leaf_name = parts[-1]
    leaf = node.get(leaf_name)
    if leaf is None:
        node[leaf_name] = {}
        leaf = node[leaf_name]
    if not isinstance(leaf, dict):
        raise OpsError(_(STRUCTURE))
    leaf[spec.key] = value


def _set_assignment(text: str, table: str, key: str, literal: str) -> str:
    lines = text.splitlines(keepends=True)
    span = _table_span(lines, table)
    pattern = _assignment(key)
    if span is None:
        newline = "\r\n" if "\r\n" in text else "\n"
        return (
            text.rstrip("\r\n") + f"{newline}{newline}[{table}]{newline}{key} = {literal}{newline}"
        )
    start, end = span
    for index in range(start + 1, end):
        match = pattern.match(lines[index])
        if match is None:
            continue
        ending = match.group(4)
        if ending is None:
            ending = "" if index == len(lines) - 1 else "\n"
        lines[index] = f"{match.group(1)}{literal}{match.group(3)}{ending}"
        return "".join(lines)
    newline = "\r\n" if lines[start].endswith("\r\n") else "\n"
    lines.insert(start + 1, f"{key} = {literal}{newline}")
    return "".join(lines)


def revise(text: str, desired: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    """Return edited TOML and the public change list. The file is not touched."""
    current = read_values(text)
    changes = [
        {
            "id": spec.id,
            "label": spec.label,
            "before": current[spec.id],
            "after": desired[spec.id],
            "before_label": _display(spec, current[spec.id]),
            "after_label": _display(spec, desired[spec.id]),
        }
        for spec in SETTINGS
        if current[spec.id] != desired[spec.id]
    ]
    if not changes:
        return text, []
    try:
        raw = tomllib.loads(text)
    except ValueError as exc:
        raise OpsError(_("The config file cannot be parsed; fix the TOML first.")) from exc
    expected = copy.deepcopy(raw)
    for spec in SETTINGS:
        if current[spec.id] != desired[spec.id]:
            _assign_expected(expected, spec, desired[spec.id])
    edited = text
    for spec in SETTINGS:
        if current[spec.id] == desired[spec.id]:
            continue
        edited = _set_assignment(edited, spec.table, spec.key, _literal(spec, desired[spec.id]))
    try:
        parsed = tomllib.loads(edited)
    except ValueError as exc:
        raise OpsError(_(STRUCTURE)) from exc
    if parsed != expected:
        raise OpsError(_(STRUCTURE))
    return edited, changes


def _backup(path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup = path.with_name(f"{path.name}.bak-{stamp}")
    number = 2
    while backup.exists():
        backup = path.with_name(f"{path.name}.bak-{stamp}-{number}")
        number += 1
    shutil.copy2(path, backup)
    return backup


def _public(changes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    keys = ("id", "label", "before", "after", "before_label", "after_label")
    shown = []
    for change in changes:
        row = {key: change[key] for key in keys}
        for key in ("label", "before_label", "after_label"):
            row[key], row[f"{key}_en"] = zh(change[key]), en(change[key])
        shown.append(row)
    return shown


def _scope_in(language: str, spec: Setting, config) -> str | None:
    if spec.scope is None:
        return None
    with use_language(language):
        return spec.scope(config)


class SettingsService:
    def __init__(self, config):
        path = config_file(config)
        if path is None:
            raise OpsError(_("Generate the config before changing fetch settings."))
        self.config = config
        self.path = path
        self.previews: dict[str, dict] = {}

    def _fresh(self):
        config = load_config(self.path)
        if Path(config.meta_root) != Path(self.config.meta_root):
            raise OpsError(
                _(
                    "The configured data directory has changed; restart serve before changing fetch settings."
                )
            )
        return config

    def home(self) -> dict:
        config = self._fresh()
        values = read_values(_read(self.path))
        sections = []
        for group_id, title in GROUPS:
            items = []
            for spec in SETTINGS:
                if spec.group != group_id:
                    continue
                item = {
                    "id": spec.id,
                    "kind": spec.kind,
                    "label": zh(spec.label),
                    "label_en": en(spec.label),
                    "help": zh(spec.help),
                    "help_en": en(spec.help),
                    "value": values[spec.id],
                    "scope": _scope_in("zh_CN", spec, config),
                    "scope_en": _scope_in("en", spec, config),
                }
                if spec.choices:
                    item["choices"] = [
                        {"value": value, "label": zh(label), "label_en": en(label)}
                        for value, label in spec.choices
                    ]
                items.append(item)
            sections.append(
                {"id": group_id, "title": zh(title), "title_en": en(title), "settings": items}
            )
        env = push2_paused_by_env()
        return {
            "note": zh(HOME_NOTE),
            "note_en": en(HOME_NOTE),
            "push2_env_paused": env,
            "push2_env_note": zh(PUSH2_ENV_NOTE) if env else None,
            "push2_env_note_en": en(PUSH2_ENV_NOTE) if env else None,
            "sections": sections,
        }

    def preview(self, raw: dict[str, Any] | None) -> dict:
        desired = normalize(raw)
        file_bytes = self.path.read_bytes()
        try:
            text = file_bytes.decode("utf-8")
        except UnicodeError as exc:
            raise OpsError(
                _("The config file is not UTF-8 and cannot be edited on the page.")
            ) from exc
        edited, changes = revise(text, desired)
        env = push2_paused_by_env()
        payload = {
            "token": None,
            "changes": _public(changes),
            "note": zh(NOTE_CHANGED if changes else NOTE_UNCHANGED),
            "note_en": en(NOTE_CHANGED if changes else NOTE_UNCHANGED),
            "acknowledgement": zh(ACKNOWLEDGEMENT) if changes else None,
            "acknowledgement_en": en(ACKNOWLEDGEMENT) if changes else None,
            "push2_env_note": zh(PUSH2_ENV_NOTE) if env else None,
            "push2_env_note_en": en(PUSH2_ENV_NOTE) if env else None,
        }
        if not changes:
            return payload
        now = time.monotonic()
        self.previews = {
            key: value for key, value in self.previews.items() if value["expires"] > now
        }
        if len(self.previews) >= 16:
            self.previews.pop(next(iter(self.previews)))
        token = secrets.token_urlsafe(32)
        self.previews[token] = {
            "digest": hashlib.sha256(file_bytes).hexdigest(),
            "text": edited,
            "desired": desired,
            "public": payload["changes"],
            "expires": now + _PREVIEW_TTL,
        }
        payload["token"] = token
        return payload

    def apply(self, token: str, *, acknowledged: bool) -> dict:
        if not acknowledged:
            raise OpsError(_("First confirm these settings will be used for later fetches."))
        try:
            with exclusive_lock(state_dir(self.config) / "control.lock", blocking=False):
                plan = self.previews.get(token)
                if plan is None or plan["expires"] <= time.monotonic():
                    self.previews.pop(token, None)
                    raise OpsError(_("The preview has expired; preview again."))
                if hashlib.sha256(self.path.read_bytes()).hexdigest() != plan["digest"]:
                    self.previews.pop(token, None)
                    raise OpsError(_("The config file has changed; preview again."))
                backup = _backup(self.path)
                try:
                    replace_config(self.path, plan["text"])
                    written = read_values(_read(self.path))
                    if any(written[spec.id] != plan["desired"][spec.id] for spec in SETTINGS):
                        raise OpsError(_("The written config does not match the preview."))
                    load_config(self.path)
                except Exception as exc:
                    shutil.copy2(backup, self.path)
                    if isinstance(exc, OpsError):
                        raise
                    raise OpsError(
                        _("Writing the config failed; the original file was restored.")
                    ) from exc
                self.previews.pop(token, None)
                return {
                    "saved": True,
                    "backup_name": backup.name,
                    "changes": plan["public"],
                }
        except LockUnavailable as exc:
            raise OpsError(_("The config is being changed; try again shortly.")) from exc
