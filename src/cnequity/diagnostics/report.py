"""Assemble the `cne doctor` report from environment, packages, and config.

Scope is deliberately narrow: `cne config validate` already checks whether a
config file is well-formed, offline and without touching the machine. This
answers the question it cannot — whether that config works in *this* environment.
"""

from __future__ import annotations

import platform
import sys
from dataclasses import dataclass, field
from enum import Enum
from importlib import metadata
from pathlib import Path

from cnequity.diagnostics.packages import (
    PackageStatus,
    probe_packages,
)
from cnequity.i18n import N_, Message


class Severity(str, Enum):
    OK = "ok"
    WARN = "warn"
    ERROR = "error"


@dataclass(frozen=True)
class Finding:
    severity: Severity
    title: str
    detail: str = ""
    fix: str = ""


@dataclass
class Report:
    environment: dict[str, str] = field(default_factory=dict)
    packages: list[PackageStatus] = field(default_factory=list)
    findings: list[Finding] = field(default_factory=list)

    @property
    def errors(self) -> list[Finding]:
        return [f for f in self.findings if f.severity is Severity.ERROR]

    @property
    def warnings(self) -> list[Finding]:
        return [f for f in self.findings if f.severity is Severity.WARN]

    @property
    def ok(self) -> bool:
        return not self.errors


def _environment() -> dict[str, str]:
    try:
        version = metadata.version("cnequity")
    except metadata.PackageNotFoundError:  # pragma: no cover - source tree without install
        version = "unknown (not installed)"
    pkg_root = Path(__file__).resolve().parent.parent
    return {
        "cnequity": version,
        "python": f"{sys.version.split()[0]} ({platform.machine()})",
        "platform": f"{platform.system()} {platform.release()}",
        "package": str(pkg_root),
        "executable": sys.executable,
    }


def _check_packages(statuses: list[PackageStatus], findings: list[Finding]) -> None:
    """A declared dependency that will not import means the install is damaged."""
    missing = [s for s in statuses if not s.importable]
    if not missing:
        return
    findings.append(
        Finding(
            severity=Severity.ERROR,
            title=Message(N_("%d required dependencies cannot be imported"), len(missing)),
            # One "  module — purpose" line per package, then the explanation;
            # each purpose is its own catalog message.
            detail=Message(
                "\n".join("  %s — %s" for _status in missing) + "%s",
                *(arg for s in missing for arg in (s.package.module, Message(s.package.purpose))),
                Message(
                    N_(
                        "\n  These are all hard dependencies; a missing one means the environment "
                        "is incomplete (usually left over from an uninstall or an interrupted "
                        "install).\n  Adapters import them lazily inside functions, so they fail "
                        "only when data is actually fetched, and the daily update silently thins out."
                    )
                ),
            ),
            fix="pip install --force-reinstall cnequity",
        )
    )


def _data_root_writable(data_root: Path) -> bool:
    """True when a file can be created and removed under *data_root*."""
    probe = data_root / ".cne_write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except OSError:
        return False


#: Inline key → (environment variable, file key) that keep it out of the config.
_CREDENTIAL_HOMES = {
    "sources.tushare.token": ("TUSHARE_TOKEN", "token_file"),
    "sources.ths_official.api_key": ("HITHINK_FINANCE_API_KEY", "api_key_file"),
}


def _check_credentials(config, findings: list[Finding]) -> None:
    """A credential written into the config file travels with every copy of it."""
    inline = tuple(getattr(config, "inline_credentials", ()) or ())
    for key in inline:
        env, file_key = _CREDENTIAL_HOMES.get(key, ("", ""))
        table = key.rsplit(".", 1)[0]
        findings.append(
            Finding(
                severity=Severity.WARN,
                title=Message(N_("A credential is written in the config file: %s"), key),
                detail=N_(
                    "The config file gets copied, backed up and pasted into bug reports. "
                    "An environment variable does not reach launchd/cron jobs, so a scheduled "
                    "lake should read the credential from a file only you can read."
                ),
                fix=Message(
                    N_("Move it to a file and set [%s].%s = <path> (chmod 600), or export %s"),
                    table,
                    file_key,
                    env,
                ),
            )
        )
    path = getattr(config, "config_path", None)
    if not inline or path is None or sys.platform == "win32":
        return
    try:
        mode = Path(path).stat().st_mode
    except OSError:
        return
    if mode & 0o077:
        findings.append(
            Finding(
                severity=Severity.WARN,
                title=Message(
                    N_("The config file holding a credential is readable by others: %s"), path
                ),
                fix=f"chmod 600 {path}",
            )
        )


def _check_migrations(config, findings: list[Finding]) -> None:
    """One-off fixes for rows an older version wrote, not yet run on this lake."""
    from cnequity.storage.migrations import owed

    meta_root = getattr(config, "meta_root", None)
    if meta_root is None or not Path(meta_root).is_dir():
        return
    pending = owed(config)
    if pending:
        findings.append(
            Finding(
                severity=Severity.WARN,
                title=Message(
                    N_("%s lake migration(s) not yet checked on this lake"), len(pending)
                ),
                detail=N_(
                    "Fixes for rows that an older version wrote. `cne migrate` previews what each "
                    "would change; `--apply` runs them and records the result."
                ),
                fix="cne migrate",
            )
        )


def _check_tushare(config, findings: list[Finding]) -> None:
    """What the optional Tushare account may call, from the cached probe only.

    Doctor stays offline: the probe itself runs with the audit's peer checks.
    """
    from cnequity.adapters.tushare.client import cached_permissions, tushare_enabled

    if not tushare_enabled(config):
        return
    statuses = cached_permissions(config)
    if not statuses:
        findings.append(
            Finding(
                severity=Severity.OK,
                title=N_("Tushare is enabled; its endpoint permissions are not probed yet"),
                detail=N_("The next `cne audit` probes them and caches the result for a week."),
            )
        )
        return
    denied = sorted(api for api, status in statuses.items() if status != "ok")
    findings.append(
        Finding(
            severity=Severity.WARN if denied else Severity.OK,
            title=Message(
                N_("Tushare permissions: %s of %s probed endpoints usable"),
                len(statuses) - len(denied),
                len(statuses),
            ),
            detail=Message(N_("Not usable: %s"), ", ".join(denied)) if denied else "",
            fix=N_(
                "Checks that need these endpoints are skipped; your account decides which apply."
            )
            if denied
            else "",
        )
    )


def _check_data_root(data_root: Path, findings: list[Finding]) -> None:
    if not data_root.is_absolute():
        findings.append(
            Finding(
                severity=Severity.ERROR,
                title=Message(N_("data.root is a relative path: %s"), data_root),
                detail=N_(
                    "A relative path resolves against the process CWD. A scheduler's "
                    "(launchd/cron) CWD differs from the one you run by hand, so it silently "
                    "builds a second, empty lake elsewhere, which may go unnoticed for days."
                ),
                fix=Message(
                    N_("Change data.root in the config to an absolute path, e.g. %s"),
                    data_root.resolve(),
                ),
            )
        )
        return

    if not data_root.exists():
        findings.append(
            Finding(
                severity=Severity.WARN,
                title=Message(N_("data.root does not exist yet: %s"), data_root),
                detail=N_("The first `cne init` creates it."),
                fix=N_("cne init --config <config path>"),
            )
        )
        return

    # Probe a real write: ``os.access(..., W_OK)`` is unreliable on Windows ACLs,
    # and ``chmod`` is the wrong remediation there.
    if not _data_root_writable(data_root):
        fix = (
            Message(
                N_('In File Explorer, grant the current user the "Modify" permission on: %s'),
                data_root,
            )
            if sys.platform == "win32"
            else f"chmod u+w {data_root}"
        )
        findings.append(
            Finding(
                severity=Severity.ERROR,
                title=Message(N_("data.root is not writable: %s"), data_root),
                detail=N_("Orchestration will fail on its first write."),
                fix=fix,
            )
        )


def build_report(config=None, config_path: Path | None = None) -> Report:
    """Collect environment, package probes, and (when available) config checks."""
    statuses = probe_packages()
    findings: list[Finding] = []

    _check_packages(statuses, findings)

    if config is None:
        findings.append(
            Finding(
                severity=Severity.WARN,
                title=N_("No config loaded — only the environment was checked"),
                detail=N_("Without a config, data.root cannot be checked."),
                fix="cne config create",
            )
        )
    else:
        _check_data_root(Path(config.data_root), findings)
        _check_credentials(config, findings)
        _check_migrations(config, findings)
        _check_tushare(config, findings)

    env = _environment()
    if config_path is not None:
        env["config"] = str(config_path)

    return Report(environment=env, packages=statuses, findings=findings)
