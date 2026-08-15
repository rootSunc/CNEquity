"""Assemble the `cml doctor` report from environment, packages, and config.

Scope is deliberately narrow: `cml config validate` already checks whether a
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

from cn_market_lake.diagnostics.packages import (
    PackageStatus,
    probe_packages,
)


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
        version = metadata.version("cn-market-lake")
    except metadata.PackageNotFoundError:  # pragma: no cover - source tree without install
        version = "unknown (not installed)"
    pkg_root = Path(__file__).resolve().parent.parent
    return {
        "cn-market-lake": version,
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
            title=f"{len(missing)} 个必需依赖无法导入",
            detail="\n".join(f"  {s.package.module} — {s.package.purpose}" for s in missing)
            + "\n  这些都是硬依赖，缺失说明环境不完整（多为卸载残留或安装中断）。"
            + "\n  适配器在函数内惰性 import，所以只在真正取数时才失败，日更会静默变薄。",
            fix="pip install --force-reinstall cn-market-lake",
        )
    )


def _data_root_writable(data_root: Path) -> bool:
    """True when a file can be created and removed under *data_root*."""
    probe = data_root / ".cml_write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
        return True
    except OSError:
        return False


def _check_data_root(data_root: Path, findings: list[Finding]) -> None:
    if not data_root.is_absolute():
        findings.append(
            Finding(
                severity=Severity.ERROR,
                title=f"data.root 是相对路径: {data_root}",
                detail=(
                    "相对路径按进程 CWD 解析。调度器（launchd/cron）的 CWD 与你手跑时不同，"
                    "会在别处静默建出第二个空湖，可能几天后才发现。"
                ),
                fix=f"把配置里的 data.root 改成绝对路径，例如 {data_root.resolve()}",
            )
        )
        return

    if not data_root.exists():
        findings.append(
            Finding(
                severity=Severity.WARN,
                title=f"data.root 尚不存在: {data_root}",
                detail="首次 `cml init` 会创建它。",
                fix="cml init --config <配置路径>",
            )
        )
        return

    # Probe a real write: ``os.access(..., W_OK)`` is unreliable on Windows ACLs,
    # and ``chmod`` is the wrong remediation there.
    if not _data_root_writable(data_root):
        fix = (
            f"在资源管理器中为当前用户授予「修改」权限: {data_root}"
            if sys.platform == "win32"
            else f"chmod u+w {data_root}"
        )
        findings.append(
            Finding(
                severity=Severity.ERROR,
                title=f"data.root 不可写: {data_root}",
                detail="编排会在首次写入时失败。",
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
                title="未加载配置——只做了环境体检",
                detail="没有配置就无法检查 data.root 是否可用。",
                fix="cml config init",
            )
        )
    else:
        _check_data_root(Path(config.data_root), findings)

    env = _environment()
    if config_path is not None:
        env["config"] = str(config_path)

    return Report(environment=env, packages=statuses, findings=findings)
