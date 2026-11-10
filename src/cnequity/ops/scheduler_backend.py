"""Current-user OS timers. Only the lake's own named entry is changed."""

from __future__ import annotations

import hashlib
import os
import plistlib
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from xml.parsers.expat import ExpatError

from cnequity.file_lock import LockUnavailable, exclusive_lock
from cnequity.i18n import _
from cnequity.ops.catalog import OpsError
from cnequity.ops.environment import command_environment

NS = "http://schemas.microsoft.com/windows/2004/02/mit/task"


def run_command(argv: list[str], *, input: bytes | None = None) -> bytes:
    env = command_environment()
    env["LC_ALL"] = "C"
    try:
        result = subprocess.run(argv, input=input, capture_output=True, env=env, timeout=15)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise OpsError(
            _("Cannot call the system scheduler ({name}).").format(name=type(exc).__name__)
        ) from exc
    if result.returncode:
        # OS diagnostics can contain credentials/paths from unrelated jobs.
        raise OpsError(
            _("The system scheduler refused the request ({name}, exit code {returncode}).").format(
                name=Path(argv[0]).name, returncode=result.returncode
            )
        )
    return result.stdout


class SchedulerBackend:
    def __init__(
        self,
        config_path: Path,
        meta_root: Path,
        *,
        working_directory: str | None = None,
        scheduler_lock_dir: str | None = None,
    ):
        self.config_path = config_path
        self.meta_root = meta_root
        self.platform = sys.platform
        self.working_directory = working_directory or os.getcwd()
        identity = os.path.normcase(str(meta_root.resolve()))
        self.key = hashlib.sha256(identity.encode()).hexdigest()[:16]
        self.name = f"com.cnequity.web.{self.key}"
        # Keep the venv executable path: resolving its symlink loses the venv.
        self.argv = [
            os.path.abspath(sys.executable),
            "-X",
            "utf8",
            "-m",
            "cnequity.serve.ops.scheduled",
            "--config",
            str(config_path),
            "--scheduler-lock-dir",
            scheduler_lock_dir or str(meta_root.parent / "locks"),
        ]
        self.plist = Path.home() / "Library" / "LaunchAgents" / f"{self.name}.plist"
        self.control_lock = Path.home() / ".cnequity" / "scheduler.lock"

    @property
    def kind(self) -> str:
        return {"darwin": "launchd", "win32": _("Windows Task Scheduler")}.get(
            self.platform, "cron" if self.platform.startswith("linux") else _("unsupported")
        )

    def available(self) -> bool:
        commands = {"darwin": ("launchctl",), "win32": ("schtasks", "powershell")}.get(
            self.platform, ("crontab",) if self.platform.startswith("linux") else ()
        )
        return bool(commands) and all(shutil.which(item) for item in commands)

    def render(self) -> str:
        if self.platform == "darwin":
            job = {
                "Label": self.name,
                "ProgramArguments": self.argv,
                "WorkingDirectory": self.working_directory,
                "StartCalendarInterval": [{"Minute": minute} for minute in range(60)],
                "AbandonProcessGroup": True,
                "EnvironmentVariables": {"PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"},
                "StandardOutPath": str(
                    self.meta_root / "state" / "web_scheduler" / "timer.out.log"
                ),
                "StandardErrorPath": str(
                    self.meta_root / "state" / "web_scheduler" / "timer.err.log"
                ),
            }
            return plistlib.dumps(job).decode()
        if self.platform == "win32":
            ET.register_namespace("", NS)
            task = ET.Element(f"{{{NS}}}Task", version="1.2")

            def child(parent, tag, value=None, **attrs):
                node = ET.SubElement(parent, f"{{{NS}}}{tag}", attrs)
                node.text = value
                return node

            triggers = child(task, "Triggers")
            trigger = child(triggers, "TimeTrigger")
            repeat = child(trigger, "Repetition")
            child(repeat, "Interval", "PT1M")
            child(repeat, "StopAtDurationEnd", "false")
            child(trigger, "StartBoundary", "2000-01-01T00:00:00")
            child(trigger, "Enabled", "true")
            principal = child(child(task, "Principals"), "Principal", id="User")
            sid = run_command(["whoami", "/user", "/fo", "csv", "/nh"]).decode("utf-8", "replace")
            match = re.search(r"S-1-\d+(?:-\d+)+", sid)
            if match is None:
                raise OpsError(_("Cannot determine the current Windows user."))
            child(principal, "UserId", match.group())
            child(principal, "LogonType", "InteractiveToken")
            child(principal, "RunLevel", "LeastPrivilege")
            settings = child(task, "Settings")
            for tag, value in {
                "MultipleInstancesPolicy": "IgnoreNew",
                "DisallowStartIfOnBatteries": "false",
                "StopIfGoingOnBatteries": "false",
                "StartWhenAvailable": "true",
                "Enabled": "true",
                "ExecutionTimeLimit": "PT0S",
            }.items():
                child(settings, tag, value)
            action = child(child(task, "Actions", Context="User"), "Exec")
            child(action, "Command", self.argv[0])
            child(action, "Arguments", subprocess.list2cmdline(self.argv[1:]))
            child(action, "WorkingDirectory", self.working_directory)
            return ET.tostring(task, encoding="unicode")
        # cron interprets % even within shell quotes. Escape after shell quoting.
        if any(
            "\n" in value or "\r" in value or "\\" in value
            for value in [*self.argv, self.working_directory]
        ):
            raise OpsError(
                _("cron paths cannot contain newlines or backslashes; use an ordinary path.")
            )
        command = (
            f"cd {shlex.quote(self.working_directory)} && exec " + shlex.join(self.argv)
        ).replace("%", r"\%")
        return f"* * * * * {command} # {self.name}\n"

    def _crontab(self) -> str:
        env = command_environment()
        env["LC_ALL"] = "C"
        try:
            result = subprocess.run(["crontab", "-l"], capture_output=True, env=env, timeout=15)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise OpsError(_("Cannot read the current user's crontab.")) from exc
        if result.returncode:
            if result.returncode == 1 and b"no crontab for" in result.stderr.lower():
                return ""
            raise OpsError(
                _(
                    "Cannot read the current user's crontab; check permissions and the cron installation."
                )
            )
        return result.stdout.decode("utf-8")

    def _windows_xml(self) -> str:
        # Enumerate with Stop so lack of permission is never mistaken for absence.
        script = (
            "$ErrorActionPreference='Stop'; [Console]::OutputEncoding="
            "[System.Text.UTF8Encoding]::new(); $t=Get-ScheduledTask -TaskPath '\\' | "
            f"Where-Object {{$_.TaskName -eq '{self.name}'}}; "
            "if($t){Export-ScheduledTask -TaskName $t.TaskName -TaskPath '\\'}"
        )
        return (
            run_command(["powershell", "-NoProfile", "-NonInteractive", "-Command", script])
            .decode("utf-8-sig")
            .strip()
        )

    def observe(self) -> dict:
        if not self.available():
            return {
                "available": False,
                "installed": False,
                "active": False,
                "matches": False,
                "fingerprint": None,
                "error": _("The current user cannot use the system scheduler."),
            }
        try:
            if self.platform == "darwin":
                content = self.plist.read_text() if self.plist.exists() else ""
                installed = bool(content)
                owned = (
                    installed
                    and plistlib.loads(content.encode()).get("ProgramArguments") == self.argv
                    and plistlib.loads(content.encode()).get("WorkingDirectory")
                    == self.working_directory
                    and plistlib.loads(content.encode()).get("AbandonProcessGroup") is True
                )
                matches = owned and plistlib.loads(content.encode()).get(
                    "StartCalendarInterval"
                ) == [{"Minute": minute} for minute in range(60)]
                # print returns nonzero for an unloaded service; no mutations.
                result = subprocess.run(
                    ["launchctl", "print", f"gui/{os.getuid()}/{self.name}"],
                    capture_output=True,
                    timeout=15,
                )
                active = result.returncode == 0
            elif self.platform == "win32":
                content = self._windows_xml()
                installed = bool(content)
                root = ET.fromstring(content) if installed else None
                owned = (
                    installed
                    and root.findtext(f".//{{{NS}}}Command") == self.argv[0]
                    and root.findtext(f".//{{{NS}}}Arguments")
                    == subprocess.list2cmdline(self.argv[1:])
                    and root.findtext(f".//{{{NS}}}WorkingDirectory") == self.working_directory
                    and root.findtext(f".//{{{NS}}}ExecutionTimeLimit") == "PT0S"
                    and root.findtext(f".//{{{NS}}}LogonType") == "InteractiveToken"
                    and root.findtext(f".//{{{NS}}}RunLevel") == "LeastPrivilege"
                    and len(root.findall(f"{{{NS}}}Actions/{{{NS}}}Exec")) == 1
                )
                matches = (
                    owned and root.findtext(f".//{{{NS}}}Repetition/{{{NS}}}Interval") == "PT1M"
                )
                active = (
                    installed
                    and root.findtext(f"{{{NS}}}Settings/{{{NS}}}Enabled") != "false"
                    and root.findtext(f"{{{NS}}}Triggers/{{{NS}}}TimeTrigger/{{{NS}}}Enabled")
                    != "false"
                )
            else:
                table = self._crontab()
                content = "".join(
                    line + "\n"
                    for line in table.splitlines()
                    if line.rstrip().endswith(f"# {self.name}")
                )
                installed = bool(content)
                matches = content == self.render()
                expected = self.render().split(maxsplit=5)[5]
                lines = content.splitlines()
                owned = (
                    len(lines) == 1
                    and len(lines[0].split(maxsplit=5)) == 6
                    and lines[0].split(maxsplit=5)[5] == expected.rstrip("\n")
                )
                active = installed
            return {
                "available": True,
                "installed": installed,
                "active": active,
                "matches": bool(matches),
                "owned": bool(owned),
                "fingerprint": hashlib.sha256((content + str(active)).encode()).hexdigest(),
                "error": None,
            }
        except (
            OpsError,
            OSError,
            ValueError,
            ET.ParseError,
            ExpatError,
            subprocess.TimeoutExpired,
        ) as exc:
            return {
                "available": True,
                "installed": None,
                "active": False,
                "matches": False,
                "fingerprint": None,
                "error": str(exc),
            }

    def apply(self, enabled: bool, artifact: str) -> None:
        try:
            with exclusive_lock(self.control_lock, blocking=False):
                self._apply(enabled, artifact)
        except LockUnavailable as exc:
            raise OpsError(
                _(
                    "Another lake of the current user is changing the system scheduler; try again shortly."
                )
            ) from exc

    def _apply(self, enabled: bool, artifact: str) -> None:
        observed = self.observe()
        if observed["error"]:
            raise OpsError(observed["error"])
        if observed["installed"] and not observed.get("owned", observed["matches"]):
            raise OpsError(
                _(
                    "The program of the same-named system task does not match the current install; check it in the system scheduler first."
                )
            )
        if self.platform == "darwin":
            service = f"gui/{os.getuid()}/{self.name}"
            if observed["active"]:
                run_command(["launchctl", "bootout", service])
            if not enabled:
                self.plist.unlink(missing_ok=True)
                return
            self.plist.parent.mkdir(parents=True, exist_ok=True)
            self.plist.write_text(artifact, encoding="utf-8")
            run_command(["launchctl", "enable", service])
            run_command(["launchctl", "bootstrap", f"gui/{os.getuid()}", str(self.plist)])
        elif self.platform == "win32":
            if not enabled:
                if observed["installed"]:
                    run_command(["schtasks", "/Delete", "/TN", self.name, "/F"])
                return
            with tempfile.TemporaryDirectory() as folder:
                xml = Path(folder) / "task.xml"
                xml.write_text(artifact, encoding="utf-16")
                run_command(["schtasks", "/Create", "/TN", self.name, "/XML", str(xml), "/F"])
        else:
            table = self._crontab()
            lines = [
                line
                for line in table.splitlines(keepends=True)
                if not line.rstrip().endswith(f"# {self.name}")
            ]
            content = "".join(lines)
            if enabled:
                content = content.rstrip("\n") + "\n" + artifact
            run_command(["crontab", "-"], input=content.encode("utf-8"))
