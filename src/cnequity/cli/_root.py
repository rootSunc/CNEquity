"""The `cne` command group itself.

It lives apart from `main` so command modules can hang themselves off it
(`@cli.command()`) while `main` imports those modules to register them. Putting
the group in `main` instead makes that a cycle.

The group is sectioned. A flat alphabetical list of nineteen entries tells a
newcomer nothing about which one to type first, and it put `audit`, `verify`
and `status` side by side as if choosing between them were obvious. The
sections are the order a lake is actually used: set up, run, check, consume,
and the low-frequency governance surface underneath.
"""

from __future__ import annotations

import logging

import click

# Section title -> commands, in the order a lake is used rather than in the
# order the commands were written. `SECTIONS` is the whole contract: a command
# missing from it shows up under "Other" in `--help`, and a test fails.
# Where a command that no sections list would land. Named so the test that
# forbids leftovers and the formatter cannot drift apart.
LEFTOVER_SECTION = "其它"

SECTIONS: list[tuple[str, tuple[str, ...]]] = [
    ("从这里开始", ("config", "doctor", "init")),
    ("跑 pipeline", ("run", "backfill", "derive")),
    ("检查数据湖", ("status", "verify", "audit")),
    ("使用数据湖", ("query", "serve", "mcp")),
    (
        "治理与检视",
        ("snapshot", "contract", "profile", "stats", "sources", "delisted", "ths-official"),
    ),
]

# Commands that used to exist at this spelling. Click's own answer for an
# unknown name is "No such command", which for a rename is the one thing the
# caller already knows. A dict of replacements costs nothing to carry and,
# unlike a hidden alias, cannot quietly outlive its deprecation — `cne servers`
# was scheduled for removal in 0.9.0 and was still shipping in 0.10.
MOVED: dict[str, str] = {
    "verify-bars": "cne verify --bars --start <date>",
    "stability": "cne verify --runs",
    "retry": "cne run retry",
    "compact": "cne run compact",
    "clean": "cne run clean",
    "demo": "cne init --profile demo",
    "servers": "cne sources probe --only tdx_protocol",
}


def moved_hints(mapping: dict[str, str], base: type[click.Group] = click.Group) -> type:
    """A `click.Group` class that answers an old command name with its new one."""

    class _Moved(base):  # type: ignore[valid-type, misc]
        def resolve_command(self, ctx: click.Context, args: list[str]):
            name = args[0] if args else ""
            if name in mapping and self.get_command(ctx, name) is None:
                ctx.fail(f"`{ctx.command_path} {name}` 已改名，请改用 `{mapping[name]}`。")
            return super().resolve_command(ctx, args)

    return _Moved


logger = logging.getLogger("cnequity.cli")


class _LoggedFailures(click.Group):
    """Put every command failure into the log, not only onto the terminal.

    Click prints `Error: ...` to stderr and stops. That is fine for a person
    watching, and invisible to everything else: a scheduled run tees its output
    into `{data.root}/logs/`, and a failure that never became a log record left
    a file whose last line is whatever the command happened to be doing. The
    one place that sees every command is here, so the record is written here
    rather than in forty-two command bodies that would each have to remember.

    Severity follows who is at fault. A `UsageError` is the caller's — a bad
    flag, an unknown name — and warrants a warning; anything else failed while
    doing the work and is an error. An unexpected exception also carries its
    traceback, because that is the case where the message alone is never
    enough. Nothing is swallowed: every exception is re-raised for Click to
    render and for the exit code to stay what it was.

    A non-zero exit is deliberately *not* recorded. `cne status --datasets`
    exits 1 on a stale dataset and `cne verify` on a gap: those are the command
    working and reporting a finding it already printed, not a failure, and the
    exit code is itself the signal a scheduler reads. Recording it would
    restate the verdict and put a line on stderr after output a caller may be
    parsing.
    """

    # Nested groups get the same behaviour, so the name in the record is the
    # command that actually failed (`run daily`) rather than its parent.
    group_class = type

    @staticmethod
    def _failed_command(ctx: click.Context) -> str:
        """The command that failed, e.g. `run daily`.

        By the time this group sees the exception Click has already popped the
        child's context, so the path is this group's own chain plus the child
        it dispatched to.
        """
        parts: list[str] = []
        node: click.Context | None = ctx
        while node is not None and node.parent is not None:
            parts.append(node.info_name or "")
            node = node.parent
        parts.reverse()
        if ctx.invoked_subcommand:
            parts.append(ctx.invoked_subcommand)
        return " ".join(p for p in parts if p) or (ctx.info_name or "cne")

    # Two commands own their logging and must keep it. `mcp` speaks JSON-RPC on
    # stdout and deliberately holds stderr at WARNING, because an INFO line
    # there is noise in the client's server log; `serve` hands logging to
    # uvicorn, which already reports every request.
    OWNS_ITS_LOGGING = frozenset({"mcp", "serve"})

    def _wire_process_logging(self, ctx: click.Context) -> None:
        """Give every command the pipeline's own INFO records on stderr.

        Done once here rather than in each command body: `cne init` and the
        fetching commands had it, and everything else ran silent — so a slow
        `cne verify` or a `snapshot export` hashing gigabytes looked hung, and
        a library warning during a `status` went nowhere. Commands that also
        want a file in the lake still call `attach_log_file` themselves, since
        only they know which config to read.

        A command with its own `--quiet` calls `_progress_logging` again; that
        uses `basicConfig(force=True)`, so the later call wins.
        """
        if ctx.invoked_subcommand in self.OWNS_ITS_LOGGING:
            return
        if ctx.parent is not None:
            return  # the root group already did it for this invocation
        from cnequity.cli._shared import _progress_logging

        # `take_over=False`: a root logger somebody else configured is theirs.
        # Importing this CLI and calling `contract show` must not destroy the
        # host application's logging, which `force=True` on every command did.
        _progress_logging(take_over=False)

    def make_context(self, info_name, args, parent=None, **extra):
        """Also record a failure in this group's *own* parameters.

        `invoke` never sees those: `cne --bogus-flag` fails while Click is
        building the context, before any command is dispatched. The window is
        small — the group has only `--version` and `--help` — but a failure
        that leaves no record is exactly what this class exists to prevent.

        Shell completion parses the same line with `resilient_parsing` and is
        expected to fail on partial input; that is not an error to report.
        """
        try:
            return super().make_context(info_name, args, parent=parent, **extra)
        except click.UsageError as exc:
            if not extra.get("resilient_parsing"):
                logger.warning("%s: %s", info_name or "cne", exc.format_message())
            raise

    def _already_logged(self, ctx: click.Context) -> bool:
        """One record per failure, written by the innermost group that saw it."""
        meta = ctx.find_root().meta
        if meta.get("cnequity.failure_logged"):
            return True
        meta["cnequity.failure_logged"] = True
        return False

    def invoke(self, ctx: click.Context):
        self._wire_process_logging(ctx)
        try:
            return super().invoke(ctx)
        except click.UsageError as exc:
            if not self._already_logged(ctx):
                logger.warning("%s: %s", self._failed_command(ctx), exc.format_message())
            raise
        except click.ClickException as exc:
            if not self._already_logged(ctx):
                logger.error("%s: %s", self._failed_command(ctx), exc.format_message())
            raise
        except click.Abort:
            if not self._already_logged(ctx):
                logger.warning("%s: aborted", self._failed_command(ctx))
            raise
        except click.exceptions.Exit:
            # `--help` and `--version` leave this way, and Click's `Exit`
            # derives from RuntimeError — so the catch-all below logged every
            # subcommand's `--help` as an unhandled error, traceback and all.
            # `SystemExit` never reached it (it is a BaseException), which is
            # why a non-zero exit from `status --datasets` stayed quiet.
            raise
        except Exception:
            if not self._already_logged(ctx):
                logger.exception("%s: unhandled error", self._failed_command(ctx))
            raise


class ZhHelpOption:
    """Give `-h/--help` a Chinese description, on every command and group.

    Click builds that option itself, with its own English text, so the one
    English line left in an otherwise Chinese `--help` was the line describing
    `--help`. Overriding the factory is the only place it can be said once
    rather than on fifty-one commands. Click's own chrome — `Usage:`,
    `Options:`, `Error:` — is gettext-driven inside Click and is left alone.
    """

    def get_help_option(self, ctx: click.Context):
        option = super().get_help_option(ctx)  # type: ignore[misc]
        if option is not None:
            option.help = "显示帮助并退出。"
        return option


class ZhCommand(ZhHelpOption, click.Command):
    """The class every leaf command is built with (see `SectionedGroup`)."""


class SectionedGroup(ZhHelpOption, moved_hints(MOVED, base=_LoggedFailures)):  # type: ignore[misc]
    """A `click.Group` that prints its commands in sections, and names moves."""

    # Both are inherited by every group and command hung off this one, so a
    # command declared with a plain `@cli.command()` still gets the Chinese
    # help option and the failure logging above.
    command_class = ZhCommand

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        listed = self.list_commands(ctx)
        remaining = set(listed)
        sections: list[tuple[str, list[tuple[str, str]]]] = []

        for title, names in SECTIONS:
            rows: list[tuple[str, str]] = []
            for name in names:
                if name not in remaining:
                    continue
                remaining.discard(name)
                command = self.get_command(ctx, name)
                if command is None or command.hidden:
                    continue
                rows.append((name, command.get_short_help_str(limit=68)))
            if rows:
                sections.append((title, rows))

        # Never silently drop a command that nobody assigned a section: an
        # unsectioned entry has to stay reachable from `--help`, or adding one
        # would make it invisible.
        leftovers: list[tuple[str, str]] = []
        for name in listed:
            if name not in remaining:
                continue
            command = self.get_command(ctx, name)
            if command is None or command.hidden:
                continue
            leftovers.append((name, command.get_short_help_str(limit=68)))
        if leftovers:
            sections.append((LEFTOVER_SECTION, leftovers))

        for title, rows in sections:
            with formatter.section(title):
                formatter.write_dl(rows)


# Click consults this before deciding a name is unknown, so one setting makes
# commands, subcommands, options and `Choice` values agree on case. Without it
# `cne STATUS` was a dead end: Click's suggestions run on edit distance, and an
# all-caps spelling is too far from its own lowercase to be offered, so the
# error named no way forward. Lowercase typos still get the usual suggestion.
CONTEXT_SETTINGS = {"token_normalize_func": str.lower, "help_option_names": ["-h", "--help"]}


@click.group(cls=SectionedGroup, context_settings=CONTEXT_SETTINGS)
@click.version_option(package_name="cnequity", help="显示版本并退出。")
def cli():
    """cnequity —— A 股数据采集 CLI。"""


@cli.group()
def run():
    """跑 pipeline，以及修复没跑完的 run。

    \b
    调度器要跑的是 `daily` 和 `events`。另外三条是失败之后手动回到正轨的路：
    `retry` 重跑，`compact` 把崩溃的 run 留在 staging 的数据发布出去，
    `clean` 清掉已经 compact 过的 staging。每个调度组自带 `compact` 步骤，
    所以正常的一天不会用到这三条。
    """
