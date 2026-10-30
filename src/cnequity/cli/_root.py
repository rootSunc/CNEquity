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
from functools import cache

import click

from cnequity.i18n import N_, _, translate_help

# Section title -> commands, in the order a lake is used rather than in the
# order the commands were written. `SECTIONS` is the whole contract: a command
# missing from it shows up under "Other" in `--help`, and a test fails.
# Where a command that no sections list would land. Named so the test that
# forbids leftovers and the formatter cannot drift apart.
LEFTOVER_SECTION = N_("Other")

SECTIONS: list[tuple[str, tuple[str, ...]]] = [
    (N_("Start here"), ("config", "doctor", "init")),
    (
        N_("Run the pipeline"),
        ("run", "schedule", "backfill", "import", "derive", "repair", "migrate"),
    ),
    (N_("Check the lake"), ("check", "status", "verify", "audit", "decision-data")),
    (N_("Use the lake"), ("query", "serve", "mcp")),
    (
        N_("Governance and inspection"),
        (
            "snapshot",
            "storage",
            "contract",
            "profile",
            "stats",
            "sources",
            "delisted",
            "ths-official",
        ),
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
                ctx.fail(
                    _("`{command_path} {name}` has been renamed; use `{value}` instead.").format(
                        command_path=ctx.command_path, name=name, value=mapping[name]
                    )
                )
            return super().resolve_command(ctx, args)

    return _Moved


logger = logging.getLogger("cnequity.cli")

# Click prints a ClickException or UsageError itself right after this record is
# logged; the terminal handler skips records carrying this mark so the message
# is not shown twice. Log files and other handlers still get them.
ECHOED_BY_CLICK = {"echoed_by_click": True}


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
                logger.warning(
                    "%s: %s", info_name or "cne", exc.format_message(), extra=ECHOED_BY_CLICK
                )
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
                logger.warning(
                    "%s: %s",
                    self._failed_command(ctx),
                    exc.format_message(),
                    extra=ECHOED_BY_CLICK,
                )
            raise
        except click.ClickException as exc:
            if not self._already_logged(ctx):
                logger.error(
                    "%s: %s",
                    self._failed_command(ctx),
                    exc.format_message(),
                    extra=ECHOED_BY_CLICK,
                )
            raise
        except click.Abort:
            if not self._already_logged(ctx):
                logger.warning(N_("%s: aborted"), self._failed_command(ctx))
            raise
        except KeyboardInterrupt:
            # Click turns this into "Aborted!" once it leaves the command, which
            # tells someone who pressed Ctrl-C an hour into `cne init` neither
            # that the finished work survived nor how to pick it up again.
            if not self._already_logged(ctx):
                command = self._failed_command(ctx)
                logger.warning(N_("%s: interrupted"), command, extra=ECHOED_BY_CLICK)
                hint = _interrupt_hint(command)
                if hint:
                    click.echo(hint, err=True)
            raise
        except click.exceptions.Exit:
            # `--help` and `--version` leave this way, and Click's `Exit`
            # derives from RuntimeError — so the catch-all below logged every
            # subcommand's `--help` as an unhandled error, traceback and all.
            # `SystemExit` never reached it (it is a BaseException), which is
            # why a non-zero exit from `status --datasets` stayed quiet.
            raise
        except Exception as exc:
            message = _expected_failure(exc)
            if message is not None:
                # The lake simply lacks something the command needs; the message
                # already says what and how to build it, a traceback adds noise.
                if not self._already_logged(ctx):
                    logger.error(
                        "%s: %s", self._failed_command(ctx), message, extra=ECHOED_BY_CLICK
                    )
                raise click.ClickException(message) from exc
            if not self._already_logged(ctx):
                logger.exception(N_("%s: unhandled error"), self._failed_command(ctx))
            raise


def _interrupt_hint(command: str) -> str | None:
    """What a Ctrl-C left behind, for the commands that write the lake."""
    if command == "init":
        return _("Stopped. Run `cne init` again to resume; finished batches are kept.")
    if command.split(" ", 1)[0] in {"run", "backfill", "derive", "repair"}:
        return _(
            "Stopped. Finished batches are kept and the run is recorded as interrupted; run the same command again to continue, and `cne status` shows what it finished."
        )
    return None


def _expected_failure(exc: Exception) -> str | None:
    """The message of a failure that is the lake's state, not a bug in the CLI.

    A dataset that was never built (`ReaderError`) or a derive whose input is
    absent (`InputUnavailableError`) reach the top of a command the same way a
    defect does, and used to print a full traceback. Imported here because both
    modules pull in the data stack, which `--help` must not pay for.
    """
    from cnequity.orchestrator.outcomes import InputUnavailableError
    from cnequity.orchestrator.run_lock import RunLockError
    from cnequity.query.reader import ReaderError
    from cnequity.storage.lifecycle import LifecycleError
    from cnequity.storage.revisions import DiskSpaceError

    # Another job holding the lake is the lake's state too: `cne backfill`
    # beside a running init printed the refusal under a traceback. A full disk
    # is the same kind of failure: the message says what to do.
    if isinstance(
        exc, ReaderError | InputUnavailableError | RunLockError | LifecycleError | DiskSpaceError
    ):
        return str(exc)
    return None


class _TranslatedText:
    """An attribute that stores help text as written and reads it translated.

    Click assigns `help`, `short_help` and `epilog` in its constructors and
    reads them while rendering. Holding the written text (the msgid) and
    translating on every read means the language is the one in force when help
    is shown, not when the module was imported, and anything that inspects the
    attribute — the docs generator, `to_info_dict` — sees what a person would.
    The written text lives in the instance `__dict__` under the same name, so
    an object built before its class was swapped (see `_translated_param`)
    keeps working.
    """

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def __get__(self, obj: object, objtype: type | None = None):
        if obj is None:
            return self
        return translate_help(obj.__dict__.get(self.name))

    def __set__(self, obj: object, value: str | None) -> None:
        obj.__dict__[self.name] = value


@cache
def _translated_param_class(cls: type[click.Parameter]) -> type[click.Parameter]:
    return type(cls.__name__, (cls,), {"help": _TranslatedText(), "__module__": cls.__module__})


def _translated_param(param: click.Parameter) -> click.Parameter:
    """Make an option's help translate when read, whatever class built it.

    `@click.option` builds a plain `click.Option` unless every call site passes
    `cls=`, so options are converted here, where the command hands them to the
    help formatter. That also reaches the help option, which Click creates on
    demand.
    """
    if "help" in param.__dict__ and not isinstance(
        type(param).__dict__.get("help"), _TranslatedText
    ):
        param.__class__ = _translated_param_class(type(param))
    return param


class TranslatedHelp:
    """Render command help, option help and Click's help chrome translated.

    The written text stays the msgid, and an untranslated one is shown as
    written, so a command nobody has migrated yet renders exactly as before.
    """

    help = _TranslatedText()
    short_help = _TranslatedText()
    epilog = _TranslatedText()

    def get_params(self, ctx: click.Context) -> list[click.Parameter]:
        return [_translated_param(param) for param in super().get_params(ctx)]  # type: ignore[misc]

    def format_usage(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        pieces = self.collect_usage_pieces(ctx)  # type: ignore[attr-defined]
        formatter.write_usage(ctx.command_path, " ".join(pieces), prefix=_("Usage: "))

    def format_options(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        records = [
            record for param in self.get_params(ctx) if (record := param.get_help_record(ctx))
        ]
        if records:
            with formatter.section(_("Options")):
                formatter.write_dl(records)
        if isinstance(self, click.Group):
            self.format_commands(ctx, formatter)


class LocalizedHelpOption:
    """Give `-h/--help` a translatable description, on every command and group.

    Click builds that option itself, with its own English text, so the one line
    the catalog could not reach in `--help` was the line describing `--help`.
    Overriding the factory is the only place it can be said once rather than
    on fifty-one commands. `TranslatedHelp` below renders it, and the `Usage:`
    and `Options` chrome; the rest of Click's own wording — `Error:`,
    `[default: …]` — still comes from Click.
    """

    def get_help_option(self, ctx: click.Context):
        option = super().get_help_option(ctx)  # type: ignore[misc]
        if option is not None:
            option.help = N_("Show this message and exit.")
        return option


class CneCommand(LocalizedHelpOption, TranslatedHelp, click.Command):
    """The class every leaf command is built with (see `SectionedGroup`)."""


class SectionedGroup(LocalizedHelpOption, TranslatedHelp, moved_hints(MOVED, base=_LoggedFailures)):  # type: ignore[misc]
    """A `click.Group` that prints its commands in sections, and names moves."""

    # Both are inherited by every group and command hung off this one, so a
    # command declared with a plain `@cli.command()` still gets the Chinese
    # help option and the failure logging above.
    command_class = CneCommand

    def format_commands(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        if ctx.parent is not None:
            # SECTIONS describe the top level. A subgroup inherits this class,
            # and sectioning it filed `cne schedule status` under "Check the
            # lake" beside a root command that merely shares its name.
            super().format_commands(ctx, formatter)
            return
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
            with formatter.section(_(title)):
                formatter.write_dl(rows)


# Click consults this before deciding a name is unknown, so one setting makes
# commands, subcommands, options and `Choice` values agree on case. Without it
# `cne STATUS` was a dead end: Click's suggestions run on edit distance, and an
# all-caps spelling is too far from its own lowercase to be offered, so the
# error named no way forward. Lowercase typos still get the usual suggestion.
CONTEXT_SETTINGS = {"token_normalize_func": str.lower, "help_option_names": ["-h", "--help"]}


@click.group(cls=SectionedGroup, context_settings=CONTEXT_SETTINGS)
@click.version_option(package_name="cnequity", help=N_("Show the version and exit."))
def cli():
    """cnequity: a CLI for collecting China A-share data."""
    from cnequity.fd_limit import raise_open_file_limit

    raise_open_file_limit()


@cli.group()
def run():
    """Run the pipeline, and repair runs that did not finish.

    \b
    Schedulers run `daily` and `events`. The other three get things back on track by hand after a failure:
    `retry` re-runs, `compact` publishes the data a crashed run left in staging,
    and `clean` clears staging that has already been compacted. Every schedule group
    has its own `compact` step, so a normal day needs none of the three.

    """
