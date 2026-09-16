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

import click

# Section title -> commands, in the order a lake is used rather than in the
# order the commands were written. `SECTIONS` is the whole contract: a command
# missing from it shows up under "Other" in `--help`, and a test fails.
SECTIONS: list[tuple[str, tuple[str, ...]]] = [
    ("Start here", ("config", "doctor", "init")),
    ("Run the pipeline", ("run", "backfill", "derive")),
    ("Check the lake", ("status", "verify", "audit")),
    ("Use the lake", ("query", "serve", "mcp")),
    (
        "Govern and inspect",
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
                ctx.fail(f"`{ctx.command_path} {name}` has moved. Use `{mapping[name]}` instead.")
            return super().resolve_command(ctx, args)

    return _Moved


class SectionedGroup(moved_hints(MOVED)):  # type: ignore[misc]
    """A `click.Group` that prints its commands in sections, and names moves."""

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
            sections.append(("Other", leftovers))

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
@click.version_option(package_name="cnequity")
def cli():
    """cnequity — A-share data ingestion CLI."""


@cli.group()
def run():
    """Run the pipeline, and repair a run that did not finish.

    `daily` and `events` are what a scheduler fires. The other three are the
    manual path back from a failure: `retry` re-runs it, `compact` publishes
    staging a crashed run left behind, and `clean` removes what compacted.
    Every schedule group already runs a `compact` step of its own, so none of
    these three is part of a healthy day.
    """
