"""The shape of the top-level command surface, as opposed to what any one does.

Twenty-five flat top-level entries is what these guards exist to stop coming
back: four clusters of near-synonyms sat side by side (`audit` / `verify` /
`verify-bars` / `stability` / `status`), and a deprecated alias outlived its own
removal notice by a whole minor version. Both are drift nothing else catches —
`--help` is the one part of the CLI no test reads.
"""

from __future__ import annotations

import click
import pytest
from click.testing import CliRunner

from cnequity.cli._root import MOVED, SECTIONS
from cnequity.cli.main import cli


def _top_level() -> list[str]:
    return cli.list_commands(click.Context(cli))


def _all_command_paths() -> list[list[str]]:
    """Every command and subcommand path in the tree."""
    out: list[list[str]] = []

    def walk(cmd: click.Command, ctx: click.Context, path: list[str]) -> None:
        if not isinstance(cmd, click.Group):
            return
        for name in cmd.list_commands(ctx):
            sub = cmd.get_command(ctx, name)
            if sub is None or sub.hidden:
                continue
            out.append([*path, name])
            walk(sub, click.Context(sub, parent=ctx), [*path, name])

    walk(cli, click.Context(cli), [])
    return out


def test_every_command_is_assigned_a_help_section():
    """An unsectioned command still prints, under "Other" — which is the bug."""
    sectioned = {name for _, names in SECTIONS for name in names}
    unassigned = sorted(set(_top_level()) - sectioned)
    assert not unassigned, (
        f"{unassigned} would land under 'Other' in `cne --help`; "
        "add each to the right section in cnequity.cli._root.SECTIONS"
    )


def test_sections_do_not_name_commands_that_no_longer_exist():
    registered = set(_top_level())
    stale = sorted({name for _, names in SECTIONS for name in names} - registered)
    assert not stale, f"SECTIONS names commands that are not registered: {stale}"


def test_help_prints_sections_and_no_leftovers():
    result = CliRunner().invoke(cli, ["--help"])
    assert result.exit_code == 0
    for title, _ in SECTIONS:
        assert f"{title}:" in result.output
    assert "Other:" not in result.output


@pytest.mark.parametrize("name", sorted(MOVED))
def test_a_moved_command_names_its_replacement(name: str):
    """The point of MOVED: the error says where the command went."""
    assert name not in _top_level(), f"{name} is registered, so it has not moved"
    result = CliRunner().invoke(cli, [name])
    assert result.exit_code != 0
    assert MOVED[name] in result.output


def test_moved_replacements_are_reachable_commands():
    """A hint pointing at a command that does not exist is worse than none."""
    for old, replacement in MOVED.items():
        words = replacement.split()
        assert words[0] == "cne", replacement
        command: click.Command = cli
        # Walk only as deep as the groups go: the first word that is not a
        # subcommand is an option or its value (`--profile demo`), not a name
        # to resolve.
        depth = 0
        for word in words[1:]:
            if not isinstance(command, click.Group) or word.startswith("-"):
                break
            found = command.get_command(click.Context(command), word)
            if found is None:
                break
            command = found
            depth += 1
        assert depth, f"{old} -> {replacement}: `{words[1]}` is not a command"
        assert command is not cli, f"{old} -> {replacement} resolves to nothing"


def test_the_run_group_carries_the_whole_run_lifecycle():
    run = cli.get_command(click.Context(cli), "run")
    assert isinstance(run, click.Group)
    assert set(run.list_commands(click.Context(run))) == {
        "daily",
        "events",
        "retry",
        "compact",
        "clean",
    }


def test_ths_official_capture_does_not_collide_with_the_lake_snapshot():
    """Two unrelated `snapshot` commands was the collision this rename removed."""
    ctx = click.Context(cli)
    group = cli.get_command(ctx, "ths-official")
    assert isinstance(group, click.Group)
    names = group.list_commands(click.Context(group))
    assert "capture" in names
    assert "snapshot" not in names


def test_a_moved_config_action_is_answered_like_a_moved_command():
    """`_root.MOVED` answers a moved command; an argument is not a command, and
    `cne config init` is the first thing a new lake ever runs — Click's own
    rejection names every valid spelling except the one the caller needs."""
    from cnequity.cli.setup_cmds import CONFIG_ACTIONS, CONFIG_ACTIONS_MOVED

    result = CliRunner().invoke(cli, ["config", "init"])

    assert result.exit_code != 0
    assert "has moved" in result.output
    assert "cne config create" in result.output
    # The moved spellings and the live ones must not overlap, or one shadows
    # the other depending on which check runs first.
    assert not set(CONFIG_ACTIONS_MOVED) & set(CONFIG_ACTIONS)


def test_an_unknown_config_action_still_names_the_valid_ones():
    """Dropping the Choice must not cost the ordinary error its usefulness."""
    result = CliRunner().invoke(cli, ["config", "nonsense"])

    assert result.exit_code != 0
    for action in ("validate", "create", "diff"):
        assert action in result.output


def test_the_cli_reference_indexes_every_top_level_command():
    """`docs/reference/cli.md` opens with an index; a new command must join it.

    The page is 700 lines of per-command sections, so the index is how anyone
    finds the right one — and an index that silently misses a command is worse
    than none. It is also what GitHub Pages serves as the CLI reference.
    """
    from pathlib import Path

    reference = Path(__file__).resolve().parents[2] / "docs" / "reference" / "cli.md"
    index = reference.read_text(encoding="utf-8").split("## 改名对照")[0]
    missing = sorted(n for n in _top_level() if f"`cne {n}" not in index)
    assert not missing, (
        f"{missing} are registered but absent from the 命令一览 table in {reference.name}"
    )


def test_the_cli_reference_documents_every_move():
    """Same for the rename table: an old spelling users may type must be listed."""
    from pathlib import Path

    reference = Path(__file__).resolve().parents[2] / "docs" / "reference" / "cli.md"
    text = reference.read_text(encoding="utf-8")
    assert "## 改名对照" in text, "the rename table is gone"
    # Bound it to that one section. The per-command sections below it also say
    # "原 `cne stability`" and similar, which would make any spelling look
    # documented no matter what the table actually lists.
    table = text.split("## 改名对照", 1)[1].split("\n## ", 1)[0]
    missing = sorted(old for old in MOVED if f"`cne {old}" not in table)
    assert not missing, f"MOVED lists {missing}, which the rename table does not"


@pytest.mark.parametrize(
    "argv",
    [
        ["STATUS", "--help"],
        ["Status", "--help"],
        ["run", "DAILY", "--help"],
        ["SOURCES", "POLICY", "--help"],
        ["ths-official", "CAPTURE", "--help"],
    ],
)
def test_command_names_are_case_insensitive(argv):
    """`cne STATUS` was a dead end: no match, and no suggestion either.

    Click offers a suggestion by edit distance, and an all-caps spelling is too
    far from its own lowercase to make the cut — so the error named no way
    forward, while `cne Status` did get one.
    """
    result = CliRunner().invoke(cli, argv)
    assert result.exit_code == 0, result.output


def test_a_prefix_is_not_silently_resolved():
    """Case-insensitivity must not turn into guessing at abbreviations."""
    result = CliRunner().invoke(cli, ["stat"])
    assert result.exit_code != 0
    assert "No such command" in result.output
    # …but it should still point at the candidates.
    assert "stats" in result.output and "status" in result.output


def test_the_config_action_accepts_any_case(tmp_path):
    """A free-form argument bypasses `token_normalize_func`; the body normalises."""
    out = tmp_path / "cnequity.toml"
    result = CliRunner().invoke(
        cli, ["config", "CREATE", "--config", str(out), "--data-root", str(tmp_path / "lake")]
    )
    assert result.exit_code == 0, result.output
    assert out.is_file()
    # The moved spelling is answered whatever its case.
    moved = CliRunner().invoke(cli, ["config", "INIT", "--config", str(out)])
    assert moved.exit_code != 0
    assert "cne config create" in moved.output


def test_resilience_refuses_a_config_path_that_does_not_exist():
    """It reads the registry, not the lake — but a typo must not pass silently."""
    result = CliRunner().invoke(cli, ["sources", "resilience", "--config", "/nope/missing.toml"])
    assert result.exit_code != 0
    assert "Config not found" in result.output
    # Without an explicit --config it still answers, because it needs no lake.
    bare = CliRunner().invoke(cli, ["sources", "resilience"])
    assert bare.exit_code == 0, bare.output


def test_backfill_refuses_a_reversed_date_range(tmp_path):
    """A transposed range used to cost a full sweep and still report success.

    The walk had no days in it, the step raised, the engine logged the
    traceback — and the command printed status=success with rows_written=0.
    """
    cfg = tmp_path / "cnequity.toml"
    cfg.write_text(f'[data]\nroot = "{(tmp_path / "lake").as_posix()}"\n', encoding="utf-8")
    result = CliRunner().invoke(
        cli,
        [
            "backfill",
            "daily_bars",
            "--config",
            str(cfg),
            "--start",
            "2026-01-02",
            "--end",
            "2026-01-01",
        ],
    )
    assert result.exit_code != 0
    assert "--start must be on or before --end" in result.output


def test_verify_refuses_a_dataset_name_it_does_not_know(tmp_path):
    """A misspelt `--dataset` reported 覆盖完整 and exited 0."""
    cfg = tmp_path / "cnequity.toml"
    cfg.write_text(f'[data]\nroot = "{(tmp_path / "lake").as_posix()}"\n', encoding="utf-8")
    result = CliRunner().invoke(cli, ["verify", "--dataset", "daily_bar", "--config", str(cfg)])
    assert result.exit_code != 0
    assert "unknown dataset 'daily_bar'" in result.output
    assert "daily_bars" in result.output, "a near miss should be offered"


def test_every_command_path_resolves_in_any_case():
    """Exhaustive, because the earlier fix was easy to leave half-applied.

    A group reached through one case and a subcommand through another is the
    shape a partial fix takes, so walk the whole tree rather than sampling.
    """
    runner = CliRunner()
    failures = []
    for path in _all_command_paths():
        for variant in (
            [w.upper() for w in path],
            [w.capitalize() for w in path],
            [w.swapcase() for w in path],
        ):
            result = runner.invoke(cli, [*variant, "--help"])
            if result.exit_code != 0:
                failures.append(" ".join(variant))
    assert not failures, f"case variants that did not resolve: {failures}"


@pytest.mark.parametrize(
    ("argv", "needle"),
    [
        (["verify", "--dataset", "DAILY_BARS"], "unknown dataset"),
        (["backfill", "Daily_Bars"], "unknown dataset"),
        (["derive", "ADJ_FACTORS"], "Unknown derive target"),
        (["contract", "show", "--dataset", "DAILY_BARS"], "KeyError"),
        (["stats", "show", "--dataset", "DAILY_BARS"], "unknown"),
    ],
)
def test_registry_names_are_case_insensitive(argv, needle, tmp_path):
    """Registry names are lower case, so a name typed in caps must still resolve."""
    cfg = tmp_path / "cnequity.toml"
    cfg.write_text(f'[data]\nroot = "{(tmp_path / "lake").as_posix()}"\n', encoding="utf-8")
    result = CliRunner().invoke(cli, [*argv, "--config", str(cfg)])
    # Whatever else happens, it must not be rejected for the *name*.
    assert needle not in (result.output or ""), result.output
