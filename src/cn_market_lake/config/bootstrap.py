"""Bootstrap a user config from the packaged example template."""

from __future__ import annotations

import re
import sys
from importlib.resources import files
from pathlib import Path

TEMPLATE_NAME = "cn-market-lake.example.toml"
DEFAULT_USER_CONFIG = Path("configs/cn-market-lake.toml")


def example_toml_text() -> str:
    """Return the packaged example config (works from PyPI wheels and editable installs)."""
    return (
        files("cn_market_lake.config.templates").joinpath(TEMPLATE_NAME).read_text(encoding="utf-8")
    )


def _toml_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def path_for_toml(path: Path | str) -> str:
    """Render *path* safe for a TOML basic string.

    Uses POSIX form so Windows ``C:\\Users\\…`` never injects ``\\U`` hex
    escapes, then applies the usual backslash/quote escaping.
    """
    return _toml_escape(Path(path).resolve().as_posix())


def render_example_toml(
    *,
    data_root: str | None = None,
    platform: str | None = None,
) -> str:
    """Render the example template with optional local tweaks."""
    text = example_toml_text()
    if data_root is not None:
        # Callable replacement: a string template would re-interpret every `\\`
        # from `_toml_escape`, undoing the Windows-path escaping.
        escaped = _toml_escape(data_root)

        def _patch_root(match: re.Match[str]) -> str:
            return f"{match.group(1)}{escaped}{match.group(2)}"

        replaced = re.sub(
            r'(?m)^(\[data\]\s*\nroot\s*=\s*")[^"]*(")',
            _patch_root,
            text,
            count=1,
        )
        if replaced == text:
            raise ValueError("could not patch [data].root in example template")
        text = replaced

    plat = platform if platform is not None else sys.platform
    if plat in ("darwin", "win32"):
        # macOS: TDX client + ProcessPool fork is unsafe (see validate_config).
        # Windows: default to 1 as well — spawn works, but first-run memory and
        # file-lock contention on a laptop are safer single-process; raise
        # workers later after `cml doctor` is green.
        text = re.sub(r"(?m)^(workers\s*=\s*)\d+", r"\g<1>1", text, count=1)

    return text


def write_user_config(
    path: Path,
    *,
    data_root: str | None = None,
    force: bool = False,
    platform: str | None = None,
) -> Path:
    """Write a user config file from the packaged example.

    When *data_root* is omitted, the template's ``./data/cn-market-lake`` is
    resolved to an absolute path so ``cml doctor`` is green on first run
    (relative roots break under launchd/cron CWDs).

    Raises:
        FileExistsError: when ``path`` exists and ``force`` is false.
    """
    path = Path(path)
    if path.exists() and not force:
        raise FileExistsError(f"Config already exists: {path}. Re-run with --force to overwrite.")
    root = Path(data_root) if data_root is not None else Path("./data/cn-market-lake")
    # resolve + as_posix; render_example_toml still applies TOML escaping.
    absolute_root = root.expanduser().resolve().as_posix()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        render_example_toml(data_root=absolute_root, platform=platform),
        encoding="utf-8",
    )
    return path
