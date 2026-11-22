"""Sources that need the user's own credential, declared once.

The loader, the key check, the operations status, the settings page and the
dashboard's key form all read this table. Adding a credentialed source is one
entry here plus its adapter and its probe in :mod:`cnequity.ops.keys`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from cnequity.i18n import N_


@dataclass(frozen=True)
class KeyedSource:
    #: The source label rows are stamped with, and ``[sources.<id>]``.
    id: str
    label: str
    #: The credential's key in ``[sources.<id>]``; ``<key>_file`` names a file holding it.
    key: str
    #: The environment variable that overrides the config file.
    env: str
    #: The ``Config`` attribute the loader puts the credential on.
    attr: str
    #: Whether ``[sources.<id>].daily`` may add the source's datasets to the daily run.
    daily_opt_in: bool = False

    def default_key_file(self) -> Path:
        """Where the dashboard writes a key it is given: outside the lake and the config."""
        base = os.environ.get("XDG_CONFIG_HOME") or "~/.config"
        return Path(base).expanduser() / "cnequity" / "keys" / self.id


KEYED_SOURCES: dict[str, KeyedSource] = {
    "tushare": KeyedSource(
        id="tushare",
        label=N_("Tushare"),
        key="token",
        env="TUSHARE_TOKEN",
        attr="tushare_token",
        # Backfill-only by default; daily only with a working key and this switch.
        daily_opt_in=True,
    ),
    "ths_official": KeyedSource(
        id="ths_official",
        label=N_("THS official API"),
        key="api_key",
        env="HITHINK_FINANCE_API_KEY",
        attr="ths_official_api_key",
    ),
}

#: The schedule group ``[sources.tushare].daily = true`` adds to the daily run.
TUSHARE_DAILY_GROUP = "tushare"


def tushare_daily_steps() -> list[str]:
    """Every Tushare-primary dataset; each is filled by the step of the same name."""
    from cnequity.domain.datasets import DATASETS

    return sorted(name for name, spec in DATASETS.items() if spec.primary_source == "tushare")
