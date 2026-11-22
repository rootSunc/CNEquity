"""Effective revision retention shared by preview and cleanup."""

from cnequity.config.errors import ConfigError

DEFAULT_KEEP_GENERATIONS = 5
DEFAULT_KEEP_DAYS = 7


def revision_keep(config, override: int | None = None) -> int:
    """Return the number to retain, with zero disabling automatic cleanup."""
    keep = (
        getattr(config, "revision_keep_generations", DEFAULT_KEEP_GENERATIONS)
        if override is None
        else override
    )
    if isinstance(keep, bool) or not isinstance(keep, int) or keep < 0:
        raise ConfigError("[storage.retention].keep_generations must be an integer >= 0")
    return keep


def revision_keep_days(config, override: int | None = None) -> int:
    """Return how many days back a kept revision may have been current; 0 sets no limit."""
    days = (
        getattr(config, "revision_keep_days", DEFAULT_KEEP_DAYS) if override is None else override
    )
    if isinstance(days, bool) or not isinstance(days, int) or days < 0:
        raise ConfigError("[storage.retention].keep_days must be an integer >= 0")
    return days
