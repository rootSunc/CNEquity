"""Effective revision retention shared by preview and cleanup."""

from cnequity.config.errors import ConfigError

DEFAULT_KEEP_GENERATIONS = 5


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
