"""Settings shared by command logging and log retention."""

from dataclasses import dataclass
from logging import getLevelName

from cnequity.config.errors import ConfigError
from cnequity.progress import HEARTBEAT_INTERVAL_SECONDS

DEFAULT_LOG_RETENTION_DAYS = 30
_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


@dataclass(frozen=True)
class LoggingSettings:
    console_level: str = "INFO"
    file_level: str = "INFO"
    heartbeat_seconds: int = HEARTBEAT_INTERVAL_SECONDS
    retention_days: int = DEFAULT_LOG_RETENTION_DAYS

    def console_threshold(self, *, quiet: bool = False) -> int:
        return (
            max(getLevelName(self.console_level), getLevelName("WARNING"))
            if quiet
            else getLevelName(self.console_level)
        )

    @property
    def file_threshold(self) -> int:
        return getLevelName(self.file_level)


DEFAULT_LOGGING = LoggingSettings()


def logging_settings(config) -> LoggingSettings:
    return getattr(config, "logging", DEFAULT_LOGGING)


def parse_logging(raw) -> LoggingSettings:
    if not isinstance(raw, dict):
        raise ConfigError("[logging] must be a table")
    values = {}
    for key in ("console_level", "file_level"):
        value = raw.get(key, getattr(DEFAULT_LOGGING, key))
        level = value.strip().upper() if isinstance(value, str) else None
        if level not in _LEVELS:
            raise ConfigError(f"[logging].{key} must be one of {', '.join(_LEVELS)}")
        values[key] = level
    for key in ("heartbeat_seconds", "retention_days"):
        value = raw.get(key, getattr(DEFAULT_LOGGING, key))
        if type(value) is not int or value < 0:
            raise ConfigError(f"[logging].{key} must be a non-negative integer")
        values[key] = value
    return LoggingSettings(**values)
