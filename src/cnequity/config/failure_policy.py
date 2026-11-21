"""Escalation rules for consecutive unhealthy job results."""

from dataclasses import dataclass

DEFAULT_RUN_FAILURE_WINDOW_HOURS = 24
DEFAULT_RUN_FAILURE_STREAK = 3
DEFAULT_DAILY_FAILURE_STREAK = 3


@dataclass(frozen=True)
class FailureStreak:
    minimum: int
    window_hours: int | None = None

    def reached(self, streak: int) -> bool:
        return self.minimum > 0 and streak >= self.minimum


def recent_run_streak(config) -> FailureStreak:
    return FailureStreak(
        getattr(config, "run_failure_streak", DEFAULT_RUN_FAILURE_STREAK),
        window_hours=getattr(config, "run_failure_window_hours", DEFAULT_RUN_FAILURE_WINDOW_HOURS),
    )


def daily_group_streak(config) -> FailureStreak:
    return FailureStreak(config.maintenance_soft_fail_max_days)
