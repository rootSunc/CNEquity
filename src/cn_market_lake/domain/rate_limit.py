from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path

from cn_market_lake.file_lock import exclusive_lock


@dataclass(frozen=True)
class RateLimitSpec:
    """Pickle-friendly rate limit parameters for worker processes."""

    state_dir: str
    source: str
    min_interval: float


@dataclass
class RateLimiter:
    """Cross-process token bucket using a file lock and shared timestamp state."""

    name: str
    min_interval: float
    state_dir: Path

    def wait(self) -> None:
        if self.min_interval <= 0:
            return

        self.state_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self.state_dir / f"{self.name}.lock"
        state_path = self.state_dir / f"{self.name}.json"

        with exclusive_lock(lock_path):
            last = 0.0
            if state_path.exists():
                try:
                    last = float(
                        json.loads(state_path.read_text(encoding="utf-8")).get("last", 0.0)
                    )
                except (json.JSONDecodeError, TypeError, ValueError):
                    last = 0.0

            now = time.time()
            elapsed = now - last
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
                now = time.time()

            state_path.write_text(json.dumps({"last": now}), encoding="utf-8")


def wait_source(state_dir: Path | str, source: str, min_interval: float) -> None:
    RateLimiter(source, min_interval, Path(state_dir)).wait()


def wait_spec(spec: RateLimitSpec | None) -> None:
    if spec is not None:
        wait_source(spec.state_dir, spec.source, spec.min_interval)
