"""DuckDB resource settings for each lake workload."""

from dataclasses import dataclass, replace
from pathlib import Path

from cnequity.config.errors import ConfigError

DEFAULT_READ_MEMORY = "2GB"
DEFAULT_READ_THREADS = 4
DEFAULT_SCAN_BUDGET_ROWS = 4_000_000


@dataclass(frozen=True)
class DuckDBResources:
    memory_limit: str
    threads: int | None = None
    temp_directory: Path | None = None
    max_temp_directory_size: str | None = None

    def options(self, *, scratch: Path | None = None) -> dict:
        return {
            key: value
            for key, value in (
                ("memory_limit", self.memory_limit),
                ("threads", self.threads),
                ("temp_directory", self.temp_directory or scratch),
                ("max_temp_directory_size", self.max_temp_directory_size),
            )
            if value is not None
        }

    def apply(self, connection, *, scratch: Path | None = None) -> None:
        for setting, value in self.options(scratch=scratch).items():
            connection.execute(
                f"SET {setting} = ?", [str(value) if isinstance(value, Path) else value]
            )


DEFAULT_RESOURCES = {
    "read": DuckDBResources(DEFAULT_READ_MEMORY, DEFAULT_READ_THREADS),
    "write": DuckDBResources("4GB"),
    "export": DuckDBResources("4GB"),
    "audit": DuckDBResources("512MB", 1),
    "maintenance": DuckDBResources("8GB"),
}


def resources_for(config, workload: str) -> DuckDBResources:
    budget = getattr(config, "duckdb_resources", {}).get(workload, DEFAULT_RESOURCES[workload])
    if workload == "read":
        # The original programmatic Config fields remain the read overrides.
        return replace(
            budget,
            memory_limit=getattr(config, "duckdb_memory_limit", budget.memory_limit),
            threads=getattr(config, "duckdb_threads", budget.threads),
        )
    return budget


def parse_resources(raw: dict, expand_path) -> dict[str, DuckDBResources]:
    result = {}
    for workload, default in DEFAULT_RESOURCES.items():
        section = raw if workload == "read" else raw.get(workload, {})
        if not isinstance(section, dict):
            raise ConfigError(f"[duckdb.{workload}] must be a table")
        memory = section.get("memory_limit", default.memory_limit)
        threads = section.get("threads", default.threads)
        scratch = section.get("temp_directory")
        max_spill = section.get("max_temp_directory_size")
        for name, value in (
            ("memory_limit", memory),
            ("temp_directory", scratch),
            ("max_temp_directory_size", max_spill),
        ):
            if value is not None and (not isinstance(value, str) or not value.strip()):
                raise ConfigError(f"DuckDB {workload} {name} must be a non-empty string")
        if threads is not None and (type(threads) is not int or threads < 1):
            raise ConfigError(f"DuckDB {workload} threads must be a positive integer")
        result[workload] = DuckDBResources(
            memory, threads, expand_path(scratch) if scratch else None, max_spill
        )
    return result
