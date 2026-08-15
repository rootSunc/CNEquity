import pytest

from cn_market_lake.config import load_config
from cn_market_lake.config.bootstrap import path_for_toml


@pytest.fixture
def config(tmp_path):
    """Minimal offline config wiring a daily Wave over mock adapters."""
    cfg_path = tmp_path / "test.toml"
    cfg_path.write_text(
        f"""
[data]
root = "{path_for_toml(tmp_path / "data")}"

[orchestrator]
workers = 1
batch_size = 2

[tdx_protocol]
allow_mock = true

[[job.daily.waves]]
name = "reference"
parallel = true
steps = ["instruments", "trading_calendar"]

[[job.daily.waves]]
name = "bars"
parallel = false
steps = ["daily_bars", "compact", "derive_adj_factors", "audit"]

[job.init.phases]
names = ["phase1_reference"]
""",
        encoding="utf-8",
    )
    return load_config(cfg_path)
