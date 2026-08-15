from datetime import date

import pytest

import cn_market_lake.steps  # noqa: F401 — register steps
from cn_market_lake.orchestrator.deps import UnknownStepError
from cn_market_lake.orchestrator.engine import JobEngine
from cn_market_lake.storage.layout import init_data_layout


def test_run_job_rejects_unknown_steps(config):
    init_data_layout(config)
    engine = JobEngine(config)
    with pytest.raises(UnknownStepError, match="not_registered"):
        engine.run_job("daily", date(2024, 6, 28), steps=["not_registered"])
