"""The step registry states what each step writes, once."""

from __future__ import annotations

import pytest

import cnequity.steps  # noqa: F401 — register the built-in steps
from cnequity.domain.datasets import DATASETS
from cnequity.orchestrator.deps import committed_reads_in_run
from cnequity.orchestrator.engine import JobEngine
from cnequity.orchestrator.registry import (
    STEP_REGISTRY,
    DuplicateStepError,
    receipt_dataset,
    register_step,
)

# Datasets no registered step writes, and what does.
EXTERNALLY_PRODUCED = {
    "delisting_events": "steps/delisted.py, from the delisted maintenance commands",
    "minute_bars_15m": "`cne derive minute_bars_15m`",
    "minute_bars_30m": "`cne derive minute_bars_30m`",
    "minute_bars_60m": "`cne derive minute_bars_60m`",
}

# The receipt keys the engine hard-coded before the registry carried outputs.
_LEGACY_RECEIPT_KEYS = {
    "derive_adj_factors": "adj_factors",
    "derive_industry_index": "industry_index",
    "derive_futures_continuous": "futures_continuous",
    "derive_option_greeks": "option_greeks",
}


def test_receipt_keys_are_unchanged_for_every_step():
    for name in STEP_REGISTRY:
        assert receipt_dataset(name) == _LEGACY_RECEIPT_KEYS.get(name, name), name
        assert JobEngine._step_dataset(name, {}) == receipt_dataset(name)


def test_an_explicit_result_dataset_still_wins():
    assert JobEngine._step_dataset("derive_adj_factors", {"dataset": "x"}) == "x"


def test_every_dataset_has_a_producer():
    produced = {dataset for entry in STEP_REGISTRY.values() for dataset in entry.outputs}
    missing = sorted(set(DATASETS) - produced - set(EXTERNALLY_PRODUCED))
    assert missing == [], f"no step declares these in `outputs`: {missing}"
    # And the allowlist cannot quietly go stale.
    assert not (produced & set(EXTERNALLY_PRODUCED))


def test_outputs_and_inputs_name_registered_datasets():
    for name, entry in STEP_REGISTRY.items():
        assert set(entry.outputs) <= set(DATASETS), name
        assert set(entry.input_datasets) <= set(DATASETS), name


@pytest.mark.parametrize(
    ("step", "outputs"),
    [
        ("daily_bars", ("daily_bars",)),
        ("daily_bars_history", ("daily_bars",)),
        ("daily_bars_delisted", ("daily_bars",)),
        ("corporate_actions_sweep", ("corporate_actions",)),
        ("trading_status_derive", ("trading_status",)),
        ("derive_adj_factors", ("adj_factors",)),
        ("compact", ()),
        ("audit", ()),
        ("financial_statement_tdx_peer", ()),
        ("ths_official_snapshot", ()),
    ],
)
def test_declared_outputs(step, outputs):
    assert STEP_REGISTRY[step].outputs == outputs


def test_a_second_function_cannot_take_a_registered_name():
    def impostor(config, trade_date, run_id, context):
        return {}

    with pytest.raises(DuplicateStepError, match="daily_bars"):
        register_step("daily_bars")(impostor)
    assert STEP_REGISTRY["daily_bars"].fn is not impostor


def test_re_registering_the_same_function_is_harmless():
    entry = STEP_REGISTRY["daily_bars"]
    register_step(
        "daily_bars",
        depends_on=list(entry.depends_on),
        group=entry.group,
        description=entry.description,
        parallelizable=entry.parallelizable,
        requires_workers=entry.requires_workers,
        input_datasets=entry.input_datasets,
        outputs=entry.outputs,
    )(entry.fn)
    assert STEP_REGISTRY["daily_bars"].fn is entry.fn


def test_an_unregistered_dataset_is_refused_at_registration():
    with pytest.raises(KeyError, match="no_such_dataset"):
        register_step("p1_probe_step", outputs=("no_such_dataset",))
    assert "p1_probe_step" not in STEP_REGISTRY


def test_committed_reads_in_the_same_run_are_reported():
    steps = ["instruments", "daily_bars", "trading_status_derive", "compact", "derive_adj_factors"]
    assert committed_reads_in_run(steps) == [
        ("trading_status_derive", "daily_bars", "daily_bars"),
        ("trading_status_derive", "instruments", "instruments"),
    ]


def test_steps_after_compact_see_this_runs_publication():
    # derive_adj_factors reads daily_bars, but only after compact published it.
    assert committed_reads_in_run(["daily_bars", "compact", "derive_adj_factors"]) == []
