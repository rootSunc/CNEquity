"""`BackfillRequest` validation reads what each dataset declares on its spec."""

from __future__ import annotations

import dataclasses
from datetime import date

import pytest

from cnequity.domain.datasets import DATASETS
from cnequity.orchestrator.backfill_request import (
    REPAIRS,
    BackfillRequest,
    BackfillRequestError,
    plan_modes,
    run_flags,
    validate,
    window,
)
from cnequity.run_scope import RunFlags


def _declaring(field: str) -> list[str]:
    return [name for name, spec in DATASETS.items() if getattr(spec, field)]


@pytest.mark.parametrize("dataset", sorted(DATASETS))
def test_source_override_is_whatever_the_spec_declares(dataset):
    allowed = DATASETS[dataset].backfill_source_overrides
    for source in allowed:
        validate(BackfillRequest(dataset, source=source))
    with pytest.raises(BackfillRequestError) as raised:
        validate(BackfillRequest(dataset, source="not-a-source"))
    # A dataset that takes some source names the bad option; one that takes
    # none lists the datasets that do.
    assert (raised.value.param_hint == "--source") == bool(allowed)


@pytest.mark.parametrize("dataset", sorted(DATASETS))
def test_refetch_and_parallel_dates_follow_the_spec(dataset):
    spec = DATASETS[dataset]
    refetch = BackfillRequest(dataset, refetch=True)
    if spec.backfill_refetch is None:
        with pytest.raises(BackfillRequestError, match="--refetch applies to"):
            validate(refetch)
    elif dataset not in {"futures_minute_bars"}:
        validate(refetch)
    parallel = BackfillRequest(dataset, workers=2)
    if spec.backfill_parallel_dates:
        validate(parallel)
    else:
        with pytest.raises(BackfillRequestError, match="--workers"):
            validate(parallel)


def test_declared_capabilities_are_the_ones_the_command_had():
    assert _declaring("backfill_parallel_dates") == ["margin_trading"]
    assert {name: DATASETS[name].backfill_refetch for name in _declaring("backfill_refetch")} == {
        "futures_bars": "derivatives",
        "option_bars": "derivatives",
        "financial_statement_items": "scope",
        "share_structure": "scope",
        "shareholder_counts": "scope",
        "top_holders": "scope",
        "fund_flow": "scope",
        "sector_bars": "checkpoint",
    }
    assert set(_declaring("backfill_symbols_in_window")) == {
        "daily_bars",
        "trading_status",
        "corporate_actions",
        "financial_statement_items",
        "share_structure",
    }


def test_refetch_reaches_the_run_the_way_the_spec_says():
    scope = BackfillRequest("fund_flow", refetch=True)
    assert window(scope)["refetch"] is True and run_flags(scope) == {}
    checkpoint = BackfillRequest("sector_bars", refetch=True)
    assert window(checkpoint)["refetch"] is False
    assert run_flags(checkpoint) == {"sector_bars_force": True}
    assert plan_modes(checkpoint) == ["force"]
    files = BackfillRequest("option_bars", refetch=True, source="tushare")
    assert run_flags(files) == {"option_source": "tushare", "derivatives_refresh": True}


def test_symbols_ride_the_window_only_where_the_step_reads_them():
    assert window(BackfillRequest("daily_bars", symbols=("600000.SH",)))["symbols"] == ["600000.SH"]
    assert "symbols" not in window(BackfillRequest("minute_bars", symbols=("600000.SH",)))


def test_every_repair_names_a_registered_dataset_and_real_run_flags():
    flag_names = {f.name for f in dataclasses.fields(RunFlags)}
    for name, mode in REPAIRS.items():
        assert mode.dataset in DATASETS, name
        assert set(mode.flags) <= flag_names, name
        request = BackfillRequest(
            mode.dataset, repair=name, ex_dates=(date(2014, 6, 30),), peer_review=True
        )
        validate(request)
        assert set(run_flags(request)) <= flag_names
        assert plan_modes(request) == [mode.plan_label]


def test_a_repair_on_the_wrong_dataset_or_without_its_dates_is_refused():
    with pytest.raises(BackfillRequestError, match="is for daily_bars"):
        validate(BackfillRequest("corporate_actions", repair="bar-amounts"))
    with pytest.raises(BackfillRequestError, match="--ex-dates"):
        validate(BackfillRequest("corporate_actions", repair="ex-dates"))


def test_transposed_window_and_outstanding_scope_are_refused():
    with pytest.raises(BackfillRequestError, match="on or before"):
        validate(BackfillRequest("daily_bars", start=date(2024, 2, 1), end=date(2024, 1, 1)))
    with pytest.raises(BackfillRequestError, match="debt ledger"):
        validate(BackfillRequest("daily_bars", outstanding=True, symbols=("600000.SH",)))
    with pytest.raises(BackfillRequestError, match="--profile delisted only accepts"):
        validate(BackfillRequest("daily_bars", profile="delisted", end=date(2024, 1, 1)))
