from datetime import date

from cnequity.config import Config
from cnequity.steps.finalize import step_derive_industry_index


def test_industry_index_empty_derive_is_retryable_warning(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.derive.industry_index.derive_industry_index",
        lambda *_args, **_kwargs: {
            "rows": 0,
            "note": "no priced returns in [2026-08-07, 2026-08-07]",
        },
    )

    result = step_derive_industry_index(cfg, date(2026, 8, 7), "run-empty", {})

    assert result["status"] == "warning"
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["check"] == "derived_empty"
    assert "no priced returns" in finding["message"]


def test_industry_index_without_membership_is_isolated_noop(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.derive.industry_index.derive_industry_index",
        lambda *_args, **_kwargs: {"rows": 0, "note": "no 申万 membership rows"},
    )

    result = step_derive_industry_index(cfg, date(2026, 8, 7), "run-no-membership", {})

    assert result == {"rows_read": 0, "rows_written": 0}


def test_industry_index_already_current_remains_success_noop(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.derive.industry_index.derive_industry_index",
        lambda *_args, **_kwargs: {"rows": 0, "note": "industry_index already current"},
    )

    result = step_derive_industry_index(cfg, date(2026, 8, 7), "run-current", {})

    assert result == {"rows_read": 0, "rows_written": 0}


def test_industry_index_daily_recomputes_full_history(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    calls: dict = {}

    def fake_derive(_config, **kwargs):
        calls.update(kwargs)
        return {"rows": 3, "note": "ok"}

    monkeypatch.setattr(
        "cnequity.derive.industry_index.derive_industry_index",
        fake_derive,
    )

    result = step_derive_industry_index(cfg, date(2026, 8, 7), "run-full", {})

    assert calls == {"full": True, "end": date(2026, 8, 7)}
    assert result["rows_written"] == 3


def test_small_adjustment_factor_failure_is_warning_and_retryable(tmp_path, monkeypatch):
    from cnequity.derive.adj_factors import AdjFactorsResult
    from cnequity.steps.finalize import step_derive_adj_factors

    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.derive.adj_factors.compute_adj_factors",
        lambda *_args, **_kwargs: AdjFactorsResult(
            rows=99,
            task_count=100,
            failed=["600001.SH:hfq"],
            findings=[],
        ),
    )

    result = step_derive_adj_factors(cfg, date(2026, 8, 7), "run-adj-warning", {})

    assert result["status"] == "warning"
    assert result["failed_tasks"] == 1
