"""Publisher cross-checks (issue #10).

These are the only checks that can catch a vendor publishing on time, in the
right shape, with a wrong number — the shape of the `m2_yoy` defect in #3.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.quality import authority_checks as ac

TD = date(2026, 8, 1)
OBS = date(2026, 7, 31)


def _lake(tmp_path, *, pmi: float | None = None, status: dict[str, str] | None = None) -> Config:
    root = tmp_path / "data"
    if pmi is not None:
        part = root / "curated" / "macro_indicators" / f"obs_date={OBS.isoformat()}"
        part.mkdir(parents=True)
        pl.DataFrame(
            {
                "indicator_id": ["pmi_manufacturing"],
                "obs_date": [OBS],
                "value": [pmi],
                "frequency": ["monthly"],
                "source": ["eastmoney"],
                "data_version": ["v1"],
                "fetched_at": [datetime.now(timezone.utc)],
            }
        ).write_parquet(part / "p.parquet")
    if status is not None:
        part = root / "curated" / "trading_status" / f"trade_date={TD.isoformat()}"
        part.mkdir(parents=True)
        pl.DataFrame(
            {
                "symbol": list(status),
                "trade_date": [TD] * len(status),
                "is_trading": [True] * len(status),
                "status": list(status.values()),
                "source": ["eastmoney"] * len(status),
                "data_version": ["v1"] * len(status),
                "fetched_at": [datetime.now(timezone.utc)] * len(status),
            }
        ).write_parquet(part / "p.parquet")
    cfg = Config(data_root=root)
    cfg.sources = {"nbs": True, "exchange": True}
    return cfg


# --- PMI vs NBS --------------------------------------------------------------


def _publish(monkeypatch, value: float, obs: date = OBS):
    """Stand in for the NBS release; the check imports the adapter lazily."""
    import cn_market_lake.adapters.nbs.pmi_release as nbs

    monkeypatch.setattr(
        nbs,
        "fetch_latest_pmi",
        lambda **_kw: {"obs_date": obs, "value": value, "url": "https://example/release"},
    )


def test_matching_pmi_is_silent(monkeypatch, tmp_path):
    _publish(monkeypatch, 49.2)
    assert ac.macro_pmi_vs_nbs(_lake(tmp_path, pmi=49.2), TD) == []


def test_drifted_pmi_is_an_error(monkeypatch, tmp_path):
    _publish(monkeypatch, 49.2)
    findings = ac.macro_pmi_vs_nbs(_lake(tmp_path, pmi=51.7), TD)
    assert len(findings) == 1
    f = findings[0]
    assert f["check"] == "macro_pmi_vs_nbs"
    assert f["severity"] == "error"
    assert f["curated_value"] == 51.7
    assert f["published_value"] == 49.2
    assert f["source_url"] == "https://example/release"


def test_float_noise_is_not_drift(monkeypatch, tmp_path):
    _publish(monkeypatch, 49.2)
    assert ac.macro_pmi_vs_nbs(_lake(tmp_path, pmi=49.2000001), TD) == []


def test_a_month_the_run_has_not_reached_is_not_compared(monkeypatch, tmp_path):
    _publish(monkeypatch, 49.2, obs=date(2026, 9, 30))
    assert ac.macro_pmi_vs_nbs(_lake(tmp_path, pmi=49.2), TD) == []


def test_missing_curated_month_is_left_to_the_staleness_check(monkeypatch, tmp_path):
    _publish(monkeypatch, 49.2)
    assert ac.macro_pmi_vs_nbs(_lake(tmp_path), TD) == []


def test_unreachable_publisher_is_silent(monkeypatch, tmp_path):
    import cn_market_lake.adapters.nbs.pmi_release as nbs

    monkeypatch.setattr(nbs, "fetch_latest_pmi", lambda **_kw: None)
    assert ac.macro_pmi_vs_nbs(_lake(tmp_path, pmi=49.2), TD) == []


def test_pmi_check_is_off_without_the_source_flag(monkeypatch, tmp_path):
    def _boom(**_kw):
        raise AssertionError("must not reach the network when [sources.nbs] is absent")

    import cn_market_lake.adapters.nbs.pmi_release as nbs

    monkeypatch.setattr(nbs, "fetch_latest_pmi", _boom)
    cfg = _lake(tmp_path, pmi=49.2)
    cfg.sources = {}
    assert ac.macro_pmi_vs_nbs(cfg, TD) == []


# --- ST vs the exchanges -----------------------------------------------------


def _exchange(monkeypatch, names: dict[str, str]):
    import cn_market_lake.adapters.exchange.st_lists as ex

    monkeypatch.setattr(ex, "fetch_exchange_names", lambda **_kw: names)


def _universe(n: int, *, st_designated: int):
    syms = [f"{600000 + i:06d}.SH" for i in range(n)]
    names = {s: (f"ST公司{i}" if i < st_designated else f"公司{i}") for i, s in enumerate(syms)}
    return syms, names


def test_agreeing_labels_are_silent(monkeypatch, tmp_path):
    syms, names = _universe(50, st_designated=10)
    _exchange(monkeypatch, names)
    status = {s: ("st" if i < 10 else "normal") for i, s in enumerate(syms)}
    assert ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD) == []


def test_missing_labels_are_an_error(monkeypatch, tmp_path):
    syms, names = _universe(50, st_designated=10)
    _exchange(monkeypatch, names)
    status = {s: "normal" for s in syms}
    findings = ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD)
    assert len(findings) == 1
    assert findings[0]["designated_not_labeled"] == 10
    assert findings[0]["labeled_not_designated"] == 0


def test_labels_the_exchange_does_not_designate_are_an_error(monkeypatch, tmp_path):
    syms, names = _universe(50, st_designated=0)
    _exchange(monkeypatch, names)
    status = {s: ("st" if i < 9 else "normal") for i, s in enumerate(syms)}
    findings = ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD)
    assert findings[0]["labeled_not_designated"] == 9


def test_names_the_lake_does_not_carry_are_not_a_shortfall(monkeypatch, tmp_path):
    """The exchanges list a company until formal delisting; feeds drop it sooner.

    Measured 2026-08-01: SSE designated 600355 and 603388 ST while neither
    EastMoney nor TDX still listed them. Counting those would burn the tolerance
    permanently, so both directions compare only over the shared universe.
    """
    syms, names = _universe(20, st_designated=0)
    # Ten ST names the exchange lists and the lake has never heard of.
    for i in range(10):
        names[f"{900000 + i:06d}.SH"] = f"*ST退市{i}"
    _exchange(monkeypatch, names)
    status = {s: "normal" for s in syms}
    assert ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD) == []


def test_small_disagreement_is_tolerated_as_naming_lag(monkeypatch, tmp_path):
    syms, names = _universe(50, st_designated=10)
    _exchange(monkeypatch, names)
    keep = 10 - ac.ST_MAX_DISAGREEMENT
    status = {s: ("st" if i < keep else "normal") for i, s in enumerate(syms)}
    assert ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD) == []


def test_unreachable_exchanges_are_silent(monkeypatch, tmp_path):
    _exchange(monkeypatch, {})
    syms, _ = _universe(20, st_designated=0)
    status = {s: "normal" for s in syms}
    assert ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD) == []


def test_st_check_is_off_without_the_source_flag(monkeypatch, tmp_path):
    import cn_market_lake.adapters.exchange.st_lists as ex

    def _boom(**_kw):
        raise AssertionError("must not reach the network when [sources.exchange] is absent")

    monkeypatch.setattr(ex, "fetch_exchange_names", _boom)
    syms, _ = _universe(20, st_designated=0)
    cfg = _lake(tmp_path, status={s: "normal" for s in syms})
    cfg.sources = {}
    assert ac.st_labels_vs_exchange(cfg, TD) == []


# --- persistence -------------------------------------------------------------


def test_a_clean_run_still_leaves_evidence(monkeypatch, tmp_path):
    """A findings file cannot distinguish "checked, agreed" from "never checked"."""
    _publish(monkeypatch, 49.2)
    syms, names = _universe(20, st_designated=2)
    _exchange(monkeypatch, names)
    status = {s: ("st" if i < 2 else "normal") for i, s in enumerate(syms)}
    cfg = _lake(tmp_path, pmi=49.2, status=status)

    assert ac.run_authority_checks(cfg, TD) == []
    written = cfg.meta_root / "quality" / "source_diffs" / f"authority-{TD.isoformat()}.json"
    payload = json.loads(written.read_text(encoding="utf-8"))
    assert payload["kind"] == "authority_crosscheck"
    assert payload["checks"] == {
        "macro_pmi_vs_nbs": "agreed",
        "st_labels_vs_exchange": "agreed",
    }


def test_a_failing_publisher_does_not_break_the_run(monkeypatch, tmp_path):
    import cn_market_lake.adapters.nbs.pmi_release as nbs

    def _boom(**_kw):
        raise RuntimeError("site down")

    monkeypatch.setattr(nbs, "fetch_latest_pmi", _boom)
    _exchange(monkeypatch, {})
    cfg = _lake(tmp_path, pmi=49.2, status={})
    assert ac.run_authority_checks(cfg, TD) == []
    written = cfg.meta_root / "quality" / "source_diffs" / f"authority-{TD.isoformat()}.json"
    assert "error" in json.loads(written.read_text(encoding="utf-8"))["checks"]["macro_pmi_vs_nbs"]


@pytest.mark.parametrize("flag", [{}, {"nbs": False, "exchange": False}])
def test_audit_stays_offline_when_the_sources_are_off(tmp_path, flag):
    cfg = _lake(tmp_path, pmi=49.2)
    cfg.sources = flag
    # No monkeypatching: a network call here would be a real request.
    assert ac.run_authority_checks(cfg, TD) == []
    payload = json.loads(
        (cfg.meta_root / "quality" / "source_diffs" / f"authority-{TD.isoformat()}.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["checks"] == {
        "macro_pmi_vs_nbs": "skipped_disabled",
        "st_labels_vs_exchange": "skipped_disabled",
    }


def test_publisher_check_records_missing_curated_state(monkeypatch, tmp_path):
    _publish(monkeypatch, 49.2)
    cfg = _lake(tmp_path)

    assert ac.run_authority_checks(cfg, TD) == []
    payload = json.loads(
        (cfg.meta_root / "quality" / "source_diffs" / f"authority-{TD.isoformat()}.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["checks"] == {
        "macro_pmi_vs_nbs": "skipped_no_curated",
        "st_labels_vs_exchange": "skipped_no_curated",
    }
