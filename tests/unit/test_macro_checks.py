"""macro_indicators staleness and revision checks (issue #10)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from cn_market_lake.config import Config
from cn_market_lake.quality.macro_checks import (
    MONTHLY_STALE_DAYS,
    macro_revision_findings,
    macro_staleness_findings,
)


def _lake(tmp_path, rows: list[dict]) -> Config:
    root = tmp_path / "data"
    for row in rows:
        part = root / "curated" / "macro_indicators" / f"obs_date={row['obs_date'].isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "indicator_id": [row["indicator_id"]],
                "obs_date": [row["obs_date"]],
                "value": [row["value"]],
                "frequency": ["monthly"],
                "source": [row.get("source", "eastmoney")],
                "data_version": ["v1"],
                "fetched_at": [datetime.now(timezone.utc)],
            }
        ).write_parquet(part / f"{row['indicator_id']}.parquet")
    return Config(data_root=root)


def _incoming(rows: list[tuple[str, date, float]]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "indicator_id": [r[0] for r in rows],
            "obs_date": [r[1] for r in rows],
            "value": [r[2] for r in rows],
        }
    )


# --- staleness ---------------------------------------------------------------


def test_fresh_monthly_series_is_not_flagged(tmp_path):
    cfg = _lake(
        tmp_path,
        [{"indicator_id": "pmi_manufacturing", "obs_date": date(2026, 7, 31), "value": 49.2}],
    )
    assert macro_staleness_findings(cfg, date(2026, 8, 1)) == []


def test_stalled_publisher_is_flagged(tmp_path):
    """A feed that stops looks identical to a healthy one in curated.

    Every run refetches the full history and dedupes on the key, so the old rows
    stay and no step fails; only the lag between newest obs and the run date
    shows it.
    """
    cfg = _lake(
        tmp_path,
        [{"indicator_id": "pmi_manufacturing", "obs_date": date(2026, 1, 31), "value": 49.3}],
    )
    findings = macro_staleness_findings(cfg, date(2026, 8, 1))
    assert len(findings) == 1
    f = findings[0]
    assert f["check"] == "macro_indicator_stale"
    assert f["severity"] == "warning"
    assert f["indicator_id"] == "pmi_manufacturing"
    assert f["lag_days"] == (date(2026, 8, 1) - date(2026, 1, 31)).days


def test_social_financing_tolerates_the_pboc_release_cadence(tmp_path):
    """The PBOC publishes 社融 mid-following-month — measured, not assumed.

    On 2026-08-01 its newest month was 2026-06, which must not be a finding or
    the check cries wolf every single day.
    """
    cfg = _lake(
        tmp_path,
        [
            {
                "indicator_id": "social_financing",
                "obs_date": date(2026, 6, 30),
                "value": 33645.0,
                "source": "pboc",
            }
        ],
    )
    assert macro_staleness_findings(cfg, date(2026, 8, 1)) == []
    # ...but missing a further release cycle is not routine.
    assert MONTHLY_STALE_DAYS["social_financing"] < (date(2026, 10, 1) - date(2026, 6, 30)).days
    assert macro_staleness_findings(cfg, date(2026, 10, 1))


def test_daily_series_are_left_to_the_freshness_checks(tmp_path):
    cfg = _lake(
        tmp_path,
        [{"indicator_id": "cnbond_yield_10y", "obs_date": date(2025, 1, 31), "value": 2.25}],
    )
    assert macro_staleness_findings(cfg, date(2026, 8, 1)) == []


def test_staleness_on_an_empty_lake_is_silent(tmp_path):
    assert macro_staleness_findings(Config(data_root=tmp_path / "data"), date(2026, 8, 1)) == []


# --- revisions ---------------------------------------------------------------


def test_unchanged_values_produce_no_finding(tmp_path):
    cfg = _lake(tmp_path, [{"indicator_id": "m2_yoy", "obs_date": date(2026, 6, 30), "value": 8.0}])
    incoming = _incoming([("m2_yoy", date(2026, 6, 30), 8.0)])
    assert macro_revision_findings(cfg, incoming, date(2026, 8, 1)) == []


def test_restated_value_is_recorded_before_the_overwrite(tmp_path):
    """curated keeps the new number; the finding is the only trace of the old one."""
    cfg = _lake(tmp_path, [{"indicator_id": "m2_yoy", "obs_date": date(2026, 6, 30), "value": 8.0}])
    incoming = _incoming([("m2_yoy", date(2026, 6, 30), 8.4)])
    findings = macro_revision_findings(cfg, incoming, date(2026, 8, 1))
    assert len(findings) == 1
    f = findings[0]
    assert f["check"] == "macro_value_revised"
    assert f["revised"] == 1
    assert f["indicators"] == ["m2_yoy"]
    assert "8.0" in f["message"] and "8.4" in f["message"]


@pytest.mark.parametrize(
    ("old", "new", "severity"),
    [
        (8.0, 8.4, "warning"),  # 5% — material
        (8.0, 8.01, "info"),  # rounding-scale restatement
    ],
)
def test_revision_severity_tracks_magnitude(tmp_path, old, new, severity):
    cfg = _lake(tmp_path, [{"indicator_id": "m2_yoy", "obs_date": date(2026, 6, 30), "value": old}])
    incoming = _incoming([("m2_yoy", date(2026, 6, 30), new)])
    assert macro_revision_findings(cfg, incoming, date(2026, 8, 1))[0]["severity"] == severity


def test_new_months_are_not_revisions(tmp_path):
    cfg = _lake(tmp_path, [{"indicator_id": "m2_yoy", "obs_date": date(2026, 6, 30), "value": 8.0}])
    incoming = _incoming([("m2_yoy", date(2026, 7, 31), 8.2)])
    assert macro_revision_findings(cfg, incoming, date(2026, 8, 1)) == []


def test_revision_check_on_a_first_run_is_silent(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    incoming = _incoming([("m2_yoy", date(2026, 6, 30), 8.0)])
    assert macro_revision_findings(cfg, incoming, date(2026, 8, 1)) == []
