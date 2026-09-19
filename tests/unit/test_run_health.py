"""An outage nobody's individual run was alarming enough to report.

The proxy died at 00:51 on 2026-09-19 and the news wire, which fires every
fifteen minutes, came back `degraded` twenty times in a row — each run losing
both EastMoney steps to `[Errno 61] Connection refused`, each filing its own
per-step finding and moving on. The audit stayed HEALTHY for five hours; a
person found it by reading a log.
"""

import sqlite3
from datetime import datetime, timedelta, timezone

from cnequity.config import Config
from cnequity.orchestrator.manifest import Manifest
from cnequity.quality.run_health import degraded_job_findings

NOW = datetime(2026, 9, 19, 6, 0, tzinfo=timezone.utc)


def _lake(tmp_path, runs: list[tuple[str, str, int]]) -> Config:
    """A lake whose manifest holds *runs* as (job, status, minutes before NOW)."""
    cfg = Config(data_root=tmp_path / "data")
    Manifest(cfg.manifest_path)
    con = sqlite3.connect(cfg.manifest_path)
    for index, (job, status, ago) in enumerate(runs):
        started = (NOW - timedelta(minutes=ago)).strftime("%Y-%m-%dT%H:%M:%S")
        con.execute(
            "insert into ingestion_runs (run_id, job_name, status, started_at, error_message)"
            " values (?, ?, ?, ?, ?)",
            (f"run-{index}", job, status, started, "[Errno 61] Connection refused"),
        )
    con.commit()
    con.close()
    return cfg


def test_a_run_of_degraded_runs_is_reported_as_one_outage(tmp_path):
    cfg = _lake(
        tmp_path,
        [("events:news_wire", "degraded", ago) for ago in (45, 30, 15)],
    )

    (finding,) = degraded_job_findings(cfg, now=NOW)

    assert finding["check"] == "job_run_streak"
    assert finding["severity"] == "warning"
    assert finding["job_name"] == "events:news_wire"
    assert finding["streak"] == 3
    assert "Connection refused" in finding["message"]


def test_a_recovery_ends_it(tmp_path):
    """Only the trailing streak counts; a job that works now is not an outage."""
    cfg = _lake(
        tmp_path,
        [
            ("events:news_wire", "degraded", 45),
            ("events:news_wire", "degraded", 30),
            ("events:news_wire", "degraded", 15),
            ("events:news_wire", "success", 5),
        ],
    )

    assert degraded_job_findings(cfg, now=NOW) == []


def test_two_misses_are_a_bad_minute_not_an_outage(tmp_path):
    cfg = _lake(tmp_path, [("events:news_wire", "degraded", ago) for ago in (30, 15)])

    assert degraded_job_findings(cfg, now=NOW) == []


def test_an_earlier_success_does_not_excuse_the_streak(tmp_path):
    """The outage began with healthy runs behind it, which is the normal case.

    A rule that asked for no success anywhere in the window would have stayed
    quiet through the whole five hours it exists to catch.
    """
    cfg = _lake(
        tmp_path,
        [
            ("events:news_wire", "success", 200),
            ("events:news_wire", "degraded", 45),
            ("events:news_wire", "degraded", 30),
            ("events:news_wire", "degraded", 15),
        ],
    )

    (finding,) = degraded_job_findings(cfg, now=NOW)

    assert finding["streak"] == 3


def test_runs_after_the_moment_asked_about_are_not_read(tmp_path):
    """Replaying a past outage must not see its own recovery."""
    cfg = _lake(
        tmp_path,
        [
            ("events:news_wire", "degraded", 45),
            ("events:news_wire", "degraded", 30),
            ("events:news_wire", "degraded", 15),
            ("events:news_wire", "success", -60),
        ],
    )

    assert degraded_job_findings(cfg, now=NOW)[0]["streak"] == 3


def test_one_job_failing_says_nothing_about_another(tmp_path):
    cfg = _lake(
        tmp_path,
        [("events:news_wire", "degraded", ago) for ago in (45, 30, 15)]
        + [("daily", "success", 20)],
    )

    findings = degraded_job_findings(cfg, now=NOW)

    assert [f["job_name"] for f in findings] == ["events:news_wire"]


def test_a_lake_with_no_manifest_is_silent(tmp_path):
    assert degraded_job_findings(Config(data_root=tmp_path / "data"), now=NOW) == []
