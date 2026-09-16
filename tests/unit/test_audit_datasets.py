from datetime import date

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.datasets import PARTITION_COLS
from cnequity.domain.schemas import MOCK_SOURCE
from cnequity.quality.audit import run_audit
from cnequity.quality.dataset_checks import (
    audit_curated_dataset,
    check_partition_row_mutation,
)


def _bar_row(symbol: str, trade_date: date) -> dict:
    return {
        "symbol": symbol,
        "trade_date": trade_date,
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "volume": 1000,
        "amount": 10_500.0,
        "source": "tdx_protocol",
        "data_version": "v1",
        "fetched_at": f"{trade_date.isoformat()}T00:00:00+00:00",
    }


def _write_daily_bars_partition(cfg: Config, trade_date: date, symbols: list[str]) -> None:
    path = cfg.curated_root / "daily_bars" / f"trade_date={trade_date.isoformat()}"
    path.mkdir(parents=True)
    pl.DataFrame([_bar_row(sym, trade_date) for sym in symbols]).write_parquet(
        path / "part-merged.parquet"
    )


def test_audit_checks_all_partition_col_datasets(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    run_id = "run-all-datasets"
    trade_date = date(2024, 6, 28)

    run_audit(cfg, run_id, trade_date, {})

    import json

    payload = json.loads(
        (cfg.meta_root / "quality" / "findings" / f"{run_id}.json").read_text(encoding="utf-8")
    )
    exists_checks = {f["dataset"] for f in payload["findings"] if f.get("check") == "exists"}
    assert exists_checks == set(PARTITION_COLS.keys())
    # Optional datasets (source not yet wired) must not fail lake health alone.
    optional_exists = [
        f
        for f in payload["findings"]
        if f.get("check") == "exists" and f.get("dataset") == "economic_calendar"
    ]
    assert optional_exists and optional_exists[0]["severity"] == "info"
    required_exists = [
        f
        for f in payload["findings"]
        if f.get("check") == "exists" and f.get("dataset") == "daily_bars"
    ]
    assert required_exists and required_exists[0]["severity"] == "error"


def test_row_count_mutation_warns_on_partial_market_drop():
    finding = check_partition_row_mutation(
        "daily_bars",
        "trade_date",
        current_value=date(2024, 6, 28),
        previous_value=date(2024, 6, 27),
        current_stats={"rows": 2400, "symbols": 2400},
        previous_stats={"rows": 5000, "symbols": 5000},
    )
    assert finding is not None
    assert finding["check"] == "row_count_mutation"
    assert finding["severity"] == "warning"
    assert finding["row_ratio"] == pytest.approx(0.48)


def test_row_count_mutation_ignores_small_baselines():
    assert (
        check_partition_row_mutation(
            "trading_calendar",
            "trade_date",
            current_value=date(2024, 6, 28),
            previous_value=date(2024, 6, 27),
            current_stats={"rows": 1, "symbols": None},
            previous_stats={"rows": 1, "symbols": None},
        )
        is None
    )


def test_audit_row_count_mutation_detects_daily_bars_drop(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    trade_date = date(2024, 6, 28)
    prev_date = date(2024, 6, 27)

    prev_symbols = [f"600{i:03d}.SH" for i in range(100)]
    cur_symbols = [f"600{i:03d}.SH" for i in range(40)]
    _write_daily_bars_partition(cfg, prev_date, prev_symbols)
    _write_daily_bars_partition(cfg, trade_date, cur_symbols)

    findings = audit_curated_dataset(
        "daily_bars",
        "trade_date",
        cfg.curated_root / "daily_bars",
        trade_date,
    )
    mutation = [f for f in findings if f.get("check") == "row_count_mutation"]
    assert len(mutation) == 1
    assert mutation[0]["current_rows"] == 40
    assert mutation[0]["previous_rows"] == 100


def test_audit_flags_mock_rows_in_trade_date_partition(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    trade_date = date(2024, 6, 28)
    path = cfg.curated_root / "daily_bars" / f"trade_date={trade_date.isoformat()}"
    path.mkdir(parents=True)
    row = _bar_row("600519.SH", trade_date)
    row["source"] = MOCK_SOURCE
    pl.DataFrame([row]).write_parquet(path / "part-0.parquet")

    findings = audit_curated_dataset(
        "daily_bars",
        "trade_date",
        cfg.curated_root / "daily_bars",
        trade_date,
    )
    mock = [f for f in findings if f.get("check") == "mock_source"]
    assert len(mock) == 1
    assert mock[0]["severity"] == "error"


def test_audit_checks_pk_duplicates_beyond_sample_files(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    trade_date = date(2024, 6, 28)
    path = cfg.curated_root / "daily_bars" / f"trade_date={trade_date.isoformat()}"
    path.mkdir(parents=True)
    for index in range(20):
        pl.DataFrame([_bar_row(f"600{index:03d}.SH", trade_date)]).write_parquet(
            path / f"part-{index:02d}.parquet"
        )
    # The old audit sampled the first 20 files, so this duplicate was invisible.
    pl.DataFrame([_bar_row("600000.SH", trade_date)]).write_parquet(path / "part-20.parquet")

    findings = audit_curated_dataset(
        "daily_bars", "trade_date", cfg.curated_root / "daily_bars", trade_date
    )
    duplicate = [f for f in findings if f.get("check") == "pk_unique"]
    assert duplicate and duplicate[0]["message"].startswith("1 duplicate PK rows")


def test_audit_checks_required_nulls_across_partition(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    trade_date = date(2024, 6, 28)
    path = cfg.curated_root / "daily_bars" / f"trade_date={trade_date.isoformat()}"
    path.mkdir(parents=True)
    row = _bar_row("600519.SH", trade_date)
    row["close"] = None
    pl.DataFrame([row]).write_parquet(path / "part-0.parquet")

    findings = audit_curated_dataset(
        "daily_bars", "trade_date", cfg.curated_root / "daily_bars", trade_date
    )
    required = [f for f in findings if f.get("check") == "required_non_null"]
    assert required and required[0]["null_counts"] == {"close": 1}


def test_full_audit_scans_historical_schema_contract(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    old_date = date(2024, 6, 27)
    current_date = date(2024, 6, 28)

    old = _bar_row("600519.SH", old_date)
    old["close"] = -1.0
    _write_daily_bars_partition(cfg, old_date, ["600519.SH"])
    old_path = (
        cfg.curated_root
        / "daily_bars"
        / f"trade_date={old_date.isoformat()}"
        / "part-merged.parquet"
    )
    pl.DataFrame([old]).write_parquet(old_path)
    _write_daily_bars_partition(cfg, current_date, ["600519.SH"])

    light = audit_curated_dataset(
        "daily_bars",
        "trade_date",
        cfg.curated_root / "daily_bars",
        current_date,
    )
    assert not [f for f in light if f.get("check") == "schema_contract"]

    full = audit_curated_dataset(
        "daily_bars",
        "trade_date",
        cfg.curated_root / "daily_bars",
        current_date,
        full=True,
    )
    contract = [f for f in full if f.get("check") == "schema_contract"]
    assert len(contract) == 1
    assert contract[0]["invalid_files"] == 1
    assert contract[0]["sample"][0]["file"].endswith(
        f"trade_date={old_date.isoformat()}/part-merged.parquet"
    )


def test_full_audit_isolates_an_unreadable_historical_parquet(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    old_date = date(2024, 6, 27)
    current_date = date(2024, 6, 28)
    _write_daily_bars_partition(cfg, current_date, ["600519.SH"])
    broken = cfg.curated_root / "daily_bars" / f"trade_date={old_date.isoformat()}"
    broken.mkdir(parents=True)
    (broken / "part-broken.parquet").write_bytes(b"not a parquet file")

    findings = audit_curated_dataset(
        "daily_bars",
        "trade_date",
        cfg.curated_root / "daily_bars",
        current_date,
        full=True,
    )

    contract = [f for f in findings if f.get("check") == "schema_contract"]
    assert contract and contract[0]["invalid_files"] == 1
    assert contract[0]["sample"][0]["file"].endswith(
        f"trade_date={old_date.isoformat()}/part-broken.parquet"
    )
    assert any(
        f.get("check") == "row_count" and f["message"].startswith("1 rows") for f in findings
    )


def test_lake_health_reports_unreadable_parquet_without_aborting(tmp_path):
    from cnequity.quality.audit import lake_health

    cfg = Config(data_root=tmp_path / "data")
    broken = cfg.curated_root / "daily_bars" / "trade_date=2024-06-27"
    broken.mkdir(parents=True)
    (broken / "part-broken.parquet").write_bytes(b"not a parquet file")

    health = lake_health(cfg, date(2024, 6, 28))

    contract = [
        f
        for f in health["error_findings"]
        if f.get("dataset") == "daily_bars" and f.get("check") == "schema_contract"
    ]
    assert contract and contract[0]["invalid_files"] == 1
    skipped = [f for f in health["warning_findings"] if f.get("check") == "quality_checks_skipped"]
    assert skipped and skipped[0]["datasets"] == ["daily_bars"]
    assert any(
        blocker["code"] == "daily_bars_unreadable"
        for blocker in health["historical_universe_validity"]["blockers"]
    )


def test_lake_health_persists_informational_findings(tmp_path, monkeypatch):
    from cnequity.quality import audit as audit_module

    cfg = Config(data_root=tmp_path / "data")
    info = {
        "dataset": "index_bars",
        "severity": "info",
        "check": "index_bars_calendar_coverage",
        "source_limited": True,
        "message": "known source gap",
    }
    monkeypatch.setattr(audit_module, "_collect_lake_findings", lambda *args, **kwargs: [info])
    monkeypatch.setattr(audit_module, "run_source_diffs", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        "cnequity.quality.historical_validity.historical_universe_validity",
        lambda *args, **kwargs: {
            "window": {"start": None, "end": None},
            "universe_ready": False,
            "blockers": [],
        },
    )

    health = audit_module.lake_health(cfg, date(2024, 6, 28))

    assert health["warning_findings"] == []
    assert health["info_findings"] == [info]
    import json

    saved = json.loads(
        (cfg.meta_root / "quality" / "health-latest.json").read_text(encoding="utf-8")
    )
    assert saved["info_findings"] == [info]


def test_lake_health_preserves_source_limited_warning(tmp_path, monkeypatch):
    from cnequity.quality import audit as audit_module

    cfg = Config(data_root=tmp_path / "data")
    finding = {
        "dataset": "trading_status",
        "severity": "warning",
        "check": "trading_status_coverage_start",
        "st_evidence_verified": False,
        "st_evidence_unsupported_symbols": 1,
        "st_evidence_unsupported_exchange_counts": {"BJ": 1},
        "source_limited": True,
        "message": "historical ST source does not cover 1 BJ symbol",
    }
    monkeypatch.setattr(audit_module, "_collect_lake_findings", lambda *args, **kwargs: [finding])
    monkeypatch.setattr(audit_module, "run_source_diffs", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        "cnequity.quality.historical_validity.historical_universe_validity",
        lambda *args, **kwargs: {
            "window": {"start": None, "end": None},
            "universe_ready": False,
            "blockers": [],
        },
    )

    health = audit_module.lake_health(cfg, date(2024, 6, 28))

    assert health["warning_findings"][0]["source_limited"] is True


def test_lake_health_persists_selected_research_universe(tmp_path, monkeypatch):
    from cnequity.quality import audit as audit_module

    cfg = Config(data_root=tmp_path / "data")
    observed: dict[str, str] = {}
    monkeypatch.setattr(audit_module, "_collect_lake_findings", lambda *args, **kwargs: [])
    monkeypatch.setattr(audit_module, "run_source_diffs", lambda *args, **kwargs: [])

    def _validity(*args, **kwargs):
        observed["universe"] = kwargs["universe"]
        return {
            "universe": kwargs["universe"],
            "window": {"start": None, "end": None},
            "universe_ready": True,
            "blockers": [],
        }

    monkeypatch.setattr(
        "cnequity.quality.historical_validity.historical_universe_validity", _validity
    )

    health = audit_module.lake_health(cfg, date(2024, 6, 28), research_universe="all_a_sh_sz")

    assert observed == {"universe": "all_a_sh_sz"}
    assert health["historical_universe"] == "all_a_sh_sz"
    assert health["historical_universe_validity"]["universe"] == "all_a_sh_sz"


def test_lake_health_keeps_all_a_st_baseline_when_research_is_scoped(tmp_path, monkeypatch):
    from cnequity.quality import audit as audit_module

    cfg = Config(data_root=tmp_path / "data")
    st_finding = {
        "dataset": "trading_status",
        "check": "trading_status_coverage_start",
        "severity": "warning",
        "st_evidence_verified": False,
        "st_evidence_coverage_start": "2001-01-01",
        "st_evidence_coverage_end": "2026-08-21",
        "st_evidence_supported_symbols": 5547,
        "st_evidence_unsupported_symbols": 580,
        "st_evidence_unsupported_exchange_counts": {"BJ": 580},
        "st_evidence_receipt_reason": "unsupported_exchange_symbols",
    }
    monkeypatch.setattr(
        audit_module, "_collect_lake_findings", lambda *args, **kwargs: [st_finding]
    )
    monkeypatch.setattr(audit_module, "run_source_diffs", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        "cnequity.quality.historical_validity.historical_universe_validity",
        lambda *args, **kwargs: {
            "universe": kwargs["universe"],
            "window": {"start": None, "end": None},
            "universe_ready": True,
            "blockers": [],
        },
    )

    health = audit_module.lake_health(cfg, date(2024, 6, 28), research_universe="all_a_sh_sz")

    baseline = health["historical_all_a_st_evidence"]
    assert baseline["verified"] is False
    assert baseline["unsupported_symbols"] == 580
    assert baseline["unsupported_exchange_counts"] == {"BJ": 580}


def test_disabled_intraday_captures_do_not_audit_historical_files(tmp_path, monkeypatch):
    from cnequity.quality import audit as audit_module

    cfg = Config(data_root=tmp_path / "data")
    cfg.minute_bars_enabled = False
    cfg.trade_ticks_enabled = False
    monkeypatch.setattr(
        audit_module,
        "minute_bars_findings",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("disabled minute scan")),
    )
    monkeypatch.setattr(
        audit_module,
        "trade_ticks_findings",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("disabled tick scan")),
    )

    assert audit_module._optional_intraday_findings(cfg, date(2024, 6, 28)) == []


def test_enabled_intraday_captures_keep_their_quality_checks(tmp_path, monkeypatch):
    from cnequity.quality import audit as audit_module

    cfg = Config(data_root=tmp_path / "data")
    cfg.minute_bars_enabled = True
    cfg.minute_bars_frequencies = ("1m",)
    cfg.trade_ticks_enabled = True
    monkeypatch.setattr(
        audit_module,
        "minute_bars_findings",
        lambda *args, **kwargs: [{"check": "minute"}],
    )
    monkeypatch.setattr(
        audit_module,
        "trade_ticks_findings",
        lambda *args, **kwargs: [{"check": "ticks"}],
    )

    assert audit_module._optional_intraday_findings(cfg, date(2024, 6, 28)) == [
        {"check": "minute"},
        {"check": "ticks"},
    ]


def test_run_audit_reports_unreadable_historical_parquet_before_scans(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    current_date = date(2024, 6, 28)
    _write_daily_bars_partition(cfg, current_date, ["600519.SH"])
    broken = cfg.curated_root / "daily_bars" / "trade_date=2024-06-27"
    broken.mkdir(parents=True)
    (broken / "part-broken.parquet").write_bytes(b"not a parquet file")

    run_audit(cfg, "run-broken-history", current_date, {})

    import json

    payload = json.loads(
        (cfg.meta_root / "quality" / "findings" / "run-broken-history.json").read_text(
            encoding="utf-8"
        )
    )
    contract = [
        f
        for f in payload["findings"]
        if f.get("dataset") == "daily_bars" and f.get("check") == "schema_contract"
    ]
    assert contract and any(f["unreadable_files"] == 1 for f in contract)


def test_historical_contract_requires_core_fields_but_allows_nullable_evolution(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    trade_date = date(2024, 6, 28)
    path = cfg.curated_root / "daily_bars" / f"trade_date={trade_date.isoformat()}"
    path.mkdir(parents=True)
    row = _bar_row("600519.SH", trade_date)
    row.pop("amount")  # nullable legacy column added after the original file
    pl.DataFrame([row]).write_parquet(path / "part-0.parquet")
    pl.DataFrame([_bar_row("600520.SH", trade_date)]).write_parquet(path / "part-1.parquet")

    findings = audit_curated_dataset(
        "daily_bars",
        "trade_date",
        cfg.curated_root / "daily_bars",
        trade_date,
        full=True,
    )
    assert not [f for f in findings if f.get("check") == "schema_contract"]

    row.pop("close")
    pl.DataFrame([row]).write_parquet(path / "part-0.parquet")
    findings = audit_curated_dataset(
        "daily_bars",
        "trade_date",
        cfg.curated_root / "daily_bars",
        trade_date,
        full=True,
    )
    contract = [f for f in findings if f.get("check") == "schema_contract"]
    assert contract and "missing required columns" in contract[0]["sample"][0]["message"]


def test_run_audit_persists_source_diff_findings_in_run_file(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    source_finding = {
        "dataset": "daily_bars",
        "check": "backup_coverage_gap",
        "severity": "warning",
        "message": "backup is missing one primary key",
    }
    monkeypatch.setattr(
        "cnequity.quality.audit.run_source_diffs",
        lambda *args, **kwargs: [source_finding],
    )

    count = run_audit(cfg, "run-source-diff", date(2024, 6, 28), {})

    import json

    payload = json.loads(
        (cfg.meta_root / "quality" / "findings" / "run-source-diff.json").read_text(
            encoding="utf-8"
        )
    )
    assert source_finding in payload["findings"]
    assert count == len(payload["findings"])


def test_full_health_includes_a_fresh_source_diff(tmp_path, monkeypatch):
    from cnequity.quality.audit import lake_health

    cfg = Config(data_root=tmp_path / "data")
    source_finding = {
        "dataset": "daily_bars",
        "check": "price_drift",
        "severity": "warning",
        "message": "primary and backup differ",
    }
    monkeypatch.setattr("cnequity.quality.audit._collect_lake_findings", lambda *a, **k: [])
    monkeypatch.setattr(
        "cnequity.quality.audit.run_source_diffs",
        lambda *args, **kwargs: [source_finding],
    )
    monkeypatch.setattr(
        "cnequity.quality.historical_validity.historical_universe_validity",
        lambda *args, **kwargs: {
            "universe_ready": True,
            "window": {"start": None, "end": None},
            "blockers": [],
        },
    )

    health = lake_health(cfg, date(2024, 6, 28))

    assert source_finding in health["warning_findings"]


def test_full_health_anchors_observations_to_last_trading_day(tmp_path, monkeypatch):
    from cnequity.quality.audit import lake_health

    cfg = Config(data_root=tmp_path / "data")
    calendar_day = date(2024, 6, 29)  # Saturday
    last_trading_day = date(2024, 6, 28)
    observed: dict[str, date] = {}

    monkeypatch.setattr(
        "cnequity.quality.audit._last_trading_day",
        lambda _config, _trade_date: last_trading_day,
    )
    monkeypatch.setattr(
        "cnequity.quality.audit._collect_lake_findings",
        lambda _config, trade_date, *_args, **_kwargs: (
            observed.setdefault("findings", trade_date) and []
        ),
    )
    monkeypatch.setattr(
        "cnequity.quality.audit.run_source_diffs",
        lambda _config, _run_id, trade_date: observed.setdefault("source_diff", trade_date) and [],
    )
    monkeypatch.setattr(
        "cnequity.quality.historical_validity.historical_universe_validity",
        lambda *args, **kwargs: {
            "universe_ready": True,
            "window": {"start": None, "end": None},
            "blockers": [],
        },
    )

    health = lake_health(cfg, calendar_day)

    assert observed == {"findings": last_trading_day, "source_diff": last_trading_day}
    assert health["trade_date"] == calendar_day.isoformat()
    assert health["last_trading_day"] == last_trading_day.isoformat()


def test_a_lagging_dataset_audits_its_newest_partition_not_the_whole_history(tmp_path):
    """The bounded daily audit must stay bounded when a dataset falls behind.

    With no partition covering the audited day the scan fell back to the whole
    dataset — so the *cheap* per-run audit became the expensive one. On the
    real lake `trade_ticks` had stopped in August, and every daily run counted
    rows across all 6 GB of it: 18 minutes for a capture nobody was ingesting.
    """
    cfg = Config(data_root=tmp_path / "data")
    for day in (date(2024, 6, 25), date(2024, 6, 26), date(2024, 6, 27)):
        _write_daily_bars_partition(cfg, day, ["600519.SH"])
    root = cfg.curated_root / "daily_bars"

    scanned: list[list] = []
    import cnequity.quality.dataset_checks as checks

    real = checks.scan_parquet_files

    def spy(files, *args, **kwargs):
        scanned.append(list(files))
        return real(files, *args, **kwargs)

    checks.scan_parquet_files = spy
    try:
        # Audited day is long after the last partition.
        audit_curated_dataset("daily_bars", "trade_date", root, date(2026, 9, 15), full=False)
    finally:
        checks.scan_parquet_files = real

    assert scanned, "expected a bounded file scan"
    widest = max(len(files) for files in scanned)
    assert widest == 1, f"audited {widest} files; only one partition at a time should be read"
    read = {str(files[0]) for files in scanned if files}
    # The newest partition, plus the one before it for the row-mutation
    # comparison. Never the 2024-06-25 file, and never the whole dataset.
    assert any("2024-06-27" in path for path in read)
    assert not any("2024-06-25" in path for path in read)


def test_the_current_partition_still_wins_when_one_covers_the_day(tmp_path):
    """The fallback must not displace a real current partition."""
    cfg = Config(data_root=tmp_path / "data")
    for day in (date(2024, 6, 26), date(2024, 6, 27)):
        _write_daily_bars_partition(cfg, day, ["600519.SH"])
    root = cfg.curated_root / "daily_bars"

    scanned: list[list] = []
    import cnequity.quality.dataset_checks as checks

    real = checks.scan_parquet_files

    def spy(files, *args, **kwargs):
        scanned.append(list(files))
        return real(files, *args, **kwargs)

    checks.scan_parquet_files = spy
    try:
        audit_curated_dataset("daily_bars", "trade_date", root, date(2024, 6, 26), full=False)
    finally:
        checks.scan_parquet_files = real

    assert all("2024-06-26" in str(files[0]) for files in scanned if files)


def test_full_audit_still_reads_every_historical_file(tmp_path):
    """The weekly sweep is what looks at everything; it must not be narrowed."""
    cfg = Config(data_root=tmp_path / "data")
    for day in (date(2024, 6, 25), date(2024, 6, 26), date(2024, 6, 27)):
        _write_daily_bars_partition(cfg, day, ["600519.SH"])
    root = cfg.curated_root / "daily_bars"

    scanned: list[list] = []
    import cnequity.quality.dataset_checks as checks

    real = checks.scan_parquet_files

    def spy(files, *args, **kwargs):
        scanned.append(list(files))
        return real(files, *args, **kwargs)

    checks.scan_parquet_files = spy
    try:
        audit_curated_dataset("daily_bars", "trade_date", root, date(2026, 9, 15), full=True)
    finally:
        checks.scan_parquet_files = real

    assert max(len(files) for files in scanned) == 3


def test_a_stale_dataset_shrink_is_reported_without_accusing_the_data(tmp_path):
    """Nine of sixteen audit warnings were restatements of staleness.

    `row_count_mutation` exists to catch a partition that lost rows it used to
    have. A dataset whose schedule group is not being run shrinks for a
    different reason, and `cne status --datasets` already names that reason
    along with the group that owns it. Keep the observation, drop the
    accusation.
    """
    cfg = Config(data_root=tmp_path / "data")
    _write_daily_bars_partition(cfg, date(2024, 6, 26), [f"60{i:04d}.SH" for i in range(400)])
    _write_daily_bars_partition(cfg, date(2024, 6, 27), ["600519.SH"])
    root = cfg.curated_root / "daily_bars"

    fresh = audit_curated_dataset("daily_bars", "trade_date", root, date(2024, 6, 27))
    behind = audit_curated_dataset("daily_bars", "trade_date", root, date(2024, 6, 27), stale=True)

    hot = next(f for f in fresh if f["check"] == "row_count_mutation")
    cold = next(f for f in behind if f["check"] == "row_count_mutation")

    assert hot["severity"] == "warning"
    assert "stale_dataset" not in hot
    # Same finding, different claim about what it means.
    assert cold["severity"] == "info"
    assert cold["stale_dataset"] is True
    assert "cne status --datasets" in cold["message"]
