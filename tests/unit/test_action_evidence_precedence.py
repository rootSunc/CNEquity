"""A later vendor sweep cannot displace corporate-action evidence it lacks."""

from datetime import date, datetime, timedelta, timezone

import polars as pl

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.domain.action_evidence import clear_invalid_payment_evidence
from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps.finalize import step_compact
from cnequity.storage import StagingWriter

THEN = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _row(symbol: str, day: date, action: str, *, source: str, fetched: datetime) -> dict:
    return {
        "symbol": symbol,
        "ex_date": day,
        "action_type": action,
        "cash_dividend": 0.0,
        "bonus_ratio": 0.0,
        "transfer_ratio": 0.0,
        "split_factor": 1.0,
        "allotment_ratio": None,
        "allotment_price": None,
        "payment_date": None,
        "payment_source": None,
        "source": source,
        "data_version": "v1",
        "fetched_at": fetched,
    }


def _publish(cfg: Config, run_id: str, rows: list[dict]) -> dict:
    manifest = Manifest(cfg.manifest_path)
    manifest.start_batch(run_id, "actions", "corporate_actions", "corporate_actions")
    StagingWriter(cfg.staging_root).write_batch(
        "corporate_actions", run_id, "actions", pl.DataFrame(rows)
    )
    manifest.finish_batch(run_id, "actions", "success", rows_written=len(rows))
    return step_compact(cfg, date(2024, 6, 28), run_id, {})


def _published(cfg: Config) -> dict[tuple, dict]:
    from cnequity.query import catalog_read, load

    catalog_read.close_all()
    rows = load("corporate_actions", config=cfg).to_dicts()
    return {(r["symbol"], r["ex_date"], r["action_type"]): r for r in rows}


def test_vendor_restatement_cannot_displace_issuer_settled_cash(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    reviewed = _row("688320.SH", date(2024, 6, 14), "cash_dividend", source="cninfo", fetched=THEN)
    reviewed.update(
        cash_dividend=0.11,
        payment_date=date(2024, 6, 14),
        payment_source="issuer_notice:1220289873:A:issuer_reviewed_cash_corrected",
    )
    _publish(cfg, "issuer", [reviewed])
    vendor = {**reviewed, "source": "tdx_protocol", "fetched_at": THEN + timedelta(days=2)}
    vendor.update(cash_dividend=0.11038000583648681, payment_date=None, payment_source=None)
    unrelated = _row(
        "600000.SH", date(2024, 6, 18), "cash_dividend", source="tdx_protocol", fetched=THEN
    )
    unrelated["cash_dividend"] = 0.2
    result = _publish(cfg, "vendor", [vendor, unrelated])

    assert not result.get("context_updates", {}).get("compact_skipped_datasets")
    rows = _published(cfg)
    kept = rows[("688320.SH", date(2024, 6, 14), "cash_dividend")]
    assert kept["cash_dividend"] == 0.11
    assert kept["payment_date"] == date(2024, 6, 14)
    # The sweep's other rows still publish; nothing is quarantined.
    assert rows[("600000.SH", date(2024, 6, 18), "cash_dividend")]["cash_dividend"] == 0.2


def test_reviewed_bj_transfer_survives_a_later_duplicate_bonus(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    reviewed = _row("920405.BJ", date(2022, 5, 23), "bonus", source="eastmoney", fetched=THEN)
    _publish(cfg, "issuer-stock", [reviewed])
    vendor = {**reviewed, "source": "tdx_protocol", "fetched_at": THEN + timedelta(days=1)}
    vendor["bonus_ratio"] = 1.0
    _publish(cfg, "vendor-stock", [vendor])
    assert _published(cfg)[("920405.BJ", date(2022, 5, 23), "bonus")]["bonus_ratio"] == 0.0


def test_issuer_notice_replaces_vendor_row_and_later_issuer_correction_wins(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    vendor = _row(
        "688320.SH", date(2024, 6, 14), "cash_dividend", source="tdx_protocol", fetched=THEN
    )
    vendor["cash_dividend"] = 0.11038000583648681
    _publish(cfg, "vendor-initial", [vendor])
    issuer = {**vendor, "source": "cninfo", "fetched_at": THEN + timedelta(days=1)}
    issuer.update(
        cash_dividend=0.11,
        payment_date=date(2024, 6, 14),
        payment_source="issuer_notice:first:A",
    )
    _publish(cfg, "issuer-first", [issuer])
    corrected = {**issuer, "fetched_at": THEN + timedelta(days=2)}
    corrected.update(payment_date=date(2024, 6, 17), payment_source="issuer_notice:second:A")
    _publish(cfg, "issuer-second", [corrected])
    kept = _published(cfg)[("688320.SH", date(2024, 6, 14), "cash_dividend")]
    assert (kept["cash_dividend"], kept["payment_source"]) == (0.11, "issuer_notice:second:A")


def test_vendor_payment_date_is_kept_until_an_issuer_notice_arrives(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    vendor = _row("600000.SH", date(2024, 6, 14), "cash_dividend", source="baostock", fetched=THEN)
    vendor.update(
        cash_dividend=0.11038000583648681,
        payment_date=date(2024, 6, 18),
        payment_source="baostock:dividPayDate",
    )
    _publish(cfg, "vendor-date", [vendor])
    weak = {**vendor, "source": "tdx_protocol", "fetched_at": THEN + timedelta(days=1)}
    weak.update(cash_dividend=0.11, payment_date=None, payment_source=None)
    _publish(cfg, "weak-refresh", [weak])
    key = ("600000.SH", date(2024, 6, 14), "cash_dividend")
    assert _published(cfg)[key]["payment_source"] == "baostock:dividPayDate"

    issuer = {**weak, "source": "cninfo", "fetched_at": THEN + timedelta(days=2)}
    issuer.update(payment_date=date(2024, 6, 14), payment_source="issuer_notice:notice:A")
    _publish(cfg, "issuer-reviewed", [issuer])
    assert _published(cfg)[key]["payment_source"] == "issuer_notice:notice:A"


def test_payment_before_ex_date_is_not_evidence(tmp_path, monkeypatch):
    typo = _row("002627.SZ", date(2016, 7, 6), "cash_dividend", source="cninfo", fetched=THEN)
    typo.update(
        cash_dividend=0.15,
        payment_date=date(2015, 7, 6),
        payment_source="issuer_notice:1202431391:pages2:A",
    )
    assert (
        clear_invalid_payment_evidence(pl.DataFrame([typo])).row(0, named=True)["payment_source"]
        is None
    )

    # Even unsanitised, an invalid issuer date ranks below a valid vendor date
    # and is never carried onto a newer row of the same event.
    vendor = {**typo, "source": "baostock", "fetched_at": THEN - timedelta(days=1)}
    vendor.update(payment_date=date(2016, 7, 6), payment_source="baostock:dividPayDate")
    kept = dedupe_by_primary_key(pl.DataFrame([vendor, typo]), "corporate_actions")
    assert kept["payment_source"].item() == "baostock:dividPayDate"
    bare = {
        **typo,
        "payment_date": None,
        "payment_source": None,
        "fetched_at": THEN + timedelta(days=1),
    }
    stale = {**typo, "fetched_at": THEN - timedelta(days=1)}
    assert (
        dedupe_by_primary_key(pl.DataFrame([stale, bare]), "corporate_actions")[
            "payment_date"
        ].item()
        is None
    )
    # Reads never rewrite a stored value, typo or not.
    assert dedupe_by_primary_key(pl.DataFrame([typo]), "corporate_actions")[
        "payment_date"
    ].item() == date(2015, 7, 6)

    # A new row never stores the typo ...
    cfg = Config(data_root=tmp_path / "lake")
    _publish(cfg, "typo", [typo])
    key = ("002627.SZ", date(2016, 7, 6), "cash_dividend")
    assert _published(cfg)[key]["payment_date"] is None

    # ... and one already stored is republished once a cleared row arrives.
    legacy = Config(data_root=tmp_path / "legacy")
    import cnequity.storage.parquet as parquet

    monkeypatch.setattr(parquet, "clear_invalid_payment_evidence", lambda frame: frame)
    _publish(legacy, "legacy-typo", [typo])
    monkeypatch.undo()
    assert _published(legacy)[key]["payment_date"] == date(2015, 7, 6)
    result = _publish(legacy, "cleared", [bare])
    assert result["dataset_revisions"]["corporate_actions"]["revision"] == 2
    assert _published(legacy)[key]["payment_date"] is None
