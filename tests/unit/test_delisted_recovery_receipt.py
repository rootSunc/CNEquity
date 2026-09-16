import json
from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.steps import delisted


def test_delisted_recovery_gate_requires_receipt_integrity_and_bars(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    start = end = date(2024, 6, 27)
    targets = {
        "600001.SH": {
            "ownership": "dedicated_fetch",
            "basis": "formal_delist_date",
            "formal_delist_date": end.isoformat(),
        }
    }
    scope = delisted._recovery_scope(start, end, targets)
    receipt_root = cfg.meta_root / "quality" / "coverage" / delisted._RECOVERY_CLAIM
    receipt_root.mkdir(parents=True)
    receipt_path = receipt_root / f"{scope['scope_id']}.json"
    receipt_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "claim": delisted._RECOVERY_CLAIM,
                "status": "complete",
                "scope": {key: value for key, value in scope.items() if key != "targets"},
                "recovered_symbols": ["600001.SH"],
                "expected_no_data_symbols": [],
                "target_symbols_sha256": delisted._recovery_symbol_hash(["600001.SH"]),
            }
        ),
        encoding="utf-8",
    )

    assert delisted.delisted_recovery_covers(cfg, start, end, ["600001.SH"]) is False

    bars = cfg.curated_root / "daily_bars" / f"trade_date={start.isoformat()}"
    bars.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600001.SH"],
            "trade_date": [start],
            "open": [10.0],
            "high": [10.0],
            "low": [10.0],
            "close": [10.0],
            "volume": [100],
            "amount": [1000.0],
        }
    ).write_parquet(bars / "part.parquet")

    assert delisted.delisted_recovery_covers(cfg, start, end, ["600001.SH"]) is True

    payload = json.loads(receipt_path.read_text(encoding="utf-8"))
    payload["scope"]["scope_id"] = "tampered"
    receipt_path.write_text(json.dumps(payload), encoding="utf-8")
    assert delisted.delisted_recovery_covers(cfg, start, end, ["600001.SH"]) is False


def test_delisted_recovery_covers_requires_bars_to_span_the_full_window(tmp_path):
    """A merely-overlapping on-disk span must not count as full coverage.

    A wide claimed window with bars on disk for only a narrow sub-range
    (e.g. after a later repair purges rows outside it) must return False,
    not True just because the two ranges touch at all.
    """
    cfg = Config(data_root=tmp_path / "data")
    start, end = date(2024, 1, 1), date(2024, 12, 31)
    targets = {
        "600001.SH": {
            "ownership": "dedicated_fetch",
            "basis": "formal_delist_date",
            "formal_delist_date": end.isoformat(),
        }
    }
    scope = delisted._recovery_scope(start, end, targets)
    receipt_root = cfg.meta_root / "quality" / "coverage" / delisted._RECOVERY_CLAIM
    receipt_root.mkdir(parents=True)
    (receipt_root / f"{scope['scope_id']}.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "claim": delisted._RECOVERY_CLAIM,
                "status": "complete",
                "scope": {key: value for key, value in scope.items() if key != "targets"},
                "recovered_symbols": ["600001.SH"],
                "expected_no_data_symbols": [],
                "target_symbols_sha256": delisted._recovery_symbol_hash(["600001.SH"]),
            }
        ),
        encoding="utf-8",
    )

    # Bars on disk cover only 2024-06-01..2024-06-05 - well inside [start,
    # end], overlapping it, but nowhere near spanning the full claimed year.
    for d in (date(2024, 6, 1), date(2024, 6, 5)):
        part = cfg.curated_root / "daily_bars" / f"trade_date={d.isoformat()}"
        part.mkdir(parents=True)
        pl.DataFrame(
            {
                "symbol": ["600001.SH"],
                "trade_date": [d],
                "open": [10.0],
                "high": [10.0],
                "low": [10.0],
                "close": [10.0],
                "volume": [100],
                "amount": [1000.0],
            }
        ).write_parquet(part / "part.parquet")

    assert delisted.delisted_recovery_covers(cfg, start, end, ["600001.SH"]) is False


def _instruments(cfg: Config, rows: list[tuple[str, date | None]]) -> None:
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": [symbol for symbol, _ in rows],
            "name": [None] * len(rows),
            "exchange": [symbol.split(".")[1] for symbol, _ in rows],
            "asset_type": ["stock"] * len(rows),
            "list_date": [date(2017, 5, 19)] * len(rows),
            "delist_date": [delist for _, delist in rows],
        }
    ).write_parquet(root / "part-merged.parquet")


def _bars(cfg: Config, symbol: str, days: list[date]) -> None:
    for day in days:
        part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
        part.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(
            {
                "symbol": [symbol],
                "trade_date": [day],
                "open": [10.0],
                "high": [10.0],
                "low": [10.0],
                "close": [10.0],
                "volume": [100],
                "amount": [1000.0],
            }
        ).write_parquet(part / f"part-{symbol}.parquet")


def test_a_name_whose_history_is_already_here_needs_no_recovery_receipt(tmp_path):
    """A freshly delisted name that has traded in this lake for years.

    The recovery sweep exists for names the live catalogue no longer lists and
    the lake has never seen. Demanding a receipt for a name whose history is
    already on disk blocked the daily compaction outright — and the receipt
    could never arrive, because the gate also asked a name that stopped trading
    in July for a bar dated in September.
    """
    cfg = Config(data_root=tmp_path / "data")
    start, end = date(2026, 9, 7), date(2026, 9, 15)
    _instruments(cfg, [("920305.BJ", date(2026, 9, 14))])
    _bars(cfg, "920305.BJ", [date(2026, 7, 28), date(2026, 7, 29)])

    assert delisted.delisted_recovery_covers(cfg, start, end, ["920305.BJ"]) is True


def test_a_name_the_lake_has_never_seen_still_blocks(tmp_path):
    """The survivorship guarantee: no bars, no proof, no pass."""
    cfg = Config(data_root=tmp_path / "data")
    start, end = date(2026, 9, 7), date(2026, 9, 15)
    _instruments(cfg, [("430999.BJ", date(2026, 9, 14))])

    assert delisted.delisted_recovery_covers(cfg, start, end, ["430999.BJ"]) is False


def test_bars_running_past_the_catalogued_delisting_are_not_accepted(tmp_path):
    """A span that contradicts the catalogue is not proof of anything."""
    cfg = Config(data_root=tmp_path / "data")
    start, end = date(2026, 9, 7), date(2026, 9, 15)
    _instruments(cfg, [("920305.BJ", date(2026, 7, 1))])
    _bars(cfg, "920305.BJ", [date(2026, 7, 28), date(2026, 7, 29)])

    assert delisted.delisted_recovery_covers(cfg, start, end, ["920305.BJ"]) is False


def test_history_starting_after_the_window_is_not_accepted(tmp_path):
    """Bars that begin mid-window leave the earlier sessions unproven."""
    cfg = Config(data_root=tmp_path / "data")
    start, end = date(2026, 9, 7), date(2026, 9, 15)
    _instruments(cfg, [("920305.BJ", date(2026, 9, 14))])
    _bars(cfg, "920305.BJ", [date(2026, 9, 10)])

    assert delisted.delisted_recovery_covers(cfg, start, end, ["920305.BJ"]) is False
