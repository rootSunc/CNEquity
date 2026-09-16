"""`status` and `risk_warning` are two facts, and delisting is a third state.

The dataset used to squeeze the ST designation and the trading state into one
string, resolved by an if/elif that let halting win. Live evidence of the
damage, 2026-08-28 on a full lake:

* 000711.SZ (ST京蓝) was `st` on 08-27 and `suspended` on 08-28 — same company,
  still under risk warning, designation gone from the stored history.
* 611 symbols carrying a `delist_date` (one since 1999-07-12) were published as
  `normal` with `is_trading=True`, because a delisted name is on neither the
  halt list nor the risk board and the `else` branch called that normal.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.schemas import validate_dataframe, with_provenance
from cnequity.domain.trading_status import (
    DELISTED_SOURCE,
    STATUS_DELISTED,
    STATUS_NORMAL,
    STATUS_SUSPENDED,
    normalize_legacy,
    risk_warning_expr,
)

TD = date(2024, 6, 28)


def _legacy(status: str, symbol: str = "600519.SH") -> pl.DataFrame:
    """A row in the pre-split encoding: no risk_warning column at all."""
    return pl.DataFrame(
        {
            "symbol": [symbol],
            "trade_date": [TD],
            "is_trading": [status != STATUS_SUSPENDED],
            "status": [status],
        }
    )


# --- reading both encodings ---------------------------------------------------


def test_legacy_st_rows_still_read_as_risk_warned():
    """A lake that has not run the migration must still answer correctly."""
    frame = _legacy("st")
    assert frame.select(risk_warning_expr(frame.columns)).to_series().to_list() == [True]


def test_legacy_normal_rows_are_not_risk_warned():
    frame = _legacy(STATUS_NORMAL)
    assert frame.select(risk_warning_expr(frame.columns)).to_series().to_list() == [False]


def test_the_new_column_wins_where_it_exists():
    frame = _legacy(STATUS_SUSPENDED).with_columns(pl.lit(True).alias("risk_warning"))
    assert frame.select(risk_warning_expr(frame.columns)).to_series().to_list() == [True]


def test_a_null_new_column_does_not_erase_a_legacy_label():
    """Mid-migration a file can have the column and legacy values in it."""
    frame = _legacy("st").with_columns(pl.lit(None, dtype=pl.Boolean).alias("risk_warning"))
    assert frame.select(risk_warning_expr(frame.columns)).to_series().to_list() == [True]


def test_normalize_moves_st_out_of_status():
    out = normalize_legacy(_legacy("st"))
    assert out["status"].to_list() == [STATUS_NORMAL]
    assert out["risk_warning"].to_list() == [True]


def test_normalize_is_idempotent():
    once = normalize_legacy(_legacy("st"))
    assert normalize_legacy(once).equals(once)


def test_normalize_leaves_the_trading_state_alone():
    out = normalize_legacy(_legacy(STATUS_SUSPENDED))
    assert out["status"].to_list() == [STATUS_SUSPENDED]
    assert out["risk_warning"].to_list() == [False]


def test_validation_upgrades_a_legacy_frame_rather_than_rejecting_it():
    """Every read path validates; a pre-split lake must not become unreadable."""
    frame = with_provenance(_legacy("st"), source="baostock", data_version="v1")
    out = validate_dataframe(frame, "trading_status")
    assert out["risk_warning"].to_list() == [True]
    assert out["status"].to_list() == [STATUS_NORMAL]


# --- the daily step -----------------------------------------------------------


@pytest.fixture
def lake(tmp_path):
    return Config(data_root=tmp_path / "data")


def _write_instruments(cfg, rows):
    part = cfg.curated_root / "instruments"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "symbol": [r["symbol"] for r in rows],
            "name": [r.get("name") for r in rows],
            "exchange": ["SH"] * len(rows),
            "asset_type": ["stock"] * len(rows),
            "list_date": [date(2000, 1, 1)] * len(rows),
            "delist_date": [r.get("delist_date") for r in rows],
            "prev_symbol": [None] * len(rows),
            "source": ["tdx_protocol"] * len(rows),
            "data_version": ["v1"] * len(rows),
            "fetched_at": [datetime.now(timezone.utc)] * len(rows),
        },
        schema_overrides={"delist_date": pl.Date, "name": pl.Utf8, "prev_symbol": pl.Utf8},
    ).write_parquet(part / "part-merged.parquet")


def _staged(cfg) -> pl.DataFrame:
    files = sorted((cfg.staging_root / "trading_status").glob("**/*.parquet"))
    assert files, "step wrote nothing"
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")


def test_a_delisted_symbol_is_not_published_as_trading(lake, monkeypatch):
    from cnequity.steps import reference

    _write_instruments(
        lake,
        [
            {"symbol": "600519.SH"},
            {"symbol": "600355.SH", "name": "*ST精伦", "delist_date": date(2024, 1, 15)},
        ],
    )
    requested: list[list[str]] = []

    def _fetch(symbols, day, **_kw):
        requested.append(list(symbols))
        return pl.DataFrame(
            {
                "symbol": list(symbols),
                "trade_date": [day] * len(symbols),
                "is_trading": [True] * len(symbols),
                "status": [STATUS_NORMAL] * len(symbols),
                "risk_warning": [False] * len(symbols),
            }
        )

    monkeypatch.setattr(reference, "load_symbols", lambda _c: ["600519.SH", "600355.SH"])
    monkeypatch.setattr(reference, "fetch_trading_status", _fetch)
    reference.step_trading_status(lake, TD, "run-delisted", {})

    # The boards cannot answer for a name that has left the market, so it is
    # never asked about — that `else` branch is what called it normal.
    assert requested == [["600519.SH"]]
    rows = {r["symbol"]: r for r in _staged(lake).iter_rows(named=True)}
    assert rows["600355.SH"]["status"] == STATUS_DELISTED
    assert rows["600355.SH"]["is_trading"] is False
    assert rows["600355.SH"]["source"] == DELISTED_SOURCE
    # The final 简称 is the only ST evidence left once the boards drop it.
    assert rows["600355.SH"]["risk_warning"] is True
    assert rows["600519.SH"]["status"] == STATUS_NORMAL
    assert rows["600519.SH"]["source"] == "eastmoney"


def test_the_delist_date_itself_is_already_gone(lake, monkeypatch):
    """`delist_date` is the first session with no bar, not the last with one.

    Verified against a real lake: 600355.SH carries delist_date 2026-04-27 and
    its last daily bar is 2026-04-24.
    """
    from cnequity.steps import reference

    _write_instruments(lake, [{"symbol": "600355.SH", "delist_date": TD}])
    monkeypatch.setattr(reference, "load_symbols", lambda _c: ["600355.SH"])
    monkeypatch.setattr(
        reference,
        "fetch_trading_status",
        lambda *a, **k: pytest.fail("a delisted symbol must not be requested"),
    )
    reference.step_trading_status(lake, TD, "run-boundary", {})
    assert _staged(lake).to_dicts()[0]["status"] == STATUS_DELISTED


def test_a_symbol_delisted_after_the_session_is_still_asked_about(lake, monkeypatch):
    """Delisting is scoped per day: a catch-up spans sessions it was alive for."""
    from cnequity.steps import reference

    _write_instruments(
        lake, [{"symbol": "600355.SH", "name": "*ST精伦", "delist_date": date(2026, 4, 27)}]
    )
    requested: list[list[str]] = []

    def _fetch(symbols, day, **_kw):
        requested.append(list(symbols))
        return pl.DataFrame(
            {
                "symbol": list(symbols),
                "trade_date": [day] * len(symbols),
                "is_trading": [True] * len(symbols),
                "status": [STATUS_NORMAL] * len(symbols),
                "risk_warning": [True] * len(symbols),
            }
        )

    monkeypatch.setattr(reference, "load_symbols", lambda _c: ["600355.SH"])
    monkeypatch.setattr(reference, "fetch_trading_status", _fetch)
    reference.step_trading_status(lake, date(2026, 4, 24), "run-alive", {})

    assert requested == [["600355.SH"]]
    rows = _staged(lake).to_dicts()
    assert [r["status"] for r in rows] == [STATUS_NORMAL]


def test_a_lake_with_no_delistings_writes_only_vendor_rows(lake, monkeypatch):
    from cnequity.steps import reference

    _write_instruments(lake, [{"symbol": "600519.SH"}])
    monkeypatch.setattr(reference, "load_symbols", lambda _c: ["600519.SH"])
    monkeypatch.setattr(
        reference,
        "fetch_trading_status",
        lambda symbols, day, **_kw: pl.DataFrame(
            {
                "symbol": list(symbols),
                "trade_date": [day],
                "is_trading": [True],
                "status": [STATUS_NORMAL],
                "risk_warning": [False],
            }
        ),
    )
    reference.step_trading_status(lake, TD, "run-clean", {})
    assert set(_staged(lake)["source"]) == {"eastmoney"}


# --- downstream consumers -----------------------------------------------------


def test_a_halted_st_name_keeps_the_narrow_limit_band():
    """The concrete damage the single column caused, in the derive that read it.

    `market_breadth` picks ±5% for a risk-warned name and ±10% otherwise. Under
    the old encoding a halted ST stock read as `suspended`, so every one of its
    sessions was measured against the wrong band.
    """
    from cnequity.derive.market_breadth import _limit_threshold

    assert _limit_threshold("000711.SZ", True) == 0.045
    assert _limit_threshold("000711.SZ", False) == 0.095
    assert _limit_threshold("000711.SZ", None) == 0.095
    # Board bands still apply when there is no risk warning.
    assert _limit_threshold("300750.SZ", False) == 0.195
    assert _limit_threshold("920001.BJ", False) == 0.295


def test_market_breadth_reads_risk_warning_from_a_halted_st_row(tmp_path):
    """End to end: the frame `market_breadth` builds must carry the flag."""
    from cnequity.derive.market_breadth import _read_trading_status

    root = tmp_path / "trading_status"
    part = root / f"trade_date={TD.isoformat()}"
    part.mkdir(parents=True)
    with_provenance(
        pl.DataFrame(
            {
                "symbol": ["000711.SZ", "600519.SH"],
                "trade_date": [TD, TD],
                "is_trading": [False, True],
                "status": [STATUS_SUSPENDED, STATUS_NORMAL],
                "risk_warning": [True, False],
            }
        ),
        source="eastmoney",
        data_version="v1",
    ).write_parquet(part / "p.parquet")

    out = _read_trading_status(root, TD)
    flags = {r["symbol"]: r["risk_warning"] for r in out.iter_rows(named=True)}
    assert flags == {"000711.SZ": True, "600519.SH": False}


@pytest.mark.parametrize(
    ("status", "risk_warning"),
    [
        (STATUS_SUSPENDED, False),
        (STATUS_DELISTED, False),
        (STATUS_NORMAL, True),
        # The legacy encoding must keep excluding, migrated or not.
        ("st", None),
    ],
)
def test_the_tradable_universe_drops_halted_delisted_and_risk_warned(status, risk_warning):
    from cnequity.query.universe import _excluded_status_expr

    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "status": [status],
            "risk_warning": pl.Series([risk_warning], dtype=pl.Boolean),
        }
    )
    assert frame.select(_excluded_status_expr(frame.columns)).to_series().to_list() == [True]


def test_the_tradable_universe_keeps_an_ordinary_name():
    from cnequity.query.universe import _excluded_status_expr

    frame = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "status": [STATUS_NORMAL],
            "risk_warning": pl.Series([False], dtype=pl.Boolean),
        }
    )
    assert frame.select(_excluded_status_expr(frame.columns)).to_series().to_list() == [False]


# --- migration ----------------------------------------------------------------


def test_migration_rewrites_legacy_st_and_is_idempotent():
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location(
        "_migrate_ts",
        Path(__file__).resolve().parents[2] / "scripts" / "migrate_trading_status_risk_warning.py",
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    legacy = with_provenance(_legacy("st"), source="baostock", data_version="v1")
    migrated, changed, legacy_rows = module.migrate_frame(legacy)
    assert changed is True
    assert legacy_rows == 1
    assert migrated["status"].to_list() == [STATUS_NORMAL]
    assert migrated["risk_warning"].to_list() == [True]

    again, changed_again, _ = module.migrate_frame(migrated)
    assert changed_again is False
    assert again.equals(migrated)


def test_eastmoney_does_not_claim_not_st_for_an_exchange_its_board_skips():
    """`_ST_FS` selects m:0 (Shenzhen) and m:1 (Shanghai). Beijing has no market
    code in it, so "absent from the returned set" means the board never looked.

    Read as "not ST", that published `risk_warning=False` for all 343 live
    Beijing names every day, while the exchange was listing *ST康乐, *ST田野 and
    *ST同辉. Null is what the lake means by unknown, and `st_coverage` is what
    tracks where the evidence is actually missing.
    """
    from cnequity.adapters.eastmoney import trading_status as em

    captured: dict = {}

    class _Client:
        def close(self):
            return None

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(em, "_fetch_st_symbols", lambda client: {"600519.SH"})
        monkeypatch.setattr(em, "_fetch_suspended_symbols", lambda client, day: set())
        out = em.fetch_trading_status_eastmoney(
            ["600519.SH", "000001.SZ", "920023.BJ"],
            date(2026, 9, 15),
            client=_Client(),
        )
    finally:
        monkeypatch.undo()

    captured = {r["symbol"]: r["risk_warning"] for r in out.to_dicts()}
    assert captured["600519.SH"] is True
    assert captured["000001.SZ"] is False
    assert captured["920023.BJ"] is None


def test_a_delisted_name_with_no_simplified_name_has_unknown_risk_warning(tmp_path):
    """The 简称 is the only ST evidence left once the boards drop a symbol, so
    its absence is the absence of evidence — not a clean bill of health.

    253 retired Beijing codes carry no name at all and were published as
    not-under-risk-warning on every session since they delisted.
    """
    from cnequity.steps import reference

    frame = pl.DataFrame(
        {
            "symbol": ["600001.SH", "600002.SH", "830001.BJ"],
            "name": ["*ST元成", "贵州茅台", None],
            "delist_date": [date(2020, 1, 1)] * 3,
        }
    )
    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(reference, "load_curated_instruments", lambda config: frame)
        out = reference._delisted_instruments(Config(data_root=tmp_path / "lake"))
    finally:
        monkeypatch.undo()

    got = {r["symbol"]: r["risk_warning"] for r in out.to_dicts()}
    assert got["600001.SH"] is True
    assert got["600002.SH"] is False
    assert got["830001.BJ"] is None


def test_beijing_rows_come_from_the_beijing_exchange_not_eastmoney(tmp_path, monkeypatch):
    """EastMoney serves neither column for this exchange, and both gaps were
    stored as facts: 15,515 BJ rows in the lake's history, every one
    `is_trading=True`, against 116 halts in 148,933 SH/SZ rows — and
    `risk_warning=False` for every BJ name while the exchange listed *ST田野.

    The replacement is row-for-row: same symbols, so the step's own
    completeness check still sees the scope it asked for.
    """
    from cnequity.adapters.bse.trading_status import BseStatusResult
    from cnequity.steps import reference

    vendor = pl.DataFrame(
        {
            "symbol": ["600519.SH", "920023.BJ", "920999.BJ"],
            "trade_date": [date(2026, 9, 15)] * 3,
            "is_trading": [True, True, True],
            "status": ["normal"] * 3,
            "risk_warning": [False, None, None],
            "source": ["eastmoney"] * 3,
        },
        schema_overrides={"risk_warning": pl.Boolean},
    )
    board = BseStatusResult(
        rows=pl.DataFrame(
            {
                "symbol": ["920023.BJ", "920999.BJ"],
                "trade_date": [date(2026, 9, 15)] * 2,
                "is_trading": [True, False],
                "status": ["normal", "suspended"],
                "risk_warning": [True, None],
            },
            schema_overrides={"risk_warning": pl.Boolean},
        ),
        complete=True,
        listed=frozenset({"920023.BJ"}),
    )
    monkeypatch.setattr(
        "cnequity.adapters.bse.trading_status.fetch_trading_status_bse",
        lambda *a, **k: board,
    )

    out = reference._beijing_status(
        Config(data_root=tmp_path / "lake", sources={"bse": True}),
        vendor,
        date(2026, 9, 15),
        vendor["symbol"].to_list(),
    )

    got = {r["symbol"]: r for r in out.to_dicts()}
    assert sorted(got) == ["600519.SH", "920023.BJ", "920999.BJ"]
    # Shanghai untouched.
    assert got["600519.SH"]["source"] == "eastmoney"
    # The exchange's own answer, on both columns, labelled as its own.
    assert got["920023.BJ"]["risk_warning"] is True
    assert got["920023.BJ"]["source"] == "bse"
    assert got["920999.BJ"]["is_trading"] is False
    assert got["920999.BJ"]["source"] == "bse"
