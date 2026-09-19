"""`trading_status` read from the exchanges instead of from a redistributor.

Its daily feed is EastMoney's current-state board, and the in-step fallback was
this lake's own previous snapshot — the same opinion a day older, not a second
one. The exchanges publish both facts the dataset needs in the board file the
daily-bar route already reads: a halted security is listed with its
open/high/low at zero beside a reference close, and ST is carried in 证券简称.

Measured against the EastMoney rows for 2026-09-15 over 5,219 comparable
symbols: ST agreed on 100.000%, halts on 99.923%, and all four disagreements
were EastMoney calling a name halted while the exchange published a full
session for it.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.adapters.exchange import trading_status as mod
from cnequity.domain.trading_status import STATUS_NORMAL, STATUS_SUSPENDED

DAY = date(2026, 9, 15)


def test_a_zeroed_session_beside_a_reference_close_is_a_halt():
    row = mod._row("603400.SH", "某某股份", DAY, (0.0, 0.0, 0.0), 54.67)

    assert row["is_trading"] is False
    assert row["status"] == STATUS_SUSPENDED


def test_a_limit_locked_session_is_not_a_halt():
    """open == high == low == close is a real session, just a still one."""
    row = mod._row("600519.SH", "贵州茅台", DAY, (10.0, 10.0, 10.0), 10.0)

    assert row["is_trading"] is True
    assert row["status"] == STATUS_NORMAL


def test_the_risk_warning_comes_from_the_name():
    assert mod._row("002731.SZ", "*ST萃华", DAY, (1.0, 1.1, 0.9), 1.0)["risk_warning"] is True
    assert mod._row("600519.SH", "贵州茅台", DAY, (1.0, 1.1, 0.9), 1.0)["risk_warning"] is False


def test_one_exchange_failing_leaves_the_other_its_reading(monkeypatch):
    """A Shenzhen outage must not cost Shanghai its answer."""
    monkeypatch.setattr(
        mod,
        "_fetch_sse",
        lambda day, config=None: [mod._row("600519.SH", "贵州茅台", day, (1.0, 1.1, 0.9), 1.0)],
    )

    def boom(day, config=None):
        raise RuntimeError("Connection reset by peer")

    monkeypatch.setattr(mod, "_fetch_szse", boom)

    result = mod.fetch_trading_status_exchange(None, DAY, config=None)

    assert result.covered == {"sse"}
    assert "szse" in result.failures
    assert result.rows.get_column("symbol").to_list() == ["600519.SH"]


def test_both_failing_is_empty_rather_than_an_exception(monkeypatch):
    """It is the fallback path: it must not replace one outage with two."""
    for name in ("_fetch_sse", "_fetch_szse"):
        monkeypatch.setattr(
            mod, name, lambda day, config=None: (_ for _ in ()).throw(RuntimeError("down"))
        )

    result = mod.fetch_trading_status_exchange(None, DAY, config=None)

    assert result.is_empty
    assert result.covered == frozenset()
    assert set(result.failures) == {"sse", "szse"}


def test_a_snapshot_for_another_session_is_refused(monkeypatch):
    """A current board relabelled as another day would manufacture a PIT fact."""

    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"date": "20260914", "time": 150000, "list": []}

    monkeypatch.setattr(
        mod, "_client", lambda: type("C", (), {"get": lambda *a, **k: _Response()})()
    )

    with pytest.raises(ValueError, match="serves 2026-09-14"):
        mod._fetch_sse(DAY, config=None)


def test_a_mid_session_snapshot_is_refused(monkeypatch):
    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"date": "20260915", "time": 113000, "list": []}

    monkeypatch.setattr(
        mod, "_client", lambda: type("C", (), {"get": lambda *a, **k: _Response()})()
    )

    with pytest.raises(ValueError, match="mid-session"):
        mod._fetch_sse(DAY, config=None)


def test_the_requested_scope_is_what_comes_back(monkeypatch):
    monkeypatch.setattr(
        mod,
        "_fetch_sse",
        lambda day, config=None: [
            mod._row("600519.SH", "贵州茅台", day, (1.0, 1.1, 0.9), 1.0),
            mod._row("600000.SH", "浦发银行", day, (1.0, 1.1, 0.9), 1.0),
        ],
    )
    monkeypatch.setattr(mod, "_fetch_szse", lambda day, config=None: [])

    result = mod.fetch_trading_status_exchange({"600519.SH"}, DAY, config=None)

    assert result.rows.get_column("symbol").to_list() == ["600519.SH"]


def test_the_frame_matches_the_eastmoney_contract(monkeypatch):
    """The step concatenates the two; a different shape would not survive it."""
    monkeypatch.setattr(
        mod,
        "_fetch_sse",
        lambda day, config=None: [mod._row("600519.SH", "贵州茅台", day, (1.0, 1.1, 0.9), 1.0)],
    )
    monkeypatch.setattr(mod, "_fetch_szse", lambda day, config=None: [])

    rows = mod.fetch_trading_status_exchange(None, DAY, config=None).rows

    assert set(rows.columns) == {"symbol", "trade_date", "is_trading", "status", "risk_warning"}
    assert rows.schema["risk_warning"] == pl.Boolean


def test_a_mixed_fallback_day_keeps_each_row_its_own_owner(tmp_path, monkeypatch):
    """An outage day is served by two sources; one label would misreport one.

    The step used to drop the vendor frame's `source` unconditionally, which
    was right while that frame was always one vendor's. With the exchange
    boards underneath EastMoney it is not, and erasing the label would publish
    the exchange's reading under EastMoney's name — in a lake whose headline
    guarantee is that every row says where it came from.
    """
    import polars as pl

    from cnequity.config import Config
    from cnequity.steps import reference

    cfg = Config(data_root=tmp_path / "data", workers=1)
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "920184.BJ"],
            "name": ["贵州茅台", "北交所某股"],
            "exchange": ["SH", "BJ"],
            "asset_type": ["stock", "stock"],
            "list_date": [date(2001, 8, 27)] * 2,
            "delist_date": [None, None],
        }
    ).write_parquet(instruments / "part-merged.parquet")

    # Yesterday's curated snapshot, so the cached path has something to serve
    # for the names the exchange boards do not carry.
    prior = cfg.curated_root / "trading_status" / "trade_date=2026-09"
    prior.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH", "920184.BJ"],
            "trade_date": [date(2026, 9, 14)] * 2,
            "is_trading": [True, True],
            "status": ["normal", "normal"],
            "risk_warning": [False, False],
            "source": ["eastmoney", "eastmoney"],
            "data_version": ["v1", "v1"],
            "fetched_at": ["2026-09-14T00:00:00+00:00"] * 2,
        }
    ).write_parquet(prior / "part-merged.parquet")

    # EastMoney down; the exchange answers for SH only, as it does in reality.
    monkeypatch.setattr(
        reference,
        "fetch_trading_status",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("outage")),
    )
    monkeypatch.setattr(
        "cnequity.adapters.exchange.trading_status.fetch_trading_status_exchange",
        lambda symbols, day, config=None: mod.ExchangeStatusResult(
            rows=pl.DataFrame(
                [mod._row("600519.SH", "贵州茅台", day, (1.0, 1.1, 0.9), 1.0)],
                schema_overrides={"risk_warning": pl.Boolean},
            ),
            covered=frozenset({"sse"}),
            failures={"szse": "down"},
        ),
    )

    from cnequity.orchestrator.manifest import Manifest

    run_id = Manifest(cfg.manifest_path).start_run("test")
    reference.step_trading_status(cfg, DAY, run_id, {})

    staged = pl.concat(
        [
            pl.read_parquet(path)
            for path in (cfg.staging_root / "trading_status" / f"run_id={run_id}").glob("*.parquet")
        ],
        how="diagonal_relaxed",
    )
    owners = dict(
        zip(*staged.select("symbol", "source").to_dict(as_series=False).values(), strict=True)
    )
    assert owners["600519.SH"] == "exchange"
    # The Beijing name is on neither board; it keeps the step's own label.
    assert owners["920184.BJ"] != "exchange"


def test_the_board_reading_is_snapshotted_every_session(tmp_path, monkeypatch):
    """Not only when EastMoney fails.

    The exchange reader sits on the failover path, so a normal day leaves the
    lake with no exchange-grade record of SH/SZ status — and the ST evidence
    receipt admits `bse` precisely because a board is not an aggregator. Two
    requests and 2.6s per session buys the record; authority over
    `trading_status` is untouched, because this writes to the snapshot store.
    """
    from datetime import date as _date

    import polars as pl

    from cnequity.config import Config
    from cnequity.quality import failover
    from cnequity.storage.layout import init_data_layout
    from cnequity.storage.source_snapshots import SnapshotStore

    day = _date(2026, 9, 18)
    cfg = Config(data_root=tmp_path / "data", sources={"exchange": True})
    init_data_layout(cfg)
    asked: dict = {}

    class _Result:
        is_empty = False
        rows = pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [day],
                "is_trading": [True],
                "status": ["normal"],
                "risk_warning": [False],
            }
        )

    def fake_fetch(symbols, trade_date, *, config=None):
        asked["symbols"] = list(symbols)
        return _Result()

    monkeypatch.setattr(
        "cnequity.adapters.exchange.trading_status.fetch_trading_status_exchange", fake_fetch
    )

    written = failover.snapshot_trading_status_exchange(
        cfg, trade_date=day, symbols=["600519.SH", "920001.BJ"], run_id="run-1"
    )

    assert asked["symbols"] == ["600519.SH"], "Beijing is not on these boards"
    assert written == 1
    snapshot = SnapshotStore(cfg.meta_root).read_latest("trading_status", source="exchange")
    assert snapshot.get_column("source").to_list() == ["exchange"]
    curated = cfg.curated_root / "trading_status"
    assert not curated.exists() or not any(curated.rglob("*.parquet")), "authority is unchanged"
