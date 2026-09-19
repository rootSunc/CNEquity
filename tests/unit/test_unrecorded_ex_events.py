"""A factor step nobody recorded: found by its ratio, healed the next run.

A fund unit split reaches this lake through one route only — TDX `xdxr`
category 11 — and that route runs on the backfill path. The daily source is
EastMoney's dividend report, which has no unit-split concept at all, so until
this a split was recorded only if somebody remembered to re-backfill that
symbol. The factor series names the day and the ratio the very next session.
"""

from datetime import date, datetime, timezone

import polars as pl

import cnequity.steps  # noqa: F401
from cnequity.config import Config
from cnequity.quality.ex_events import unexplained_factor_steps
from cnequity.steps import events
from cnequity.storage.layout import init_data_layout

FETCHED = datetime(2026, 9, 19, tzinfo=timezone.utc)
SESSIONS = [date(2026, 9, 14), date(2026, 9, 15), date(2026, 9, 16), date(2026, 9, 17)]


def _write(root, partition: str, rows: list[dict]) -> None:
    target = root / partition if partition else root
    target.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target / "part-0.parquet")


def _lake(tmp_path, *, factors: dict[date, float], recorded: list[date]) -> Config:
    """A lake whose hfq factor follows *factors*, with actions on *recorded*."""
    cfg = Config(data_root=tmp_path / "data", raw_archive_enabled=False)
    init_data_layout(cfg)
    _write(
        cfg.curated_root / "trading_calendar",
        "",
        [
            {
                "trade_date": session,
                "is_trading": True,
                "source": "tdx_protocol",
                "data_version": "v1",
                "fetched_at": FETCHED,
            }
            for session in SESSIONS
        ],
    )
    for session, factor in factors.items():
        _write(
            cfg.derived_root / "adj_factors",
            f"trade_date={session.isoformat()}",
            [
                {
                    "symbol": "159327.SZ",
                    "trade_date": session,
                    "adjust_type": "hfq",
                    "factor": factor,
                    "source": "sina",
                    "data_version": "v1",
                    "fetched_at": FETCHED,
                }
            ],
        )
    for ex_date in recorded:
        _write(
            cfg.curated_root / "corporate_actions",
            f"ex_date={ex_date.year}",
            [
                {
                    "symbol": "159327.SZ",
                    "ex_date": ex_date,
                    "action_type": "unit_split",
                    "cash_dividend": 0.0,
                    "bonus_ratio": 0.0,
                    "transfer_ratio": 0.0,
                    "allotment_ratio": None,
                    "allotment_price": None,
                    "split_factor": 3.0,
                    "source": "tdx_protocol",
                    "data_version": "v1",
                    "fetched_at": FETCHED,
                }
            ],
        )
    return cfg


def test_the_step_is_found_by_its_ratio_not_by_a_price_threshold(tmp_path):
    """A 1-for-1.1 restatement moves the price 9% and never trips the 11% bar."""
    cfg = _lake(
        tmp_path,
        factors={SESSIONS[0]: 1.0, SESSIONS[1]: 1.0, SESSIONS[2]: 1.1, SESSIONS[3]: 1.1},
        recorded=[],
    )

    steps = unexplained_factor_steps(cfg, upto=SESSIONS[-1])

    assert steps.select("symbol", "ex_date").to_dicts() == [
        {"symbol": "159327.SZ", "ex_date": SESSIONS[2]}
    ]
    assert steps["factor_ratio"].to_list() == [1.1]


def test_a_recorded_action_explains_the_step(tmp_path):
    cfg = _lake(
        tmp_path,
        factors={SESSIONS[0]: 1.0, SESSIONS[1]: 1.0, SESSIONS[2]: 3.0, SESSIONS[3]: 3.0},
        recorded=[SESSIONS[2]],
    )

    assert unexplained_factor_steps(cfg, upto=SESSIONS[-1]).is_empty()


def test_nothing_older_than_the_window_is_re_asked(tmp_path):
    """A gap no source can fill must stop being a daily request forever."""
    cfg = _lake(
        tmp_path,
        factors={SESSIONS[0]: 1.0, SESSIONS[1]: 1.0, SESSIONS[2]: 3.0, SESSIONS[3]: 3.0},
        recorded=[],
    )

    assert unexplained_factor_steps(cfg, upto=SESSIONS[-1], sessions=1).is_empty()


def test_the_daily_run_asks_tdx_about_the_step_and_records_the_answer(tmp_path, monkeypatch):
    cfg = _lake(
        tmp_path,
        factors={SESSIONS[0]: 1.0, SESSIONS[1]: 1.0, SESSIONS[2]: 3.0, SESSIONS[3]: 3.0},
        recorded=[],
    )
    asked: dict = {}

    def fake_tdx(trade_date, **kwargs):
        asked["symbols"] = kwargs["symbols"]
        return pl.DataFrame(
            {
                "symbol": ["159327.SZ", "159327.SZ"],
                # The day in question, plus one this run was not asked about.
                "ex_date": [SESSIONS[2], date(2024, 3, 1)],
                "action_type": ["unit_split", "cash_dividend"],
                "cash_dividend": [0.0, 0.1],
                "bonus_ratio": [0.0, 0.0],
                "transfer_ratio": [0.0, 0.0],
                "allotment_ratio": [None, None],
                "allotment_price": [None, None],
                "split_factor": [3.0, None],
            },
            schema_overrides={"allotment_ratio": pl.Float64, "allotment_price": pl.Float64},
        )

    monkeypatch.setattr(events, "fetch_corporate_actions", fake_tdx)

    written = events._heal_unrecorded_ex_events(cfg, SESSIONS[-1], "run-1", None)

    assert asked["symbols"] == ["159327.SZ"]
    assert written == 1, "only the day the factor stepped on"
    staged = list((cfg.staging_root / "corporate_actions").glob("**/*.parquet"))
    rows = pl.concat([pl.read_parquet(path) for path in staged], how="diagonal_relaxed")
    assert rows["ex_date"].to_list() == [SESSIONS[2]]
    assert rows["source"].to_list() == ["tdx_protocol"]


def test_an_unreachable_peer_does_not_fail_the_day(tmp_path, monkeypatch):
    """The daily fetch has already succeeded; a best-effort repair cannot undo that."""
    cfg = _lake(
        tmp_path,
        factors={SESSIONS[0]: 1.0, SESSIONS[1]: 1.0, SESSIONS[2]: 3.0, SESSIONS[3]: 3.0},
        recorded=[],
    )

    def down(*args, **kwargs):
        raise RuntimeError("TDX unreachable")

    monkeypatch.setattr(events, "fetch_corporate_actions", down)

    assert events._heal_unrecorded_ex_events(cfg, SESSIONS[-1], "run-1", None) == 0


def test_the_audit_reports_what_the_heal_could_not_fix(tmp_path):
    from cnequity.quality.cross_checks import unrecorded_ex_event_findings

    cfg = _lake(
        tmp_path,
        factors={SESSIONS[0]: 1.0, SESSIONS[1]: 1.0, SESSIONS[2]: 3.0, SESSIONS[3]: 3.0},
        recorded=[],
    )

    (finding,) = unrecorded_ex_event_findings(cfg, SESSIONS[-1])

    assert finding["check"] == "unrecorded_ex_event"
    assert finding["factor_ratio"] == 3.0
    assert "x3.0000" in finding["message"]

    excluded = unrecorded_ex_event_findings(
        cfg, SESSIONS[-1], exclude={("159327.SZ", SESSIONS[2].isoformat())}
    )
    assert excluded == [], "the price-based check already filed this day"
