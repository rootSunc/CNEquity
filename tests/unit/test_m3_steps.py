from datetime import date

import polars as pl
import pytest

import cn_market_lake.steps  # noqa: F401
from cn_market_lake.config import Config, ScheduleGroup, WaveConfig, validate_config
from cn_market_lake.domain.schemas import validate_dataframe
from cn_market_lake.orchestrator.registry import get_step


def test_m3_steps_are_registered():
    for name in (
        "fund_flow",
        "northbound_holdings",
        "northbound_flows",
        "margin_trading",
        "valuation_metrics",
        "sector_members",
        "announcement_index",
        "dragon_tiger",
        "block_trades",
    ):
        entry = get_step(name)
        assert entry.fn is not None


def test_fund_flow_schema_normalization():
    raw = pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 28)],
            "main_net_inflow": [1_000_000.0],
            "super_large_net_inflow": [500_000.0],
            "large_net_inflow": [300_000.0],
            "medium_net_inflow": [100_000.0],
            "small_net_inflow": [100_000.0],
            "source": ["eastmoney"],
            "data_version": ["v1"],
            "fetched_at": ["2024-06-28T00:00:00+00:00"],
        }
    )
    out = validate_dataframe(raw, "fund_flow")
    assert out.height == 1


@pytest.fixture
def cfg(tmp_path):
    return Config(data_root=tmp_path / "data")


def test_step_fund_flow_writes_staging(cfg, monkeypatch):
    from cn_market_lake.steps import capital as cap
    from cn_market_lake.storage.state import StateStore

    StateStore(cfg.meta_root).set_date("fund_flow", date(2024, 6, 27))

    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [trade_date],
                "main_net_inflow": [1.0],
                "super_large_net_inflow": [0.0],
                "large_net_inflow": [0.0],
                "medium_net_inflow": [0.0],
                "small_net_inflow": [0.0],
            }
        )

    monkeypatch.setattr(cap, "fetch_fund_flow", fake_fetch)
    cfg.staging_root.mkdir(parents=True)
    result = cap.step_fund_flow(cfg, date(2024, 6, 28), "run-1", {})
    assert result["rows_written"] == 1
    staged = list(cfg.staging_root.glob("fund_flow/**/*.parquet"))
    assert len(staged) == 1


def test_valuation_snapshot_filters_to_bar_universe(cfg, monkeypatch):
    """The EastMoney clist returns delisted names with no bar; the daily snapshot
    must drop them so valuation stays in lock-step with daily_bars (audit:
    valuation_bars_orphan_symbol)."""
    from cn_market_lake.steps import fundamentals as fund
    from cn_market_lake.storage.state import StateStore

    # Bar universe: only 600519.SH has ever traded.
    bars_part = cfg.curated_root / "daily_bars" / "trade_date=2024-06-28"
    bars_part.mkdir(parents=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [date(2024, 6, 28)]}).write_parquet(
        bars_part / "part-merged.parquet"
    )

    StateStore(cfg.meta_root).set_date("valuation_metrics", date(2024, 6, 27))

    def fake_fetch(trade_date, **kwargs):
        return pl.DataFrame(
            {
                # 600519.SH trades; 000003.SZ is a delisted orphan the clist still returns.
                "symbol": ["600519.SH", "000003.SZ"],
                "trade_date": [trade_date, trade_date],
                "pe_ttm": [30.0, 1.0],
                "pb": [9.0, 0.1],
                "ps_ttm": [12.0, 0.5],
                "total_mv": [2.0e12, 1.0e8],
                "float_mv": [2.0e12, 1.0e8],
            }
        )

    monkeypatch.setattr(fund, "fetch_valuation_metrics", fake_fetch)
    cfg.staging_root.mkdir(parents=True)
    result = fund.step_valuation_metrics(cfg, date(2024, 6, 28), "run-1", {})

    assert result["rows_written"] == 1
    staged = pl.read_parquet(list(cfg.staging_root.glob("valuation_metrics/**/*.parquet")))
    assert staged["symbol"].to_list() == ["600519.SH"]


def test_validate_config_accepts_capital_group(tmp_path):
    cfg = Config(
        data_root=tmp_path / "data",
        # Explicit: the Config default is 8, which validate_config rejects on
        # macOS. This test is about the group shape, not the worker count.
        workers=1,
        daily_waves=[WaveConfig(name="core", parallel=True, steps=["instruments"])],
        schedule_groups={
            "capital": ScheduleGroup(at="16:30", steps=["fund_flow", "margin_trading"]),
        },
    )
    assert validate_config(cfg) == []


def test_northbound_reads_the_hsgt_report_not_an_index_fund_flow_kline():
    """Regression: northbound must not be sourced from a fund-flow kline.

    It used to read ``push2his /stock/fflow/kline/get?secid=1.000001`` and map
    f52/f53 onto SH/SZ. Those fields are 上证指数's 主力净流入 and 小单净流入 —
    two legs of a zero-sum decomposition, not two geographic channels — so the
    column carried plausible-looking numbers that were never northbound at all.
    """
    from cn_market_lake.adapters.eastmoney import capital

    assert capital._NORTH_FLOW_REPORT == "RPT_MUTUAL_DEAL_HISTORY"
    assert capital._NORTHBOUND_CHANNELS == {"001": "SH", "003": "SZ"}
    assert not hasattr(capital, "_FFLOW_KLINE_URL")
    assert not hasattr(capital, "_KAMT_URL")


def test_backup_snapshot_failure_does_not_abort_the_ca_backfill(tmp_path, monkeypatch):
    """Regression: a best-effort audit artifact took down the primary fetch.

    `snapshot_corporate_actions_backup` writes an EastMoney snapshot for
    cross-source audit. When EastMoney changed its filter grammar it started
    raising, and the raise propagated out of the step — aborting
    `cml backfill corporate_actions` before TDX, the actual canonical source,
    was contacted at all.
    """
    from datetime import date

    import polars as pl

    from cn_market_lake.steps import events

    monkeypatch.setattr(events, "load_symbols", lambda cfg: ["600519.SH"])

    def _boom(*a, **k):
        raise RuntimeError("EastMoney datacenter rejected schema")

    monkeypatch.setattr(events, "snapshot_corporate_actions_backup", _boom)
    fetched = {}

    def _fake_tdx(trade_date, **kwargs):
        fetched["called"] = True
        return pl.DataFrame(
            [
                {
                    "symbol": "600519.SH",
                    "ex_date": date(2024, 6, 28),
                    "action_type": "cash_dividend",
                    "cash_dividend": 1.0,
                    "bonus_ratio": 0.0,
                    "transfer_ratio": 0.0,
                    "allotment_ratio": None,
                    "allotment_price": None,
                }
            ]
        )

    monkeypatch.setattr(events, "fetch_corporate_actions", _fake_tdx)
    monkeypatch.setattr(events, "write_simple", lambda *a, **k: {"rows_read": 1, "rows_written": 1})

    cfg = Config(data_root=tmp_path / "lake")
    cfg._backfill = True
    out = events.step_corporate_actions(cfg, date(2024, 6, 28), "run-1", {})

    assert fetched.get("called") is True, "TDX must still be contacted"
    assert out["rows_written"] == 1
