from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.quality.cross_checks import financial_statement_peer_findings
from cnequity.storage.source_snapshots import SnapshotStore


def _row(value, *, announced=date(2025, 4, 20), statement="income"):
    return {
        "symbol": "600519.SH",
        "report_period": "2024Q4",
        "statement_type": statement,
        "item_code": "net_profit",
        "item_value": float(value),
        "announce_date": announced,
        "source": "eastmoney",
        "data_version": "v1",
        "fetched_at": datetime(2025, 9, 1, tzinfo=timezone.utc),
    }


def _findings(tmp_path, primary, peer):
    cfg = Config(data_root=tmp_path)
    root = cfg.curated_root / "financial_statement_items" / "report_period=2024Q4"
    root.mkdir(parents=True)
    pl.DataFrame(primary).write_parquet(root / "part.parquet")
    SnapshotStore(cfg.meta_root).write(
        "financial_statement_items",
        pl.DataFrame(peer).with_columns(pl.lit("ths_official").alias("source")),
        source="ths_official",
        data_version="v1",
        run_id="test",
    )
    return financial_statement_peer_findings(cfg)


@pytest.mark.parametrize("value", [10.0, -10.0])
def test_zero_primary_does_not_hide_nonzero_peer(tmp_path, value):
    (finding,) = _findings(tmp_path, [_row(0)], [_row(value)])
    assert finding["disagreed"] == 1
    assert finding["compared"] == 1


def test_two_zero_values_agree(tmp_path):
    assert _findings(tmp_path, [_row(0)], [_row(0)]) == []


@pytest.mark.parametrize("reverse", [False, True])
def test_peer_latest_disclosure_is_independent_of_file_order(tmp_path, reverse):
    peer = [_row(100), _row(120, announced=date(2025, 8, 30))]
    if reverse:
        peer.reverse()
    assert _findings(tmp_path, [_row(120, announced=date(2025, 8, 30))], peer) == []


def test_statement_types_are_not_joined_together(tmp_path):
    assert _findings(tmp_path, [_row(100)], [_row(200, statement="cashflow")]) == []


def _revenue(value):
    return {**_row(value), "item_code": "revenue"}


def test_peer_revenue_below_the_lakes_total_is_not_a_disagreement(tmp_path):
    # The lake's revenue is 营业总收入; the peer's 营业收入 leaves out a finance
    # subsidiary's interest income, so it can only be the smaller figure.
    assert _findings(tmp_path, [_revenue(100)], [_revenue(90)]) == []


def test_peer_revenue_above_the_lakes_total_still_disagrees(tmp_path):
    (finding,) = _findings(tmp_path, [_revenue(100)], [_revenue(110)])
    assert finding["disagreed"] == 1


def test_restated_comparatives_are_reported_apart(tmp_path):
    # A later annual report restates 2024Q4; the peer shows the restated figure
    # under the first disclosure date, the lake the figure first disclosed.
    peer = [_row(150), {**_row(0), "report_period": "2025Q4"}]
    findings = _findings(tmp_path, [_row(100)], peer)
    assert [f["check"] for f in findings] == ["financial_statement_peer_restated"]
    assert findings[0]["severity"] == "info"
    assert findings[0]["disagreed"] == 1


def test_a_correction_the_lake_recorded_is_reported_apart(tmp_path):
    # 000909.SZ: FY2024 corrected on 2026-09-28 (前期会计差错更正); the lake holds
    # both vintages, the peer only the first filing, and 2024Q4 is still the
    # latest annual report, so only the lake's own vintages show the change.
    lake = [_row(100), _row(99, announced=date(2026, 9, 28))]
    findings = _findings(tmp_path, lake, [_row(100)])
    assert [f["check"] for f in findings] == ["financial_statement_peer_restated"]
