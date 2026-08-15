from datetime import date

from cn_market_lake.config import Config
from cn_market_lake.quality.audit import run_audit


def test_audit_warns_when_calendar_forward_coverage_under_90_days(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    run_id = "run-cal-warn"
    trade_date = date(2027, 10, 3)

    run_audit(cfg, run_id, trade_date, {})

    import json

    payload = json.loads(
        (cfg.meta_root / "quality" / "findings" / f"{run_id}.json").read_text(encoding="utf-8")
    )
    coverage = [f for f in payload["findings"] if f.get("check") == "calendar_forward_coverage"]
    assert len(coverage) == 1
    assert coverage[0]["severity"] == "warning"
    assert coverage[0]["forward_days"] == 89
    assert coverage[0]["seed_end"] == "2027-12-31"
    assert "extend holidays_cn.py" in coverage[0]["message"]


def test_audit_silent_when_calendar_forward_coverage_adequate(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    run_id = "run-cal-ok"
    trade_date = date(2027, 10, 2)

    run_audit(cfg, run_id, trade_date, {})

    import json

    payload = json.loads(
        (cfg.meta_root / "quality" / "findings" / f"{run_id}.json").read_text(encoding="utf-8")
    )
    coverage = [f for f in payload["findings"] if f.get("check") == "calendar_forward_coverage"]
    assert coverage == []
