import io
import json
from datetime import date

import pandas as pd
import polars as pl
import pytest

from cnequity.adapters.exchange.fund_listings import (
    parse_sse_fund_listings,
    parse_szse_fund_listings,
)
from cnequity.config import Config
from cnequity.steps.reference import _enrich_fund_list_dates_from_exchange


def test_sse_listings_include_lof_without_classifying_research_eligibility():
    rows = [
        {"fundCode": "501001", "fundType": "10", "listingDate": "20150925"},
        {"fundCode": "513000", "fundType": "00", "listingDate": "20260105"},
        {"fundCode": "508000", "fundType": "50", "listingDate": "20210105"},
    ]
    payload = json.dumps({"result": rows, "pageHelp": {"total": 3}}).encode()
    assert parse_sse_fund_listings(payload) == {
        "501001.SH": date(2015, 9, 25),
        "513000.SH": date(2026, 1, 5),
    }
    with pytest.raises(ValueError, match="incomplete"):
        parse_sse_fund_listings(json.dumps({"result": rows, "pageHelp": {"total": 4}}).encode())


def test_szse_listings_include_lof_and_reject_duplicate_codes():
    rows = [
        {"基金代码": "160105", "基金类别": "LOF", "上市日期": "2004-12-20"},
        {"基金代码": "159920", "基金类别": "ETF", "上市日期": "2012-10-22"},
    ]

    def payload(values):
        stream = io.BytesIO()
        pd.DataFrame(values).to_excel(stream, index=False)
        return stream.getvalue()

    assert parse_szse_fund_listings(payload(rows)) == {
        "160105.SZ": date(2004, 12, 20),
        "159920.SZ": date(2012, 10, 22),
    }
    with pytest.raises(ValueError, match="duplicates"):
        parse_szse_fund_listings(payload(rows + rows[:1]))


def test_exchange_dates_fill_only_null_funds_and_do_not_activate_future_listings(
    tmp_path, monkeypatch
):
    cfg = Config(data_root=tmp_path / "lake")
    df = pl.DataFrame(
        {
            "symbol": ["501001.SH", "159920.SZ", "159921.SZ", "600001.SH"],
            "asset_type": ["etf", "etf", "etf", "stock"],
            "list_date": [None, date(2012, 10, 22), None, None],
        },
        schema_overrides={"list_date": pl.Date},
    )
    monkeypatch.setattr(
        "cnequity.adapters.exchange.fund_listings.fetch_exchange_fund_listings",
        lambda *args, **kwargs: (
            {
                "501001.SH": date(2015, 9, 25),
                "159920.SZ": date(2020, 1, 1),
                "159921.SZ": date(2026, 10, 12),
                "600001.SH": date(2000, 1, 1),
            },
            [],
        ),
    )
    result, findings = _enrich_fund_list_dates_from_exchange(cfg, df, date(2026, 10, 11), "test")
    assert result["list_date"].to_list() == [date(2015, 9, 25), date(2012, 10, 22), None, None]
    assert findings == []


def _rows(root):
    """The rows of a published root, whether a directory or a catalog revision."""
    from cnequity.query.parquet_scan import scan_parquet_root

    return scan_parquet_root(root, committed=False).collect()


def test_listing_repair_preserves_known_dates_and_old_revision(tmp_path, monkeypatch, settle):
    from datetime import datetime, timezone

    from cnequity.domain.schemas import DATASET_SCHEMAS
    from cnequity.storage.repairs.fund_listing_dates import repair_fund_listing_dates
    from cnequity.storage.revisions import RevisionStore

    cfg = Config(data_root=tmp_path / "lake")
    path = cfg.curated_root / "instruments" / "part-merged.parquet"
    path.parent.mkdir(parents=True)
    rows = [
        dict(
            symbol="501001.SH",
            code="501001",
            exchange="SH",
            name="LOF",
            asset_type="etf",
            list_date=None,
            source="tdx_protocol",
            data_version="v1",
            fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
        dict(
            symbol="159920.SZ",
            code="159920",
            exchange="SZ",
            name="ETF",
            asset_type="etf",
            list_date=date(2012, 10, 22),
            source="tdx_protocol",
            data_version="v1",
            fetched_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
    ]
    pl.DataFrame(rows, schema=DATASET_SCHEMAS["instruments"]).write_parquet(path)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    store.commit(
        "instruments",
        run_id="seed",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="fixture",
    )
    old = store.current_root("instruments")
    monkeypatch.setattr(
        "cnequity.storage.repairs.fund_listing_dates.fetch_exchange_fund_listings",
        lambda *a, **k: ({"501001.SH": date(2015, 9, 25), "159920.SZ": date(2020, 1, 1)}, []),
    )
    settle(cfg)
    preview = repair_fund_listing_dates(cfg)
    assert preview["rows"] == 1 and not preview["applied"]
    result = repair_fund_listing_dates(cfg, apply=True)
    assert result["applied"] and result["revision"] == 2
    current = _rows(store.current_root("instruments")).sort("symbol")
    assert current["list_date"].to_list() == [date(2012, 10, 22), date(2015, 9, 25)]
    assert _rows(old)["list_date"].null_count() == 1
    assert repair_fund_listing_dates(cfg)["rows"] == 0
