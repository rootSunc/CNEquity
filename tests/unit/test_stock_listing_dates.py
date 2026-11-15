import json
from datetime import date, datetime, timezone

import httpx
import polars as pl
import pytest
from tests.catalog_seed import commit_tree

from cnequity.adapters.cninfo.listings import (
    IPO_CALENDAR_URL,
    fetch_listing_calendar,
    parse_listing_calendar,
)
from cnequity.config import Config
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.repairs.stock_listing_dates import repair_stock_listing_dates
from cnequity.storage.revisions import RevisionStore


def calendar(day="2026-09-23", code="920025", plate="012046"):
    return json.dumps(
        {
            "listed": [{"seccode": code, "plate": plate, "listedTime": day}],
            "subscribe": [{"seccode": "301660", "listedTime": "2026-10-13"}],
        }
    ).encode()


def test_only_confirmed_listed_group_is_evidence():
    assert parse_listing_calendar(calendar(), date(2026, 9, 23)) == {"920025.BJ": date(2026, 9, 23)}
    assert parse_listing_calendar(b'{"listed": []}', date(2026, 9, 23)) == {}


@pytest.mark.parametrize(
    "payload", [b"{}", calendar("2026-09-24"), calendar(code="920025", plate="012015")]
)
def test_bad_identity_rejected(payload):
    with pytest.raises(ValueError):
        parse_listing_calendar(payload, date(2026, 9, 23))


def test_daily_calendar_reuses_verified_archive_and_refreshes_wrong_identity(tmp_path):
    cfg = Config(
        data_root=tmp_path / "lake", sources={"cninfo": True}, source_intervals={"cninfo": 0}
    )
    requests = []

    def serve(request):
        requests.append(request)
        assert request.url.params["queryDate"] == "2026-09-23"
        return httpx.Response(200, content=calendar())

    with httpx.Client(transport=httpx.MockTransport(serve)) as client:
        day = date(2026, 9, 23)
        assert fetch_listing_calendar(cfg, day, client=client) == {"920025.BJ": day}
        assert fetch_listing_calendar(cfg, day, client=client) == {"920025.BJ": day}
        assert len(requests) == 1
        path = cfg.meta_root / "source_cache" / "stock_listing_dates" / f"{day}.json"
        receipt = json.loads(path.read_text())
        receipt["request_params"] = {"queryDate": "2026-09-24"}
        write_json_atomic(path, receipt)
        assert fetch_listing_calendar(cfg, day, client=client) == {"920025.BJ": day}
        assert len(requests) == 2


def _rows(root):
    """The rows of a published root, whether a directory or a catalog revision."""
    from cnequity.query.parquet_scan import scan_parquet_root

    return scan_parquet_root(root, committed=False).collect()


def test_offline_repair_is_retained_and_idempotent(tmp_path, settle):
    cfg = Config(data_root=tmp_path / "lake")
    now = datetime(2026, 10, 12, tzinfo=timezone.utc)
    rows = [
        dict(
            symbol="920025.BJ",
            code="920025",
            exchange="BJ",
            name="stock",
            asset_type="stock",
            list_date=None,
            source="bse",
            data_version="v1",
            fetched_at=now,
        ),
        dict(
            symbol="301660.SZ",
            code="301660",
            exchange="SZ",
            name="pending",
            asset_type="stock",
            list_date=None,
            source="tdx_protocol",
            data_version="v1",
            fetched_at=now,
        ),
    ]
    root = cfg.curated_root / "instruments"
    root.mkdir(parents=True)
    path = root / "part-0.parquet"
    pl.DataFrame(rows, schema=DATASET_SCHEMAS["instruments"]).write_parquet(path)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    commit_tree(
        store,
        "instruments",
        run_id="seed",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="fixture",
    )
    old = store.current_root("instruments")
    bars = cfg.curated_root / "daily_bars"
    bars.mkdir(parents=True)
    path = bars / "part-0.parquet"
    pl.DataFrame(
        [
            dict(
                symbol="920025.BJ",
                trade_date=date(2026, 9, 23),
                open=10.0,
                high=11.0,
                low=9.0,
                close=10.0,
                volume=100.0,
                amount=1000.0,
                source="sina",
                data_version="v1",
                fetched_at=now,
            )
        ],
        schema=DATASET_SCHEMAS["daily_bars"],
    ).write_parquet(path)
    commit_tree(
        store,
        "daily_bars",
        run_id="seed",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="fixture",
    )
    archive = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=["instruments"])
    record = archive.archive(
        "instruments",
        calendar(),
        source="cninfo",
        run_id="proof",
        url=IPO_CALENDAR_URL,
        request_params={"queryDate": "2026-09-23"},
        payload_format="json",
    )
    cache = cfg.meta_root / "source_cache" / "stock_listing_dates" / "2026-09-23.json"
    write_json_atomic(cache, record.to_dict())
    settle(cfg)
    assert repair_stock_listing_dates(cfg)["rows"] == 1
    result = repair_stock_listing_dates(cfg, apply=True)
    assert result["applied"] and result["revision"] == 2
    current = _rows(store.current_root("instruments"))
    assert current.filter(pl.col("symbol") == "920025.BJ")["list_date"][0] == date(2026, 9, 23)
    assert current.filter(pl.col("symbol") == "301660.SZ")["list_date"][0] is None
    assert _rows(old)["list_date"].null_count() == 2
    assert repair_stock_listing_dates(cfg)["rows"] == 0
    cache.write_text("{}")
    # No unexplained row can be inferred from the bar when the proof is missing.
    assert not repair_stock_listing_dates(cfg)["applied"]


def seed_listing_catalog(tmp_path):
    cfg = Config(data_root=tmp_path / "lake")
    path = cfg.curated_root / "instruments" / "part-0.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            dict(
                symbol=symbol,
                code=symbol[:6],
                exchange=symbol[-2:],
                name="stock",
                asset_type="stock",
                list_date=None,
                source="fixture",
                data_version="v1",
                fetched_at=datetime(2026, 10, 12, tzinfo=timezone.utc),
            )
            for symbol in ("920025.BJ", "301660.SZ")
        ],
        schema=DATASET_SCHEMAS["instruments"],
    ).write_parquet(path)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    commit_tree(
        store,
        "instruments",
        run_id="seed",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="fixture",
    )
    return cfg, store


def archive_listing_day(cfg, day):
    record = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=["instruments"]).archive(
        "instruments",
        calendar(str(day)),
        source="cninfo",
        run_id="proof",
        url=IPO_CALENDAR_URL,
        request_params={"queryDate": str(day)},
        payload_format="json",
    )
    write_json_atomic(
        cfg.meta_root / "source_cache" / "stock_listing_dates" / f"{day}.json", record.to_dict()
    )


def test_explicit_event_can_fill_a_stock_without_any_bars_and_is_retained(tmp_path):
    cfg, store = seed_listing_catalog(tmp_path)
    old = store.current_root("instruments")
    day = date(2026, 9, 23)
    archive_listing_day(cfg, day)
    assert repair_stock_listing_dates(cfg)["rows"] == 0
    assert repair_stock_listing_dates(cfg, event_dates=[day])["rows"] == 1
    result = repair_stock_listing_dates(cfg, event_dates=[day], apply=True)
    assert result["applied"] and result["rows"] == 1
    current = _rows(store.current_root("instruments"))
    assert current.filter(pl.col("symbol") == "920025.BJ")["list_date"][0] == day
    assert current.filter(pl.col("symbol") == "301660.SZ")["list_date"][0] is None
    assert _rows(old)["list_date"].null_count() == 2
    assert repair_stock_listing_dates(cfg, event_dates=[day])["rows"] == 0
    assert not (cfg.curated_root / "daily_bars").exists()


def test_explicit_event_cannot_contradict_an_earlier_real_trade(tmp_path):
    cfg, store = seed_listing_catalog(tmp_path)
    day = date(2026, 9, 23)
    archive_listing_day(cfg, day)
    path = cfg.curated_root / "daily_bars" / "part-0.parquet"
    path.parent.mkdir(parents=True)
    pl.DataFrame(
        [
            dict(
                symbol="920025.BJ",
                trade_date=date(2026, 9, 22),
                volume=100.0,
                source="fixture",
                data_version="v1",
                fetched_at=datetime(2026, 10, 12, tzinfo=timezone.utc),
            )
        ],
        schema=DATASET_SCHEMAS["daily_bars"],
    ).write_parquet(path)
    commit_tree(
        store,
        "daily_bars",
        run_id="seed",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="fixture",
    )
    assert repair_stock_listing_dates(cfg, event_dates=[day], max_sessions=1)["rows"] == 0


def test_conflicting_official_listing_days_do_not_pick_a_winner(tmp_path):
    cfg, _ = seed_listing_catalog(tmp_path)
    days = [date(2026, 9, 23), date(2026, 9, 24)]
    for day in days:
        archive_listing_day(cfg, day)
    result = repair_stock_listing_dates(cfg, event_dates=days)
    assert result["rows"] == 0 and result["conflicting_symbols"] == ["920025.BJ"]
    with pytest.raises(ValueError, match="max_sessions"):
        repair_stock_listing_dates(cfg, event_dates=days, max_sessions=1)


def test_cli_explicit_event_dates_are_typed_and_repeatable(monkeypatch):
    from click.testing import CliRunner

    from cnequity.cli import repair_cmds
    from cnequity.cli.main import cli
    from cnequity.storage.repairs import stock_listing_dates as module

    captured = {}
    monkeypatch.setattr(repair_cmds, "_cfg", lambda path: "isolated-config")

    def repair(config, **kwargs):
        assert config == "isolated-config"
        captured.update(kwargs)
        return {"rows": 0}

    monkeypatch.setattr(module, "repair_stock_listing_dates", repair)
    result = CliRunner().invoke(
        cli,
        [
            "repair",
            "stock-listing-dates",
            "--event-date",
            "2026-09-23",
            "--event-date",
            "2026-09-24",
        ],
    )
    assert result.exit_code == 0, result.output
    assert captured["event_dates"] == (date(2026, 9, 23), date(2026, 9, 24))


@pytest.mark.parametrize(
    "delisted,expected", [(date(2026, 9, 1), None), (date(2026, 9, 30), date(2026, 9, 30))]
)
def test_official_listing_clears_only_an_impossible_earlier_end_date(tmp_path, delisted, expected):
    cfg, store = seed_listing_catalog(tmp_path)
    path = cfg.curated_root / "instruments" / "part-0.parquet"
    frame = pl.read_parquet(path).with_columns(pl.lit(delisted).alias("delist_date"))
    frame.write_parquet(path)
    commit_tree(
        store,
        "instruments",
        run_id="stale-end",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="fixture",
    )
    old = store.current_root("instruments")
    day = date(2026, 9, 23)
    archive_listing_day(cfg, day)
    preview = repair_stock_listing_dates(cfg, event_dates=[day])
    assert preview["cleared_invalid_delist_dates"] == (expected is None)
    result = repair_stock_listing_dates(cfg, event_dates=[day], apply=True)
    assert result["cleared_invalid_delist_dates"] == (expected is None)
    current = _rows(store.current_root("instruments"))
    assert current.filter(pl.col("symbol") == "920025.BJ")["delist_date"][0] == expected
    assert _rows(old)["delist_date"][0] == delisted
