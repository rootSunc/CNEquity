"""The read-only dashboard API."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import polars as pl
import pytest
from fastapi.testclient import TestClient

from cnequity.serve.app import create_app
from cnequity.storage.stats import rebuild_stats

FETCHED = datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc)


def _write(root, partition: str | None, rows: list[dict]) -> None:
    target = root if partition is None else root / partition
    target.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(target / "part-0.parquet")


def _meta(source: str = "exchange_calendar") -> dict:
    return {"source": source, "data_version": "v1", "fetched_at": FETCHED}


def _row(symbol: str, day: date, source: str = "tdx_protocol", **overrides) -> dict:
    # trade_date is not decoration: with no curated trading_calendar,
    # is_trading_day derives the sessions from daily_bars itself.
    return {
        "symbol": symbol,
        "trade_date": day,
        "source": source,
        "data_version": "v2",
        "fetched_at": FETCHED,
        **overrides,
    }


def _full_row(dataset: str, **values) -> dict:
    """A row carrying every column the dataset's schema declares.

    ``load()`` validates against the contract, so a fixture row with four
    columns fails there for a reason that has nothing to do with the test.
    Built from DATASET_SCHEMAS rather than hand-written, so a schema change
    cannot leave these fixtures quietly wrong.
    """
    from cnequity.domain.schemas import DATASET_SCHEMAS

    blank = {
        pl.Utf8: "x",
        pl.Date: date(2026, 7, 31),
        pl.Boolean: False,
    }
    row = {}
    for col, dtype in DATASET_SCHEMAS[dataset].items():
        if isinstance(dtype, pl.Datetime):
            row[col] = FETCHED
        elif dtype in blank:
            row[col] = blank[dtype]
        else:
            row[col] = 0
    if dataset in {"daily_bars", "index_bars", "minute_bars", "minute_bars_5m"}:
        row.update({"open": 9.0, "high": 11.0, "low": 8.0, "close": 10.0, "volume": 100})
        if "amount" in row:
            row["amount"] = 1_000.0
    row["source"] = "tdx_protocol"
    row["data_version"] = "v2"
    return {**row, **values}


# The lake below is dated, and freshness is judged against the last trading day
# *today*. Left to the wall clock these tests pass on the day they are written
# and report every dataset stale from then on — which is what happened. The
# fixture dates are the readable part of every assertion in this file, so the
# clock is what gets pinned, not the data.
FROZEN_TODAY = date(2026, 7, 31)


class _FrozenDate(date):
    @classmethod
    def today(cls) -> date:
        return FROZEN_TODAY


@pytest.fixture
def lake(config, monkeypatch):
    """A tiny lake spanning several dataset shapes, measured.

    Rows carry their full schema so ``load()`` — which validates against the
    contract — can read them; the browsing tests go through it.
    """
    monkeypatch.setattr("cnequity.serve.lake.date", _FrozenDate)
    monkeypatch.setattr("cnequity.serve.lake.shanghai_today", lambda: FROZEN_TODAY)
    last, prev = FROZEN_TODAY, FROZEN_TODAY - timedelta(days=1)
    bar = lambda sym, day, src="tdx_protocol": _full_row(  # noqa: E731
        "daily_bars", symbol=sym, trade_date=day, source=src
    )
    _write(config.curated_root / "daily_bars", f"trade_date={last}", [bar("600519.SH", last)])
    _write(
        config.curated_root / "daily_bars",
        f"trade_date={prev}",
        [bar("600519.SH", prev), bar("000001.SZ", prev, "ths")],
    )
    _write(
        config.curated_root / "instruments",
        None,
        [_full_row("instruments", symbol="600519.SH")],
    )
    _write(
        config.curated_root / "financial_statement_items",
        "report_period=2026Q1",
        [
            _full_row(
                "financial_statement_items",
                symbol="600519.SH",
                report_period="2026Q1",
                announce_date=date(2026, 4, 20),
            )
        ],
    )
    # The heatmap's x-axis is the trading calendar; without one it has no days
    # to draw and every assertion about cells would pass vacuously.
    _write(
        config.curated_root / "trading_calendar",
        "trade_date=2026",
        [
            {"trade_date": prev, "is_trading": True, **_meta()},
            {"trade_date": last, "is_trading": True, **_meta()},
        ],
    )
    rebuild_stats(config)
    return config


@pytest.fixture
def client(lake):
    return TestClient(create_app(lake))


def test_health_counts_every_registered_dataset(client):
    body = client.get("/api/health").json()
    from cnequity.domain.datasets import DATASETS

    assert body["datasets"] == len(DATASETS)
    assert body["fresh"] + body["stale"] + body["empty"] + body["not_applicable"] == len(DATASETS)
    # 3 daily_bars + 1 instruments + 2 trading_calendar + 1 fsi
    assert body["rows"] == 7


def test_health_separates_optional_and_required_empties(client):
    """An opt-in dataset nobody enabled looks identical on disk to a failure."""
    body = client.get("/api/health").json()
    assert "minute_bars" in body["empty_optional"]
    assert "minute_bars" not in body["empty_required"]
    assert not set(body["empty_optional"]) & set(body["empty_required"])


def test_disabled_optional_historical_capture_is_not_stale(lake):
    part = lake.curated_root / "trade_ticks" / "trade_date=2026-07-01"
    part.mkdir(parents=True, exist_ok=True)
    _write(
        lake.curated_root / "trade_ticks",
        "trade_date=2026-07-01",
        [_full_row("trade_ticks", symbol="600519.SH", trade_date=date(2026, 7, 1))],
    )
    rebuild_stats(lake, datasets=["trade_ticks"])

    app_client = TestClient(create_app(lake))
    body = app_client.get("/api/health").json()
    assert "trade_ticks" not in body["stale_datasets"]
    row = next(r for r in app_client.get("/api/datasets").json() if r["dataset"] == "trade_ticks")
    assert row["freshness"] == "n/a"


def test_tiers_partition_the_datasets(client):
    tiers = client.get("/api/tiers").json()
    datasets = client.get("/api/datasets").json()
    members = [name for tier in tiers for name in tier["members"]]
    assert sorted(members) == sorted(d["dataset"] for d in datasets)
    assert len(members) == len(set(members))
    for tier in tiers:
        assert tier["datasets"] == len(tier["members"])


def test_tier_rows_sum_to_the_lake_total(client):
    tiers = client.get("/api/tiers").json()
    health = client.get("/api/health").json()
    assert sum(t["rows"] for t in tiers) == health["rows"]
    assert sum(t["bytes"] for t in tiers) == health["bytes"]


def test_datasets_can_be_filtered_to_one_tier(client):
    rows = client.get("/api/datasets", params={"tier": "L1"}).json()
    assert rows
    assert {r["tier"] for r in rows} == {"L1"}
    assert "daily_bars" in {r["dataset"] for r in rows}


def test_dataset_rows_carry_registry_and_measurement(client):
    rows = {r["dataset"]: r for r in client.get("/api/datasets").json()}
    bars = rows["daily_bars"]
    assert (bars["tier"], bars["granularity"], bars["freshness"]) == ("L1", "day", "fresh")
    assert bars["row_count"] == 3
    assert rows["instruments"]["granularity"] is None  # merge-style


def test_provenance_splits_one_dataset_by_source(client):
    rows = client.get("/api/datasets/daily_bars/provenance").json()
    assert {r["source"]: r["row_count"] for r in rows} == {"tdx_protocol": 2, "ths": 1}
    assert all(r["data_version"] == "v2" for r in rows)


def test_provenance_rejects_an_unregistered_dataset(client):
    assert client.get("/api/datasets/nope/provenance").status_code == 404


# --- one dataset -------------------------------------------------------------


def test_detail_carries_the_registry_contract(client):
    d = client.get("/api/datasets/daily_bars").json()
    assert d["primary_key"] == ["symbol", "trade_date"]
    assert d["partition_col"] == "trade_date"
    assert d["max_staleness_days"] == 1
    columns = {c["column"]: c["dtype"] for c in d["schema"]}
    assert columns["trade_date"] == "Date"
    assert columns["source"] == "String"


def test_detail_reports_the_source_floor_not_the_backlog(client):
    """history_horizon_days is the vendor's floor; earlier windows return nothing."""
    intraday = client.get("/api/datasets/minute_bars").json()
    assert intraday["history_horizon_days"] == 95
    assert intraday["earliest_available"] is not None

    daily = client.get("/api/datasets/daily_bars").json()
    assert daily["history_horizon_days"] is None
    assert daily["earliest_available"] is None


def test_row_grain_marks_intraday_data_that_is_not_bars(client):
    """trade_ticks is intraday but holds no bars.

    The catalog used to expose only `intraday`, the behavioural field, which
    trade_ticks leaves unset on purpose so it cannot inherit bar-shaped checks
    — so the panel showed a dash and the dataset read as daily.
    """
    ticks = client.get("/api/datasets/trade_ticks").json()
    assert ticks["row_grain"] == "tick"
    assert ticks["intraday"] is None

    bars = client.get("/api/datasets/minute_bars").json()
    assert bars["row_grain"] == bars["intraday"] == "1m"

    assert client.get("/api/datasets/daily_bars").json()["row_grain"] is None


def test_detail_distinguishes_a_fixed_floor_from_a_rolling_horizon(client):
    """A date-limited source must not read as unlimited.

    Both mechanisms produce an `earliest_available`, so that field alone cannot
    tell them apart. Without `history_floor_date`, the dashboard keyed on
    `history_horizon_days` and printed 无上限 for trade_ticks — directly
    contradicting the 最早可得 line beside it.
    """
    ticks = client.get("/api/datasets/trade_ticks").json()
    assert ticks["history_floor_date"] == "2024-01-02"
    assert ticks["history_horizon_days"] is None
    assert ticks["earliest_available"] == "2024-01-02"

    # A rolling horizon carries no floor date, and an unlimited source neither.
    assert client.get("/api/datasets/minute_bars").json()["history_floor_date"] is None
    assert client.get("/api/datasets/daily_bars").json()["history_floor_date"] is None


def test_detail_omits_the_partition_series(client):
    """6,202 partitions must not ride along on every tab switch."""
    assert "partitions_detail" not in client.get("/api/datasets/daily_bars").json()
    series = client.get("/api/datasets/daily_bars/partitions").json()
    assert len(series) == 2
    assert {p["partition"] for p in series} == {"2026-07-30", "2026-07-31"}


def test_gaps_are_counted_in_the_datasets_own_period(client):
    """A year-partitioned dataset is not missing 364 days per directory."""
    for name in ("daily_bars", "financial_statement_items", "corporate_actions"):
        gaps = client.get(f"/api/datasets/{name}").json()["gaps"]
        assert gaps["unit"] in {"day", "month", "quarter", "year"}
        assert len(gaps["missing"]) <= 60
        assert gaps["total"] >= len(gaps["missing"])


def test_gaps_ignore_non_sessions(client):
    """Only trading days count; the fixture's two sessions are adjacent."""
    assert client.get("/api/datasets/daily_bars").json()["gaps"]["total"] == 0


def test_detail_names_the_fix_without_offering_to_run_it(client):
    commands = client.get("/api/datasets/daily_bars").json()["commands"]
    assert commands
    assert all(c["cmd"].startswith("cne ") and c["why"] for c in commands)


def test_provenance_series_buckets_and_says_so(client):
    body = client.get("/api/datasets/daily_bars/provenance/series").json()
    assert body["bucket"] in {"day", "month", "year"}
    assert {p["source"] for p in body["points"]} == {"tdx_protocol", "ths"}
    assert sum(p["row_count"] for p in body["points"]) == 3


def test_provenance_series_stays_chartable_on_a_long_history(config):
    """11k daily points would be a megabyte of JSON to draw a few hundred px."""
    from cnequity.serve.lake import LakeView

    root = config.curated_root / "daily_bars"
    for year in range(2001, 2027):
        for month in (1, 7):
            day = date(year, month, 1)
            _write(root, f"trade_date={day}", [_row("600519.SH", day)])
    rebuild_stats(config, datasets=["daily_bars"])

    series = LakeView(config).provenance_series("daily_bars", max_buckets=20)
    assert series["bucket"] == "year"
    assert len(series["points"]) <= 30


def test_detail_and_series_reject_an_unregistered_dataset(client):
    assert client.get("/api/datasets/nope").status_code == 404
    assert client.get("/api/datasets/nope/partitions").status_code == 404
    assert client.get("/api/datasets/nope/provenance/series").status_code == 404


def test_heatmap_cells_are_one_char_per_day(client):
    body = client.get("/api/heatmap", params={"days": 5}).json()
    width = len(body["days"])
    assert width <= 5
    for row in body["rows"]:
        assert len(row["cells"]) == width
        assert set(row["cells"]) <= set(body["legend"])


def test_heatmap_marks_unpartitioned_datasets_apart_from_gaps(client):
    """instruments has no per-day notion; that is not the same as missing."""
    rows = {r["dataset"]: r for r in client.get("/api/heatmap").json()["rows"]}
    assert set(rows["instruments"]["cells"]) == {"-"}
    assert rows["instruments"]["granularity"] is None


def test_heatmap_says_whether_a_gap_is_a_fault_or_the_datasets_shape(client):
    """Only a daily by_date dataset can honestly be missing a day it should have."""
    rows = {r["dataset"]: r for r in client.get("/api/heatmap").json()["rows"]}

    # Daily and replayable — a hole here is real.
    assert rows["daily_bars"]["gap_meaning"] == "fault"
    assert rows["daily_bars"]["cadence_days"] == 1

    # Quarterly: nearly every session in its span legitimately has no partition.
    assert rows["northbound_holdings"]["gap_meaning"] == "cadence"
    assert rows["northbound_holdings"]["cadence_days"] == 100

    # Snapshot: a day nobody ran has no snapshot and cannot be given one,
    # because replaying it would forge rows.
    for name in ("fund_flow", "sector_members", "industry_members"):
        assert rows[name]["gap_meaning"] == "cadence", name


def test_every_snapshot_dataset_is_exempt_from_fault_gaps(client):
    """The rule is read off fetch_semantics, not a hand-kept list of names."""
    from cnequity.domain.datasets import DATASETS

    rows = {r["dataset"]: r for r in client.get("/api/heatmap").json()["rows"]}
    for name, spec in DATASETS.items():
        expected = (
            "cadence"
            if spec.fetch_semantics == "snapshot" or spec.max_staleness_days > 1
            else "fault"
        )
        assert rows[name]["gap_meaning"] == expected, name


def test_heatmap_rejects_an_absurd_window(client):
    assert client.get("/api/heatmap", params={"days": 0}).status_code == 422
    assert client.get("/api/heatmap", params={"days": 10_000}).status_code == 422


# --- browsing rows -----------------------------------------------------------


def test_the_date_control_is_chosen_per_dataset_shape(client):
    """Twelve date columns in four shapes; one picker would misfit most of them."""
    kinds = {
        "daily_bars": "trading_day",
        "financial_statement_items": "report_period",
        "corporate_actions": "period",
        "announcement_index": "event_day",
        "instruments": "none",
    }
    for name, kind in kinds.items():
        body = client.get(f"/api/datasets/{name}/dates").json()
        assert body["kind"] == kind, name


def test_only_dates_that_exist_are_offered(client):
    body = client.get("/api/datasets/daily_bars/dates").json()
    assert body["values"] == ["2026-07-31", "2026-07-30"]
    assert body["total"] == 2


def test_snapshot_only_datasets_say_why_a_missing_day_stays_missing(client):
    note = client.get("/api/datasets/fund_flow/dates").json()["note"]
    assert "snapshot_only" in note


def test_merge_style_datasets_offer_no_date_control(client):
    body = client.get("/api/datasets/instruments/dates").json()
    assert body["values"] == []
    assert "merge" in body["note"]


def test_rows_keep_the_provenance_columns(client):
    """Row-level provenance is the point of this lake; a viewer hiding it lies."""
    page = client.get("/api/datasets/daily_bars/rows", params={"period": "2026-07-30"}).json()
    assert {"source", "data_version", "fetched_at"} <= set(
        page.columns if False else page["columns"]
    )
    assert page["total"] == 2


def test_rows_filter_by_symbol(client):
    page = client.get(
        "/api/datasets/daily_bars/rows", params={"period": "2026-07-30", "symbol": "600519.SH"}
    ).json()
    assert page["total"] == 1
    symbol_at = page["columns"].index("symbol")
    assert page["rows"][0][symbol_at] == "600519.SH"


def test_rows_page_without_rescanning_the_whole_dataset(client):
    first = client.get(
        "/api/datasets/daily_bars/rows", params={"period": "2026-07-30", "limit": 1}
    ).json()
    second = client.get(
        "/api/datasets/daily_bars/rows", params={"period": "2026-07-30", "limit": 1, "offset": 1}
    ).json()
    assert first["total"] == second["total"] == 2
    assert len(first["rows"]) == len(second["rows"]) == 1
    assert first["rows"] != second["rows"]


def test_rows_are_capped(client):
    assert client.get("/api/datasets/daily_bars/rows", params={"limit": 5000}).status_code == 422
    assert client.get("/api/datasets/daily_bars/rows", params={"limit": 0}).status_code == 422


def test_a_period_that_is_not_a_period_is_refused(client):
    r = client.get("/api/datasets/daily_bars/rows", params={"period": "junk"})
    assert r.status_code == 422
    assert "junk" in r.json()["detail"]


def test_pit_datasets_refuse_a_query_without_a_cutoff(client):
    """load() has always required as_of there; the viewer must not paper over it."""
    r = client.get("/api/datasets/financial_statement_items/rows", params={"period": "2026Q1"})
    assert r.status_code == 422
    assert "as_of" in r.json()["detail"]


def test_report_period_filters_by_value_not_by_date_range(client):
    """report_period is a String column; a date range over it compares text to dates."""
    ok = client.get(
        "/api/datasets/financial_statement_items/rows",
        params={"period": "2026Q1", "as_of": "2026-07-31"},
    )
    assert ok.status_code == 200


def test_merge_style_datasets_browse_without_a_period(client):
    page = client.get("/api/datasets/instruments/rows").json()
    assert page["total"] == 1
    assert "symbol" in page["columns"]


def test_only_adjustable_datasets_advertise_adjustment(client):
    assert client.get("/api/datasets/daily_bars").json()["adjustable"] is True
    assert client.get("/api/datasets/fund_flow").json()["adjustable"] is False
    assert client.get("/api/datasets/fund_flow/rows", params={"adjust": "hfq"}).status_code in (
        200,
        422,
    )


def test_rows_reject_an_unknown_adjustment(client):
    assert client.get("/api/datasets/daily_bars/rows", params={"adjust": "nope"}).status_code == 422


# --- the read-only and auth contracts ----------------------------------------


def test_no_route_can_mutate_the_lake(client):
    """The dashboard shows; the CLI acts. Guard the boundary, not the intent."""
    methods = set()
    for route in client.app.routes:
        methods |= set(getattr(route, "methods", set()))
    assert methods <= {"GET", "HEAD"}, f"a mutating method is routed: {methods}"


def test_a_token_is_required_when_one_is_configured(lake):
    client = TestClient(create_app(lake, token="s3cret"))
    assert client.get("/api/health").status_code == 401
    assert client.get("/api/health", headers={"Authorization": "Bearer s3cret"}).status_code == 200
    # The page fetches with a query token; a browser cannot set a header.
    assert client.get("/api/health", params={"token": "s3cret"}).status_code == 200
    assert client.get("/api/health", params={"token": "wrong"}).status_code == 401


def test_the_page_reaches_nothing_outside_this_host(client):
    """No CDN. A lake behind a proxy would render a page that never loads."""
    body = client.get("/").text
    assert "<title>CNEquity</title>" in body
    for external in ("http://", "https://", "//cdn", 'src="//'):
        assert external not in body, f"page reaches outside for {external!r}"


def test_the_bundle_ships_beside_the_page(client):
    """The page is a shell; without the bundle it renders nothing at all."""
    assert "/static/bundle.js" in client.get("/").text
    bundle = client.get("/static/bundle.js")
    assert bundle.status_code == 200
    assert "javascript" in bundle.headers["content-type"]
    # Proof it is the real build rather than a stub left behind.
    assert len(bundle.content) > 100_000


def test_the_stylesheet_ships_beside_the_page(client):
    """The dashboard remains self-contained while keeping CSS out of HTML."""
    body = client.get("/").text
    assert "/static/styles.css" in body
    assert "<style>" not in body
    stylesheet = client.get("/static/styles.css")
    assert stylesheet.status_code == 200
    assert "text/css" in stylesheet.headers["content-type"]
    assert len(stylesheet.content) > 1_000


def test_static_serves_only_what_is_packaged(client):
    assert client.get("/static/nope.js").status_code == 404
    assert client.get("/static/../../pyproject.toml").status_code in (403, 404)


def test_a_non_loopback_bind_demands_a_token():
    from click.testing import CliRunner

    from cnequity.cli.main import cli

    result = CliRunner().invoke(cli, ["serve", "--host", "0.0.0.0", "--config", "nope.toml"])
    assert result.exit_code != 0
    # Fails on the bind guard, not later on the missing config.
    assert "--token" in result.output


def test_stats_are_refreshed_in_the_background_when_the_lake_moves(lake):
    from cnequity.orchestrator.manifest import Manifest
    from cnequity.serve.lake import LakeView

    view = LakeView(lake)
    assert view.refresh_stats_in_background() is False  # already current

    Manifest(lake.manifest_path).start_run("daily")
    assert view.refresh_stats_in_background() is True


def test_an_unmeasured_lake_still_answers(config):
    """No meta/stats yet: rows are unknown, but nothing errors.

    Driven through LakeView rather than the endpoint because /api/health kicks
    off the background rebuild, which would race this to the assertion.
    """
    from cnequity.serve.lake import LakeView

    day = date(2026, 7, 31)
    _write(config.curated_root / "daily_bars", f"trade_date={day}", [_row("600519.SH", day)])
    view = LakeView(config)

    assert view.health()["rows"] == 0
    rows = {r["dataset"]: r for r in view.datasets()}
    assert rows["daily_bars"]["has_data"] is True
    assert rows["daily_bars"]["row_count"] is None
    assert view.provenance("daily_bars") == []


def test_dates_serialise_as_plain_iso_days(client):
    body = client.get("/api/health").json()
    assert date.fromisoformat(body["anchor"])


def test_static_asset_urls_are_stamped_so_an_upgrade_is_not_served_from_cache(client):
    """StaticFiles sends no Cache-Control, so the URL has to change instead.

    Without this an upgraded install can pair a new API with the browser's
    cached copy of the old JavaScript — a failure neither side can explain.
    """
    import re

    body = client.get("/").text
    for asset in ("bundle.js", "styles.css"):
        match = re.search(rf"/static/{re.escape(asset)}\?v=(\d+)", body)
        assert match, f"{asset} URL is not version-stamped"
        assert client.get(f"/static/{asset}?v={match.group(1)}").status_code == 200


# --- quality -----------------------------------------------------------------


def _quality(config, kind: str, name: str, payload: dict) -> None:
    import json as _json

    root = config.meta_root / "quality" / kind
    root.mkdir(parents=True, exist_ok=True)
    (root / f"{name}.json").write_text(_json.dumps(payload), encoding="utf-8")


RUN_A = "11111111-1111-1111-1111-111111111111"


def test_quality_summarises_findings_by_severity(lake):
    _quality(
        lake,
        "findings",
        RUN_A,
        {
            "run_id": RUN_A,
            "trade_date": "2026-07-31",
            "findings": [
                {"severity": "error", "dataset": "daily_bars", "check": "exists"},
                {"severity": "error", "dataset": "index_bars", "check": "exists"},
                {"severity": "info", "dataset": "daily_bars", "check": "row_count"},
            ],
        },
    )
    body = TestClient(create_app(lake)).get("/api/quality").json()
    run = body["findings_runs"][0]
    assert run["total"] == 3
    assert run["by_severity"] == {"error": 2, "info": 1}
    assert run["top_checks"][0] == ["exists", 2]


def test_quality_skips_artefacts_that_are_not_per_run(lake):
    """meta/quality also holds authority-<date>.json, a different shape."""
    _quality(lake, "findings", RUN_A, {"trade_date": "2026-07-31", "findings": []})
    _quality(lake, "findings", "authority-2026-08-02", {"totally": "different"})

    body = TestClient(create_app(lake)).get("/api/quality").json()
    assert [r["run_id"] for r in body["findings_runs"]] == [RUN_A]


def test_quality_lists_quarantine_with_its_size(lake):
    """Evidence, not rubbish — sizing it is how you decide to keep it."""
    entry = lake.data_root / "_quarantine" / "sector_bars_2026-07-16"
    entry.mkdir(parents=True, exist_ok=True)
    (entry / "part.parquet").write_bytes(b"x" * 2048)

    body = TestClient(create_app(lake)).get("/api/quality").json()
    assert body["quarantine"] == [
        {
            "name": "sector_bars_2026-07-16",
            "files": 1,
            "bytes": 2048,
            "modified": body["quarantine"][0]["modified"],
        }
    ]


def test_an_empty_on_demand_cache_is_not_an_error(lake):
    """Nobody has queried one yet; that is a normal state."""
    assert TestClient(create_app(lake)).get("/api/quality").json()["on_demand"] == []


def test_quality_run_returns_findings_and_diffs_together(lake):
    _quality(lake, "findings", RUN_A, {"trade_date": "2026-07-31", "findings": [{"a": 1}]})
    _quality(
        lake,
        "source_diffs",
        RUN_A,
        {"trade_date": "2026-07-31", "diff_count": 2, "diffs": [{"b": 1}, {"b": 2}]},
    )
    client = TestClient(create_app(lake))

    detail = client.get(f"/api/quality/runs/{RUN_A}").json()
    assert len(detail["findings"]) == 1
    assert len(detail["diffs"]) == 2
    assert detail["trade_date"] == "2026-07-31"
    assert client.get("/api/quality/runs/nope").status_code == 404


def test_heatmap_cell_coverage_is_exact_at_the_interval_edges(client, lake):
    """Guards the bisect rewrite of the cell loop.

    The original tested every trading day against every partition interval —
    O(datasets × intervals × days), and a day-partitioned dataset contributes
    one interval per session. On a real lake (daily_bars ~6,200 partitions) the
    endpoint took 0.1s at days=60 and 24-67s at days=250, which is the whole
    first paint of the dashboard, blank, with no loading state. Binary search
    made it 0.02s at the 750-day maximum. Cells must be identical.
    """
    body = client.get("/api/heatmap", params={"days": 30}).json()
    days = body["days"]
    rows = {r["dataset"]: r for r in body["rows"]}
    covered_char = next(c for c, label in body["legend"].items() if label == "covered")

    row = rows["daily_bars"]
    assert len(row["cells"]) == len(days)
    # Recompute independently from the partition spans and compare char for char.
    from cnequity.storage.stats import load_partition_stats

    stats = load_partition_stats(lake)
    spans = [
        (r["period_start"], r["period_end"])
        for r in stats.iter_rows(named=True)
        if r["dataset"] == "daily_bars" and r["period_start"] and r["period_end"]
    ]
    # The API serialises days as ISO strings; partition spans are date objects.
    parsed = [date.fromisoformat(d) if isinstance(d, str) else d for d in days]
    expected = {i for i, d in enumerate(parsed) for s, e in spans if s <= d <= e}
    actual = {i for i, c in enumerate(row["cells"]) if c == covered_char}
    assert actual == expected
