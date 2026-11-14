"""Replay and publication use only isolated lakes and synthetic source bytes."""

import hashlib
import io
from datetime import date, datetime, timezone

import httpx
import pandas as pd
import polars as pl
import pytest

from cnequity.config import Config
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.repairs import index_history as history
from cnequity.storage.revisions import RevisionStore


def fixture_catalog():
    members = [f"{code:06d}.SZ" for code in range(1, 501)]
    buffer = io.BytesIO()
    pd.DataFrame(
        {
            "证券代码": [member[:6] for member in members],
            "公告时间：2015-3-20 正式实施时间：2015-5-20": [""] * 500,
        }
    ).to_excel(buffer, index=False)
    opening = buffer.getvalue()
    change = (
        "决定于2015年7月1日进行调整。深证成份指数样本股调整名单 "
        "调入名单 调出名单 证券代码 证券简称 证券代码 证券简称 "
        "000501 入选 000001 调出 创业板指数样本股调整名单"
    ).encode()

    def spec(payload, name):
        return {
            "url": f"https://www.cnindex.com.cn/{name}",
            "request_params": {},
            "source": "cni",
            "sha256": hashlib.sha256(payload).hexdigest(),
            "format": "html" if name.endswith(".html") else "xlsx",
        }

    catalog = {
        "schema_version": 1,
        "catalog_version": "isolated-fixture",
        "member_count": 500,
        "index_symbol": "399001.SZ",
        "opening_date": "2015-05-20",
        "closing_date": "2015-09-30",
        "opening": spec(opening, "opening.xlsx"),
        "closing_reference": {
            "url": "https://www.cnindex.com.cn/reference",
            "request_params": {"indexcode": "399001"},
            "publisher_payload_sha256": "0" * 64,
            "members": members[1:] + ["000501.SZ"],
        },
        "events": [
            {
                "effective_date": "2015-07-01",
                "parser": "announcement",
                "payload": spec(change, "change.html"),
            }
        ],
    }
    return catalog, [opening, change]


def archive_sources(cfg, catalog, payloads):
    archive = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=["index_constituents"])
    for spec, payload in zip(
        [catalog["opening"], catalog["events"][0]["payload"]], payloads, strict=True
    ):
        archive.archive(
            "index_constituents",
            payload,
            source=spec["source"],
            run_id="fixture",
            url=spec["url"],
            request_params=spec["request_params"],
            payload_format=spec["format"],
        )


def book(day, members, *, weighted=False, index="399001.SZ"):
    return pl.DataFrame(
        [
            dict(
                index_symbol=index,
                symbol=member,
                as_of_date=day,
                weight=1 / len(members) if weighted else None,
                source="cni_weights" if weighted else "eastmoney",
                data_version="v1",
                fetched_at=datetime(2026, 10, 13, tzinfo=timezone.utc),
            )
            for member in members
        ],
        schema={
            "index_symbol": pl.String,
            "symbol": pl.String,
            "as_of_date": pl.Date,
            "weight": pl.Float64,
            "source": pl.String,
            "data_version": pl.String,
            "fetched_at": pl.Datetime("us", "UTC"),
        },
    )


def seed(cfg, frame):
    path = cfg.curated_root / "index_constituents/as_of_date=2015-09/seed.parquet"
    path.parent.mkdir(parents=True)
    frame.write_parquet(path)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    store.commit(
        "index_constituents",
        run_id="fixture",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="fixture",
    )
    return store


def _published(root):
    """The rows of a published root, whether a directory or a catalog revision."""
    from cnequity.query.parquet_scan import scan_parquet_root

    return scan_parquet_root(root, committed=False).collect()


def current(store):
    return _published(store.current_root("index_constituents"))


def no_requests(monkeypatch):
    def unexpected(*args, **kwargs):
        raise AssertionError("offline replay must not request a provider")

    monkeypatch.setattr(httpx.Client, "get", unexpected)


def test_replace_range_keep_actual_weights_other_indices_and_old_revision(
    tmp_path, monkeypatch, settle
):
    cfg = Config(data_root=tmp_path / "lake")
    catalog, payloads = fixture_catalog()
    monkeypatch.setattr(history, "_catalog", lambda index_symbol="399001.SZ": catalog)
    archive_sources(cfg, catalog, payloads)
    no_requests(monkeypatch)
    before = pl.concat(
        [
            book(date(2015, 7, 31), ["000001.SZ"]),
            book(date(2015, 9, 30), catalog["closing_reference"]["members"], weighted=True),
            book(date(2015, 10, 12), ["000555.SZ"]),
            book(date(2015, 5, 1), ["000001.SZ"]),
            book(date(2015, 7, 31), ["688001.SH"], index="000688.SH"),
        ]
    )
    store = seed(cfg, before)
    settle(cfg)
    old = store.current_root("index_constituents")
    preview = history.repair_index_history(cfg)
    assert (
        not preview["applied"]
        and preview["rows"] == 1000
        and preview["retained_weighted_rows"] == 500
    )
    assert current(store).height == 504
    result = history.repair_index_history(cfg, apply=True)
    assert result["applied"] and not result["failures"] and result["revision"] == 2
    frame = current(store)
    assert frame.height == 1503
    assert frame.filter(
        (pl.col("index_symbol") == "399001.SZ") & (pl.col("as_of_date") == date(2015, 7, 31))
    ).is_empty()
    event = frame.filter(pl.col("as_of_date") == date(2015, 7, 1))
    assert (
        event.height == 500
        and "000001.SZ" not in event["symbol"]
        and "000501.SZ" in event["symbol"]
    )
    assert event["weight"].null_count() == 500
    weighted = frame.filter(pl.col("as_of_date") == date(2015, 9, 30))
    assert weighted["weight"].sum() == pytest.approx(1)
    assert weighted["source"].unique().to_list() == ["cni_weights"]
    assert _published(old).height == 504
    assert not history.repair_index_history(cfg, apply=True)["applied"]
    assert store.current_root("index_constituents") != old
    assert (
        current(store)
        .sort("index_symbol", "as_of_date", "symbol")
        .equals(frame.sort("index_symbol", "as_of_date", "symbol"))
    )


def test_conflicting_weights_block_all_mutation(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    catalog, payloads = fixture_catalog()
    monkeypatch.setattr(history, "_catalog", lambda index_symbol="399001.SZ": catalog)
    archive_sources(cfg, catalog, payloads)
    no_requests(monkeypatch)
    wrong = catalog["closing_reference"]["members"][:-1] + ["000502.SZ"]
    store = seed(cfg, book(date(2015, 9, 30), wrong, weighted=True))
    old = store.current_root("index_constituents")
    result = history.repair_index_history(cfg, apply=True)
    assert not result["applied"] and "conflicts" in result["failures"][0]["error"]
    assert store.current_root("index_constituents") == old
    assert "000502.SZ" in current(store)["symbol"]


def test_missing_offline_sources_do_not_bootstrap_or_request(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    catalog, _ = fixture_catalog()
    monkeypatch.setattr(history, "_catalog", lambda index_symbol="399001.SZ": catalog)
    no_requests(monkeypatch)
    result = history.repair_index_history(cfg, apply=True)
    assert not result["applied"] and "--refresh-sources" in result["failures"][0]["error"]
    assert not cfg.curated_root.exists()


def test_capture_missing_sources_then_replay_without_refetch(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    catalog, payloads = fixture_catalog()
    monkeypatch.setattr(history, "_catalog", lambda index_symbol="399001.SZ": catalog)
    calls = []
    mapping = dict(
        zip(
            [catalog["opening"]["url"], catalog["events"][0]["payload"]["url"]],
            payloads,
            strict=True,
        )
    )

    def get(self, url, *, params):
        calls.append(url)
        return httpx.Response(200, content=mapping[url], request=httpx.Request("GET", url))

    monkeypatch.setattr(httpx.Client, "get", get)
    assert history.repair_index_history(cfg, refresh_sources=True)["rows"] == 1000
    assert len(calls) == 2
    no_requests(monkeypatch)
    assert history.repair_index_history(cfg, refresh_sources=True)["rows"] == 1000
    assert history.repair_index_history(cfg)["rows"] == 1000


def test_source_switch_and_changed_bytes_are_explicit_failures(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake", sources={"cni": False})
    catalog, _ = fixture_catalog()
    monkeypatch.setattr(history, "_catalog", lambda index_symbol="399001.SZ": catalog)
    no_requests(monkeypatch)
    result = history.repair_index_history(cfg, refresh_sources=True)
    assert not result["applied"] and "disabled" in result["failures"][0]["error"]
    cfg = Config(data_root=tmp_path / "other")
    monkeypatch.setattr(
        httpx.Client,
        "get",
        lambda self, url, params: httpx.Response(
            200, content=b"changed publisher bytes", request=httpx.Request("GET", url)
        ),
    )
    result = history.repair_index_history(cfg, refresh_sources=True, apply=True)
    assert not result["applied"] and "differs" in result["failures"][0]["error"]
    assert list((cfg.meta_root / "raw/index_constituents/source=cni").rglob("*.gz"))
    assert not cfg.curated_root.exists()


def test_current_payload_cannot_be_backdated_to_requested_history(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "lake")
    catalog, payloads = fixture_catalog()
    catalog["events"][0]["effective_date"] = "2015-07-02"
    monkeypatch.setattr(history, "_catalog", lambda index_symbol="399001.SZ": catalog)
    archive_sources(cfg, catalog, payloads)
    no_requests(monkeypatch)
    result = history.repair_index_history(cfg, apply=True)
    assert not result["applied"] and "effective date differs" in result["failures"][0]["error"]
    assert not cfg.curated_root.exists()


def test_bundled_reference_and_reviewed_sources_are_portable():
    catalog = history._catalog()
    assert catalog["index_symbol"] == "399001.SZ" and len(catalog["events"]) == 38
    assert len(catalog["closing_reference"]["members"]) == 500
    assert "captured_at" not in catalog["closing_reference"]
    assert all(
        set(event["payload"]) == {"url", "request_params", "source", "sha256", "format"}
        for event in catalog["events"]
    )


def star_catalog():
    members = [f"{688001 + i:06d}.SH" for i in range(50)]
    buffer = io.BytesIO()
    pd.DataFrame(
        {
            "指数代码\nIndex Code": ["000688"] * 50,
            "指数名称\nIndex Name": ["科创50"] * 50,
            "证券代码\nSecurities Code": [member[:6] for member in members],
        }
    ).to_excel(buffer, index=False)
    opening = buffer.getvalue()
    launch = (
        "将于2020年7月22日收盘后发布上证科创板50成份指数历史行情，7月23日正式发布实时行情".encode()
    )
    change = "决定于2024年9月13日收市后调整。科创50指数样本调整名单 调出名单 调入名单 证券代码 证券简称 证券代码 证券简称 688001 原成员 688051 新成员 备选名单".encode()
    unchanged = "决定于2024年12月13日收市后调整其他指数。科创50指数样本无变动".encode()

    def spec(payload, name):
        return dict(
            url=f"https://www.sse.com.cn/{name}",
            request_params={},
            source="exchange",
            sha256=hashlib.sha256(payload).hexdigest(),
            format="xlsx" if name.endswith("xlsx") else "html",
        )

    specs = [
        spec(opening, "opening.xlsx"),
        spec(launch, "launch.html"),
        spec(change, "change.html"),
        spec(unchanged, "unchanged.html"),
    ]
    catalog = dict(
        schema_version=1,
        catalog_version="star-fixture",
        index_symbol="000688.SH",
        member_count=50,
        opening_date="2020-07-23",
        closing_date="2024-12-31",
        opening=specs[0],
        opening_notice=specs[1],
        closing_reference=dict(
            url="https://www.csindex.com.cn/reference",
            request_params={},
            publisher_payload_sha256="0" * 64,
            members=members[1:] + ["688051.SH"],
        ),
        events=[dict(effective_date="2024-09-18", parser="sse_announcement", payload=specs[2])],
        verified_no_change=[dict(effective_date="2024-12-16", payload=specs[3])],
    )
    return catalog, list(zip(specs, [opening, launch, change, unchanged], strict=True))


def archive_star(cfg, sources):
    archive = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=["index_constituents"])
    for spec, payload in sources:
        archive.archive(
            "index_constituents",
            payload,
            source=spec["source"],
            run_id="fixture",
            url=spec["url"],
            request_params=spec["request_params"],
            payload_format=spec["format"],
        )


def test_star50_apply_complete_books_after_close_preserve_other_indices(
    tmp_path, monkeypatch, settle
):
    cfg = Config(data_root=tmp_path / "lake")
    catalog, sources = star_catalog()
    monkeypatch.setattr(history, "_catalog", lambda index_symbol: catalog)
    archive_star(cfg, sources)
    no_requests(monkeypatch)
    other = book(date(2024, 9, 18), ["000001.SZ"])
    weighted = book(
        date(2024, 12, 31),
        catalog["closing_reference"]["members"],
        weighted=True,
        index="000688.SH",
    ).with_columns(pl.lit("exchange").alias("source"))
    store = seed(
        cfg, pl.concat([other, weighted, book(date(2024, 9, 30), ["688001.SH"], index="000688.SH")])
    )
    settle(cfg)
    old = store.current_root("index_constituents")
    result = history.repair_index_history(cfg, index_symbol="000688.SH", apply=True)
    assert result["applied"] and result["rows"] == 100 and result["snapshots"] == 2
    assert result["retained_weighted_rows"] == 50 and len(result["source_receipts"]) == 4
    frame = current(store)
    kept = frame.filter(pl.col("index_symbol") == "399001.SZ")
    assert kept.sort(kept.columns).equals(other.sort(other.columns))
    event = frame.filter(
        (pl.col("index_symbol") == "000688.SH") & (pl.col("as_of_date") == date(2024, 9, 18))
    )
    assert (
        event.height == 50 and "688001.SH" not in event["symbol"] and "688051.SH" in event["symbol"]
    )
    assert event["weight"].null_count() == 50 and event["source"].unique().to_list() == ["exchange"]
    held = frame.filter(pl.col("as_of_date") == date(2024, 12, 31))
    assert held.sort(held.columns).equals(weighted.sort(weighted.columns))
    assert frame.filter(pl.col("as_of_date") == date(2024, 12, 16)).is_empty()
    assert _published(old).height == 52
    assert not history.repair_index_history(cfg, index_symbol="000688.SH", apply=True)["applied"]
    assert store.current_root("index_constituents") != old


@pytest.mark.parametrize("field", ["opening_date", "effective_date", "no_change"])
def test_star50_date_authorities_block_backdating(tmp_path, monkeypatch, field):
    cfg = Config(data_root=tmp_path / "lake")
    catalog, sources = star_catalog()
    if field == "opening_date":
        catalog[field] = "2019-12-31"
    elif field == "effective_date":
        catalog["events"][0][field] = "2024-09-13"
    else:
        catalog["verified_no_change"][0]["effective_date"] = "2024-12-13"
    monkeypatch.setattr(history, "_catalog", lambda index_symbol: catalog)
    archive_star(cfg, sources)
    no_requests(monkeypatch)
    report = history.repair_index_history(cfg, index_symbol="000688.SH", apply=True)
    assert report["failures"] and not report["applied"] and not cfg.curated_root.exists()


@pytest.mark.parametrize("effective", ["2024-09-18", "2024-09-13"])
def test_star50_attachment_date_must_be_proved_by_parent_notice(tmp_path, monkeypatch, effective):
    cfg = Config(data_root=tmp_path / "lake")
    catalog, sources = star_catalog()
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer) as writer:
        for name, code in [("调入", "688051"), ("调出", "688001")]:
            pd.DataFrame(
                {"指数代码": ["000688"], "指数简称": ["科创50"], "证券代码": [code]}
            ).to_excel(writer, sheet_name=name, index=False)
    payload = buffer.getvalue()
    spec = dict(
        url="https://www.sse.com.cn/change.xlsx",
        request_params={},
        source="exchange",
        sha256=hashlib.sha256(payload).hexdigest(),
        format="xlsx",
    )
    catalog["events"][0].update(
        parser="sse_attachment",
        date_authority=sources[2][0],
        effective_date=effective,
        payload=spec,
    )
    sources.append((spec, payload))
    monkeypatch.setattr(history, "_catalog", lambda index_symbol: catalog)
    archive_star(cfg, sources)
    no_requests(monkeypatch)
    report = history.repair_index_history(cfg, index_symbol="000688.SH", apply=True)
    if effective == "2024-09-18":
        assert report["applied"] and report["rows"] == 100 and len(report["source_receipts"]) == 5
    else:
        assert "effective date differs" in report["failures"][0]["error"]
        assert not cfg.curated_root.exists()


def test_star50_portable_catalog_has_whole_chain_and_no_fabricated_receipts():
    catalog = history._catalog("000688.SH")
    assert catalog["opening_date"] == "2020-07-23" and len(catalog["events"]) == 24
    assert len(catalog["closing_reference"]["members"]) == 50
    assert catalog["verified_no_change"][0]["effective_date"] == "2024-12-16"
    assert "captured_at" not in catalog["closing_reference"]
    assert set(event["parser"] for event in catalog["events"]) == {
        "sse_announcement",
        "sse_attachment",
    }
    with pytest.raises(ValueError, match="unsupported"):
        history._catalog("000300.SH")
