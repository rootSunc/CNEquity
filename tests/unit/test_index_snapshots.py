import io
from datetime import date, datetime, timezone

import pandas as pd
import polars as pl
import pytest
from tests.catalog_seed import commit_tree

from cnequity.adapters.exchange.index_snapshots import CSI_ROOT, parse_index_snapshot
from cnequity.config import Config
from cnequity.domain.schemas import DATASET_SCHEMAS
from cnequity.storage.atomic import write_json_atomic
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.repairs.index_snapshots import repair_index_snapshots
from cnequity.storage.revisions import RevisionStore


def workbook(*, weight=True, duplicate=False, wrong_index=False, count=50, total=100, offset=0):
    codes = list(range(688001 + offset, 688001 + offset + count))
    if duplicate:
        codes[-1] = codes[0]
    rows = [
        {
            "日期Date": 20260930,
            "指数代码 Index Code": 300 if wrong_index else 688,
            "成份券代码Constituent Code": code,
            "交易所Exchange": "上海证券交易所",
            **({"权重(%)weight": total / count} if weight else {}),
        }
        for code in codes
    ]
    out = io.BytesIO()
    pd.DataFrame(rows).to_excel(out, index=False)
    return out.getvalue()


def test_source_date_and_percentage_conversion():
    frame = parse_index_snapshot(workbook(), "000688.SH", weights=True)
    assert frame["as_of_date"].unique().to_list() == [date(2026, 9, 30)]
    assert frame["weight"].sum() == pytest.approx(1)
    assert frame["weight"][0] == 0.02
    members = parse_index_snapshot(workbook(weight=False), "000688.SH", weights=False)
    assert members["weight"].null_count() == 50


@pytest.mark.parametrize(
    "args",
    [
        {"duplicate": True},
        {"wrong_index": True},
        {"count": 49},
        {"total": 50},
        {"total": float("nan")},
    ],
)
def test_incomplete_or_misidentified_books_rejected(args):
    with pytest.raises(ValueError):
        parse_index_snapshot(workbook(**args), "000688.SH", weights=True)


def _published(root):
    """The rows of a published root, whether a directory or a catalog revision."""
    from cnequity.query.parquet_scan import scan_parquet_root

    return scan_parquet_root(root, committed=False).collect()


def test_replace_whole_book_and_keep_old_revision(tmp_path, monkeypatch, settle):
    cfg = Config(data_root=tmp_path / "lake")
    monkeypatch.setattr(
        "cnequity.storage.repairs.index_snapshots.last_closed_session",
        lambda *a: date(2026, 10, 12),
    )
    now = datetime(2026, 10, 12, tzinfo=timezone.utc)
    root = cfg.curated_root / "index_constituents" / "as_of_date=2026-09"
    root.mkdir(parents=True)
    path = root / "part-0.parquet"
    old = pl.DataFrame(
        [
            dict(
                index_symbol="000688.SH",
                symbol="688999.SH",
                as_of_date=date(2026, 9, 30),
                weight=None,
                source="eastmoney",
                data_version="v1",
                fetched_at=now,
            ),
            dict(
                index_symbol="000300.SH",
                symbol="600519.SH",
                as_of_date=date(2026, 9, 30),
                weight=None,
                source="eastmoney",
                data_version="v1",
                fetched_at=now,
            ),
        ],
        schema=DATASET_SCHEMAS["index_constituents"],
    )
    old.write_parquet(path)
    store = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    commit_tree(
        store,
        "index_constituents",
        run_id="seed",
        changed_files=[path],
        schema_version=1,
        contract_fingerprint="fixture",
    )
    old_root = store.current_root("index_constituents")
    archive = RawPayloadArchive(cfg.meta_root, enabled=True, datasets=["index_constituents"])
    for kind, weighted in [("cons", False), ("closeweight", True)]:
        record = archive.archive(
            "index_constituents",
            workbook(weight=weighted, offset=0 if weighted else 50),
            source="exchange",
            run_id="proof",
            url=f"{CSI_ROOT}/{kind}/000688{kind}.xls",
            request_params={},
            payload_format="xls",
        )
        write_json_atomic(
            cfg.meta_root
            / "source_cache"
            / "index_snapshots"
            / f"000688.SH-{'weights' if weighted else 'members'}.json",
            record.to_dict(),
        )
    settle(cfg)
    assert repair_index_snapshots(cfg, indices=["000688.SH"])["weighted_rows"] == 50
    result = repair_index_snapshots(cfg, indices=["000688.SH"], apply=True)
    assert result["applied"] and result["revision"] == 2
    current = _published(store.current_root("index_constituents"))
    assert current.height == 51
    assert "688999.SH" not in current["symbol"].to_list()
    assert current.filter(pl.col("index_symbol") == "000688.SH")["weight"].null_count() == 0
    assert _published(old_root).height == 2
    assert not repair_index_snapshots(cfg, indices=["000688.SH"], apply=True)["applied"]
    # A subsequent failed weight download must not replace a validated book
    # with an unweighted cohort, even if that cohort differs on the same date.
    weights_cache = cfg.meta_root / "source_cache" / "index_snapshots" / "000688.SH-weights.json"
    weights_cache.unlink()
    result = repair_index_snapshots(cfg, indices=["000688.SH"], apply=True)
    assert not result["applied"] and result["snapshots_retained_weights"]
    assert any("conflicts" in f["error"] for f in result["failures"])
    assert _published(store.current_root("index_constituents")).equals(current)
